package ru.itmo.graphdb.router;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.itmo.graphdb.client.GraphDBClient;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.network.DBServer;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.sharding.ShardCommandTranslator;
import ru.itmo.graphdb.sharding.ShardEndpoint;
import ru.itmo.graphdb.sharding.ShardRouter;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardedClusterIntegrationTest {

    private static final int SHARD_COUNT = 2;

    private final List<DBServer> shardServers = new ArrayList<>();
    private final List<ShardEndpoint> shardEndpoints = new ArrayList<>();
    private RouterServer routerServer;
    private GraphDBClient client;
    private ShardRouter router;

    @BeforeEach
    void setUp() throws IOException {
        for (int i = 0; i < SHARD_COUNT; i++) {
            final int shardId = i;
            InMemoryGraphStorage storage = new InMemoryGraphStorage();
            MetricRegistry metrics = new MetricRegistry();
            SimpleDijkstraPathFinder pf = new SimpleDijkstraPathFinder();

            DBServer shard = new DBServer(
                    () -> new ShardCommandTranslator(storage, metrics, pf, shardId));
            shard.start(0);

            shardServers.add(shard);
            shardEndpoints.add(new ShardEndpoint(shardId, "localhost", shard.getPort()));
        }

        router = new ShardRouter(shardEndpoints);
        routerServer = new RouterServer(router);
        routerServer.start(0);

        client = new GraphDBClient("localhost", routerServer.getPort());
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (routerServer != null) {
            routerServer.close();
        }
        for (DBServer s : shardServers) {
            s.stop();
        }
    }

    // ---------- helpers ----------

    /**
     * Возвращает два id, гарантированно лежащих на разных шардах
     * при текущем {@link ShardRouter}. Так как хэш детерминирован, перебираем
     * пространство небольших id.
     */
    private String[] twoIdsOnDifferentShards() {
        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 1000; j++) {
                String a = "node" + i;
                String b = "node" + j;
                if (router.shardForNode(a) != router.shardForNode(b)) {
                    return new String[]{a, b};
                }
            }
        }
        throw new IllegalStateException("Cannot find two ids on different shards");
    }

    // ---------- basic routing ----------

    @Test
    void addNodeGoesToHomeShard() {
        String id = "warehouse-1";
        assertEquals("OK: node '" + id + "' added", client.execute("ADD_NODE " + id));

        int home = router.shardForNode(id);
        DBServer homeServer = shardServers.get(home);
        // Проверяем на самом шарде через внутренний клиент (в тесте — прямо TCP)
        try (GraphDBClient direct = new GraphDBClient("localhost", homeServer.getPort())) {
            String resp = direct.execute("_GET_NODE " + id);
            assertTrue(resp.startsWith("NODE\t" + id), "actual: " + resp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void setParameterReachesHomeShard() {
        String id = "warehouse-2";
        client.execute("ADD_NODE " + id);
        String resp = client.execute("SET_PARAMETER " + id + " capacity 100");
        assertTrue(resp.startsWith("OK"), "actual: " + resp);

        int home = router.shardForNode(id);
        try (GraphDBClient direct = new GraphDBClient(
                "localhost", shardServers.get(home).getPort())) {
            String node = direct.execute("_GET_NODE " + id);
            assertTrue(node.contains("capacity=100"), "actual: " + node);
        }
    }

    // ---------- pathfinding ----------

    @Test
    void pathWithinSingleShard() {
        String[] ids = twoIdsOnDifferentShards();   // не важно, что разные — маршрут может быть один
        String a = ids[0];
        String b = a + "_neighbor";                 // оба могут оказаться на одном шарде
        client.execute("ADD_NODE " + a);
        client.execute("ADD_NODE " + b);
        client.execute("ADD_EDGE " + a + " " + b + " distance 5");

        String resp = client.execute("PATH_EDGE " + a + " " + b + " distance");
        assertTrue(resp.contains(a + " -> " + b), "actual: " + resp);
        assertTrue(resp.contains("Total: 5.0"), "actual: " + resp);
    }

    @Test
    void pathAcrossShards() {
        String[] ids = twoIdsOnDifferentShards();
        String a = ids[0];
        String b = ids[1];

        assertTrue(client.execute("ADD_NODE " + a).startsWith("OK"));
        assertTrue(client.execute("ADD_NODE " + b).startsWith("OK"));
        assertTrue(client.execute("ADD_EDGE " + a + " " + b + " distance 7").startsWith("OK"));

        String resp = client.execute("PATH_EDGE " + a + " " + b + " distance");
        assertTrue(resp.contains(a + " -> " + b), "actual: " + resp);
        assertTrue(resp.contains("Total: 7.0"), "actual: " + resp);
    }

    @Test
    void pathAcrossTwoHopsWithIntermediateNode() {
        // A → B → C, каждый узел на произвольном шарде
        String a = "A-node";
        String b = "B-node";
        String c = "C-node";
        client.execute("ADD_NODE " + a);
        client.execute("ADD_NODE " + b);
        client.execute("ADD_NODE " + c);
        client.execute("ADD_EDGE " + a + " " + b + " distance 3");
        client.execute("ADD_EDGE " + b + " " + c + " distance 4");

        String resp = client.execute("PATH_EDGE " + a + " " + c + " distance");
        assertTrue(resp.contains(a + " -> " + b), "actual: " + resp);
        assertTrue(resp.contains(b + " -> " + c), "actual: " + resp);
        assertTrue(resp.contains("Total: 7.0"), "actual: " + resp);
    }

    @Test
    void pathChoosesCheaperOfTwoRoutes() {
        // A→B→C (3+4=7) vs A→D→C (2+2=4). Должен быть выбран второй.
        client.execute("ADD_NODE A");
        client.execute("ADD_NODE B");
        client.execute("ADD_NODE C");
        client.execute("ADD_NODE D");
        client.execute("ADD_EDGE A B distance 3");
        client.execute("ADD_EDGE B C distance 4");
        client.execute("ADD_EDGE A D distance 2");
        client.execute("ADD_EDGE D C distance 2");

        String resp = client.execute("PATH_EDGE A C distance");
        assertTrue(resp.contains("A -> D"), "actual: " + resp);
        assertTrue(resp.contains("D -> C"), "actual: " + resp);
        assertTrue(resp.contains("Total: 4.0"), "actual: " + resp);
    }

    @Test
    void noPathReturnsMessage() {
        client.execute("ADD_NODE X");
        client.execute("ADD_NODE Y");
        String resp = client.execute("PATH_EDGE X Y distance");
        assertTrue(resp.startsWith("No path"), "actual: " + resp);
    }

    // ---------- cross-shard deletes ----------

    @Test
    void deleteNodeRemovesIncomingEdgesAcrossShards() {
        String[] ids = twoIdsOnDifferentShards();
        String a = ids[0];
        String b = ids[1];

        client.execute("ADD_NODE " + a);
        client.execute("ADD_NODE " + b);
        client.execute("ADD_EDGE " + a + " " + b + " distance 5");

        assertTrue(client.execute("DELETE_NODE " + b).startsWith("OK"));

        String resp = client.execute("PRINT_EDGES " + a);
        assertEquals("(no edges)", resp);
    }

    @Test
    void deleteEdgeRoutesToSourceShard() {
        String[] ids = twoIdsOnDifferentShards();
        String a = ids[0];
        String b = ids[1];
        client.execute("ADD_NODE " + a);
        client.execute("ADD_NODE " + b);
        client.execute("ADD_EDGE " + a + " " + b + " distance 5");

        String edgeId = a + "->" + b;
        assertTrue(client.execute("DELETE_EDGE " + edgeId).startsWith("OK"));
        String resp = client.execute("PATH_EDGE " + a + " " + b + " distance");
        assertTrue(resp.startsWith("No path"), "actual: " + resp);
    }

    // ---------- metrics fan-out ----------

    @Test
    void addCustomMetricFansOutToShards() {
        assertTrue(client.execute("ADD_METRIC fuel Fuel").startsWith("OK"));

        // Проверим, что шарды тоже знают про fuel — иначе ADD_EDGE с fuel упадёт.
        String a = "metric-test-a";
        String b = "metric-test-b";
        client.execute("ADD_NODE " + a);
        client.execute("ADD_NODE " + b);
        String addEdge = client.execute("ADD_EDGE " + a + " " + b + " fuel 42");
        assertTrue(addEdge.startsWith("OK"), "actual: " + addEdge);

        String path = client.execute("PATH_EDGE " + a + " " + b + " fuel");
        assertTrue(path.contains("Total: 42.0"), "actual: " + path);
    }

    @Test
    void deleteMetricRemovesFromRegistry() {
        client.execute("ADD_METRIC fuel2 Fuel2");
        assertTrue(client.execute("DELETE_METRIC fuel2").startsWith("OK"));
        String resp = client.execute("PRINT_METRICS");
        assertTrue(!resp.contains("fuel2"), "actual: " + resp);
    }

    @Test
    void deleteDefaultMetricIsRejected() {
        String resp = client.execute("DELETE_METRIC distance");
        assertTrue(resp.startsWith("ERROR"), "actual: " + resp);
    }

    // ---------- push SHARD_MAP ----------

    @Test
    void shardMapIsPushedOnConnect() {
        AtomicReference<String> received = new AtomicReference<>();
        client.setPushListener(received::set);

        // Триггерит lazy connect; push уходит до ответа на REQ.
        client.execute("PRINT_METRICS");

        assertNotNull(received.get(), "SHARD_MAP push was not received");
        assertTrue(received.get().startsWith("SHARD_MAP"), "actual: " + received.get());
        assertTrue(received.get().contains("0="), "actual: " + received.get());
        assertTrue(received.get().contains("1="), "actual: " + received.get());
    }

    // ---------- clear-all ----------

    @Test
    void createDbClearsAllShards() {
        String a = "cl-a";
        String b = "cl-b";
        client.execute("ADD_NODE " + a);
        client.execute("ADD_NODE " + b);
        client.execute("ADD_EDGE " + a + " " + b + " distance 1");

        assertTrue(client.execute("CREATE_DB fresh").startsWith("OK"));

        String resp = client.execute("PATH_EDGE " + a + " " + b + " distance");
        assertTrue(resp.startsWith("No path"), "actual: " + resp);

        // Убедимся, что узлы тоже исчезли с домашних шардов
        assertNull(getNodeDirect(a));
        assertNull(getNodeDirect(b));
    }

    // ---------- direct shard access for assertions ----------

    private String getNodeDirect(String id) {
        int shard = router.shardForNode(id);
        try (GraphDBClient direct = new GraphDBClient(
                "localhost", shardServers.get(shard).getPort())) {
            String resp = direct.execute("_GET_NODE " + id);
            return "NOT_FOUND".equals(resp) ? null : resp;
        }
    }
}