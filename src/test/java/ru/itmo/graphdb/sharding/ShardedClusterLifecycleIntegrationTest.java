package ru.itmo.graphdb.router;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.graphdb.client.GraphDBClient;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.network.DBServer;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.sharding.ShardCommandTranslator;
import ru.itmo.graphdb.sharding.ShardEndpoint;
import ru.itmo.graphdb.sharding.ShardRouter;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SAVE_DB/READ_DB на шардированном кластере: Router раскладывает один
 * логический дамп в {@code <name>.shard<i>} для каждого шарда, а
 * {@code READ_DB <name>} затем читает те же файлы. Проверяем, что
 * граф через шард-границы переживает полный цикл.
 */
class ShardedClusterLifecycleIntegrationTest {

    @TempDir
    Path tempDir;

    private final List<DBServer> shardServers = new ArrayList<>();
    private final List<ShardEndpoint> shardEndpoints = new ArrayList<>();
    private RouterServer routerServer;
    private ShardRouter router;
    private GraphDBClient client;

    @BeforeEach
    void setUp() throws IOException {
        for (int i = 0; i < 2; i++) {
            final int shardId = i;
            DBServer s = new DBServer(() -> new ShardCommandTranslator(
                    new InMemoryGraphStorage(), new MetricRegistry(),
                    new SimpleDijkstraPathFinder(), shardId));
            s.start(0);
            shardServers.add(s);
            shardEndpoints.add(new ShardEndpoint(shardId, "localhost", s.getPort()));
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

    private String findIdOnShard(int shardId, int startFrom) {
        for (int i = startFrom; i < startFrom + 100_000; i++) {
            String id = "node" + i;
            if (router.shardForNode(id) == shardId) {
                return id;
            }
        }
        throw new IllegalStateException("Cannot find id on shard " + shardId);
    }

    private String dbName() {
        // Абсолютный путь, но без пробелов — Router оперирует путями
        // как «одним токеном».
        return tempDir.resolve("snapshot").toString();
    }

    // ---------- 1. save + load восстанавливает кросс-шардовый путь ----------

    @Test
    void saveAndLoadPreserveCrossShardPath() {
        String a = findIdOnShard(0, 0);
        String b = findIdOnShard(1, 0);

        client.execute("ADD_NODE " + a);
        client.execute("ADD_NODE " + b);
        client.execute("ADD_EDGE " + a + " " + b + " distance 5");
        client.execute("ADD_EDGE " + a + " " + b + " time 2");

        String db = dbName();
        String saveResp = client.execute("SAVE_DB " + db);
        assertTrue(saveResp.startsWith("OK"), "actual:\n" + saveResp);

        // Проверяем, что per-shard файлы действительно созданы.
        assertTrue(Files.exists(Path.of(db + ".shard0")),
                "missing " + db + ".shard0");
        assertTrue(Files.exists(Path.of(db + ".shard1")),
                "missing " + db + ".shard1");

        // Сносим всё и восстанавливаем.
        client.execute("CREATE_DB wiped");
        String empty = client.execute("PATH_EDGE " + a + " " + b + " distance");
        assertTrue(empty.startsWith("No path"), "actual:\n" + empty);

        String readResp = client.execute("READ_DB " + db);
        assertTrue(readResp.startsWith("OK"), "actual:\n" + readResp);

        String afterLoad = client.execute("PATH_EDGE " + a + " " + b + " distance");
        assertTrue(afterLoad.contains(a + " -> " + b), "actual:\n" + afterLoad);
        assertTrue(afterLoad.contains("Total: 5.0"), "actual:\n" + afterLoad);

        String afterLoadTime = client.execute("PATH_EDGE " + a + " " + b + " time");
        assertTrue(afterLoadTime.contains("Total: 2.0"), "actual:\n" + afterLoadTime);
    }

    // ---------- 2. READ_DB отсутствующего имени → ERROR ----------

    @Test
    void loadMissingDatabaseReportsError() {
        String resp = client.execute("READ_DB " + tempDir.resolve("nonexistent"));
        assertTrue(resp.startsWith("ERROR:"), "actual:\n" + resp);
    }

    // ---------- 3. SAVE_DB создаёт ровно по файлу на шард ----------

    @Test
    void saveCreatesExactlyOneFilePerShard() throws IOException {
        client.execute("ADD_NODE x");
        String db = dbName();
        assertTrue(client.execute("SAVE_DB " + db).startsWith("OK"));

        try (var files = Files.list(tempDir)) {
            long count = files
                    .filter(p -> p.getFileName().toString().startsWith("snapshot.shard"))
                    .count();
            assertTrue(count == 2L,
                    "expected exactly 2 shard files, found " + count);
        }
    }
}