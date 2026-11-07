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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Проверки протокола: {@code PUSH} не путается с {@code RESP}, push
 * отдаётся листенеру, буферизуется до подписки, и каждый клиент
 * получает свой {@code SHARD_MAP}.
 */
class PushProtocolIntegrationTest {

    private final List<DBServer> shardServers = new ArrayList<>();
    private final List<ShardEndpoint> shardEndpoints = new ArrayList<>();
    private final List<GraphDBClient> clients = new ArrayList<>();
    private RouterServer routerServer;

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
        ShardRouter router = new ShardRouter(shardEndpoints);
        routerServer = new RouterServer(router);
        routerServer.start(0);
    }

    @AfterEach
    void tearDown() {
        for (GraphDBClient c : clients) {
            c.close();
        }
        clients.clear();
        if (routerServer != null) {
            routerServer.close();
        }
        for (DBServer s : shardServers) {
            s.stop();
        }
    }

    private GraphDBClient newClient() {
        GraphDBClient c = new GraphDBClient("localhost", routerServer.getPort());
        clients.add(c);
        return c;
    }

    // ---------- 1. listener установлен ДО первого execute ----------

    @Test
    void listenerSetBeforeExecuteReceivesShardMap() {
        GraphDBClient c = newClient();
        AtomicReference<String> received = new AtomicReference<>();
        c.setPushListener(received::set);

        // Первый execute триггерит lazy-connect, во время которого Router
        // пушит SHARD_MAP. execute обязан вернуть RESP, а не push.
        String resp = c.execute("PRINT_METRICS");
        assertTrue(resp.contains("distance"),
                "RESP must not be consumed by push, actual:\n" + resp);

        String pushed = received.get();
        assertNotNull(pushed, "SHARD_MAP push was not delivered");
        assertTrue(pushed.startsWith("SHARD_MAP"), "actual: " + pushed);
        assertTrue(pushed.contains("0="), "actual: " + pushed);
        assertTrue(pushed.contains("1="), "actual: " + pushed);
    }

    // ---------- 2. listener установлен ПОСЛЕ первого execute ----------

    @Test
    void listenerSetAfterExecuteReceivesBufferedShardMap() {
        GraphDBClient c = newClient();
        // Без листенера — SHARD_MAP уйдёт в буфер.
        String resp = c.execute("PRINT_METRICS");
        assertTrue(resp.contains("distance"));

        // Теперь подписываемся — буферизованное должно дойти.
        AtomicReference<String> received = new AtomicReference<>();
        c.setPushListener(received::set);

        String pushed = received.get();
        assertNotNull(pushed, "buffered SHARD_MAP was not delivered on setPushListener");
        assertTrue(pushed.startsWith("SHARD_MAP"), "actual: " + pushed);
    }

    // ---------- 3. push не ломает поток запрос-ответ ----------

    @Test
    void responseAfterPushIsTheRealResponse() {
        GraphDBClient c = newClient();
        AtomicReference<String> received = new AtomicReference<>();
        c.setPushListener(received::set);

        // Первый запрос: push(SHARD_MAP), затем RESP.
        String r1 = c.execute("ADD_NODE alpha");
        assertEquals("OK: node 'alpha' added", r1);

        // Второй запрос: push не приходит, сразу RESP. Убедимся, что клиент
        // не ждёт push и не съедает следующий ответ.
        String r2 = c.execute("ADD_NODE beta");
        assertEquals("OK: node 'beta' added", r2);

        String r3 = c.execute("PATH_EDGE alpha beta distance");
        assertTrue(r3.startsWith("No path"),
                "expected 'No path' (no edges), actual:\n" + r3);

        // Listener получил ровно один push — SHARD_MAP.
        assertNotNull(received.get());
        assertTrue(received.get().startsWith("SHARD_MAP"));
    }

    // ---------- 4. у каждого клиента свой SHARD_MAP ----------

    @Test
    void eachClientGetsItsOwnShardMap() {
        GraphDBClient a = newClient();
        GraphDBClient b = newClient();

        AtomicReference<String> pushA = new AtomicReference<>();
        AtomicReference<String> pushB = new AtomicReference<>();
        a.setPushListener(pushA::set);
        b.setPushListener(pushB::set);

        a.execute("PRINT_METRICS");
        b.execute("PRINT_METRICS");

        assertNotNull(pushA.get(), "client A did not get SHARD_MAP");
        assertNotNull(pushB.get(), "client B did not get SHARD_MAP");
        assertTrue(pushA.get().startsWith("SHARD_MAP"));
        assertTrue(pushB.get().startsWith("SHARD_MAP"));
        // Мэп содержит актуальные порты — они одинаковы для обоих клиентов.
        assertEquals(pushA.get(), pushB.get(),
                "both clients must receive the same topology");
    }

    // ---------- 5. listener не мешает повторным execute ----------

    @Test
    void manyExecutesWithListenerStillReturnCorrectResponses() {
        GraphDBClient c = newClient();
        int[] counter = {0};
        c.setPushListener(msg -> {
            assertTrue(msg.startsWith("SHARD_MAP"),
                    "unexpected push: " + msg);
            counter[0]++;
        });

        for (int i = 0; i < 20; i++) {
            String id = "seq-" + i;
            assertEquals("OK: node '" + id + "' added",
                    c.execute("ADD_NODE " + id));
        }

        // За 20 execute клиент переподключался максимум один раз (lazy connect),
        // поэтому SHARD_MAP должен прийти ровно один раз.
        assertEquals(1, counter[0],
                "expected exactly one SHARD_MAP push for a single connection");
    }
}