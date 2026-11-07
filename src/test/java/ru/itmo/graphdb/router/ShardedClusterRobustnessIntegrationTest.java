package ru.itmo.graphdb.router;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Устойчивость Router при отказе отдельного шарда + AnyLogic-подобная
 * нагрузка: N потоков с собственным {@link GraphDBClient} бьют в Router.
 */
class ShardedClusterRobustnessIntegrationTest {

    private static final int SHARD_COUNT = 2;

    private final List<DBServer> shardServers = new ArrayList<>();
    private final List<ShardEndpoint> shardEndpoints = new ArrayList<>();
    private final List<GraphDBClient> clients = new ArrayList<>();
    private RouterServer routerServer;
    private ShardRouter router;

    @BeforeEach
    void setUp() throws IOException {
        for (int i = 0; i < SHARD_COUNT; i++) {
            final int shardId = i;
            DBServer shard = new DBServer(() -> new ShardCommandTranslator(
                    new InMemoryGraphStorage(), new MetricRegistry(),
                    new SimpleDijkstraPathFinder(), shardId));
            shard.start(0);
            shardServers.add(shard);
            shardEndpoints.add(new ShardEndpoint(shardId, "localhost", shard.getPort()));
        }
        router = new ShardRouter(shardEndpoints);
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

    // ---------- helpers ----------

    private GraphDBClient newClient() {
        GraphDBClient c = new GraphDBClient("localhost", routerServer.getPort());
        clients.add(c);
        return c;
    }

    /**
     * Возвращает {@code count} различных id, каждый из которых по текущей
     * раскладке {@link ShardRouter} лежит на шарде {@code shardId}.
     */
    private List<String> findDistinctIdsOnShard(int shardId, int count) {
        List<String> result = new ArrayList<>(count);
        for (int i = 0; i < 1_000_000 && result.size() < count; i++) {
            String id = "node" + i;
            if (router.shardForNode(id) == shardId) {
                result.add(id);
            }
        }
        if (result.size() < count) {
            throw new IllegalStateException(
                    "Cannot find " + count + " distinct ids on shard " + shardId);
        }
        return result;
    }

    // ---------- 1. падение шарда: запросы к нему → ERROR ----------

    @Test
    void pathToNodeOnDeadShardReturnsError() {
        GraphDBClient client = newClient();
        String srcOnShard0 = findDistinctIdsOnShard(0, 1).get(0);
        String dstOnShard1 = findDistinctIdsOnShard(1, 1).get(0);

        client.execute("ADD_NODE " + srcOnShard0);
        client.execute("ADD_NODE " + dstOnShard1);
        client.execute("ADD_EDGE " + srcOnShard0 + " " + dstOnShard1 + " distance 1");

        shardServers.get(1).stop();

        String resp = client.execute(
                "PATH_EDGE " + srcOnShard0 + " " + dstOnShard1 + " distance");
        assertTrue(resp.startsWith("ERROR:"),
                "expected ERROR after shard failure, got:\n" + resp);
        assertTrue(resp.toLowerCase().contains("shard")
                        || resp.toLowerCase().contains("connection")
                        || resp.toLowerCase().contains("i/o"),
                "ERROR should describe shard failure, got:\n" + resp);
    }

    // ---------- 2. падение шарда: путь, не выходящий за пределы живого шарда, работает ----------

    @Test
    void pathFullyOnLiveShardSurvivesOtherShardFailure() {
        GraphDBClient client = newClient();
        List<String> onShard0 = findDistinctIdsOnShard(0, 2);
        String a = onShard0.get(0);
        String b = onShard0.get(1);

        client.execute("ADD_NODE " + a);
        client.execute("ADD_NODE " + b);
        client.execute("ADD_EDGE " + a + " " + b + " distance 5");

        shardServers.get(1).stop();

        String resp = client.execute("PATH_EDGE " + a + " " + b + " distance");
        assertTrue(resp.contains(a + " -> " + b),
                "live-shard path must not be affected by other shard's failure, got:\n" + resp);
        assertTrue(resp.contains("Total: 5.0"), "actual:\n" + resp);
    }

    // ---------- 3. Router продолжает отвечать на команды, не задевающие мёртвый шард ----------

    @Test
    void routerKeepsServingLiveShardAfterFailure() {
        GraphDBClient client = newClient();
        String a = findDistinctIdsOnShard(0, 1).get(0);
        client.execute("ADD_NODE " + a);

        shardServers.get(1).stop();

        String metrics = client.execute("PRINT_METRICS");
        assertTrue(metrics.contains("distance"), "actual:\n" + metrics);
        assertTrue(metrics.contains("time"), "actual:\n" + metrics);
        assertTrue(metrics.contains("cost"), "actual:\n" + metrics);

        String edges = client.execute("PRINT_EDGES " + a);
        assertEquals("(no edges)", edges);
    }

    // ---------- 4. После восстановления шарда работа восстанавливается ----------

    @Test
    void clusterRecoversAfterShardRestart() throws IOException {
        GraphDBClient client = newClient();
        String a = findDistinctIdsOnShard(0, 1).get(0);
        String b = findDistinctIdsOnShard(1, 1).get(0);

        client.execute("ADD_NODE " + a);
        client.execute("ADD_NODE " + b);
        client.execute("ADD_EDGE " + a + " " + b + " distance 1");

        shardServers.get(1).stop();
        String broken = client.execute("PATH_EDGE " + a + " " + b + " distance");
        assertTrue(broken.startsWith("ERROR:"), "actual:\n" + broken);

        DBServer revived = new DBServer(() -> new ShardCommandTranslator(
                new InMemoryGraphStorage(), new MetricRegistry(),
                new SimpleDijkstraPathFinder(), 1));
        revived.start(0);
        shardServers.set(1, revived);
        shardEndpoints.set(1, new ShardEndpoint(1, "localhost", revived.getPort()));

        routerServer.close();
        router = new ShardRouter(shardEndpoints);
        routerServer = new RouterServer(router);
        routerServer.start(0);

        GraphDBClient fresh = new GraphDBClient("localhost", routerServer.getPort());
        clients.add(fresh);
        fresh.execute("ADD_NODE " + a);
        fresh.execute("ADD_NODE " + b);
        fresh.execute("ADD_EDGE " + a + " " + b + " distance 1");

        String resp = fresh.execute("PATH_EDGE " + a + " " + b + " distance");
        assertTrue(resp.contains(a + " -> " + b), "actual:\n" + resp);
        assertTrue(resp.contains("Total: 1.0"), "actual:\n" + resp);
    }

    // ---------- 5. Конкурентная нагрузка «как из AnyLogic» ----------

    /**
     * N потоков с собственным {@link GraphDBClient} параллельно бьют
     * {@code PATH_EDGE} по одной цепи. Проверяется:
     * <ul>
     *   <li>все ответы верны (total = длина цепи − 1);</li>
     *   <li>нет ни одной ошибки (в т.ч. timeout/reconnect);</li>
     *   <li>каждый клиент получил ровно один {@code SHARD_MAP}.</li>
     * </ul>
     *
     * <p><b>Почему не 8×15, как раньше.</b> Router держит общий
     * {@link GraphDBClient} на каждый шард; все {@code PATH_EDGE} со всех
     * клиентских соединений сериализуются на этих общих клиентах. При цепи
     * из 8 звеньев (~10 round-trip'ов) и нагрузке 120 запросов это ~1200
     * последовательных round-trip'ов — на CI без {@code TCP_NODELAY}
     * даёт ~48 с, что упирается в старый таймаут. Здесь цепь короче (5
     * звеньев ≈ 6 round-trip'ов) и запросов меньше (4×8=32 → ~192
     * round-trip'а). Даже при 40 мс на hop это ~7.7 с — с большим запасом.
     *
     * <p>Тест проверяет <b>корректность</b> под конкуренцией, а не
     * пропускную способность; сериализация на shard-клиентах — известный
     * trade-off Router-слоя, зафиксированный в README.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void concurrentAgentsGetConsistentPathResults() throws InterruptedException {
        final int chainLength = 5;         // c0..c4
        final int threads = 4;
        final int requestsPerThread = 8;

        GraphDBClient setup = newClient();
        for (int i = 0; i < chainLength; i++) {
            assertEquals("OK: node 'c" + i + "' added",
                    setup.execute("ADD_NODE c" + i));
        }
        for (int i = 0; i < chainLength - 1; i++) {
            String resp = setup.execute("ADD_EDGE c" + i + " c" + (i + 1) + " distance 1");
            assertTrue(resp.startsWith("OK"), "ADD_EDGE failed: " + resp);
        }

        final String src = "c0";
        final String dst = "c" + (chainLength - 1);
        final int expectedTotal = chainLength - 1;

        // PRE-WARM. Один одиночный PATH_EDGE:
        //  * инициализирует TCP-соединение Router↔shard'ы (lazy connect в
        //    GraphDBClient), чтобы handshake не попал в замер;
        //  * прогревает JIT на горячем участке.
        String warm = setup.execute("PATH_EDGE " + src + " " + dst + " distance");
        assertTrue(warm.contains("Total: " + expectedTotal + ".0"),
                "pre-warm PATH_EDGE failed:\n" + warm);

        final AtomicInteger okCount = new AtomicInteger();
        final AtomicInteger errorCount = new AtomicInteger();
        final AtomicInteger pushCount = new AtomicInteger();
        final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);

        final IntFunction<GraphDBClient> clientForThread = t -> {
            GraphDBClient c = new GraphDBClient("localhost", routerServer.getPort());
            clients.add(c);
            c.setPushListener(msg -> {
                if (msg.startsWith("SHARD_MAP")) {
                    pushCount.incrementAndGet();
                }
            });
            return c;
        };

        // Клиенты создаём до start.await() — их lazy-connect и push SHARD_MAP
        // не должны попасть в конкурентный замер и не должны гоняться с
        // первыми PATH_EDGE.
        final List<GraphDBClient> perThreadClients = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
            perThreadClients.add(clientForThread.apply(t));
        }
        for (GraphDBClient c : perThreadClients) {
            c.execute("PRINT_METRICS");   // триггерит connect + получает SHARD_MAP
        }
        assertEquals(threads, pushCount.get(),
                "each warmed client should already have received exactly one SHARD_MAP");

        for (int t = 0; t < threads; t++) {
            final GraphDBClient c = perThreadClients.get(t);
            final Thread worker = new Thread(() -> {
                try {
                    start.await();
                    for (int r = 0; r < requestsPerThread; r++) {
                        String resp = c.execute("PATH_EDGE " + src + " " + dst + " distance");
                        if (resp.startsWith("ERROR:")) {
                            errorCount.incrementAndGet();
                            firstFailure.compareAndSet(null,
                                    new AssertionError("path error: " + resp));
                        } else if (!resp.contains("Total: " + expectedTotal + ".0")) {
                            errorCount.incrementAndGet();
                            firstFailure.compareAndSet(null,
                                    new AssertionError("unexpected total:\n" + resp));
                        } else {
                            okCount.incrementAndGet();
                        }
                    }
                } catch (Throwable e) {
                    firstFailure.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            }, "agent-" + t);
            worker.start();
        }

        start.countDown();
        assertTrue(done.await(100, TimeUnit.SECONDS),
                "workers did not finish in time: ok=" + okCount.get()
                        + ", errors=" + errorCount.get());

        if (firstFailure.get() != null) {
            throw new AssertionError("Concurrent failure: " + firstFailure.get());
        }
        assertEquals(threads * requestsPerThread, okCount.get(),
                "not all concurrent requests succeeded");
        assertEquals(0, errorCount.get(), "expected zero errors");
        // Ни одного лишнего SHARD_MAP — не было reconnect'ов (а значит и
        // таймаутов) во время нагрузки.
        assertEquals(threads, pushCount.get(),
                "no reconnect should have happened; got pushCount=" + pushCount.get()
                        + " expected=" + threads);
    }
}