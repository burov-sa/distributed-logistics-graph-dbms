package ru.itmo.graphdb.router;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import ru.itmo.graphdb.client.GraphDBClient;
import ru.itmo.graphdb.model.DefaultMetric;
import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.model.Node;
import ru.itmo.graphdb.network.DBServer;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.sharding.ShardCommandTranslator;
import ru.itmo.graphdb.sharding.ShardEndpoint;
import ru.itmo.graphdb.sharding.ShardRouter;
import ru.itmo.graphdb.storage.GraphStorage;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Интеграционные тесты маршрутизации через границы шардов.
 *
 * <p>Основной сценарий — «двумерная» сетка 4×4, распределённая по 3 шардам.
 * Результаты PATH_EDGE через Router сверяются с эталоном, посчитанным
 * локально над {@link InMemoryGraphStorage} тем же алгоритмом
 * {@link SimpleDijkstraPathFinder}. Это доказывает, что шардирование
 * не влияет на семантику {@code PATH_EDGE}.
 *
 * <p>Дополнительно:
 * <ul>
 *   <li>неравномерные веса рёбер — тоже сверяем с эталоном;</li>
 *   <li>форсированный зигзаг через все три шарда — проверяем, что
 *       кросс-шардовый путь реально строится и не «залипает» на одном шарде;</li>
 *   <li>«одна пара — два маршрута»: оптимальный по distance ≠ оптимальный
 *       по time — проверяем, что Router умеет каждый, а веса не путаются.</li>
 * </ul>
 */
class ShardedGridPathfindingIntegrationTest {

    private static final int SHARD_COUNT = 3;
    private static final int GRID_SIZE = 4;

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

    private static String nodeId(int i, int j) {
        return "n" + i + "_" + j;
    }

    private static String id(int i, int j) {
        return nodeId(i, j);
    }

    /**
     * Возвращает {@code count} различных id, каждый из которых по текущей
     * раскладке {@link ShardRouter} лежит на шарде {@code shardId}.
     *
     * <p>Раньше helper назывался {@code findIdOnShard(shardId, startFrom)} и
     * возвращал первый попавшийся id ≥ startFrom — что при двух вызовах
     * подряд с {@code startFrom=0, 1} могло вернуть один и тот же id.
     * Теперь контракт другой: «дай мне N разных id на этом шарде» — это
     * ровно то, что нужно тестам.
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

    /**
     * Строит двумерную сетку {@code GRID_SIZE × GRID_SIZE} в шардированном
     * кластере через Router: узлы {@code nI_J}, рёбра «вправо» и «вниз».
     * Возвращает ту же сетку, собранную локально в {@link InMemoryGraphStorage}
     * — эталон для сверки.
     */
    private GraphStorage buildGrid(Map<String, Map<DefaultMetric, Double>> weightsByEdge) {
        GraphStorage reference = new InMemoryGraphStorage();

        for (int i = 0; i < GRID_SIZE; i++) {
            for (int j = 0; j < GRID_SIZE; j++) {
                String id = nodeId(i, j);
                assertEquals("OK: node '" + id + "' added",
                        client.execute("ADD_NODE " + id));
                reference.addNode(new Node(id));
            }
        }

        // Рёбра «вправо» (i, j) → (i, j+1)
        for (int i = 0; i < GRID_SIZE; i++) {
            for (int j = 0; j < GRID_SIZE - 1; j++) {
                addEdge(id(i, j), id(i, j + 1), weightsByEdge, reference);
            }
        }
        // Рёбра «вниз» (i, j) → (i+1, j)
        for (int i = 0; i < GRID_SIZE - 1; i++) {
            for (int j = 0; j < GRID_SIZE; j++) {
                addEdge(id(i, j), id(i + 1, j), weightsByEdge, reference);
            }
        }
        return reference;
    }

    private void addEdge(String src, String dst,
                         Map<String, Map<DefaultMetric, Double>> weightsByEdge,
                         GraphStorage reference) {
        String edgeKey = src + "->" + dst;
        Map<DefaultMetric, Double> weights = weightsByEdge.get(edgeKey);
        assertNotNull(weights, "test missing weights for edge: " + edgeKey);

        // Через Router: сначала distance, потом time, потом cost (если заданы).
        if (weights.containsKey(DefaultMetric.DISTANCE)) {
            String resp = client.execute(
                    "ADD_EDGE " + src + " " + dst + " distance "
                            + weights.get(DefaultMetric.DISTANCE));
            assertTrue(resp.startsWith("OK"), "ADD_EDGE distance failed: " + resp);
        }
        if (weights.containsKey(DefaultMetric.TIME)) {
            String resp = client.execute(
                    "ADD_EDGE " + src + " " + dst + " time "
                            + weights.get(DefaultMetric.TIME));
            assertTrue(resp.startsWith("OK"), "ADD_EDGE time failed: " + resp);
        }
        if (weights.containsKey(DefaultMetric.COST)) {
            String resp = client.execute(
                    "ADD_EDGE " + src + " " + dst + " cost "
                            + weights.get(DefaultMetric.COST));
            assertTrue(resp.startsWith("OK"), "ADD_EDGE cost failed: " + resp);
        }

        // В эталон — единым Edge со всеми заданными метриками.
        Node s = reference.getNode(src);
        Node d = reference.getNode(dst);
        Map<String, Double> rawWeights = new LinkedHashMap<>();
        weights.forEach((m, v) -> rawWeights.put(m.key(), v));
        reference.addEdge(new Edge(edgeKey, s, d, rawWeights));
    }

    /**
     * Сравнение шардированного {@code PATH_EDGE} с локальным эталоном.
     * Проверяем суммарный вес (а не конкретный путь — кратчайших путей
     * может быть несколько, и Dijkstra не обязан давать один и тот же).
     */
    private void assertPathEqualsReference(GraphStorage reference,
                                           String src, String dst,
                                           DefaultMetric metric) {
        double expected = totalOf(reference, src, dst, metric);
        String shardedResp = client.execute(
                "PATH_EDGE " + src + " " + dst + " " + metric.key());

        if (Double.isNaN(expected)) {
            assertTrue(shardedResp.startsWith("No path"),
                    "expected No path for " + src + "→" + dst + " by " + metric.key()
                            + ", actual: " + shardedResp);
            return;
        }

        double actual = extractTotal(shardedResp);
        assertEquals(expected, actual, 1e-9,
                "Weight mismatch for " + src + "→" + dst + " by " + metric.key()
                        + ": reference=" + expected + ", sharded response:\n" + shardedResp);
    }

    /** {@link Double#NaN} — нет пути. */
    private static double totalOf(GraphStorage storage, String src, String dst,
                                  DefaultMetric metric) {
        List<Edge> path = new SimpleDijkstraPathFinder()
                .findShortestPath(storage, src, dst, metric);
        if (path.isEmpty()) {
            return Double.NaN;
        }
        double sum = 0.0;
        for (Edge e : path) {
            sum += e.getWeight(metric);
        }
        return sum;
    }

    private static final Pattern TOTAL_PATTERN =
            Pattern.compile("Total:\\s+(-?\\d+(?:\\.\\d+)?(?:[eE][-+]?\\d+)?)\\s*$");

    private static double extractTotal(String response) {
        Matcher m = TOTAL_PATTERN.matcher(response);
        if (!m.find()) {
            throw new AssertionError("Cannot find 'Total:' in response:\n" + response);
        }
        return Double.parseDouble(m.group(1));
    }

    // ---------- 1. равномерные веса ----------

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void gridUniformAllPairsMatchReference() {
        Map<String, Map<DefaultMetric, Double>> weights = new HashMap<>();
        for (int i = 0; i < GRID_SIZE; i++) {
            for (int j = 0; j < GRID_SIZE; j++) {
                if (j + 1 < GRID_SIZE) {
                    weights.put(id(i, j) + "->" + id(i, j + 1),
                            Map.of(DefaultMetric.DISTANCE, 1.0));
                }
                if (i + 1 < GRID_SIZE) {
                    weights.put(id(i, j) + "->" + id(i + 1, j),
                            Map.of(DefaultMetric.DISTANCE, 1.0));
                }
            }
        }
        GraphStorage reference = buildGrid(weights);

        // Все пары (s != d), достижимые в направленном DAG «вправо/вниз».
        for (int si = 0; si < GRID_SIZE; si++) {
            for (int sj = 0; sj < GRID_SIZE; sj++) {
                for (int di = 0; di < GRID_SIZE; di++) {
                    for (int dj = 0; dj < GRID_SIZE; dj++) {
                        String src = id(si, sj);
                        String dst = id(di, dj);
                        if (src.equals(dst)) {
                            continue;
                        }
                        assertPathEqualsReference(reference, src, dst, DefaultMetric.DISTANCE);
                    }
                }
            }
        }
    }

    // ---------- 2. неравномерные веса ----------

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void gridNonUniformAllPairsMatchReference() {
        Map<String, Map<DefaultMetric, Double>> weights = new HashMap<>();
        // Псевдослучайные, но детерминированные веса — чтобы тест был
        // воспроизводим, а оптимальный путь не сводился к манхэттенскому.
        for (int i = 0; i < GRID_SIZE; i++) {
            for (int j = 0; j < GRID_SIZE; j++) {
                if (j + 1 < GRID_SIZE) {
                    double w = 1.0 + ((i * 7 + j * 13) % 9);
                    weights.put(id(i, j) + "->" + id(i, j + 1),
                            Map.of(DefaultMetric.DISTANCE, w));
                }
                if (i + 1 < GRID_SIZE) {
                    double w = 1.0 + ((i * 11 + j * 5) % 7);
                    weights.put(id(i, j) + "->" + id(i + 1, j),
                            Map.of(DefaultMetric.DISTANCE, w));
                }
            }
        }
        GraphStorage reference = buildGrid(weights);

        for (int si = 0; si < GRID_SIZE; si++) {
            for (int sj = 0; sj < GRID_SIZE; sj++) {
                for (int di = 0; di < GRID_SIZE; di++) {
                    for (int dj = 0; dj < GRID_SIZE; dj++) {
                        String src = id(si, sj);
                        String dst = id(di, dj);
                        if (src.equals(dst)) {
                            continue;
                        }
                        assertPathEqualsReference(reference, src, dst, DefaultMetric.DISTANCE);
                    }
                }
            }
        }
    }

    // ---------- 3. форсированный зигзаг через все три шарда ----------

    /**
     * Путь s0-src → s1 → s2 → s0-dst, каждое ребро пересекает границу шардов,
     * source и destination — разные узлы на шарде 0.
     *
     * <p>Раньше здесь использовались {@code findIdOnShard(0, 0)} и
     * {@code findIdOnShard(0, 1)}; при текущей хэш-раскладке оба вызова могли
     * вернуть один и тот же id. Теперь явно запрашиваем два РАЗНЫХ id на
     * шарде 0 через {@link #findDistinctIdsOnShard(int, int)}.
     */
    @Test
    void crossShardZigzagPathHitsAllShards() {
        List<String> onShard0 = findDistinctIdsOnShard(0, 2);
        String onShard1 = findDistinctIdsOnShard(1, 1).get(0);
        String onShard2 = findDistinctIdsOnShard(2, 1).get(0);

        String src = onShard0.get(0);   // шард 0
        String dst = onShard0.get(1);   // снова шард 0, гарантированно другой id

        for (String id : List.of(src, onShard1, onShard2, dst)) {
            assertTrue(client.execute("ADD_NODE " + id).startsWith("OK"));
        }
        assertTrue(client.execute(
                "ADD_EDGE " + src + " " + onShard1 + " distance 1").startsWith("OK"));
        assertTrue(client.execute(
                "ADD_EDGE " + onShard1 + " " + onShard2 + " distance 1").startsWith("OK"));
        assertTrue(client.execute(
                "ADD_EDGE " + onShard2 + " " + dst + " distance 1").startsWith("OK"));

        String resp = client.execute(
                "PATH_EDGE " + src + " " + dst + " distance");

        // Путь единственный: s0(src) → s1 → s2 → s0(dst).
        assertTrue(resp.contains(src + " -> " + onShard1),       "actual:\n" + resp);
        assertTrue(resp.contains(onShard1 + " -> " + onShard2),  "actual:\n" + resp);
        assertTrue(resp.contains(onShard2 + " -> " + dst),       "actual:\n" + resp);
        assertEquals(3.0, extractTotal(resp), 1e-9, "actual:\n" + resp);

        // Санити-чек: рёбра видны на «своих» шардах через PRINT_EDGES Router-а.
        String edgesFromSrc = client.execute("PRINT_EDGES " + src);
        assertTrue(edgesFromSrc.contains(src + " -> " + onShard1),
                "actual:\n" + edgesFromSrc);
    }

    // ---------- 4. оптимальный маршрут зависит от метрики ----------

    @Test
    void optimalRouteDependsOnMetric() {
        //     B
        //   1/ \100
        //   A    D
        //   3\  /1
        //     C
        //
        // A→B (dist=1, time=100), B→D (dist=1, time=100)  ⇒ dist=2, time=200
        // A→C (dist=3, time=1),   C→D (dist=3, time=1)    ⇒ dist=6, time=2
        String a = "md-a";
        String b = "md-b";
        String c = "md-c";
        String d = "md-d";

        for (String id : List.of(a, b, c, d)) {
            assertTrue(client.execute("ADD_NODE " + id).startsWith("OK"));
        }
        // distance и time прописываем в двух командах каждое ребро.
        client.execute("ADD_EDGE " + a + " " + b + " distance 1");
        client.execute("ADD_EDGE " + a + " " + b + " time 100");
        client.execute("ADD_EDGE " + b + " " + d + " distance 1");
        client.execute("ADD_EDGE " + b + " " + d + " time 100");
        client.execute("ADD_EDGE " + a + " " + c + " distance 3");
        client.execute("ADD_EDGE " + a + " " + c + " time 1");
        client.execute("ADD_EDGE " + c + " " + d + " distance 3");
        client.execute("ADD_EDGE " + c + " " + d + " time 1");

        String byDistance = client.execute("PATH_EDGE " + a + " " + d + " distance");
        assertTrue(byDistance.contains(a + " -> " + b), "actual:\n" + byDistance);
        assertTrue(byDistance.contains(b + " -> " + d), "actual:\n" + byDistance);
        assertEquals(2.0, extractTotal(byDistance), 1e-9, "actual:\n" + byDistance);

        String byTime = client.execute("PATH_EDGE " + a + " " + d + " time");
        assertTrue(byTime.contains(a + " -> " + c), "actual:\n" + byTime);
        assertTrue(byTime.contains(c + " -> " + d), "actual:\n" + byTime);
        assertEquals(2.0, extractTotal(byTime), 1e-9, "actual:\n" + byTime);
    }
}