package ru.itmo.graphdb.router;

import ru.itmo.graphdb.client.CommandExecutor;
import ru.itmo.graphdb.client.GraphDBClient;
import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.Metric;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.routing.PathFinder;
import ru.itmo.graphdb.sharding.ShardEndpoint;
import ru.itmo.graphdb.sharding.ShardRouter;
import ru.itmo.graphdb.storage.GraphStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Маршрутизатор клиентских команд по шардам.
 *
 * <p>Один экземпляр на клиентское соединение (как и {@code CommandTranslator}):
 * хранит {@code currentDbName}, чтобы состояние сессии не протекало между
 * клиентами. Общие компоненты — {@link ShardRouter}, карта
 * {@code shardId → GraphDBClient}, {@link ShardedGraphStorage},
 * {@link MetricRegistry}, {@link PathFinder} — приходят снаружи и шарятся
 * (см. {@link RouterServer}).
 *
 * <p><b>Таблица маршрутизации.</b>
 * <table>
 *   <caption>Куда уходит каждая команда</caption>
 *   <tr><th>Команда</th><th>Маршрут</th></tr>
 *   <tr><td>ADD_NODE id</td><td>shard[h(id)] : ADD_NODE id</td></tr>
 *   <tr><td>SET_PARAMETER id k v</td><td>shard[h(id)] : SET_PARAMETER …</td></tr>
 *   <tr><td>DELETE_NODE id</td>
 *       <td>_DELETE_EDGES_TO на всех шардах (снимает заглушки и локальные
 *           инцидентные рёбра, а на домашнем — сам узел)</td></tr>
 *   <tr><td>ADD_EDGE src dst m v</td>
 *       <td>shard[h(src)] : _ADD_EDGE_RAW … (dst может быть чужим)</td></tr>
 *   <tr><td>PRINT_EDGES id / src dst</td><td>shard[h(id)] / shard[h(src)]</td></tr>
 *   <tr><td>DELETE_EDGE src-&gt;dst</td><td>shard[h(src)] : DELETE_EDGE …</td></tr>
 *   <tr><td>PATH_EDGE src dst m</td>
 *       <td>локально через {@link ShardedGraphStorage}</td></tr>
 *   <tr><td>ADD_METRIC / DELETE_METRIC</td>
 *       <td>локальный {@link MetricRegistry} + fan-out на все шарды</td></tr>
 *   <tr><td>CREATE_DB name</td><td>fan-out {@code _CLEAR} на все шарды</td></tr>
 *   <tr><td>SAVE_DB / READ_DB name</td>
 *       <td>fan-out с суффиксом {@code ".<name>.shard<id>"}</td></tr>
 * </table>
 *
 * <p>Контракт {@link CommandExecutor} соблюдён: ошибки упаковываются в
 * строку {@code "ERROR: ..."}, исключения наружу не выходят.
 */
public class RouterCommandTranslator implements CommandExecutor {

    private final ShardRouter router;
    private final Map<Integer, GraphDBClient> shardClients;
    private final GraphStorage shardedStorage;
    private final MetricRegistry metrics;
    private final PathFinder pathFinder;

    /** Имя «текущей» БД в сессии. Совпадает по смыслу с CommandTranslator. */
    private String currentDbName = "default";

    public RouterCommandTranslator(ShardRouter router,
                                   Map<Integer, GraphDBClient> shardClients,
                                   GraphStorage shardedStorage,
                                   MetricRegistry metrics,
                                   PathFinder pathFinder) {
        this.router = Objects.requireNonNull(router, "router");
        this.shardClients = Objects.requireNonNull(shardClients, "shardClients");
        this.shardedStorage = Objects.requireNonNull(shardedStorage, "shardedStorage");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.pathFinder = Objects.requireNonNull(pathFinder, "pathFinder");
    }

    @Override
    public String execute(String line) {
        if (line == null || line.isBlank()) {
            return "ERROR: empty command";
        }
        String[] parts = line.trim().split("\\s+");
        String command = parts[0].toUpperCase(Locale.ROOT);
        try {
            return switch (command) {
                case "CREATE_DB"     -> createDb(parts);
                case "READ_DB"       -> readDb(parts);
                case "SAVE_DB"       -> saveDb(parts);

                case "ADD_METRIC"    -> addMetric(parts);
                case "DELETE_METRIC" -> deleteMetric(parts);
                case "PRINT_METRICS" -> printMetrics();

                case "ADD_NODE"      -> addNode(parts);
                case "SET_PARAMETER" -> setParameter(parts);
                case "DELETE_NODE"   -> deleteNode(parts);

                case "ADD_EDGE"      -> addEdge(parts);
                case "PRINT_EDGES"   -> printEdges(parts);
                case "DELETE_EDGE"   -> deleteEdge(parts);

                case "PATH_EDGE"     -> pathEdge(parts);

                default -> "ERROR: unknown command: " + command;
            };
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    // ---------- DB ----------

    private String createDb(String[] parts) {
        requireArgs(parts, 2);
        String name = parts[1];
        List<String> failures = new ArrayList<>();
        for (ShardEndpoint ep : router.allShards()) {
            String resp = clientOf(ep).execute("_CLEAR");
            if (resp.startsWith("ERROR:")) {
                failures.add("shard " + ep.shardId() + ": " + resp);
            }
        }
        if (!failures.isEmpty()) {
            return "ERROR: cannot clear all shards: " + String.join("; ", failures);
        }
        currentDbName = name;
        return "OK: database '" + name + "' created";
    }

    private String readDb(String[] parts) {
        requireArgs(parts, 2);
        String name = parts[1];
        List<String> failures = new ArrayList<>();
        for (ShardEndpoint ep : router.allShards()) {
            String shardFile = name + ".shard" + ep.shardId();
            String resp = clientOf(ep).execute("READ_DB " + shardFile);
            if (resp.startsWith("ERROR:")) {
                failures.add("shard " + ep.shardId() + ": " + resp);
            }
        }
        if (!failures.isEmpty()) {
            return "ERROR: cannot load database '" + name + "': "
                    + String.join("; ", failures);
        }
        currentDbName = name;
        return "OK: database '" + name + "' loaded";
    }

    private String saveDb(String[] parts) {
        requireArgs(parts, 2);
        String name = parts[1];
        List<String> failures = new ArrayList<>();
        for (ShardEndpoint ep : router.allShards()) {
            String shardFile = name + ".shard" + ep.shardId();
            String resp = clientOf(ep).execute("SAVE_DB " + shardFile);
            if (resp.startsWith("ERROR:")) {
                failures.add("shard " + ep.shardId() + ": " + resp);
            }
        }
        if (!failures.isEmpty()) {
            return "ERROR: cannot save database '" + name + "': "
                    + String.join("; ", failures);
        }
        return "OK: database saved to '" + name + "' (per-shard files)";
    }

    // ---------- Metrics ----------

    private String addMetric(String[] parts) {
        requireArgs(parts, 2);
        String key = parts[1];
        String displayName = parts.length > 2 ? parts[2] : key;

        List<String> failures = new ArrayList<>();
        for (ShardEndpoint ep : router.allShards()) {
            String resp = clientOf(ep).execute("ADD_METRIC " + key + " " + displayName);
            // «already exists» — идемпотентно, не считаем ошибкой.
            if (resp.startsWith("ERROR:") && !resp.contains("already exists")) {
                failures.add("shard " + ep.shardId() + ": " + resp);
            }
        }
        if (!failures.isEmpty()) {
            return "ERROR: metric '" + key + "' failed on shard(s): "
                    + String.join("; ", failures);
        }
        boolean added = metrics.addCustom(key, displayName);
        return "OK: metric '" + key + "' " + (added ? "added" : "already present");
    }

    private String deleteMetric(String[] parts) {
        requireArgs(parts, 2);
        String key = parts[1];
        if (!metrics.removeCustom(key)) {
            return "ERROR: cannot delete metric '" + key + "' (not custom or unknown)";
        }
        for (ShardEndpoint ep : router.allShards()) {
            clientOf(ep).execute("DELETE_METRIC " + key);
        }
        return "OK: metric '" + key + "' deleted";
    }

    private String printMetrics() {
        List<Metric> all = metrics.all();
        if (all.isEmpty()) {
            return "(no metrics)";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < all.size(); i++) {
            if (i > 0) {
                sb.append('\n');
            }
            Metric m = all.get(i);
            sb.append(m.key()).append(" (").append(m.displayName()).append(')');
        }
        return sb.toString();
    }

    // ---------- Nodes ----------

    private String addNode(String[] parts) {
        requireArgs(parts, 2);
        String id = parts[1];
        return routeToShardOf(id, "ADD_NODE " + id);
    }

    private String setParameter(String[] parts) {
        requireArgs(parts, 4);
        String id = parts[1];
        String line = "SET_PARAMETER " + id + " " + parts[2] + " " + parts[3];
        return routeToShardOf(id, line);
    }

    /**
     * Удаление узла: {@code _DELETE_EDGES_TO id} на всех шардах.
     * На домашнем шарде {@code InMemoryGraphStorage.deleteNode} снимает сам
     * узел и все его рёбра. На прочих шардах — заглушки (если были) и
     * случайно залетевшие входящие рёбра. Инвариант «исходящие рёбра живут
     * рядом с source» гарантирует, что больше нигде реальных данных о
     * {@code id} не осталось.
     */
    private String deleteNode(String[] parts) {
        requireArgs(parts, 2);
        String id = parts[1];
        List<String> failures = new ArrayList<>();
        for (ShardEndpoint ep : router.allShards()) {
            String resp = clientOf(ep).execute("_DELETE_EDGES_TO " + id);
            if (resp.startsWith("ERROR:")) {
                failures.add("shard " + ep.shardId() + ": " + resp);
            }
        }
        if (!failures.isEmpty()) {
            return "ERROR: partial delete: " + String.join("; ", failures);
        }
        return "OK: node '" + id + "' deleted";
    }

    // ---------- Edges ----------

    private String addEdge(String[] parts) {
        requireArgs(parts, 5);
        String srcId = parts[1];
        String dstId = parts[2];
        String metricKey = parts[3];
        String value = parts[4];

        // Проверяем метрику здесь — Router авторитетен по реестру метрик.
        if (!metrics.contains(metricKey)) {
            return "ERROR: metric not found: " + metricKey;
        }

        int shardId = router.shardForNode(srcId);
        String line = "_ADD_EDGE_RAW " + srcId + " " + dstId + " " + metricKey + " " + value;
        return shardClients.get(shardId).execute(line);
    }

    private String printEdges(String[] parts) {
        requireArgs(parts, 2);
        if (parts.length == 2) {
            return routeToShardOf(parts[1], "PRINT_EDGES " + parts[1]);
        }
        if (parts.length == 3) {
            String srcId = parts[1];
            int shardId = router.shardForNode(srcId);
            return shardClients.get(shardId).execute("PRINT_EDGES " + parts[1] + " " + parts[2]);
        }
        throw new IllegalArgumentException("PRINT_EDGES requires 1 or 2 node ids");
    }

    private String deleteEdge(String[] parts) {
        requireArgs(parts, 2);
        String edgeId = parts[1];
        int arrow = edgeId.indexOf("->");
        if (arrow <= 0) {
            return "ERROR: malformed edge id (expected 'src->dst'): " + edgeId;
        }
        String srcId = edgeId.substring(0, arrow);
        int shardId = router.shardForNode(srcId);
        return shardClients.get(shardId).execute("DELETE_EDGE " + edgeId);
    }

    // ---------- Routing ----------

    private String pathEdge(String[] parts) {
        requireArgs(parts, 4);
        String srcId = parts[1];
        String dstId = parts[2];
        String metricKey = parts[3];

        Metric metric = metrics.find(metricKey)
                .orElseThrow(() -> new IllegalArgumentException("metric not found: " + metricKey));

        List<Edge> path = pathFinder.findShortestPath(shardedStorage, srcId, dstId, metric);

        if (path.isEmpty()) {
            return "No path from '" + srcId + "' to '" + dstId + "' by " + metricKey;
        }

        StringBuilder sb = new StringBuilder("Path by ").append(metricKey).append(":\n");
        double total = 0.0;
        for (Edge edge : path) {
            double weight = edge.getWeight(metric);
            total += weight;
            sb.append(edge.getSource().getId())
                    .append(" -> ")
                    .append(edge.getDestination().getId())
                    .append(" [").append(edge.getId()).append("] ")
                    .append(metricKey).append("=").append(weight).append("\n");
        }
        sb.append("Total: ").append(total);
        return sb.toString();
    }

    // ---------- Helpers ----------

    private String routeToShardOf(String nodeId, String command) {
        int shardId = router.shardForNode(nodeId);
        return shardClients.get(shardId).execute(command);
    }

    private GraphDBClient clientOf(ShardEndpoint ep) {
        return shardClients.get(ep.shardId());
    }

    private void requireArgs(String[] parts, int count) {
        if (parts.length < count) {
            throw new IllegalArgumentException(
                    "Command " + parts[0] + " requires " + (count - 1) + " argument(s)");
        }
    }
}