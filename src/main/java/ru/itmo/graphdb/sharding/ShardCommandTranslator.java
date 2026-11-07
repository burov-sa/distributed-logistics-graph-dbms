package ru.itmo.graphdb.sharding;

import ru.itmo.graphdb.client.CommandExecutor;
import ru.itmo.graphdb.client.CommandTranslator;
import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.Metric;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.model.Node;
import ru.itmo.graphdb.routing.PathFinder;
import ru.itmo.graphdb.storage.GraphStorage;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Исполнитель команд для шард-ноды.
 *
 * <p>Расширяет «словарь» обычного {@link CommandTranslator} набором
 * внутренних команд с префиксом {@code _}. Они не предназначены для
 * пользователя и вызываются только Router-слоем (шаг 3):
 *
 * <table>
 *   <caption>Внутренние команды шарда</caption>
 *   <tr><th>Команда</th><th>Назначение</th></tr>
 *   <tr><td>{@code _GET_NODE <id>}</td>
 *       <td>Вернуть {@code NODE\t<id>\t<attrs>} или {@code NOT_FOUND}</td></tr>
 *   <tr><td>{@code _GET_EDGES_FROM <id>}</td>
 *       <td>Список исходящих рёбер (по строке на ребро, формат save-файла);
 *           {@code NOT_FOUND} если узла нет; {@code NO_EDGES} если рёбер нет</td></tr>
 *   <tr><td>{@code _DELETE_EDGES_TO <id>}</td>
 *       <td>Удалить все входящие рёбра на {@code <id>} и локальную заглушку узла
 *           (при fan-out из {@code DELETE_NODE} на домашнем шарде узла)</td></tr>
 *   <tr><td>{@code _ADD_EDGE_RAW <src> <dst> <metric> <value>}</td>
 *       <td>Добавить ребро без требования, чтобы {@code <dst>} был реальным узлом
 *           этого шарда; при отсутствии создаёт локальную «заглушку» dst</td></tr>
 *   <tr><td>{@code _CLEAR}</td>
 *       <td>Очистить хранилище шарда</td></tr>
 * </table>
 *
 * <p>Все прочие команды делегируются обычному {@link CommandTranslator},
 * поэтому шард остаётся самодостаточной нодой: если клиент (по ошибке) обратится
 * к шарду напрямую обычной командой, она отработает в пределах этого шарда.
 *
 * <p><b>Заглушки (foreign stubs).</b> Ребро живёт на шарде своего source.
 * Когда source и destination лежат на разных шардах, source-шарду нужна
 * ссылка на dst, чтобы удовлетворить инвариант {@code InMemoryGraphStorage.addEdge}.
 * Создаётся {@link Node} c атрибутом {@code __foreign__=true}. Такие заглушки
 * удаляются при {@code _DELETE_EDGES_TO}, а также невидимы клиенту (Router
 * маршрутизирует {@code getNode} по хэшу id — на домашний шард).
 */
public class ShardCommandTranslator implements CommandExecutor {

    /** Ключ атрибута-маркера, помечающего «заглушку» (foreign reference). */
    public static final String FOREIGN_ATTR = "__foreign__";
    public static final String FOREIGN_TRUE = "true";

    private final CommandTranslator delegate;
    private final GraphStorage storage;
    private final MetricRegistry metrics;
    private final int shardId;

    public ShardCommandTranslator(GraphStorage storage,
                                  MetricRegistry metrics,
                                  PathFinder pathFinder) {
        this(storage, metrics, pathFinder, -1);
    }

    /**
     * @param shardId логический id шарда; {@code -1} — «не шард» (standalone-нода,
     *                удобно для отладки `_`-команд)
     */
    public ShardCommandTranslator(GraphStorage storage,
                                  MetricRegistry metrics,
                                  PathFinder pathFinder,
                                  int shardId) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.delegate = new CommandTranslator(storage, metrics, pathFinder);
        this.shardId = shardId;
    }

    public int getShardId() {
        return shardId;
    }

    @Override
    public String execute(String line) {
        if (line == null || line.isBlank()) {
            return "ERROR: empty command";
        }
        try {
            String trimmed = line.trim();
            if (trimmed.startsWith("_")) {
                return executeInternal(trimmed);
            }
            return delegate.execute(line);
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    // ---------- dispatch internal ----------

    private String executeInternal(String trimmed) {
        String[] parts = trimmed.split("\\s+");
        String cmd = parts[0]; // case-sensitive: внутренние команды — наш контракт
        return switch (cmd) {
            case "_GET_NODE"          -> internalGetNode(parts);
            case "_GET_EDGES_FROM"    -> internalGetEdgesFrom(parts);
            case "_DELETE_EDGES_TO"   -> internalDeleteEdgesTo(parts);
            case "_ADD_EDGE_RAW"      -> internalAddEdgeRaw(parts);
            case "_CLEAR"             -> internalClear();
            default -> "ERROR: unknown internal command: " + cmd;
        };
    }

    // ---------- internal commands ----------

    private String internalGetNode(String[] parts) {
        requireArgs(parts, 2);
        String id = parts[1];
        Node node = storage.getNode(id);
        if (node == null) {
            return "NOT_FOUND";
        }
        return "NODE\t" + node.getId() + "\t" + attributesToString(node.getAttributes());
    }

    private String internalGetEdgesFrom(String[] parts) {
        requireArgs(parts, 2);
        String id = parts[1];
        Node node = storage.getNode(id);
        if (node == null) {
            return "NOT_FOUND";
        }
        Collection<Edge> edges = storage.getEdgesFrom(node);
        if (edges.isEmpty()) {
            return "NO_EDGES";
        }
        return edges.stream()
                .map(this::edgeToTextLine)
                .collect(Collectors.joining("\n"));
    }

    private String internalDeleteEdgesTo(String[] parts) {
        requireArgs(parts, 2);
        String id = parts[1];

        // Если на этом шарде есть заглушка узла id, deleteNode сам снимет
        // все входящие рёбра (см. InMemoryGraphStorage.deleteNode).
        Node existing = storage.getNode(id);
        if (existing != null) {
            storage.deleteNode(id);
            return "OK: incoming edges to '" + id + "' dropped (local stub removed)";
        }

        // Заглушки нет — но, защищаясь от нарушения инварианта,
        // всё равно вычищаем возможные висячие рёбра.
        int deleted = 0;
        for (Node node : storage.getAllNodes()) {
            for (Edge edge : storage.getEdgesFrom(node)) {
                if (edge.getDestination().getId().equals(id)) {
                    storage.deleteEdge(edge.getId());
                    deleted++;
                }
            }
        }
        return "OK: " + deleted + " incoming edge(s) to '" + id + "' deleted";
    }

    private String internalAddEdgeRaw(String[] parts) {
        requireArgs(parts, 5);
        String srcId = parts[1];
        String dstId = parts[2];
        String metricKey = parts[3];
        double value = Double.parseDouble(parts[4]);

        Node src = storage.getNode(srcId);
        if (src == null) {
            throw new IllegalArgumentException("source node not found: " + srcId);
        }
        if (FOREIGN_TRUE.equals(String.valueOf(src.getAttribute(FOREIGN_ATTR)))) {
            throw new IllegalArgumentException(
                    "source node is a foreign stub on this shard: " + srcId);
        }

        Metric metric = metrics.find(metricKey)
                .orElseThrow(() -> new IllegalArgumentException("metric not found: " + metricKey));

        Node dst = storage.getNode(dstId);
        if (dst == null) {
            // Cross-shard: создаём локальную заглушку (foreign stub).
            // Ребро будет корректно «указывать» на неё; при маршрутизации через
            // Router getNode(dstId) всё равно уйдёт на домашний шард dstId.
            dst = new Node(dstId);
            dst.setAttribute(FOREIGN_ATTR, FOREIGN_TRUE);
            storage.addNode(dst);
        }

        String edgeId = srcId + "->" + dstId;
        Edge existing = storage.getEdge(edgeId);
        Edge edge;
        if (existing == null) {
            Map<String, Double> weights = new LinkedHashMap<>();
            weights.put(metric.key(), value);
            edge = new Edge(edgeId, src, dst, weights);
        } else {
            edge = existing.withWeight(metric, value);
        }
        storage.addEdge(edge);
        return "OK: edge '" + edgeId + "' updated: " + metricKey + "=" + value;
    }

    private String internalClear() {
        storage.clear();
        return "OK: shard cleared";
    }

    // ---------- helpers ----------

    /** Тот же формат, что в save-файле: {@code EDGE\t<id>\t<src>\t<dst>\t<k=v;...>}. */
    private String edgeToTextLine(Edge edge) {
        return "EDGE\t"
                + edge.getId() + "\t"
                + edge.getSource().getId() + "\t"
                + edge.getDestination().getId() + "\t"
                + weightsToString(edge.getWeights());
    }

    private static String attributesToString(Map<String, Object> attributes) {
        return attributes.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(";"));
    }

    private static String weightsToString(Map<String, Double> weights) {
        return weights.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(";"));
    }

    private void requireArgs(String[] parts, int count) {
        if (parts.length < count) {
            throw new IllegalArgumentException(
                    "Command " + parts[0] + " requires " + (count - 1) + " argument(s)");
        }
    }
}