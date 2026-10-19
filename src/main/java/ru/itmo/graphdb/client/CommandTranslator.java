package ru.itmo.graphdb.client;

import ru.itmo.graphdb.model.DefaultMetric;
import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.Metric;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.model.Node;
import ru.itmo.graphdb.routing.PathFinder;
import ru.itmo.graphdb.storage.GraphStorage;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Транслятор текстовых команд графовой СУБД.
 *
 * <p>Живёт в клиентском пакете, потому что именно клиент (AnyLogic или консоль)
 * формулирует команды по словарю. На Этапе 2 клиент и нода — один процесс,
 * поэтому транслятор дёргает {@link GraphStorage} напрямую. На Этапе 3
 * внутренности методов заменятся на сетевые вызовы, а сигнатура {@link #execute(String)}
 * останется неизменной.
 *
 * <p>Поддерживаемые команды — см. «Словарь команд графовой СУБД».
 */
public class CommandTranslator {

    private final GraphStorage storage;
    private final MetricRegistry metrics;
    private final PathFinder pathFinder;

    private String currentDbName = "default";

    public CommandTranslator(GraphStorage storage, MetricRegistry metrics, PathFinder pathFinder) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.pathFinder = Objects.requireNonNull(pathFinder, "pathFinder");
    }

    /**
     * Выполняет одну строку команды и возвращает человекочитаемый ответ.
     * Никогда не кидает исключение наружу - ошибки упаковываются в строку с префиксом ERROR.
     */
    public String execute(String line) {
        if (line == null || line.isBlank()) {
            return "ERROR: empty command";
        }

        String[] parts = line.trim().split("\\s+");
        String command = parts[0].toUpperCase(Locale.ROOT);

        try {
            switch (command) {
                case "CREATE_DB":    return createDb(parts);
                case "READ_DB":      return readDb(parts);
                case "SAVE_DB":      return saveDb(parts);

                case "ADD_METRIC":   return addMetric(parts);
                case "DELETE_METRIC":return deleteMetric(parts);
                case "PRINT_METRICS":return printMetrics();

                case "ADD_NODE":     return addNode(parts);
                case "SET_PARAMETER":return setParameter(parts);
                case "DELETE_NODE":  return deleteNode(parts);

                case "ADD_EDGE":     return addEdge(parts);
                case "PRINT_EDGES":  return printEdges(parts);
                case "DELETE_EDGE":  return deleteEdge(parts);

                case "PATH_EDGE":    return pathEdge(parts);

                default:
                    return "ERROR: unknown command: " + command;
            }
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    // ---------- DB ----------

    private String createDb(String[] parts) {
        requireArgs(parts, 2);
        storage.clear();
        currentDbName = parts[1];
        return "OK: database '" + currentDbName + "' created";
    }

    private String readDb(String[] parts) {
        requireArgs(parts, 2);
        boolean ok = storage.loadFromFile(parts[1]);
        if (ok) {
            currentDbName = parts[1];
        }
        return ok
                ? "OK: database '" + parts[1] + "' loaded"
                : "ERROR: cannot load database '" + parts[1] + "'";
    }

    private String saveDb(String[] parts) {
        requireArgs(parts, 2);
        boolean ok = storage.saveToFile(parts[1]);
        return ok
                ? "OK: database saved to '" + parts[1] + "'"
                : "ERROR: cannot save database to '" + parts[1] + "'";
    }

    // ---------- Metrics ----------

    private String addMetric(String[] parts) {
        requireArgs(parts, 2);
        String key = parts[1];
        String displayName = parts.length > 2 ? parts[2] : key;

        boolean ok = metrics.addCustom(key, displayName);
        return ok
                ? "OK: metric '" + key + "' added"
                : "ERROR: metric '" + key + "' already exists";
    }

    private String deleteMetric(String[] parts) {
        requireArgs(parts, 2);
        String key = parts[1];

        boolean ok = metrics.removeCustom(key);
        return ok
                ? "OK: metric '" + key + "' deleted"
                : "ERROR: cannot delete metric '" + key + "'";
    }

    private String printMetrics() {
        List<Metric> all = metrics.all();
        if (all.isEmpty()) {
            return "(no metrics)";
        }
        return all.stream()
                .map(m -> m.key() + " (" + m.displayName() + ")")
                .collect(Collectors.joining("\n"));
    }

    // ---------- Nodes ----------

    private String addNode(String[] parts) {
        requireArgs(parts, 2);
        String id = parts[1];
        storage.addNode(new Node(id));
        return "OK: node '" + id + "' added";
    }

    private String setParameter(String[] parts) {
        requireArgs(parts, 4);

        String nodeId = parts[1];
        String parameterName = parts[2];
        String parameterValue = parts[3];

        Node node = storage.getNode(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("node not found: " + nodeId);
        }
        node.setAttribute(parameterName, parameterValue);
        return "OK: parameter '" + parameterName + "' set for node '" + nodeId + "'";
    }

    private String deleteNode(String[] parts) {
        requireArgs(parts, 2);
        String id = parts[1];

        boolean ok = storage.deleteNode(id);
        return ok
                ? "OK: node '" + id + "' deleted"
                : "ERROR: node not found: " + id;
    }

    // ---------- Edges ----------

    private String addEdge(String[] parts) {
        requireArgs(parts, 5);

        String sourceId      = parts[1];
        String destinationId = parts[2];
        String metricKey     = parts[3];
        double value         = Double.parseDouble(parts[4]);

        Node source = storage.getNode(sourceId);
        if (source == null) {
            throw new IllegalArgumentException("source node not found: " + sourceId);
        }
        Node destination = storage.getNode(destinationId);
        if (destination == null) {
            throw new IllegalArgumentException("destination node not found: " + destinationId);
        }

        Metric metric = metrics.find(metricKey)
                .orElseThrow(() -> new IllegalArgumentException("metric not found: " + metricKey));

        // Одно ребро на пару source->destination: повторный ADD_EDGE добавляет метрику.
        String edgeId = sourceId + "->" + destinationId;
        Edge edge = storage.getEdge(edgeId);

        if (edge == null) {
            Map<String, Double> weights = new LinkedHashMap<>();
            weights.put(metric.key(), value);
            edge = new Edge(edgeId, source, destination, weights);
        } else {
            edge = edge.withWeight(metric, value);
        }

        storage.addEdge(edge);
        return "OK: edge '" + edgeId + "' updated: " + metricKey + "=" + value;
    }

    private String printEdges(String[] parts) {
        requireArgs(parts, 2);

        if (parts.length == 2) {
            Node node = storage.getNode(parts[1]);
            if (node == null) {
                throw new IllegalArgumentException("node not found: " + parts[1]);
            }
            return formatEdges(storage.getEdgesFrom(node));
        }
        if (parts.length == 3) {
            return formatEdges(storage.getEdgesBetween(parts[1], parts[2]));
        }
        throw new IllegalArgumentException("PRINT_EDGES requires 1 or 2 node ids");
    }

    private String deleteEdge(String[] parts) {
        requireArgs(parts, 2);
        String id = parts[1];

        boolean ok = storage.deleteEdge(id);
        return ok
                ? "OK: edge '" + id + "' deleted"
                : "ERROR: edge not found: " + id;
    }

    // ---------- Routing ----------

    private String pathEdge(String[] parts) {
        requireArgs(parts, 4);

        String sourceId      = parts[1];
        String destinationId = parts[2];
        String metricKey     = parts[3];

        Metric metric = metrics.find(metricKey)
                .orElseThrow(() -> new IllegalArgumentException("metric not found: " + metricKey));

        List<Edge> path = pathFinder.findShortestPath(storage, sourceId, destinationId, metric);

        if (path.isEmpty()) {
            return "No path from '" + sourceId + "' to '" + destinationId + "' by " + metricKey;
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

    private String formatEdges(Collection<Edge> edges) {
        if (edges.isEmpty()) {
            return "(no edges)";
        }
        return edges.stream()
                .map(e -> e.getId() + ": " + e.getSource().getId()
                        + " -> " + e.getDestination().getId() + " " + e.getWeights())
                .collect(Collectors.joining("\n"));
    }

    private void requireArgs(String[] parts, int count) {
        if (parts.length < count) {
            throw new IllegalArgumentException(
                    "Command " + parts[0] + " requires " + (count - 1) + " argument(s)"
            );
        }
    }
}