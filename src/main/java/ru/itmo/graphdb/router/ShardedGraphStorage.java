package ru.itmo.graphdb.router;

import ru.itmo.graphdb.client.GraphDBClient;
import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.Node;
import ru.itmo.graphdb.sharding.ShardRouter;
import ru.itmo.graphdb.storage.GraphStorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Клиентское представление распределённого графа на стороне Router.
 *
 * <p>Реализует {@link GraphStorage} поверх набора шардов, чтобы
 * {@link ru.itmo.graphdb.routing.SimpleDijkstraPathFinder} работал без изменений:
 * ему нужны только {@link #getNode(String)} и {@link #getEdgesFrom(Node)},
 * а маршрутизация по шардам скрыта здесь.
 *
 * <p><b>Модель данных.</b> Узел {@code X} живёт на {@code shard[h(X)]}.
 * Ребро {@code A→B} живёт на {@code shard[h(A)]} — рядом со своим source.
 * Значит {@code getEdgesFrom(node)} — всегда ровно один сетевой вызов на
 * шард {@code h(node.id)}, без fan-out.
 *
 * <p><b>Заглушки на destination.</b> Узел-назначение ребра, который лежит
 * на другом шарде, представлен локально «лёгким» {@link Node} (только id).
 * Dijkstra использует от destination только {@link Node#getId()}, а
 * {@link #getEdgesFrom(Node)} маршрутизирует по id — этого достаточно, а
 * лишних round-trip на каждое ребро не возникает.
 *
 * <p><b>Мутирующие операции.</b> Не поддерживаются: запись идёт только
 * через {@link RouterCommandTranslator}. Все {@code UnsupportedOperationException}
 * для {@code addNode}/{@code addEdge}/{@code deleteNode}/... — намеренно.
 * Если клиенту нужен {@code getAllNodes}, это добавляется отдельной командой
 * на Router — сам {@code ShardedGraphStorage} для Dijkstra не обязан уметь
 * перебирать все вершины.
 */
public class ShardedGraphStorage implements GraphStorage {

    private final ShardRouter router;
    private final Map<Integer, GraphDBClient> clientsByShardId;

    public ShardedGraphStorage(ShardRouter router, Map<Integer, GraphDBClient> clientsByShardId) {
        this.router = Objects.requireNonNull(router, "router");
        this.clientsByShardId = Objects.requireNonNull(clientsByShardId, "clientsByShardId");
        if (clientsByShardId.size() != router.shardCount()) {
            throw new IllegalArgumentException(
                    "shardClients.size()=" + clientsByShardId.size()
                            + " does not match shardCount=" + router.shardCount());
        }
    }

    // ---------- чтение: реализовано ----------

    @Override
    public Node getNode(String id) {
        Objects.requireNonNull(id, "id");
        String resp = clientForNode(id).execute("_GET_NODE " + id);
        if (resp.startsWith("ERROR:")) {
            throw new RuntimeException("shard error on getNode(" + id + "): " + resp);
        }
        if ("NOT_FOUND".equals(resp)) {
            return null;
        }
        return parseNodeLine(resp);
    }

    @Override
    public Collection<Edge> getEdgesFrom(Node node) {
        Objects.requireNonNull(node, "node");
        String resp = clientForNode(node.getId()).execute("_GET_EDGES_FROM " + node.getId());
        if (resp.startsWith("ERROR:")) {
            throw new RuntimeException("shard error on getEdgesFrom(" + node.getId() + "): " + resp);
        }
        if ("NOT_FOUND".equals(resp) || "NO_EDGES".equals(resp)) {
            return Collections.emptyList();
        }
        return parseEdgeLines(resp, node);
    }

    @Override
    public Collection<Edge> getEdgesBetween(String sourceId, String destinationId) {
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(destinationId, "destinationId");
        Node source = getNode(sourceId);
        if (source == null) {
            return Collections.emptyList();
        }
        return getEdgesFrom(source).stream()
                .filter(e -> e.getDestination().getId().equals(destinationId))
                .collect(Collectors.toList());
    }

    // ---------- запись: только через RouterCommandTranslator ----------

    @Override
    public void addNode(Node node) {
        throw unsupported("addNode");
    }

    @Override
    public void addEdge(Edge edge) {
        throw unsupported("addEdge");
    }

    @Override
    public boolean deleteNode(String id) {
        throw unsupported("deleteNode");
    }

    @Override
    public boolean deleteEdge(String id) {
        throw unsupported("deleteEdge");
    }

    @Override
    public Edge getEdge(String id) {
        throw unsupported("getEdge");
    }

    @Override
    public Collection<Node> getAllNodes() {
        throw unsupported("getAllNodes");
    }

    @Override
    public Collection<Edge> getSortedEdgesFrom(Node node, ru.itmo.graphdb.model.DefaultMetric metric) {
        throw unsupported("getSortedEdgesFrom");
    }

    @Override
    public void clear() {
        throw unsupported("clear");
    }

    @Override
    public boolean saveToFile(String filePath) {
        throw unsupported("saveToFile");
    }

    @Override
    public boolean loadFromFile(String filePath) {
        throw unsupported("loadFromFile");
    }

    // ---------- helpers ----------

    private GraphDBClient clientForNode(String nodeId) {
        return clientsByShardId.get(router.shardForNode(nodeId));
    }

    private static UnsupportedOperationException unsupported(String op) {
        return new UnsupportedOperationException(
                "ShardedGraphStorage." + op
                        + " is not supported: mutations go through RouterCommandTranslator");
    }

    /** Разбор ответа {@code "NODE\t<id>\t<attrs>"}; attrs — {@code "k=v;k=v"} или пусто. */
    private static Node parseNodeLine(String line) {
        String[] parts = line.split("\t", -1);
        if (parts.length < 2 || !"NODE".equals(parts[0])) {
            throw new IllegalStateException("Malformed NODE line from shard: " + line);
        }
        Node node = new Node(parts[1]);
        if (parts.length > 2 && !parts[2].isEmpty()) {
            for (String token : parts[2].split(";")) {
                if (token.isEmpty()) {
                    continue;
                }
                int eq = token.indexOf('=');
                if (eq <= 0) {
                    continue;
                }
                node.setAttribute(token.substring(0, eq), token.substring(eq + 1));
            }
        }
        return node;
    }

    /**
     * Разбор многострочного ответа {@code _GET_EDGES_FROM}: каждая строка —
     * {@code "EDGE\t<id>\t<src>\t<dst>\t<k=v;k=v>"}. Источник — {@code source}
     * из аргумента (мы его уже знаем и он гарантированно на этом шарде);
     * destination — лёгкая заглушка, дедуплицируется по id внутри вызова.
     */
    private static Collection<Edge> parseEdgeLines(String multiline, Node source) {
        List<Edge> result = new ArrayList<>();
        Map<String, Node> destinationStubs = new HashMap<>();
        for (String line : multiline.split("\n")) {
            if (line.isEmpty()) {
                continue;
            }
            String[] parts = line.split("\t", -1);
            if (parts.length < 5 || !"EDGE".equals(parts[0])) {
                throw new IllegalStateException("Malformed EDGE line from shard: " + line);
            }
            String edgeId = parts[1];
            String dstId = parts[3];
            Node dst = destinationStubs.computeIfAbsent(dstId, Node::new);
            Map<String, Double> weights = parseWeights(parts[4]);
            result.add(new Edge(edgeId, source, dst, weights));
        }
        return result;
    }

    private static Map<String, Double> parseWeights(String text) {
        Map<String, Double> weights = new LinkedHashMap<>();
        if (text.isEmpty()) {
            return weights;
        }
        for (String token : text.split(";")) {
            if (token.isEmpty()) {
                continue;
            }
            int eq = token.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            weights.put(token.substring(0, eq), Double.parseDouble(token.substring(eq + 1)));
        }
        return weights;
    }
}