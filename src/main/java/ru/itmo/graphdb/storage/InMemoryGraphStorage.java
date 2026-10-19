package ru.itmo.graphdb.storage;

import ru.itmo.graphdb.model.DefaultMetric;
import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.Node;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Реализация интерфейса GraphStorage для хранения данных
 * на одной ноде.
 * <p>
 *
 */

public class InMemoryGraphStorage implements GraphStorage {

    private final Map<String, Node> nodes = new LinkedHashMap<>();
    private final Map<String, Edge> edges = new LinkedHashMap<>();
    private final Map<String, List<Edge>> outgoing = new HashMap<>();

    @Override
    public synchronized void addNode(Node node) {
        Objects.requireNonNull(node, "node");
        if (nodes.containsKey(node.getId())) {
            throw new IllegalArgumentException("Node already exists: " + node.getId());
        }
        nodes.put(node.getId(), node);
        outgoing.put(node.getId(), new ArrayList<>());
    }

    @Override
    public synchronized void addEdge(Edge edge) {
        Objects.requireNonNull(edge, "edge");

        if (!nodes.containsKey(edge.getSource().getId())) {
            throw new IllegalArgumentException("Source node not found: " + edge.getSource().getId());
        }
        if (!nodes.containsKey(edge.getDestination().getId())) {
            throw new IllegalArgumentException("Destination node not found: " + edge.getDestination().getId());
        }

        Edge old = edges.put(edge.getId(), edge);
        if (old != null) {
            List<Edge> oldOut = outgoing.get(old.getSource().getId());
            if (oldOut != null) {
                oldOut.remove(old);
            }
        }

        outgoing.computeIfAbsent(edge.getSource().getId(), k -> new ArrayList<>()).add(edge);
    }

    @Override
    public synchronized Node getNode(String id) {
        return nodes.get(id);
    }

    @Override
    public synchronized Collection<Node> getAllNodes() {
        return new ArrayList<>(nodes.values());
    }

    @Override
    public synchronized Collection<Edge> getEdgesFrom(Node node) {
        Objects.requireNonNull(node, "node");
        return new ArrayList<>(outgoing.getOrDefault(node.getId(), Collections.emptyList()));
    }

    @Override
    public synchronized Collection<Edge> getSortedEdgesFrom(Node node, DefaultMetric metric) {
        Objects.requireNonNull(metric, "metric");
        return getEdgesFrom(node).stream()
                .filter(e -> e.hasWeight(metric))
                .sorted(Comparator.comparingDouble(e -> e.getWeight(metric)))
                .collect(Collectors.toList());
    }

    @Override
    public synchronized boolean deleteNode(String id) {
        if (!nodes.containsKey(id)) {
            return false;
        }

        nodes.remove(id);
        outgoing.remove(id);

        Iterator<Map.Entry<String, Edge>> it = edges.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Edge> entry = it.next();
            Edge edge = entry.getValue();

            if (edge.getSource().getId().equals(id) || edge.getDestination().getId().equals(id)) {
                it.remove();

                List<Edge> out = outgoing.get(edge.getSource().getId());
                if (out != null) {
                    out.remove(edge);
                }
            }
        }

        return true;
    }

    @Override
    public synchronized boolean deleteEdge(String id) {
        Edge edge = edges.remove(id);
        if (edge == null) {
            return false;
        }

        List<Edge> out = outgoing.get(edge.getSource().getId());
        if (out != null) {
            out.remove(edge);
        }
        return true;
    }

    @Override
    public synchronized Edge getEdge(String id) {
        return edges.get(id);
    }

    @Override
    public synchronized Collection<Edge> getEdgesBetween(String sourceId, String destinationId) {
        List<Edge> result = new ArrayList<>();

        for (Edge edge : outgoing.getOrDefault(sourceId, Collections.emptyList())) {
            if (edge.getDestination().getId().equals(destinationId)) {
                result.add(edge);
            }
        }

        return result;
    }

    @Override
    public synchronized void clear() {
        nodes.clear();
        edges.clear();
        outgoing.clear();
    }

    @Override
    public synchronized boolean saveToFile(String filePath) {
        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(filePath)))) {
            for (Node node : nodes.values()) {
                out.print("NODE\t");
                out.print(node.getId());
                out.print('\t');
                out.println(attributesToString(node.getAttributes()));
            }
            for (Edge edge : edges.values()) {
                out.print("EDGE\t");
                out.print(edge.getId());
                out.print('\t');
                out.print(edge.getSource().getId());
                out.print('\t');
                out.print(edge.getDestination().getId());
                out.print('\t');
                out.println(weightsToString(edge.getWeights()));
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public synchronized boolean loadFromFile(String filePath) {
        clear();

        try (BufferedReader in = new BufferedReader(new FileReader(filePath))) {
            String line;

            while ((line = in.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }

                String[] parts = line.split("\t", -1);
                if (parts.length < 2) {
                    continue;
                }

                if ("NODE".equals(parts[0])) {
                    Node node = new Node(parts[1]);

                    if (parts.length > 2 && !parts[2].isBlank()) {
                        parseAttributes(parts[2], node);
                    }

                    addNode(node);
                } else if ("EDGE".equals(parts[0])) {
                    if (parts.length < 5) {
                        continue;
                    }

                    String id = parts[1];
                    String sourceId = parts[2];
                    String destinationId = parts[3];

                    Map<String, Double> weights = parseWeights(parts[4]);

                    Node source = getNode(sourceId);
                    Node destination = getNode(destinationId);

                    if (source == null || destination == null) {
                        continue;
                    }

                    addEdge(new Edge(id, source, destination, weights));
                }
            }

            return true;
        } catch (IOException | RuntimeException e) {
            clear();
            return false;
        }
    }

    private String attributesToString(Map<String, Object> attributes) {
        return attributes.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(";"));
    }

    private String weightsToString(Map<String, Double> weights) {
        return weights.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(";"));
    }

    private void parseAttributes(String text, Node node) {
        for (String token : text.split(";")) {
            if (token.isBlank()) {
                continue;
            }

            int eq = token.indexOf('=');
            if (eq <= 0) {
                continue;
            }

            String key = token.substring(0, eq);
            String value = token.substring(eq + 1);
            node.setAttribute(key, value);
        }
    }

    private Map<String, Double> parseWeights(String text) {
        Map<String, Double> weights = new LinkedHashMap<>();

        for (String token : text.split(";")) {
            if (token.isBlank()) {
                continue;
            }

            int eq = token.indexOf('=');
            if (eq <= 0) {
                continue;
            }

            String key = token.substring(0, eq);
            double value = Double.parseDouble(token.substring(eq + 1));
            weights.put(key, value);
        }

        return weights;
    }
}