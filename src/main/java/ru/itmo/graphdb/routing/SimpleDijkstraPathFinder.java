package ru.itmo.graphdb.routing;

import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.Metric;
import ru.itmo.graphdb.model.Node;
import ru.itmo.graphdb.storage.GraphStorage;

import java.util.*;

public class SimpleDijkstraPathFinder implements PathFinder {

    @Override
    public List<Edge> findShortestPath(GraphStorage storage, String source, String destination, Metric metric) {
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(metric, "metric");

        Node start = storage.getNode(source);
        Node end = storage.getNode(destination);

        if (start == null || end == null) {
            return Collections.emptyList();
        }
        if (source.equals(destination)) {
            return Collections.emptyList();
        }

        Map<String, Double> dist = new HashMap<>();
        Map<String, Edge> prev = new HashMap<>();
        Set<String> visited = new HashSet<>();

        PriorityQueue<NodeDistance> queue =
                new PriorityQueue<>(Comparator.comparingDouble(nd -> nd.distance));

        dist.put(source, 0.0);
        queue.add(new NodeDistance(start, 0.0));

        while (!queue.isEmpty()) {
            NodeDistance current = queue.poll();
            String currentId = current.node.getId();

            if (!visited.add(currentId)) {
                continue;
            }
            if (currentId.equals(destination)) {
                break;
            }

            for (Edge edge : storage.getEdgesFrom(current.node)) {
                if (!edge.hasWeight(metric)) {
                    continue;
                }

                double weight = edge.getWeight(metric);
                if (weight < 0) {
                    continue;
                }

                String nextId = edge.getDestination().getId();
                double newDistance = current.distance + weight;

                if (newDistance < dist.getOrDefault(nextId, Double.POSITIVE_INFINITY)) {
                    dist.put(nextId, newDistance);
                    prev.put(nextId, edge);
                    queue.add(new NodeDistance(edge.getDestination(), newDistance));
                }
            }
        }

        if (!prev.containsKey(destination)) {
            return Collections.emptyList();
        }

        LinkedList<Edge> path = new LinkedList<>();
        String currentId = destination;

        while (!currentId.equals(source)) {
            Edge edge = prev.get(currentId);
            if (edge == null) {
                return Collections.emptyList();
            }

            path.addFirst(edge);
            currentId = edge.getSource().getId();
        }

        return path;
    }

    private static class NodeDistance {
        final Node node;
        final double distance;

        NodeDistance(Node node, double distance) {
            this.node = node;
            this.distance = distance;
        }
    }
}