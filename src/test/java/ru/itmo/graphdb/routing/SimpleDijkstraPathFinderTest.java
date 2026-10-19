package ru.itmo.graphdb.routing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.itmo.graphdb.model.DefaultMetric;
import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.Node;
import ru.itmo.graphdb.storage.GraphStorage;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SimpleDijkstraPathFinderTest {

    private GraphStorage storage;
    private SimpleDijkstraPathFinder finder;

    @BeforeEach
    void setUp() {
        storage = new InMemoryGraphStorage();
        finder = new SimpleDijkstraPathFinder();
    }


    private void addNode(String id) {
        storage.addNode(new Node(id));
    }

    private void addEdge(String from, String to, double time, double distance, double cost) {
        String id = from + "->" + to;
        storage.addEdge(new Edge(id,
                storage.getNode(from),
                storage.getNode(to),
                time, distance, cost));
    }

    private void addEdgeOnlyCost(String from, String to, double cost) {
        String id = from + "->" + to;
        storage.addEdge(new Edge(id,
                storage.getNode(from),
                storage.getNode(to),
                Map.of(DefaultMetric.COST.key(), cost)));
    }


    @Test
    void directPath_found() {
        addNode("A");
        addNode("B");
        addEdge("A", "B", 2.0, 100.0, 500.0);

        List<Edge> path = finder.findShortestPath(storage, "A", "B", DefaultMetric.DISTANCE);

        assertEquals(1, path.size());
        assertEquals("A", path.get(0).getSource().getId());
        assertEquals("B", path.get(0).getDestination().getId());
    }

    @Test
    void multiHopPath_foundInOrder() {
        addNode("A");
        addNode("B");
        addNode("C");

        addEdge("A", "B", 2.0, 100.0, 500.0);
        addEdge("B", "C", 3.0, 150.0, 700.0);

        List<Edge> path = finder.findShortestPath(storage, "A", "C", DefaultMetric.DISTANCE);

        assertEquals(2, path.size());
        assertEquals("A->B", path.get(0).getId());
        assertEquals("B->C", path.get(1).getId());
    }

    @Test
    void choosesCheaperRouteByMetric() {
        // Прямой путь дорогой по distance, обходной — дешевле.
        addNode("A");
        addNode("B");
        addNode("C");
        addNode("D");

        addEdge("A", "C", 1.0, 500.0, 100.0);   // дорогой
        addEdge("C", "D", 1.0, 500.0, 100.0);

        addEdge("A", "B", 1.0, 10.0, 100.0);    // дешёвый обход
        addEdge("B", "D", 1.0, 10.0, 100.0);

        List<Edge> byDistance = finder.findShortestPath(storage, "A", "D", DefaultMetric.DISTANCE);
        assertEquals(2, byDistance.size());
        assertEquals("A->B", byDistance.get(0).getId());
        assertEquals("B->D", byDistance.get(1).getId());

        // А по cost оба пути равны — значит допускается любой. Проверим лишь, что путь найден.
        List<Edge> byCost = finder.findShortestPath(storage, "A", "D", DefaultMetric.COST);
        assertEquals(2, byCost.size());
    }

    @Test
    void noPath_returnsEmptyList() {
        addNode("A");
        addNode("B");
        addNode("D"); // изолированный
        addEdge("A", "B", 1.0, 1.0, 1.0);

        List<Edge> path = finder.findShortestPath(storage, "A", "D", DefaultMetric.DISTANCE);

        assertTrue(path.isEmpty());
    }

    @Test
    void unknownSourceOrDestination_returnsEmptyList() {
        addNode("A");

        assertTrue(finder.findShortestPath(storage, "A", "Z", DefaultMetric.DISTANCE).isEmpty());
        assertTrue(finder.findShortestPath(storage, "Z", "A", DefaultMetric.DISTANCE).isEmpty());
    }

    @Test
    void sameSourceAndDestination_returnsEmptyList() {
        addNode("A");

        List<Edge> path = finder.findShortestPath(storage, "A", "A", DefaultMetric.DISTANCE);

        assertTrue(path.isEmpty());
    }

    @Test
    void edgeWithoutRequestedMetric_isSkipped() {
        addNode("A");
        addNode("B");
        // Ребро есть, но метрики distance на нём нет.
        addEdgeOnlyCost("A", "B", 100.0);

        List<Edge> byDistance = finder.findShortestPath(storage, "A", "B", DefaultMetric.DISTANCE);
        assertTrue(byDistance.isEmpty(), "ребро без distance должно игнорироваться");

        List<Edge> byCost = finder.findShortestPath(storage, "A", "B", DefaultMetric.COST);
        assertEquals(1, byCost.size(), "по cost путь должен найтись");
    }

    @Test
    void negativeWeightEdge_isIgnored() {
        addNode("A");
        addNode("B");
        // Отрицательный вес не поддерживается в этой учебной реализации — ребро пропускается.
        addEdge("A", "B", 1.0, -10.0, 1.0);

        List<Edge> path = finder.findShortestPath(storage, "A", "B", DefaultMetric.DISTANCE);

        assertTrue(path.isEmpty());
    }
}