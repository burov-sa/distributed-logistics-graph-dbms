package ru.itmo.graphdb.sharding;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.itmo.graphdb.model.DefaultMetric;
import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.model.Node;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.storage.GraphStorage;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardCommandTranslatorTest {

    private GraphStorage storage;
    private MetricRegistry metrics;
    private ShardCommandTranslator translator;

    @BeforeEach
    void setUp() {
        storage = new InMemoryGraphStorage();
        metrics = new MetricRegistry();
        translator = new ShardCommandTranslator(
                storage, metrics, new SimpleDijkstraPathFinder(), 0);
    }

    // ---------- _GET_NODE ----------

    @Test
    void getNodeReturnsAttributes() {
        Node a = new Node("A");
        a.setAttribute("capacity", "10");
        storage.addNode(a);

        assertEquals("NODE\tA\tcapacity=10", translator.execute("_GET_NODE A"));
    }

    @Test
    void getNodeReturnsNotFound() {
        assertEquals("NOT_FOUND", translator.execute("_GET_NODE missing"));
    }

    // ---------- _GET_EDGES_FROM ----------

    @Test
    void getEdgesFromEmpty() {
        storage.addNode(new Node("A"));
        assertEquals("NO_EDGES", translator.execute("_GET_EDGES_FROM A"));
    }

    @Test
    void getEdgesFromMissingNode() {
        assertEquals("NOT_FOUND", translator.execute("_GET_EDGES_FROM missing"));
    }

    @Test
    void getEdgesFromReturnsEdgeLines() {
        Node a = new Node("A");
        Node b = new Node("B");
        storage.addNode(a);
        storage.addNode(b);
        storage.addEdge(new Edge("A->B", a, b, Map.of("distance", 5.0)));

        String resp = translator.execute("_GET_EDGES_FROM A");
        assertTrue(resp.startsWith("EDGE\tA->B\tA\tB\t"), "actual: " + resp);
        assertTrue(resp.contains("distance=5"), "actual: " + resp);
    }

    // ---------- _ADD_EDGE_RAW ----------

    @Test
    void addEdgeRawWithinSameShard() {
        storage.addNode(new Node("A"));
        storage.addNode(new Node("B"));

        String resp = translator.execute("_ADD_EDGE_RAW A B distance 3.5");
        assertTrue(resp.startsWith("OK"), "actual: " + resp);

        Edge e = storage.getEdge("A->B");
        assertNotNull(e);
        assertEquals(3.5, e.getWeight(DefaultMetric.DISTANCE), 1e-9);
    }

    @Test
    void addEdgeRawCreatesForeignStubForMissingDestination() {
        storage.addNode(new Node("A"));

        translator.execute("_ADD_EDGE_RAW A Z distance 7");

        Edge e = storage.getEdge("A->Z");
        assertNotNull(e);
        Node z = storage.getNode("Z");
        assertNotNull(z);
        assertEquals(ShardCommandTranslator.FOREIGN_TRUE,
                String.valueOf(z.getAttribute(ShardCommandTranslator.FOREIGN_ATTR)));
    }

    @Test
    void addEdgeRawFailsIfSourceMissing() {
        String resp = translator.execute("_ADD_EDGE_RAW A B distance 1");
        assertTrue(resp.startsWith("ERROR"), "actual: " + resp);
    }

    @Test
    void addEdgeRawFailsIfSourceIsForeignStub() {
        storage.addNode(new Node("A"));
        translator.execute("_ADD_EDGE_RAW A Z distance 1");   // создаёт stub Z

        String resp = translator.execute("_ADD_EDGE_RAW Z A distance 1");
        assertTrue(resp.startsWith("ERROR"), "actual: " + resp);
        assertTrue(resp.contains("foreign stub"), "actual: " + resp);
    }

    // ---------- _DELETE_EDGES_TO ----------

    @Test
    void deleteEdgesToRemovesEdgesAndStub() {
        storage.addNode(new Node("A"));
        translator.execute("_ADD_EDGE_RAW A Z distance 1");
        assertNotNull(storage.getNode("Z"));

        String resp = translator.execute("_DELETE_EDGES_TO Z");
        assertTrue(resp.startsWith("OK"), "actual: " + resp);
        assertNull(storage.getNode("Z"));
        assertNull(storage.getEdge("A->Z"));
    }

    @Test
    void deleteEdgesToOnUnknownNodeIsOk() {
        String resp = translator.execute("_DELETE_EDGES_TO Q");
        assertTrue(resp.startsWith("OK"), "actual: " + resp);
    }

    // ---------- _CLEAR ----------

    @Test
    void clearEmptiesStorage() {
        storage.addNode(new Node("A"));
        translator.execute("_CLEAR");
        assertEquals(0, storage.getAllNodes().size());
    }

    // ---------- delegate behavior ----------

    @Test
    void normalCommandIsDelegated() {
        assertEquals("OK: node 'A' added", translator.execute("ADD_NODE A"));
        assertNotNull(storage.getNode("A"));
    }

    @Test
    void unknownInternalCommandErrors() {
        String resp = translator.execute("_NOPE x");
        assertTrue(resp.startsWith("ERROR"), "actual: " + resp);
    }

    @Test
    void internalCommandsAreCaseSensitive() {
        // Строчный вариант — не внутренняя команда, а неизвестная обычная.
        String resp = translator.execute("_get_node A");
        assertTrue(resp.startsWith("ERROR"), "actual: " + resp);
    }
}