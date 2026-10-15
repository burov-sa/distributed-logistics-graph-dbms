package ru.itmo.graphdb.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.itmo.graphdb.model.DefaultMetric;
import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.model.Node;
import ru.itmo.graphdb.routing.PathFinder;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.storage.GraphStorage;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CommandTranslatorTest {

    private GraphStorage storage;
    private MetricRegistry metrics;
    private PathFinder finder;
    private CommandTranslator cli;

    @BeforeEach
    void setUp() {
        storage = new InMemoryGraphStorage();
        metrics = new MetricRegistry();
        finder  = new SimpleDijkstraPathFinder();
        cli     = new CommandTranslator(storage, metrics, finder);
    }

    // ---------- общие ----------

    @Test
    void emptyCommand_returnsError() {
        assertEquals("ERROR: empty command", cli.execute(""));
        assertEquals("ERROR: empty command", cli.execute("   "));
        assertEquals("ERROR: empty command", cli.execute(null));
    }

    @Test
    void unknownCommand_returnsError() {
        assertEquals("ERROR: unknown command: FOOBAR", cli.execute("FOOBAR"));
    }

    @Test
    void commandCaseInsensitive() {
        assertTrue(cli.execute("create_db x").startsWith("OK"));
        assertTrue(cli.execute("ADD_NODE A").startsWith("OK"));
        assertTrue(cli.execute("add_node B").startsWith("OK"));
        assertNotNull(storage.getNode("B"));
    }

    // ---------- DB ----------

    @Test
    void createDb_clearsPreviousState() {
        cli.execute("ADD_NODE A");
        assertNotNull(storage.getNode("A"));

        assertTrue(cli.execute("CREATE_DB fresh").startsWith("OK"));
        assertNull(storage.getNode("A"));
    }

    // ---------- Metrics ----------

    @Test
    void addMetric_ok() {
        assertTrue(cli.execute("ADD_METRIC fuel").startsWith("OK"));
        assertTrue(metrics.contains("fuel"));
    }

    @Test
    void addMetric_withDisplayName() {
        assertTrue(cli.execute("ADD_METRIC fuel Расход").startsWith("OK"));
        assertTrue(metrics.find("fuel").isPresent());
        assertEquals("Расход", metrics.find("fuel").get().displayName());
    }

    @Test
    void addMetric_duplicate_returnsError() {
        cli.execute("ADD_METRIC fuel");
        assertTrue(cli.execute("ADD_METRIC fuel").startsWith("ERROR"));
    }

    @Test
    void addMetric_defaultKey_returnsError() {
        // "distance" уже занят DefaultMetric
        assertTrue(cli.execute("ADD_METRIC distance").startsWith("ERROR"));
    }

    @Test
    void deleteMetric_custom_ok() {
        cli.execute("ADD_METRIC fuel");
        assertTrue(cli.execute("DELETE_METRIC fuel").startsWith("OK"));
        assertFalse(metrics.contains("fuel"));
    }

    @Test
    void deleteMetric_default_returnsError() {
        assertTrue(cli.execute("DELETE_METRIC distance").startsWith("ERROR"));
    }

    @Test
    void printMetrics_containsDefaults() {
        String out = cli.execute("PRINT_METRICS");
        assertTrue(out.contains("distance"));
        assertTrue(out.contains("time"));
        assertTrue(out.contains("cost"));
    }

    // ---------- Nodes ----------

    @Test
    void addNode_ok() {
        assertTrue(cli.execute("ADD_NODE A").startsWith("OK"));
        assertNotNull(storage.getNode("A"));
    }

    @Test
    void addNode_duplicate_returnsError() {
        cli.execute("ADD_NODE A");
        assertTrue(cli.execute("ADD_NODE A").startsWith("ERROR"));
    }

    @Test
    void setParameter_ok() {
        cli.execute("ADD_NODE A");
        assertTrue(cli.execute("SET_PARAMETER A capacity 100").startsWith("OK"));

        Node a = storage.getNode("A");
        assertEquals("100", a.getAttribute("capacity"));
    }

    @Test
    void setParameter_unknownNode_returnsError() {
        assertTrue(cli.execute("SET_PARAMETER Z capacity 100").startsWith("ERROR"));
    }

    @Test
    void deleteNode_ok_andRemovesEdges() {
        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");
        cli.execute("ADD_EDGE A B distance 100");

        assertTrue(cli.execute("DELETE_NODE A").startsWith("OK"));
        assertNull(storage.getNode("A"));
        assertNull(storage.getEdge("A->B"));
    }

    @Test
    void deleteNode_unknown_returnsError() {
        assertTrue(cli.execute("DELETE_NODE Z").startsWith("ERROR"));
    }

    // ---------- Edges ----------

    @Test
    void addEdge_requiresExistingNodes() {
        assertTrue(cli.execute("ADD_EDGE A B distance 100").startsWith("ERROR"));
    }

    @Test
    void addEdge_requiresKnownMetric() {
        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");

        assertTrue(cli.execute("ADD_EDGE A B teleport 100").startsWith("ERROR"));
    }

    @Test
    void addEdge_createsEdgeWithOneWeight() {
        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");

        assertTrue(cli.execute("ADD_EDGE A B distance 100").startsWith("OK"));

        Edge e = storage.getEdge("A->B");
        assertNotNull(e);
        assertEquals(100.0, e.getWeight(DefaultMetric.DISTANCE));
        assertFalse(e.hasWeight(DefaultMetric.TIME));
    }

    @Test
    void addEdge_samePair_accumulatesMetrics() {
        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");

        cli.execute("ADD_EDGE A B distance 100");
        cli.execute("ADD_EDGE A B time 2");
        cli.execute("ADD_EDGE A B cost 500");

        Edge e = storage.getEdge("A->B");
        assertEquals(100.0, e.getWeight(DefaultMetric.DISTANCE));
        assertEquals(2.0,   e.getWeight(DefaultMetric.TIME));
        assertEquals(500.0, e.getWeight(DefaultMetric.COST));
    }

    @Test
    void addEdge_invalidNumber_returnsError() {
        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");

        assertTrue(cli.execute("ADD_EDGE A B distance abc").startsWith("ERROR"));
    }

    @Test
    void printEdges_byNode() {
        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");
        cli.execute("ADD_EDGE A B distance 100");

        String out = cli.execute("PRINT_EDGES A");
        assertTrue(out.contains("A->B"));
    }

    @Test
    void printEdges_betweenPair() {
        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");
        cli.execute("ADD_EDGE A B distance 100");

        String out = cli.execute("PRINT_EDGES A B");
        assertTrue(out.contains("A->B"));
    }

    @Test
    void printEdges_emptyForIsolatedNode() {
        cli.execute("ADD_NODE A");
        assertEquals("(no edges)", cli.execute("PRINT_EDGES A"));
    }

    @Test
    void deleteEdge_ok() {
        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");
        cli.execute("ADD_EDGE A B distance 100");

        assertTrue(cli.execute("DELETE_EDGE A->B").startsWith("OK"));
        assertNull(storage.getEdge("A->B"));
    }

    @Test
    void deleteEdge_unknown_returnsError() {
        assertTrue(cli.execute("DELETE_EDGE X->Y").startsWith("ERROR"));
    }

    // ---------- Path ----------

    @Test
    void pathEdge_returnsRouteAndTotal() {
        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");
        cli.execute("ADD_NODE C");
        cli.execute("ADD_EDGE A B distance 100");
        cli.execute("ADD_EDGE B C distance 150");

        String out = cli.execute("PATH_EDGE A C distance");

        assertTrue(out.contains("Path by distance"));
        assertTrue(out.contains("A -> B"));
        assertTrue(out.contains("B -> C"));
        assertTrue(out.contains("Total: 250"));
    }

    @Test
    void pathEdge_noRoute_returnsMessage() {
        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");

        String out = cli.execute("PATH_EDGE A B distance");
        assertTrue(out.startsWith("No path"));
    }

    @Test
    void pathEdge_unknownMetric_returnsError() {
        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");

        assertTrue(cli.execute("PATH_EDGE A B teleport").startsWith("ERROR"));
    }

    // ---------- Save / Load ----------

    @Test
    void saveAndLoad_roundTrip(@TempDir Path tempDir) {
        Path file = tempDir.resolve("demo.graphdb");

        cli.execute("ADD_NODE A");
        cli.execute("ADD_NODE B");
        cli.execute("SET_PARAMETER A capacity 100");
        cli.execute("ADD_EDGE A B distance 100");
        cli.execute("ADD_EDGE A B time 2");

        assertTrue(cli.execute("SAVE_DB " + file).startsWith("OK"));

        // Пересоздаём клиента — state пустой.
        GraphStorage freshStorage = new InMemoryGraphStorage();
        CommandTranslator freshCli = new CommandTranslator(
                freshStorage, new MetricRegistry(), new SimpleDijkstraPathFinder()
        );

        assertTrue(freshCli.execute("READ_DB " + file).startsWith("OK"));

        assertNotNull(freshStorage.getNode("A"));
        assertEquals("100", freshStorage.getNode("A").getAttribute("capacity"));

        Edge e = freshStorage.getEdge("A->B");
        assertNotNull(e);
        assertEquals(100.0, e.getWeight(DefaultMetric.DISTANCE));
        assertEquals(2.0,   e.getWeight(DefaultMetric.TIME));
    }

    @Test
    void readDb_missingFile_returnsError() {
        assertTrue(cli.execute("READ_DB /nonexistent/path.graphdb").startsWith("ERROR"));
    }
}