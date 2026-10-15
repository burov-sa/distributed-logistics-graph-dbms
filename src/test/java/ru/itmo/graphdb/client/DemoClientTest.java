package ru.itmo.graphdb.client;

import org.junit.jupiter.api.Test;
import ru.itmo.graphdb.model.DefaultMetric;
import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.routing.PathFinder;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.storage.GraphStorage;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import static org.junit.jupiter.api.Assertions.*;

class DemoClientTest {

    @Test
    void demoFlow_endToEnd() {
        // --- ровно как в DemoClient.main ---
        GraphStorage storage   = new InMemoryGraphStorage();
        MetricRegistry metrics = new MetricRegistry();
        PathFinder pathFinder  = new SimpleDijkstraPathFinder();
        CommandTranslator cli  = new CommandTranslator(storage, metrics, pathFinder);

        assertTrue(cli.execute("CREATE_DB demo").startsWith("OK"));
        assertTrue(cli.execute("ADD_METRIC fuel").startsWith("OK"));

        assertTrue(cli.execute("ADD_NODE A").startsWith("OK"));
        assertTrue(cli.execute("ADD_NODE B").startsWith("OK"));
        assertTrue(cli.execute("ADD_NODE C").startsWith("OK"));

        assertTrue(cli.execute("SET_PARAMETER A capacity 100").startsWith("OK"));

        assertTrue(cli.execute("ADD_EDGE A B distance 100").startsWith("OK"));
        assertTrue(cli.execute("ADD_EDGE A B time 2").startsWith("OK"));
        assertTrue(cli.execute("ADD_EDGE A B cost 500").startsWith("OK"));

        assertTrue(cli.execute("ADD_EDGE B C distance 150").startsWith("OK"));
        assertTrue(cli.execute("ADD_EDGE B C time 3").startsWith("OK"));
        assertTrue(cli.execute("ADD_EDGE B C cost 700").startsWith("OK"));

        // --- проверяем финальное состояние ---

        // узлы на месте
        assertNotNull(storage.getNode("A"));
        assertNotNull(storage.getNode("B"));
        assertNotNull(storage.getNode("C"));

        // атрибут узла
        assertEquals("100", storage.getNode("A").getAttribute("capacity"));

        // ребро A->B содержит все три метрики
        Edge ab = storage.getEdge("A->B");
        assertNotNull(ab);
        assertEquals(100.0, ab.getWeight(DefaultMetric.DISTANCE));
        assertEquals(2.0,   ab.getWeight(DefaultMetric.TIME));
        assertEquals(500.0, ab.getWeight(DefaultMetric.COST));

        // ребро B->C тоже
        Edge bc = storage.getEdge("B->C");
        assertNotNull(bc);
        assertEquals(150.0, bc.getWeight(DefaultMetric.DISTANCE));
        assertEquals(3.0,   bc.getWeight(DefaultMetric.TIME));
        assertEquals(700.0, bc.getWeight(DefaultMetric.COST));

        // PRINT_EDGES A возвращает ребро
        String printed = cli.execute("PRINT_EDGES A");
        assertTrue(printed.contains("A->B"));

        // PATH_EDGE A C distance = 100 + 150 = 250
        String path = cli.execute("PATH_EDGE A C distance");
        assertTrue(path.contains("Path by distance"));
        assertTrue(path.contains("Total: 250"));

        // пользовательская метрика зарегистрирована, но на рёбрах отсутствует —
        // поиск по ней должен вернуть "No path ...", а не упасть.
        String byFuel = cli.execute("PATH_EDGE A C fuel");
        assertTrue(byFuel.startsWith("No path"));
    }
}
