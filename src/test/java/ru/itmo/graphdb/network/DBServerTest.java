package ru.itmo.graphdb.network;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.itmo.graphdb.client.GraphDBClient;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class DBServerTest {

    private DBServer server;
    private GraphDBClient client;

    @BeforeEach
    void startServer() throws IOException {
        server = new DBServer(
                new InMemoryGraphStorage(),
                new MetricRegistry(),
                new SimpleDijkstraPathFinder()
        );
        server.start(0);  // ОС даст свободный порт — не конфликтуем с другими тестами
        client = new GraphDBClient("localhost", server.getPort(),
                1000, 2000);  // короткие таймауты — чтобы тест не висел
    }

    @AfterEach
    void stopServer() {
        if (client != null) client.close();
        if (server != null) server.stop();
    }

    @Test
    void simpleScenario_overNetwork() {
        assertTrue(client.execute("CREATE_DB demo").startsWith("OK"));
        assertTrue(client.execute("ADD_NODE A").startsWith("OK"));
        assertTrue(client.execute("ADD_NODE B").startsWith("OK"));
        assertTrue(client.execute("ADD_EDGE A B distance 100").startsWith("OK"));
        assertTrue(client.execute("ADD_EDGE A B time 2").startsWith("OK"));

        String path = client.execute("PATH_EDGE A B distance");
        assertTrue(path.contains("Total: 100"));
    }

    @Test
    void multilineResponse_deliveredCorrectly() {
        client.execute("ADD_NODE A");
        client.execute("ADD_NODE B");
        client.execute("ADD_NODE C");
        client.execute("ADD_EDGE A B distance 100");
        client.execute("ADD_EDGE B C distance 150");

        String path = client.execute("PATH_EDGE A C distance");
        assertTrue(path.contains("A -> B"));
        assertTrue(path.contains("B -> C"));
        assertTrue(path.contains("Total: 250"));
    }

    @Test
    void stateSharedBetweenClients() {
        GraphDBClient second = new GraphDBClient("localhost", server.getPort(), 1000, 2000);
        try {
            assertTrue(second.execute("ADD_NODE A").startsWith("OK"));
            // Второй клиент видит узел, добавленный первым — storage общий
            assertTrue(client.execute("PRINT_EDGES A").contains("(no edges)"));
        } finally {
            second.close();
        }
    }

    @Test
    void errorResponses_doNotBreakConnection() {
        // OK → ERROR (дубликат) → OK: соединение живо
        assertTrue(client.execute("ADD_NODE A").startsWith("OK"));
        assertTrue(client.execute("ADD_NODE A").startsWith("ERROR"));
        assertTrue(client.execute("ADD_NODE B").startsWith("OK"));
    }

    @Test
    void unknownCommand_returnedAsError() {
        assertTrue(client.execute("FOOBAR").startsWith("ERROR"));
        // И соединение не сломалось
        assertTrue(client.execute("ADD_NODE A").startsWith("OK"));
    }

    @Test
    void clientReconnect_afterServerRestart() throws IOException {
        assertTrue(client.execute("ADD_NODE A").startsWith("OK"));

        int oldPort = server.getPort();
        server.stop();

        // Новый сервер с чистым хранилищем на том же порту.
        server = new DBServer(
                new InMemoryGraphStorage(),
                new MetricRegistry(),
                new SimpleDijkstraPathFinder()
        );
        server.start(oldPort);

        // Клиент сам переподключится (одна попытка retry).
        assertTrue(client.execute("ADD_NODE B").startsWith("OK"));
    }
}