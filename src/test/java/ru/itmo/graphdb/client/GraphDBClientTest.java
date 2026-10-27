package ru.itmo.graphdb.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GraphDBClientTest {

    @Test
    void executeWithoutServer_returnsErrorNotThrows() {
        // port 1 — обычно не занят и требует root. Короткие таймауты — чтобы тест был быстрым.
        GraphDBClient client = new GraphDBClient("localhost", 1, 200, 200);
        try {
            String result = client.execute("ADD_NODE A");
            assertTrue(result.startsWith("ERROR"),
                    "ожидали ERROR, получили: " + result);
        } finally {
            client.close();
        }
    }

    @Test
    void isConnected_falseBeforeConnect() {
        GraphDBClient client = new GraphDBClient("localhost", 1);
        try {
            assertFalse(client.isConnected());
        } finally {
            client.close();
        }
    }

    @Test
    void close_isIdempotent() {
        GraphDBClient client = new GraphDBClient("localhost", 1);
        client.close();
        client.close();  // не бросает
    }
}