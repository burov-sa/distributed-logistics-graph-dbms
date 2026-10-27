package ru.itmo.graphdb.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * Тонкий TCP-клиент графовой СУБД.
 *
 * <p>Контракт совпадает с {@link CommandTranslator}: {@link #execute(String)}
 * принимает строку-команду и возвращает строку-ответ. Любая проблема (сеть,
 * таймаут, отказ сервера) приходит как строка {@code "ERROR: ..."} - исключения
 * выходят наружу, чтобы AnyLogic-агент или подобный мог просто смотреть на префикс.
 *
 * <p>API для клиентских приложений, планирую использовать для агентов AnyLogic: {@code new GraphDBClient(host, port)}
 * в конструкторе агента, {@code client.execute(cmd)} при каждом запросе.
 */
public class GraphDBClient implements AutoCloseable {

    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 3000;
    private static final int DEFAULT_READ_TIMEOUT_MS = 10_000;

    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    public GraphDBClient(String host, int port) {
        this(host, port, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
    }

    /** Для тестов с короткими таймаутами. */
    public GraphDBClient(String host, int port, int connectTimeoutMs, int readTimeoutMs) {
        this.host = host;
        this.port = port;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    /** Ленивое подключение. Можно не вызывать {@link #execute}, сам подключится. */
    public synchronized void connect() throws IOException {
        if (isConnected()) return;

        Socket s = new Socket();
        // Отдельный от read-таймаута, таймаут на connect. Для предотвращения слишком
        // длительного ожидания connection refused.
        s.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        s.setSoTimeout(readTimeoutMs);

        this.socket = s;
        this.in = new DataInputStream(s.getInputStream());
        this.out = new DataOutputStream(s.getOutputStream());
    }

    public synchronized boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }

    /**
     * Выполняет одну команду и возвращает ответ.
     *
     * <p>{@code synchronized} - в соответствии с заложенной архитектурой и протоколом: строго «один запрос - один ответ».
     * Параллельные вызовы невозможны. Для многопоточного приложения, например, AnyLogic
     * (несколько агентов на одном клиенте) необходим клиент на каждого агента, либо
     * теоретически замена synchronized на пул сокетов.
     */
    public synchronized String execute(String line) {
        try {
            return doExecute(line);
        } catch (IOException first) {
            // Одна попытка переподключения.
            closeQuietly();
            try {
                connect();
                return doExecute(line);
            } catch (IOException second) {
                return mapError(second);
            }
        }
    }

    private String doExecute(String line) throws IOException {
        if (!isConnected()) {
            connect();
        }
        out.writeUTF(line);
        out.flush();
        return in.readUTF();
    }

    private String mapError(IOException e) {
        if (e instanceof ConnectException) {
            return "ERROR: connection refused: " + host + ":" + port;
        }
        if (e instanceof SocketTimeoutException) {
            return "ERROR: timeout talking to " + host + ":" + port;
        }
        if (e instanceof EOFException) {
            return "ERROR: server closed connection";
        }
        return "ERROR: I/O error: " + e.getMessage();
    }

    @Override
    public synchronized void close() {
        closeQuietly();
    }

    private void closeQuietly() {
        try {
            if (socket != null) socket.close();
        } catch (IOException ignored) {
        }
        socket = null;
        in = null;
        out = null;
    }
}
