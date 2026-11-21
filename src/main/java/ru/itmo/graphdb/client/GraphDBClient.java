package ru.itmo.graphdb.client;

import ru.itmo.graphdb.network.protocol.Frame;
import ru.itmo.graphdb.network.protocol.FrameType;
import ru.itmo.graphdb.network.protocol.Frames;

import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

/**
 * Тонкий TCP-клиент графовой СУБД.
 *
 * <p>Контракт совпадает с {@link CommandTranslator}: {@link #execute(String)}
 * принимает строку-команду и возвращает строку-ответ. Любая проблема (сеть,
 * таймаут, отказ сервера) приходит как строка {@code "ERROR: ..."} — исключения
 * не выходят наружу, чтобы AnyLogic-агент мог смотреть на префикс.
 *
 * <p>Протокол — кадры {@link Frame}. Клиент шлёт {@code REQ}, читает кадры;
 * {@code RESP} — это ответ на конкретный запрос, {@code PUSH} — асинхронное
 * уведомление сервера. Пользователь может подписаться на push через
 * {@link #setPushListener(Consumer)}. Push, пришедшие до установки листенера,
 * буферизуются и выдаются при подписке.
 *
 * <p>API для агентов AnyLogic: {@code new GraphDBClient(host, port)} в
 * конструкторе агента, {@code setPushListener(...)} при желании принимать
 * серверные события, {@code execute(cmd)} — на каждый запрос.
 *
 * <p>Гарантия порядка: в полёте всегда ровно один {@code REQ}.
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

    /**
     * Буфер push-сообщений, пришедших до установки листенера.
     * После {@link #setPushListener(Consumer)} буфер опорожняется в порядке
     * поступления.
     */
    private final Queue<String> pendingPushes = new ConcurrentLinkedQueue<>();

    /** Текущий листенер push-сообщений. Может быть {@code null}. */
    private volatile Consumer<String> pushListener;

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

    /**
     * Подписывает клиента на асинхронные push-сообщения сервера.
     *
     * <p>Если listener = {@code null} — push снова начинают копиться в буфере.
     * При установке непустого листенера все ранее накопленные сообщения
     * выдаются ему синхронно в порядке поступления.
     *
     * <p>Листенер не должен бросать наружу; исключения игнорируются, чтобы
     * не порвать протокол.
     */
    public synchronized void setPushListener(Consumer<String> listener) {
        this.pushListener = listener;
        if (listener == null) {
            return;
        }
        String pending;
        while ((pending = pendingPushes.poll()) != null) {
            try {
                listener.accept(pending);
            } catch (RuntimeException ignored) {
                // см. javadoc
            }
        }
    }

    /** Ленивое подключение. Если не звать — {@link #execute} сам подключится. */
    public synchronized void connect() throws IOException {
        if (isConnected()) {
            return;
        }

        Socket s = new Socket();
        // Отдельный от read-таймаута таймаут на connect, чтобы не ждать
        // connection refused слишком долго.
        s.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        s.setSoTimeout(readTimeoutMs);

        // КРИТИЧНО для request-response протокола:
        // отключаем алгоритм Нейгла. Иначе writeByte(type) + writeUTF(payload)
        // (3 маленьких сисколла на кадр) задерживаются на ~40 мс из-за
        // Nagle + Delayed ACK. При сотнях/тысячах round-trip'ов (PATH_EDGE
        // через шард-границы) это даёт десятки секунд.
        s.setTcpNoDelay(true);

        this.socket = s;
        // Буферизуем потоки: writeByte + writeUTF идут в буфер, flush()
        // отправляет один TCP-сегмент. В паре с TCP_NODELAY убирает
        // фрагментацию кадра на проводе.
        this.in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
    }

    public synchronized boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }

    /**
     * Выполняет одну команду и возвращает ответ.
     *
     * <p>{@code synchronized} — по протоколу в полёте ровно один запрос.
     * Для многопоточного приложения (несколько агентов AnyLogic на одном
     * клиенте) нужен клиент на каждого агента.
     *
     * <p>При {@link IOException} — одна попытка переподключения и повтор
     * того же {@code REQ}. Ограничение: если сервер успел применить команду,
     * но ответ потерялся, повтор может привести к дублирующему эффекту
     * (идемпотентности на уровне протокола нет).
     *
     * <p>Во время чтения ответа клиент корректно принимает {@code PUSH} и
     * передаёт их в листенер.
     */
    public synchronized String execute(String line) {
        try {
            return doExecute(line);
        } catch (IOException first) {
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

        Frames.write(out, Frame.request(line));
        out.flush();

        while (true) {
            Frame frame = Frames.read(in);
            switch (frame.type()) {
                case RESP:
                    return frame.payload();
                case PUSH:
                    dispatchPush(frame.payload());
                    break;
                case REQ:
                default:
                    throw new IOException("Unexpected frame type from server: " + frame.type());
            }
        }
    }

    private void dispatchPush(String payload) {
        Consumer<String> listener = pushListener;
        if (listener == null) {
            pendingPushes.add(payload);
            return;
        }
        try {
            listener.accept(payload);
        } catch (RuntimeException ignored) {
            // см. javadoc setPushListener
        }
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
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
            // ignore
        }
        socket = null;
        in = null;
        out = null;
    }
}