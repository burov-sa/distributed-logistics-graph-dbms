package ru.itmo.graphdb.network;

import lombok.AccessLevel;
import ru.itmo.graphdb.client.CommandTranslator;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.routing.PathFinder;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.storage.GraphStorage;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * TCP-сервер графовой ноды.
 *
 * <p>Жизненный цикл: {@link #start(int)} → accept-loop → обработчики в пуле →
 * {@link #stop()}. Состояние всей ноды ({@link GraphStorage}, {@link MetricRegistry},
 * {@link PathFinder}) создаётся снаружи и шарится между соединениями.
 */
public class DBServer {

    /** Пул под обработчики соединений (фиксированный, чтобы один клиент с сотней
     *  открытых сокетов не положил сервер). */
    private static final int CLIENT_POOL_SIZE = 8;

    private final GraphStorage storage;
    private final MetricRegistry metrics;
    private final PathFinder pathFinder;

    private final ExecutorService clientPool = Executors.newFixedThreadPool(CLIENT_POOL_SIZE);

    /**
     * Трекинг активных клиентских сокетов.
     * Нужен, чтобы {@link #stop()} мог их принудительно закрыть — иначе
     * {@code readUTF} в обработчиках зависнет на 30 минут idle-таймаута,
     * и JVM не завершится (потоки пула не daemon).
     */
    private final Set<Socket> activeClients = ConcurrentHashMap.newKeySet();

    private volatile boolean running = false;
    private ServerSocket serverSocket;
    private Thread acceptThread;
    private int port = -1;

    public DBServer(GraphStorage storage, MetricRegistry metrics, PathFinder pathFinder) {
        this.storage = storage;
        this.metrics = metrics;
        this.pathFinder = pathFinder;
    }

    /**
     * Запускает сервер. Возвращает управление сразу после открытия сокета.
     * {@code port = 0} - ОС выберет свободный порт, фактический даст {@link #getPort()}.
     */
    public synchronized void start(int port) throws IOException {
        if (running) {
            throw new IllegalStateException("Server already running on port " + this.port);
        }

        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
        this.port = serverSocket.getLocalPort();
        this.running = true;

        acceptThread = new Thread(this::acceptLoop, "db-server-accept");
        // Daemon — чтобы, если пользователь забудет stop(), JVM всё равно завершилась.
        acceptThread.setDaemon(true);
        acceptThread.start();

        System.out.println("[server] listening on port " + this.port);
    }

    /**
     * Останавливает сервер и дожидается завершения обработчиков.
     * Идемпотентен.
     */
    public synchronized void stop() {
        if (!running) return;
        running = false;

        // 1) Закрываем ServerSocket — это разбудит accept() с SocketException.
        try {
            serverSocket.close();
        } catch (IOException ignored) {
        }

        // 2) Закрываем все активные клиентские сокеты — это разбудит readUTF()
        //    с SocketException. Без этого обработчики будут висеть до idle-таймаута.
        for (Socket s : activeClients) {
            try {
                s.close();
            } catch (IOException ignored) {
            }
        }
        activeClients.clear();

        // 3) Гасим пул и ждём.
        clientPool.shutdownNow();
        try {
            clientPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 4) Дожидаемся accept-потока.
        if (acceptThread != null) {
            try {
                acceptThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            acceptThread = null;
        }

        System.out.println("[server] stopped");
    }

    public int getPort() {
        return port;
    }

    public boolean isRunning() {
        return running;
    }

    // ---- внутреннее ----

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                activeClients.add(client);

                // Свой транслятор на соединение: изолирует currentDbName.
                CommandTranslator perConnTranslator =
                        new CommandTranslator(storage, metrics, pathFinder);

                clientPool.submit(() -> {
                    try {
                        new ClientHandler(client, perConnTranslator).run();
                    } finally {
                        activeClients.remove(client);
                    }
                });
            } catch (SocketException e) {
                // serverSocket.close() во время stop() — это штатно
                if (!running) break;
                System.err.println("[server] accept socket error: " + e.getMessage());
            } catch (IOException e) {
                if (!running) break;
                System.err.println("[server] accept I/O error: " + e.getMessage());
            }
        }
    }

    /** CLI-запуск: {@code java ... DBServer --port 5555}. */
    public static void main(String[] args) throws IOException {
        int port = 5555;
        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
            }
        }

        DBServer server = new DBServer(
                new InMemoryGraphStorage(),
                new MetricRegistry(),
                new SimpleDijkstraPathFinder()
        );
        server.start(port);

        // Ctrl+C → shutdown hook → stop() → счётчик → main просыпается → JVM выходит.
        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            shutdown.countDown();
        }, "db-server-shutdown"));

        try {
            shutdown.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
