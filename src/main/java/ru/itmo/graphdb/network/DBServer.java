package ru.itmo.graphdb.network;

import ru.itmo.graphdb.client.CommandExecutor;
import ru.itmo.graphdb.client.CommandTranslator;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.routing.PathFinder;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.sharding.ShardCommandTranslator;
import ru.itmo.graphdb.storage.GraphStorage;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * TCP-сервер графовой ноды.
 *
 * <p>Жизненный цикл: {@link #start(int)} → accept-loop → обработчики в пуле →
 * {@link #stop()}. Исполнители команд создаются фабрикой
 * {@link CommandExecutorFactory} — по одному на соединение. Общее состояние
 * ноды (хранилище, реестр метрик, pathfinder) контролирует фабрика
 * (обычно — замыкание с общими ссылками).
 *
 * <p>Сервер ничего не знает про шардирование. В {@link #main(String[])}
 * он собирает {@link ShardCommandTranslator}, если задан {@code --shard-id},
 * но это выбор точки входа, а не свойство класса.
 */
public class DBServer {

    private static final int CLIENT_POOL_SIZE = 8;

    private final CommandExecutorFactory executorFactory;

    private final ExecutorService clientPool =
            Executors.newFixedThreadPool(CLIENT_POOL_SIZE);

    private final Set<Socket> activeClients = ConcurrentHashMap.newKeySet();
    private final Set<ClientHandler> activeHandlers = ConcurrentHashMap.newKeySet();

    private volatile Consumer<ClientHandler> onClientConnected;
    private volatile boolean running = false;
    private ServerSocket serverSocket;
    private Thread acceptThread;
    private int port = -1;

    /**
     * Обратно совместимый конструктор: общее состояние ноды передаётся
     * напрямую, фабрика строит per-connection {@link CommandTranslator}
     * поверх этих же ссылок.
     */
    public DBServer(GraphStorage storage, MetricRegistry metrics, PathFinder pathFinder) {
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(metrics, "metrics");
        Objects.requireNonNull(pathFinder, "pathFinder");
        this.executorFactory = () -> new CommandTranslator(storage, metrics, pathFinder);
    }

    /** Конструктор с фабрикой: полный контроль над исполнителем на соединение. */
    public DBServer(CommandExecutorFactory executorFactory) {
        this.executorFactory = Objects.requireNonNull(executorFactory, "executorFactory");
    }

    /** Callback на успешное подключение; вызывается один раз до read-loop. */
    public void setOnClientConnected(Consumer<ClientHandler> callback) {
        this.onClientConnected = callback;
    }

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
        acceptThread.setDaemon(true);
        acceptThread.start();

        System.out.println("[server] listening on port " + this.port);
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;

        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // ignore
        }
        for (Socket s : activeClients) {
            try {
                s.close();
            } catch (IOException ignored) {
                // ignore
            }
        }
        activeClients.clear();
        activeHandlers.clear();

        clientPool.shutdownNow();
        try {
            clientPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

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

    /** Push-сообщение всем активным клиентам. */
    public void broadcastPush(String message) {
        if (message == null) {
            return;
        }
        for (ClientHandler handler : activeHandlers) {
            handler.sendPush(message);
        }
    }

    public int getActiveClientCount() {
        return activeHandlers.size();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                activeClients.add(client);
                CommandExecutor executor = executorFactory.createForConnection();

                clientPool.submit(() -> {
                    ClientHandler handler = new ClientHandler(
                            client,
                            executor,
                            registered -> {
                                activeHandlers.add(registered);
                                Consumer<ClientHandler> userCallback = onClientConnected;
                                if (userCallback != null) {
                                    try {
                                        userCallback.accept(registered);
                                    } catch (RuntimeException e) {
                                        System.err.println(
                                                "[server] onClientConnected callback failed: "
                                                        + e.getMessage());
                                    }
                                }
                            }
                    );
                    try {
                        handler.run();
                    } finally {
                        activeHandlers.remove(handler);
                        activeClients.remove(client);
                    }
                });
            } catch (SocketException e) {
                if (!running) {
                    break;
                }
                System.err.println("[server] accept socket error: " + e.getMessage());
            } catch (IOException e) {
                if (!running) {
                    break;
                }
                System.err.println("[server] accept I/O error: " + e.getMessage());
            }
        }
    }

    /**
     * CLI-запуск:
     * <pre>
     *   java ... DBServer --port 5555
     *   java ... DBServer --port 7000 --shard-id 0
     * </pre>
     *
     * <p>Флаг {@code --shard-id N} включает {@link ShardCommandTranslator}
     * (становятся доступны {@code _}-команды для Router'а и меняется метка
     * в логах). Без флага используется обычный {@link CommandTranslator}
     * через конструктор {@link #DBServer(GraphStorage, MetricRegistry, PathFinder)}.
     */
    public static void main(String[] args) throws IOException {
        int port = 5555;
        int shardId = -1;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    if (i + 1 >= args.length) {
                        System.err.println("--port requires a value");
                        return;
                    }
                    port = Integer.parseInt(args[++i]);
                    break;
                case "--shard-id":
                    if (i + 1 >= args.length) {
                        System.err.println("--shard-id requires a value");
                        return;
                    }
                    shardId = Integer.parseInt(args[++i]);
                    break;
                default:
                    System.err.println("Unknown option: " + args[i]);
                    return;
            }
        }

        GraphStorage storage = new InMemoryGraphStorage();
        MetricRegistry metrics = new MetricRegistry();
        PathFinder pathFinder = new SimpleDijkstraPathFinder();

        String label = (shardId >= 0) ? "[shard-" + shardId + "]" : "[node]";
        System.out.println(label + " starting");

        final int finalShardId = shardId;
        DBServer server = new DBServer(
                () -> new ShardCommandTranslator(storage, metrics, pathFinder, finalShardId)
        );
        server.start(port);

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