package ru.itmo.graphdb.network;

import ru.itmo.graphdb.client.CommandExecutor;
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
 * <p>Двунаправленность: помимо ответов на запросы сервер умеет асинхронно
 * пушить сообщения всем живым клиентам через {@link #broadcastPush(String)},
 * а на этапе подключения — вызывать пользовательский callback
 * {@code setOnClientConnected(Consumer)} (например, чтобы отправить
 * {@code SHARD_MAP} на Router-слое).
 */
public class DBServer {

    /**
     * Пул под обработчики соединений. Фиксированный, чтобы один клиент
     * с сотней открытых сокетов не положил сервер.
     */
    private static final int CLIENT_POOL_SIZE = 8;

    private final CommandExecutorFactory executorFactory;

    private final ExecutorService clientPool =
            Executors.newFixedThreadPool(CLIENT_POOL_SIZE);

    /**
     * Трекинг активных клиентских сокетов — нужен, чтобы {@link #stop()}
     * мог их принудительно закрыть. Иначе {@code read} в обработчиках
     * зависнет на 30-минутном idle-таймауте, и JVM не завершится
     * (потоки пула не daemon).
     */
    private final Set<Socket> activeClients = ConcurrentHashMap.newKeySet();

    /**
     * Трекинг активных {@link ClientHandler} — нужен для {@link #broadcastPush(String)}.
     * Хэндлер попадает сюда в onConnect и удаляется по завершении {@code run()}.
     */
    private final Set<ClientHandler> activeHandlers = ConcurrentHashMap.newKeySet();

    /**
     * Пользовательский callback на подключение клиента. Может быть {@code null}.
     * Устанавливается до {@link #start(int)}; читается из accept-потока.
     */
    private volatile Consumer<ClientHandler> onClientConnected;

    private volatile boolean running = false;
    private ServerSocket serverSocket;
    private Thread acceptThread;
    private int port = -1;

    /**
     * Обратно совместимый конструктор: общее состояние ноды передаётся
     * напрямую, фабрика строит per-connection {@link CommandTranslator}
     * поверх этих же ссылок. Используется текущим {@link #main(String[])}
     * и существующими тестами.
     */
    public DBServer(GraphStorage storage, MetricRegistry metrics, PathFinder pathFinder) {
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(metrics, "metrics");
        Objects.requireNonNull(pathFinder, "pathFinder");
        this.executorFactory = () -> new CommandTranslator(storage, metrics, pathFinder);
    }

    /**
     * Конструктор с фабрикой: даёт полный контроль над тем, какой
     * {@link CommandExecutor} получит каждое соединение (например, для
     * Router-слоя или read-only режима на Slave).
     */
    public DBServer(CommandExecutorFactory executorFactory) {
        this.executorFactory = Objects.requireNonNull(executorFactory, "executorFactory");
    }

    /**
     * Регистрирует пользовательский callback, вызываемый один раз на каждое
     * успешно установленное соединение, до входа в read-loop. Внутри callback
     * можно, например, отправить клиенту push с топологией шардов.
     *
     * <p>Callback не должен бросать наружу — исключения логируются и
     * игнорируются.
     */
    public void setOnClientConnected(Consumer<ClientHandler> callback) {
        this.onClientConnected = callback;
    }

    /**
     * Запускает сервер. Возвращает управление сразу после открытия сокета.
     * {@code port = 0} — ОС выберет свободный порт, фактический даст
     * {@link #getPort()}.
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
        if (!running) {
            return;
        }
        running = false;

        // 1) Закрываем ServerSocket — это разбудит accept() с SocketException.
        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // ignore
        }

        // 2) Закрываем все активные клиентские сокеты — это разбудит чтение
        //    кадра в обработчиках. Без этого они будут висеть до idle-таймаута.
        for (Socket s : activeClients) {
            try {
                s.close();
            } catch (IOException ignored) {
                // ignore
            }
        }
        activeClients.clear();
        activeHandlers.clear();

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

    /**
     * Отправляет push-сообщение всем активным клиентам.
     *
     * <p>Каждый {@link ClientHandler} сериализует запись через свой
     * {@code writeLock}, поэтому одновременный {@code broadcastPush} из
     * нескольких потоков не перемешает байты. Медленный клиент может
     * задержать остальных — для MVP приемлемо; в проде понадобился бы
     * per-client исходящий буфер и отдельный sender-поток.
     *
     * @param message текстовая нагрузка (не {@code null}); {@code null} игнорируется
     */
    public void broadcastPush(String message) {
        if (message == null) {
            return;
        }
        for (ClientHandler handler : activeHandlers) {
            handler.sendPush(message);
        }
    }

    /** Текущее число подключённых клиентов — для health-check / метрик. */
    public int getActiveClientCount() {
        return activeHandlers.size();
    }

    // ---- внутреннее ----

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
                // serverSocket.close() во время stop() — это штатно.
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

    /** CLI-запуск: {@code java ... DBServer --port 5555}. */
    public static void main(String[] args) throws IOException {
        int port = 5555;
        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
            }
        }

        GraphStorage storage = new InMemoryGraphStorage();
        MetricRegistry metrics = new MetricRegistry();
        PathFinder pathFinder = new SimpleDijkstraPathFinder();

        // Фабрика замыкает общее состояние ноды и создаёт per-connection
        // CommandTranslator с изолированным currentDbName.
        DBServer server = new DBServer(
                () -> new CommandTranslator(storage, metrics, pathFinder)
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