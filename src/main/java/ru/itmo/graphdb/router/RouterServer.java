package ru.itmo.graphdb.router;

import ru.itmo.graphdb.client.GraphDBClient;
import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.network.DBServer;
import ru.itmo.graphdb.routing.PathFinder;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.sharding.ShardEndpoint;
import ru.itmo.graphdb.sharding.ShardRouter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/**
 * Router-нода: единственная точка входа для клиентов.
 *
 * <p>Композиция:
 * <ul>
 *   <li>{@link DBServer} — сетевой слой (accept-loop, пул, push),</li>
 *   <li>{@link ShardRouter} — раскладка узлов по шардам,</li>
 *   <li>по одному {@link GraphDBClient} на шард — для команд вида
 *       {@code ADD_NODE}/{@code _ADD_EDGE_RAW}/…,</li>
 *   <li>{@link ShardedGraphStorage} — «распределённый» {@code GraphStorage}
 *       для {@link PathFinder},</li>
 *   <li>{@link MetricRegistry} и {@link PathFinder} — Router-локальные
 *       (метрики синхронизируются на шарды через fan-out в
 *       {@link RouterCommandTranslator}).</li>
 * </ul>
 *
 * <p><b>Push на коннект.</b> При подключении клиента Router отправляет
 * push {@code SHARD_MAP <id>=<host>:<port>;…} через
 * {@code DBServer.setOnClientConnected}. Используется тот же механизм,
 * что будет использоваться для событий репликации (этап 4).
 *
 * <p><b>Топология фиксирована на старте.</b> Динамическое добавление/удаление
 * шардов (rebalancing, consistent hashing) — future work; после изменения
 * топологии потребовался бы broadcast нового {@code SHARD_MAP} уже
 * подключённым клиентам (инфраструктура push для этого готова).
 */
public class RouterServer implements AutoCloseable {

    private final ShardRouter router;
    private final Map<Integer, GraphDBClient> shardClients;
    private final DBServer server;

    /**
     * @param router топология шардов; endpoint'ы должны быть доступны
     *               (или станут доступны к моменту первого запроса —
     *               {@link GraphDBClient} подключается лениво)
     */
    public RouterServer(ShardRouter router) {
        this.router = Objects.requireNonNull(router, "router");
        this.shardClients = new LinkedHashMap<>();
        for (ShardEndpoint ep : router.allShards()) {
            shardClients.put(ep.shardId(), new GraphDBClient(ep.host(), ep.port()));
        }

        MetricRegistry metrics = new MetricRegistry();
        ShardedGraphStorage shardedStorage = new ShardedGraphStorage(router, shardClients);
        PathFinder pathFinder = new SimpleDijkstraPathFinder();

        this.server = new DBServer(
                () -> new RouterCommandTranslator(
                        router, shardClients, shardedStorage, metrics, pathFinder)
        );
        this.server.setOnClientConnected(
                handler -> handler.sendPush(router.topologyAsText())
        );
    }

    /** Запуск на указанном порту; {@code 0} — ОС выберет свободный. */
    public void start(int port) throws IOException {
        server.start(port);
        System.out.println("[router] " + router.topologyAsText());
    }

    public void stop() {
        server.stop();
    }

    public int getPort() {
        return server.getPort();
    }

    public boolean isRunning() {
        return server.isRunning();
    }

    /**
     * Останавливает сервер и закрывает соединения с шардами.
     * {@link DBServer#stop()} дожидается завершения обработчиков клиентов,
     * поэтому шард-клиенты закрываются уже после того, как последние
     * in-flight команды отработали.
     */
    @Override
    public void close() {
        server.stop();
        for (GraphDBClient c : shardClients.values()) {
            c.close();
        }
    }

    /**
     * CLI:
     * <pre>
     *   java ... RouterServer --port 5555 \
     *       --shard 0=localhost:7000 \
     *       --shard 1=localhost:7001 \
     *       --shard 2=localhost:7002
     * </pre>
     * Флаг {@code --shard} повторяемый; минимум один обязателен.
     * Формат значения — {@code <shardId>=<host>:<port>} (см. {@link ShardEndpoint#fromText}).
     */
    public static void main(String[] args) throws IOException {
        int port = 5555;
        List<ShardEndpoint> shards = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> {
                    if (i + 1 >= args.length) {
                        System.err.println("--port requires a value");
                        return;
                    }
                    port = Integer.parseInt(args[++i]);
                }
                case "--shard" -> {
                    if (i + 1 >= args.length) {
                        System.err.println("--shard requires a value");
                        return;
                    }
                    shards.add(ShardEndpoint.fromText(args[++i]));
                }
                default -> {
                    System.err.println("Unknown option: " + args[i]);
                    return;
                }
            }
        }

        if (shards.isEmpty()) {
            System.err.println(
                    "At least one --shard <id>=<host>:<port> is required");
            return;
        }

        ShardRouter router = new ShardRouter(shards);
        RouterServer server = new RouterServer(router);
        server.start(port);
        System.out.println("[router] listening on port " + server.getPort());

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            shutdown.countDown();
        }, "router-shutdown"));

        try {
            shutdown.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}