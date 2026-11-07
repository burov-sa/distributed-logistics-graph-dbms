package ru.itmo.graphdb.network;

import ru.itmo.graphdb.client.CommandExecutor;
import ru.itmo.graphdb.network.protocol.Frame;
import ru.itmo.graphdb.network.protocol.Frames;
import ru.itmo.graphdb.network.protocol.FrameType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Обслуживает одно TCP-соединение.
 *
 * <p>Протокол: кадры {@link ru.itmo.graphdb.network.protocol.Frame}
 * ({@code [type][UTF payload]}). Клиент присылает {@code REQ}, сервер отвечает
 * {@code RESP}. Сервер в любой момент сеанса может отправить {@code PUSH}
 * — клиент обязан его корректно принять и не спутать с ответом.
 *
 * <p>Соединение долгоживущее, в полёте всегда ровно один {@code REQ} на клиента
 * (это гарантирует {@code GraphDBClient.execute} через {@code synchronized}).
 *
 * <p>Каждое соединение получает свой {@link CommandExecutor}, чтобы состояние
 * сессии (например, {@code currentDbName} в {@code CommandTranslator}) не
 * «протекало» между клиентами. Общее состояние ноды
 * ({@code GraphStorage}, {@code MetricRegistry}, {@code PathFinder}) шарятся
 * через фабрику исполнителей.
 *
 * <p>Жизненный цикл: {@code onConnect.accept(this)} вызывается один раз после
 * инициализации потоков и до входа в read-loop — это окно для серверной
 * инициализации соединения (например, push {@code SHARD_MAP} на Router).
 * После выхода из {@code run()} сокет закрывается, ссылка на выходной поток
 * обнуляется, а хэндлер удаляется из реестра сервером.
 */
public class ClientHandler implements Runnable {

    /**
     * Idle-таймаут: если клиент молчит 30 минут, закрываем соединение.
     * Нужен, чтобы «мёртвый» клиент не занимал поток пула бесконечно.
     */
    private static final int IDLE_TIMEOUT_MS = 30 * 60 * 1000;

    private final Socket socket;
    private final CommandExecutor commandExecutor;
    private final Consumer<ClientHandler> onConnect;

    /**
     * Выходной поток. Инициализируется в {@link #run()} до вызова {@code onConnect},
     * обнуляется в {@code finally} после выхода из цикла — под {@link #writeLock}.
     * Может быть {@code null}, если {@link #sendPush(String)} пришёл до инициализации
     * или после завершения — в этом случае push молча отбрасывается.
     */
    private DataOutputStream out;

    /**
     * Сериализует запись кадров в сокет между:
     * <ul>
     *   <li>потоком-обработчиком, пишущим {@code RESP};</li>
     *   <li>любым потоком, вызывающим {@link #sendPush(String)} (обычно
     *       {@code DBServer.broadcastPush}).</li>
     * </ul>
     * Без лока байты двух кадров могли бы перемешаться на проводе.
     */
    private final Object writeLock = new Object();

    /** Старый конструктор без onConnect — сохранён для обратной совместимости. */
    public ClientHandler(Socket socket, CommandExecutor commandExecutor) {
        this(socket, commandExecutor, null);
    }

    public ClientHandler(Socket socket,
                         CommandExecutor commandExecutor,
                         Consumer<ClientHandler> onConnect) {
        this.socket = Objects.requireNonNull(socket, "socket");
        this.commandExecutor = Objects.requireNonNull(commandExecutor, "commandExecutor");
        this.onConnect = onConnect;
    }

    @Override
    public void run() {
        String remote = String.valueOf(socket.getRemoteSocketAddress());

        try (Socket s = socket) {
            // Без таймаута read-кадр будет висеть, пока клиент физически не закроет
            // соединение, что может и не случиться.
            s.setSoTimeout(IDLE_TIMEOUT_MS);

            // КРИТИЧНО для request-response протокола:
            // отключаем алгоритм Нейгла. Иначе writeByte(type) + writeUTF(payload)
            // (3 маленьких сисколла на кадр) задерживаются на ~40 мс из-за
            // Nagle + Delayed ACK. При шардированном PATH_EDGE с десятком
            // round-trip'ов на запрос это критично.
            s.setTcpNoDelay(true);

            // Буферизация: writeByte + writeUTF идут в память, flush() отдаёт
            // кадр одним TCP-сегментом. В паре с TCP_NODELAY убирает
            // фрагментацию кадра и лишние сисколлы.
            DataInputStream in = new DataInputStream(
                    new BufferedInputStream(s.getInputStream()));
            DataOutputStream outStream = new DataOutputStream(
                    new BufferedOutputStream(s.getOutputStream()));

            // out выставляем до onConnect, чтобы callback мог отправлять push.
            synchronized (writeLock) {
                this.out = outStream;
            }

            System.out.println("[server] client connected: " + remote);

            if (onConnect != null) {
                try {
                    onConnect.accept(this);
                } catch (RuntimeException e) {
                    // callback — инфраструктурный; его падение не должно рвать сеанс.
                    System.err.println("[server] onConnect callback failed for "
                            + remote + ": " + e.getMessage());
                }
            }

            while (true) {
                Frame frame;
                try {
                    frame = Frames.read(in);
                } catch (EOFException e) {
                    System.out.println("[server] client disconnected: " + remote);
                    return;
                } catch (SocketTimeoutException e) {
                    System.out.println("[server] idle timeout: " + remote);
                    return;
                } catch (SocketException e) {
                    // Сюда же попадает server.stop(): сокет закрывают извне.
                    System.out.println("[server] socket closed: " + remote);
                    return;
                }

                if (frame.type() != FrameType.REQ) {
                    // По протоколу клиент шлёт только REQ. Иное — либо ошибка
                    // клиента, либо мусор после реконнекта. Игнорируем, не падая.
                    System.err.println("[server] unexpected frame type from "
                            + remote + ": " + frame.type());
                    continue;
                }

                // CommandExecutor.execute никогда не бросает, упаковывает
                // ошибку в "ERROR: ...".
                String response = commandExecutor.execute(frame.payload());

                synchronized (writeLock) {
                    if (this.out != null) {
                        Frames.write(this.out, Frame.response(response));
                        this.out.flush();
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("[server] I/O error with " + remote + ": " + e.getMessage());
        } finally {
            synchronized (writeLock) {
                this.out = null;
            }
        }
    }

    /**
     * Отправляет клиенту асинхронное push-уведомление.
     *
     * <p>Может вызываться из любого потока. Если соединение ещё не
     * инициализировано или уже закрыто, либо запись упала — сообщение
     * молча отбрасывается (DBServer удалит хэндлер из реестра по завершении
     * {@link #run()}).
     */
    public void sendPush(String message) {
        if (message == null) {
            return;
        }
        synchronized (writeLock) {
            if (out == null) {
                return;
            }
            try {
                Frames.write(out, Frame.push(message));
                out.flush();
            } catch (IOException ignored) {
                // Соединение отвалилось; зарегистрированный обработчик
                // удалится из реестра по завершении run().
            }
        }
    }
}