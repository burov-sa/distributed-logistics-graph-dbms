package ru.itmo.graphdb.network;

import ru.itmo.graphdb.client.CommandTranslator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * Обслуживает одно TCP-соединение.
 *
 * <p>Протокол: один кадр {@code readUTF} → {@code translator.execute()} → один кадр
 * {@code writeUTF}. Соединение долгоживущее, запросы идут строго по одному.
 *
 * <p>Каждое соединение получает свой {@link CommandTranslator}, чтобы
 * {@code currentDbName} не «протекал» между клиентами. При этом {@code GraphStorage},
 * {@code MetricRegistry} и {@code PathFinder} шарятся — это состояние всей ноды.
 */
public class ClientHandler implements Runnable {

    /**
     * Idle-таймаут: если клиент молчит 30 минут, закрываем соединение. Сделал для того, чтобы
     * возможно мёртвый клиент не занимал поток пула бесконечно.
     */
    private static final int IDLE_TIMEOUT_MS = 30 * 60 * 1000;

    private final Socket socket;
    private final CommandTranslator translator;

    public ClientHandler(Socket socket, CommandTranslator translator) {
        this.socket = socket;
        this.translator = translator;
    }

    @Override
    public void run() {
        String remote = String.valueOf(socket.getRemoteSocketAddress());

        try (Socket s = socket) {
            // Установка таймаута на чтение. Без него readUTF() будет висеть, пока клиент
            // физически не закроет соединение, что может и не случится.
            s.setSoTimeout(IDLE_TIMEOUT_MS);

            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());

            System.out.println("[server] client connected: " + remote);

            while (true) {
                String line;
                try {
                    line = in.readUTF();
                } catch (EOFException e) {
                    // нормальное закрытие клиентом
                    System.out.println("[server] client disconnected: " + remote);
                    return;
                } catch (SocketTimeoutException e) {
                    System.out.println("[server] idle timeout: " + remote);
                    return;
                } catch (SocketException e) {
                    // например, сервер делает stop() и закрывает сокет
                    System.out.println("[server] socket closed: " + remote);
                    return;
                }

                // CommandTranslator.execute() никогда не бросает, упаковывает
                // ошибку в "ERROR: ...".
                String response = translator.execute(line);

                out.writeUTF(response);
                out.flush();
            }
        } catch (IOException e) {
            System.out.println("[server] I/O error with " + remote + ": " + e.getMessage());
        }
    }
}
