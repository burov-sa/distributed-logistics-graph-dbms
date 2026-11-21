package ru.itmo.graphdb.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * REPL для ручной проверки Router'а через {@link GraphDBClient}.
 *
 * <p>В отличие от {@link ru.itmo.graphdb.client.ConsoleRepl} (работает поверх
 * {@code CommandTranslator} и ходит в одну ноду), этот REPL говорит по TCP
 * с Router'ом — а значит, идёт через весь кластер шардов.
 *
 * <p>Умеет:
 * <ul>
 *   <li>отправлять любую команду СУБД ({@code ADD_NODE}, {@code PATH_EDGE} …);</li>
 *   <li>{@code SHARD <id>} — локально посчитать {@code floorMod(hashCode(id), N)},
 *       чтобы визуально показать, на каком шарде живёт узел;</li>
 *   <li>{@code exit} — выйти из REPL.</li>
 * </ul>
 * Push-сообщения ({@code SHARD_MAP} и любые будущие события репликации)
 * печатаются с префиксом {@code [PUSH]}.
 *
 * <p>Запуск: {@code main("localhost", "5555", "2")} — host, port, число шардов.
 */
public class RouterConsoleRepl {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5555;
        int shards = args.length > 2 ? Integer.parseInt(args[2]) : 2;

        try (GraphDBClient client = new GraphDBClient(host, port)) {

            client.setPushListener(msg -> System.out.println("[PUSH] " + msg));

            System.out.println("Router REPL connected to " + host + ":" + port);
            System.out.println("Shards in cluster: " + shards);
            System.out.println("Commands: <any DB command> | SHARD <id> | exit");
            System.out.println("--------------------------------------------");

            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            String line;
            while ((line = in.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if ("exit".equalsIgnoreCase(trimmed) || "quit".equalsIgnoreCase(trimmed)) {
                    break;
                }

                // Локальный помощник: на каком шарде живёт узел.
                if (trimmed.toUpperCase().startsWith("SHARD ")) {
                    String id = trimmed.substring("SHARD ".length()).trim();
                    int shard = Math.floorMod(id.hashCode(), shards);
                    System.out.println("  shard[" + id + "] = " + shard
                            + "  (hash=" + id.hashCode() + ", N=" + shards + ")");
                    continue;
                }

                // Обычная команда — уходит на Router.
                String response = client.execute(trimmed);
                System.out.println(response);
            }
            System.out.println("Bye.");
        }
    }
}