package ru.itmo.graphdb.client;

import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.storage.GraphStorage;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.function.Function;

/**
 * REPL для ручной проверки словаря команд.
 *
 * <p>Два режима:
 * <ul>
 *   <li>локальный (по умолчанию): {@code CommandTranslator} + {@code InMemoryGraphStorage};</li>
 *   <li>удалённый: {@code java ... ConsoleRepl --remote --host localhost --port 5555}
 *       — работает через {@link GraphDBClient}.</li>
 * </ul>
 * Словарь команд и вывод для локального и удалённого режима идентичны - это демонстрация того, что контракт
 * {@code execute(String)} не зависит от транспорта.
 */
public class ConsoleRepl {

    public static void main(String[] args) throws IOException {
        boolean remote = false;
        String host = "localhost";
        int port = 5555;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--remote" -> remote = true;
                case "--host"   -> host = args[++i];
                case "--port"   -> port = Integer.parseInt(args[++i]);
                default         -> {
                    System.err.println("unknown arg: " + args[i]);
                    return;
                }
            }
        }

        if (remote) {
            GraphDBClient client = new GraphDBClient(host, port);
            System.out.println("GraphDB REPL (remote " + host + ":" + port + "). 'exit' — выход.");
            try {
                runRepl(client::execute);
            } finally {
                client.close();
            }
        } else {
            GraphStorage storage = new InMemoryGraphStorage();
            MetricRegistry metrics = new MetricRegistry();
            SimpleDijkstraPathFinder pathFinder = new SimpleDijkstraPathFinder();
            CommandTranslator cli = new CommandTranslator(storage, metrics, pathFinder);
            System.out.println("GraphDB REPL (local). 'exit' — выход.");
            runRepl(cli::execute);
        }
    }

    private static void runRepl(Function<String, String> executeFn) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("> ");
            System.out.flush();

            String line = in.readLine();
            if (line == null) { System.out.println(); break; }

            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;

            String lower = trimmed.toLowerCase();
            if (lower.equals("exit") || lower.equals("quit")) break;
            if (lower.equals("help")) { printHelp(); continue; }

            try {
                System.out.println(executeFn.apply(trimmed));
            } catch (RuntimeException e) {
                System.out.println("ERROR: " + e.getMessage());
            }
        }
        System.out.println("Bye.");
    }

    private static void printHelp() {
        System.out.println("""
                Команды:
                  CREATE_DB <name_db>
                  READ_DB <name_db>
                  SAVE_DB <name_db>
                  ADD_METRIC <metric> [display_name]
                  DELETE_METRIC <metric>
                  PRINT_METRICS
                  ADD_NODE <name_node>
                  SET_PARAMETER <name_node> <name_parameter> <value_parameter>
                  DELETE_NODE <name_node>
                  ADD_EDGE <source> <destination> <metric> <value>
                  PRINT_EDGES <name_node>
                  PRINT_EDGES <name_node> <name_node>
                  DELETE_EDGE <id_edge>
                  PATH_EDGE <source> <destination> <metric>
                  help | exit | quit
                """);
    }
}