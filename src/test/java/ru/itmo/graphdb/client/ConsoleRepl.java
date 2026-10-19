package ru.itmo.graphdb.client;

import ru.itmo.graphdb.model.MetricRegistry;
import ru.itmo.graphdb.routing.SimpleDijkstraPathFinder;
import ru.itmo.graphdb.storage.GraphStorage;
import ru.itmo.graphdb.storage.InMemoryGraphStorage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Минимальный консольный REPL для ручной проверки словаря команд.
 *
 * <p>Читает строки из stdin, передаёт их в {@link CommandTranslator#execute(String)}
 * и печатает ответ. Работает до {@code exit}, {@code quit} или EOF (Ctrl+D).
 *
 * <p>Для многословных значений (например, displayName метрики с пробелом)
 * кавычки не поддерживаются — этот REPL намеренно простой. Если понадобится,
 * добавь разбор кавычек вручную.
 */
public class ConsoleRepl {

    public static void main(String[] args) throws IOException {
        GraphStorage storage   = new InMemoryGraphStorage();
        MetricRegistry metrics = new MetricRegistry();
        SimpleDijkstraPathFinder pathFinder = new SimpleDijkstraPathFinder();

        CommandTranslator cli = new CommandTranslator(storage, metrics, pathFinder);

        System.out.println("GraphDB REPL. Введите команду или 'help'. 'exit' - выход.");

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            System.out.print("> ");
            System.out.flush();

            String line = in.readLine();
            if (line == null) {          // EOF (Ctrl+D / Ctrl+Z)
                System.out.println();
                break;
            }

            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            String lower = trimmed.toLowerCase();
            if (lower.equals("exit") || lower.equals("quit")) {
                break;
            }
            if (lower.equals("help")) {
                printHelp();
                continue;
            }

            try {
                String response = cli.execute(trimmed);
                System.out.println(response);
            } catch (RuntimeException e) {
                // execute() сам ловит исключения, но подстрахуемся — REPL не должен падать.
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
                  help
                  exit | quit
                """);
    }
}
