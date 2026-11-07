package ru.itmo.graphdb.client;

/**
 * Контракт «что-то, что умеет выполнять одну текстовую команду и возвращать
 * текстовый ответ».
 *
 * <p>Введён, чтобы:
 * <ul>
 *   <li>развязать сетевой слой ({@code ClientHandler}) и конкретную реализацию
 *       парсера/исполнителя команд ({@link CommandTranslator});</li>
 *   <li>дать единый тип для будущих реализаций — например, шардирующего
 *       {@code RouterCommandTranslator} или read-only транслятора на Slave;</li>
 *   <li>позволить {@code DBServer} принимать фабрику исполнителей вместо
 *       жёстко зашитого {@link CommandTranslator}.</li>
 * </ul>
 *
 * <p>Реализация обязана соблюдать контракт {@link CommandTranslator#execute(String)}:
 * не бросать исключения наружу, упаковывать ошибки в строку с префиксом
 * {@code "ERROR: ..."}.
 */
@FunctionalInterface
public interface CommandExecutor {

    /**
     * Выполняет одну команду.
     *
     * @param line исходная строка команды (не {@code null})
     * @return человекочитаемый ответ; ошибки — строка с префиксом {@code "ERROR: "}
     */
    String execute(String line);
}