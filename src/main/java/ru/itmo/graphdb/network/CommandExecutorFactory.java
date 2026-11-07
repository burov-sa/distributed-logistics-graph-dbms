package ru.itmo.graphdb.network;

import ru.itmo.graphdb.client.CommandExecutor;

/**
 * Фабрика исполнителей команд на одно клиентское соединение.
 *
 * <p>Зачем фабрика, а не один общий {@link CommandExecutor}:
 * <ul>
 *   <li>часть состояния транслятора привязана к сессии и не должна шариться
 *       между клиентами (например, {@code currentDbName});</li>
 *   <li>будущий Router-слой и Slave-режим могут подставлять свои реализации
 *       ({@code RouterCommandTranslator}, read-only транслятор) без правок
 *       {@link DBServer}.</li>
 * </ul>
 *
 * <p>Фабрика вызывается в accept-loop для каждого нового соединения,
 * поэтому реализация должна быть дешёвой и потокобезопасной.
 */
@FunctionalInterface
public interface CommandExecutorFactory {

    /** Создаёт исполнителя для одного нового клиентского соединения. */
    CommandExecutor createForConnection();
}