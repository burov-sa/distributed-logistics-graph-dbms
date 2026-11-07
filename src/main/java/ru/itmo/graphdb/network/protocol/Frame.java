package ru.itmo.graphdb.network.protocol;

import java.util.Objects;

/**
 * Один кадр протокола: тип + текстовый payload.
 *
 * <p>Иммутабельный value-объект. Payload — произвольная строка (команда,
 * ответ или push-уведомление), кодируется через {@code writeUTF/readUTF}.
 *
 * <p>Фабричные методы {@link #request}/{@link #response}/{@link #push}
 * убирают риск перепутать тип при создании кадра на стороне клиента или сервера.
 */
public record Frame(FrameType type, String payload) {

    public Frame {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(payload, "payload");
    }

    /** Кадр клиентского запроса. */
    public static Frame request(String payload) {
        return new Frame(FrameType.REQ, payload);
    }

    /** Кадр ответа на конкретный запрос. */
    public static Frame response(String payload) {
        return new Frame(FrameType.RESP, payload);
    }

    /** Кадр асинхронного уведомления сервер → клиент. */
    public static Frame push(String payload) {
        return new Frame(FrameType.PUSH, payload);
    }
}