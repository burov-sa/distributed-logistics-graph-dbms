package ru.itmo.graphdb.network.protocol;

/**
 * Тип кадра в протоколе обмена между клиентом и сервером.
 *
 * <p>Инкапсулирует однобайтовый код, чтобы не размазывать литералы по
 * {@link Frames} и {@link ru.itmo.graphdb.network.ClientHandler}.
 *
 * <p>Почему отдельный тип кадра, а не «просто UTF-туда-обратно»:
 * сокет двунаправленный, и серверу нужно уметь в любой момент сеанса
 * отправить клиенту асинхронное уведомление ({@link #PUSH}), не ломая
 * цикл «один запрос — один ответ». Тип кадра отделяет ответ на конкретный
 * запрос от внеполосного сообщения.
 */
public enum FrameType {

    /** Клиент → сервер: одна команда. */
    REQ((byte) 1),

    /** Сервер → клиент: ответ на конкретный {@link #REQ}. */
    RESP((byte) 2),

    /** Сервер → клиент: асинхронное сообщение вне цикла запрос-ответ. */
    PUSH((byte) 3);

    private final byte code;

    FrameType(byte code) {
        this.code = code;
    }

    /** Байтовый код для сериализации в сокет. */
    public byte code() {
        return code;
    }

    /**
     * Обратное преобразование по коду.
     *
     * @throws IllegalArgumentException если код не принадлежит известным типам
     */
    public static FrameType fromCode(byte code) {
        for (FrameType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown frame type code: " + code);
    }
}