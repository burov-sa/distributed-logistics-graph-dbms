package ru.itmo.graphdb.sharding;

import java.util.Objects;

/**
 * Описание одного шарда: логический id + сетевой адрес.
 *
 * <p>Иммутабельный value-объект. Используется:
 * <ul>
 *   <li>{@link ShardRouter} — как элемент топологии;</li>
 *   <li>Router'ом (шаг 3) — как endpoint для установления соединения;</li>
 *   <li>клиентом — при получении {@code SHARD_MAP} через push.</li>
 * </ul>
 *
 * <p>Текстовое представление — {@code "<shardId>=<host>:<port>"}, удобное для
 * конфигурации через CLI и для wire-формата push-сообщения.
 */
public record ShardEndpoint(int shardId, String host, int port) {

    public ShardEndpoint {
        if (shardId < 0) {
            throw new IllegalArgumentException("shardId must be >= 0, got: " + shardId);
        }
        Objects.requireNonNull(host, "host");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port must be in [1, 65535], got: " + port);
        }
    }

    /** Текстовое представление для конфигов и wire-формата: {@code "0=localhost:7000"}. */
    public String asText() {
        return shardId + "=" + host + ":" + port;
    }

    /**
     * Разбор строки вида {@code "0=localhost:7000"}.
     *
     * @throws IllegalArgumentException при неверном формате
     */
    public static ShardEndpoint fromText(String text) {
        Objects.requireNonNull(text, "text");
        int eq = text.indexOf('=');
        int colon = text.lastIndexOf(':');
        if (eq <= 0 || colon <= eq + 1) {
            throw new IllegalArgumentException("Malformed shard endpoint: " + text);
        }
        int shardId = Integer.parseInt(text.substring(0, eq));
        String host = text.substring(eq + 1, colon);
        int port = Integer.parseInt(text.substring(colon + 1));
        return new ShardEndpoint(shardId, host, port);
    }
}