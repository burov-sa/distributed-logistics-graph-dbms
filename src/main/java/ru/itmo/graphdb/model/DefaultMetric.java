package ru.itmo.graphdb.model;
import java.util.Arrays;
import java.util.Optional;

/**
 * Перечисление дефолтных метрик (для транспортной логистики) связей графа.
 */
public enum DefaultMetric implements Metric{

    DISTANCE("distance",    "Distance"),
    TIME   ("time",   "Time"),
    COST   ("cost",     "Cost");

    private final String key;
    private final String displayName;

    DefaultMetric(String key, String displayName) {
        this.key = key;
        this.displayName = displayName;
    }

    @Override public String key() { return key; }
    @Override public String displayName() { return displayName; }

    /** Поиск дефолта по ключу. Возвращает Optional, потоому что ключа может не быть. */
    public static Optional<DefaultMetric> byKey(String key) {
        return Arrays   .stream(values())
                        .filter(p -> p.key.equals(key))
                        .findFirst();
    }
}