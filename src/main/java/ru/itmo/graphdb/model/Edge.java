package ru.itmo.graphdb.model;

import lombok.Getter;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;

/**
 * Представляет направленное ребро графа (транспортный маршрут между лог.объектами).
 * Хранит физические и экономические характеристики перемещения.
 *
 * <p>Все веса - и дефолтные ({@link DefaultMetric}), и пользовательские
 * ({@link CustomMetric}) - хранятся единообразно в карте {@code weights}
 * по ключу метрики ({@link Metric#key()}).
 */
@Getter
public class Edge {

    private final String id;
    private final Node source;
    private final Node destination;

    /**
     * Значения весов по ключу метрики.
     * Для дефолтных метрик ключи — {@code "time"}, {@code "distance"}, {@code "cost"}.
     * Хранится в неизменяемой LinkedHashMap, чтобы порядок добавления сохранялся.
     */
    private final Map<String, Double> weights;

    /**
     * Адпатированный под транспортную логистику конструктор для случая, когда на ребре есть ровно три дефолтных веса.
     * Использует ключи из {@link DefaultMetric}.
     */
    public Edge(String id, Node source, Node destination,
                double time, double distance, double cost) {
        this(id, source, destination, Map.of(
                DefaultMetric.TIME.key(),     time,
                DefaultMetric.DISTANCE.key(), distance,
                DefaultMetric.COST.key(),     cost
        ));
    }

    /**
     * Полный конструктор: любые метрики (дефолтные и кастомные) через карту.
     * Ключ — {@link Metric#key()}.
     */
    public Edge(String id, Node source, Node destination,
                Map<String, Double> weights) {
        this.id = Objects.requireNonNull(id, "id");
        this.source = Objects.requireNonNull(source, "source");
        this.destination = Objects.requireNonNull(destination, "destination");
        // Копия в неизменяемую мапу — состояние ребра нельзя изменить извне.
        this.weights = Collections.unmodifiableMap(
                new LinkedHashMap<>(Objects.requireNonNull(weights, "weights")));
    }

    /**
     * Возвращает вес ребра по выбранной метрике.
     * Используется алгоритмами поиска путей для мультикритериальной маршрутизации.
     *
     * @param metric критерий оптимизации (дефолтный или пользовательский)
     * @return численное значение веса маршрута
     * @throws IllegalArgumentException если для этой метрики на ребре нет значения
     */
    public double getWeight(Metric metric) {
        Objects.requireNonNull(metric, "metric");
        Double value = weights.get(metric.key());
        if (value == null) {
            throw new IllegalArgumentException(
                    "No value for metric '" + metric.key() + "' on edge " + id);
        }
        return value;
    }

    /**
     * Безопасный вариант {@link #getWeight(Metric)} дополнительны, не кидает исключение,
     * если значения нет.
     */
    public OptionalDouble tryGetWeight(Metric metric) {
        Objects.requireNonNull(metric, "metric");
        Double value = weights.get(metric.key());
        return value == null ? OptionalDouble.empty() : OptionalDouble.of(value);
    }

    /** Проверить, задан ли вес для данной метрики на этом ребре. */
    public boolean hasWeight(Metric metric) {
        Objects.requireNonNull(metric, "metric");
        return weights.containsKey(metric.key());
    }

    /**
     * Иммутабельно возвращает копию ребра с добавленным/заменённым весом.
     * Исходное ребро не меняется.
     */
    public Edge withWeight(Metric metric, double value) {
        Objects.requireNonNull(metric, "metric");
        Map<String, Double> copy = new LinkedHashMap<>(weights);
        copy.put(metric.key(), value);
        return new Edge(id, source, destination, copy);
    }

    /** Иммутабельно возвращает копию ребра без указанной метрики. */
    public Edge withoutWeight(Metric metric) {
        Objects.requireNonNull(metric, "metric");
        if (!weights.containsKey(metric.key())) return this;
        Map<String, Double> copy = new LinkedHashMap<>(weights);
        copy.remove(metric.key());
        return new Edge(id, source, destination, copy);
    }

    // ---- Геттеры дополнительные для транспортной логистики----

    /** Эквивалент {@code getWeight(DefaultMetric.TIME)}. */
    public double getTime()     { return getWeight(DefaultMetric.TIME); }

    /** Эквивалент {@code getWeight(DefaultMetric.DISTANCE)}. */
    public double getDistance() { return getWeight(DefaultMetric.DISTANCE); }

    /** Эквивалент {@code getWeight(DefaultMetric.COST)}. */
    public double getCost()     { return getWeight(DefaultMetric.COST); }
}