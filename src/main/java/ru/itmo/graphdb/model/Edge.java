package ru.itmo.graphdb.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Представляет направленное ребро графа (транспортный маршрут между лог.объектами).
 * Хранит физические и экономические характеристики перемещения.
 */
@Getter
@AllArgsConstructor
public class Edge {
    private final String id;
    private final Node source;
    private final Node destination;
    private final double time;
    private final double distance;
    private final double cost;

    /**
     * Возвращает вес ребра в зависимости от выбранного критерия оптимизации.
     * Используется алгоритмами поиска путей для мультикритериальной маршрутизации.
     *
     * @param metric критерий оптимизации (время, расстояние, стоимость)
     * @return численное значение веса маршрута
     */
    public double getWeight(Metric metric) {
        return switch (metric) {
            case TIME -> time;
            case DISTANCE -> distance;
            case COST -> cost;
        };
    }
}
