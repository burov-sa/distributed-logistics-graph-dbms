package ru.itmo.graphdb.routing;

import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.Metric;
import ru.itmo.graphdb.storage.GraphStorage;
import java.util.List;

/**
 * Интерфейс для вычисления оптимальных маршрутов в графовой структуре.
 */
public interface PathFinder {
    /**
     * Вычисляет кратчайший путь между двумя узлами на основе заданной логистической метрики.
     *
     * @param storage целевое хранилище данных графа
     * @param source ID начального логистического объекта
     * @param destination ID конечного логистического объекта
     * @param metric критерий оптимизации (цена, время, расстояние)
     * @return упорядоченный список ребер, составляющих маршрут
     */
    List<Edge> findShortestPath(GraphStorage storage, String source, String destination, Metric metric);
}
