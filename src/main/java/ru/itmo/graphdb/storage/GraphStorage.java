package ru.itmo.graphdb.storage;

import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.Node;
import ru.itmo.graphdb.model.Metric;
import java.util.Collection;

/**
 * Базовый интерфейс для управления данными графовой СУБД.
 * Абстрагирует механизмы хранения от сетевого слоя и алгоритмов маршрутизации.
 */
public interface GraphStorage {

    /**
     * Сохраняет новый логистический узел в базу данных.
     * @param node объект узла
     */
    void addNode(Node node);

    /**
     * Добавляет направленный маршрут (ребро) между существующими узлами графа.
     * @param edge объект ребра
     */
    void addEdge(Edge edge);

    /**
     * Находит узел графа по его уникальному идентификатору.
     * @param id уникальный идентификатор узла
     * @return объект Node или null, если объект не найден
     */
    Node getNode(String id);

    /**
     * Возвращает все зарегистрированные узлы базы данных.
     * @return коллекция всех узлов
     */
    Collection<Node> getAllNodes();

    /**
     * Возвращает коллекцию всех исходящих связей (маршрутов)
     * из указанного логистического объекта (узла).
     *
     * @param node исходный узел графа
     * @return коллекция смежных ребер
     */
    Collection<Edge> getEdgesFrom(Node node);

    /**
     * Возвращает сортированную коллекцию всех исходящих связей (маршрутов)
     * из указанного логистического объекта (узла) по задаваемому критерию.
     *
     * @param node исходный узел графа
     * @param metric выбранная метрика для сортировки (DISTANCE, TIME или COST)
     * @return коллекция смежных ребер
     */
    Collection<Edge> getSortedEdgesFrom(Node node,  Metric metric);
}
