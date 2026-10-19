package ru.itmo.graphdb.storage;

import ru.itmo.graphdb.model.Edge;
import ru.itmo.graphdb.model.Node;
import ru.itmo.graphdb.model.DefaultMetric;
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
     * Удаление вершины графа.
     * @param id идентификатор вершины.
     * @return признак успешного выполнения операции
     */
    boolean deleteNode(String id);

    /**
     * Добавляет направленный маршрут (ребро) между существующими узлами графа.
     * @param edge объект ребра
     */
    void addEdge(Edge edge);
    /**
     * Удаление ребра графа.
     * @param id идентификатор ребра.
     * @return признак успешного выполнения операции
     */
    boolean deleteEdge(String id);

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
     * Возвращает ребро по его идентификатору.
     * @param id идентификатор ребра.
     * @return
     */
    Edge getEdge(String id);

    /**
     * Возвращает все ребра между двумя вершинами.
     * @param sourceId индентификатор первой вершины.
     * @param destinationId индентификатор второй вершниы.
     * @return
     */
    Collection<Edge> getEdgesBetween(String sourceId, String destinationId);
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
    Collection<Edge> getSortedEdgesFrom(Node node,  DefaultMetric metric);

    /**
     * Очищает граф от данных.
     */
    void clear();

    /**
     * Сохраняет данные в файл.
     * @param filePath путь к файлу сохранения
     * @return признак успешного выполнения операции
     */
    boolean saveToFile(String filePath);

    /**
     * Загружает данные из файла.
     * @param filePath путь к загружаемому файлу.
     * @return признак успешного выполнения операции
     */
    boolean loadFromFile(String filePath);


}
