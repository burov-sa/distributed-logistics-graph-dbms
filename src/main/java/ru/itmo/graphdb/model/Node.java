package ru.itmo.graphdb.model;

import lombok.Getter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Представляет вершину графа (лог. объект).
 * Поддерживает хранение динамических атрибутов в потокобезопасном режиме.
 */
@Getter
public class Node {
    private final String id;
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public Node(String id) {
        this.id = id;
    }

    /**
     * Устанавливает кастомный динамический атрибут для узла (например, вместимость).
     */
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    /**
     * Возвращает значение кастомного атрибута по ключу.
     */
    public Object getAttribute(String key) {
        return attributes.get(key);
    }
}
