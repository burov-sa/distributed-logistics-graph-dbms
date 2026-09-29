package ru.itmo.graphdb.model;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MetricRegistry {

    /** Предполагается учитывать порядок метрик, поэтому использован LinkedHashMap */
    private final Map<String, Metric> byKey = new ConcurrentHashMap<>();
    /** Отдельно держим кастомные, чтобы знать, что можно удалять. */
    private final Set<String> customKeys = ConcurrentHashMap.newKeySet();

    public MetricRegistry() {
        // СНачала наши дефолты для транспортнойо логистики
        for (DefaultMetric p : DefaultMetric.values()) {
            byKey.put(p.key(), p);
        }
    }

    /**
     * Добавить метрику веса поьзователя.
     * @return true если добавлен, false если ключ уже занят (кем угодно - дефолтным значением или метрикой пользователя).
     */
    public boolean addCustom(String key, String displayName) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(displayName, "displayName");

        Metric created = new CustomMetric(key, displayName);

        // Испоьзован putIfAbsent, атомарная операция для потокобезопасности
        Metric prev = byKey.putIfAbsent(key, created);
        if (prev != null) {
            return false; // ключ занят
        }
        customKeys.add(key);
        return true;
    }

    /** Найти по ключу. */
    public Optional<Metric> find(String key) {
        return Optional.ofNullable(byKey.get(key));
    }

    /** Все параметры: сначала дефолты, потом кастомные. */
    public List<Metric> all() {
        List<Metric> result = new ArrayList<>();
        // Дефолты в порядке объявления enum
        for (DefaultMetric p : DefaultMetric.values()) {
            result.add(p);
        }
        // Кастомные в произвольном порядке (но можно сортировать по displayName)
        customKeys.stream()
                .map(byKey::get)
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(Metric::displayName))
                .forEach(result::add);
        return result;
    }

    /** Проверка на существование. */
    public boolean contains(String key) {
        return byKey.containsKey(key);
    }

    /** Можно ли удалить параметр (дефолты — нельзя). */
    public boolean removeCustom(String key) {
        if (!customKeys.contains(key)) return false;
        customKeys.remove(key);
        byKey.remove(key);
        return true;
    }
}