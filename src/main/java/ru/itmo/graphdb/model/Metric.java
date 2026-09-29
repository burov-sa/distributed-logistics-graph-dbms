package ru.itmo.graphdb.model;

/**
 * Метрика по которому оценивается вес ребра
 */
public interface Metric {
    /** По ключу ищется имя метрики в реестре метрик, сравниваем, храним в БД. */
    String key();

    /** Человекочитаемое имя для UI и пользователя. */
    String displayName();
}