package ru.itmo.graphdb.model;

/**
 * Перечисление типов логистических объектов в транспортной сети.
 */
public enum Label {
    /** Склад */
    WAREHOUSE,
    /** Сортировочный центр  */
    SORTING_CENTRE,
    /** Пункт выдачи заказов */
    ORDER_PICKUP_POINT
}
