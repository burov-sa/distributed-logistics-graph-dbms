package ru.itmo.graphdb.model;

public class CustomMetric implements Metric{
    private final String key;
    private final String displayName;

    public CustomMetric(String key, String displayName) {
        this.key = requireNonBlank(key, "key");
        this.displayName = requireNonBlank(displayName, "displayName");
    }
    //проверка правильности ключа
    private static String requireNonBlank(String v, String name) {
        if (v == null || v.isBlank())
            throw new IllegalArgumentException(name + " must not be blank");
        return v;
    }

    @Override public String key() { return key; }
    @Override public String displayName() { return displayName; }

    // Равенство реализовано исключительно по key, потому что ключ уникален в реестре метрик.
    @Override
    public boolean equals(Object o) {
        return (o instanceof Metric p) && key.equals(p.key());
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return "CustomMetric{" + key + "}";
    }

}
