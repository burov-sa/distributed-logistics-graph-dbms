package ru.itmo.graphdb.sharding;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Раскладка узлов графа по шардам.
 *
 * <p>Правило: {@code shardForNode(nodeId) = floorMod(nodeId.hashCode(), N)}.
 * Детерминированно, дёшево, достаточно для учебного проекта. В комментариях
 * честно фиксируем ограничение: при добавлении/удалении шарда все ключи
 * переезжают — rebalancing и consistent hashing вынесены в future work.
 *
 * <p>Класс не делает сетевых вызовов: он только отвечает на вопрос
 * «на каком шарде живёт узел». Соединения с шардами держит Router
 * (шаг 3), используя {@link #endpointForNode(String)} / {@link #allShards()}.
 *
 * <p>Потокобезопасен: состояние иммутабельно после конструктора.
 */
public class ShardRouter {

    private final List<ShardEndpoint> shards;

    /**
     * @param shards непустой список с уникальными {@code shardId}
     * @throws IllegalArgumentException если список пуст или shardId дублируются
     */
    public ShardRouter(List<ShardEndpoint> shards) {
        Objects.requireNonNull(shards, "shards");
        if (shards.isEmpty()) {
            throw new IllegalArgumentException("shards must not be empty");
        }
        Set<Integer> seen = new HashSet<>();
        for (ShardEndpoint s : shards) {
            if (!seen.add(s.shardId())) {
                throw new IllegalArgumentException("Duplicate shardId: " + s.shardId());
            }
        }
        this.shards = List.copyOf(shards);
    }

    /**
     * Домашний шард узла по его id.
     *
     * @return индекс в диапазоне {@code [0, shardCount())}
     */
    public int shardForNode(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId");
        return Math.floorMod(nodeId.hashCode(), shards.size());
    }

    /** Endpoint домашнего шарда узла. */
    public ShardEndpoint endpointForNode(String nodeId) {
        return shards.get(shardForNode(nodeId));
    }

    /** Полная топология (иммутабельный список). */
    public List<ShardEndpoint> allShards() {
        return shards;
    }

    public int shardCount() {
        return shards.size();
    }

    /**
     * Текстовое представление топологии для push-сообщения {@code SHARD_MAP}:
     * {@code "SHARD_MAP 0=localhost:7000;1=localhost:7001;2=localhost:7002"}.
     */
    public String topologyAsText() {
        return "SHARD_MAP " + shards.stream()
                .map(ShardEndpoint::asText)
                .collect(Collectors.joining(";"));
    }
}