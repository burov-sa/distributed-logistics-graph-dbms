package ru.itmo.graphdb.sharding;

import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Раскладка узлов графа по шардам на основе consistent hashing.
 *
 * <p><b>Кольцо.</b> Каждый шард представлен {@code VIRTUAL_NODES_PER_SHARD}
 * виртуальными узлами (vnodes). Позиция vnode на кольце —
 * {@code fnv1a64("vnode:<shardId>:<v>")}. Позиция ключа (id узла) —
 * {@code fnv1a64(nodeId)}. Домашний шард ключа — первый vnode по часовой
 * стрелке от позиции ключа (с замыканием на начало кольца).
 *
 * <p><b>Почему виртуальные узлы.</b> Без них один физический шард мог бы
 * «съесть» непропорционально большой кусок кольца. С 100 vnodes на шард
 * нагрузка между шардами выравнивается до долей процента.
 *
 * <p><b>Почему FNV-1a, а не {@link String#hashCode()}.</b>
 * {@code String.hashCode()} — 32-битный и плохо перемешивает похожие строки
 * ({@code "node1"}, {@code "node2"} и т.д. часто попадают рядом на кольце).
 * FNV-1a 64-битный — быстрый, стабильный между запусками JVM (это нужно для
 * тестов), с хорошим лавинным эффектом на последних байтах.
 *
 * <p><b>Свойство minimal movement.</b> При добавлении/удалении шарда
 * позиции существующих vnodes не меняются, поэтому переезжает только доля
 * ключей, попавших в «зону ответственности» нового vnode. В среднем —
 * {@code 1/(N+1)} при добавлении к {@code N} шардам.
 *
 * <p><b>Ограничение.</b> Топология по-прежнему фиксирована на старте
 * Router: добавить шард «на лету» без пересборки ring нельзя. Broadcast
 * нового {@code SHARD_MAP} подключённым клиентам — инфраструктура готова,
 * политика rebalance — future work.
 *
 * <p><b>Контракт {@link #shardForNode(String)}.</b> Возвращает <b>shardId</b>
 * (значение {@link ShardEndpoint#shardId()}), а не индекс в списке шардов.
 * Именно так его используют {@code RouterCommandTranslator} и
 * {@code ShardedGraphStorage} — как ключ в {@code Map<Integer, GraphDBClient>}.
 */
public class ShardRouter {

    /** Стандартное число vnodes на шард — достаточно для равномерной нагрузки. */
    public static final int DEFAULT_VIRTUAL_NODES_PER_SHARD = 100;

    private final List<ShardEndpoint> shards;
    private final NavigableMap<Long, ShardEndpoint> ring;
    private final int virtualNodesPerShard;

    /**
     * @param shards непустой список с уникальными {@code shardId}
     * @throws IllegalArgumentException если список пуст или {@code shardId} дублируются
     */
    public ShardRouter(List<ShardEndpoint> shards) {
        this(shards, DEFAULT_VIRTUAL_NODES_PER_SHARD);
    }

    /**
     * @param virtualNodesPerShard число vnodes на каждый физический шард;
     *                             должно быть {@code >= 1}. Меньше — точнее
     *                             минимальное перемещение ключей, но хуже
     *                             равномерность. 100 — стандартный компромисс.
     */
    public ShardRouter(List<ShardEndpoint> shards, int virtualNodesPerShard) {
        Objects.requireNonNull(shards, "shards");
        if (shards.isEmpty()) {
            throw new IllegalArgumentException("shards must not be empty");
        }
        if (virtualNodesPerShard < 1) {
            throw new IllegalArgumentException(
                    "virtualNodesPerShard must be >= 1, got: " + virtualNodesPerShard);
        }
        Set<Integer> seenIds = new HashSet<>();
        for (ShardEndpoint s : shards) {
            if (!seenIds.add(s.shardId())) {
                throw new IllegalArgumentException("Duplicate shardId: " + s.shardId());
            }
        }
        this.shards = List.copyOf(shards);
        this.virtualNodesPerShard = virtualNodesPerShard;

        TreeMap<Long, ShardEndpoint> r = new TreeMap<>();
        for (ShardEndpoint shard : shards) {
            for (int v = 0; v < virtualNodesPerShard; v++) {
                long h = fnv1a64("vnode:" + shard.shardId() + ":" + v);
                // Коллизия 64-битного хеша на ~сотнях vnodes практически
                // невозможна; putIfAbsent защищает от «потери» vnode в
                // редком случае — баланс от этого не пострадает.
                r.putIfAbsent(h, shard);
            }
        }
        this.ring = r;
    }

    /**
     * Домашний шард узла.
     *
     * @return {@code shardId} (не индекс в {@link #allShards()})
     */
    public int shardForNode(String nodeId) {
        return endpointForNode(nodeId).shardId();
    }

    /**
     * Endpoint домашнего шарда узла.
     *
     * <p>Ищет первый vnode по часовой стрелке от позиции ключа; если ключ
     * оказался за последним vnode — оборачивается на первый (кольцо).
     */
    public ShardEndpoint endpointForNode(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId");
        long h = fnv1a64(nodeId);
        var entry = ring.ceilingEntry(h);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    /** Полная топология (иммутабельный список, порядок — как передали в конструктор). */
    public List<ShardEndpoint> allShards() {
        return shards;
    }

    public int shardCount() {
        return shards.size();
    }

    /** Число vnodes на каждый шард — для тестов и диагностики. */
    public int virtualNodesPerShard() {
        return virtualNodesPerShard;
    }

    /** Текущий размер кольца — для тестов и диагностики. */
    public int ringSize() {
        return ring.size();
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

    /**
     * 64-битный FNV-1a. Быстрее MD5, стабилен между запусками JVM,
     * даёт достаточно равномерное распределение по кольцу.
     *
     * <p><b>Почему финализатор.</b> Чистый FNV-1a имеет слабое перемешивание
     * при изменении только последних символов строки: если два входа
     * отличаются в последнем байте, разница до умножения равна
     * {@code c1 ^ c2}, и после единственного умножения на простое число
     * хэши отличаются всего на {@code (c1 ^ c2) * P}, что ничтожно мало по
     * сравнению с размером кольца {@code 2^64}. В consistent hashing это
     * означает, что vnodes одного шарда ("vnode:3:0" … "vnode:3:99")
     * кластеризуются в одной точке кольца — суммарное покрытие падает до
     * процентов.
     *
     * <p>Финализатор MurmurHash3 (3 раунда XOR-shift + умножение на
     * нечётные константы) перемешивает все 64 бита, устраняя кластеризацию.
     * Это стандартный приём: FNV-1a хорош для инкрементального хеширования
     * байтов, а финализатор даёт лавинный эффект для финального значения.
     */
    private static long fnv1a64(String s) {
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < s.length(); i++) {
            hash ^= s.charAt(i);
            hash *= 0x100000001b3L;
        }
        // Финализатор MurmurHash3 (64-bit).
        hash ^= hash >>> 33;
        hash *= 0xff51afd7ed558ccdL;
        hash ^= hash >>> 33;
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= hash >>> 33;
        return hash;
    }
}