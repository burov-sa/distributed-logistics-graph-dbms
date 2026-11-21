package ru.itmo.graphdb.sharding;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardRouterTest {

    private ShardRouter router(int n) {
        List<ShardEndpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            endpoints.add(new ShardEndpoint(i, "localhost", 7000 + i));
        }
        return new ShardRouter(endpoints);
    }

    // ---------- базовое поведение ----------

    @Test
    void shardForNodeIsDeterministicAndInRange() {
        ShardRouter r = router(3);
        for (String id : List.of("A", "B", "node1", "warehouse-42", "")) {
            int a = r.shardForNode(id);
            int b = r.shardForNode(id);
            assertEquals(a, b, "shardForNode must be deterministic for: " + id);
            assertTrue(a >= 0 && a < 3, "in range [0,3), got: " + a);
        }
    }

    @Test
    void shardForNodeReturnsActualShardIdNotIndex() {
        // Ключевой контракт: shardForNode возвращает shardId (10/20/30),
        // а не индекс (0/1/2). Иначе Map<Integer, GraphDBClient>,
        // ключом которого служит shardId, сломается на ненулевых id.
        List<ShardEndpoint> endpoints = List.of(
                new ShardEndpoint(10, "h", 7000),
                new ShardEndpoint(20, "h", 7001),
                new ShardEndpoint(30, "h", 7002)
        );
        ShardRouter r = new ShardRouter(endpoints);
        Set<Integer> shardIds = Set.of(10, 20, 30);
        for (int i = 0; i < 100; i++) {
            int id = r.shardForNode("node" + i);
            assertTrue(shardIds.contains(id),
                    "shardForNode must return a shardId, got: " + id);
        }
    }

    @Test
    void shardsAreReasonablyDistributed() {
        ShardRouter r = router(4);
        Set<Integer> used = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            used.add(r.shardForNode("node_" + i));
        }
        assertTrue(used.size() >= 3,
                "Expected at least 3 distinct shards for 100 IDs, got: " + used);
    }

    @Test
    void endpointForNodeIsConsistentWithShardForNode() {
        ShardRouter r = router(3);
        for (String id : List.of("A", "B", "C", "D")) {
            assertEquals(r.shardForNode(id), r.endpointForNode(id).shardId());
        }
    }

    @Test
    void topologyAsTextFormat() {
        ShardRouter r = router(2);
        assertEquals("SHARD_MAP 0=localhost:7000;1=localhost:7001", r.topologyAsText());
    }

    @Test
    void rejectsEmptyAndDuplicateShardIds() {
        assertThrows(IllegalArgumentException.class, () -> new ShardRouter(List.of()));
        assertThrows(IllegalArgumentException.class, () -> new ShardRouter(List.of(
                new ShardEndpoint(0, "localhost", 7000),
                new ShardEndpoint(0, "localhost", 7001)
        )));
    }

    @Test
    void endpointRoundTripsThroughText() {
        ShardEndpoint ep = new ShardEndpoint(7, "10.0.0.5", 7107);
        assertEquals(ep, ShardEndpoint.fromText(ep.asText()));
    }

    // ---------- consistent hashing ----------

    @Test
    void ringSizeEqualsShardsTimesVirtualNodes() {
        ShardRouter r = new ShardRouter(List.of(
                new ShardEndpoint(0, "h", 7000),
                new ShardEndpoint(1, "h", 7001),
                new ShardEndpoint(2, "h", 7002)
        ), 50);
        assertEquals(150, r.ringSize());
        assertEquals(50, r.virtualNodesPerShard());
    }

    @Test
    void defaultVirtualNodeCountIsApplied() {
        ShardRouter r = router(4);
        assertEquals(ShardRouter.DEFAULT_VIRTUAL_NODES_PER_SHARD, r.virtualNodesPerShard());
        assertEquals(4 * ShardRouter.DEFAULT_VIRTUAL_NODES_PER_SHARD, r.ringSize());
    }

    @Test
    void rejectsNonPositiveVirtualNodeCount() {
        assertThrows(IllegalArgumentException.class, () -> new ShardRouter(
                List.of(new ShardEndpoint(0, "h", 7000)), 0));
        assertThrows(IllegalArgumentException.class, () -> new ShardRouter(
                List.of(new ShardEndpoint(0, "h", 7000)), -1));
    }

    @Test
    void distributionIsCloseToUniform() {
        int shardCount = 4;
        int total = 10_000;
        ShardRouter r = router(shardCount);

        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < total; i++) {
            counts.merge(r.shardForNode("key-" + i), 1, Integer::sum);
        }
        for (int shardId = 0; shardId < shardCount; shardId++) {
            int c = counts.getOrDefault(shardId, 0);
            int percent = c * 100 / total;
            assertTrue(percent >= 15 && percent <= 35,
                    "shard " + shardId + " got " + percent + "% of keys (expected ~25%)");
        }
    }

    @Test
    void highVnodeCountGivesTightBalance() {
        int shardCount = 4;
        int total = 10_000;
        List<ShardEndpoint> endpoints = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            endpoints.add(new ShardEndpoint(i, "h", 7000 + i));
        }
        ShardRouter r = new ShardRouter(endpoints, 200);

        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < total; i++) {
            counts.merge(r.shardForNode("k-" + i), 1, Integer::sum);
        }
        for (int shardId = 0; shardId < shardCount; shardId++) {
            int c = counts.getOrDefault(shardId, 0);
            int percent = c * 100 / total;
            assertTrue(percent >= 20 && percent <= 30,
                    "with 200 vnodes, shard " + shardId + " got " + percent
                            + "% (expected ~25%)");
        }
    }

    @Test
    void addingShardMovesOnlyFractionOfKeys() {
        List<ShardEndpoint> three = List.of(
                new ShardEndpoint(0, "h", 7000),
                new ShardEndpoint(1, "h", 7001),
                new ShardEndpoint(2, "h", 7002)
        );
        List<ShardEndpoint> four = new ArrayList<>(three);
        four.add(new ShardEndpoint(3, "h", 7003));

        ShardRouter r3 = new ShardRouter(three);
        ShardRouter r4 = new ShardRouter(four);

        int total = 10_000;
        int moved = 0;
        for (int i = 0; i < total; i++) {
            String key = "key-" + i;
            if (r3.shardForNode(key) != r4.shardForNode(key)) {
                moved++;
            }
        }
        double ratio = (double) moved / total;
        // Consistent hashing: при добавлении 4-го шарда переезжает
        // в среднем ~1/4 = 25% ключей. Допускаем 15-35%.
        assertTrue(ratio >= 0.15 && ratio <= 0.35,
                "expected ~25% moved, got " + String.format("%.1f%%", ratio * 100));
    }

    @Test
    void removingShardOnlyAffectsItsFormerKeys() {
        List<ShardEndpoint> three = List.of(
                new ShardEndpoint(0, "h", 7000),
                new ShardEndpoint(1, "h", 7001),
                new ShardEndpoint(2, "h", 7002)
        );
        List<ShardEndpoint> two = List.of(three.get(0), three.get(1));

        ShardRouter r3 = new ShardRouter(three);
        ShardRouter r2 = new ShardRouter(two);

        int total = 10_000;
        int moved = 0;
        for (int i = 0; i < total; i++) {
            String key = "key-" + i;
            if (r3.shardForNode(key) != r2.shardForNode(key)) {
                moved++;
            }
        }
        double ratio = (double) moved / total;
        // Удаление шарда: ~1/3 = 33% ключей переезжает. Допускаем 20-45%.
        assertTrue(ratio >= 0.20 && ratio <= 0.45,
                "expected ~33% moved, got " + String.format("%.1f%%", ratio * 100));
    }
}