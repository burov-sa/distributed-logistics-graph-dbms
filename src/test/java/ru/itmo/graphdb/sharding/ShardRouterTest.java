package ru.itmo.graphdb.sharding;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardRouterTest {

    private ShardRouter router(int n) {
        List<ShardEndpoint> endpoints = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) {
            endpoints.add(new ShardEndpoint(i, "localhost", 7000 + i));
        }
        return new ShardRouter(endpoints);
    }

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
    void endpointForNodeMatchesShardForNode() {
        ShardRouter r = router(3);
        for (String id : List.of("A", "B", "C", "D")) {
            int idx = r.shardForNode(id);
            assertEquals(idx, r.endpointForNode(id).shardId());
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
}