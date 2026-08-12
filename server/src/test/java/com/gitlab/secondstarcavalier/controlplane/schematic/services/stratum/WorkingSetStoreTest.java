package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;

/**
 * Working sets have to cluster now that stratum runs several replicas.
 *
 * They used to be deliberately node-local, which was invisible while stratum ran
 * one pod: a map created through the shared Service and then read through it
 * again lands on whichever pod the Service picks, so with three pods a caller
 * gets "unknown map_id" for a map it just created two thirds of the time.
 */
class WorkingSetStoreTest {

    private static final List<String> SEEDS = List.of(
            "stratum-0.stratum:5701", "stratum-1.stratum:5701", "stratum-2.stratum:5701");

    @Test
    void joinsTheSeededPeersOverTcp() {
        NetworkConfig net = WorkingSetStore.buildHazelcastConfig(SEEDS, 900).getNetworkConfig();

        assertTrue(net.getJoin().getTcpIpConfig().isEnabled(),
                "seeded members must join over TCP/IP");
        assertFalse(net.getJoin().getMulticastConfig().isEnabled(),
                "multicast does not work in Kubernetes and must stay off");
        assertEquals(SEEDS, net.getJoin().getTcpIpConfig().getMembers(),
                "every seed address must be registered, in order");
    }

    @Test
    void pinsItsOwnPortWithAutoIncrementOff() {
        NetworkConfig net = WorkingSetStore.buildHazelcastConfig(SEEDS, 900).getNetworkConfig();

        assertEquals(WorkingSetStore.WS_PORT, net.getPort(),
                "the port must match the seed addresses the operator renders");
        assertFalse(net.isPortAutoIncrement(),
                "auto-increment moves a member to a port no seed list can name");
    }

    @Test
    void doesNotShareThePortOrTheClusterWithTheEventStore() {
        // Two Hazelcast clusters live in this one JVM. Sharing either the port
        // or the cluster name merges or collides them; auto-increment silently
        // relocating the second is what hid this the first time.
        assertNotEquals(
                com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast
                        .StratumMapConfig.HAZELCAST_PORT,
                WorkingSetStore.WS_PORT,
                "working sets need a port of their own");
        assertNotEquals(
                com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast
                        .StratumMapConfig.CLUSTER_NAME,
                WorkingSetStore.CLUSTER_NAME,
                "working sets need a cluster of their own");
    }

    @Test
    void isNodeLocalWithoutSeeds() {
        // Single-node dev: no peers to find, and multicast must not be used to
        // guess at some.
        NetworkConfig net = WorkingSetStore.buildHazelcastConfig(List.of(), 900).getNetworkConfig();

        assertFalse(net.getJoin().getTcpIpConfig().isEnabled());
        assertFalse(net.getJoin().getMulticastConfig().isEnabled());
    }

    @Test
    void keepsTheMapRegistryOutOfTheIdleEvictedNamespace() {
        // Every operation gates on the registry ("unknown map_id"). Under ws:*
        // it would inherit maxIdleSeconds and evict the record of maps that are
        // still holding data.
        assertFalse(WorkingSetStore.META_MAP.startsWith(WorkingSetStore.WS_PREFIX),
                "the registry must not match the ws:* idle-TTL MapConfig");

        Config cfg = WorkingSetStore.buildHazelcastConfig(SEEDS, 900);
        assertEquals(900, cfg.getMapConfig(WorkingSetStore.WS_PREFIX + "anything").getMaxIdleSeconds(),
                "guard: ws:* really does carry an idle TTL");
        assertEquals(0, cfg.getMapConfig(WorkingSetStore.META_MAP).getMaxIdleSeconds(),
                "the registry must not be idle-evicted");
    }
}
