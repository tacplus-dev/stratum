// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.github.tacplusdev.stratum.hazelcast.StratumRole;
import com.github.tacplusdev.stratum.model.chassis.diskstore.DiskStoreException;

/**
 * The write gate counts WRITERS, and search members do not count.
 *
 * <p>This is the acceptance test the member-roles decision names: one writer and
 * five search members, with {@code min_durable_copies = 2}, must REFUSE the write.
 * Six members are present and every one of them will hold partitions and answer
 * queries, so a gate counting members sees six and acks a write that exactly one
 * volume will ever hold.
 *
 * <p>It replaced {@code HotWritesPerMapDurabilityTest}, which asserted the coupling
 * this decision removes: that {@code durableCopies} was
 * {@code min(members, that map's backupCount + 1)}, so a record declaring a floor of
 * 3 needed a third physical replica or every write to it was refused. The durable
 * copies are no longer the partition's replicas — a writer appends every write
 * cluster-wide — so one number answers for every map and {@code backupCount} is
 * memory redundancy only. The half of that test which is still a live property, a
 * record declaring more than the deployment floor being ACKED on a cluster that can
 * hold it, is {@link #aWriteAboveTheDeploymentFloorIsAckedWhenTheWritersAreThere}.
 */
class HotWritesWriterCountTest {

    private static final String MAP = "demo.Event";
    private static final int FIRST_PORT = 5811;

    private final List<HazelcastInstance> cluster = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (HazelcastInstance hz : cluster) {
            hz.shutdown();
        }
        cluster.clear();
    }

    /**
     * Brings up {@code writers} writer members followed by {@code searchers} search
     * members, all in one cluster, and returns the first writer.
     */
    private HazelcastInstance cluster(int writers, int searchers) {
        int size = writers + searchers;
        List<String> seeds = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            seeds.add("127.0.0.1:" + (FIRST_PORT + i));
        }
        for (int i = 0; i < size; i++) {
            cluster.add(Hazelcast.newHazelcastInstance(memberConfig(FIRST_PORT + i, seeds,
                    i < writers ? StratumRole.WRITER : StratumRole.SEARCH)));
        }
        assertEquals(size, cluster.get(0).getCluster().getMembers().size(),
                "the test needs every member up before the roles can be counted apart");
        return cluster.get(0);
    }

    private static Config memberConfig(int port, List<String> seeds, StratumRole role) {
        Config cfg = new Config();
        cfg.setClusterName("stratum-writer-count-test");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().setPort(port);
        cfg.getNetworkConfig().setPortAutoIncrement(false);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        for (String seed : seeds) {
            cfg.getNetworkConfig().getJoin().getTcpIpConfig().addMember(seed);
        }
        cfg.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        cfg.getMemberAttributeConfig().setAttribute(StratumRole.ATTRIBUTE, role.name());
        cfg.addMapConfig(new MapConfig(MAP).setBackupCount(1).setAsyncBackupCount(0));
        return cfg;
    }

    /**
     * The decision's own case. Five search members raise the member count to six and
     * the durable-copy count not at all, and the refusal leaves the map untouched —
     * asserting the throw alone would pass for a gate that threw after writing.
     */
    @Test
    void oneWriterAndFiveSearchMembersRefuseAWriteNeedingTwoCopies() {
        HazelcastInstance hz = cluster(1, 5);
        HotWrites hot = new HotWrites(hz, 0);
        IMap<String, String> map = hz.getMap(MAP);

        assertEquals(6, hz.getCluster().getMembers().size(), "six members are in the view");
        assertEquals(1, hot.durableCopies(), "one of them has a volume");

        DiskStoreException e = assertThrows(DiskStoreException.class,
                () -> hot.write(map, MAP, "k1", "v1", 2, () -> "{}", 1L));

        assertEquals(DiskStoreException.FAILED_PRECONDITION, e.getCode());
        assertTrue(e.getMessage().contains("2"), e.getMessage());
        assertNull(map.get("k1"), "the refused write must leave nothing behind");
        assertEquals(0, map.size());
    }

    /**
     * The paired positive, and the point of search members: adding a second WRITER to
     * that same cluster is what makes the write legal. Nothing about the five search
     * members changed.
     */
    @Test
    void theSameWriteIsAckedOnceASecondWriterIsThere() {
        HazelcastInstance hz = cluster(2, 4);
        HotWrites hot = new HotWrites(hz, 0);
        IMap<String, String> map = hz.getMap(MAP);

        assertEquals(6, hz.getCluster().getMembers().size());
        assertEquals(2, hot.durableCopies());

        hot.write(map, MAP, "k1", "v1", 2, () -> "{}", 1L);

        assertEquals("v1", map.get("k1"));
    }

    /**
     * A record declaring a floor above the deployment's is accepted on a cluster that
     * has that many WRITERS — the live half of the per-map test this replaced. What
     * changed is what "a cluster that can hold it" means: three writers, at whatever
     * backup count the maps carry.
     */
    @Test
    void aWriteAboveTheDeploymentFloorIsAckedWhenTheWritersAreThere() {
        HazelcastInstance hz = cluster(3, 0);
        HotWrites hot = new HotWrites(hz, 0);
        IMap<String, String> map = hz.getMap(MAP);

        assertEquals(3, hot.durableCopies());
        hot.write(map, MAP, "k1", "v1", 3, () -> "{}", 1L);

        assertEquals("v1", map.get("k1"));
    }

    /**
     * A member that declares no role at all counts as a writer. That is the direction
     * that cannot invent durability: every member of a cluster running an image from
     * before the attribute existed is in fact a writer, and reading absence the other
     * way would refuse every write on a healthy cluster.
     */
    @Test
    void aMemberWithNoRoleAttributeCountsAsAWriter() {
        Config cfg = new Config();
        cfg.setClusterName("stratum-writer-count-test-bare");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        cluster.add(hz);

        assertNull(hz.getCluster().getLocalMember().getAttribute(StratumRole.ATTRIBUTE));
        assertEquals(StratumRole.WRITER, StratumRole.of(hz.getCluster().getLocalMember()));
        assertEquals(1, new HotWrites(hz, 0).durableCopies());
    }
}
