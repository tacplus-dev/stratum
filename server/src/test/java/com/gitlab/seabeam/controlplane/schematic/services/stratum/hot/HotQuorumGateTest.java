// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.DiskStoreException;

/**
 * A hot write below its declared durability floor must be REFUSED, with no local
 * effect — BIG GOAL 4 acceptance item 5.
 *
 * The failure this guards is the auto-degrade: "one node, so one copy will do".
 * A production cluster that has lost two members looks exactly like this
 * single-member test, so a degrade keyed on cluster size turns the guarantee off
 * precisely when it was the thing protecting the data. The floor is therefore a
 * DECLARED number, and a one-node deployment says 1 out loud.
 */
class HotQuorumGateTest {

    private HazelcastInstance hz;

    @BeforeEach
    void setUp() {
        Config cfg = new Config();
        cfg.setClusterName("stratum-quorum-gate-test");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        // A record map sized above the deployment floor, the way StratumMapConfig
        // sizes one whose record declares more than the deployment does.
        cfg.addMapConfig(new MapConfig("demo.HighFloor").setBackupCount(2).setAsyncBackupCount(0));
        hz = Hazelcast.newHazelcastInstance(cfg);
    }

    @AfterEach
    void tearDown() {
        if (hz != null) hz.shutdown();
    }

    /**
     * One member, floor of two: refused, and the map is untouched. Asserting the
     * throw alone would pass for a gate that threw AFTER writing locally, which is
     * the "ack-then-hope" shape the refusal exists to prevent.
     */
    @Test
    void aWriteBelowTheFloorIsRefusedAndHasNoLocalEffect() {
        HotWrites hot = new HotWrites(hz, 0);
        IMap<String, String> map = hz.getMap("gate");

        DiskStoreException e = assertThrows(DiskStoreException.class,
                () -> hot.write(map, "demo.Event", "k1", "v1", 2, () -> "{}", 1L));

        assertEquals(DiskStoreException.FAILED_PRECONDITION, e.getCode(),
                "a below-quorum write must be FAILED_PRECONDITION, not an internal error");
        assertTrue(e.getMessage().contains("2"), e.getMessage());
        assertNull(map.get("k1"), "the refused write must leave nothing behind");
        assertEquals(0, map.size());
    }

    /** The paired positive: at the declared floor the same write goes through. */
    @Test
    void aWriteAtTheFloorIsAcked() {
        HotWrites hot = new HotWrites(hz, 0);
        IMap<String, String> map = hz.getMap("gate");

        hot.write(map, "demo.Event", "k1", "v1", 1, () -> "{}", 1L);

        assertEquals("v1", map.get("k1"));
    }

    /**
     * The override is the deliberate single-node setting, not a degrade: it takes
     * a record declaring 2 down to 1 only because the deployment SAID 1.
     */
    @Test
    void theDeploymentOverrideReplacesTheDeclaredFloor() {
        HotWrites hot = new HotWrites(hz, 1);
        IMap<String, String> map = hz.getMap("gate");

        assertEquals(1, hot.effectiveFloor(2));
        hot.write(map, "demo.Event", "k1", "v1", 2, () -> "{}", 1L);

        assertEquals("v1", map.get("k1"));
    }

    /**
     * Durable copies are the WRITERS in the cluster view, and no longer a per-map
     * number derived from that map's backup count. This one-member cluster carries no
     * role attribute at all — which reads as writer, the direction that cannot invent
     * durability — so it supplies one copy however many backups a map asks for.
     */
    @Test
    void durableCopiesAreTheWritersInTheView() {
        HotWrites hot = new HotWrites(hz, 0);
        assertEquals(1, hot.durableCopies(),
                "one member, no role attribute: one writer, so one durable copy —"
                        + " backupCount=2 on demo.HighFloor buys memory redundancy, not a log");
    }
}
