// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The restore barrier must be satisfied by the instances that are HERE, not by
 * however many reports are sitting in the ledger.
 *
 * <p>The ledger is keyed by Hazelcast member UUID, which is minted fresh on every
 * restart, so a pod that dies leaves an entry no restart of it will ever reuse.
 * Counting the raw map size therefore answers "how many reports have ever been
 * filed?" — and a mix of dead members' leftovers and live members' fresh reports
 * reaches the expected count while instances that have replayed nothing are still
 * starting. That is the same "a wrong set satisfies the barrier" shape as the
 * whole-namespace-restart incident, reached by the barrier meant to prevent it.
 */
class HotRestoreLedgerTest {

    private HazelcastInstance hz;

    @BeforeEach
    void setUp() {
        Config cfg = new Config();
        cfg.setClusterName("stratum-restore-ledger-test");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(cfg);
    }

    @AfterEach
    void tearDown() {
        if (hz != null) hz.shutdown();
    }

    private IMap<String, Integer> ledger() {
        return hz.getMap(HotRestore.LEDGER_MAP);
    }

    /**
     * The reported failure, verbatim: three instances are expected, two of the
     * three pods died leaving their entries behind, and this pod is the only one
     * actually present. Raw size is 3 — enough — while the live contributor count
     * is 1. The barrier must refuse.
     */
    @Test
    void staleEntriesFromDepartedMembersDoNotSatisfyTheBarrier() {
        ledger().put(UUID.randomUUID().toString(), 40);
        ledger().put(UUID.randomUUID().toString(), 40);

        assertEquals(2, ledger().size(), "precondition: two departed members' entries are in the ledger");

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> HotRestore.awaitClusterReplay(hz, 12, 0, 3, Duration.ofMillis(300)),
                "two dead entries plus this node's fresh one must not clear a barrier of 3");

        assertTrue(e.getMessage().contains("only 1 of 3"),
                "the refusal must count the live reports, not the map size: " + e.getMessage());
    }

    /**
     * The paired positive, so the test above cannot be passed by a barrier that
     * simply never opens: with the floor matching what is actually here, the same
     * call returns rather than throwing, and the stale entries are gone.
     */
    @Test
    void aLiveMembersOwnReportStillClearsTheBarrier() {
        ledger().put(UUID.randomUUID().toString(), 40);

        int contributors = HotRestore.awaitClusterReplay(hz, 12, 0, 1, Duration.ofSeconds(5));

        assertEquals(1, contributors, "only the one live member contributed a log");
        assertEquals(1, ledger().size(), "the departed member's entry must have been pruned");
        assertTrue(ledger().containsKey(hz.getCluster().getLocalMember().getUuid().toString()));
    }

    /**
     * A departed member's entry must not be able to talk the contributor count up
     * to the durability floor either. This node read nothing (no redo log at all);
     * the leftover entry claims 40 records. Contributors must be 0, not 1.
     */
    @Test
    void staleEntriesDoNotCountAsDurabilityContributors() {
        ledger().put(UUID.randomUUID().toString(), 40);

        int contributors = HotRestore.awaitClusterReplay(hz, 0, 0, 1, Duration.ofSeconds(5));

        assertEquals(0, contributors,
                "a node that read nothing contributed nothing; the dead entry's 40 records are not its own");
        assertFalse(ledger().containsValue(40), "the departed member's entry must have been pruned");
    }
}
