// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A recovery that came in UNDER the declared durability floor must refuse to open
 * the hot store, not open it with a log line.
 *
 * <p>The barrier clearing only says every instance that is here has reported. It
 * does not say the reports add up: the instances present can all have reported
 * while fewer of them than {@code min_durable_copies} actually had a redo log to
 * contribute, and the store assembled from those is thinner than the durability
 * every acked write was acked under — it can be missing rows whose only surviving
 * copies lived on the instances that contributed nothing. Logging that and
 * returning hands {@code EventStore.restoreHotTier} a store the log line itself
 * calls incomplete, and it goes straight on to open the read gate over it.
 */
class HotRestoreDurabilityFloorTest {

    private HazelcastInstance a;
    private HazelcastInstance b;

    @BeforeEach
    void setUp() {
        a = Hazelcast.newHazelcastInstance(config());
        b = Hazelcast.newHazelcastInstance(config());
        assertEquals(2, a.getCluster().getMembers().size(), "the two members did not form a cluster");
    }

    @AfterEach
    void tearDown() {
        if (a != null) a.shutdown();
        if (b != null) b.shutdown();
    }

    private static Config config() {
        Config cfg = new Config();
        cfg.setClusterName("stratum-restore-floor-test");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        // Loopback TCP/IP only: multicast would pick up whatever else is on the
        // developer's network and make the member count non-deterministic.
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .addMember("127.0.0.1:5821").addMember("127.0.0.1:5822");
        cfg.getNetworkConfig().setPort(5821).setPortAutoIncrement(true);
        cfg.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        return cfg;
    }

    private IMap<String, Integer> ledger() {
        return a.getMap(HotRestore.LEDGER_MAP);
    }

    /** Files a report for the OTHER live member, as that member's own replay would. */
    private void peerReports(int redoRecordsRead) {
        ledger().put(b.getCluster().getLocalMember().getUuid().toString(), redoRecordsRead);
    }

    /**
     * Both instances are here and both have reported, so the barrier clears — but
     * the peer came back with an empty volume and read nothing, so only one of the
     * two required durable copies is behind the recovered store. That must throw.
     */
    @Test
    void aRecoveryBelowTheDurabilityFloorRefusesToOpenTheStore() {
        peerReports(0);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> HotRestore.awaitClusterReplay(a, 12, 2, Duration.ofSeconds(5)),
                "one contributor against min_durable_copies=2 must not open the hot store");

        assertTrue(e.getMessage().contains("only 1 of 2"),
                "the refusal must name what contributed against what was expected: " + e.getMessage());
        assertTrue(e.getMessage().contains("min_durable_copies 2"), e.getMessage());
    }

    /**
     * The paired positive, so the test above cannot be passed by a barrier that
     * simply never opens: with both instances contributing a log, the same call
     * returns the contributor count.
     */
    @Test
    void aRecoveryAtTheDurabilityFloorOpensTheStore() {
        peerReports(40);

        int contributors = HotRestore.awaitClusterReplay(a, 12, 2, Duration.ofSeconds(5));

        assertEquals(2, contributors, "both live members contributed a redo log");
    }

    /**
     * Zero contributors is NOT the failure above and must not throw: no live
     * instance had a redo log at all, which is a first deployment or a total loss
     * of every volume — an empty hot tier, not a thin one. Its recovery is the
     * event-log cold-start replay, which {@code hotReadable()} separately waits
     * for. Throwing here would crash-loop every first-ever bring-up.
     */
    @Test
    void noContributorsAnywhereIsAnEmptyStoreNotAnUnderDurableOne() {
        peerReports(0);

        int contributors = HotRestore.awaitClusterReplay(a, 0, 2, Duration.ofSeconds(5));

        assertEquals(0, contributors, "neither live member had a redo log");
    }
}
