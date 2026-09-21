// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A peer that is UP AND SERVING counts toward the durability floor even when the
 * number it filed in the ledger says it holds nothing.
 *
 * <p>The ledger entry is written once, at that member's own start, and never
 * revised ({@link HotRestore#LEDGER_MAP}). So it is a statement about a moment,
 * and the barrier was reading it as a statement about now. Two ways it goes
 * stale, both live:
 *
 * <ul>
 *   <li>the peer has checkpointed since it booted, so what its entry describes
 *       (its redo tail at boot) is no longer where its rows are; and
 *   <li>the peer is running an image that predates {@code 9fa7ebc} and computed
 *       its entry from the redo tail ALONE, so a checkpointed volume filed a 0.
 * </ul>
 *
 * <p>The second is what took stratum-1 and stratum-2 down on 2026-09-21: both
 * restarted onto the fixed image against a serving stratum-0 that was still on the
 * old one, counted their own checkpoints (1) plus stratum-0's permanent 0, and
 * exited on "only 1 of 2 instances contributed a durable copy" — while stratum-0
 * was serving 468 rows. An already-running peer cannot re-file a report, so no
 * amount of counting better fixes it; what fixes it is counting the peer on the
 * strength of the barrier it already passed.
 *
 * <p>Two members here, not three: this bug does not need the three-way split
 * {@link HotRestoreCheckpointContributorTest} needs, because the production shape
 * IS two — one serving peer, one pod restarting alone into it. The serving peer is
 * put into that state by running the real {@link HotRestore#awaitClusterReplay} to
 * completion rather than by writing a flag, so what the restarting pod reads is
 * what production leaves behind.
 */
class HotRestoreServingPeerContributorTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    /** The peer that stays up throughout, standing in for stratum-0. */
    private HazelcastInstance serving;
    /** The pod that restarts: shut down and replaced by a member with a fresh UUID. */
    private HazelcastInstance restarting;
    private ExecutorService peerThreads;

    @BeforeEach
    void setUp() {
        serving = Hazelcast.newHazelcastInstance(config());
        restarting = Hazelcast.newHazelcastInstance(config());
        assertEquals(2, serving.getCluster().getMembers().size(),
                "the two members did not form a cluster");
        peerThreads = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        if (peerThreads != null) peerThreads.shutdownNow();
        for (HazelcastInstance hz : List.of(serving, restarting)) {
            if (hz != null && hz.getLifecycleService().isRunning()) hz.shutdown();
        }
    }

    private static Config config() {
        Config cfg = new Config();
        cfg.setClusterName("stratum-serving-peer-contributor-test");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .addMember("127.0.0.1:5841").addMember("127.0.0.1:5842").addMember("127.0.0.1:5843");
        cfg.getNetworkConfig().setPort(5841).setPortAutoIncrement(true);
        cfg.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        return cfg;
    }

    /**
     * The production crash, reproduced end to end: stratum-0 up and serving with a
     * ledger entry of 0, stratum-1 restarting fresh and alone against it.
     *
     * <p>The 0 is produced the way the old image produced it — the peer's boot is
     * run through {@code awaitClusterReplay} with the redo tail it had then, and its
     * entry is afterwards overwritten with the redo-tail-only number a pre-9fa7ebc
     * member would have filed for a volume that has since been checkpointed. What
     * the restarting pod reads is therefore a real report from a real completed
     * restore, stale in exactly the way production's was.
     *
     * <p>Before the serving leg existed this threw "only 1 of 2 instances
     * contributed a durable copy", which is the stack stratum-1 and stratum-2
     * exited on.
     */
    @Test
    void aServingPeerWithAStaleLedgerEntryStillCountsTowardTheDurabilityFloor() throws Exception {
        String servingUuid = passTheBarrierTogether();

        // What a pre-9fa7ebc member holding a checkpointed volume reports: the redo
        // tail alone, which DailyCheckpoint truncated to nothing. The member is
        // still up and still holding the maps; only its number is from before.
        serving.<String, Integer>getMap(HotRestore.LEDGER_MAP).put(servingUuid, 0);
        assertFalse(serving.<String, Boolean>getMap(HotRestore.INFLIGHT_MAP).containsKey(servingUuid),
                "a member that completed its restore must not be left marked as restoring");

        int contributors = restartAlone(0, 468);

        assertEquals(2, contributors,
                "the serving peer and the restarting pod's own checkpoint are both durable copies");
    }

    /**
     * The paired negative, so the fix cannot be "count every live member": a peer
     * that is ITSELF inside its own restore is not serving anything yet, and two
     * pods restarting together onto empty volumes must not credit each other into a
     * quorum neither of them has.
     *
     * <p>This is the simultaneous-restart shape — the one a bulk {@code
     * RollingUpdate} produces, and the reason the serving state is published before
     * the ledger entry rather than after it.
     */
    @Test
    void twoPodsRestartingTogetherDoNotCreditEachOther() {
        // Both members are at the barrier at once, and the one with a tail is the
        // only durable copy in the cluster: one contributor against a floor of two.
        CompletableFuture<Integer> peer = CompletableFuture.supplyAsync(
                () -> HotRestore.awaitClusterReplay(serving, 371, 0, 2, WAIT), peerThreads);

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> HotRestore.awaitClusterReplay(restarting, 0, 0, 2, WAIT),
                "a peer still inside its own restore must not be counted as serving");
        assertTrue(e.getMessage().contains("only 1 of 2"),
                "the refusal must still name what contributed against what was expected: "
                        + e.getMessage());
        // The peer refuses off the same ledger, as every member of an under-durable
        // cluster does; joining without allowing for that fails on the right answer.
        assertThrows(Exception.class, () -> peer.get(WAIT.toMillis(), TimeUnit.MILLISECONDS));
    }

    /**
     * Brings both members through a normal, healthy restore so the peer ends up in
     * the state a serving pod is actually in, and returns the peer's member UUID.
     */
    private String passTheBarrierTogether() throws Exception {
        CompletableFuture<Integer> peer = CompletableFuture.supplyAsync(
                () -> HotRestore.awaitClusterReplay(serving, 468, 0, 2, WAIT), peerThreads);
        assertEquals(2, HotRestore.awaitClusterReplay(restarting, 468, 0, 2, WAIT),
                "the healthy two-member restore must pass before anything is made stale");
        assertEquals(2, peer.get(WAIT.toMillis(), TimeUnit.MILLISECONDS).intValue(),
                "both members must reach the same conclusion");
        return serving.getCluster().getLocalMember().getUuid().toString();
    }

    /**
     * Restarts the non-serving pod — a real shutdown and a real new member, so its
     * UUID is new and its old ledger entry is pruned as a departed member's — and
     * returns what its barrier concluded.
     */
    private int restartAlone(int redoRecordsRead, long checkpointRecords) {
        restarting.shutdown();
        restarting = Hazelcast.newHazelcastInstance(config());
        assertEquals(2, restarting.getCluster().getMembers().size(),
                "the restarted member did not rejoin the serving peer");
        return HotRestore.awaitClusterReplay(restarting, redoRecordsRead, checkpointRecords, 2, WAIT);
    }
}
