// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.checkpoint.Checkpoint;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.checkpoint.DailyCheckpoint;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.checkpoint.TombstoneLog;

/**
 * A peer whose redo log is empty BECAUSE ITS CHECKPOINT JUST RAN still holds a
 * durable copy, and the durability floor must count it.
 *
 * <p>{@link DailyCheckpoint} ends every cycle by truncating this pod's redo logs
 * to the dump point: the rows move out of the tail and into the checkpoint file,
 * which DURABILITY.md names as the durable copy. A barrier that counts redo rows
 * alone reads that pod as an empty volume — the same report a first boot or a lost
 * disk files — and a third pod restarting into that view is refused with
 * "below min_durable_copies" while nothing whatsoever has been lost.
 *
 * <p>That is not a rare alignment. Every pod truncates on every cycle, so the
 * window is open for as long as it takes the fleet's checkpoints to stagger, and
 * the stale ledger entry a pod filed at ITS OWN boot is never revised — the
 * refusal outlives the cycle that caused it rather than clearing on the next one.
 * Observed in production 2026-09-21 as stratum-0 crash-looping against a healthy
 * stratum-1/stratum-2; see
 * {@code decisions/2026-09-21-stratum0-checkpoint-quorum-crashloop.md}.
 *
 * <p>Three members, because the bug lives strictly between the two carve-outs:
 * with the just-checkpointed peer reported as empty the count lands on ONE, which
 * is neither the zero that is allowed nor the two that is enough. Two members
 * cannot express it. Each member files its report through
 * {@link HotRestore#awaitClusterReplay} itself rather than by a hand-written
 * ledger entry, so what is under test is the number production actually files.
 */
class HotRestoreCheckpointContributorTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    /** The restarting pod (stratum-0), the just-checkpointed peer, the peer with a tail. */
    private HazelcastInstance restarting;
    private HazelcastInstance checkpointed;
    private HazelcastInstance tailed;
    /**
     * The peers' barrier calls, on a pool of their own. NOT the common pool: these
     * tasks block until every member has reported, and a common pool sized to a
     * one-core runner would run the first and never start the second, which is a
     * deadlock disguised as a barrier timeout.
     */
    private ExecutorService peerThreads;

    @BeforeEach
    void setUp() {
        restarting = Hazelcast.newHazelcastInstance(config());
        checkpointed = Hazelcast.newHazelcastInstance(config());
        tailed = Hazelcast.newHazelcastInstance(config());
        assertEquals(3, restarting.getCluster().getMembers().size(),
                "the three members did not form a cluster");
        peerThreads = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        if (peerThreads != null) peerThreads.shutdownNow();
        for (HazelcastInstance hz : List.of(restarting, checkpointed, tailed)) {
            if (hz != null) hz.shutdown();
        }
    }

    private static Config config() {
        Config cfg = new Config();
        cfg.setClusterName("stratum-checkpoint-contributor-test");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        // Loopback TCP/IP only, as in HotRestoreDurabilityFloorTest: multicast would
        // pick up whatever else is on the developer's network and make the member
        // count non-deterministic.
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .addMember("127.0.0.1:5831").addMember("127.0.0.1:5832").addMember("127.0.0.1:5833");
        cfg.getNetworkConfig().setPort(5831).setPortAutoIncrement(true);
        cfg.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        return cfg;
    }

    /**
     * Runs the real purge -> dump -> truncate transaction over {@code volume} and
     * returns what the pod's redo logs hold afterwards, which is the number its
     * replay would read on the next boot.
     *
     * <p>The rows are {@link StringValue}s: the transaction's purge step selects on
     * {@code (spec.deleted_flag)}, a marker this type does not carry, so nothing is
     * purged and the dump is exactly the rows put in — which is what this test wants
     * to count. The purge step has its own coverage in {@code DailyCheckpointTest}.
     */
    private static int checkpointAndTruncate(Path volume, int rowCount) throws Exception {
        Map<String, Message> rows = new LinkedHashMap<>();
        for (int i = 0; i < rowCount; i++) {
            rows.put("row-" + i, StringValue.of("payload-" + i));
        }
        // Stands in for HotRedoLogs.truncateToCheckpoint, whose effect on the next
        // boot is exactly this: the replay reads zero records from a rolled log.
        AtomicInteger redoRecords = new AtomicInteger(rowCount);
        new DailyCheckpoint(new SingleMapRows(rows), DailyCheckpoint.DiskPurge.NONE,
                new TombstoneLog(volume), new Checkpoint(volume),
                () -> redoRecords.set(0))
                .run(Duration.ofDays(30).toMillis(), System.currentTimeMillis());
        return redoRecords.get();
    }

    /**
     * The production crash, reproduced: the restarting pod's own volume holds
     * nothing, one peer sits just after its checkpoint, and the third peer still has
     * a tail. Two of the three hold a durable copy and {@code min_durable_copies} is
     * two, so the restart must be allowed to complete.
     *
     * <p>Before the checkpoint leg was reported, the just-checkpointed peer filed a
     * zero and this threw "only 1 of 3 instances contributed" — which is the stack
     * stratum-0 exited on, seven times.
     */
    @Test
    void aJustCheckpointedPeerStillCountsTowardTheDurabilityFloor(@TempDir Path peerVolume)
            throws Exception {
        int redoAfterTruncate = checkpointAndTruncate(peerVolume, 298);
        assertEquals(0, redoAfterTruncate,
                "the transaction must leave the redo log empty, or this reproduces nothing");
        long checkpointRows = new Checkpoint(peerVolume).rowCount();
        assertEquals(298, checkpointRows,
                "the rows the truncate took out of the tail must be countable in the dump");

        CompletableFuture<Integer> peers = CompletableFuture.allOf(
                report(checkpointed, redoAfterTruncate, checkpointRows),
                // stratum-1's shape: booted long before its own first checkpoint, so
                // its tail is what it replayed and its ledger entry is non-zero.
                report(tailed, 371, 0))
                .thenApply(ignored -> 0);

        // stratum-0's shape: evicted and rescheduled onto a volume whose tail had
        // already been truncated and whose dump this boot has no reason to load.
        int contributors = HotRestore.awaitClusterReplay(restarting, 0, 0, 2, WAIT);
        peers.get(WAIT.toMillis(), TimeUnit.MILLISECONDS);

        assertEquals(2, contributors,
                "the checkpointed peer and the tailed peer both hold a durable copy");
    }

    /**
     * The paired negative, so the fix cannot be "count everyone": a peer that holds
     * NEITHER a tail nor a checkpoint — a replaced or wiped volume — is still not a
     * contributor, and one lone contributor against a floor of two is still refused.
     */
    @Test
    void aPeerWithNoTailAndNoCheckpointIsStillNotAContributor() {
        CompletableFuture<Void> peers = CompletableFuture.allOf(
                report(checkpointed, 0, 0),
                report(tailed, 371, 0));

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> HotRestore.awaitClusterReplay(restarting, 0, 0, 2, WAIT),
                "an empty volume must not be talked up to the floor by the checkpoint leg");
        // The peers refuse too, off the same ledger — every pod in an under-durable
        // cluster crash-loops, which is the designed outcome. Joining without
        // allowing for it would fail this test on the right answer.
        assertThrows(CompletionException.class, peers::join,
                "a refusal this member reached must be reachable by every member");

        assertTrue(e.getMessage().contains("only 1 of 3"),
                "the refusal must still name what contributed against what was expected: "
                        + e.getMessage());
    }

    /**
     * Files one peer's report the way that peer's own {@code EventStore.init} would.
     *
     * <p>In a background task because every member has to be at the barrier before
     * any of them leaves it — a peer reporting from the main thread would deadlock
     * the barrier it is part of. The returned future carries the peer's own outcome
     * so a failure there surfaces as a test failure rather than as a timeout.
     */
    private CompletableFuture<Integer> report(HazelcastInstance hz, int redoRecordsRead,
                                              long checkpointRecords) {
        return CompletableFuture.supplyAsync(
                () -> HotRestore.awaitClusterReplay(hz, redoRecordsRead, checkpointRecords, 2, WAIT),
                peerThreads);
    }

    /** {@link DailyCheckpoint.Rows} over one plain map, which is all this test dumps. */
    private static final class SingleMapRows implements DailyCheckpoint.Rows {

        private static final String MAP = "test.CheckpointedRecord";

        private final Map<String, Message> rows;

        SingleMapRows(Map<String, Message> rows) {
            this.rows = rows;
        }

        @Override
        public Collection<String> mapNames() {
            return List.of(MAP);
        }

        @Override
        public Collection<Map.Entry<String, Message>> entries(String mapName) {
            return new ArrayList<>(rows.entrySet());
        }

        @Override
        public boolean remove(String mapName, String key, Message expected) {
            return rows.remove(key, expected);
        }
    }
}
