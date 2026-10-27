// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import com.google.protobuf.Message;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.tacplusdev.stratum.StratumHotStores;
import com.github.tacplusdev.stratum.hot.HotDataset;
import com.github.tacplusdev.stratum.hot.HotRedoLogs;
import com.github.tacplusdev.stratum.hot.HotRestore;
import com.github.tacplusdev.stratum.hot.HotWrites;
import com.github.tacplusdev.stratum.model.chassis.diskstore.Codec;
import com.github.tacplusdev.stratum.model.types.services.letsencrypt.Letsencrypt;
import com.github.tacplusdev.stratum.model.types.services.letsencrypt.Helpers;

/**
 * Restoring every writer from a disk snapshot is the checkpoint plus the redo
 * tail AS OF THE SNAPSHOT, and the event log has to make up the rest.
 *
 * <p>A snapshot of one writer's volume is a crash-consistent copy of its
 * checkpoint and redo tail, so a cold start from it recovers exactly what had
 * reached that disk when it was taken. Everything published after that is only
 * on the event log — and stratum's per-pod consumer groups have already
 * committed past it, because eventbus commits as soon as a batch is pushed to the
 * stream. The restored pods come back under the same names, so they resume at
 * those offsets. The pair below states both halves: resuming loses the rows, and
 * rewinding the groups to the snapshot time brings them back durably.
 *
 * <p>The event log here is one partition held in a list, and {@link #deliver} is
 * the generated subscription: stamp {@code store_epoch_ms}, then write through
 * {@link HotWrites}.
 */
class SnapshotRestoreReplayTest {

    private static final String MAP = "letsencrypt.LetsEncryptCert";

    /** One partition of the record topic. The offset is the index. */
    private record Published(long publishedAtMs, Letsencrypt.LetsEncryptCert payload) {}

    /** What the scenario hands each test once the writers are back from the snapshot. */
    private record Restored(long committedOffset, long snapshotAtMs) {}

    private final List<Published> topic = new ArrayList<>();
    private final AtomicLong publishClock = new AtomicLong(1_000L);
    // Monotonic across the restore, as wall time is: a redelivered record is
    // stamped later than anything the snapshot holds.
    private final AtomicLong epochClock = new AtomicLong(100L);
    private long delivered;

    private HazelcastInstance hz;
    private HotRedoLogs logs;

    @AfterEach
    void tearDown() {
        if (logs != null) {
            logs.close();
            logs = null;
        }
        if (hz != null) {
            hz.shutdown();
            hz = null;
        }
    }

    // ------------------------------------------------------------------ fixtures

    private static HotDataset dataset() {
        return new HotDataset(MAP, 1,
                m -> ((Letsencrypt.LetsEncryptCert) m).getCertSetId(),
                m -> ((Letsencrypt.LetsEncryptCert) m).getIssuedAt(),
                "issued_at",
                m -> {
                    try {
                        return Helpers.letsEncryptCertToJson((Letsencrypt.LetsEncryptCert) m);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(e);
                    }
                },
                json -> {
                    try {
                        return Helpers.letsEncryptCertFromJson(json);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
    }

    /** As published: no store epoch, which only the subscription stamps. */
    private static Letsencrypt.LetsEncryptCert event(String id, String version) {
        return Letsencrypt.LetsEncryptCert.newBuilder()
                .setCertSetId(id)
                .setSpiffeId(version)
                .build();
    }

    private HazelcastInstance member(String cluster) {
        Config cfg = new Config();
        cfg.setClusterName(cluster);
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(cfg);
    }

    private void write(HotWrites hot, IMap<String, Message> map, Letsencrypt.LetsEncryptCert e) {
        hot.write(map, MAP, e.getCertSetId(), e, 1,
                () -> Helpers.letsEncryptCertToJson(e), e.getIssuedAt());
    }

    private void publish(String id, String version) {
        topic.add(new Published(publishClock.addAndGet(10L), event(id, version)));
    }

    /**
     * The generated subscription, from {@code fromOffset} to the end of the
     * partition. Returns the offset eventbus commits once the batch is pushed.
     */
    private long deliver(long fromOffset) {
        HotWrites hot = new HotWrites(hz, 0);
        IMap<String, Message> map = hz.getMap(MAP);
        for (int offset = (int) fromOffset; offset < topic.size(); offset++) {
            Letsencrypt.LetsEncryptCert stamped = topic.get(offset).payload().toBuilder()
                    .setIssuedAt(epochClock.addAndGet(10L))
                    .build();
            write(hot, map, stamped);
            delivered++;
        }
        return topic.size();
    }

    /** Kafka's offsetsForTimes: the first offset published at or after {@code ts}. */
    private long offsetForTimestamp(long ts) {
        for (int offset = 0; offset < topic.size(); offset++) {
            if (topic.get(offset).publishedAtMs() >= ts) {
                return offset;
            }
        }
        return topic.size();
    }

    /**
     * Waits until every delivered write has reached this node's log. The persist
     * task is itself queued on a Hazelcast executor before it queues the append,
     * so draining the append thread alone can run ahead of it.
     */
    private void settle() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (logs.appendedCount() < delivered && System.currentTimeMillis() < deadline) {
            logs.awaitAppends(100);
        }
        logs.awaitAppends(5_000);
        assertEquals(delivered, logs.appendedCount(), "every acked write must be on disk before the copy");
    }

    private static void copyTree(Path from, Path to) throws IOException {
        try (Stream<Path> paths = Files.walk(from)) {
            for (Path p : (Iterable<Path>) paths::iterator) {
                Path target = to.resolve(from.relativize(p).toString());
                if (Files.isDirectory(p)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(p, target);
                }
            }
        }
    }

    private static void deleteContents(Path dir) throws IOException {
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                if (!p.equals(dir)) {
                    Files.delete(p);
                }
            }
        }
    }

    /** Cold start exactly as EventStore does it: the checkpoint putIfAbsent, then the redo tail. */
    private IMap<String, Message> coldStart(Path volume, String cluster) throws IOException {
        hz = member(cluster);
        logs = new HotRedoLogs(volume, List.of(dataset()), 3, Codec.NONE);
        delivered = 0;
        IMap<String, Message> map = hz.getMap(MAP);
        assertEquals(0, map.size(), "a cold start begins with the store empty cluster-wide");
        for (Checkpoint.Row row : new Checkpoint(volume).load()) {
            map.putIfAbsent(row.key, row.value);
        }
        HotRestore.Result replayed = HotRestore.replay(logs.log(MAP), dataset(), map);
        assertTrue(replayed.diverged.isEmpty(), "a restamped redelivery must never look like divergence");
        return map;
    }

    /**
     * Steps 1 to 4: a checkpoint and a redo tail, the snapshot, three more acked
     * writes, every writer lost, and a cold start from the snapshot.
     */
    private Restored snapshotThenLoseEveryWriter(Path dataRoot, Path snapshot, Path restored)
            throws Exception {
        hz = member("stratum-snapshot-restore-before");
        logs = new HotRedoLogs(dataRoot, List.of(dataset()), 3, Codec.NONE);

        // A checkpoint, then a tail over it: one new row and one update.
        publish("r1", "v1");
        publish("r2", "v1");
        publish("r3", "v1");
        long committed = deliver(0);
        settle();
        new DailyCheckpoint(new DurableRows(hz, null, List.of(MAP)),
                (m, k) -> { },
                new TombstoneLog(dataRoot),
                new Checkpoint(dataRoot),
                logs::truncateToCheckpoint)
                .run(30L * 24 * 60 * 60 * 1000, 1_000_000L);
        publish("r4", "v1");
        publish("r2", "v2");
        committed = deliver(committed);
        settle();

        // The snapshot: a crash-consistent copy of a live writer's volume.
        copyTree(dataRoot, snapshot);
        long snapshotAt = publishClock.addAndGet(10L);

        // After the snapshot: acked into memory, on disk, and committed past.
        publish("r5", "v1");
        publish("r6", "v1");
        publish("r3", "v2");
        committed = deliver(committed);
        settle();
        assertEquals(6, hz.getMap(MAP).size());

        // Every writer is gone, volume and all.
        logs.close();
        logs = null;
        hz.shutdown();
        hz = null;
        deleteContents(dataRoot);

        copyTree(snapshot, restored);
        return new Restored(committed, snapshotAt);
    }

    private static String version(IMap<String, Message> map, String id) {
        Letsencrypt.LetsEncryptCert e = (Letsencrypt.LetsEncryptCert) map.get(id);
        return e == null ? null : e.getSpiffeId();
    }

    // ------------------------------------------------------------------ tests

    /**
     * Today's behaviour, and the control for the test below. The restored pods
     * keep their names, so their {@code stratum-<POD_NAME>} groups resume at the
     * offsets they committed — which eventbus moved past r5, r6 and r3's update
     * the moment those were pushed to the stream. Nothing redelivers them, and
     * the snapshot never held them.
     */
    @Test
    void resumingAtTheCommittedOffsetLosesEveryRowWrittenAfterTheSnapshot(
            @TempDir Path dataRoot, @TempDir Path snapshot, @TempDir Path restored) throws Exception {
        assumeFalse(StratumHotStores.datasets().isEmpty(),
                "no record in this fleet declares (spec.store_epoch); the hot tier has nothing to log");
        Restored r = snapshotThenLoseEveryWriter(dataRoot, snapshot, restored);
        IMap<String, Message> map = coldStart(restored, "stratum-snapshot-restore-resume");

        assertEquals(r.committedOffset(), deliver(r.committedOffset()));
        assertEquals(0, delivered, "the committed offset is the end of the partition");

        assertEquals(4, map.size());
        assertEquals("v1", version(map, "r1"));
        assertEquals("v2", version(map, "r2"), "the snapshot's tail holds r2's update");
        assertEquals("v1", version(map, "r3"), "r3's update landed after the snapshot and is gone");
        assertEquals("v1", version(map, "r4"));
        assertNull(map.get("r5"), "r5 was acked, then lost with every writer");
        assertNull(map.get("r6"), "r6 was acked, then lost with every writer");
    }

    /**
     * Rewinding the groups to before the snapshot redelivers what the snapshot
     * missed, and the margin redelivers a row it already held too — restamped
     * later, so it simply wins over its own older copy. The rows are then durable
     * again: a second cold start from the same restored volume, with no delivery
     * at all, gets every one of them back.
     */
    @Test
    void rewindingTheGroupToTheSnapshotBringsBackEveryRowWrittenAfterIt(
            @TempDir Path dataRoot, @TempDir Path snapshot, @TempDir Path restored) throws Exception {
        assumeFalse(StratumHotStores.datasets().isEmpty(),
                "no record in this fleet declares (spec.store_epoch); the hot tier has nothing to log");
        Restored r = snapshotThenLoseEveryWriter(dataRoot, snapshot, restored);
        IMap<String, Message> map = coldStart(restored, "stratum-snapshot-restore-rewind");

        long from = offsetForTimestamp(r.snapshotAtMs() - 15L);
        assertEquals(4, from, "the margin reaches back to r2's update, which the snapshot holds");
        assertEquals(r.committedOffset(), deliver(from));
        assertEquals(4, delivered);

        assertEquals(6, map.size());
        assertEquals("v1", version(map, "r1"));
        assertEquals("v2", version(map, "r2"));
        assertEquals("v2", version(map, "r3"));
        assertEquals("v1", version(map, "r4"));
        assertEquals("v1", version(map, "r5"));
        assertEquals("v1", version(map, "r6"));

        // Durable again, not merely back in memory.
        settle();
        logs.close();
        logs = null;
        hz.shutdown();
        hz = null;

        IMap<String, Message> again = coldStart(restored, "stratum-snapshot-restore-again");
        assertEquals(6, again.size());
        assertEquals("v1", version(again, "r1"));
        assertEquals("v2", version(again, "r2"));
        assertEquals("v2", version(again, "r3"), "the redelivered update must outrank the checkpoint's copy");
        assertEquals("v1", version(again, "r4"));
        assertEquals("v1", version(again, "r5"));
        assertEquals("v1", version(again, "r6"));
    }
}
