// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.disk;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;

import com.github.tacplusdev.stratum.checkpoint.RecordAnnotations;

/**
 * The node's on-disk record leg — one {@link RecordIndex} per record type on this
 * pod's volume — and the single thread that writes them.
 *
 * <h2>What this leg is for</h2>
 * Three things, and it is worth being exact because a fourth keeps suggesting
 * itself and is NOT one of them:
 *
 * <ol>
 *   <li>A row Hazelcast evicted on its {@code (spec.record).residency.idle_seconds}
 *       clock is still here, so the eviction is a cache miss and not a deletion.
 *       {@link RecordDiskLoader} is what turns the miss back into a hit.</li>
 *   <li>{@code DailyCheckpoint}'s dump has a complete picture to write: the rows
 *       that are off-heap at dump time are on disk, and the dump is the union of
 *       the two (see {@code checkpoint/DurableRows}).</li>
 *   <li>The purge step has a second index to remove a dead row FROM, which is what
 *       {@code DailyCheckpoint.DiskPurge} was a no-op for until this existed.</li>
 * </ol>
 *
 * <p>It is NOT the crash-recovery record. The redo log is, and stays so: it is
 * ordered, it is append-only, it carries the store epoch the restore merge
 * resolves on. This leg is a per-key latest-value store with no ordering and only
 * the stale-write guard in {@link #writeAsync}, so nothing about recovery is moved
 * onto it and nothing here duplicates it.
 *
 * <h2>Only a WRITER has one</h2>
 * A search member has no volume, so it opens no leg and none of the three uses
 * above applies to it: it keeps no index, runs no checkpoint, and — because
 * {@code StratumMapConfig.durableMap} gives a map the idle clock only where the leg
 * exists — never evicts a row it would have nothing to answer for. A writer's index
 * holds EVERY row in the store, not just the partitions this member happens to own,
 * because the persist fans out to every writer ({@code HotWrites}).
 *
 * <h2>Why the node-local handle is reachable statically</h2>
 * Exactly the reason {@link com.github.tacplusdev.stratum.hot.HotRedoLogs}
 * is: the disk write arrives as a {@code RecordPersistTask} submitted to this
 * member, deserialized here, and a deserialized task carries no reference to
 * anything on the receiving member. A node-local registry is the only way its body
 * can reach that member's own indexes.
 *
 * <p>{@link RecordDiskLoader} resolves through the same registry rather than
 * holding an index reference, because the loaders are built by
 * {@code StratumMapConfig.build} — before the Hazelcast instance exists, and in
 * tests that never open a volume at all. A loader with no registry behind it
 * answers null, which is the pre-disk-leg behaviour and is safe: nothing is
 * evicted in that configuration either, because the max-idle clock and this leg
 * are wired together.
 *
 * <h2>The disk write is off the ack path</h2>
 * Same discipline, and the same single thread, as the redo append. The ack
 * condition is the write's in-memory replication; a Lucene commit is not on it. One
 * thread rather than a pool so the appends land in the order this member received
 * them; ordering ACROSS members is what the stale-write guard covers.
 *
 * <p>What the asynchrony costs, stated: a process killed between the in-memory ack
 * and the commit leaves that row absent from this node's index. It is not lost —
 * it is in memory, it is in the other writers' logs, and the checkpoint dump reads
 * memory as well as disk precisely so a row that has not reached disk yet is still
 * dumped. The window matters only for a row that is ALSO idle-evicted before it
 * ever reached disk, which cannot happen: eviction needs a full idle window of no
 * touch, and the write that just happened was a touch.
 */
public final class RecordDisk implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RecordDisk.class);

    /** Where the record indexes live under the pod's volume. */
    public static final String RECORDS_SUBDIR = "records";

    /**
     * The node-local registry the entry-processor bodies and the map loaders
     * resolve through. Not a cache and not a singleton for convenience: see the
     * class note.
     */
    private static final AtomicReference<RecordDisk> ACTIVE = new AtomicReference<>();

    private final Map<String, RecordIndex> indexes = new LinkedHashMap<>();

    /** The asynchronous write thread. See the class note for why there is one. */
    private final ExecutorService writePool =
            Executors.newSingleThreadExecutor(daemon("stratum-record-disk-writer"));

    /**
     * How long a flagged-deleted row may still be loaded back into memory, in
     * millis. Read per load rather than captured, because {@code EventStore} reads
     * its own source for it once at init and a supplier keeps the two from drifting
     * into two numbers.
     */
    private final java.util.function.LongSupplier deletedMaxAgeMs;

    private final AtomicLong written = new AtomicLong();
    private final AtomicLong writeFailures = new AtomicLong();
    /** Writes refused because the index already held a newer row — see {@link #writeAsync}. */
    private final AtomicLong staleWrites = new AtomicLong();
    private final AtomicLong loaded = new AtomicLong();
    private final AtomicLong refusedDead = new AtomicLong();

    /**
     * Opens one index per record type under {@code dataRoot/records/}, and installs
     * this as the node's active registry.
     *
     * <p>The indexes are objects, not directories: {@link RecordIndex} opens its
     * Lucene directory on first use, so a type this deployment never sees costs a
     * map entry and nothing on the volume.
     *
     * @param mapNames        every record map name, i.e. {@code StratumEventStores.mapNames()}
     * @param deletedMaxAgeMs how long a flagged-deleted row may still be loaded
     */
    public RecordDisk(Path dataRoot, Iterable<String> mapNames,
                      java.util.function.LongSupplier deletedMaxAgeMs) {
        Path base = dataRoot.resolve(RECORDS_SUBDIR);
        for (String mapName : mapNames) {
            indexes.put(mapName, new RecordIndex(mapName, base.resolve(mapName)));
        }
        this.deletedMaxAgeMs = deletedMaxAgeMs;
        install(this);
        log.info("record disk leg opened under {}: {}", base, indexes.keySet());
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }

    private static void install(RecordDisk instance) {
        if (!ACTIVE.compareAndSet(null, instance)) {
            throw new IllegalStateException(
                    "a RecordDisk is already active in this JVM; each index is a single-writer"
                            + " Lucene directory, so a second one would share a writer lock with it");
        }
    }

    /** The node's active leg, or null when this JVM has none open. */
    public static RecordDisk active() {
        return ACTIVE.get();
    }

    /** This node's index for {@code mapName}, or null when it is not a record map. */
    public RecordIndex index(String mapName) {
        return indexes.get(mapName);
    }

    /** Every record type this leg holds an index for. */
    public java.util.Collection<String> mapNames() {
        return indexes.keySet();
    }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /**
     * Queues {@code value} for this node's {@code mapName} index and returns at
     * once, WITHOUT waiting for it to reach disk.
     *
     * <p>Called from {@code RecordPersistTask}, i.e. on a Hazelcast executor thread
     * on every WRITER member of the cluster. The queueing is what keeps the Lucene
     * commit of one record write off the ack path.
     *
     * <p>An OLDER row never overwrites a newer one. The persist task reaches each
     * writer over its own connection, so two writes to one key can arrive in one
     * order here and the other order on the next writer — and this index is a
     * latest-value store with no merge of its own, so the later arrival would win
     * whatever it was. That is not a theoretical reordering: it is the price of
     * fanning the persist out to every writer rather than riding one partition's
     * replication, and the consequence would be a stale row served back after an
     * idle eviction. The redo log needs no such guard because {@code HotRestore}
     * resolves its records by the same epoch on the way in.
     *
     * <p>A record type carrying no {@code (spec.store_epoch)} reports 0 for both
     * rows, so the comparison never refuses one: such a type has no write order to
     * preserve and its last arrival is as good an answer as its first.
     *
     * @return false when this JVM has no leg open, or the name is not a record map,
     *         so nothing was queued
     */
    public static boolean writeAsync(String mapName, String key, Message value) {
        RecordDisk active = ACTIVE.get();
        if (active == null) {
            return false;
        }
        RecordIndex index = active.indexes.get(mapName);
        if (index == null) {
            return false;
        }
        active.writePool.execute(() -> {
            try {
                Message held = index.get(key);
                if (held != null
                        && RecordAnnotations.storeEpochMs(held) > RecordAnnotations.storeEpochMs(value)) {
                    active.staleWrites.incrementAndGet();
                    return;
                }
                index.put(key, value);
                active.written.incrementAndGet();
            } catch (Exception e) {
                active.writeFailures.incrementAndGet();
                log.error("record disk write failed for {} key={}: {}", mapName, key, e.getMessage(), e);
            }
        });
        return true;
    }

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    /**
     * The row this node's disk leg holds for {@code key}, or null.
     *
     * <p>A flagged-deleted row past its {@code deleted_max_age_seconds} answers
     * null even though the index still holds it, because the decision is explicit
     * that such a row "is never loaded into Hazelcast again": it is dead, and the
     * only reason it is still on disk is that the next daily sweep has not reached
     * it yet. Answering with it would make the purge's timing visible as
     * readability, which is the one thing a hard age cutoff is for.
     *
     * <p>Errors are logged and answered as null rather than thrown. This runs
     * inside Hazelcast's map-loader call, where a throw fails the {@code get} that
     * triggered it — turning one unreadable index into an error on every read of
     * every key of that type. Null is "memory is all there is", which is exactly
     * the state the store was in before this leg existed.
     */
    public static Message load(String mapName, String key) {
        RecordDisk active = ACTIVE.get();
        if (active == null) {
            return null;
        }
        RecordIndex index = active.indexes.get(mapName);
        if (index == null) {
            return null;
        }
        Message row;
        try {
            row = index.get(key);
        } catch (Exception e) {
            log.error("record disk read failed for {} key={}: {}", mapName, key, e.getMessage(), e);
            return null;
        }
        if (row == null) {
            return null;
        }
        if (RecordAnnotations.isDeleted(row)
                && RecordAnnotations.storeEpochMs(row)
                        < System.currentTimeMillis() - active.deletedMaxAgeMs.getAsLong()) {
            active.refusedDead.incrementAndGet();
            return null;
        }
        active.loaded.incrementAndGet();
        return row;
    }

    // -----------------------------------------------------------------------
    // Purge
    // -----------------------------------------------------------------------

    /**
     * Removes one key from this node's index — {@code DailyCheckpoint.DiskPurge}
     * over this leg.
     *
     * <p>Synchronous, unlike {@link #writeAsync}, and deliberately: the purge runs
     * once a day off every request path, and step 2 of the same transaction dumps
     * what survived step 1. A queued delete could land after that dump and put the
     * purged row back into the checkpoint it was supposed to be gone from.
     */
    public void purge(String mapName, String key) throws IOException {
        RecordIndex index = indexes.get(mapName);
        if (index != null) {
            index.delete(key);
        }
    }

    // -----------------------------------------------------------------------
    // Stats and lifecycle
    // -----------------------------------------------------------------------

    public long writtenCount()      { return written.get(); }
    public long writeFailureCount() { return writeFailures.get(); }
    public long staleWriteCount()   { return staleWrites.get(); }
    public long loadedCount()       { return loaded.get(); }
    public long refusedDeadCount()  { return refusedDead.get(); }

    /** Blocks until every queued write has been attempted. Tests and shutdown only. */
    public void awaitWrites(long timeoutMs) throws InterruptedException {
        CountDownLatch drained = new CountDownLatch(1);
        writePool.execute(drained::countDown);
        drained.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        ACTIVE.compareAndSet(this, null);
        writePool.shutdown();
        try {
            if (!writePool.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("record disk writer did not drain in 10s; queued writes are lost"
                        + " (memory and the redo logs still hold them)");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (RecordIndex index : indexes.values()) {
            index.close();
        }
    }
}
