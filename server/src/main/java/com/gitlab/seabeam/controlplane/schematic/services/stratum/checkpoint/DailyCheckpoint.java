// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;

/**
 * The daily record-lifecycle transaction: purge, dump, truncate, in that order
 * and only in that order.
 *
 * <p>Per the 2026-09-20 record lifecycle decision, each pod runs this on its own
 * local replica on the existing cleanup cadence:
 *
 * <ol>
 *   <li><b>Purge.</b> Every row whose {@code (spec.deleted_flag)} is set and whose
 *       delete {@code (spec.store_epoch)} is older than the type's
 *       {@code deleted_max_age_seconds} is appended to the pod's tombstone log and
 *       then removed from memory and from this node's disk index.</li>
 *   <li><b>Dump.</b> Every row that survived is written to a new checkpoint,
 *       committed by one atomic rename. The rows come from the union of this
 *       node's on-disk indexes and its resident memory, never from memory alone:
 *       a row evicted on its idle clock is off-heap and would otherwise be absent
 *       from the dump that step 3 then truncates the redo log behind.</li>
 *   <li><b>Truncate.</b> Only once that rename has succeeded, the redo logs are
 *       truncated to the dump point.</li>
 * </ol>
 *
 * <h2>Why the order is the correctness argument</h2>
 * Failure at any step leaves the PREVIOUS checkpoint plus an un-truncated redo
 * log, which is a complete recovery set. So the transaction does not roll back and
 * has nothing to roll back: it aborts, and the outcome of an aborted run is "try
 * again tomorrow", never data loss. That holds only while the truncate is strictly
 * last — truncating before the new dump is committed would discard the tail that
 * the still-current OLD checkpoint needs.
 *
 * <p>The sweep is idempotent for the same reason: a row purged in a partial run and
 * seen again on retry is simply purged again, and the second tombstone line is a
 * duplicate in a write-only log rather than a conflict.
 *
 * <h2>Why the memory tier arrives through an interface</h2>
 * {@link Rows} is the three operations this transaction performs on the memory
 * tier and nothing else. Production passes {@link DurableRows}; a test passes a
 * plain map, which is what lets the purge step be exercised over a record type
 * carrying {@code (spec.deleted_flag)} — a marker no fleet type declares yet,
 * because the annotation is on tacplus's unmerged branch. Without the seam the
 * only rows available to test with are rows the purge can never select.
 */
public final class DailyCheckpoint {

    private static final Logger log = LoggerFactory.getLogger(DailyCheckpoint.class);

    /** The memory tier, as this transaction needs it. */
    public interface Rows {
        /** Every record type's Hazelcast map name. */
        Collection<String> mapNames();

        /** One type's rows. */
        Collection<Map.Entry<String, Message>> entries(String mapName);

        /**
         * Removes {@code key} only while it still holds {@code expected}. Conditional
         * because a row republished between the scan and the remove has a fresh
         * store epoch — it may no longer be deleted, and it is certainly no longer
         * the row the tombstone line describes.
         */
        boolean remove(String mapName, String key, Message expected);
    }

    /** Removes a purged key from this node's on-disk index for that type. */
    public interface DiskPurge {
        /**
         * Purges nothing. For a deployment that opened no on-disk record leg, and
         * for the tests that run the transaction over plain maps.
         *
         * <p>This was production's wiring between 2026-09-20 and the disk leg
         * landing, on the reasoning that a pod's only durable copy of a row was the
         * checkpoint and step 2 rewrote that from what survived step 1. That
         * reasoning no longer holds: {@code RecordDisk} keeps a Lucene index per
         * record type, it is where an idle-evicted row lives, and step 2 now dumps
         * the union of it and memory. A purge that removed the row from memory
         * alone would leave the index's copy to be dumped straight back into the
         * next checkpoint — i.e. the row would be tombstoned and then resurrected,
         * every day, forever. So production passes a real purge and this stays for
         * the case it names.
         */
        DiskPurge NONE = (mapName, key) -> { };

        void purge(String mapName, String key) throws IOException;
    }

    /** Truncates this node's redo logs to the dump point. */
    public interface Truncate {
        void toDumpPoint();
    }

    /** What one run did. */
    public static final class Result {
        public final long purged;
        public final long dumped;
        public final Path checkpoint;

        Result(long purged, long dumped, Path checkpoint) {
            this.purged = purged;
            this.dumped = dumped;
            this.checkpoint = checkpoint;
        }
    }

    private final Rows rows;
    private final DiskPurge disk;
    private final TombstoneLog tombstones;
    private final Checkpoint checkpoint;
    private final Truncate truncate;

    public DailyCheckpoint(Rows rows, DiskPurge disk, TombstoneLog tombstones,
                           Checkpoint checkpoint, Truncate truncate) {
        this.rows = rows;
        this.disk = disk;
        this.tombstones = tombstones;
        this.checkpoint = checkpoint;
        this.truncate = truncate;
    }

    /**
     * Runs one transaction.
     *
     * @param deletedMaxAgeMs how long a flagged-deleted row survives after the
     *                        delete write
     * @param nowMs           this run's clock, read once: the purge cutoff and the
     *                        checkpoint's name come from the same instant, so the
     *                        dump is exactly "what survived at nowMs"
     * @throws IOException when the transaction aborted; the previous checkpoint and
     *                     the un-truncated redo logs remain a complete recovery set
     */
    public Result run(long deletedMaxAgeMs, long nowMs) throws IOException {
        long purged = purge(nowMs - deletedMaxAgeMs, nowMs);
        List<Checkpoint.Row> surviving = dump();
        Path committed = checkpoint.write(nowMs, surviving);
        truncate.toDumpPoint();
        log.info("daily checkpoint complete: {} row(s) purged, {} row(s) dumped to {},"
                + " redo logs truncated", purged, surviving.size(), committed);
        return new Result(purged, surviving.size(), committed);
    }

    /**
     * Step 1. The tombstone line is written BEFORE the removal: a crash between the
     * two leaves a line for a row that is still present, which the next sweep writes
     * again and which is harmless, while the other order can lose the row with no
     * trace left anywhere.
     */
    private long purge(long cutoffMs, long nowMs) throws IOException {
        long purged = 0;
        for (String mapName : rows.mapNames()) {
            for (Map.Entry<String, Message> entry : rows.entries(mapName)) {
                Message row = entry.getValue();
                if (!RecordAnnotations.isDeleted(row)
                        || RecordAnnotations.storeEpochMs(row) >= cutoffMs) {
                    continue;
                }
                tombstones.append(mapName, entry.getKey(), row, nowMs);
                if (!rows.remove(mapName, entry.getKey(), row)) {
                    // Republished under us. The row is live again at a fresh epoch, so
                    // it is not this sweep's to remove and its disk copy must stay.
                    continue;
                }
                disk.purge(mapName, entry.getKey());
                purged++;
            }
        }
        return purged;
    }

    /** Step 2's input: every row of every type that survived step 1. */
    private List<Checkpoint.Row> dump() {
        List<Checkpoint.Row> out = new ArrayList<>();
        for (String mapName : rows.mapNames()) {
            for (Map.Entry<String, Message> entry : rows.entries(mapName)) {
                out.add(new Checkpoint.Row(mapName, entry.getKey(), entry.getValue()));
            }
        }
        return out;
    }
}
