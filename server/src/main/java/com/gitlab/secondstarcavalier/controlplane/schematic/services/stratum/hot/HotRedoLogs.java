// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
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

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.Codec;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.StorePath;

/**
 * The node's own redo logs — one per hot record type, on this pod's volume — and
 * the single thread that writes them.
 *
 * <h2>Per WRITER, no leader</h2>
 * {@link RedoLog} is single-writer, single-process, local-filesystem only. Each
 * writer therefore owns its own logs, satisfying that contract untouched, and the
 * cluster-level durability property is composed above the log rather than inside it.
 * A search member opens none: it has no volume, and it is never counted as a durable
 * copy or as a restore contributor.
 *
 * <p>Each writer's log is COMPLETE, not partial. Every write is sent to every writer
 * ({@link RecordPersistTask}) whichever member owns the partition, so a writer holds
 * the whole store on disk and can restore it alone; recovery is still run as the
 * union of the reachable writers' logs, resolved by epoch ({@link HotRestore}),
 * because a writer that joined after some of those writes has a log that starts
 * later than its peers'.
 *
 * <h2>Why the node-local handle is reachable statically</h2>
 * A redo append arrives as a {@link RecordPersistTask}, which is SERIALIZED to
 * every writer member and deserialized there. A deserialized task carries no
 * reference to anything on the receiving member, so the only way its body can reach
 * that member's own log is a node-local lookup — this registry. It is set once at
 * init and cleared at
 * shutdown; a second live instance in one JVM would be a second process by
 * RedoLog's contract, so {@link #install} refuses it rather than let two writers
 * share an append stream.
 *
 * <h2>The disk append is off the ack path</h2>
 * The hot-path safety bar is the write's in-memory replication; disk is not the
 * blocker. So {@link #appendAsync} hands the record to ONE thread and returns, and
 * the persist task never waits on a file write. One thread, not a pool: appends then
 * land in the order this member received them, and it makes this the log's single
 * writer in fact as well as in contract, which a set of Hazelcast partition threads
 * calling {@code append} directly was not. Ordering ACROSS writers is not preserved
 * and does not need to be — {@link HotRestore} resolves every replayed record by its
 * store epoch.
 *
 * <p>What the asynchrony costs, stated: a process killed between the in-memory
 * ack and the append loses that append. The row is not lost while any writer
 * that also queued it survives — the union in {@link HotRestore} recovers it from
 * that writer's log — and it is still on the event log besides. A simultaneous loss
 * of every writer inside that window does lose the write's disk copies, which is
 * the accepted, named consequence of acking on memory replication instead of on
 * the write.
 */
public final class HotRedoLogs implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HotRedoLogs.class);

    /** Where the hot redo logs live under the pod's volume. */
    public static final String HOT_SUBDIR = "hot";

    /**
     * The node-local registry the entry-processor bodies resolve through. Not a
     * cache and not a singleton for convenience: see the class note.
     */
    private static final AtomicReference<HotRedoLogs> ACTIVE = new AtomicReference<>();

    private final Map<String, RedoLog> logs = new LinkedHashMap<>();

    /** The asynchronous append thread. See the class note for why there is one. */
    private final ExecutorService writePool =
            Executors.newSingleThreadExecutor(daemon("stratum-hot-redo-writer"));

    private final AtomicLong appended = new AtomicLong();
    private final AtomicLong appendFailures = new AtomicLong();

    /**
     * Opens one log per dataset under {@code dataRoot/hot/}, and installs this as
     * the node's active registry.
     */
    public HotRedoLogs(Path dataRoot, Iterable<HotDataset> datasets, int archiveCount, Codec codec) {
        Path base = dataRoot.resolve(HOT_SUBDIR);
        for (HotDataset d : datasets) {
            logs.put(d.mapName, RedoLog.open(base, StorePath.ROOT_TENANT, d.mapName, archiveCount, codec));
        }
        install(this);
        log.info("hot redo logs opened under {}: {}", base, logs.keySet());
    }

    private static ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }

    private static void install(HotRedoLogs instance) {
        if (!ACTIVE.compareAndSet(null, instance)) {
            throw new IllegalStateException(
                    "a HotRedoLogs is already active in this JVM; a redo log is single-writer,"
                            + " single-process, so a second one would share an append stream with it");
        }
    }

    /**
     * Queues {@code rec} for this node's {@code mapName} log and returns at once,
     * WITHOUT waiting for it to reach disk.
     *
     * <p>Called from the entry-processor bodies, i.e. on a Hazelcast partition
     * thread on the partition owner and on its synchronous backup. The queueing is
     * what keeps the ~2 file writes of one hot write off the ack path.
     *
     * @return false when this node keeps no log for {@code mapName} — the state of
     *         a node with no disk_persistence configured for the type — so nothing
     *         was queued.
     */
    public static boolean appendAsync(String mapName, RedoLog.Record rec) {
        HotRedoLogs active = ACTIVE.get();
        if (active == null) {
            return false;
        }
        RedoLog redoLog = active.logs.get(mapName);
        if (redoLog == null) {
            return false;
        }
        active.writePool.execute(() -> {
            try {
                redoLog.append(rec);
                active.appended.incrementAndGet();
            } catch (Exception e) {
                active.appendFailures.incrementAndGet();
                log.error("hot redo append failed for {} key={}: {}", mapName, rec.key, e.getMessage(), e);
            }
        });
        return true;
    }

    /** This node's log for {@code mapName}. */
    public RedoLog log(String mapName) {
        return logs.get(mapName);
    }

    /**
     * Truncates every log to the checkpoint just committed — step 3 of the daily
     * transaction, and the only caller.
     *
     * <p>{@link RedoLog#roll} rebuilds the baseline from a caller-supplied snapshot
     * and folds in whatever arrived while it ran. The snapshot passed here is EMPTY
     * because the checkpoint file IS the baseline now: what has to survive is
     * exactly the tail, the writes that landed after the dump, and that is what
     * roll's fold leaves behind. Passing the dumped rows instead would write the
     * whole store to disk a second time and make cold start replay it twice.
     *
     * <p>Called only after the dump's rename has succeeded. Before it, the previous
     * checkpoint is still the one {@code current} names, and this tail is what makes
     * that checkpoint a complete recovery set.
     */
    public void truncateToCheckpoint() {
        for (Map.Entry<String, RedoLog> e : logs.entrySet()) {
            e.getValue().roll(List.of());
            log.info("hot redo log for {} truncated to the checkpoint", e.getKey());
        }
    }

    /** Blocks until every queued append has been attempted. Tests and shutdown only. */
    public void awaitAppends(long timeoutMs) throws InterruptedException {
        CountDownLatch drained = new CountDownLatch(1);
        writePool.execute(drained::countDown);
        drained.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Occupies the append thread until {@code release} counts down. Tests only:
     * it is how "the ack does not wait on disk" is asserted rather than assumed.
     */
    void blockWriter(CountDownLatch release) {
        writePool.execute(() -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    public long appendedCount()      { return appended.get(); }
    public long appendFailureCount() { return appendFailures.get(); }

    @Override
    public void close() {
        ACTIVE.compareAndSet(this, null);
        writePool.shutdown();
        try {
            if (!writePool.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("hot redo writer did not drain in 10s; queued appends are lost"
                        + " (memory and the peers' logs still hold them)");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (RedoLog l : logs.values()) {
            l.close();
        }
    }
}
