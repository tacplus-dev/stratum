// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

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

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.Codec;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.StorePath;

/**
 * The node's own redo logs — one per hot record type, on this pod's volume — and
 * the single thread that writes them.
 *
 * <h2>Per node, no leader</h2>
 * {@link RedoLog} is single-writer, single-process, local-filesystem only. Three
 * instances therefore own three logs, each satisfying that contract untouched,
 * and the cluster-level durability property is composed above the log rather
 * than inside it. Each log is PARTIAL — it holds the rows this node owned or
 * backed up — so recovery is the union of the reachable nodes' logs, resolved by
 * epoch ({@link HotRestore}).
 *
 * <h2>Why the node-local handle is reachable statically</h2>
 * A redo append rides the Hazelcast write as an {@link RedoAppendProcessor},
 * which is SERIALIZED to the partition owner and to its backup and deserialized
 * there. A deserialized entry processor carries no reference to anything on the
 * receiving member, so the only way its body can reach that member's own log is a
 * node-local lookup — this registry. It is set once at init and cleared at
 * shutdown; a second live instance in one JVM would be a second process by
 * RedoLog's contract, so {@link #install} refuses it rather than let two writers
 * share an append stream.
 *
 * <h2>The disk append is off the ack path</h2>
 * The hot-path safety bar is in-memory replication across
 * {@code min_durable_copies} instances; disk is not the blocker. So
 * {@link #appendAsync} hands the record to ONE thread and returns, and the entry
 * processor bodies — primary and backup alike — never wait on a file write. One
 * thread, not a pool: appends then land in the order the partition threads
 * produced them, so a key's own updates cannot reorder against each other. It
 * also makes this the log's single writer in fact as well as in contract, which
 * a set of Hazelcast partition threads calling {@code append} directly was not.
 *
 * <p>What the asynchrony costs, stated: a process killed between the in-memory
 * ack and the append loses that append. The row is not lost while any replica
 * that also queued it survives — the union in {@link HotRestore} recovers it from
 * the peer's log — and it is still on the event log besides. A simultaneous loss
 * of every replica inside that window does lose the write's disk copies, which is
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
     * This node's log for {@code mapName}, or null when the node keeps no redo log
     * for it. Called from entry-processor bodies on whichever member they landed
     * on — the primary resolves its own log, the backup resolves its own.
     */
    public static RedoLog logFor(String mapName) {
        HotRedoLogs active = ACTIVE.get();
        return active == null ? null : active.logs.get(mapName);
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
