// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;


import com.github.tacplusdev.stratum.checkpoint.Checkpoint;
import com.github.tacplusdev.stratum.checkpoint.DailyCheckpoint;
import com.github.tacplusdev.stratum.checkpoint.DurableRows;
import com.github.tacplusdev.stratum.checkpoint.TombstoneLog;
import com.github.tacplusdev.stratum.disk.ClassNameMigration;
import com.github.tacplusdev.stratum.disk.RecordDisk;
import com.github.tacplusdev.stratum.hazelcast.StratumMapConfig;
import com.github.tacplusdev.stratum.hazelcast.StratumRole;
import com.github.tacplusdev.stratum.hot.HotArchive;
import com.github.tacplusdev.stratum.hot.HotDataset;
import com.github.tacplusdev.stratum.hot.HotRedoLogs;
import com.github.tacplusdev.stratum.hot.HotRestore;
import com.github.tacplusdev.stratum.hot.HotWrites;
import com.github.tacplusdev.stratum.model.chassis.backoff.Backoff;
import com.github.tacplusdev.stratum.model.chassis.diskstore.ArchiveSink;
import com.github.tacplusdev.stratum.model.chassis.diskstore.Codec;
import com.github.tacplusdev.stratum.model.chassis.diskstore.GcsArchiveSink;
import com.github.tacplusdev.stratum.model.types.sys.diskstore.Diskstore.ArchiveConfig;
import com.github.tacplusdev.stratum.model.types.sys.diskstore.Diskstore.DiskPersistenceConfig;

/**
 * EventStore manages the Hazelcast IMap that backs stratum's hot event store.
 *
 * Data model: one typed Hazelcast IMap per record topic (StratumEventStores).
 *
 * Indexes (via ValueExtractor or Hazelcast native attribute reflection):
 *   - SORTED:  epoch, timestampMs
 *
 * Record residency:
 *   One clock per record type, and Hazelcast runs it: per-entry max-idle set
 *   from (spec.record).residency.idle_seconds (see StratumMapConfig). A row
 *   stays resident while it is read or written and is evicted from memory once
 *   it has gone idle_seconds with neither. Nothing here sweeps for it, and a
 *   read never restamps (spec.store_epoch) or appends to the redo log.
 *
 *   Underneath the clock is the read-through leg that makes eviction a cache
 *   miss rather than a deletion: one Lucene index per record type on this pod's
 *   volume ({@link RecordDisk}), written from the same entry processor that
 *   queues the redo append, and read back by the maps' MapLoader on a memory
 *   miss. The two are wired together in StratumMapConfig.durableMap so a map
 *   cannot get the clock without the leg.
 *
 *   This replaced the HOT/WARM/COLD residency classes, the separate
 *   retention_days sweep and the node-local Lucene tier they partitioned
 *   against, per the 2026-09-20 record lifecycle decision. The index here is
 *   NOT that tier returning: it has no slices, no epoch pruning and no
 *   disk-side query plane, because there are no tiers to partition against.
 *
 * Daily checkpoint:
 *   On the same cleanup_interval_seconds tick, and per pod rather than per
 *   cluster, {@link #runDailyCheckpoint()} runs the record-lifecycle transaction:
 *   purge flagged-deleted rows past their max age into the tombstone log, dump
 *   every surviving row to a new checkpoint, truncate the redo logs to it. It is
 *   also what makes cold start bounded — see {@link #loadCheckpoint()}.
 *
 * Thread safety: HazelcastInstance is thread-safe. AtomicLong stats are thread-safe.
 */
public class EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStore.class);

    private HazelcastInstance hz;
    // Generated, vservice-neutral write path: one typed IMap per EventBus topic.
    private final StratumEventStores stores = new StratumEventStores();
    private HotRedoLogs hotLogs;
    private HotWrites hotWrites;
    /**
     * False until every reachable instance has replayed its redo log. Hot reads
     * refuse until then: a node holding only its OWN partial log looks answerable
     * and is missing every row the other nodes owned, and serving that silently is
     * worse than a slow start.
     */
    private volatile boolean redoReplayDone;
    /**
     * False until the event-log cold-start replay has caught up, or until the
     * caller has said there was none to run.
     *
     * <p>This is a SECOND leg of the same gate, not a refinement of the first. The
     * redo barrier only covers record types that have a redo log; the types that
     * do not are recovered by the event-log replay,
     * which runs asynchronously and long after the redo barrier clears. Opening
     * reads on the redo barrier alone answers queries for those types out of a
     * store that is still filling — empty or partial results, indistinguishable
     * from "there is no such data", which is the same silent-incompleteness
     * failure the redo barrier exists to refuse.
     *
     * <p>It is the CALLER that sets this, because only the caller knows whether a
     * replay was needed: {@code StratumHandler} decides that from the cluster-wide
     * count and either starts a rebuild that signals on catch-up or declares
     * immediately that none was required. A deployment with nothing to replay
     * therefore opens as promptly as it did before rather than waiting on a
     * rebuild that was never going to run.
     */
    private volatile boolean coldStartReplayDone;
    /**
     * What this pod filed in the hot-restore ledger at its boot, so
     * {@link #repairRestoreReport} can put exactly that back and never a revision
     * of it. Zero until the barrier has been passed, which is also when the repair
     * first becomes meaningful.
     */
    private volatile int bootDurableRecords;
    private Checkpoint checkpoint;
    private DailyCheckpoint dailyCheckpoint;
    private RecordDisk recordDisk;
    /**
     * {@code grpcconfig.disk_persistence}: the hot logs' roll interval and the
     * archive they ship to ({@link HotArchive}). Init-time, like the rest of the
     * bootstrap config.
     */
    private final DiskPersistenceConfig persistence;
    private HotArchive hotArchive;
    private long deletedMaxAgeSeconds;
    /**
     * What this member is for. Set once at {@link #init} from {@link StratumRole#configured}
     * and never revised; a search member has no volume, so everything this class
     * opens on one is behind a check of it.
     */
    private StratumRole role = StratumRole.WRITER;

    /**
     * How long a cold start waits for its peers' redo replay before giving up.
     *
     * <p>Runs after the writer-quorum wait ({@link StratumMapConfig#withWriterQuorum}),
     * and deliberately sequential with it: this barrier cannot begin before the
     * cluster floor is met, because the replay it waits on opens
     * split-brain-protected maps and needs that many live members to have filed
     * reports. A quorum lost while it runs restarts it with a fresh timeout once the
     * quorum is back. None of this costs the pod its listener: the generated chassis
     * binds its grpc.health.v1 listener in {@code ServiceManager.create()} and holds
     * it at NOT_SERVING until bootstrap completes. See {@code StartupHealthGateTest}.
     */
    static final java.time.Duration RESTORE_TIMEOUT = java.time.Duration.ofMinutes(5);

    /**
     * How often a serving pod checks that its hot-restore ledger report is still
     * there, and puts it back if it is not ({@link #repairRestoreReport}).
     *
     * <p>Well inside {@link #RESTORE_TIMEOUT}, deliberately: a restarting peer waits
     * that long for a report from every live member, so a report that comes back
     * within a fraction of it costs that peer a slower start and nothing else. The
     * alternative — a report that never comes back — cost the 2026-09-22 incident
     * every restart of every other pod, permanently.
     */
    private static final long REPORT_REPAIR_SECONDS = 30;

    /** Archives retained per hot redo log, matching the diskstore proto default. */
    private static final int HOT_ARCHIVE_COUNT = 3;

    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor(
            r -> { Thread t = new Thread(r, "stratum-event-cleaner"); t.setDaemon(true); return t; });

    private final AtomicLong lastCheckpointMs     = new AtomicLong(0);
    private final AtomicLong lastCheckpointPurged = new AtomicLong(0);

    /**
     * The platform default for how long a flagged-deleted row survives after the
     * delete write, in seconds — 30 days.
     *
     * <p>PLACEHOLDER, deliberately flat and deliberately not per-type. The decision
     * this implements puts the real number on each record as
     * {@code (spec.record).residency.deleted_max_age_seconds}, and that annotation
     * is on tacplus's unmerged branch — there is no generated accessor to read it
     * from, and writing one by hand would be exactly the generated surface the
     * platform rules forbid. One value for the whole store is the honest stand-in:
     * it is correct for every type the moment the fleet annotation pass gives every
     * type the same number, and it is replaced by the per-type read when the
     * annotation lands, at the one call site in {@link #runDailyCheckpoint}.
     *
     * <p>Not a {@code Config} field because {@code Config} is a tacplus-owned proto
     * in another repository; {@code STRATUM_DELETED_MAX_AGE_SECONDS} overrides it,
     * the same shape {@code STRATUM_MIN_DURABLE_COPIES} already uses for a knob that
     * has no home in that proto.
     */
    static final long DEFAULT_DELETED_MAX_AGE_SECONDS = 30L * 24 * 60 * 60;

    /** Env name for the {@link #DEFAULT_DELETED_MAX_AGE_SECONDS} override. */
    static final String DELETED_MAX_AGE_ENV = "STRATUM_DELETED_MAX_AGE_SECONDS";

    // -----------------------------------------------------------------------
    // Init
    // -----------------------------------------------------------------------

    /** A store whose hot logs stay on the volume: no archive, no scheduled roll. */
    public EventStore() {
        this(DiskPersistenceConfig.getDefaultInstance());
    }

    public EventStore(DiskPersistenceConfig persistence) {
        this.persistence = persistence;
    }

    /**
     * Initialises the Hazelcast member and the record maps.
     *
     * @param cleanupIntervalSecs how often the daily checkpoint runs
     * @param seedMembers         list of "host:port" addresses for TCP/IP join;
     *                            empty → multicast (dev only)
     */
    public void init(long cleanupIntervalSecs, List<String> seedMembers) {
        int floorOverride = StratumMapConfig.configuredMinDurableCopies();
        int minDurableCopies = floorOverride > 0
                ? floorOverride
                : StratumMapConfig.DEFAULT_MIN_DURABLE_COPIES;
        // What this member is FOR, and the only branch in this method: a writer opens
        // the three things that live on a volume — the record indexes, the redo logs
        // and the checkpoint — and a search member opens none of them, because it has
        // no volume. Everything else below is identical, which is the point: a search
        // member is a full member of the cluster in memory and in the query plane.
        this.role = StratumRole.configured();

        // The on-disk record leg comes up BEFORE the Hazelcast member, because the
        // member's record maps carry a read-through loader that resolves this
        // node-local registry, and the first map proxy is opened a few lines below.
        // A loader with no leg behind it answers null, which is not wrong but is
        // the pre-leg store, and the window would be exactly the boot.
        this.deletedMaxAgeSeconds = configuredDeletedMaxAgeSeconds();
        if (role.isWriter()) {
            // Before anything reads the volume: a dump or index written when rows were
            // named by Java class is rewritten to protobuf full names first.
            try {
                ClassNameMigration.run(dataRoot(), StratumEventStores.mapNames());
            } catch (IOException e) {
                throw new IllegalStateException("durable format at " + dataRoot()
                        + " could not be migrated; refusing to start on a baseline that may be partial", e);
            }
            recordDisk = new RecordDisk(dataRoot(), StratumEventStores.mapNames(),
                    () -> deletedMaxAgeSeconds * 1000L);
        }

        hz = Hazelcast.newHazelcastInstance(StratumMapConfig.build(
                seedMembers, minDurableCopies, StratumMapConfig.configuredBackupCount(), role));

        // Every protected-map step from here to the end of the restore runs under the
        // writer quorum, and waits again if it is lost. newHazelcastInstance returns
        // when this member is up, not when the cluster is assembled, so the first pod
        // of the StatefulSet is briefly a cluster of one — and during a roll the peers
        // restart underneath this one. Either way every map below refuses with
        // SplitBrainProtectionException, which used to go straight out of init.
        hotWrites = new HotWrites(hz, floorOverride);
        StratumMapConfig.withWriterQuorum(hz, "opening the hot maps", () -> {
            stores.init(hz, hotWrites);
            return null;
        }, Backoff.THREAD_SLEEPER);

        // The hot tier's redo logs, on THIS pod's volume — writers only. One per
        // replayable record type; with no record carrying (spec.store_epoch) the list
        // is empty, the node opens no log, and the tier is exactly what it is today
        // plus the quorum gate.
        List<HotDataset> hotDatasets = StratumHotStores.datasets();
        ArchiveConfig archive = HotArchive.configFor(role, persistence);
        String archiveMember = archive == null ? null : HotArchive.member();
        if (role.isWriter()) {
            hotLogs = new HotRedoLogs(dataRoot(), hotDatasets, HOT_ARCHIVE_COUNT, Codec.NONE);
            if (archive != null) {
                openArchive(archive, archiveMember, hotDatasets);
            }

            // BEFORE the redo replay, and that order is the cold start: the checkpoint
            // is the baseline, the redo logs hold only the tail that landed after it
            // (see HotRedoLogs.truncateToCheckpoint), and replaying the tail onto the
            // baseline is what makes recovery proportional to the data rather than to
            // the whole history.
            checkpoint = new Checkpoint(dataRoot());
            loadCheckpoint();
        }
        // Both roles, and a search member reports nothing and waits: it is handed
        // partitions the moment it joins, so it must not answer a query before the
        // writers have finished recovering. See HotRestore.awaitClusterReplay.
        restoreHotTier(hotDatasets, minDurableCopies);

        if (role.isWriter()) {
            // The union of this node's disk indexes and its resident memory.
            // Memory alone is what made idle eviction destructive: an off-heap
            // row was absent from the dump and its redo entries were truncated
            // behind it. See DurableRows for why disk alone is not the answer
            // either.
            DurableRows durableRows = new DurableRows(hz, recordDisk, StratumEventStores.mapNames());
            this.dailyCheckpoint = new DailyCheckpoint(
                    durableRows,
                    // And the same leg is what a purge has to remove from. Removing the
                    // row from memory alone would leave the index's copy for step 2's
                    // dump to write straight back into the next checkpoint.
                    recordDisk::purge,
                    new TombstoneLog(dataRoot()),
                    checkpoint,
                    hotLogs::truncateToCheckpoint);

            // Only now that the hot tier is restored: every roll and checkpoint
            // snapshots the store, and one taken while it was still filling would
            // fold a partial baseline into the log.
            if (archive != null) {
                hotArchive = new HotArchive(hotDatasets, hotLogs, durableRows, archiveMember,
                        Duration.ofSeconds(persistence.getRollIntervalSeconds()),
                        Duration.ofSeconds(archive.getCheckpointIntervalSeconds()));
            }

            // Per writer, not leader-gated: the transaction writes this pod's own
            // volume and truncates this pod's own redo logs, so one pod running it for
            // the cluster would leave every other writer with no baseline to start
            // from. A search member runs no cycle at all — it has nothing to dump,
            // nothing to truncate and no tombstone log to purge into.
            cleaner.scheduleWithFixedDelay(
                    this::runDailyCheckpoint,
                    cleanupIntervalSecs, cleanupIntervalSecs, TimeUnit.SECONDS);

            // On the cleaner rather than on a thread of its own: it is the same kind of
            // work — keeping what this pod publishes about its own volume true — and a
            // putIfAbsent every 30s does not need one. A search member files no report
            // in the first place, so there is nothing of its to repair.
            cleaner.scheduleWithFixedDelay(
                    this::repairRestoreReport,
                    REPORT_REPAIR_SECONDS, REPORT_REPAIR_SECONDS, TimeUnit.SECONDS);
        }

        log.info("EventStore initialised: role={}, deleted_max_age_seconds={}, seed_members={}",
                role, deletedMaxAgeSeconds, seedMembers.isEmpty() ? "multicast" : seedMembers);
    }

    // -----------------------------------------------------------------------
    // Ingest
    // -----------------------------------------------------------------------

    /**
     * Stores any subscribed event into its generated typed store, keyed by
     * event_id. Vservice-neutral: the typed dispatch lives in the generated
     * StratumEventStores, so this code names no vservice.
     */
    public boolean store(Message event) {
        return stores.store(event);
    }

    /**
     * Replays this node's redo logs and waits for its peers to do the same before
     * hot reads open.
     *
     * <p>An equal-epoch divergence aborts the start rather than picking a winner:
     * two logs holding one key at one epoch with different bytes is proof the
     * atomic write path was violated, and choosing between them would convert
     * stratum's atomic store into a convergent one silently. See
     * {@code EpochMergeProcessor}.
     */
    private void restoreHotTier(List<HotDataset> datasets, int minDurableCopies) {
        int applied = 0;
        int read = 0;
        List<String> diverged = new ArrayList<>();
        // A search member has no log to replay. It still goes through the barrier
        // below — reporting nothing, waiting for the writers — because it is handed
        // partitions as soon as it joins.
        for (HotDataset d : role.isWriter() ? datasets : List.<HotDataset>of()) {
            // Re-runnable: the replay merges by epoch, so a record applied by an attempt
            // the quorum interrupted is SKIPPED by the next one.
            HotRestore.Result result = StratumMapConfig.withWriterQuorum(hz, "replaying " + d.mapName,
                    () -> HotRestore.replay(hotLogs.log(d.mapName), d, hz.getMap(d.mapName)),
                    Backoff.THREAD_SLEEPER);
            applied += result.applied;
            read += result.read;
            diverged.addAll(result.diverged);
        }
        if (!diverged.isEmpty()) {
            throw new IllegalStateException("hot restore: equal-epoch divergence on " + diverged.size()
                    + " key(s), which the atomic write path makes impossible; refusing to start."
                    + " Keys: " + diverged);
        }
        // What the barrier is told is records READ, not records applied: on a rolling
        // restart the peers still own the partitions, so this node's rows come back
        // already at their own-or-newer epoch and every record replays as SKIPPED.
        // Reporting applied records would make a healthy node look like it had no log.
        //
        // The checkpoint's row count goes with it, and it is read here rather than
        // taken from loadCheckpoint because loadCheckpoint SKIPS the dump whenever
        // the peers already hold the data — which is every rolling restart, i.e.
        // exactly the boots where the barrier is decided. What the ledger needs is
        // not "what did this pod load" but "what does this volume hold durably", and
        // after a DailyCheckpoint cycle that is the dump, not the emptied redo tail.
        long checkpointRows = role.isWriter() ? checkpointRows() : 0L;
        // Re-runnable: the report and the in-flight mark are re-put under this
        // member's UUID, which does not change while it lives.
        int redoRecordsRead = read;
        StratumMapConfig.withWriterQuorum(hz, "the hot-restore barrier",
                () -> HotRestore.awaitClusterReplay(hz, redoRecordsRead, checkpointRows,
                        minDurableCopies, RESTORE_TIMEOUT),
                Backoff.THREAD_SLEEPER);
        // The SAME number, kept so it can be put back unchanged if the entry is ever
        // lost — read once above rather than twice, because a repair that filed a
        // different number from the report would be revising it. The barrier needs a
        // report from every live member and a member that is serving has no other
        // occasion to file one; see repairRestoreReport.
        bootDurableRecords = HotRestore.durableRecordCount(read, checkpointRows);
        redoReplayDone = true;
        log.info("hot tier restored ({} member): {} record(s) read from this node's redo logs,"
                + " {} applied", role, read, applied);
    }

    /**
     * The size of this pod's committed checkpoint, for the hot-restore ledger.
     *
     * <p>An unreadable checkpoint directory fails the start rather than counting as
     * zero. Zero is the report of a volume that holds nothing durable, and it is the
     * one report that can talk the barrier below min_durable_copies; a pod that
     * cannot tell whether it has a baseline must not file that claim about itself.
     * Same posture as {@link #loadCheckpoint}, which refuses to start on a baseline
     * it cannot read.
     */
    private long checkpointRows() {
        try {
            return checkpoint.rowCount();
        } catch (IOException e) {
            throw new IllegalStateException("checkpoint at " + dataRoot() + " could not be counted;"
                    + " refusing to report this volume's durability to the restore ledger"
                    + " without knowing whether it holds a baseline", e);
        }
    }

    /**
     * Declares the event-log cold-start replay finished — either because it has
     * caught up to where the log ended when it started, or because there was none
     * to run. Idempotent, and called from the rebuild's own thread.
     */
    public void coldStartReplayComplete() {
        coldStartReplayDone = true;
    }

    /**
     * True once every reachable instance has replayed its redo log AND the
     * event-log cold-start replay has finished. Both legs, because they recover
     * disjoint sets of record types: neither alone means the store is complete.
     */
    public boolean hotReadable() {
        return redoReplayDone && coldStartReplayDone;
    }

    private void requireHotReadable() {
        if (!redoReplayDone) {
            throw new IllegalStateException(
                    "hot reads are closed until every reachable instance has replayed its redo log;"
                            + " serving a partially recovered hot store would answer as if complete");
        }
        if (!coldStartReplayDone) {
            throw new IllegalStateException(
                    "hot reads are closed until the event-log cold-start replay has caught up;"
                            + " record types with no redo log are still being rebuilt, and answering"
                            + " now would report them as empty rather than as not yet recovered");
        }
    }

    /**
     * This pod's own volume — the checkpoint, the redo logs, the record indexes
     * and the tombstone log. The operator mounts the pod's per-replica volume
     * at /app/data; STRATUM_DATA_DIR overrides it for a local run, which is the
     * only case where /app/data does not exist.
     */
    private static Path dataRoot() {
        String dir = System.getenv("STRATUM_DATA_DIR");
        return Paths.get(dir == null || dir.isBlank() ? "/app/data" : dir);
    }

    // -----------------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------------



    @SuppressWarnings("unchecked")

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    /**
     * Rows held cluster-wide. Read by StratumHandler's cold-boot decision, right after
     * {@link #init}, so it waits out a lost writer quorum the way init does.
     */
    public long eventCount() {
        return StratumMapConfig.withWriterQuorum(hz, "counting the store", stores::totalCount,
                Backoff.THREAD_SLEEPER);
    }
    public boolean isEmpty()    { return stores.totalCount() == 0; }
    public int  clusterSize()   { return hz.getCluster().getMembers().size(); }
    public long lastCheckpointMs() { return lastCheckpointMs.get(); }
    public long lastCheckpointPurged() { return lastCheckpointPurged.get(); }

    /** The generated typed per-record hot stores — used by the RecordMap plane. */
    public StratumEventStores stores() { return stores; }

    /** The member's Hazelcast instance, for the maps the record plane shares across nodes. */
    public HazelcastInstance hazelcast() { return hz; }

    // -----------------------------------------------------------------------
    // Daily checkpoint
    // -----------------------------------------------------------------------

    /**
     * Runs this pod's purge -> dump -> truncate transaction.
     *
     * <p>A failure is logged and dropped rather than rethrown: the previous
     * checkpoint plus the un-truncated redo logs are a complete recovery set, so the
     * outcome of a failed run is "try again on the next tick", and throwing out of a
     * {@code scheduleWithFixedDelay} task would instead cancel the schedule and mean
     * no run ever again.
     */
    private void runDailyCheckpoint() {
        try {
            DailyCheckpoint.Result result =
                    dailyCheckpoint.run(deletedMaxAgeSeconds * 1000L, System.currentTimeMillis());
            lastCheckpointMs.set(System.currentTimeMillis());
            lastCheckpointPurged.set(result.purged);
        } catch (Exception e) {
            log.error("daily checkpoint failed; the previous checkpoint and the un-truncated"
                    + " redo logs remain a complete recovery set, retrying next cycle", e);
        }
    }

    /**
     * Puts this pod's hot-restore ledger report back if it has gone missing.
     *
     * <p>Only after the barrier: before it, this pod has filed nothing to repair and
     * {@link #bootDurableRecords} is not yet the number it filed. Putting a 0 there
     * early would be this pod claiming an empty volume in the one place the barrier
     * reads to decide whether the cluster is under its durability floor.
     *
     * <p>Swallowing the failure is right here where it is right for the checkpoint:
     * a repair that could not run is a repair that runs next cycle, and throwing out
     * of a scheduled task on a single-threaded executor would stop the daily
     * checkpoint with it.
     */
    private void repairRestoreReport() {
        if (!redoReplayDone) {
            return;
        }
        try {
            HotRestore.refileReport(hz, bootDurableRecords);
        } catch (Exception e) {
            log.warn("could not check this pod's hot-restore ledger report, retrying next cycle", e);
        }
    }

    /**
     * Restores this writer's hot logs from the archive when {@link HotArchive#RESTORE_ENV}
     * names a member, then attaches the shippers — both before the redo replay, so
     * the restored records replay like any others and the first roll already ships.
     */
    private void openArchive(ArchiveConfig archive, String member, List<HotDataset> datasets) {
        if (archive.getGcsBucket().isEmpty()) {
            throw new IllegalArgumentException(
                    "grpcconfig.disk_persistence.archive.gcs_bucket is required when the archive is set");
        }
        ArchiveSink sink = new GcsArchiveSink(archive.getGcsBucket());
        String restoreFrom = System.getenv(HotArchive.RESTORE_ENV);
        if (restoreFrom != null && !restoreFrom.isBlank()) {
            try {
                HotArchive.restore(sink, archive.getPrefix(), restoreFrom.trim(), datasets, hotLogs);
            } catch (IOException e) {
                throw new IllegalStateException("archive restore from " + restoreFrom.trim()
                        + " failed; refusing to start on a partial restore", e);
            }
        }
        HotArchive.attach(sink, archive.getPrefix(), member,
                Codec.fromProtoInt(persistence.getCompressionValue()), datasets, hotLogs);
    }

    /**
     * Loads this pod's newest complete checkpoint into the maps, on a cold boot only.
     *
     * <p>The gate is {@code ColdStartRebuilder.shouldReplay} — the same
     * "is the data already here?" test the event-log replay uses, and the same
     * answer: a member restarting alongside live peers gets its partitions back from
     * them, and loading a stale local dump over that would be this pod overwriting
     * the cluster's newer rows with its own older ones. {@code putIfAbsent} is the
     * second half of that guard, for a peer that joins mid-load.
     *
     * <p>Loading is what makes the event-log replay unnecessary: the store is no
     * longer empty cluster-wide once the checkpoint is in, so {@code StratumHandler}
     * takes the skip-the-replay branch on its own. The full replay from offset zero
     * is not removed, it is simply no longer reached by a pod that has a checkpoint —
     * which is exactly what should still happen on a pod that does not.
     */
    private void loadCheckpoint() {
        try {
            long held = StratumMapConfig.withWriterQuorum(hz, "counting the store for the checkpoint",
                    stores::totalCount, Backoff.THREAD_SLEEPER);
            if (!ColdStartRebuilder.shouldReplay(held)) {
                log.info("checkpoint: not a cold boot ({} row(s) held cluster-wide) — the peers"
                        + " hold the data, this pod's checkpoint is not loaded", held);
                return;
            }
            List<com.github.tacplusdev.stratum.checkpoint.Checkpoint.Row>
                    rows = checkpoint.load();
            if (rows.isEmpty()) {
                log.info("checkpoint: none on this volume — recovery falls back to the redo logs"
                        + " and the event log");
                return;
            }
            // The cold-boot decision above is taken once: re-taking it after an
            // interrupted load would read this load's own rows as the peers' data.
            // The load itself is re-runnable, being putIfAbsent.
            StratumMapConfig.withWriterQuorum(hz, "loading the checkpoint", () -> {
                for (var row : rows) {
                    hz.<String, Message>getMap(row.mapName).putIfAbsent(row.key, row.value);
                }
                return null;
            }, Backoff.THREAD_SLEEPER);
            log.info("checkpoint: {} row(s) loaded from {}", rows.size(), checkpoint.current());
        } catch (IOException e) {
            throw new IllegalStateException("checkpoint at " + dataRoot()
                    + " could not be read; refusing to start on a baseline that may be partial", e);
        }
    }

    private static long configuredDeletedMaxAgeSeconds() {
        String raw = System.getenv(DELETED_MAX_AGE_ENV);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_DELETED_MAX_AGE_SECONDS;
        }
        long value = Long.parseLong(raw.trim());
        if (value < 1) {
            throw new IllegalArgumentException(
                    DELETED_MAX_AGE_ENV + " must be at least 1, got " + value);
        }
        return value;
    }

    // -----------------------------------------------------------------------
    // Shutdown
    // -----------------------------------------------------------------------

    public void shutdown() {
        cleaner.shutdownNow();
        if (hotArchive != null) hotArchive.close();
        if (hotLogs != null) hotLogs.close();
        if (hz != null) hz.shutdown();
        // After the member, so nothing can still be queueing writes into an index
        // that is closing. The queue is drained by close() rather than dropped.
        if (recordDisk != null) recordDisk.close();
    }


}
