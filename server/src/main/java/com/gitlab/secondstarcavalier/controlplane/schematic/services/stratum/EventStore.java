// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentEvent;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorEventType;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.checkpoint.Checkpoint;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.checkpoint.DailyCheckpoint;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.checkpoint.HazelcastRows;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.checkpoint.TombstoneLog;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast.StratumMapConfig;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast.StratumEventTypeExtractor;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotDataset;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotRedoLogs;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotRestore;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotWrites;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.Codec;

/**
 * EventStore manages the Hazelcast IMap that backs stratum's hot event store.
 *
 * Data model: IMap<event_id, AgentEvent> named "events".
 *
 * Indexes (via ValueExtractor or Hazelcast native attribute reflection):
 *   - SORTED:  epoch, timestampMs
 *   - HASH:    spiffeId, sessionId, agentId, eventTypeValue (StratumEventTypeExtractor)
 *
 * Record residency:
 *   One clock per record type, and Hazelcast runs it: per-entry max-idle set
 *   from (spec.record).residency.idle_seconds (see StratumMapConfig). A row
 *   stays resident while it is read or written and is evicted from memory once
 *   it has gone idle_seconds with neither. Nothing here sweeps for it, and a
 *   read never restamps (spec.store_epoch) or appends to the redo log.
 *
 *   This replaced the HOT/WARM/COLD residency classes, the separate
 *   retention_days sweep and the node-local Lucene tier they partitioned
 *   against, per the 2026-09-20 record lifecycle decision.
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
    // Agent-specific read path: reads the same Hazelcast map the generated
    // store writes for agentsensor.AgentEvent.
    private IMap<String, AgentEvent> events;
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
    private Checkpoint checkpoint;
    private DailyCheckpoint dailyCheckpoint;
    private long deletedMaxAgeSeconds;

    /**
     * How long a cold start waits for its peers' redo replay before giving up.
     *
     * <p>The SECOND of two sequential waits in {@link #init}, after
     * {@link StratumMapConfig#FORMATION_TIMEOUT}, and deliberately still
     * sequential: this barrier cannot begin before the cluster floor is met,
     * because the replay it waits on opens split-brain-protected maps and needs
     * that many live members to have filed reports. The combined worst case and
     * why it does not cost the pod its listener are documented on
     * {@code FORMATION_TIMEOUT}.
     */
    static final java.time.Duration RESTORE_TIMEOUT = java.time.Duration.ofMinutes(5);

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

    /**
     * Initialises the Hazelcast member and the "events" IMap.
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
        hz = Hazelcast.newHazelcastInstance(StratumMapConfig.build(seedMembers, minDurableCopies));

        // Before ANY hot map is touched. newHazelcastInstance returns when this
        // member is up, not when the cluster is assembled, so the first pod of the
        // StatefulSet is briefly a cluster of one — and every line below opens a map
        // whose split-brain protection refuses a proxy below the floor. Opening one
        // there threw SplitBrainProtectionException straight out of init.
        StratumMapConfig.awaitProtectedClusterSize(hz, minDurableCopies,
                StratumMapConfig.FORMATION_TIMEOUT);

        hotWrites = new HotWrites(hz, floorOverride);
        stores.init(hz, hotWrites);
        events = hz.getMap(StratumMapConfig.MAP_NAME);

        // The hot tier's redo logs, on THIS pod's volume. One per replayable record
        // type; with no record carrying (spec.store_epoch) the list is empty, the
        // node opens no log, and the tier is exactly what it is today plus the
        // quorum gate.
        List<HotDataset> hotDatasets = StratumHotStores.datasets();
        hotLogs = new HotRedoLogs(dataRoot(), hotDatasets, HOT_ARCHIVE_COUNT, Codec.NONE);

        // BEFORE the redo replay, and that order is the cold start: the checkpoint
        // is the baseline, the redo logs hold only the tail that landed after it
        // (see HotRedoLogs.truncateToCheckpoint), and replaying the tail onto the
        // baseline is what makes recovery proportional to the data rather than to
        // the whole history.
        checkpoint = new Checkpoint(dataRoot());
        loadCheckpoint();
        restoreHotTier(hotDatasets, minDurableCopies);

        this.deletedMaxAgeSeconds = configuredDeletedMaxAgeSeconds();
        this.dailyCheckpoint = new DailyCheckpoint(
                new HazelcastRows(hz, StratumEventStores.mapNames()),
                // No second on-disk row index to purge from. The node-local Lucene
                // tier was the warm model's disk leg and went with it; this pod's
                // durable copy of a row is the checkpoint, and step 2 rewrites that
                // from what survived step 1. A purged row is therefore absent from
                // the next dump, and the redo tail that still named it is discarded
                // by step 3's truncate.
                DailyCheckpoint.DiskPurge.NONE,
                new TombstoneLog(dataRoot()),
                checkpoint,
                hotLogs::truncateToCheckpoint);

        // Per pod, not leader-gated: the transaction writes this pod's own volume
        // and truncates this pod's own redo logs, so one pod running it for the
        // cluster would leave every other pod with no baseline to start from.
        cleaner.scheduleWithFixedDelay(
                this::runDailyCheckpoint,
                cleanupIntervalSecs, cleanupIntervalSecs, TimeUnit.SECONDS);

        log.info("EventStore initialised: deleted_max_age_seconds={}, seed_members={}",
                deletedMaxAgeSeconds, seedMembers.isEmpty() ? "multicast" : seedMembers);
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
        for (HotDataset d : datasets) {
            HotRestore.Result result = HotRestore.replay(hotLogs.log(d.mapName), d, hz.getMap(d.mapName));
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
        HotRestore.awaitClusterReplay(hz, read, checkpointRows(), minDurableCopies, RESTORE_TIMEOUT);
        redoReplayDone = true;
        log.info("hot tier restored: {} record(s) read from this node's redo logs, {} applied",
                read, applied);
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
     * This pod's own volume — the checkpoint, the redo logs and the tombstone
     * log. The operator mounts the pod's per-replica volume
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

    public Optional<AgentEvent> getByKey(String eventId) {
        requireHotReadable();
        return Optional.ofNullable(events.get(eventId));
    }

    /**
     * Executes a filter query using the SqlParser to extract predicates.
     * Results are sorted by (timestampMs DESC, event_id ASC).
     */
    public List<AgentEvent> query(SqlPredicates preds) {
        requireHotReadable();
        Predicate<String, AgentEvent> predicate = buildPredicate(preds);
        Collection<AgentEvent> raw = (predicate != null) ? events.values(predicate) : events.values();
        List<AgentEvent> sorted = new ArrayList<>(raw);
        sorted.sort(Comparator
                .comparingLong(AgentEvent::getTimestampMs).reversed()
                .thenComparing(AgentEvent::getEventId));
        return sorted;
    }

    @SuppressWarnings("unchecked")
    private Predicate<String, AgentEvent> buildPredicate(SqlPredicates p) {
        List<Predicate<String, AgentEvent>> parts = new ArrayList<>();

        // "spiffeId", not "spiffe_id": Hazelcast resolves a predicate attribute
        // through the getter, and protoc emits getSpiffeId(). The snake_case form
        // matched no accessor, so this predicate could never evaluate.
        if (p.spiffeId != null) parts.add(Predicates.equal("spiffeId", p.spiffeId));
        if (p.sessionId != null) parts.add(Predicates.equal("sessionId", p.sessionId));
        if (p.agentId   != null) parts.add(Predicates.equal("agentId", p.agentId));
        if (p.eventType != null) parts.add(Predicates.equal(
                StratumEventTypeExtractor.ATTRIBUTE_NAME, p.eventType.getNumber()));
        if (p.startMs > 0 && p.endMs > 0) {
            parts.add(Predicates.between("timestampMs", p.startMs, p.endMs));
        } else if (p.startMs > 0) {
            parts.add(Predicates.greaterEqual("timestampMs", p.startMs));
        } else if (p.endMs > 0) {
            parts.add(Predicates.lessEqual("timestampMs", p.endMs));
        }

        if (parts.isEmpty()) return null;
        if (parts.size() == 1) return parts.get(0);
        return Predicates.and(parts.toArray(new Predicate[0]));
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    public long eventCount()    { return stores.totalCount(); }
    public boolean isEmpty()    { return stores.totalCount() == 0; }
    public int  clusterSize()   { return hz.getCluster().getMembers().size(); }
    public long lastCheckpointMs() { return lastCheckpointMs.get(); }
    public long lastCheckpointPurged() { return lastCheckpointPurged.get(); }

    /** The generated typed per-record hot stores — used by the RecordMap plane. */
    public StratumEventStores stores() { return stores; }

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
            long held = stores.totalCount();
            if (!ColdStartRebuilder.shouldReplay(held)) {
                log.info("checkpoint: not a cold boot ({} row(s) held cluster-wide) — the peers"
                        + " hold the data, this pod's checkpoint is not loaded", held);
                return;
            }
            List<com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.checkpoint.Checkpoint.Row>
                    rows = checkpoint.load();
            if (rows.isEmpty()) {
                log.info("checkpoint: none on this volume — recovery falls back to the redo logs"
                        + " and the event log");
                return;
            }
            for (var row : rows) {
                hz.<String, Message>getMap(row.mapName).putIfAbsent(row.key, row.value);
            }
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
        if (hotLogs != null) hotLogs.close();
        if (hz != null) hz.shutdown();
    }

    // -----------------------------------------------------------------------
    // SQL predicate extraction (shared with StratumHandler for load_by_sql)
    // -----------------------------------------------------------------------

    static final class SqlPredicates {
        String          spiffeId;
        String          sessionId;
        String          agentId;
        SensorEventType eventType;
        long            startMs;
        long            endMs;
    }

    static final class SqlParser {
        static SqlPredicates parse(String sql) {
            SqlPredicates p = new SqlPredicates();
            if (sql == null) return p;
            String upper = sql.toUpperCase();

            p.spiffeId      = extractStringPredicate(sql, "spiffe_id");
            p.sessionId = extractStringPredicate(sql, "session_id");
            p.agentId   = extractStringPredicate(sql, "agent_id");

            // event_type = 'SENSOR_EVENT_TYPE_FILE_READ'
            String etVal = extractStringPredicate(sql, "event_type");
            if (etVal != null) {
                try { p.eventType = SensorEventType.valueOf(etVal); } catch (IllegalArgumentException ignored) {}
            }

            // timestamp_ms BETWEEN <start> AND <end>
            int betweenIdx = upper.indexOf("TIMESTAMP_MS BETWEEN");
            if (betweenIdx >= 0) {
                String rest = sql.substring(betweenIdx + "TIMESTAMP_MS BETWEEN".length()).trim();
                String[] parts = rest.split("(?i)\\s+AND\\s+", 2);
                if (parts.length == 2) {
                    try { p.startMs = Long.parseLong(parts[0].trim()); } catch (NumberFormatException ignored) {}
                    try {
                        p.endMs = Long.parseLong(parts[1].trim().split("\\s+")[0]);
                    } catch (NumberFormatException ignored) {}
                }
            }

            // timestamp_ms > <start>
            int gtIdx = upper.indexOf("TIMESTAMP_MS >");
            if (gtIdx >= 0 && p.startMs == 0) {
                String rest = sql.substring(gtIdx + "TIMESTAMP_MS >".length()).trim();
                if (rest.startsWith("=")) rest = rest.substring(1).trim();
                try { p.startMs = Long.parseLong(rest.split("\\s+")[0]); } catch (NumberFormatException ignored) {}
            }

            // timestamp_ms < <end>
            int ltIdx = upper.indexOf("TIMESTAMP_MS <");
            if (ltIdx >= 0 && p.endMs == 0) {
                String rest = sql.substring(ltIdx + "TIMESTAMP_MS <".length()).trim();
                if (rest.startsWith("=")) rest = rest.substring(1).trim();
                try { p.endMs = Long.parseLong(rest.split("\\s+")[0]); } catch (NumberFormatException ignored) {}
            }

            return p;
        }

        private static String extractStringPredicate(String sql, String field) {
            int idx = sql.toUpperCase().indexOf(field.toUpperCase() + " = '");
            if (idx < 0) idx = sql.toUpperCase().indexOf(field.toUpperCase() + "='");
            if (idx < 0) return null;
            int start = sql.indexOf('\'', idx) + 1;
            int end   = sql.indexOf('\'', start);
            if (start <= 0 || end < 0) return null;
            return sql.substring(start, end);
        }
    }
}
