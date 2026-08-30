// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
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

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast.StratumMapConfig;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast.StratumEpochExtractor;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast.StratumEventTypeExtractor;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmStore;
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
 *   - SORTED:  epoch (StratumEpochExtractor), timestampMs
 *   - HASH:    spiffeId, sessionId, agentId, eventTypeValue (StratumEventTypeExtractor)
 *
 * Retention:
 * Warm tier:
 *   Record types declaring (spec.record).residency = {window: RESIDENCY_WARM}
 *   are ALSO written to a node-local Lucene index under the pod's volume, so a
 *   row evicted from the memory window is still complete and queryable. The disk
 *   write is off the ack path — see WarmStore — so ingest latency is unchanged.
 *   With no warm-typed record declared the tier holds nothing and touches no
 *   file, which is stratum's state today.
 *
 *   A row whose store epoch has aged past the residency window is served by the
 *   disk leg alone — the memory leg's cut (WarmResidency) is applied per query, so
 *   that is true from the instant the boundary passes, not from the next sweep.
 *   WarmEviction then drops the row from the map to get the memory back; see
 *   {@link #runWarmEviction()}.
 *
 * Retention:
 *   Runs on cleanup_interval_seconds. Deletes entries older than
 *   (now - retentionDays * 86400000) from every typed store. Which field
 *   carries an event's age differs per record type (timestampMs on AgentEvent,
 *   occurredAtMs on TaskActivity), so the per-type predicate is emitted by the
 *   generator from each record's declared retention timestamp_field, not named
 *   here. Runs only on the oldest cluster member to avoid duplicate work
 *   across nodes.
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
    private WarmStore warm;
    private HotRedoLogs hotLogs;
    private HotWrites hotWrites;
    /**
     * False until every reachable instance has replayed its redo log. Hot reads
     * refuse until then: a node holding only its OWN partial log looks answerable
     * and is missing every row the other nodes owned, and serving that silently is
     * worse than a slow start.
     */
    private volatile boolean hotReadable;
    private int retentionDays;

    /** How long a cold start waits for its peers' redo replay before giving up. */
    static final java.time.Duration RESTORE_TIMEOUT = java.time.Duration.ofMinutes(5);

    /** Archives retained per hot redo log, matching the diskstore proto default. */
    private static final int HOT_ARCHIVE_COUNT = 3;

    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor(
            r -> { Thread t = new Thread(r, "stratum-event-cleaner"); t.setDaemon(true); return t; });

    private final AtomicLong lastCleanupMs      = new AtomicLong(0);
    private final AtomicLong lastCleanupRemoved = new AtomicLong(0);

    // -----------------------------------------------------------------------
    // Init
    // -----------------------------------------------------------------------

    /**
     * Initialises the Hazelcast member and the "events" IMap.
     *
     * @param retentionDays       days to retain events
     * @param cleanupIntervalSecs how often the retention job runs
     * @param seedMembers         list of "host:port" addresses for TCP/IP join;
     *                            empty → multicast (dev only)
     */
    public void init(int retentionDays, long cleanupIntervalSecs, List<String> seedMembers) {
        this.retentionDays = retentionDays;

        int floorOverride = StratumMapConfig.configuredMinDurableCopies();
        int minDurableCopies = floorOverride > 0
                ? floorOverride
                : StratumMapConfig.DEFAULT_MIN_DURABLE_COPIES;
        hz = Hazelcast.newHazelcastInstance(StratumMapConfig.build(seedMembers, minDurableCopies));
        hotWrites = new HotWrites(hz, minDurableCopies - 1, floorOverride);
        stores.init(hz, hotWrites);
        events = hz.getMap(StratumMapConfig.MAP_NAME);

        // The hot tier's redo logs, on THIS pod's volume. One per replayable record
        // type; with no record carrying (spec.store_epoch) the list is empty, the
        // node opens no log, and the tier is exactly what it is today plus the
        // quorum gate.
        List<HotDataset> hotDatasets = StratumHotStores.datasets();
        hotLogs = new HotRedoLogs(dataRoot(), hotDatasets, HOT_ARCHIVE_COUNT, Codec.NONE);
        restoreHotTier(hotDatasets, minDurableCopies);

        try {
            warm = new WarmStore(dataRoot(), StratumWarmStores.datasets(WarmStore.DEFAULT_WINDOW_SECONDS));
        } catch (IOException e) {
            throw new IllegalStateException("warm tier could not open its directory under " + dataRoot(), e);
        }
        // Unconditional, before the first write: a cross-slice update interrupted
        // between its two commits leaves a stale copy nothing else would notice.
        // Same posture as ColdStartRebuilder — repair rather than trust.
        warm.reconcile();

        cleaner.scheduleWithFixedDelay(
                this::runRetentionCleanupIfLeader,
                cleanupIntervalSecs, cleanupIntervalSecs, TimeUnit.SECONDS);

        log.info("EventStore initialised: retention_days={}, seed_members={}", retentionDays,
                seedMembers.isEmpty() ? "multicast" : seedMembers);
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
        return ingest(warm, event, () -> stores.store(event));
    }

    /**
     * The warm ingest sequence: claim, memory put, disk enqueue.
     *
     * <p>The claim comes FIRST, before the memory put — not with the enqueue that
     * follows it. From the memory put onwards memory's copy may be the newer one,
     * and for a MUTABLE row whose previous copy has aged past the residency boundary
     * that makes both legs match the key at once: memory because the update's epoch
     * is back inside the window, disk because it still holds the pre-update copy and
     * the key is not yet pending. Claiming first closes that window; claiming at the
     * enqueue leaves it open for the width of these two calls.
     *
     * <p>The other order — enqueue then memory put — is strictly worse: it turns the
     * duplicate into a LOST row whenever the asynchronous writer commits before the
     * memory put lands.
     *
     * <p>The {@code finally} exists because a claim not released is worse than the
     * duplicate it prevents: a permanently pending key is hidden from the disk leg
     * for the life of the process. It covers the leg where {@code memoryPut} throws
     * — a HotWrites quorum refusal, say — before the enqueue ever runs. The other
     * leg, a record the warm validator rejects, is released inside
     * {@link WarmStore#enqueue}, which is where that decision is made.
     *
     * <p>{@code memoryPut} is a parameter rather than an inlined call so this
     * sequence — the thing the correctness argument is about — is one testable unit
     * instead of something a test has to re-spell to exercise.
     */
    static boolean ingest(WarmStore warm, Message event, BooleanSupplier memoryPut) {
        boolean claimed = warm.claim(event);
        boolean enqueued = false;
        try {
            boolean handled = memoryPut.getAsBoolean();
            // AFTER the memory put, and without waiting for it: the ack is the
            // in-memory replication, the disk commit follows asynchronously.
            enqueued = warm.enqueue(event);
            return handled;
        } finally {
            if (claimed && !enqueued) {
                warm.release(event);
            }
        }
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
        HotRestore.awaitClusterReplay(hz, read, minDurableCopies, RESTORE_TIMEOUT);
        hotReadable = true;
        log.info("hot tier restored: {} record(s) read from this node's redo logs, {} applied",
                read, applied);
    }

    /** True once every reachable instance has replayed its redo log. */
    public boolean hotReadable() {
        return hotReadable;
    }

    private void requireHotReadable() {
        if (!hotReadable) {
            throw new IllegalStateException(
                    "hot reads are closed until every reachable instance has replayed its redo log;"
                            + " serving a partially recovered hot store would answer as if complete");
        }
    }

    /**
     * Where the warm tier lives. The operator mounts the pod's per-replica volume
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
    public long lastCleanupMs() { return lastCleanupMs.get(); }

    /** The generated typed per-record hot stores — used by the RecordMap plane. */
    public StratumEventStores stores() { return stores; }

    /** The warm disk tier — the RecordMap plane's second leg for warm-typed records. */
    public WarmStore warm() { return warm; }

    // -----------------------------------------------------------------------
    // Retention cleanup
    // -----------------------------------------------------------------------

    private void runRetentionCleanupIfLeader() {
        Iterator<com.hazelcast.cluster.Member> members = hz.getCluster().getMembers().iterator();
        if (!members.hasNext()) return;
        if (!members.next().equals(hz.getCluster().getLocalMember())) return;
        runRetentionCleanup();
        // Same schedule and the same one-sweeper gate, deliberately not the same
        // pass: retention DELETES rows the service no longer keeps, eviction MOVES
        // rows that are already on disk out of memory. Sharing a timer is all they
        // share.
        runWarmEviction();
    }

    /**
     * Releases warm rows the disk leg already owns from the Hazelcast map.
     *
     * @return rows evicted, by dataset name
     */
    private Map<String, Long> runWarmEviction() {
        return WarmEviction.sweep(hz, warm);
    }

    private void runRetentionCleanup() {
        long cutoffMs = System.currentTimeMillis()
                - (long) retentionDays * StratumEpochExtractor.MILLIS_PER_DAY;
        log.info("retention cleanup: removing events older than {} (retention_days={})",
                cutoffMs, retentionDays);
        try {
            long before  = stores.totalCount();
            stores.runRetention(cutoffMs);
            // The disk tier retires whole slices rather than rows, and it measures
            // from the STORE EPOCH the slice is named for, not from each record's
            // business timestamp. Same cutoff, different clock — stated because the
            // two sweeps look interchangeable and are not.
            warm.runRetention(cutoffMs);
            long removed = before - stores.totalCount();
            lastCleanupMs.set(System.currentTimeMillis());
            lastCleanupRemoved.set(removed);
            log.info("retention cleanup complete: removed {} entries", removed);
        } catch (Exception e) {
            log.error("retention cleanup failed", e);
        }
    }

    // -----------------------------------------------------------------------
    // Shutdown
    // -----------------------------------------------------------------------

    public void shutdown() {
        cleaner.shutdownNow();
        if (warm != null) warm.close();
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
