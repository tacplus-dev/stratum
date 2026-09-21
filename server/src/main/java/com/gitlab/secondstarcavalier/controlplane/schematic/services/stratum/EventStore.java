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
import java.util.function.BiPredicate;
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
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast.StratumEpochExtractor;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast.StratumEventTypeExtractor;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmDataset;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmIndex;
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
 * Daily checkpoint:
 *   On the same cleanup_interval_seconds tick, and per pod rather than per
 *   cluster, {@link #runDailyCheckpoint()} runs the record-lifecycle transaction:
 *   purge flagged-deleted rows past their max age into the tombstone log, dump
 *   every surviving row to a new checkpoint, truncate the redo logs to it. It is
 *   also what makes cold start bounded — see {@link #loadCheckpoint()}. The
 *   retention sweep above is what it is meant to REPLACE and has not yet; see
 *   {@link #runCleanupCycle()} and docs/DURABILITY.md for why both still run.
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
    private volatile boolean redoReplayDone;
    /**
     * False until the event-log cold-start replay has caught up, or until the
     * caller has said there was none to run.
     *
     * <p>This is a SECOND leg of the same gate, not a refinement of the first. The
     * redo barrier only covers record types that have a redo log; the types that
     * do not are recovered by {@code ColdStartRebuilder} replaying the event log,
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
    private int retentionDays;
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

    private final AtomicLong lastCleanupMs      = new AtomicLong(0);
    private final AtomicLong lastCleanupRemoved = new AtomicLong(0);
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
     * @param retentionDays       days to retain events
     * @param cleanupIntervalSecs how often the retention job runs
     * @param warmWindowSeconds   memory-residency window for warm-tiered records;
     *                            rows older than it are served from disk. Shorter
     *                            than retention, or nothing ever reaches the disk
     *                            leg before retention deletes it.
     * @param seedMembers         list of "host:port" addresses for TCP/IP join;
     *                            empty → multicast (dev only)
     */
    public void init(int retentionDays, long cleanupIntervalSecs, long warmWindowSeconds,
                     List<String> seedMembers) {
        this.retentionDays = retentionDays;

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

        try {
            warm = new WarmStore(dataRoot(), StratumWarmStores.datasets(warmWindowSeconds));
        } catch (IOException e) {
            throw new IllegalStateException("warm tier could not open its directory under " + dataRoot(), e);
        }
        // Unconditional, before the first write: a cross-slice update interrupted
        // between its two commits leaves a stale copy nothing else would notice.
        // Same posture as ColdStartRebuilder — repair rather than trust.
        warm.reconcile();

        this.deletedMaxAgeSeconds = configuredDeletedMaxAgeSeconds();
        this.dailyCheckpoint = new DailyCheckpoint(
                new HazelcastRows(hz, StratumEventStores.mapNames()),
                warm::purge,
                new TombstoneLog(dataRoot()),
                checkpoint,
                hotLogs::truncateToCheckpoint);

        cleaner.scheduleWithFixedDelay(
                this::runCleanupCycle,
                cleanupIntervalSecs, cleanupIntervalSecs, TimeUnit.SECONDS);

        log.info("EventStore initialised: retention_days={}, warm_window_seconds={},"
                + " deleted_max_age_seconds={}, seed_members={}",
                retentionDays, warmWindowSeconds, deletedMaxAgeSeconds,
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
        return ingest(warm, event, this::memoryHolds, () -> stores.store(event));
    }

    /**
     * True when the memory leg already holds this key of this dataset.
     *
     * The dataset name IS the Hazelcast map name — one identifier names the type in
     * both tiers (see {@link com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmDataset#name}) —
     * so this asks the typed map the generated store writes, not a second copy of it.
     */
    private boolean memoryHolds(String datasetName, String key) {
        return hz.getMap(datasetName).containsKey(key);
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
     * <p>The claim is also what makes {@link #rejectsAppendOnlyDuplicate} sound under
     * concurrency. That check reads two legs and then acts, and between the read and
     * the act a second delivery of the same key can read them too — both see a key
     * neither leg holds, both append a Lucene document, and for an append-only
     * dataset nothing ever removes one of them. The claim closes that window without
     * a second mechanism: {@link WarmStore#claim} is a compare-and-set, so exactly
     * one of the two takes it, and the loser is the duplicate the check was looking
     * for. Node-local is the right scope because the corruption is: the memory leg
     * upserts by key and cannot double-count, while the Lucene index that can is this
     * node's own, one full replica per instance.
     *
     * <p>{@code memoryPut} is a parameter rather than an inlined call so this
     * sequence — the thing the correctness argument is about — is one testable unit
     * instead of something a test has to re-spell to exercise.
     */
    static boolean ingest(WarmStore warm, Message event, BiPredicate<String, String> memoryHolds,
                          BooleanSupplier memoryPut) {
        if (rejectsAppendOnlyDuplicate(warm, event, memoryHolds)) {
            return true;
        }
        boolean claimed = warm.claim(event);
        if (!claimed && rejectsConcurrentAppendOnlyDelivery(warm, event)) {
            return true;
        }
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
     * Refuses a write that would break an APPEND_ONLY dataset's unique-key contract.
     *
     * <p>The two legs disagree about what a duplicate key MEANS: the memory leg is an
     * IMap keyed by the record's key, so a second write of an existing key upserts,
     * while the disk leg appends a second document. Nothing notices until the first
     * copy ages past the residency boundary — from then on the key is served by memory
     * with the new copy and by disk with the old one, and the row count visibly jumps.
     * Neither leg is wrong on its own; the record's own declaration is what says the
     * key may not repeat, so this is the place to hold it.
     *
     * <p>Refused rather than reconciled: making memory hold multiple values per key,
     * or making an append-only log update in place, are both changes to the tiers'
     * write semantics for a case the declaration says cannot happen. Nothing in
     * production emits a duplicate today, so a legitimate write sees no change — this
     * only fires on the violation that previously corrupted the two legs silently.
     *
     * <p>Logged and dropped, not thrown — the same shape as the validator rejection in
     * {@link WarmStore#enqueue}. This runs on the EventBus subscription thread, where
     * throwing would retry the offending record forever.
     *
     * <p>BOTH legs are asked, and the disk leg is why: a key the memory leg does not
     * hold is not thereby absent. The whole store is empty on a COLD BOOT, and a cold
     * boot is precisely when {@code ColdStartRebuilder} replays the entire event log
     * from offset 0 — while this node's warm volume survived the restart and still
     * holds every one of those records. Checking memory alone, every replayed
     * append-only record reads as fresh and is appended to the disk index a second
     * time. Nothing repairs it afterwards either: {@link WarmIndex#reconcile} keeps
     * one copy per KEY and so is a no-op for a type whose keys never repeat by
     * declaration. The duplication is therefore permanent, and it surfaces as a
     * doubled Get&lt;T&gt;Count the moment the rows age past the residency boundary.
     * The same check closes the smaller window the memory leg always missed: a key
     * whose only copy had been evicted from memory.
     *
     * <p>What that costs, stated: one Lucene term lookup across the dataset's slices on
     * the ingest path of every append-only record whose key memory does not already
     * hold — which in steady state is every one of them. The warm tier is sized for
     * order 25k rows/year (see {@link WarmIndex}), so this is three orders of magnitude
     * inside its headroom; on a hot record type it costs nothing at all, because
     * {@code datasetFor} returns null before any of it runs.
     *
     * @return true when the record was refused — meaning handled, not stored.
     */
    private static boolean rejectsAppendOnlyDuplicate(WarmStore warm, Message event,
                                                      BiPredicate<String, String> memoryHolds) {
        WarmDataset dataset = warm.datasetFor(event);
        if (dataset == null || !dataset.appendOnly) {
            return false;
        }
        String key = dataset.key(event);
        String leg = memoryHolds.test(dataset.name, key) ? "memory" : diskLegHolding(warm, dataset, key);
        if (leg == null) {
            return false;
        }
        log.warn("rejected duplicate key on append-only {}: {} is already stored on the {} leg."
                + " The record type declares MUTABILITY_APPEND_ONLY, so its key may not repeat;"
                + " storing it would leave the memory and disk legs permanently disagreeing"
                + " about that key.", dataset.name, key, leg);
        return true;
    }

    /**
     * Refuses an append-only record whose key another in-flight delivery already claimed.
     *
     * <p>Reached only when {@link WarmStore#claim} returned false, which has two causes:
     * no warm dataset holds the type at all — the ordinary hot-only record, and no
     * refusal — or the key is already claimed. For an append-only dataset the second
     * cause IS a duplicate: the claim is held from before a memory put until the disk
     * commit that makes both legs agree, so a key claimed elsewhere is a key
     * {@link #rejectsAppendOnlyDuplicate} would have caught had the other delivery been
     * a moment further along.
     *
     * @return true when the record was refused — meaning handled, not stored.
     */
    private static boolean rejectsConcurrentAppendOnlyDelivery(WarmStore warm, Message event) {
        WarmDataset dataset = warm.datasetFor(event);
        if (dataset == null || !dataset.appendOnly) {
            return false;
        }
        log.warn("rejected duplicate key on append-only {}: {} is already being stored by a"
                + " concurrent delivery. The record type declares MUTABILITY_APPEND_ONLY, so its"
                + " key may not repeat; storing it would append a second document the disk leg"
                + " never removes.", dataset.name, dataset.key(event));
        return true;
    }

    /**
     * {@code "disk"} when this node's warm index already holds the key, else null.
     *
     * <p>A disk error resolves as "not held", i.e. the record is STORED. That direction
     * is deliberate: an unreadable index makes the answer unknown, and on an unknown
     * answer a duplicate row is recoverable — the reconcile and retention sweeps both
     * still run over it — while a dropped record is not, because the refusal reads as
     * handled and the subscription never redelivers it.
     */
    private static String diskLegHolding(WarmStore warm, WarmDataset dataset, String key) {
        try {
            return warm.isDurableOnDisk(dataset.name, key) ? "disk" : null;
        } catch (IOException e) {
            log.error("append-only duplicate check could not read the disk leg of {} for key {};"
                    + " storing the record rather than dropping it", dataset.name, key, e);
            return null;
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
        redoReplayDone = true;
        log.info("hot tier restored: {} record(s) read from this node's redo logs, {} applied",
                read, applied);
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
    public long lastCheckpointMs() { return lastCheckpointMs.get(); }
    public long lastCheckpointPurged() { return lastCheckpointPurged.get(); }

    /** The generated typed per-record hot stores — used by the RecordMap plane. */
    public StratumEventStores stores() { return stores; }

    /** The warm disk tier — the RecordMap plane's second leg for warm-typed records. */
    public WarmStore warm() { return warm; }

    // -----------------------------------------------------------------------
    // Retention cleanup
    // -----------------------------------------------------------------------

    /**
     * The scheduled tick: the leader-only retention sweep, then this pod's own
     * daily checkpoint.
     *
     * <p>Two sweeps on one timer, not one sweep. The checkpoint transaction is what
     * the record-lifecycle decision replaces retention WITH, and it does not replace
     * it yet: no fleet record type carries {@code (spec.deleted_flag)}, so the purge
     * step selects nothing and removing retention today would simply let every store
     * grow without bound. Retention therefore stays until the fleet annotation pass
     * has run, and this line is the seam where it is deleted — see
     * {@code docs/DURABILITY.md}.
     *
     * <p>The checkpoint is NOT leader-gated: retention mutates the cluster-wide maps
     * once, while the checkpoint writes this pod's own volume and truncates this
     * pod's own redo logs. One pod doing that for the cluster would leave every other
     * pod with no baseline to start from.
     */
    private void runCleanupCycle() {
        runRetentionCleanupIfLeader();
        runDailyCheckpoint();
    }

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
