// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;

/**
 * Hot-tier recovery: replay this node's redo logs into the cluster, then wait for
 * the other reachable nodes to have done the same before hot reads are served.
 *
 * <h2>The union</h2>
 * Every node's log is partial. Each node replays its OWN log — {@code
 * RedoLog.restore} unchanged, including its recovery of a crashed
 * {@code .building}/{@code .temp} — through {@link EpochMergeProcessor}, which
 * resolves each key at its partition owner by highest store epoch. The union is
 * therefore the map itself, assembled by the same partitioning that decided the
 * writes; nothing ships logs between nodes.
 *
 * <h2>Why reads wait (Q2)</h2>
 * A node that has replayed only its own log holds a store that is
 * SILENTLY INCOMPLETE — it looks answerable and is missing every row the other
 * nodes owned. Serving that is worse than a slow start, so hot reads are refused
 * until every reachable instance has reported its replay done. What that costs is
 * named: cold-start latency becomes a function of the slowest volume.
 *
 * <h2>The barrier is also what covers the async append window</h2>
 * The redo append is queued after the in-memory ack, not written before it
 * ({@link HotRedoLogs}), so an instance killed inside that window comes back with
 * a log missing rows it had acked. Nothing extra is needed for that: those rows
 * were acked on {@code min_durable_copies} instances, every one of which queued
 * the same append, so a peer's log still holds them and the union puts them back.
 * What makes that a guarantee rather than a race is precisely the wait below —
 * a node serving reads off its own replayed log alone would report exactly the
 * rows it lost as absent.
 *
 * <p>The residual, named rather than papered over: if EVERY replica of a row
 * loses the same queued append, the row is not in any log and no barrier can
 * recover it. That is the accepted consequence of acking on in-memory
 * replication instead of on the disk write; the record is still on the event log,
 * which a cold rebuild replays.
 *
 * <h2>Why equal-epoch divergence stops the service (Q3)</h2>
 * See {@link EpochMergeProcessor}. A key reported DIVERGED is proof the atomic
 * write path was violated; this refuses to start and names the key rather than
 * pick a winner.
 */
public final class HotRestore {

    private static final Logger log = LoggerFactory.getLogger(HotRestore.class);

    /**
     * The cluster-visible replay ledger: member UUID -> records this member holds
     * durably, which is its replayed redo tail PLUS its committed checkpoint.
     *
     * <p>Public because it is a map the Hazelcast config has to configure by name:
     * it carries the same backup count and the same split-brain protection as the
     * record maps (see {@code StratumMapConfig.build}). A coordination map left on
     * Hazelcast's defaults is the same split-brain hole the record maps had — a
     * minority partition would file and read its own reports locally, and the
     * barrier that exists to refuse an incomplete recovery would be decided off a
     * ledger the majority never saw.
     */
    public static final String LEDGER_MAP = "stratum.hot.__restore_ledger";

    private HotRestore() {}

    /** What one node's replay produced, for the caller to log and assert on. */
    public static final class Result {
        public final int applied;
        public final int skipped;
        /**
         * Records READ from this node's log, whatever each one's outcome was.
         *
         * <p>This, not {@link #applied}, is what says the node had a redo log: on a
         * rolling restart the peers still own the partitions, so this node's rows
         * are already present at their own-or-newer epoch and every record replays
         * as SKIPPED. {@code applied == 0} there means "nothing needed catching
         * up", not "nothing was there" — the two are only distinguishable by
         * whether anything was read at all.
         */
        public final int read;
        public final List<String> diverged;

        Result(int applied, int skipped, int read, List<String> diverged) {
            this.applied = applied;
            this.skipped = skipped;
            this.read = read;
            this.diverged = diverged;
        }
    }

    /**
     * Replays one node's log for one dataset into {@code map}.
     *
     * <p>Does NOT re-append: the records being replayed are already in this
     * node's log, and the merge runs through {@link EpochMergeProcessor}, not
     * through the write path's {@link RedoAppendProcessor}.
     */
    public static Result replay(RedoLog redoLog, HotDataset dataset, IMap<String, Message> map) {
        List<String> diverged = new ArrayList<>();
        int[] counts = new int[2];
        int read = redoLog.restore(rec -> {
            long epoch = epochOf(rec, dataset);
            Message candidate = rec.deleted ? null : dataset.fromJson.apply(rec.payload);
            Integer outcome = map.executeOnKey(rec.key,
                    new EpochMergeProcessor(dataset.epochField, candidate, epoch, rec.deleted));
            if (outcome != null && outcome == EpochMergeProcessor.DIVERGED) {
                diverged.add(rec.key);
            } else if (outcome != null && outcome == EpochMergeProcessor.APPLIED) {
                counts[0]++;
            } else {
                counts[1]++;
            }
        });
        return new Result(counts[0], counts[1], read, diverged);
    }

    /**
     * The epoch a replayed record is resolved by.
     *
     * <p>It is read from the record's metadata, where the write path stamped it,
     * NOT recomputed from the payload: a tombstone has no payload to read one
     * from, and a record whose epoch could be recomputed at restore time would be
     * ordering by restore time rather than by write time. A record with no
     * metadata predates the epoch stamp and cannot be ordered against anything —
     * refused rather than defaulted to 0, which would make it lose silently to
     * every other copy.
     */
    private static long epochOf(RedoLog.Record rec, HotDataset dataset) {
        if (rec.metadata == null || rec.metadata.isEmpty()) {
            throw new IllegalStateException("redo record for " + dataset.mapName + " key=" + rec.key
                    + " carries no store epoch; it cannot be ordered against the other nodes' logs");
        }
        try {
            return Long.parseLong(rec.metadata);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("redo record for " + dataset.mapName + " key=" + rec.key
                    + " has an unreadable store epoch " + rec.metadata, e);
        }
    }

    /**
     * Reports this node's replay done and waits for every other reachable member
     * to report the same (Q2).
     *
     * @param redoRecordsRead records read from this node's logs across all datasets
     *                         ({@link Result#read} summed) — NOT records applied.
     *                         A node whose log replayed entirely as SKIPPED still
     *                         had a log, and on a rolling restart that is the
     *                         normal case; counting applied records only would
     *                         raise the alarm below on every healthy restart.
     * @param checkpointRecords rows in this node's committed checkpoint
     *                         ({@code Checkpoint.rowCount}), counted whether or not
     *                         the dump was loaded this boot. This node's durable
     *                         copy is the checkpoint PLUS the redo tail since it
     *                         (DURABILITY.md: "the checkpoint file IS the durable
     *                         copy"), and {@code DailyCheckpoint} moves rows from the
     *                         tail into the file every cycle — so an empty redo log
     *                         on a volume holding a checkpoint means "nothing new
     *                         since the dump", not "nothing". Counting the tail alone
     *                         reported every just-checkpointed pod as holding nothing.
     * @param minDurableCopies the declared floor. It bounds the wait as well as the
     *                         alarm: it is how many instances there are SUPPOSED to
     *                         be, so the barrier cannot be satisfied by fewer, and
     *                         fewer contributors than this is LOUD, because the
     *                         recovered store is then thinner than the durability
     *                         the writes were acked under
     * @return the number of instances that contributed a durable copy
     * @throws IllegalStateException if the wait times out, or if the instances that
     *                               did contribute came in below {@code
     *                               minDurableCopies} — a recovery thinner than the
     *                               durability its writes were acked under is refused,
     *                               not served. Zero contributors is not that case;
     *                               see the comment at the check.
     */
    public static int awaitClusterReplay(HazelcastInstance hz, int redoRecordsRead,
                                         long checkpointRecords, int minDurableCopies,
                                         Duration timeout) {
        IMap<String, Integer> ledger = hz.getMap(LEDGER_MAP);
        // One number, because the check below asks one question of it: did this
        // member have a durable copy of its rows to bring back? The tail and the
        // baseline are the two halves of that copy and neither alone answers it.
        // Saturated rather than wrapped: an overflowed sum could come out zero or
        // negative and report a full volume as an empty one, which is the exact
        // misreading this parameter exists to stop.
        long durableRecords = (long) redoRecordsRead + checkpointRecords;
        ledger.put(hz.getCluster().getLocalMember().getUuid().toString(),
                (int) Math.min(durableRecords, Integer.MAX_VALUE));

        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        int expected;
        Map<String, Integer> reports;
        while (true) {
            // The expected count is recomputed on every pass, and it never falls below
            // the declared floor. The pods of a StatefulSet do not start together, so
            // the FIRST one up is alone: it sees a cluster of one, its own report
            // satisfies "has everyone I can see reported?" immediately, and hot reads
            // open on a store holding one node's redo log — the exact condition this
            // barrier exists to refuse, reached by the barrier itself. The observed
            // size cannot answer "is the cluster here yet?" while the cluster is still
            // assembling; min_durable_copies is the declaration that can, so it is the
            // floor, and the observed size only ever raises the expectation above it.
            //
            // A deployment that really runs one instance says so with
            // STRATUM_MIN_DURABLE_COPIES=1 — the same declared number that already
            // decides the map's backup count and its split-brain minimum, so a
            // single-node run that has not set it cannot take a write either.
            expected = Math.max(hz.getCluster().getMembers().size(), minDurableCopies);
            // Only reports from members of the CURRENT cluster view count. The
            // ledger is keyed by member UUID, which Hazelcast mints fresh on every
            // restart, so a departed member's entry outlives it and the raw map
            // size answers "how many reports have ever been filed?" rather than
            // "how many of the instances that are here now have replayed?".
            // Counting the raw size is the barrier's own version of the failure it
            // exists to refuse: two of three pods die, the survivor's entry stays,
            // the recovering pod adds its own, the count reaches expected, and hot
            // reads open while the third pod has replayed nothing.
            reports = liveReports(hz, ledger);
            if (reports.size() >= expected) {
                break;
            }
            if (System.currentTimeMillis() >= deadlineMs) {
                throw new IllegalStateException("hot restore: only " + reports.size() + " of " + expected
                        + " expected instances reported their redo replay within " + timeout
                        + " (cluster size " + hz.getCluster().getMembers().size()
                        + ", min_durable_copies " + minDurableCopies
                        + "); refusing to serve a silently incomplete hot store");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("hot restore interrupted while awaiting peer replay", e);
            }
        }

        // A contributor is an instance that HAD A DURABLE COPY of its rows — not one
        // that had rows to apply, and not one that happened to have a non-empty redo
        // log. A node whose every record was already present at its own-or-newer
        // epoch (the rolling-restart case) contributed its log in full; a node whose
        // log is empty because DailyCheckpoint truncated it to the dump point
        // contributed its checkpoint; a node with neither — a first boot, a replaced
        // or lost volume — is the one that contributed nothing.
        //
        // Counting redo rows alone conflated the middle case with the last one, and
        // that is a scheduled fault, not a rare one: every pod truncates its log to
        // empty on every checkpoint cycle, so a pod restarting while its peers sit
        // just after their own checkpoints would find them reported as empty volumes
        // and refuse below. Observed 2026-09-21 as stratum-0 crash-looping against a
        // healthy 2-of-3 cluster; see
        // decisions/2026-09-21-stratum0-checkpoint-quorum-crashloop.md.
        //
        // Counted over the same live-member-filtered reports the barrier broke on,
        // not over the raw map: a departed member's entry must not be able to talk
        // the store up to min_durable_copies either.
        int contributors = 0;
        for (Integer durable : reports.values()) {
            if (durable != null && durable > 0) {
                contributors++;
            }
        }
        // Below the floor is a REFUSAL, not a warning. The message below says the
        // recovered store may be missing rows whose only surviving copies were on
        // the instances that contributed nothing; returning normally after saying
        // that hands the caller a store it has just described as incomplete, and
        // restoreHotTier goes straight on to open the hot read gate over it. Same
        // posture as the barrier timeout above: refuse to serve a silently
        // incomplete hot store rather than serve it loudly.
        //
        // ZERO contributors is the one case that is not this failure. It means no
        // live instance had a redo log OR a checkpoint — a first deployment, or a
        // total loss of every volume. Neither is a partial recovery there is anything to refuse:
        // nothing was replayed thinly, nothing was replayed at all, and the hot tier
        // is empty rather than under-durable. That case is already covered by the
        // OTHER leg of the read gate — hotReadable() also requires the event-log
        // cold-start replay to have finished, which is the recovery path for a store
        // with no redo logs behind it. Throwing here instead would crash-loop every
        // first-ever bring-up, which is not a durability failure to fail closed on.
        if (contributors > 0 && contributors < minDurableCopies) {
            throw new IllegalStateException("hot restore: only " + contributors + " of " + expected
                    + " instances contributed a durable copy, below min_durable_copies " + minDurableCopies
                    + ". Every acked write was durable on " + minDurableCopies + " nodes, so the"
                    + " recovered store may be missing rows whose only surviving copies were on the"
                    + " instances that held neither a redo log nor a checkpoint; refusing to serve a"
                    + " silently incomplete hot store");
        }
        log.info("hot restore complete: {} of {} instances contributed a durable copy"
                + " (this node: {} redo record(s) + {} checkpoint row(s))",
                contributors, expected, redoRecordsRead, checkpointRecords);
        return contributors;
    }

    /**
     * The ledger entries filed by members of the CURRENT cluster view, with every
     * other entry deleted on the way past.
     *
     * <p>The correctness is in the FILTER, not in the pruning: the barrier is
     * decided against the live view read in the same pass, so an entry left by a
     * member that has since departed cannot be counted whether or not anything
     * ever removed it. The delete is housekeeping — the ledger is keyed by a UUID
     * that changes on every restart, so without it the map grows by one dead entry
     * per pod per restart for the life of the cluster.
     *
     * <p>A {@code MembershipListener} evicting on {@code memberRemoved} was the
     * alternative and is NOT used: it would have to be registered, unregistered
     * and reasoned about across the EventStore's init/shutdown/config-reload
     * lifecycle, and it fixes nothing this does not — a listener still races the
     * barrier (the check can run between the member leaving and the event being
     * delivered), so the check-time filter would have to exist anyway. One
     * mechanism that is sufficient on its own beats two that are each necessary.
     *
     * <p>Filtering costs a cluster-view read per pass, which is a local field read.
     */
    private static Map<String, Integer> liveReports(HazelcastInstance hz, IMap<String, Integer> ledger) {
        Set<String> live = new HashSet<>();
        for (Member m : hz.getCluster().getMembers()) {
            live.add(m.getUuid().toString());
        }
        Map<String, Integer> reports = new HashMap<>();
        for (Map.Entry<String, Integer> e : ledger.entrySet()) {
            if (live.contains(e.getKey())) {
                reports.put(e.getKey(), e.getValue());
            } else {
                ledger.delete(e.getKey());
            }
        }
        return reports;
    }
}
