// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hot;

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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast.StratumRole;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;

/**
 * Hot-tier recovery: replay this node's redo logs into the cluster, then wait for
 * the other reachable nodes to have done the same before hot reads are served.
 *
 * <h2>The union</h2>
 * Each WRITER replays its OWN log — {@code RedoLog.restore} unchanged, including its
 * recovery of a crashed {@code .building}/{@code .temp} — through
 * {@link EpochMergeProcessor}, which resolves each key at its partition owner by
 * highest store epoch. The union is therefore the map itself, assembled by the same
 * partitioning that decided the writes; nothing ships logs between nodes.
 *
 * <p>A writer's log is complete, so one writer could in principle restore the store
 * alone. The union is still what runs, because a writer that joined the cluster later
 * than its peers has a log that starts later than theirs, and the epoch merge is what
 * decides between them.
 *
 * <p>SEARCH members replay nothing: no volume, no log, never a contributor. They
 * still wait at the barrier below, because they are handed partitions the moment they
 * join and answering a query off a half-recovered store is the failure this refuses.
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
     * ledger the majority never saw. It carries MORE replication than they do, and
     * why is on {@code StratumMapConfig.coordinationMap}.
     *
     * <p>The entry is still filed once per boot and never revised. What changed on
     * 2026-09-22 is that it is no longer filed once and then unwritable: a member
     * that finds its own entry gone puts it back ({@link #refileReport}), because
     * the barrier needs a report from every live member and a serving member has no
     * other occasion to file one.
     */
    public static final String LEDGER_MAP = "stratum.hot.__restore_ledger";

    /**
     * The members that are INSIDE their own hot restore right now: member UUID ->
     * TRUE, put on entry to {@link #awaitClusterReplay} and removed when that
     * member's restore completes.
     *
     * <p>It exists because the ledger entry is a BOOT SNAPSHOT and the barrier was
     * reading it as if it were a statement about the present. A member files its
     * number once, at its own start, and never revises it; a peer that started
     * hours ago therefore reports what its volume held hours ago, under whatever
     * code it was running then. A restarting sibling was deciding its own
     * durability against that.
     *
     * <p>What this map adds is the one fact the ledger cannot carry: whether the
     * peer is STILL BUSY RECOVERING or has finished and is serving. A live member
     * that has a ledger entry and is not in this map has already passed this
     * barrier and is holding the record maps in memory — it IS a live durable copy
     * for the purposes of the restart happening now, whatever number it filed at
     * its own boot. See the contributor count below.
     *
     * <p>Absence is the serving state deliberately, not presence: a member running
     * code that predates this map writes nothing to it and is therefore read as
     * serving, which is exactly right for it — an already-running peer on an older
     * image is precisely the case that cannot file a fresh report. A member that
     * has NOT finished (including every member of a simultaneous restart) marks
     * itself before filing its ledger entry, so it can never be mistaken for a
     * serving one by a sibling that reads the two in either order.
     *
     * <p>Public and configured by name for the same reason {@link #LEDGER_MAP} is:
     * it is read by the barrier, so a minority partition must not be able to keep
     * its own private copy ({@code StratumMapConfig.build}).
     */
    public static final String INFLIGHT_MAP = "stratum.hot.__restore_inflight";

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
     * through the write path's {@link RecordPersistTask}.
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
     *                         alarm: it is how many WRITERS there are SUPPOSED to
     *                         be, so the barrier cannot be satisfied by fewer, and
     *                         fewer contributors than this is LOUD, because the
     *                         recovered store is then thinner than the durability
     *                         the writes were acked under
     * @return the number of WRITERS that contributed a durable copy — the ones
     *         that reported holding one at their own boot, plus the live writer peers
     *         that have already completed this barrier and are serving the maps now
     *         ({@link #INFLIGHT_MAP}). Search members are never counted.
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
        IMap<String, Boolean> inflight = hz.getMap(INFLIGHT_MAP);
        String self = hz.getCluster().getLocalMember().getUuid().toString();
        boolean writer = StratumRole.of(hz.getCluster().getLocalMember()).isWriter();
        // A SEARCH member files NOTHING and waits. It has no volume, so it has no
        // durable copy to report and no replay of its own to report done; it is
        // never a contributor and must never be counted as one. What it still has to
        // do is wait, because it will be handed partitions by the live cluster the
        // moment it joins and a query it answers off a half-recovered store is the
        // same silent incompleteness this barrier exists to refuse — answered from a
        // member that never had a volume to be honest about.
        //
        // Filing nothing is also what keeps it out of the counts without a special
        // case in them: it has no ledger entry, so liveReports cannot see it, and it
        // has no in-flight mark, which only ever suppresses a contributor.
        if (writer) {
            // Marked BEFORE the ledger entry is filed, so the two are never observable
            // in the order that would let a sibling read this member as serving while
            // it is in fact still here. Removed only on the way out through the
            // success path: a member that refuses below is exiting, and its entries —
            // this one and its ledger report — stop counting when it leaves the
            // cluster view, which liveReports already handles. Clearing it on the way
            // out of a REFUSAL would be the one direction that is unsafe, because it
            // would advertise a member that never finished as one that had.
            inflight.put(self, Boolean.TRUE);
            ledger.put(self, durableRecordCount(redoRecordsRead, checkpointRecords));
        }

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
            //
            // WRITERS, not members. Search members file no report, so counting them
            // into the expectation would make the barrier unsatisfiable by exactly
            // the number of them the deployment happened to be running — every
            // search member added for query throughput would cost every pod's
            // restart a five-minute timeout.
            expected = Math.max(StratumRole.writerCount(hz), minDurableCopies);
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
                        + " expected writer members reported their redo replay within " + timeout
                        + " (cluster size " + hz.getCluster().getMembers().size()
                        + ", writers " + StratumRole.writerCount(hz)
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
        // A SECOND way to be a contributor, for the half of the problem the number
        // above cannot express. The ledger entry is filed once, at that member's own
        // boot, and never revised — so it describes a moment, not the present, and a
        // peer that is up and serving right now can be carrying a number that is
        // stale (its volume has been checkpointed since), or that was computed by an
        // older image under the pre-9fa7ebc redo-tail-only rule. Both read as "this
        // instance holds nothing" and both talk a healthy cluster below its floor.
        //
        // Observed 2026-09-21 as stratum-1 and stratum-2 crash-looping "1 of 2"
        // against a serving stratum-0: stratum-0 was still on the pre-9fa7ebc image,
        // had checkpointed since its own boot, and its permanent entry was therefore
        // 0 while it was serving 468 rows. Nothing about that is recoverable by
        // counting better — the number is simply from before.
        //
        // So a peer that has finished its own restore counts on the strength of
        // having finished it. It passed this same barrier when it started, and it is
        // holding the record maps in memory now; for the restart happening HERE it
        // is a live durable copy, and requiring it to re-file a report every time a
        // sibling restarts is asking it to prove again something it already proved.
        //
        // What keeps this from becoming "count everyone": a member that has NOT
        // finished says so in INFLIGHT_MAP before it files anything, and every
        // member of a simultaneous restart is in that state, so restarting pods
        // cannot credit each other into a quorum none of them has. Only the local
        // member is excluded by name — it is inside this call, so its own absence
        // from the map would otherwise vouch for it.
        Set<String> restoring = liveInflight(hz);
        int contributors = 0;
        for (Map.Entry<String, Integer> report : reports.entrySet()) {
            String member = report.getKey();
            Integer durable = report.getValue();
            boolean holdsDurableCopy = durable != null && durable > 0;
            boolean servingPeer = !member.equals(self) && !restoring.contains(member);
            if (holdsDurableCopy || servingPeer) {
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
                    + " instances that held neither a redo log nor a checkpoint and are not"
                    + " already serving; refusing to serve a silently incomplete hot store");
        }
        // Only now, and only here: this member has a complete store and is about to
        // serve it, which is the whole of what a peer reads out of this map's
        // absence.
        if (writer) {
            inflight.delete(self);
        }
        log.info("hot restore complete ({} member): {} of {} writers contributed a durable copy"
                + " ({} of them already serving; this node: {} redo record(s) + {} checkpoint row(s))",
                writer ? "writer" : "search", contributors, expected,
                reports.size() - restoring.size(), redoRecordsRead, checkpointRecords);
        return contributors;
    }

    /**
     * The one number a member files about itself, because the contributor check
     * asks one question of it: did this member have a durable copy of its rows to
     * bring back? The redo tail and the checkpoint baseline are the two halves of
     * that copy and neither alone answers it.
     *
     * <p>Saturated rather than wrapped: an overflowed sum could come out zero or
     * negative and report a full volume as an empty one, which is the exact
     * misreading the count exists to stop.
     */
    public static int durableRecordCount(int redoRecordsRead, long checkpointRecords) {
        return (int) Math.min((long) redoRecordsRead + checkpointRecords, Integer.MAX_VALUE);
    }

    /**
     * Puts this member's ledger report back if it is missing, for a member that has
     * already passed the barrier and is serving.
     *
     * <p>The entry is written once, at boot, and until now nothing could ever write
     * it again — so anything that removed it removed it for good, and the barrier,
     * which needs a report from every live member, became unsatisfiable for as long
     * as this member kept serving. Every other pod's restart then timed out on
     * "only N of N+1 expected instances reported" and no number of restarts could
     * help, because the missing report belonged to the one member that was not
     * restarting. That is the 2026-09-22 crash-loop.
     *
     * <p>Two things remove it. Member death takes it with the partition replicas it
     * lived on, which {@code StratumMapConfig.coordinationMap} now prevents by
     * replicating this map to every member. {@link #liveReports} removes it too,
     * pruning any entry whose member is absent from the pruner's cluster view — and
     * a view that is momentarily missing a member which is in fact alive turns
     * housekeeping into a permanent wedge. This repair is what makes the second one
     * survivable, and it is the reason to repair rather than to stop pruning: the
     * ledger is keyed by a UUID minted on every restart, so a map that is never
     * pruned grows by one dead entry per pod per restart for the life of the
     * cluster.
     *
     * <p>{@code putIfAbsent}, never {@code put}: this REPLACES NOTHING. The entry is
     * a boot snapshot by design — {@code 396d952} settled that a serving peer counts
     * on the strength of the barrier it already passed, not on its number being
     * current — and a repair that revised the number would be re-deciding that.
     *
     * @param durableRecords what this member filed at its boot
     *                       ({@link #durableRecordCount}), not a fresh count
     * @return true if a missing entry was put back
     */
    public static boolean refileReport(HazelcastInstance hz, int durableRecords) {
        String self = hz.getCluster().getLocalMember().getUuid().toString();
        IMap<String, Integer> ledger = hz.getMap(LEDGER_MAP);
        if (ledger.putIfAbsent(self, durableRecords) != null) {
            return false;
        }
        log.warn("hot restore: this member's ledger report was missing and has been re-filed"
                + " ({} durable record(s) at boot). Until it was, every peer restart would have"
                + " timed out waiting for a report from a member that is serving and cannot"
                + " restart itself to file one.", durableRecords);
        return true;
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
    /**
     * The members of the CURRENT cluster view that are inside their own restore,
     * with every other entry deleted on the way past.
     *
     * <p>Same shape and same reasoning as {@link #liveReports}: the correctness is
     * in the filter, and the delete is housekeeping against a map keyed by a UUID
     * that changes on every restart. The filter matters more here than there,
     * because a departed member's entry would be read as "that member is still
     * recovering" and so would suppress a contributor rather than invent one — the
     * safe direction, but a permanent one, since nothing else ever removes the
     * entry of a member that exited inside its barrier.
     */
    private static Set<String> liveInflight(HazelcastInstance hz) {
        Set<String> live = StratumRole.writerUuids(hz);
        IMap<String, Boolean> inflight = hz.getMap(INFLIGHT_MAP);
        Set<String> restoring = new HashSet<>();
        for (Map.Entry<String, Boolean> e : inflight.entrySet()) {
            if (!live.contains(e.getKey())) {
                inflight.delete(e.getKey());
            } else if (Boolean.TRUE.equals(e.getValue())) {
                restoring.add(e.getKey());
            }
        }
        return restoring;
    }

    private static Map<String, Integer> liveReports(HazelcastInstance hz, IMap<String, Integer> ledger) {
        // WRITERS of the current view, not members of it: only a writer ever files a
        // report, and the population the barrier is decided against has to be the
        // same one the expectation is computed over.
        Set<String> live = StratumRole.writerUuids(hz);
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
