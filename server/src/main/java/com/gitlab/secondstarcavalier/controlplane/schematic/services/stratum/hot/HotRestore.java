// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
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

    /** The cluster-visible replay ledger: member UUID -> records replayed. */
    static final String LEDGER_MAP = "stratum.hot.__restore_ledger";

    private HotRestore() {}

    /** What one node's replay produced, for the caller to log and assert on. */
    public static final class Result {
        public final int applied;
        public final int skipped;
        public final List<String> diverged;

        Result(int applied, int skipped, List<String> diverged) {
            this.applied = applied;
            this.skipped = skipped;
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
        redoLog.restore(rec -> {
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
        return new Result(counts[0], counts[1], diverged);
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
     * @param minDurableCopies the declared floor; fewer contributors than this is
     *                         LOUD, because the recovered store is then thinner
     *                         than the durability the writes were acked under
     * @return the number of instances that contributed a log
     * @throws IllegalStateException if the wait times out
     */
    public static int awaitClusterReplay(HazelcastInstance hz, int recordsReplayed,
                                         int minDurableCopies, Duration timeout) {
        IMap<String, Integer> ledger = hz.getMap(LEDGER_MAP);
        ledger.put(hz.getCluster().getLocalMember().getUuid().toString(), recordsReplayed);

        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        int members;
        while (true) {
            members = hz.getCluster().getMembers().size();
            if (ledger.size() >= members) {
                break;
            }
            if (System.currentTimeMillis() >= deadlineMs) {
                throw new IllegalStateException("hot restore: only " + ledger.size() + " of " + members
                        + " reachable instances reported their redo replay within " + timeout
                        + "; refusing to serve a silently incomplete hot store");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("hot restore interrupted while awaiting peer replay", e);
            }
        }

        int contributors = 0;
        for (Integer replayed : ledger.values()) {
            if (replayed != null && replayed > 0) {
                contributors++;
            }
        }
        if (contributors < minDurableCopies) {
            log.error("hot restore: only {} of {} instances contributed a redo log, below"
                    + " min_durable_copies={}. Every acked write was durable on {} nodes, so the"
                    + " recovered store may be missing rows whose only surviving copies were on the"
                    + " instances that contributed nothing.",
                    contributors, members, minDurableCopies, minDurableCopies);
        } else {
            log.info("hot restore complete: {} of {} instances contributed a redo log", contributors, members);
        }
        return contributors;
    }
}
