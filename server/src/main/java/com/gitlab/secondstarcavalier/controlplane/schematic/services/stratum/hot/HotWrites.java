// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import java.util.concurrent.Callable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.QuorumGate;

/**
 * The hot write path: the quorum gate, then the backup-aware write.
 *
 * The generated {@code StratumEventStores} calls this instead of
 * {@code imap.put(...)}, so the durability discipline is stated once here rather
 * than emitted per record type.
 *
 * <h2>What the ack waits on</h2>
 * In-memory replication across {@code min_durable_copies} instances, and nothing
 * else. The gate refuses the write below that floor, and the put itself does not
 * return until the synchronous backup has run. The redo append rides the same
 * entry processor onto both of those instances but is QUEUED there, not written,
 * so disk is not the blocker — see {@link RedoAppendProcessor} and
 * {@link HotRedoLogs}.
 */
public final class HotWrites {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HotWrites.class);

    private final HazelcastInstance hz;
    private final int syncBackupCount;
    private final int floorOverride;

    /**
     * @param syncBackupCount the map's configured {@code backupCount} — the number
     *                        of SYNCHRONOUS in-memory replicas the put waits for.
     *                        {@code asyncBackupCount} deliberately plays no part:
     *                        an async replica is not held when the write returns,
     *                        so counting it would inflate the durable-copy count
     *                        with a copy that may not exist yet.
     */
    /**
     * @param floorOverride the deployment's own durability floor, replacing the
     *                      one the record declares, or 0 to honour the
     *                      declaration. A single-instance deployment (a developer
     *                      laptop, a single-node test) sets 1 DELIBERATELY and is
     *                      logged saying so — which is the point of it being a
     *                      declared number rather than an automatic degrade on
     *                      cluster size. A production cluster that has lost two
     *                      members looks exactly like a laptop, and the automatic
     *                      version cannot tell them apart.
     */
    public HotWrites(HazelcastInstance hz, int syncBackupCount, int floorOverride) {
        this.hz = hz;
        this.syncBackupCount = syncBackupCount;
        this.floorOverride = floorOverride;
        if (floorOverride > 0) {
            log.warn("hot writes: durability floor overridden to {} distinct nodes by deployment"
                    + " configuration; the records' declared min_durable_copies is NOT in force",
                    floorOverride);
        }
    }

    /** The floor actually enforced for a record declaring {@code declared}. */
    public int effectiveFloor(int declared) {
        return floorOverride > 0 ? floorOverride : declared;
    }

    /**
     * Distinct nodes that will hold the value when the write returns: the owner
     * plus its synchronous backups, capped by the cluster the node can actually
     * see. A partition cannot create replicas out of members that are not there,
     * so a 3-member map on a 1-member view supplies one copy, not two.
     */
    public int durableCopies() {
        int members = hz.getCluster().getMembers().size();
        return Math.min(members, syncBackupCount + 1);
    }

    /**
     * Writes one record: refuse below the declared floor, otherwise put through
     * the redo-queueing entry processor.
     *
     * <p>The refusal comes FIRST and throws, so a below-quorum write has no local
     * effect at all. That is the whole point of the floor: a cluster that has lost
     * members is otherwise indistinguishable from a single-node development tree,
     * and an implicit degrade removes the guarantee exactly where it mattered.
     * A one-node deployment declares {@code min_durable_copies: 1} and says so.
     *
     * @param toJson the record's redo payload, computed on the INGESTING node —
     *               once, before the processor is serialized, so the two logs hold
     *               identical bytes for one write
     * @param epoch  the row's store epoch, likewise stamped once here
     */
    public <V> void write(IMap<String, V> map, String mapName, String key, V value,
                          int minDurableCopies, Callable<String> toJson, long epoch) {
        QuorumGate.require(mapName, durableCopies(), effectiveFloor(minDurableCopies));
        // The redo payload is computed only when this deployment keeps a log for
        // the type. Every node opens the same dataset list, so no log here means no
        // log anywhere, and encoding a payload nothing will append is pure cost on
        // the ingest thread.
        String payload = null;
        if (HotRedoLogs.logFor(mapName) != null) {
            try {
                payload = toJson.call();
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "hot record " + mapName + " key=" + key + " cannot be encoded for the redo log", e);
            }
        }
        map.executeOnKey(key, new RedoAppendProcessor<>(mapName, value, payload, epoch));
    }
}
