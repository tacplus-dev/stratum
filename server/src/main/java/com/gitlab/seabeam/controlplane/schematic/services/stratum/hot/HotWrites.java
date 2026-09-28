// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hot;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.protobuf.Message;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.StratumHotStores;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast.StratumRole;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.QuorumGate;

/**
 * The hot write path: the quorum gate, the in-memory put, then the disk legs on
 * every writer.
 *
 * The generated {@code StratumEventStores} calls this instead of
 * {@code imap.put(...)}, so the durability discipline is stated once here rather
 * than emitted per record type.
 *
 * <h2>What the ack waits on</h2>
 * The value's in-memory replication — the primary and {@code backupCount}
 * synchronous backups — and nothing else. The disk legs ride
 * {@link RecordPersistTask} out to every writer member and are QUEUED there, not
 * written, so disk is not the blocker; see {@link HotRedoLogs}.
 *
 * <h2>What the gate counts</h2>
 * WRITER members, and nothing else. A writer appends every write cluster-wide to
 * its own redo log whichever member owns the partition, so the number of writers in
 * the cluster IS the number of durable copies a write gets — and a cluster below
 * {@code min_durable_copies} writers refuses the write rather than taking one it
 * cannot make durable. Search members hold partitions and answer queries and have
 * no volume, so a cluster of one writer and five search members supplies ONE durable
 * copy; counting members would report six and ack a write with one copy on disk.
 *
 * <p>{@code backupCount} plays no part in this number any more. It used to be the
 * whole of it: the append rode the partition replication onto the owner and its
 * synchronous backups, so the logs were the replicas and the only way to count
 * durable copies was to count them. That is the coupling the member roles remove —
 * see {@code StratumMapConfig.DEFAULT_BACKUP_COUNT}.
 */
public final class HotWrites {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HotWrites.class);

    /**
     * The Hazelcast executor the persist tasks are submitted on, named so it is
     * identifiable in a thread dump and in Hazelcast's own metrics.
     */
    public static final String PERSIST_EXECUTOR = "stratum-hot-persist";

    /**
     * The record maps this BUILD keeps a redo log for, from the same generated
     * dataset list {@link HotRedoLogs} opens its logs from.
     *
     * <p>Read from the generated list rather than from this node's own open logs.
     * The question the write path asks is "will anything append this payload
     * anywhere?", and on a search member the answer off the local logs is always no —
     * it has none — which would leave the payload uncomputed and the writers sent a
     * task with nothing to append. Every member of the cluster runs the same build,
     * so the generated list is the same on all of them and is the right source.
     */
    private static final Set<String> LOGGED_MAPS = loggedMaps();

    private static Set<String> loggedMaps() {
        Set<String> names = new HashSet<>();
        for (HotDataset d : StratumHotStores.datasets()) {
            names.add(d.mapName);
        }
        return names;
    }

    private final HazelcastInstance hz;
    private final int floorOverride;

    /**
     * @param floorOverride the deployment's own durability floor, replacing the
     *                      one the record declares, or 0 to honour the
     *                      declaration. A single-instance deployment (a developer
     *                      laptop, a single-node test) sets 1 DELIBERATELY and is
     *                      logged saying so — which is the point of it being a
     *                      declared number rather than an automatic degrade on
     *                      cluster size. A production cluster that has lost two
     *                      writers looks exactly like a laptop, and the automatic
     *                      version cannot tell them apart.
     */
    public HotWrites(HazelcastInstance hz, int floorOverride) {
        this.hz = hz;
        this.floorOverride = floorOverride;
        if (floorOverride > 0) {
            log.warn("hot writes: durability floor overridden to {} distinct writer members by"
                    + " deployment configuration; the records' declared min_durable_copies is NOT"
                    + " in force", floorOverride);
        }
    }

    /** The floor actually enforced for a record declaring {@code declared}. */
    public int effectiveFloor(int declared) {
        return floorOverride > 0 ? floorOverride : declared;
    }

    /**
     * Distinct members that will hold the write durably: the WRITERS in the cluster
     * view.
     *
     * <p>Not capped by a backup count and not a per-map number, which is what it was
     * until the member roles landed. Both of those followed from the append riding
     * the partition replication: the logs were the map's own replicas, so the count
     * was {@code min(members, that map's backupCount + 1)} and a record declaring a
     * higher floor needed an extra physical replica to reach it. The persist now goes
     * to every writer directly, so one number answers for every map — how many
     * members have a volume — and a record declaring a higher floor is declaring it
     * needs more of them.
     */
    public int durableCopies() {
        return StratumRole.writerCount(hz);
    }

    /**
     * Writes one record: refuse below the declared floor, put it in memory, then send
     * the disk legs to every writer.
     *
     * <p>The refusal comes FIRST and throws, so a below-quorum write has no local
     * effect at all. That is the whole point of the floor: a cluster that has lost
     * writers is otherwise indistinguishable from a single-node development tree,
     * and an implicit degrade removes the guarantee exactly where it mattered.
     * A one-node deployment declares {@code min_durable_copies: 1} and says so.
     *
     * <p>The put comes before the persist because that is the order the ack is
     * defined on: the write is acked on its in-memory replication, and the persist is
     * a queue on each writer that the ack does not wait for. The writer set is read
     * ONCE, before the gate, and the same set is both counted and sent to — so the
     * task cannot go to fewer members than the gate passed on.
     *
     * @param toJson the record's redo payload, computed on the INGESTING node —
     *               once, before the task is serialized, so every writer's log holds
     *               identical bytes for one write
     * @param epoch  the row's store epoch, likewise stamped once here
     */
    public <V> void write(IMap<String, V> map, String mapName, String key, V value,
                          int minDurableCopies, Callable<String> toJson, long epoch) {
        List<Member> writers = StratumRole.writers(hz);
        QuorumGate.require(mapName, writers.size(), effectiveFloor(minDurableCopies));
        // The redo payload is computed only when this build keeps a log for the type.
        // Encoding a payload nothing will append is pure cost on the ingest thread.
        String payload = null;
        if (LOGGED_MAPS.contains(mapName)) {
            try {
                payload = toJson.call();
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "hot record " + mapName + " key=" + key + " cannot be encoded for the redo log", e);
            }
        }
        map.set(key, value);
        // Fire and forget: executeOnMembers does not wait, and both legs the task runs
        // are themselves queues. See RecordPersistTask for the window that opens and
        // what bounds it.
        hz.getExecutorService(PERSIST_EXECUTOR).executeOnMembers(
                new RecordPersistTask(mapName, key,
                        value instanceof Message ? (Message) value : null, payload, epoch),
                writers);
    }
}
