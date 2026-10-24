// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.protobuf.Message;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.splitbrainprotection.SplitBrainProtection;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.StratumEventStores;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.disk.RecordDiskLoader;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.hot.HotRestore;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.backoff.Backoff;

/**
 * StratumMapConfig builds the Hazelcast Config for the stratum hot event store.
 *
 * Data model: one typed IMap per record topic, as the generator emits them.
 *
 * Indexes:
 *   SORTED: timestampMs        — range scans for retention cleanup and time-range queries
 *   HASH:   spiffe_id              — per-engineer lookups
 *   HASH:   sessionId          — per-session lookups
 *   HASH:   agentId            — per-agent lookups
 *   HASH:   eventTypeValue     — event type filtering
 */
public final class StratumMapConfig {

    private static final Logger log = LoggerFactory.getLogger(StratumMapConfig.class);

    /**
     * The split-brain protection attached to the hot map, named so the MapConfig
     * can reference it.
     */
    public static final String SPLIT_BRAIN_PROTECTION_NAME = "stratum-hot-min-durable-copies";

    /**
     * The deployment's durability floor override, read once at build time.
     *
     * A single-instance deployment sets {@code STRATUM_MIN_DURABLE_COPIES=1}
     * DELIBERATELY and this says so in the log. It is a declared number rather than
     * an automatic degrade on cluster size because a production cluster that has
     * lost two members is indistinguishable from a laptop, and an automatic degrade
     * removes the guarantee exactly where it was needed.
     *
     * @return the override, or 0 to honour each record's declared floor
     */
    public static int configuredMinDurableCopies() {
        String raw = System.getenv("STRATUM_MIN_DURABLE_COPIES");
        if (raw == null || raw.isBlank()) {
            return 0;
        }
        int value = Integer.parseInt(raw.trim());
        if (value < 1) {
            throw new IllegalArgumentException(
                    "STRATUM_MIN_DURABLE_COPIES must be at least 1, got " + value);
        }
        return value;
    }


    /** Must match the chart's HasHazelcast containerPort (kernel's convention). */
    public static final int HAZELCAST_PORT = 5001;

    /**
     * The durability floor applied when no record declares one and the deployment
     * sets no override — spec.proto's documented (spec.min_durable_copies) default.
     */
    public static final int DEFAULT_MIN_DURABLE_COPIES = 2;

    /**
     * Synchronous in-memory replicas per record map, beyond the primary.
     *
     * <p>ONE, and independent of {@code min_durable_copies} since the member roles
     * landed. Until then it was {@code min_durable_copies - 1}, because the durable
     * copies WERE the partition replicas: the redo append rode the replication onto
     * the owner and its backups, so the only way to get two logs was to configure
     * two replicas. Writers now append every write cluster-wide regardless of which
     * member owns the partition ({@code HotWrites}), so durability is the writer
     * count and this number is what it always read as and never was — how much
     * MEMORY redundancy a partition carries, i.e. how many members may die at once
     * without the cluster having to reload rows from disk.
     *
     * <p>The two have to be separate for search members to exist at all. A search
     * member holds partitions and no volume, so a partition whose primary and backup
     * both landed on search members holds no durable copy; tying durability to the
     * replica count would make that write undurable, and raising the replica count
     * until it could not happen would put the cost of durability on every member
     * added for query throughput.
     */
    public static final int DEFAULT_BACKUP_COUNT = 1;

    /** Env name for the {@link #DEFAULT_BACKUP_COUNT} override. */
    public static final String BACKUP_COUNT_ENV = "STRATUM_BACKUP_COUNT";

    /**
     * How many synchronous backups each record map carries, from
     * {@code STRATUM_BACKUP_COUNT} or {@link #DEFAULT_BACKUP_COUNT}.
     *
     * <p>Zero is legal and means "no memory redundancy": every partition lives on
     * one member, and a member's death costs the cluster a reload from a writer's
     * disk rather than a row. It is not a durability statement in either direction,
     * which is the whole point of the split.
     */
    public static int configuredBackupCount() {
        String raw = System.getenv(BACKUP_COUNT_ENV);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_BACKUP_COUNT;
        }
        int value = Integer.parseInt(raw.trim());
        if (value < 0 || value > MapConfig.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException(BACKUP_COUNT_ENV + " must be between 0 and "
                    + MapConfig.MAX_BACKUP_COUNT + ", got " + value);
        }
        return value;
    }

    public static final String CLUSTER_NAME = "stratum-events";

    private StratumMapConfig() {}

    /**
     * Builds the Hazelcast Config for the stratum event store.
     *
     * @param seedMembers list of "host:port" addresses for TCP/IP join;
     *                    empty list → multicast (local dev only)
     */
    public static Config build(List<String> seedMembers) {
        return build(seedMembers, configuredMinDurableCopies() > 0
                ? configuredMinDurableCopies()
                : DEFAULT_MIN_DURABLE_COPIES);
    }

    /**
     * Builds the Hazelcast Config with an explicit durability floor.
     *
     * <p>The three settings below used to be silence: {@code setBackupCount} was
     * never called, so the map inherited Hazelcast's default of one synchronous
     * backup, and the "two instances" half of the durability story was
     * ACCIDENTALLY true and nowhere stated. Nothing configured split-brain
     * protection anywhere in the platform, so a minority partition accepted writes
     * locally — the direct enemy of an atomic store. Both are now stated, and
     * logged, the way kernel's HazelcastFactory states its own copy count.
     *
     * @param minDurableCopies distinct nodes that must hold a value before a write
     *                         is acked
     */
    public static Config build(List<String> seedMembers, int minDurableCopies) {
        return build(seedMembers, minDurableCopies, configuredBackupCount(), StratumRole.configured());
    }

    /**
     * Builds the Hazelcast Config with an explicit durability floor, memory
     * redundancy and member role.
     *
     * @param minDurableCopies distinct WRITER members that must hold a value before
     *                         a write is acked
     * @param backupCount      synchronous in-memory replicas per record map, beyond
     *                         the primary — memory redundancy, not durability
     * @param role             what this member is for; see {@link StratumRole}
     */
    public static Config build(List<String> seedMembers, int minDurableCopies,
                               int backupCount, StratumRole role) {
        Config cfg = new Config();
        // Records travel as (proto full name, proto bytes), never as a Java class name.
        cfg.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(Message.class)
                .setImplementation(new ProtoMessageSerializer()));
        cfg.setClusterName(CLUSTER_NAME);
        cfg.setInstanceName("stratum-events-" + UUID.randomUUID().toString().substring(0, 8));

        // The role, published to the cluster. Every durability decision in stratum
        // is counted over the writers in the view — the write gate, the split-brain
        // minimum, the formation wait and the restore barrier — so each member has
        // to be able to read every other member's role, and a member attribute is
        // the one thing that travels with the member itself.
        cfg.getMemberAttributeConfig().setAttribute(StratumRole.ATTRIBUTE, role.name());

        // Network: TCP/IP join from explicit seed members, or multicast for local dev.
        //
        // The port is pinned and auto-increment disabled so a peer's address is
        // predictable: the operator renders seed addresses as
        // "stratum0-<i>.stratum0:5001" from the StatefulSet's stable DNS, and
        // that only resolves to a member if every member binds that exact port.
        // With Hazelcast's default (5701 + auto-increment) the second instance
        // in the JVM silently lands on 5702 and nobody can find it.
        NetworkConfig net = cfg.getNetworkConfig();
        net.setPort(HAZELCAST_PORT);
        net.setPortAutoIncrement(false);
        if (seedMembers == null || seedMembers.isEmpty()) {
            net.getJoin().getMulticastConfig().setEnabled(true);
            net.getJoin().getTcpIpConfig().setEnabled(false);
        } else {
            net.getJoin().getMulticastConfig().setEnabled(false);
            net.getJoin().getTcpIpConfig().setEnabled(true);
            for (String member : seedMembers) {
                net.getJoin().getTcpIpConfig().addMember(member);
            }
        }

        // The durability floor is a property of the STORE, not of one record type, so
        // it is applied to every typed map the generator emits — not to one map
        // alone. It used to be the agentsensor map alone, and the other three maps
        // (scotty.ScottyActivity, accountingrecord.AccountingRecord,
        // agentsensor.AgentSessionState) therefore took Hazelcast's defaults: one
        // synchronous backup, which coincidentally matches a floor of 2 and stops
        // matching the moment the floor moves, and NO split-brain protection at all,
        // so a minority partition accepted their writes locally. The "2 durable
        // copies" claim was true of one map out of four.
        //
        // The protection config is one shared, named object; each map references it.
        //
        // The minimum is counted over WRITERS rather than over members
        // ({@link WriterCountProtection}), because with search members in the view
        // the member count stopped answering the question. One writer and five
        // search members satisfy "six members are here" while holding one durable
        // copy, and a minority partition of that shape would go on accepting writes
        // at the cluster level — the hole the protection exists to close, reopened
        // by the members added for query throughput.
        // The number is set as well as the function. The function is what Hazelcast
        // actually applies; minimumClusterSize would otherwise sit at its own default
        // of 2 and read as the floor to anyone inspecting the config, which is the
        // kind of number that is right by accident until the floor moves.
        SplitBrainProtectionConfig protection = new SplitBrainProtectionConfig(
                SPLIT_BRAIN_PROTECTION_NAME, true, minDurableCopies);
        protection.setFunctionImplementation(new WriterCountProtection(minDurableCopies));
        cfg.addSplitBrainProtectionConfig(protection);
        // One backup count for every record map, and a record's own
        // (spec.min_durable_copies) no longer raises it.
        //
        // It used to: the durable copies were the partition replicas, so a record
        // declaring 3 against a deployment floor of 2 needed a third replica or its
        // every write was refused. Durability is now the writer count — a writer
        // appends every write cluster-wide, whichever member owns the partition — so
        // a record that declares more asks for more WRITERS, not for more replicas of
        // its partitions, and the write gate still enforces its declared number
        // (HotWrites.effectiveFloor) against exactly that population. Raising the
        // replica count for it would buy memory redundancy the record never asked
        // for and no durability at all.
        for (String name : StratumEventStores.mapNames()) {
            int declared = StratumEventStores.minDurableCopies(name);
            if (declared > minDurableCopies) {
                log.info("{} declares min_durable_copies={}, above the deployment floor of {}:"
                        + " its writes need that many WRITER members; backupCount stays {}",
                        name, declared, minDurableCopies, backupCount);
            }
            cfg.addMapConfig(durableMap(name, backupCount, role));
        }
        // The restore ledger is a MAP too, and it was outside the list above — the
        // same omission the record maps had before this loop existed, with the same
        // consequence. It is the map the hot-restore barrier decides on
        // ({@link HotRestore#awaitClusterReplay}), so on Hazelcast's defaults a
        // minority partition files and counts its own replay reports locally and
        // opens hot reads over a store the majority never agreed was recovered. The
        // barrier is protection against an incomplete recovery; it cannot be
        // protection while the ledger it reads is the one thing a partition can
        // still write.
        cfg.addMapConfig(coordinationMap(HotRestore.LEDGER_MAP, role));
        // The in-flight marks the barrier reads alongside the ledger, and they are
        // read in the same breath as it, so they carry the same protection for the
        // same reason: a minority partition that could keep its own copy of "who is
        // still recovering" could talk itself up to the floor on peers the majority
        // never saw finish.
        cfg.addMapConfig(coordinationMap(HotRestore.INFLIGHT_MAP, role));

        log.info("stratum member role={}; hot maps {}: deployment min_durable_copies={} counted over"
                + " WRITER members, backupCount={} (memory redundancy, independent of durability),"
                + " asyncBackupCount=0, split-brain minimum {} writers. The restore ledger {} and the"
                + " in-flight marks {} carry the same protection at backupCount={} — see"
                + " coordinationMap.",
                role, StratumEventStores.mapNames(), minDurableCopies, backupCount,
                minDurableCopies, HotRestore.LEDGER_MAP, HotRestore.INFLIGHT_MAP,
                MapConfig.MAX_BACKUP_COUNT);

        return cfg;
    }

    /**
     * Runs {@code step} — anything that touches a map built by {@link #durableMap} —
     * once the cluster holds the writer quorum, and again whenever the quorum is lost
     * while it runs. The wait is unbounded and on the chassis reconnect schedule
     * ({@link Backoff}: 0.5s doubling to 30s), with one log line per attempt.
     *
     * <p>{@link com.hazelcast.core.Hazelcast#newHazelcastInstance} returns as soon
     * as THIS member is up, not when the cluster is assembled, and the pods of a
     * StatefulSet do not start together — so the first pod up is a cluster of one.
     * Every hot map carries {@link #SPLIT_BRAIN_PROTECTION_NAME} with a minimum of
     * the floor, and Hazelcast enforces that on proxy CREATION, not only on
     * writes: {@code hz.getMap(name)} below the floor throws
     * {@code SplitBrainProtectionException} before a single record is read.
     *
     * <p>What is waited on is the protection's OWN state, not a count of the member
     * list. The two disagree: a fresh member's protection starts out refusing and only
     * takes the view into account on the next membership event or periodic scan,
     * seconds later, so a count that says "enough writers" was followed by a
     * {@code getMap} that threw.
     *
     * <p>And the quorum is not a one-time precondition. During a roll the peers that
     * made it up restart while this member is still inside its own startup — the redo
     * replay and the restore barrier run for minutes against protected maps — and
     * every protected operation refuses from that moment. That is defect #14: a
     * starting writer exited on exactly that refusal. Below the quorum this member
     * cannot serve, but it is a state the cluster recovers from on its own, so it is
     * waited out rather than turned into an exit; the health probe answers
     * NOT_SERVING throughout (see {@code StartupHealthGateTest}). A deployment that
     * really runs one instance declares {@code STRATUM_MIN_DURABLE_COPIES=1}.
     *
     * <p>{@code step} must therefore be safe to re-run from the start. Only the
     * refusal is retried: every other failure is the step's own and is rethrown on the
     * attempt that produced it.
     *
     * <p>WRITERS, because that is the population the protection counts
     * ({@link WriterCountProtection}). A search member waits for the same thing: its
     * maps carry the same protection.
     *
     * @param what the step, for the log line
     * @throws IllegalStateException if the thread is interrupted while waiting
     */
    public static <T> T withWriterQuorum(HazelcastInstance hz, String what, Supplier<T> step,
                                         Backoff.Sleeper sleeper) {
        SplitBrainProtection protection =
                hz.getSplitBrainProtectionService().getSplitBrainProtection(SPLIT_BRAIN_PROTECTION_NAME);
        int minWriters = hz.getConfig().getSplitBrainProtectionConfig(SPLIT_BRAIN_PROTECTION_NAME)
                .getMinimumClusterSize();
        Duration delay = Backoff.RECONNECT_INITIAL_BACKOFF;
        while (true) {
            String refused = "below the floor";
            if (protection.hasMinimumSize()) {
                try {
                    return step.get();
                } catch (SplitBrainProtectionException e) {
                    refused = "refused mid-step: " + e.getMessage();
                }
            }
            log.info("{}: waiting for {} WRITER members ({}; currently {} writer(s) of {} member(s));"
                    + " retrying in {}ms. Search members do not count — scale stratum_nodes, not"
                    + " stratum_search_nodes.",
                    what, minWriters, refused, StratumRole.writerCount(hz),
                    hz.getCluster().getMembers().size(), delay.toMillis());
            try {
                sleeper.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("stratum: interrupted while waiting for the writer quorum", e);
            }
            delay = Backoff.nextReconnectBackoff(delay);
        }
    }

    /**
     * One of the two maps the hot-restore barrier is decided on, at the record maps'
     * protection but replicated to EVERY member instead of to {@code
     * min_durable_copies - 1} of them.
     *
     * <p>The record maps' backup count is a DURABILITY statement: a write is acked
     * once that many members hold it. These two hold no records. What they hold is
     * the coordination state that says which members have recovered — and the event
     * they have to survive is precisely the one the record maps' number is chosen
     * against, SEVERAL MEMBERS DYING AT ONCE. At one backup on three members, each
     * entry lives on two of the three, so losing two members at the same instant
     * destroys outright every entry whose owner and backup were both among them —
     * measured at 112 of 300 keys, a third, in {@code
     * HotRestoreLostReportDeadlockTest}.
     *
     * <p>A destroyed ledger entry does not come back on its own. A member files its
     * report once, at its own boot ({@link HotRestore#LEDGER_MAP}), so a SERVING
     * peer whose entry is destroyed is invisible to {@code
     * HotRestore.awaitClusterReplay} for as long as it keeps serving, and every
     * restart of every other pod then times out against a barrier that can never be
     * satisfied. That is the 2026-09-22 crash-loop; see
     * decisions/2026-09-22-stratum-mutual-restart-deadlock.md. Losing an in-flight
     * mark is the worse direction of the same fault: a member still inside its
     * restore reads as one that has finished.
     *
     * <p>Full replication is affordable here and nowhere else: these maps hold ONE
     * ENTRY PER MEMBER, written twice per boot, against record maps holding the
     * store. {@code MAX_BACKUP_COUNT} is Hazelcast's ceiling of 6 and is a maximum,
     * not a requirement — a cluster of three places two backups — so no cluster size
     * this deployment can reach leaves a coordination entry on fewer members than
     * are alive to hold it.
     */
    private static MapConfig coordinationMap(String name, StratumRole role) {
        MapConfig mapCfg = durableMap(name, MapConfig.MAX_BACKUP_COUNT, role);
        return mapCfg;
    }

    /**
     * One typed record map, at the deployment's memory redundancy and this member's
     * role.
     *
     * <p>Named per map rather than declared once as a wildcard: Hazelcast resolves a
     * map to the most specific config, so a wildcard entry alongside the named
     * per-map entry below would leave that map reading only its own — the exact
     * split that produced this bug. Naming every map keeps one code path.
     *
     * @param backupCount synchronous in-memory replicas beyond the primary. The same
     *                    number on every member: Hazelcast reads a partition's backup
     *                    count from the owner's config, and two members disagreeing
     *                    about it would place different numbers of replicas depending
     *                    on where a key landed.
     * @param role        this member's role. It changes only the DISK legs below, and
     *                    they are the whole of what a search member does not have.
     */
    private static MapConfig durableMap(String name, int backupCount, StratumRole role) {
        MapConfig mapCfg = new MapConfig(name);

        // SYNCHRONOUS backups only. The put must not complete until every replica
        // holds the value; an async backup is not held when the write returns, so a
        // member's death inside that window would cost the cluster a row it believed
        // it had two copies of in memory.
        mapCfg.setBackupCount(backupCount);
        mapCfg.setAsyncBackupCount(0);

        // Below the floor the members are not there to replicate to, so the write
        // is refused at the cluster level as well as at the write gate. A minority
        // partition that keeps accepting writes is how a store stops being atomic.
        mapCfg.setSplitBrainProtectionName(SPLIT_BRAIN_PROTECTION_NAME);

        // The memory clock, per record type, from the record's own
        // (spec.record).residency.idle_seconds. A row that has gone this long with
        // neither a read nor a write is evicted from MEMORY; it stays on disk and
        // the next touch reloads it, so this is cache maintenance and never data
        // loss. Hazelcast runs the clock itself, per entry, and a get() resets it —
        // which is the point: reads keep data warm without turning a read into a
        // write, so nothing here restamps (spec.store_epoch) or appends to the redo
        // log.
        //
        // Every record map gets a number: 0/unset on the annotation means "apply
        // the platform default", never "never", so the generated lookup has already
        // resolved it. A name with no record behind it — the restore ledger — gets
        // 0, Hazelcast's "no max-idle": it holds the replay reports the hot-restore
        // barrier counts, and a barrier whose ledger expires is not a barrier.
        //
        // The clock was hotfixed off on 2026-09-21 because the "stays on disk, next
        // touch reloads it" half did not exist: with no read-through leg, an
        // idle-evicted row was absent from the next DailyCheckpoint dump and its
        // redo entries were truncated by the same transaction, so eviction was
        // permanent loss (decisions/stratum-idle-eviction-durability-gap-2026-09-20.md).
        // The leg now exists, and the two are wired together HERE, in one place and
        // under one condition, so they cannot drift apart again: a map gets the
        // clock exactly when it also gets the loader that can answer for what the
        // clock evicts.
        //
        // On a SEARCH member neither half exists, and that is why the clock does not
        // either. A search member has no volume, so there is no index for the loader
        // to answer from — and the rule above is exactly that a map gets the clock
        // only when it also gets the leg that can answer for what the clock evicts.
        // Giving a search member the clock alone would make eviction a DELETION on
        // every partition it owned: the row would be off-heap with nothing behind it,
        // the loader would answer null, and a Get would report a durable row as
        // absent. So a search member holds its partitions resident for as long as it
        // is a member, which is what "partitions in memory only" means and what its
        // memory request is sized for.
        int idleSeconds = role.isWriter() ? StratumEventStores.idleSeconds(name) : 0;
        if (idleSeconds > 0) {
            mapCfg.setMaxIdleSeconds(idleSeconds);

            // The read-through leg. Hazelcast calls the loader on a get() that
            // misses in memory and on nothing else that matters here — a size() or
            // a values() scan runs over the resident entries only — which is
            // exactly the decision's "what counts as a read" rule, so Count and a
            // predicate that returns no row pull nothing off disk.
            //
            // The loader carries the map NAME rather than an index handle: this
            // config is built before the Hazelcast instance and before the pod's
            // volume is opened, and in tests that open neither. It resolves
            // RecordDisk's node-local registry per call, and answers null when
            // there is none — the pre-disk-leg behaviour, which is safe because a
            // JVM with no leg open is a JVM whose maps also carry no clock in any
            // configuration this method can produce.
            //
            // MapLoader, never MapStore: the write leg rides RecordPersistTask out
            // to every writer member, so each writer's index holds the whole store.
            // Hazelcast would write from the partition owner alone — and the owner
            // may be a search member with no volume to write to.
            MapStoreConfig store = new MapStoreConfig();
            store.setEnabled(true);
            store.setImplementation(new RecordDiskLoader(name));
            store.setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY);
            mapCfg.setMapStoreConfig(store);
        }

        return mapCfg;
    }
}
