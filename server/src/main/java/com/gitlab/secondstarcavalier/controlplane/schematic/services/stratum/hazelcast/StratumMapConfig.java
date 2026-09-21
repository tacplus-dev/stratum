// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.HazelcastInstance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.StratumEventStores;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotRestore;

/**
 * StratumMapConfig builds the Hazelcast Config for the stratum hot event store.
 *
 * Data model: IMap<event_id, AgentEvent> named "events" in OBJECT format.
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

    // Must equal the generated StratumEventStores map name for agentsensor.AgentEvent
    // (EventStoreGenerator.mapName = "<package>.<EventType>"). This map is shared:
    // the generated store writes it, the (agent-specific, read-path) query reads it.
    // The OBJECT format + agent indexes below are read-path concerns and remain
    // hand-written until the read side is also generated.
    public static final String MAP_NAME     = "agentsensor.AgentEvent";

    /** Must match the chart's HasHazelcast containerPort (kernel's convention). */
    public static final int HAZELCAST_PORT = 5001;

    /**
     * The durability floor applied when no record declares one and the deployment
     * sets no override — spec.proto's documented (spec.min_durable_copies) default.
     */
    public static final int DEFAULT_MIN_DURABLE_COPIES = 2;

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
        Config cfg = new Config();
        cfg.setClusterName(CLUSTER_NAME);
        cfg.setInstanceName("stratum-events-" + UUID.randomUUID().toString().substring(0, 8));

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
        // it is applied to every typed map the generator emits — not to MAP_NAME
        // alone. It used to be MAP_NAME alone, and the other three maps
        // (scotty.ScottyActivity, accountingrecord.AccountingRecord,
        // agentsensor.AgentSessionState) therefore took Hazelcast's defaults: one
        // synchronous backup, which coincidentally matches a floor of 2 and stops
        // matching the moment the floor moves, and NO split-brain protection at all,
        // so a minority partition accepted their writes locally. The "2 durable
        // copies" claim was true of one map out of four.
        //
        // The protection config is one shared, named object; each map references it.
        SplitBrainProtectionConfig protection = new SplitBrainProtectionConfig(
                SPLIT_BRAIN_PROTECTION_NAME, true, minDurableCopies);
        cfg.addSplitBrainProtectionConfig(protection);
        // The deployment number is a FLOOR, not the answer. Each record declares its
        // own (spec.min_durable_copies), the write gate enforces that declared value
        // per record (HotWrites.effectiveFloor), and until now the map's backupCount
        // was derived from the deployment number alone — so a record declaring 3
        // against a deployment default of 2 got one backup, could reach only 2
        // durable copies, and had EVERY write rejected permanently on a healthy
        // cluster. max() keeps the deployment number as the floor it is and lets a
        // record that asks for more get more.
        for (String name : StratumEventStores.mapNames()) {
            int floor = Math.max(minDurableCopies, StratumEventStores.minDurableCopies(name));
            if (floor > minDurableCopies) {
                log.info("{} declares min_durable_copies={}, above the deployment floor of {}:"
                        + " backupCount={}", name, floor, minDurableCopies, floor - 1);
            }
            cfg.addMapConfig(durableMap(name, floor));
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
        cfg.addMapConfig(durableMap(HotRestore.LEDGER_MAP, minDurableCopies));

        log.info("stratum hot maps {} + the restore ledger {}: deployment min_durable_copies={},"
                + " backupCount={} unless a record declares more (logged above),"
                + " asyncBackupCount=0, split-brain minimumClusterSize={}",
                StratumEventStores.mapNames(), HotRestore.LEDGER_MAP, minDurableCopies,
                minDurableCopies - 1, minDurableCopies);

        return cfg;
    }

    /**
     * How long a starting member waits for the cluster to reach the declared floor
     * before giving up, so a cluster that never forms fails loudly rather than
     * looking like a hang.
     *
     * <p>It is NOT bounded inside a probe window on its own, and must not be read
     * as if it were: {@link com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.EventStore#RESTORE_TIMEOUT}
     * runs after it on the same thread, so a worst-case cold start spends 2 + 5
     * minutes inside {@code EventStore.init} before the handler returns. The two
     * cannot be overlapped — the redo barrier needs the cluster the first wait is
     * waiting for, both to open a protected map and to have enough live members to
     * file reports — so the combined wait is what a start has to survive. What lets
     * it survive is that nothing about serving depends on this thread finishing
     * promptly: the generated chassis binds its plaintext grpc.health.v1 listener
     * in {@code ServiceManager.create()} and holds it at NOT_SERVING until
     * bootstrap completes, which is after config delivery has run this wait, so a
     * probe gets a connected "not ready" answer throughout rather than a refused
     * connection. See {@code StartupHealthGateTest}.
     */
    public static final Duration FORMATION_TIMEOUT = Duration.ofMinutes(2);

    /**
     * Blocks until the cluster holds {@code minDurableCopies} members, which is the
     * precondition for touching any map built by {@link #durableMap}.
     *
     * <p>{@link com.hazelcast.core.Hazelcast#newHazelcastInstance} returns as soon
     * as THIS member is up, not when the cluster is assembled, and the pods of a
     * StatefulSet do not start together — so the first pod up is a cluster of one.
     * Every hot map carries {@link #SPLIT_BRAIN_PROTECTION_NAME} with a
     * minimumClusterSize of the same floor, and Hazelcast enforces that on proxy
     * CREATION, not only on writes: {@code hz.getMap(name)} from a cluster of one
     * throws {@code SplitBrainProtectionException} before a single record is read.
     * The floor is a statement about the assembled cluster, so the assembly is
     * waited for rather than the exception being caught — catching it would only
     * move the same incomplete-cluster start one line later.
     *
     * <p>Waiting is bounded and then fatal on purpose. With the floor unmet this
     * member genuinely cannot serve: it may not take a write, and the redo barrier
     * it would go on to run ({@code HotRestore.awaitClusterReplay}) cannot be
     * satisfied by fewer members either. A deployment that really runs one instance
     * declares {@code STRATUM_MIN_DURABLE_COPIES=1} and never waits here.
     *
     * @throws IllegalStateException if the floor is not reached within {@code timeout}
     */
    public static void awaitProtectedClusterSize(HazelcastInstance hz, int minDurableCopies,
                                                 Duration timeout) {
        int size = hz.getCluster().getMembers().size();
        if (size >= minDurableCopies) {
            return;
        }
        log.info("waiting for the cluster to reach min_durable_copies={} before opening the hot maps"
                + " (currently {} member(s)); every hot map's split-brain protection refuses a"
                + " proxy below the floor", minDurableCopies, size);

        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        while (true) {
            size = hz.getCluster().getMembers().size();
            if (size >= minDurableCopies) {
                log.info("cluster reached {} member(s) — opening the hot maps", size);
                return;
            }
            if (System.currentTimeMillis() >= deadlineMs) {
                throw new IllegalStateException("stratum: cluster reached only " + size + " of the "
                        + minDurableCopies + " members min_durable_copies declares, within " + timeout
                        + "; every hot map refuses to open below that floor, so this member cannot"
                        + " serve. Set STRATUM_MIN_DURABLE_COPIES=1 for a deliberately"
                        + " single-instance deployment.");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("stratum: interrupted while awaiting cluster formation", e);
            }
        }
    }

    /**
     * One typed record map, at its effective durability floor — the deployment floor
     * raised, for a record map, by anything the record itself declares.
     *
     * <p>Named per map rather than declared once as a wildcard: Hazelcast resolves a
     * map to the most specific config, so a wildcard entry alongside the named
     * MAP_NAME entry below would leave MAP_NAME reading only its own — the exact
     * split that produced this bug. Naming every map keeps one code path.
     */
    private static MapConfig durableMap(String name, int effectiveMinDurableCopies) {
        MapConfig mapCfg = new MapConfig(name);

        // SYNCHRONOUS backups only. The put must not complete until every replica
        // that counts towards the floor holds the value; an async backup is not
        // held when the write returns, so counting one would mean acking a
        // durability that does not exist yet.
        mapCfg.setBackupCount(effectiveMinDurableCopies - 1);
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
        mapCfg.setMaxIdleSeconds(StratumEventStores.idleSeconds(name));

        if (!MAP_NAME.equals(name)) {
            return mapCfg;
        }

        // events map: OBJECT in-memory format so Hazelcast reflects directly on AgentEvent
        // getters for predicate evaluation — no serialization overhead for in-JVM queries.
        // Read-path concerns, and still hand-written because the read side is not
        // generated; they attach to this one map only.
        mapCfg.setInMemoryFormat(InMemoryFormat.OBJECT);

        // Attribute names are JavaBean PROPERTY names, not proto field names.
        // Hazelcast resolves an index attribute by reflecting for a getter, so
        // "spiffe_id" looks for getSpiffe_id() on the protobuf class, which does
        // not exist — protoc emits getSpiffeId(). That single snake_case name
        // among three camelCase siblings made EVERY put on this map throw
        //
        //   IllegalArgumentException: There is no suitable accessor for 'spiffe_id'
        //
        // so no AgentEvent was ever stored, the subscriber retried the same
        // failure every second forever, and the record query returned an empty
        // set that looked like "nothing has been published yet".
        mapCfg.addIndexConfig(new IndexConfig(IndexType.SORTED, "timestampMs"));
        mapCfg.addIndexConfig(new IndexConfig(IndexType.HASH,   "spiffeId"));
        mapCfg.addIndexConfig(new IndexConfig(IndexType.HASH,   "sessionId"));
        mapCfg.addIndexConfig(new IndexConfig(IndexType.HASH,   "agentId"));
        mapCfg.addIndexConfig(new IndexConfig(IndexType.HASH,
                StratumEventTypeExtractor.ATTRIBUTE_NAME));
        return mapCfg;
    }
}
