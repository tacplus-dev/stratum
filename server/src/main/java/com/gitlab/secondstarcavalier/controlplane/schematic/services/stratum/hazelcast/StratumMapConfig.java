// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast;

import java.util.List;
import java.util.UUID;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.StratumEventStores;

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
        for (String name : StratumEventStores.mapNames()) {
            cfg.addMapConfig(durableMap(name, minDurableCopies));
        }

        log.info("stratum hot maps {}: min_durable_copies={}, backupCount={}, asyncBackupCount=0,"
                + " split-brain minimumClusterSize={}",
                StratumEventStores.mapNames(), minDurableCopies, minDurableCopies - 1, minDurableCopies);

        return cfg;
    }

    /**
     * One typed record map, at the deployment's durability floor.
     *
     * <p>Named per map rather than declared once as a wildcard: Hazelcast resolves a
     * map to the most specific config, so a wildcard entry alongside the named
     * MAP_NAME entry below would leave MAP_NAME reading only its own — the exact
     * split that produced this bug. Naming every map keeps one code path.
     */
    private static MapConfig durableMap(String name, int minDurableCopies) {
        MapConfig mapCfg = new MapConfig(name);

        // SYNCHRONOUS backups only. The put must not complete until every replica
        // that counts towards the floor holds the value; an async backup is not
        // held when the write returns, so counting one would mean acking a
        // durability that does not exist yet.
        mapCfg.setBackupCount(minDurableCopies - 1);
        mapCfg.setAsyncBackupCount(0);

        // Below the floor the members are not there to replicate to, so the write
        // is refused at the cluster level as well as at the write gate. A minority
        // partition that keeps accepting writes is how a store stops being atomic.
        mapCfg.setSplitBrainProtectionName(SPLIT_BRAIN_PROTECTION_NAME);

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
