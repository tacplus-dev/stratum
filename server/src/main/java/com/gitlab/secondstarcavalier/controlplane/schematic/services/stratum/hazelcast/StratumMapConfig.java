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

    // Must equal the generated StratumEventStores map name for agentsensor.AgentEvent
    // (EventStoreGenerator.mapName = "<package>.<EventType>"). This map is shared:
    // the generated store writes it, the (agent-specific, read-path) query reads it.
    // The OBJECT format + agent indexes below are read-path concerns and remain
    // hand-written until the StratumQuery read side is also generated.
    public static final String MAP_NAME     = "agentsensor.AgentEvent";
    public static final String CLUSTER_NAME = "stratum-events";

    private StratumMapConfig() {}

    /**
     * Builds the Hazelcast Config for the stratum event store.
     *
     * @param seedMembers list of "host:port" addresses for TCP/IP join;
     *                    empty list → multicast (local dev only)
     */
    public static Config build(List<String> seedMembers) {
        Config cfg = new Config();
        cfg.setClusterName(CLUSTER_NAME);
        cfg.setInstanceName("stratum-events-" + UUID.randomUUID().toString().substring(0, 8));

        // Network: TCP/IP join from explicit seed members, or multicast for local dev.
        NetworkConfig net = cfg.getNetworkConfig();
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

        // events map: OBJECT in-memory format so Hazelcast reflects directly on AgentEvent
        // getters for predicate evaluation — no serialization overhead for in-JVM queries.
        MapConfig mapCfg = new MapConfig(MAP_NAME);
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

        cfg.addMapConfig(mapCfg);
        return cfg;
    }
}
