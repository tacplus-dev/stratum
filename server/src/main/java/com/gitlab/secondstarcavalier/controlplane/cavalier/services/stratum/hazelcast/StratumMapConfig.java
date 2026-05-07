// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.hazelcast;

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
 * Data model: IMap<event_id, SensorEvent> named "events" in OBJECT format.
 *
 * Indexes:
 *   SORTED: timestampMs        — range scans for retention cleanup and time-range queries
 *   HASH:   email              — per-engineer lookups
 *   HASH:   sessionId          — per-session lookups
 *   HASH:   agentId            — per-agent lookups
 *   HASH:   eventTypeValue     — event type filtering
 */
public final class StratumMapConfig {

    public static final String MAP_NAME     = "events";
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

        // events map: OBJECT in-memory format so Hazelcast reflects directly on SensorEvent
        // getters for predicate evaluation — no serialization overhead for in-JVM queries.
        MapConfig mapCfg = new MapConfig(MAP_NAME);
        mapCfg.setInMemoryFormat(InMemoryFormat.OBJECT);

        mapCfg.addIndexConfig(new IndexConfig(IndexType.SORTED, "timestampMs"));
        mapCfg.addIndexConfig(new IndexConfig(IndexType.HASH,   "email"));
        mapCfg.addIndexConfig(new IndexConfig(IndexType.HASH,   "sessionId"));
        mapCfg.addIndexConfig(new IndexConfig(IndexType.HASH,   "agentId"));
        mapCfg.addIndexConfig(new IndexConfig(IndexType.HASH,
                StratumEventTypeExtractor.ATTRIBUTE_NAME));

        cfg.addMapConfig(mapCfg);
        return cfg;
    }
}
