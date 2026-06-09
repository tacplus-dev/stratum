// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.services.stratum.Stratum;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.MapDetail;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.MapEntry;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.claudesensor.Claudesensor.SensorEvent;

/**
 * WorkingSetStore manages the Hazelcast namespaces for stratum's working sets:
 *
 *   ws:<map_id>   — working-set maps: IMap<event_id, byte[] SensorEvent>
 *                   In-memory only, idle TTL eviction.
 *
 * Map IDs are chosen by the caller and must be unique.
 * Thread safety: HazelcastInstance is thread-safe. mapMeta is a ConcurrentHashMap.
 */
public class WorkingSetStore {

    private static final Logger log = LoggerFactory.getLogger(WorkingSetStore.class);

    static final String WS_PREFIX      = "ws:";

    private final Stratum.Config cfg;
    private HazelcastInstance hz;

    // Tracks creation time for working-set maps (not stored in Hazelcast).
    private final ConcurrentHashMap<String, Long> mapMeta = new ConcurrentHashMap<>();

    public WorkingSetStore(Stratum.Config cfg) {
        this.cfg = cfg;
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    public void init() {
        Config hzCfg = buildHazelcastConfig();
        hz = Hazelcast.newHazelcastInstance(hzCfg);
        log.info("WorkingSetStore started: memory_mb={}, map_idle_ttl_seconds={}",
                cfg.getHazelcastMemoryMb(), cfg.getMapIdleTtlSeconds());
    }

    private Config buildHazelcastConfig() {
        Config cfg = new Config();
        cfg.setClusterName("stratum-ws");
        cfg.setInstanceName("stratum-ws-" + java.util.UUID.randomUUID().toString().substring(0, 8));

        // Standalone — no clustering between nodes for working sets.
        NetworkConfig net = cfg.getNetworkConfig();
        net.getJoin().getMulticastConfig().setEnabled(false);
        net.getJoin().getTcpIpConfig().setEnabled(false);

        // ws:* — working-set maps: in-memory, idle TTL eviction.
        MapConfig wsCfg = new MapConfig(WS_PREFIX + "*");
        wsCfg.setMaxIdleSeconds(this.cfg.getMapIdleTtlSeconds());
        cfg.addMapConfig(wsCfg);

        return cfg;
    }

    public void shutdown() {
        if (hz != null) {
            log.info("WorkingSetStore shutting down");
            hz.shutdown();
        }
    }

    // -----------------------------------------------------------------------
    // Working-set map management
    // -----------------------------------------------------------------------

    /**
     * Creates a new working-set map with a caller-chosen map_id.
     * Enforces max_maps limit. Errors if the map_id is already in use.
     */
    public void createMap(String mapId) {
        if (mapMeta.containsKey(mapId)) {
            throw new IllegalArgumentException("map already exists: " + mapId +
                    " — delete it first or reuse it by loading additional events");
        }
        if (mapMeta.size() >= cfg.getMaxMaps()) {
            throw new IllegalStateException(
                    "max_maps limit reached (" + cfg.getMaxMaps() +
                    "); delete idle maps before creating new ones");
        }
        mapMeta.put(mapId, System.currentTimeMillis());
        hz.getMap(WS_PREFIX + mapId); // touch to register in Hazelcast
        log.info("created working-set map: {}", mapId);
    }

    /**
     * Deletes a working-set map and frees its Hazelcast memory.
     */
    public void deleteMap(String mapId) {
        hz.getMap(WS_PREFIX + mapId).destroy();
        mapMeta.remove(mapId);
        log.info("deleted working-set map: {}", mapId);
    }

    /**
     * Returns summary metadata for all active working-set maps.
     */
    public List<MapEntry> listMaps() {
        List<MapEntry> result = new ArrayList<>();
        for (Map.Entry<String, Long> e : mapMeta.entrySet()) {
            String mapId = e.getKey();
            IMap<String, byte[]> imap = hz.getMap(WS_PREFIX + mapId);
            long memBytes = imap.getLocalMapStats().getOwnedEntryMemoryCost();
            result.add(MapEntry.newBuilder()
                    .setMapId(mapId)
                    .setEventCount(imap.size())
                    .setSizeMb(memBytes / (1024 * 1024))
                    .setCreatedMs(e.getValue())
                    .build());
        }
        return result;
    }

    /**
     * Returns detailed breakdown of a working-set map.
     */
    public MapDetail describeMap(String mapId) {
        if (!mapMeta.containsKey(mapId)) {
            throw new IllegalArgumentException("unknown map_id: " + mapId);
        }
        IMap<String, byte[]> imap = hz.getMap(WS_PREFIX + mapId);
        long memBytes = imap.getLocalMapStats().getOwnedEntryMemoryCost();

        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;
        Map<String, Long> typeBreakdown = new HashMap<>();
        Map<String, Long> emailCount   = new HashMap<>();
        Map<String, Long> sessionCount = new HashMap<>();
        Map<String, Long> agentCount   = new HashMap<>();

        for (byte[] raw : imap.values()) {
            try {
                SensorEvent ev = SensorEvent.parseFrom(raw);
                if (ev.getTimestampMs() < minTs) minTs = ev.getTimestampMs();
                if (ev.getTimestampMs() > maxTs) maxTs = ev.getTimestampMs();
                typeBreakdown.merge(ev.getEventType().name(), 1L, Long::sum);
                if (!ev.getEmail().isEmpty())     emailCount.merge(ev.getEmail(), 1L, Long::sum);
                if (!ev.getSessionId().isEmpty()) sessionCount.merge(ev.getSessionId(), 1L, Long::sum);
                if (!ev.getAgentId().isEmpty())   agentCount.merge(ev.getAgentId(), 1L, Long::sum);
            } catch (Exception ignored) {}
        }

        long size = imap.size();
        MapDetail.Builder b = MapDetail.newBuilder()
                .setEventCount(size)
                .setSizeMb(memBytes / (1024 * 1024))
                .setTimeRangeStartMs(size > 0 ? minTs : 0)
                .setTimeRangeEndMs(size > 0 ? maxTs : 0);
        typeBreakdown.forEach(b::putEventTypeBreakdown);
        topN(emailCount,   5).forEach(b::addTopEmails);
        topN(sessionCount, 5).forEach(b::addTopSessions);
        topN(agentCount,   5).forEach(b::addTopAgents);
        return b.build();
    }

    /** Returns event_count and size_mb for a map without full iteration. */
    public long[] mapSize(String mapId) {
        if (!mapMeta.containsKey(mapId)) {
            throw new IllegalArgumentException("unknown map_id: " + mapId);
        }
        IMap<String, byte[]> imap = hz.getMap(WS_PREFIX + mapId);
        long count = imap.size();
        long mb    = imap.getLocalMapStats().getOwnedEntryMemoryCost() / (1024 * 1024);
        return new long[]{count, mb};
    }

    /**
     * Puts a SensorEvent into a working-set map (keyed by event_id, deduplicating).
     * The map must already exist (created via createMap).
     */
    public void putEvent(String mapId, SensorEvent event) {
        hz.getMap(WS_PREFIX + mapId).put(event.getEventId(), event.toByteArray());
    }

    /**
     * Merges all events from srcMapId into destMapId. srcMap is unchanged.
     * Returns the number of events added (new, not overwritten).
     */
    public long merge(String srcMapId, String destMapId) {
        if (!mapMeta.containsKey(srcMapId)) throw new IllegalArgumentException("unknown src_map_id: " + srcMapId);
        if (!mapMeta.containsKey(destMapId)) throw new IllegalArgumentException("unknown dest_map_id: " + destMapId);

        IMap<String, byte[]> src  = hz.getMap(WS_PREFIX + srcMapId);
        IMap<String, byte[]> dest = hz.getMap(WS_PREFIX + destMapId);

        long added = 0;
        for (Map.Entry<String, byte[]> e : src.entrySet()) {
            if (dest.putIfAbsent(e.getKey(), e.getValue()) == null) {
                added++;
            }
        }
        log.info("merged {} → {}: {} events added", srcMapId, destMapId, added);
        return added;
    }

    /**
     * Filters a working-set map in-place using a Calcite SQL SELECT.
     * Events NOT returned by the SQL are removed from the map.
     * Returns the number of events removed.
     */
    public long filterMap(String mapId, String sql) throws Exception {
        if (!mapMeta.containsKey(mapId)) throw new IllegalArgumentException("unknown map_id: " + mapId);
        IMap<String, byte[]> imap = hz.getMap(WS_PREFIX + mapId);

        Set<String> keepIds = CalciteEngine.getMatchingIds(imap, sql);
        Set<String> allIds  = new HashSet<>(imap.keySet());

        long removed = 0;
        for (String id : allIds) {
            if (!keepIds.contains(id)) {
                imap.remove(id);
                removed++;
            }
        }
        log.info("filter_map {}: removed {} events", mapId, removed);
        return removed;
    }

    /**
     * Executes Calcite SQL over a working-set map.
     * Returns Object[]{Long totalRows, String resultsJson}.
     */
    public Object[] queryMap(String mapId, String sql, int limit) throws Exception {
        if (!mapMeta.containsKey(mapId)) throw new IllegalArgumentException("unknown map_id: " + mapId);
        IMap<String, byte[]> imap = hz.getMap(WS_PREFIX + mapId);
        return CalciteEngine.queryToJson(imap, sql, limit);
    }

    /**
     * Returns the raw IMap for direct access (used by CalciteEngine).
     */
    public IMap<String, byte[]> getWorkingSetMap(String mapId) {
        if (!mapMeta.containsKey(mapId)) {
            throw new IllegalArgumentException("unknown map_id: " + mapId);
        }
        return hz.getMap(WS_PREFIX + mapId);
    }

    // -----------------------------------------------------------------------
    // Memory stats
    // -----------------------------------------------------------------------

    public long totalEventsLoaded() {
        long total = 0;
        for (String mapId : mapMeta.keySet()) {
            total += hz.getMap(WS_PREFIX + mapId).size();
        }
        return total;
    }

    public long hazelcastMemoryUsedBytes() {
        long used = 0;
        for (String mapId : mapMeta.keySet()) {
            used += hz.getMap(WS_PREFIX + mapId).getLocalMapStats().getOwnedEntryMemoryCost();
        }
        return used;
    }

    public long hazelcastMemoryTotalBytes() {
        return (long) cfg.getHazelcastMemoryMb() * 1024 * 1024;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static List<String> topN(Map<String, Long> counts, int n) {
        return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue(Comparator.reverseOrder()))
                .limit(n)
                .map(Map.Entry::getKey)
                .toList();
    }
}
