// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.services.stratum.Stratum;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentEvent;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.MapEntry;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.MapDetail;

/**
 * WorkingSetStore manages the Hazelcast namespaces for stratum's working sets:
 *
 *   ws:<map_id>   — working-set maps: IMap<event_id, byte[] AgentEvent>
 *                   In-memory only, idle TTL eviction.
 *
 * Map IDs are chosen by the caller and must be unique.
 * Thread safety: HazelcastInstance is thread-safe; mapMeta is a Hazelcast IMap.
 */
public class WorkingSetStore {

    private static final Logger log = LoggerFactory.getLogger(WorkingSetStore.class);

    static final String WS_PREFIX      = "ws:";

    /** Cluster name for the working-set maps — distinct from the event store's. */
    static final String CLUSTER_NAME   = "stratum-ws";

    /**
     * Working-set Hazelcast port. Must match the chart's hazelcast-ws
     * containerPort (operator: stratumWorkingSetPort) and differ from the event
     * store's 5001 — two clusters in one JVM cannot share a port.
     */
    static final int WS_PORT           = 5701;

    /** Cluster-wide registry of working-set maps; deliberately NOT under ws:*. */
    static final String META_MAP       = "stratum-ws-meta";

    private final Stratum.Config cfg;
    private HazelcastInstance hz;

    /**
     * map_id -> creation time. CLUSTER-WIDE, not a local ConcurrentHashMap.
     *
     * This is the registry every other operation gates on ("unknown map_id"),
     * so clustering the working-set data alone would have fixed nothing: a
     * createMap served by stratum-0 would record the id in stratum-0's heap,
     * and the very next request — the Service picks a pod per connection — would
     * be answered by stratum-1, which would reject a map whose entries it was
     * itself holding a share of.
     *
     * Named outside the WS_PREFIX namespace on purpose: the ws:* MapConfig sets
     * maxIdleSeconds, and an idle-evicted registry would delete the record of
     * maps that still hold data.
     */
    private IMap<String, Long> mapMeta;

    public WorkingSetStore(Stratum.Config cfg) {
        this.cfg = cfg;
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    public void init() {
        Config hzCfg = buildHazelcastConfig(seedMembers());
        hz = Hazelcast.newHazelcastInstance(hzCfg);
        mapMeta = hz.getMap(META_MAP);
        log.info("WorkingSetStore started: memory_mb={}, map_idle_ttl_seconds={}",
                cfg.getHazelcastMemoryMb(), cfg.getMapIdleTtlSeconds());
    }

    /**
     * Peer addresses for the working-set cluster, from STRATUM_WS_SEED_MEMBERS.
     *
     * Rendered by the operator from the StatefulSet's stable per-pod DNS, on
     * WS_PORT — a SEPARATE list from the event store's STRATUM_SEED_MEMBERS,
     * because the two clusters live on different ports in the same JVM.
     */
    static List<String> seedMembers() {
        String raw = System.getenv("STRATUM_WS_SEED_MEMBERS");
        if (raw == null || raw.isBlank()) {
            log.info("STRATUM_WS_SEED_MEMBERS unset — working sets are node-local (single-node/dev)");
            return List.of();
        }
        List<String> members = Arrays.stream(raw.split(","))
                .map(String::trim).filter(m -> !m.isEmpty()).toList();
        log.info("working-set cluster seed members: {}", members);
        return members;
    }

    /**
     * Builds the working-set Hazelcast config.
     *
     * Working sets used to be deliberately standalone — "no clustering between
     * nodes" — which was fine while stratum ran one replica and wrong the moment
     * it ran three: a map created on one pod was invisible from the other two,
     * and the shared Service hands the next request to whichever pod it likes,
     * so createMap on stratum-0 followed by a read on stratum-1 is a NOT_FOUND
     * for a map that plainly exists. Nothing had shipped against it yet, which
     * is the only reason this was not an outage.
     *
     * Package-private and taking the seed list as a parameter so the join
     * configuration can be asserted without standing up a cluster.
     *
     * @param seedMembers "host:port" addresses; empty → node-local (dev)
     */
    static Config buildHazelcastConfig(List<String> seedMembers, int idleTtlSeconds) {
        Config cfg = new Config();
        cfg.setClusterName(CLUSTER_NAME);
        cfg.setInstanceName(CLUSTER_NAME + "-" + java.util.UUID.randomUUID().toString().substring(0, 8));

        // The port is pinned with auto-increment OFF, exactly as the event store
        // pins 5001: a seed address only resolves to a member if every member
        // binds that precise port. It must also differ from the event store's —
        // two clusters in one JVM cannot share one — and relying on Hazelcast's
        // auto-increment to separate them is what silently put this instance on
        // a port no seed list could name.
        NetworkConfig net = cfg.getNetworkConfig();
        net.setPort(WS_PORT);
        net.setPortAutoIncrement(false);
        if (seedMembers == null || seedMembers.isEmpty()) {
            net.getJoin().getMulticastConfig().setEnabled(false);
            net.getJoin().getTcpIpConfig().setEnabled(false);
        } else {
            net.getJoin().getMulticastConfig().setEnabled(false);
            net.getJoin().getTcpIpConfig().setEnabled(true);
            for (String member : seedMembers) {
                net.getJoin().getTcpIpConfig().addMember(member);
            }
        }

        // ws:* — working-set maps: in-memory, idle TTL eviction.
        MapConfig wsCfg = new MapConfig(WS_PREFIX + "*");
        wsCfg.setMaxIdleSeconds(idleTtlSeconds);
        cfg.addMapConfig(wsCfg);

        return cfg;
    }

    private Config buildHazelcastConfig(List<String> seedMembers) {
        return buildHazelcastConfig(seedMembers, this.cfg.getMapIdleTtlSeconds());
    }

    public void shutdown() {
        if (hz != null) {
            log.info("WorkingSetStore shutting down");
            hz.shutdown();
        }
    }

    /**
     * Approximate memory a working-set map occupies ACROSS the cluster.
     *
     * getLocalMapStats reports only the entries this member owns. That was the
     * whole map while working sets were node-local; now that they cluster, it is
     * roughly 1/members of it — and it was being reported alongside imap.size(),
     * which is cluster-wide. The pair was inconsistent: a caller would read 300
     * events occupying a third of their actual bytes, and the discrepancy grows
     * with every member added.
     *
     * Scaling this member's owned cost by the cluster-wide entry count keeps the
     * two halves of that pair talking about the same set of entries. It is an
     * estimate — Hazelcast exposes owned-entry cost per member only, and summing
     * it exactly would mean an executor round-trip to every member on what is a
     * plain reporting path — and it is a good one, because partitions are
     * balanced and working-set entries are same-shaped serialized AgentEvents.
     */
    private static long clusterWideBytes(IMap<String, byte[]> imap) {
        var stats = imap.getLocalMapStats();
        long ownedEntries = stats.getOwnedEntryCount();
        if (ownedEntries <= 0) {
            return 0;
        }
        long bytesPerEntry = stats.getOwnedEntryMemoryCost() / ownedEntries;
        return bytesPerEntry * imap.size();
    }

    // -----------------------------------------------------------------------
    // Working-set map management
    // -----------------------------------------------------------------------

    /**
     * Creates a new working-set map with a caller-chosen map_id.
     * Enforces max_maps limit. Errors if the map_id is already in use.
     */
    public void createMap(String mapId) {
        if (mapMeta.size() >= cfg.getMaxMaps()) {
            throw new IllegalStateException(
                    "max_maps limit reached (" + cfg.getMaxMaps() +
                    "); delete idle maps before creating new ones");
        }
        // putIfAbsent, not containsKey-then-put: with several members serving
        // createMap concurrently, a check followed by a write is two operations
        // and both callers can pass the check. The limit above stays a soft one
        // — enforcing it exactly would need a lock across members for a bound
        // that exists to stop runaway growth, not to be precise at the boundary.
        if (mapMeta.putIfAbsent(mapId, System.currentTimeMillis()) != null) {
            throw new IllegalArgumentException("map already exists: " + mapId +
                    " — delete it first or reuse it by loading additional events");
        }
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
            long memBytes = clusterWideBytes(imap);
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
        long memBytes = clusterWideBytes(imap);

        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;
        Map<String, Long> typeBreakdown = new HashMap<>();
        Map<String, Long> spiffeIdCount = new HashMap<>();
        Map<String, Long> sessionCount = new HashMap<>();
        Map<String, Long> agentCount = new HashMap<>();

        for (byte[] raw : imap.values()) {
            try {
                AgentEvent ev = AgentEvent.parseFrom(raw);
                if (ev.getTimestampMs() < minTs) minTs = ev.getTimestampMs();
                if (ev.getTimestampMs() > maxTs) maxTs = ev.getTimestampMs();
                typeBreakdown.merge(ev.getEventType().name(), 1L, Long::sum);
                if (!ev.getSpiffeId().isEmpty()) spiffeIdCount.merge(ev.getSpiffeId(), 1L, Long::sum);
                if (!ev.getSessionId().isEmpty()) sessionCount.merge(ev.getSessionId(), 1L, Long::sum);
                if (!ev.getAgentId().isEmpty()) agentCount.merge(ev.getAgentId(), 1L, Long::sum);
            } catch (Exception ignored) {}
        }

        long size = imap.size();
        MapDetail.Builder b = MapDetail.newBuilder()
                .setEventCount(size)
                .setSizeMb(memBytes / (1024 * 1024))
                .setTimeRangeStartMs(size > 0 ? minTs : 0)
                .setTimeRangeEndMs(size > 0 ? maxTs : 0);
        typeBreakdown.forEach(b::putEventTypeBreakdown);
        topN(spiffeIdCount, 5).forEach(b::addTopSpiffeIds);
        topN(sessionCount, 5).forEach(b::addTopSessions);
        topN(agentCount, 5).forEach(b::addTopAgents);
        return b.build();
    }

    /** Returns event_count and size_mb for a map without full iteration. */
    public long[] mapSize(String mapId) {
        if (!mapMeta.containsKey(mapId)) {
            throw new IllegalArgumentException("unknown map_id: " + mapId);
        }
        IMap<String, byte[]> imap = hz.getMap(WS_PREFIX + mapId);
        long count = imap.size();
        long mb    = clusterWideBytes(imap) / (1024 * 1024);
        return new long[]{count, mb};
    }

    /**
     * Puts a AgentEvent into a working-set map (keyed by event_id, deduplicating).
     * The map must already exist (created via createMap).
     */
    public void putEvent(String mapId, AgentEvent event) {
        hz.getMap(WS_PREFIX + mapId).put(event.getEventId(), event.toByteArray());
    }

    /**
     * Vservice-neutral working-set insert: stores the raw record bytes under the
     * given key. Used by the generated RecordMap plane (StratumRecordMaps), which
     * holds any record type, keyed by its record key. The map must already exist.
     */
    public void putEvent(String mapId, String key, byte[] recordBytes) {
        hz.getMap(WS_PREFIX + mapId).put(key, recordBytes);
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

    /**
     * Memory this MEMBER holds for working sets.
     *
     * Deliberately local, unlike the reported per-map footprints: it is checked
     * against hazelcast_memory_mb, a budget for this JVM's heap. Clustering
     * therefore raises effective capacity to members x budget, which is the
     * point of adding members.
     */
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

    private static List<String> topN(Map<String, Long> counts, int n) {
        return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue(Comparator.reverseOrder()))
                .limit(n)
                .map(Map.Entry::getKey)
                .toList();
    }
}
