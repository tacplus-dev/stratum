// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentEvent;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorEventType;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast.StratumMapConfig;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast.StratumEpochExtractor;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast.StratumEventTypeExtractor;

/**
 * EventStore manages the Hazelcast IMap that backs stratum's hot event store.
 *
 * Data model: IMap<event_id, AgentEvent> named "events".
 *
 * Indexes (via ValueExtractor or Hazelcast native attribute reflection):
 *   - SORTED:  epoch (StratumEpochExtractor), timestampMs
 *   - HASH:    spiffeId, sessionId, agentId, eventTypeValue (StratumEventTypeExtractor)
 *
 * Retention:
 *   Runs on cleanup_interval_seconds. Deletes entries where
 *   timestampMs < (now - retentionDays * 86400000). Runs only on the
 *   oldest cluster member to avoid duplicate work across nodes.
 *
 * Thread safety: HazelcastInstance is thread-safe. AtomicLong stats are thread-safe.
 */
public class EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStore.class);

    private HazelcastInstance hz;
    // Generated, vservice-neutral write path: one typed IMap per EventBus topic.
    private final StratumEventStores stores = new StratumEventStores();
    // Agent-specific read path: reads the same Hazelcast map the generated
    // store writes for agentsensor.AgentEvent.
    private IMap<String, AgentEvent> events;
    private int retentionDays;

    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor(
            r -> { Thread t = new Thread(r, "stratum-event-cleaner"); t.setDaemon(true); return t; });

    private final AtomicLong lastCleanupMs      = new AtomicLong(0);
    private final AtomicLong lastCleanupRemoved = new AtomicLong(0);

    // -----------------------------------------------------------------------
    // Init
    // -----------------------------------------------------------------------

    /**
     * Initialises the Hazelcast member and the "events" IMap.
     *
     * @param retentionDays       days to retain events
     * @param cleanupIntervalSecs how often the retention job runs
     * @param seedMembers         list of "host:port" addresses for TCP/IP join;
     *                            empty → multicast (dev only)
     */
    public void init(int retentionDays, long cleanupIntervalSecs, List<String> seedMembers) {
        this.retentionDays = retentionDays;

        hz = Hazelcast.newHazelcastInstance(StratumMapConfig.build(seedMembers));
        stores.init(hz);
        events = hz.getMap(StratumMapConfig.MAP_NAME);

        cleaner.scheduleWithFixedDelay(
                this::runRetentionCleanupIfLeader,
                cleanupIntervalSecs, cleanupIntervalSecs, TimeUnit.SECONDS);

        log.info("EventStore initialised: retention_days={}, seed_members={}", retentionDays,
                seedMembers.isEmpty() ? "multicast" : seedMembers);
    }

    // -----------------------------------------------------------------------
    // Ingest
    // -----------------------------------------------------------------------

    /**
     * Stores any subscribed event into its generated typed store, keyed by
     * event_id. Vservice-neutral: the typed dispatch lives in the generated
     * StratumEventStores, so this code names no vservice.
     */
    public boolean store(Message event) {
        return stores.store(event);
    }

    // -----------------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------------

    public Optional<AgentEvent> getByKey(String eventId) {
        return Optional.ofNullable(events.get(eventId));
    }

    /**
     * Executes a filter query using the SqlParser to extract predicates.
     * Results are sorted by (timestampMs DESC, event_id ASC).
     */
    public List<AgentEvent> query(SqlPredicates preds) {
        Predicate<String, AgentEvent> predicate = buildPredicate(preds);
        Collection<AgentEvent> raw = (predicate != null) ? events.values(predicate) : events.values();
        List<AgentEvent> sorted = new ArrayList<>(raw);
        sorted.sort(Comparator
                .comparingLong(AgentEvent::getTimestampMs).reversed()
                .thenComparing(AgentEvent::getEventId));
        return sorted;
    }

    @SuppressWarnings("unchecked")
    private Predicate<String, AgentEvent> buildPredicate(SqlPredicates p) {
        List<Predicate<String, AgentEvent>> parts = new ArrayList<>();

        // "spiffeId", not "spiffe_id": Hazelcast resolves a predicate attribute
        // through the getter, and protoc emits getSpiffeId(). The snake_case form
        // matched no accessor, so this predicate could never evaluate.
        if (p.spiffeId != null) parts.add(Predicates.equal("spiffeId", p.spiffeId));
        if (p.sessionId != null) parts.add(Predicates.equal("sessionId", p.sessionId));
        if (p.agentId   != null) parts.add(Predicates.equal("agentId", p.agentId));
        if (p.eventType != null) parts.add(Predicates.equal(
                StratumEventTypeExtractor.ATTRIBUTE_NAME, p.eventType.getNumber()));
        if (p.startMs > 0 && p.endMs > 0) {
            parts.add(Predicates.between("timestampMs", p.startMs, p.endMs));
        } else if (p.startMs > 0) {
            parts.add(Predicates.greaterEqual("timestampMs", p.startMs));
        } else if (p.endMs > 0) {
            parts.add(Predicates.lessEqual("timestampMs", p.endMs));
        }

        if (parts.isEmpty()) return null;
        if (parts.size() == 1) return parts.get(0);
        return Predicates.and(parts.toArray(new Predicate[0]));
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    public long eventCount()    { return stores.totalCount(); }
    public boolean isEmpty()    { return stores.totalCount() == 0; }
    public int  clusterSize()   { return hz.getCluster().getMembers().size(); }
    public long lastCleanupMs() { return lastCleanupMs.get(); }

    /** The generated typed per-record hot stores — used by the RecordMap plane. */
    public StratumEventStores stores() { return stores; }

    // -----------------------------------------------------------------------
    // Retention cleanup
    // -----------------------------------------------------------------------

    private void runRetentionCleanupIfLeader() {
        Iterator<com.hazelcast.cluster.Member> members = hz.getCluster().getMembers().iterator();
        if (!members.hasNext()) return;
        if (!members.next().equals(hz.getCluster().getLocalMember())) return;
        runRetentionCleanup();
    }

    private void runRetentionCleanup() {
        long cutoffMs = System.currentTimeMillis()
                - (long) retentionDays * StratumEpochExtractor.MILLIS_PER_DAY;
        log.info("retention cleanup: removing events with timestampMs < {} (retention_days={})",
                cutoffMs, retentionDays);
        try {
            long before  = stores.totalCount();
            stores.runRetention(cutoffMs);
            long removed = before - stores.totalCount();
            lastCleanupMs.set(System.currentTimeMillis());
            lastCleanupRemoved.set(removed);
            log.info("retention cleanup complete: removed {} entries", removed);
        } catch (Exception e) {
            log.error("retention cleanup failed", e);
        }
    }

    // -----------------------------------------------------------------------
    // Shutdown
    // -----------------------------------------------------------------------

    public void shutdown() {
        cleaner.shutdownNow();
        if (hz != null) hz.shutdown();
    }

    // -----------------------------------------------------------------------
    // SQL predicate extraction (shared with StratumHandler for load_by_sql)
    // -----------------------------------------------------------------------

    static final class SqlPredicates {
        String          spiffeId;
        String          sessionId;
        String          agentId;
        SensorEventType eventType;
        long            startMs;
        long            endMs;
    }

    static final class SqlParser {
        static SqlPredicates parse(String sql) {
            SqlPredicates p = new SqlPredicates();
            if (sql == null) return p;
            String upper = sql.toUpperCase();

            p.spiffeId      = extractStringPredicate(sql, "spiffe_id");
            p.sessionId = extractStringPredicate(sql, "session_id");
            p.agentId   = extractStringPredicate(sql, "agent_id");

            // event_type = 'SENSOR_EVENT_TYPE_FILE_READ'
            String etVal = extractStringPredicate(sql, "event_type");
            if (etVal != null) {
                try { p.eventType = SensorEventType.valueOf(etVal); } catch (IllegalArgumentException ignored) {}
            }

            // timestamp_ms BETWEEN <start> AND <end>
            int betweenIdx = upper.indexOf("TIMESTAMP_MS BETWEEN");
            if (betweenIdx >= 0) {
                String rest = sql.substring(betweenIdx + "TIMESTAMP_MS BETWEEN".length()).trim();
                String[] parts = rest.split("(?i)\\s+AND\\s+", 2);
                if (parts.length == 2) {
                    try { p.startMs = Long.parseLong(parts[0].trim()); } catch (NumberFormatException ignored) {}
                    try {
                        p.endMs = Long.parseLong(parts[1].trim().split("\\s+")[0]);
                    } catch (NumberFormatException ignored) {}
                }
            }

            // timestamp_ms > <start>
            int gtIdx = upper.indexOf("TIMESTAMP_MS >");
            if (gtIdx >= 0 && p.startMs == 0) {
                String rest = sql.substring(gtIdx + "TIMESTAMP_MS >".length()).trim();
                if (rest.startsWith("=")) rest = rest.substring(1).trim();
                try { p.startMs = Long.parseLong(rest.split("\\s+")[0]); } catch (NumberFormatException ignored) {}
            }

            // timestamp_ms < <end>
            int ltIdx = upper.indexOf("TIMESTAMP_MS <");
            if (ltIdx >= 0 && p.endMs == 0) {
                String rest = sql.substring(ltIdx + "TIMESTAMP_MS <".length()).trim();
                if (rest.startsWith("=")) rest = rest.substring(1).trim();
                try { p.endMs = Long.parseLong(rest.split("\\s+")[0]); } catch (NumberFormatException ignored) {}
            }

            return p;
        }

        private static String extractStringPredicate(String sql, String field) {
            int idx = sql.toUpperCase().indexOf(field.toUpperCase() + " = '");
            if (idx < 0) idx = sql.toUpperCase().indexOf(field.toUpperCase() + "='");
            if (idx < 0) return null;
            int start = sql.indexOf('\'', idx) + 1;
            int end   = sql.indexOf('\'', start);
            if (start <= 0 || end < 0) return null;
            return sql.substring(start, end);
        }
    }
}
