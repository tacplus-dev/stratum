// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.chassis.collection.CollectionRuntime;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.agentsensor.Agentsensor;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentEvent;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.agentsensor.Agentsensor.SessionSummary;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorEventType;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorType;

/**
 * Unit tests for StratumQueryImpl session aggregation fold and keyset paging.
 * These test the pure logic without Hazelcast (no EventStore/WorkingSetStore needed).
 */
class StratumQueryImplTest {

    // -----------------------------------------------------------------------
    // Session aggregation fold
    // -----------------------------------------------------------------------

    @Test
    void buildSessionSummaries_singleSession() {
        long now = 1_000_000L;
        List<AgentEvent> events = List.of(
                event("s1", "spiffe://x", 100, SensorEventType.SENSOR_EVENT_TYPE_SESSION_START, 0),
                event("s1", "spiffe://x", 200, SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, 1),
                event("s1", "spiffe://x", 300, SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, 2));

        List<SessionSummary> summaries = StratumQueryImpl.buildSessionSummaries(events, now, 900);

        assertEquals(1, summaries.size());
        SessionSummary s = summaries.get(0);
        assertEquals("s1", s.getSessionId());
        assertEquals("spiffe://x", s.getSpiffeId());
        assertEquals(100, s.getFirstSeenMs());
        assertEquals(300, s.getLastSeenMs());
        assertEquals(3, s.getEventCount());
        assertEquals(2, s.getMaxDepth());
        assertEquals(SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, s.getLatestEventType());
        // idle = (1_000_000 - 300) / 1000 = 999 > 900 → not active
        assertFalse(s.getIsActive());
        assertTrue(s.getIdleSeconds() > 0);
    }

    @Test
    void buildSessionSummaries_activeSessions() {
        long now = 1_000L;
        List<AgentEvent> events = List.of(
                event("s1", "spiffe://a", 500, SensorEventType.SENSOR_EVENT_TYPE_SESSION_START, 0));

        List<SessionSummary> summaries = StratumQueryImpl.buildSessionSummaries(events, now, 900);

        assertEquals(1, summaries.size());
        SessionSummary s = summaries.get(0);
        assertTrue(s.getIsActive());
        assertEquals(0, s.getIdleSeconds());
    }

    @Test
    void buildSessionSummaries_multipleSessions() {
        long now = 100_000L;
        List<AgentEvent> events = List.of(
                event("s1", "spiffe://a", 100, SensorEventType.SENSOR_EVENT_TYPE_SESSION_START, 0),
                event("s2", "spiffe://b", 200, SensorEventType.SENSOR_EVENT_TYPE_SESSION_START, 0),
                event("s1", "spiffe://a", 300, SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, 1),
                event("s2", "spiffe://b", 400, SensorEventType.SENSOR_EVENT_TYPE_SESSION_END, 0));

        List<SessionSummary> summaries = StratumQueryImpl.buildSessionSummaries(events, now, 900);

        assertEquals(2, summaries.size());
        // Both should exist with correct event counts
        SessionSummary s1 = summaries.stream().filter(s -> s.getSessionId().equals("s1")).findFirst().orElseThrow();
        SessionSummary s2 = summaries.stream().filter(s -> s.getSessionId().equals("s2")).findFirst().orElseThrow();
        assertEquals(2, s1.getEventCount());
        assertEquals(2, s2.getEventCount());
        assertEquals(300, s1.getLastSeenMs());
        assertEquals(400, s2.getLastSeenMs());
    }

    @Test
    void buildSessionSummaries_skipsEmptySessionId() {
        long now = 1_000L;
        List<AgentEvent> events = List.of(
                event("", "spiffe://a", 100, SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, 0),
                event("s1", "spiffe://a", 200, SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, 0));

        List<SessionSummary> summaries = StratumQueryImpl.buildSessionSummaries(events, now, 900);

        assertEquals(1, summaries.size());
        assertEquals("s1", summaries.get(0).getSessionId());
    }

    @Test
    void buildSessionSummaries_maxDepthTracked() {
        long now = 1_000L;
        List<AgentEvent> events = List.of(
                event("s1", "spiffe://a", 100, SensorEventType.SENSOR_EVENT_TYPE_SUBAGENT_START, 0),
                event("s1", "spiffe://a", 200, SensorEventType.SENSOR_EVENT_TYPE_SUBAGENT_START, 3),
                event("s1", "spiffe://a", 300, SensorEventType.SENSOR_EVENT_TYPE_SUBAGENT_STOP, 1));

        List<SessionSummary> summaries = StratumQueryImpl.buildSessionSummaries(events, now, 900);

        assertEquals(3, summaries.get(0).getMaxDepth());
    }

    @Test
    void buildQuickSummary_active() {
        AgentEvent ev = event("s1", "spiffe://x", 500, SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, 2);
        SessionSummary s = StratumQueryImpl.buildQuickSummary(ev, 1000, 900);

        assertEquals("s1", s.getSessionId());
        assertEquals("spiffe://x", s.getSpiffeId());
        assertTrue(s.getIsActive());
        assertEquals(SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, s.getLatestEventType());
    }

    // -----------------------------------------------------------------------
    // Keyset pagination (StratumHandler.pageSorted)
    // -----------------------------------------------------------------------

    @Test
    void pageSorted_firstPage() {
        List<AgentEvent> events = new ArrayList<>(List.of(
                event("s1", "spiffe://a", 300, SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, 0),
                event("s1", "spiffe://a", 100, SensorEventType.SENSOR_EVENT_TYPE_SESSION_START, 0),
                event("s1", "spiffe://a", 200, SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, 0)));

        CollectionRuntime.PageParams page = new CollectionRuntime.PageParams(2, null);
        CollectionRuntime.Page<AgentEvent> result = StratumHandler.pageSorted(events, page,
                e -> List.of(StratumHandler.i64Key(e.getTimestampMs()), e.getEventId()));

        assertEquals(2, result.items.size());
        // Sorted ascending by timestamp
        assertEquals(100, result.items.get(0).getTimestampMs());
        assertEquals(200, result.items.get(1).getTimestampMs());
        assertNotNull(result.nextAfter);
        assertEquals(3, result.total);
    }

    @Test
    void pageSorted_secondPage() {
        List<AgentEvent> events = new ArrayList<>(List.of(
                event("s1", "spiffe://a", 300, SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, 0),
                event("s1", "spiffe://a", 100, SensorEventType.SENSOR_EVENT_TYPE_SESSION_START, 0),
                event("s1", "spiffe://a", 200, SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE, 0)));

        // First page: get cursor
        CollectionRuntime.PageParams page1 = new CollectionRuntime.PageParams(2, null);
        CollectionRuntime.Page<AgentEvent> r1 = StratumHandler.pageSorted(new ArrayList<>(events), page1,
                e -> List.of(StratumHandler.i64Key(e.getTimestampMs()), e.getEventId()));

        // Second page: use cursor
        CollectionRuntime.PageParams page2 = new CollectionRuntime.PageParams(2, r1.nextAfter);
        CollectionRuntime.Page<AgentEvent> r2 = StratumHandler.pageSorted(events, page2,
                e -> List.of(StratumHandler.i64Key(e.getTimestampMs()), e.getEventId()));

        assertEquals(1, r2.items.size());
        assertEquals(300, r2.items.get(0).getTimestampMs());
        assertNull(r2.nextAfter); // last page
    }

    @Test
    void pageSorted_emptyList() {
        List<AgentEvent> events = new ArrayList<>();
        CollectionRuntime.PageParams page = new CollectionRuntime.PageParams(10, null);
        CollectionRuntime.Page<AgentEvent> result = StratumHandler.pageSorted(events, page,
                e -> List.of(StratumHandler.i64Key(e.getTimestampMs()), e.getEventId()));

        assertTrue(result.items.isEmpty());
        assertNull(result.nextAfter);
        assertEquals(0, result.total);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static int eventCounter = 0;

    private static AgentEvent event(String sessionId, String spiffeId, long timestampMs,
                                     SensorEventType eventType, int depth) {
        return AgentEvent.newBuilder()
                .setEventId("evt-" + (++eventCounter))
                .setSessionId(sessionId)
                .setSpiffeId(spiffeId)
                .setTimestampMs(timestampMs)
                .setEventType(eventType)
                .setSensorType(SensorType.SENSOR_TYPE_CLAUDE_CODE)
                .setWorkingDirectory("/tmp/test")
                .setAgentId("agent-1")
                .setDepth(depth)
                .build();
    }
}
