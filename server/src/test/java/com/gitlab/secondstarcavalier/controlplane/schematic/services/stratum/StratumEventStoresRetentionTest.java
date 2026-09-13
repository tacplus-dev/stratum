package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.services.tasks.TasksOuterClass;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotWrites;

/**
 * The retention sweep must name each record type's OWN age field.
 *
 * The generated sweep hard-coded the attribute "timestampMs" for every typed
 * store. That is AgentEvent's field name; TaskActivity's is occurredAtMs. So
 * every hourly sweep threw
 *
 *   IllegalArgumentException: There is no suitable accessor for 'timestampMs'
 *   on class '...TasksOuterClass$TaskActivity'
 *
 * out of the FIRST removeAll, which aborted the whole pass — nothing was ever
 * purged from ANY store, including the one whose attribute was correct. The
 * failure is invisible at build time and the store grows without bound.
 *
 * These tests exercise the generated StratumEventStores against a real
 * Hazelcast member, because the bug lives in Hazelcast's reflective attribute
 * resolution and only a real map evaluates the predicate. Asserting "no
 * exception" alone would not catch a predicate that resolves but matches
 * nothing, so each test asserts WHICH entries survive.
 */
class StratumEventStoresRetentionTest {

    private static final long HOUR_MS = 3_600_000L;

    private HazelcastInstance hz;
    private StratumEventStores stores;

    @BeforeEach
    void setUp() {
        // Standalone member: no multicast, no TCP/IP join. A test that joined a
        // cluster would pick up whatever else is on the developer's network.
        Config cfg = new Config();
        cfg.setClusterName("stratum-retention-test");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(cfg);
        stores = new StratumEventStores();
        // Single-member test cluster, so the durability floor is declared as 1
        // rather than degraded to 1 — the same explicit statement a developer
        // laptop makes. Without it every write here would be refused, which is
        // exactly what the gate is for.
        stores.init(hz, new HotWrites(hz, 1));
    }

    @AfterEach
    void tearDown() {
        if (hz != null) hz.shutdown();
    }

    @Test
    void purgesOnlyTheTaskActivitiesOlderThanTheCutoff() {
        long now = System.currentTimeMillis();
        assertTrue(stores.store(taskActivity("old", now - 2 * HOUR_MS)));
        assertTrue(stores.store(taskActivity("fresh", now)));
        assertEquals(2, stores.tasksTaskActivity().size());

        stores.runRetention(now - HOUR_MS);

        assertFalse(stores.tasksTaskActivity().containsKey("old"),
                "a TaskActivity older than the cutoff must be purged");
        assertTrue(stores.tasksTaskActivity().containsKey("fresh"),
                "a TaskActivity newer than the cutoff must survive");
    }

    @Test
    void purgesOnlyTheAgentEventsOlderThanTheCutoff() {
        long now = System.currentTimeMillis();
        assertTrue(stores.store(agentEvent("old", now - 2 * HOUR_MS)));
        assertTrue(stores.store(agentEvent("fresh", now)));
        assertEquals(2, stores.agentsensorAgentEvent().size());

        stores.runRetention(now - HOUR_MS);

        assertFalse(stores.agentsensorAgentEvent().containsKey("old"),
                "an AgentEvent older than the cutoff must be purged");
        assertTrue(stores.agentsensorAgentEvent().containsKey("fresh"),
                "an AgentEvent newer than the cutoff must survive");
    }

    @Test
    void sweepsEveryStoreInOnePass() {
        // The regression itself: one store's predicate throwing took the other
        // store's sweep with it, so a pass that touches both must purge both.
        long now = System.currentTimeMillis();
        stores.store(taskActivity("old", now - 2 * HOUR_MS));
        stores.store(agentEvent("old", now - 2 * HOUR_MS));
        assertEquals(2, stores.totalCount());

        stores.runRetention(now - HOUR_MS);

        assertEquals(0, stores.totalCount(),
                "every typed store must be swept in a single retention pass");
    }

    private static TasksOuterClass.TaskActivity taskActivity(String activityId, long occurredAtMs) {
        return TasksOuterClass.TaskActivity.newBuilder()
                .setActivityId(activityId)
                .setTaskId("task-1")
                .setOccurredAtMs(occurredAtMs)
                .setStatus(TasksOuterClass.TaskStatus.TASK_STATUS_OPEN)
                .setSpiffeId("spiffe://example/tenants/t/users/u")
                .build();
    }

    private static Agentsensor.AgentEvent agentEvent(String eventId, long timestampMs) {
        return Agentsensor.AgentEvent.newBuilder()
                .setEventId(eventId)
                .setTimestampMs(timestampMs)
                .setEventType(Agentsensor.SensorEventType.SENSOR_EVENT_TYPE_SESSION_START)
                .setSessionId("session-1")
                .setAgentId("agent-1")
                .setSensorType(Agentsensor.SensorType.SENSOR_TYPE_CLAUDE_CODE)
                .setHookPhase(Agentsensor.HookPhase.HOOK_PHASE_PRE)
                .setSpiffeId("spiffe://example/tenants/t/users/u")
                .build();
    }
}
