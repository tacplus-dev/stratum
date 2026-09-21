// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;

import com.google.protobuf.Message;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotDataset;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotRedoLogs;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotRestore;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotWrites;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.Codec;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Helpers;

/**
 * Cold start is the checkpoint plus the redo tail, not a replay from offset zero.
 *
 * <p>This is the property the whole transaction exists for: after a checkpoint the
 * redo log holds only what landed AFTER the dump, so recovery costs the size of
 * the data plus one day of writes rather than the size of the history.
 */
class CheckpointColdStartTest {

    private static final String MAP = "agentsensor.AgentEvent";

    private HazelcastInstance hz;
    private HotRedoLogs logs;

    @AfterEach
    void tearDown() {
        if (logs != null) {
            logs.close();
            logs = null;
        }
        if (hz != null) {
            hz.shutdown();
            hz = null;
        }
    }

    private static HotDataset dataset() {
        return new HotDataset(MAP, 1,
                m -> ((Agentsensor.AgentEvent) m).getEventId(),
                m -> ((Agentsensor.AgentEvent) m).getStoreEpochMs(),
                "store_epoch_ms",
                m -> {
                    try {
                        return Helpers.agentEventToJson((Agentsensor.AgentEvent) m);
                    } catch (Exception e) {
                        throw new IllegalArgumentException(e);
                    }
                },
                json -> {
                    try {
                        return Helpers.agentEventFromJson(json);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
    }

    private static Agentsensor.AgentEvent event(String id, long epochMs) {
        return Agentsensor.AgentEvent.newBuilder()
                .setEventId(id)
                .setStoreEpochMs(epochMs)
                .setTimestampMs(epochMs)
                .setEventType(Agentsensor.SensorEventType.SENSOR_EVENT_TYPE_SESSION_START)
                .setSessionId("s1")
                .setAgentId("agent-1")
                .setSensorType(Agentsensor.SensorType.SENSOR_TYPE_CLAUDE_CODE)
                .setHookPhase(Agentsensor.HookPhase.HOOK_PHASE_PRE)
                .setSpiffeId("spiffe://example/tenants/t/users/u")
                .build();
    }

    private HazelcastInstance member(String cluster) {
        Config cfg = new Config();
        cfg.setClusterName(cluster);
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(cfg);
    }

    private void write(HotWrites hot, IMap<String, Message> map, Agentsensor.AgentEvent e) {
        hot.write(map, MAP, e.getEventId(), e, 1,
                () -> Helpers.agentEventToJson(e), e.getStoreEpochMs());
    }

    @Test
    void aColdStartLoadsTheCheckpointAndReplaysOnlyTheTail(@TempDir Path dataRoot) throws Exception {
        hz = member("stratum-checkpoint-coldstart-test");
        logs = new HotRedoLogs(dataRoot, List.of(dataset()), 3, Codec.NONE);
        HotWrites hot = new HotWrites(hz, 0);
        IMap<String, Message> map = hz.getMap(MAP);

        // Three writes before the checkpoint — the history the checkpoint replaces.
        write(hot, map, event("evt-1", 100L));
        write(hot, map, event("evt-2", 200L));
        write(hot, map, event("evt-3", 300L));
        logs.awaitAppends(5_000);
        assertEquals(3, RedoLog.dedupFile(logs.log(MAP).activePath()).size());

        new DailyCheckpoint(new HazelcastRows(hz, List.of(MAP)),
                (m, k) -> { },
                new TombstoneLog(dataRoot),
                new Checkpoint(dataRoot),
                logs::truncateToCheckpoint)
                .run(30L * 24 * 60 * 60 * 1000, 1_000_000L);

        assertEquals(0, RedoLog.dedupFile(logs.log(MAP).activePath()).size(),
                "after the dump the log is the TAIL, and the tail is empty at this instant");

        // The tail: one new row and one update of a row the checkpoint already holds.
        write(hot, map, event("evt-4", 400L));
        write(hot, map, event("evt-2", 500L));
        logs.awaitAppends(5_000);
        assertEquals(2, RedoLog.dedupFile(logs.log(MAP).activePath()).size(),
                "recovery reads two records, not the five writes this store has taken");

        // Cold start: the volume survives, the memory does not.
        map.clear();

        List<Checkpoint.Row> baseline = new Checkpoint(dataRoot).load();
        assertEquals(3, baseline.size());
        for (Checkpoint.Row row : baseline) {
            map.putIfAbsent(row.key, row.value);
        }
        HotRestore.Result replayed = HotRestore.replay(logs.log(MAP), dataset(), map);

        assertEquals(2, replayed.read, "only the tail is replayed");
        assertEquals(4, map.size());
        assertEquals(100L, ((Agentsensor.AgentEvent) map.get("evt-1")).getStoreEpochMs());
        assertEquals(500L, ((Agentsensor.AgentEvent) map.get("evt-2")).getStoreEpochMs(),
                "the tail's newer copy must win over the checkpoint's");
        assertEquals(400L, ((Agentsensor.AgentEvent) map.get("evt-4")).getStoreEpochMs());
    }

    /**
     * A checkpoint can never resurrect a purged row: the row is gone from the dump,
     * and the redo tail that still mentions it is discarded by the same
     * transaction's truncate.
     */
    @Test
    void aPurgedRowIsNotInTheCheckpointAndItsRedoTailIsGone(@TempDir Path dataRoot) throws Exception {
        hz = member("stratum-checkpoint-purge-test");
        logs = new HotRedoLogs(dataRoot, List.of(dataset()), 3, Codec.NONE);
        HotWrites hot = new HotWrites(hz, 0);
        IMap<String, Message> map = hz.getMap(MAP);

        write(hot, map, event("evt-1", 100L));
        logs.awaitAppends(5_000);

        // Removed by the memory tier the way the purge step removes a row, then
        // checkpointed: the dump holds nothing and the tail is discarded.
        map.remove("evt-1");
        new DailyCheckpoint(new HazelcastRows(hz, List.of(MAP)),
                (m, k) -> { },
                new TombstoneLog(dataRoot),
                new Checkpoint(dataRoot),
                logs::truncateToCheckpoint)
                .run(30L * 24 * 60 * 60 * 1000, 2_000_000L);

        assertTrue(new Checkpoint(dataRoot).load().isEmpty());
        assertEquals(0, RedoLog.dedupFile(logs.log(MAP).activePath()).size());

        map.clear();
        assertEquals(0, HotRestore.replay(logs.log(MAP), dataset(), map).read);
        assertEquals(0, map.size(), "nothing left anywhere can bring the row back");
    }
}
