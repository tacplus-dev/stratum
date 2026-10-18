// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.Codec;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.RedoLog;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Helpers;

/**
 * The hot tier's durability path, end to end: the write appends, and a restart
 * gets every pre-restart record back — BIG GOAL 4 acceptance item 4, and the
 * first time {@code service_store}'s redo log has actually run in stratum.
 *
 * <p>The dataset here is hand-built rather than taken from the generated
 * {@code StratumHotStores}: the runtime under test is entirely generic — it reads
 * the descriptor, not the type — and a hand-built descriptor is how that
 * genericity is stated, over an epoch field the declaration does not use. The
 * generated descriptor is driven end to end by {@code HotAgentEventE2ETest}
 * instead, and the generator's own output is pinned by
 * {@code HotStoreGeneratorTest}.
 */
class HotRedoRestoreTest {

    private static final String MAP = "agentsensor.AgentEvent";

    private HazelcastInstance hz;
    private HazelcastInstance peer;
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
        if (peer != null) {
            peer.shutdown();
            peer = null;
        }
    }

    // ------------------------------------------------------------------ fixtures

    /**
     * The store epoch is {@code timestamp_ms} here. On a record declaring
     * {@code (spec.store_epoch)} the generator names that field instead; nothing
     * in the mechanism depends on which field it is, only that there IS one.
     */
    private static HotDataset dataset(int minDurableCopies) {
        return new HotDataset(MAP, minDurableCopies,
                m -> ((Agentsensor.AgentEvent) m).getEventId(),
                m -> ((Agentsensor.AgentEvent) m).getTimestampMs(),
                "timestamp_ms",
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

    private static Agentsensor.AgentEvent event(String id, long epochMs, String session) {
        return Agentsensor.AgentEvent.newBuilder()
                .setEventId(id)
                .setTimestampMs(epochMs)
                .setEventType(Agentsensor.SensorEventType.SENSOR_EVENT_TYPE_SESSION_START)
                .setSessionId(session)
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

    // ------------------------------------------------------------------ append

    @Test
    void aHotWriteAppendsToThisNodesRedoLog(@TempDir Path dir) throws Exception {
        hz = member("stratum-redo-append-test");
        HotDataset d = dataset(1);
        logs = new HotRedoLogs(dir, List.of(d), 3, Codec.NONE);
        HotWrites hot = new HotWrites(hz, 0);
        IMap<String, Message> map = hz.getMap(MAP);

        Agentsensor.AgentEvent e = event("evt-1", 1000L, "s1");
        hot.write(map, MAP, "evt-1", e, 1, () -> Helpers.agentEventToJson(e), 1000L);

        // The append is queued, not written, by the time the write returns — see
        // theAckDoesNotWaitOnTheDiskAppend for that being the point rather than a
        // concession. This asserts WHAT is appended, so it waits for the writer.
        logs.awaitAppends(5_000);

        String log = Files.readString(logs.log(MAP).activePath());
        assertTrue(log.contains("\"key\":\"evt-1\""), log);
        assertTrue(log.contains("evt-1"), log);
        assertTrue(log.contains("1000"), "the store epoch must ride the record as metadata:\n" + log);
        assertEquals(e, map.get("evt-1"));
    }

    /**
     * Acceptance item 5, second half: THE ACK DOES NOT WAIT ON DISK.
     *
     * <p>The node's append thread is occupied before the write, so a synchronous
     * append could not complete. The write still returns and the value is
     * readable from memory, and the log is still empty at that moment — which is
     * the falsification: put {@code log.append} back on the entry-processor body
     * and the record is on disk here, so the emptiness assertion goes red.
     *
     * <p>The elapsed bound is deliberately loose. It is not the claim — the claim
     * is that the write completed at all while the only thread that can touch the
     * file was parked — it only catches a write that limped through some other
     * wait.
     */
    @Test
    void theAckDoesNotWaitOnTheDiskAppend(@TempDir Path dir) throws Exception {
        hz = member("stratum-redo-ack-latency-test");
        HotDataset d = dataset(1);
        logs = new HotRedoLogs(dir, List.of(d), 3, Codec.NONE);
        HotWrites hot = new HotWrites(hz, 0);
        IMap<String, Message> map = hz.getMap(MAP);

        CountDownLatch release = new CountDownLatch(1);
        logs.blockWriter(release);

        Agentsensor.AgentEvent e = event("evt-1", 1000L, "s1");
        long startedNs = System.nanoTime();
        hot.write(map, MAP, "evt-1", e, 1, () -> Helpers.agentEventToJson(e), 1000L);
        long elapsedMs = (System.nanoTime() - startedNs) / 1_000_000L;

        assertEquals(e, map.get("evt-1"), "the write must have acked from memory with the writer parked");
        assertFalse(logContents(logs).contains("evt-1"),
                "the redo append ran on the ack path: the record reached disk while the only"
                        + " append thread was blocked, which it can only do from inside the write");
        assertTrue(elapsedMs < 5_000,
                "the write took " + elapsedMs + "ms with a parked writer; it is waiting on something");

        release.countDown();
        logs.awaitAppends(5_000);
        assertTrue(logContents(logs).contains("evt-1"),
                "the queued append must still land once the writer is released");
        assertEquals(1, logs.appendedCount());
        assertEquals(0, logs.appendFailureCount());
    }

    /**
     * The restore-time gap the asynchrony opens, and what closes it: a replica
     * killed between the ack and its queued append has a log MISSING a row it
     * acked. The row is recovered from the peer that did land its append, because
     * restore is the union of the reachable nodes' logs — and
     * {@link HotRestore#awaitClusterReplay} is what makes that union complete
     * before a hot read is served. Without the barrier this node would answer from
     * its own partial log and report the row absent.
     *
     * <p>What is NOT closed, and cannot be by any barrier: every replica losing
     * the same queued append. That is the named cost of acking on in-memory
     * replication.
     */
    @Test
    void aQueuedAppendLostOnOneReplicaIsRecoveredFromTheOther(@TempDir Path dir) throws Exception {
        HotDataset d = dataset(1);
        Path crashed = dir.resolve("crashed");
        Path survivor = dir.resolve("survivor");
        // The crashed node's log exists and is empty: it acked the write in memory
        // and died before the writer thread got to it.
        writeLog(crashed, d, event("evt-other", 500L, "unrelated"));
        writeLog(survivor, d, event("evt-1", 1000L, "acked"));

        hz = member("stratum-lost-append-test");
        IMap<String, Message> map = hz.getMap(MAP);
        HotRestore.Result fromCrashed = replayFrom(crashed, d, map);
        assertNull(map.get("evt-1"), "the crashed node's own log cannot produce the row it lost");
        HotRestore.Result fromSurvivor = replayFrom(survivor, d, map);

        assertEquals(1, fromCrashed.applied);
        assertEquals(1, fromSurvivor.applied);
        assertTrue(fromSurvivor.diverged.isEmpty());
        assertEquals("acked", ((Agentsensor.AgentEvent) map.get("evt-1")).getSessionId(),
                "the union across reachable logs must recover a row whose append the other node lost");
    }

    // ------------------------------------------------------- restart recovery

    /**
     * Acceptance item 4: every record written before the restart is back after it.
     *
     * <p>The map is deliberately destroyed with the member — a restart that kept
     * its memory would pass without the log having done anything.
     */
    @Test
    void everyRecordWrittenBeforeARestartIsBackAfterIt(@TempDir Path dir) throws Exception {
        HotDataset d = dataset(1);

        hz = member("stratum-redo-restart-test-a");
        logs = new HotRedoLogs(dir, List.of(d), 3, Codec.NONE);
        HotWrites hot = new HotWrites(hz, 0);
        IMap<String, Message> map = hz.getMap(MAP);
        for (int i = 0; i < 25; i++) {
            Agentsensor.AgentEvent e = event("evt-" + i, 1000L + i, "s" + i);
            hot.write(map, MAP, e.getEventId(), e, 1, () -> Helpers.agentEventToJson(e), e.getTimestampMs());
        }
        assertEquals(25, map.size());

        // Full stop: the member and its memory are gone, and so is the registry.
        logs.close();
        hz.shutdown();

        hz = member("stratum-redo-restart-test-b");
        logs = new HotRedoLogs(dir, List.of(d), 3, Codec.NONE);
        IMap<String, Message> recovered = hz.getMap(MAP);
        assertEquals(0, recovered.size(), "the restarted member must start empty");

        HotRestore.Result result = HotRestore.replay(logs.log(MAP), d, recovered);

        assertEquals(0, result.diverged.size(), "clean logs must not report divergence");
        assertEquals(25, result.applied);
        assertEquals(25, recovered.size());
        for (int i = 0; i < 25; i++) {
            Agentsensor.AgentEvent back = (Agentsensor.AgentEvent) recovered.get("evt-" + i);
            assertNotNull(back, "evt-" + i + " did not survive the restart");
            assertEquals("s" + i, back.getSessionId());
            assertEquals(1000L + i, back.getTimestampMs());
        }
    }

    // ---------------------------------------------------------- the union rule

    /**
     * Two nodes' logs, one key: the higher epoch wins regardless of which log is
     * replayed first. Replay ORDER deciding the winner would be
     * last-writer-wins by file position — the convergent behaviour that must not
     * reach stratum's store.
     */
    @Test
    void theHigherEpochWinsWhicheverLogReplaysFirst(@TempDir Path dir) throws Exception {
        HotDataset d = dataset(1);
        Path nodeA = dir.resolve("a");
        Path nodeB = dir.resolve("b");
        writeLog(nodeA, d, event("evt-1", 1000L, "old"));
        writeLog(nodeB, d, event("evt-1", 2000L, "new"));

        assertEquals("new", replayBoth(d, nodeA, nodeB, "stratum-union-ab").getSessionId());
        assertEquals("new", replayBoth(d, nodeB, nodeA, "stratum-union-ba").getSessionId());
    }

    /**
     * Q3: same key, same epoch, different bytes. The atomic write path makes this
     * impossible, so its appearance is proof the path was violated — it fails
     * closed and names the key rather than picking a winner.
     */
    @Test
    void equalEpochDivergenceFailsClosed(@TempDir Path dir) throws Exception {
        HotDataset d = dataset(1);
        Path nodeA = dir.resolve("a");
        Path nodeB = dir.resolve("b");
        writeLog(nodeA, d, event("evt-1", 1000L, "one"));
        writeLog(nodeB, d, event("evt-1", 1000L, "other"));

        hz = member("stratum-diverge-test");
        IMap<String, Message> map = hz.getMap(MAP);
        HotRestore.Result first = replayFrom(nodeA, d, map);
        HotRestore.Result second = replayFrom(nodeB, d, map);

        assertTrue(first.diverged.isEmpty(), "the first log has nothing to disagree with");
        assertEquals(List.of("evt-1"), second.diverged,
                "an equal-epoch, different-bytes key must be reported, not resolved");
        assertEquals("one", ((Agentsensor.AgentEvent) map.get("evt-1")).getSessionId(),
                "a diverged key must be left exactly as it was, not overwritten");
    }

    /**
     * The schema-drift crash loop, and its end.
     *
     * <p>The redo payload is a field-by-field JSON encoding of the record's
     * DECLARED fields, so a producer shipping a new field before stratum picks up
     * the same proto writes a row whose extra bytes land in the wire object's
     * unknown field set — which {@code equals} compares and the encoding cannot
     * carry. On a rolling restart the peer still owns the partition and holds the
     * row exactly as it came off the wire, so this node's replay meets a row that
     * differs from its own candidate in those unknown fields ALONE.
     *
     * <p>Before the fix that reported DIVERGED, and a non-empty {@code diverged}
     * list is exactly what {@code EventStore.restoreHotTier} throws on: init
     * aborts, the pod restarts, replays the same log against the same peer row and
     * refuses again. Permanently, until the producer is rolled back. So the
     * assertion that {@code diverged} is empty IS the assertion that init no longer
     * throws.
     */
    @Test
    void aFieldTheRedoEncodingDropsIsNotDivergence(@TempDir Path dir) throws Exception {
        HotDataset d = dataset(1);
        Agentsensor.AgentEvent wire = withUnknownField(event("evt-1", 1000L, "s1"), "shipped-ahead");
        // The payload the encoding actually produced: field 300 is gone from it.
        assertFalse(d.toJson.apply(wire).contains("shipped-ahead"),
                "premise: the redo encoding must be the thing that drops the new field");
        Path node = dir.resolve("a");
        writeLog(node, d, wire);

        hz = member("stratum-schema-drift-test");
        IMap<String, Message> map = hz.getMap(MAP);
        map.put("evt-1", wire);

        HotRestore.Result result = replayFrom(node, d, map);

        assertTrue(result.diverged.isEmpty(),
                "a row differing only in fields the redo encoding cannot carry was reported as a"
                        + " write-path violation, which aborts init and crash-loops the pod: "
                        + result.diverged);
        assertEquals(1, result.skipped);
        assertEquals(wire, map.get("evt-1"),
                "the wire row holds a superset of the replayed one and must be left in place");
    }

    /**
     * The paired control: the exemption is for the ENCODING's blind spot, not for
     * disagreement. The same unknown field on the map's row, and a log whose
     * declared fields genuinely disagree at the same epoch, still fails closed.
     */
    @Test
    void unknownFieldsDoNotExcuseARealEqualEpochDivergence(@TempDir Path dir) throws Exception {
        HotDataset d = dataset(1);
        Path node = dir.resolve("a");
        writeLog(node, d, withUnknownField(event("evt-1", 1000L, "other"), "shipped-ahead"));

        hz = member("stratum-schema-drift-control-test");
        IMap<String, Message> map = hz.getMap(MAP);
        map.put("evt-1", withUnknownField(event("evt-1", 1000L, "one"), "shipped-ahead"));

        assertEquals(List.of("evt-1"), replayFrom(node, d, map).diverged,
                "session_id is a declared field; the two logs disagreeing about it at one epoch is"
                        + " still the write-path violation, whatever else the row carries");
    }

    /** The same write replayed from the owner's log and the backup's is idempotent. */
    @Test
    void theSameWriteFromTwoLogsIsNotDivergence(@TempDir Path dir) throws Exception {
        HotDataset d = dataset(1);
        Path nodeA = dir.resolve("a");
        Path nodeB = dir.resolve("b");
        writeLog(nodeA, d, event("evt-1", 1000L, "same"));
        writeLog(nodeB, d, event("evt-1", 1000L, "same"));

        hz = member("stratum-idempotent-test");
        IMap<String, Message> map = hz.getMap(MAP);
        replayFrom(nodeA, d, map);
        HotRestore.Result second = replayFrom(nodeB, d, map);

        assertTrue(second.diverged.isEmpty(), "identical bytes at one epoch are one write, replayed twice");
        assertEquals(1, map.size());
    }

    // ------------------------------------------------------------- Q2 barrier

    @Test
    void hotReadsWaitForEveryReachableInstance(@TempDir Path dir) throws Exception {
        hz = member("stratum-barrier-test");
        int contributors = HotRestore.awaitClusterReplay(hz, 7, 0, 1, java.time.Duration.ofSeconds(5));
        assertEquals(1, contributors);
    }

    /**
     * The staggered-start race.
     *
     * <p>A StatefulSet's pods do not start together. The FIRST one up is alone, so a
     * barrier that asks only "has every member I can SEE reported?" is satisfied by
     * its own report before a peer has joined at all — and hot reads open on a store
     * holding one node's redo log, which is the precise condition the barrier exists
     * to refuse. min_durable_copies is the declaration of how many instances there
     * are meant to be, so the expected count never falls below it.
     *
     * <p>Two members is what the default floor asks for; this node is one, and it
     * must not pass alone.
     */
    @Test
    void theFirstPodUpDoesNotPassTheBarrierAlone(@TempDir Path dir) throws Exception {
        hz = member("stratum-barrier-stagger-test");

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> HotRestore.awaitClusterReplay(hz, 5, 0, 2, java.time.Duration.ofSeconds(2)),
                "the first pod up passed a two-instance barrier by itself");
        assertTrue(e.getMessage().contains("only 1 of 2 expected writer members"), e.getMessage());
    }

    /**
     * The paired control: the same node, same floor, passes the moment the peer's
     * report lands. The refusal above is the barrier waiting, not the barrier stuck.
     */
    @Test
    void theBarrierPassesOnceThePeerHasReported(@TempDir Path dir) throws Exception {
        twoMemberCluster("stratum-barrier-peer-test");
        peerReported(peer, 5);

        assertEquals(2, HotRestore.awaitClusterReplay(hz, 5, 0, 2, java.time.Duration.ofSeconds(5)),
                "both instances read a log, so both contributed one");
    }

    /**
     * A node that replayed nothing is not a contributor. Below the floor that is
     * LOGGED loudly rather than treated as a normal start — the recovered store is
     * then thinner than the durability its writes were acked under.
     */
    @Test
    void anEmptyReplayIsNotCountedAsAContribution(@TempDir Path dir) throws Exception {
        twoMemberCluster("stratum-barrier-empty-test");
        peerReported(peer, 0);
        assertEquals(0, HotRestore.awaitClusterReplay(hz, 0, 0, 2, java.time.Duration.ofSeconds(5)));
    }

    /**
     * The rolling-restart false positive: a node whose log replayed entirely as
     * SKIPPED still contributed its log.
     *
     * <p>One node at a time restarts; its peers keep owning the partitions, so
     * every row in the restarted node's log is already in the map at the
     * same-or-newer epoch and the replay applies NOTHING. That is a healthy
     * restart, and the durability alarm must not fire on it. The map here is
     * pre-loaded by replaying the same log once — that is what a peer still
     * holding the partition looks like to the second replay.
     *
     * <p>The assertion is on the contributor count rather than on captured log
     * output: the count IS the condition the {@code log.error} is gated on, and
     * the module has both logback-classic and slf4j-simple on the test classpath,
     * so which binding an appender would attach to is not determined.
     */
    @Test
    void aLogThatReplayedEntirelySkippedStillCountsAsAContribution(@TempDir Path dir) throws Exception {
        HotDataset d = dataset(1);
        Path node = dir.resolve("rolling");
        writeLog(node, d, event("evt-1", 1000L, "s1"));
        writeLog(node, d, event("evt-2", 1001L, "s2"));

        hz = member("stratum-rolling-restart-test");
        IMap<String, Message> map = hz.getMap(MAP);
        // The peers' copies, still owned across this node's restart.
        assertEquals(2, replayFrom(node, d, map).applied);

        HotRestore.Result afterRestart = replayFrom(node, d, map);

        assertEquals(0, afterRestart.applied, "a rolling restart has nothing to catch up");
        assertEquals(2, afterRestart.skipped);
        assertEquals(2, afterRestart.read, "the log was present and read in full");
        assertTrue(afterRestart.diverged.isEmpty());

        assertEquals(1, HotRestore.awaitClusterReplay(hz, afterRestart.read, 0, 1, java.time.Duration.ofSeconds(5)),
                "a node whose whole log replayed as SKIPPED had a redo log; reporting applied"
                        + " records instead would raise the durability alarm on every healthy"
                        + " rolling restart");
    }

    /**
     * The paired control: the alarm still fires when it should. A node with no
     * redo log at all reads nothing, and reading nothing is still no contribution.
     */
    @Test
    void aNodeWithNoRedoLogStillContributesNothing(@TempDir Path dir) throws Exception {
        HotDataset d = dataset(1);
        Path empty = dir.resolve("never-owned-a-partition");

        twoMemberCluster("stratum-absent-log-test");
        IMap<String, Message> map = hz.getMap(MAP);
        HotRestore.Result result = replayFrom(empty, d, map);

        assertEquals(0, result.applied);
        assertEquals(0, result.skipped);
        assertEquals(0, result.read, "there was no log to read");

        peerReported(peer, 0);
        assertEquals(0, HotRestore.awaitClusterReplay(hz, result.read, 0, 2, java.time.Duration.ofSeconds(5)),
                "an absent log is still zero contribution, and below the floor that stays LOUD");
    }

    // ------------------------------------------------------------------ helpers

    /**
     * A peer's entry in the shared replay ledger, filed under that peer's REAL
     * member UUID.
     *
     * <p>This used to invent a UUID ("peer-a") and write the report directly,
     * because {@link #member} disables both join mechanisms and two such instances
     * cannot form one cluster. That simulation is no longer faithful: the barrier
     * now counts only reports belonging to members of the current cluster view,
     * precisely because the ledger is keyed by a UUID that changes on every restart
     * and an entry under a UUID nobody in the cluster holds is indistinguishable
     * from a dead pod's leftover. A fabricated UUID was therefore simulating the
     * stale entry the barrier must ignore, not the live peer these tests mean.
     *
     * <p>So the peer is a real second member now, and what it reports is filed under
     * the UUID the cluster actually sees it by.
     */
    private static void peerReported(HazelcastInstance peer, int recordsRead) {
        String uuid = peer.getCluster().getLocalMember().getUuid().toString();
        // With the in-flight mark awaitClusterReplay puts before its ledger write,
        // for the same reason the UUID is real: a report without it is not a peer
        // reporting from inside its own restore, it is a peer that has FINISHED one
        // and is serving the maps — which the barrier counts as a contributor
        // whatever number it filed (HotRestoreServingPeerContributorTest). These
        // tests mean a peer that is here and recovering alongside this node.
        peer.<String, Boolean>getMap(HotRestore.INFLIGHT_MAP).put(uuid, Boolean.TRUE);
        peer.<String, Integer>getMap(HotRestore.LEDGER_MAP).put(uuid, recordsRead);
    }

    /**
     * A second member of {@code cluster}, joined over loopback TCP so the two
     * instances share one cluster view — which is what {@link #member}'s
     * join-disabled config cannot produce.
     */
    private HazelcastInstance joinedMember(String cluster) {
        Config cfg = new Config();
        cfg.setClusterName(cluster);
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).setMembers(List.of("127.0.0.1"));
        return Hazelcast.newHazelcastInstance(cfg);
    }

    /** Brings up a two-member cluster and waits for both to see each other. */
    private void twoMemberCluster(String cluster) throws Exception {
        hz = joinedMember(cluster);
        peer = joinedMember(cluster);
        for (int i = 0; i < 100 && hz.getCluster().getMembers().size() < 2; i++) {
            Thread.sleep(100);
        }
        assertEquals(2, hz.getCluster().getMembers().size(), "the two members did not form one cluster");
    }

    /** The active log's bytes, or "" before the first append has created it. */
    private static String logContents(HotRedoLogs logs) throws Exception {
        Path active = logs.log(MAP).activePath();
        return Files.exists(active) ? Files.readString(active) : "";
    }

    /**
     * The event as a producer running a NEWER proto than stratum's would leave it:
     * a field stratum's copy does not declare, parsed into the unknown field set.
     * Field 300 is unused by AgentEvent, which is the point — it is what a field
     * added after this build looks like from here.
     */
    private static Agentsensor.AgentEvent withUnknownField(Agentsensor.AgentEvent e, String value) {
        return e.toBuilder()
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(300, UnknownFieldSet.Field.newBuilder()
                                .addLengthDelimited(ByteString.copyFromUtf8(value)).build())
                        .build())
                .build();
    }

    /** Writes one record straight into a node's log, without a cluster. */
    private static void writeLog(Path nodeDir, HotDataset d, Agentsensor.AgentEvent e) {
        try (RedoLog log = RedoLog.open(nodeDir.resolve(HotRedoLogs.HOT_SUBDIR), "/", d.mapName, 3, Codec.NONE)) {
            log.append(new RedoLog.Record(e.getEventId(), false,
                    Long.toString(e.getTimestampMs()), d.toJson.apply(e)));
        }
    }

    private static HotRestore.Result replayFrom(Path nodeDir, HotDataset d, IMap<String, Message> map) {
        try (RedoLog log = RedoLog.open(nodeDir.resolve(HotRedoLogs.HOT_SUBDIR), "/", d.mapName, 3, Codec.NONE)) {
            return HotRestore.replay(log, d, map);
        }
    }

    private Agentsensor.AgentEvent replayBoth(HotDataset d, Path first, Path second, String cluster) {
        HazelcastInstance member = member(cluster);
        try {
            IMap<String, Message> map = member.getMap(MAP);
            assertFalse(replayFrom(first, d, map).diverged.size() > 0);
            assertFalse(replayFrom(second, d, map).diverged.size() > 0);
            return (Agentsensor.AgentEvent) map.get("evt-1");
        } finally {
            member.shutdown();
        }
    }
}
