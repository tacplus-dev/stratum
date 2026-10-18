// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Message;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.hot.HotDataset;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.hot.HotRedoLogs;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.hot.HotRestore;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.hot.HotWrites;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.Codec;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.token.TokenWrapper;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.s2sm.internal.grpc.servers.vservices.query.RecordMap;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.sys.errors.ProtoException;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentEvent;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.HookPhase;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorEventType;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorType;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.query.Query;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.query.RecordMapGrpc;

/**
 * The hot tier over a REAL declared hot record: AgentEvent, which carries
 * {@code (spec.store_epoch) store_epoch_ms} as of BIG GOAL 4 P4a.
 *
 * <p>Everything under test here is generated from that one declaration and taken
 * as generated: the redo descriptor ({@link StratumHotStores}), the typed store
 * and its hot write ({@link StratumEventStores}), the query handler
 * ({@code StratumRecordMaps}), the server wrapper and the client stub. That is
 * the distinction from {@code HotRedoRestoreTest}, which drives the same runtime
 * through a HAND-BUILT descriptor and so cannot show that a declaration produces
 * one.
 *
 * <p>What it still does NOT cover, stated plainly: the stamp itself. The store's
 * clock is stamped in the generated ServiceManager's subscription dispatch, which
 * needs a bootstrapped service and a broker; here the fixtures arrive already
 * stamped, exactly as that dispatch hands them to the handler. Whether the
 * deployed subscription stamps is P4's live drill, not this test's claim.
 */
class HotAgentEventE2ETest {

    private static final String MAP = "agentsensor.AgentEvent";

    /** Store epochs, distinct from the business timestamps below on purpose. */
    private static final long EPOCH_1 = 1_700_000_000_000L;
    private static final long EPOCH_2 = 1_700_000_001_000L;

    private static HazelcastInstance hz;
    private static HotRedoLogs logs;
    private static StratumEventStores stores;
    private static Server server;
    private static ManagedChannel channel;
    private static RecordMapGrpc.RecordMapBlockingStub stub;

    /**
     * The hot tier's restore gate, as the plane reads it. Mutable so the closed
     * state — a node whose peers have not replayed yet — can be driven over a real
     * RPC rather than asserted about the generator's output.
     */
    private static final java.util.concurrent.atomic.AtomicBoolean hotReadable =
            new java.util.concurrent.atomic.AtomicBoolean(true);

    /**
     * A record as the subscription dispatch hands it over: store epoch stamped by
     * the store, business timestamp written by the author. They are deliberately
     * far apart so an assertion cannot pass by reading the wrong clock.
     */
    private static AgentEvent event(String eventId, long timestampMs, long storeEpochMs) {
        return AgentEvent.newBuilder()
                .setEventId(eventId)
                .setSessionId("s1")
                .setAgentId("ag1")
                .setSpiffeId("spiffe://cav/tenants/t1/users/a")
                .setTimestampMs(timestampMs)
                .setStoreEpochMs(storeEpochMs)
                .setEventType(SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE)
                .setSensorType(SensorType.SENSOR_TYPE_CLAUDE_CODE)
                .setHookPhase(HookPhase.HOOK_PHASE_POST)
                .build();
    }

    @BeforeAll
    static void up(@TempDir Path dataRoot) throws Exception {
        Config cfg = new Config();
        cfg.setClusterName("stratum-hot-agentevent-e2e-" + UUID.randomUUID());
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().setPortAutoIncrement(true);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(cfg);

        // The GENERATED descriptor list — the point of the test. A single-member
        // cluster declares its floor as 1 rather than degrading to it, the same
        // explicit statement a developer laptop makes.
        logs = new HotRedoLogs(dataRoot, StratumHotStores.datasets(), 3, Codec.NONE);
        stores = new StratumEventStores();
        stores.init(hz, new HotWrites(hz, 1));

        stores.store(event("hot-1", 1000, EPOCH_1));
        stores.store(event("hot-2", 2000, EPOCH_2));
        assertEquals(2, stores.totalCount(),
                "a fixture failed the generated AgentEvent validation and was dropped");

        var authorizer = new com.gitlab.seabeam.controlplane.schematic.services.stratum
                .model.chassis.grpcserver.Authorizer(sink -> { }) {
            @Override
            public TokenWrapper authorize(String fullMethod, Object request) throws ProtoException {
                return null;
            }
        };
        server = NettyServerBuilder.forPort(0)
                .addService(new RecordMap(new StratumRecordMaps(() -> stores, hotReadable::get), authorizer))
                .build()
                .start();
        channel = PullAgentEvent.channel("localhost:" + server.getPort(), true);
        stub = PullAgentEvent.stub(channel, null);
    }

    @AfterAll
    static void down() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (logs != null) {
            logs.close();
        }
        if (hz != null) {
            hz.shutdown();
        }
    }

    private static HotDataset agentEventDataset() {
        return StratumHotStores.datasets().stream()
                .filter(d -> MAP.equals(d.mapName))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "StratumHotStores.datasets() has no descriptor for " + MAP
                                + "; the (spec.store_epoch) declaration did not reach the generator"));
    }

    // -----------------------------------------------------------------------

    /**
     * The declaration produced a descriptor, and it reads the STORE's clock. A
     * descriptor pointed at timestamp_ms would order the restore union by the
     * author's clock — the failure the epoch field exists to prevent.
     */
    @Test
    void theDeclarationGeneratesAReplayableDescriptorReadingTheStoreEpoch() {
        HotDataset d = agentEventDataset();
        assertEquals("store_epoch_ms", d.epochField);
        assertEquals(2, d.minDurableCopies, "the declared default floor");
        AgentEvent e = event("probe", 1000, EPOCH_2);
        assertEquals("probe", d.key.apply(e));
        assertEquals(EPOCH_2, d.epoch.applyAsLong(e),
                "the descriptor must read store_epoch_ms, not the business timestamp");
    }

    /**
     * A write through the generated store is readable back through the generated
     * query plane, filtered on the newly-generated store-epoch range criterion.
     * The control is the second row: a bound that excludes it must exclude it,
     * or the filter is matching everything and proving nothing.
     */
    @Test
    void aWriteThroughTheGeneratedStoreIsReadableBackOverTheStoreEpochRange() {
        PullAgentEvent.Result all = PullAgentEvent.run(
                stub, Query.AgentEventFilter.newBuilder().build(), null, 10);
        assertEquals(2, all.count);

        PullAgentEvent.Result first = PullAgentEvent.run(stub,
                Query.AgentEventFilter.newBuilder()
                        .setStoreEpochMsFrom(EPOCH_1)
                        .setStoreEpochMsTo(EPOCH_1)
                        .build(),
                null, 10);
        assertEquals(1, first.count, "the store-epoch range must cut the set, not pass everything");
        assertEquals("hot-1", first.items.get(0).getEventId());
        assertEquals(EPOCH_1, first.items.get(0).getStoreEpochMs());

        PullAgentEvent.Result none = PullAgentEvent.run(stub,
                Query.AgentEventFilter.newBuilder()
                        .setStoreEpochMsFrom(EPOCH_2 + 1)
                        .build(),
                null, 10);
        assertEquals(0, none.count, "a bound past every row must return nothing");
    }

    /**
     * The restore gate reaches the LIVE read path.
     *
     * <p>It used to guard {@code EventStore.getByKey} and {@code EventStore.query},
     * which nothing calls; the RPCs that actually read the store — this one included —
     * never asked. A node that had replayed only its own redo log therefore answered
     * counts and pages off a store missing every row its peers owned, and the answer
     * looked complete.
     *
     * <p>Both halves, over a real gRPC round trip: closed refuses with UNAVAILABLE
     * (the condition clears on its own, so the caller should retry), and the SAME call
     * with the gate open returns the same two rows the other tests see. Without the
     * second half this would pass just as well against a plane that refused
     * everything.
     */
    @Test
    void theLiveReadPathRefusesUntilTheRestoreGateOpens() {
        hotReadable.set(false);
        try {
            io.grpc.StatusRuntimeException refused = org.junit.jupiter.api.Assertions.assertThrows(
                    io.grpc.StatusRuntimeException.class,
                    () -> PullAgentEvent.run(stub, Query.AgentEventFilter.newBuilder().build(), null, 10),
                    "the read path served a store that had not finished restoring");
            assertEquals(io.grpc.Status.Code.UNAVAILABLE, refused.getStatus().getCode(),
                    "a restoring store is retryable, not an error the caller must handle: "
                            + refused.getStatus());
        } finally {
            hotReadable.set(true);
        }

        assertEquals(2, PullAgentEvent.run(
                stub, Query.AgentEventFilter.newBuilder().build(), null, 10).count,
                "the gate stayed shut after the restore completed");
    }

    /**
     * The same write reached this node's redo log through the generated
     * descriptor's encoder, and replays back out of it — the durability leg of
     * the hot tier, over a declared record rather than a hand-built descriptor.
     */
    @Test
    void theSameWriteIsInThisNodesRedoLogAndReplaysBack() throws Exception {
        logs.awaitAppends(10_000);
        assertEquals(0, logs.appendFailureCount(), "a redo append failed");
        assertTrue(logs.appendedCount() >= 2,
                "expected both writes appended, got " + logs.appendedCount());

        HotDataset d = agentEventDataset();
        IMap<String, Message> restored = hz.getMap("restored." + MAP);
        restored.clear();
        HotRestore.Result r = HotRestore.replay(logs.log(MAP), d, restored);

        assertTrue(r.diverged.isEmpty(), "diverged keys: " + r.diverged);
        assertEquals(2, restored.size(), "replay applied " + r.applied + ", skipped " + r.skipped);
        AgentEvent replayed = (AgentEvent) restored.get("hot-1");
        assertNotNull(replayed, "hot-1 did not come back out of the redo log");
        assertEquals(EPOCH_1, replayed.getStoreEpochMs());
        assertEquals(1000, replayed.getTimestampMs());
        assertEquals(event("hot-1", 1000, EPOCH_1), replayed,
                "the record must round-trip byte-identical through the generated JSON codec");
    }
}
