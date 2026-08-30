// Copyright (c) 2026 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotWrites;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmDataset;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmFilter;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmResidency;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmStore;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.token.TokenWrapper;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.s2sm.internal.grpc.servers.vservices.query.RecordMap;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.errors.ProtoException;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentSessionState;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SessionStatus;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.query.Query;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.query.RecordMapGrpc;

/**
 * The warm tier over a real MUTABLE record: {@code agentsensor.AgentSessionState},
 * which declares {@code (spec.record).residency = {window: RESIDENCY_WARM}} and NO
 * {@code mutability}, i.e. {@code MUTABILITY_CRUD}.
 *
 * <p>This is the corner of the residency/mutability square the fleet had no
 * inhabitant for. {@code AgentEvent} is hot; {@code AccountingRecord} is warm and
 * append-only, and both are write-once by their own nature — an event and a
 * completed RPC have no later state. So nothing exercised the store epoch actually
 * MOVING, which is the whole reason it is a last-write clock rather than a creation
 * stamp, and P4b/P4c both recorded drill 8 and half of drill 2 as blocked on
 * exactly that.
 *
 * <h2>What an "update" is here</h2>
 * There is no row-mutating RPC and there is not meant to be one — see
 * tacplus's {@code mutability_validate.go}: rows reach the store by EventBus
 * publish, so an update is a republish of the same {@code session_id}. The
 * generated subscription dispatch restamps {@code store_epoch_ms} on every one of
 * them (unconditionally — the wrapper owns the clock), which is what a test fixture
 * is standing in for when it builds a second row with the same key and a newer
 * epoch. Ingest here is {@code stores.store} then {@code warm.enqueue}, in
 * {@code EventStore.store}'s own order, exactly as {@code WarmEvictionE2ETest} does.
 *
 * <h2>Boundary semantics, as the code actually defines them</h2>
 * {@code WarmResidency.inMemory} owns {@code epoch >= boundary}; {@code
 * WarmStore.diskRange} stops the disk leg at {@code boundary} EXCLUSIVE. The
 * boundary value itself therefore belongs to memory and to nothing else, and the
 * two legs neither overlap nor leave a gap at it —
 * {@link #theBoundaryValueBelongsToMemoryAndTheDiskLegStopsBelowIt()} pins that
 * rather than assuming it.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WarmMutableSessionStateE2ETest {

    private static final String DATASET = "agentsensor.AgentSessionState";
    private static final String APPEND_ONLY_DATASET = "accountingrecord.AccountingRecord";

    /** A one-day residency window, so the 2025 fixtures are aged without waiting. */
    private static final long WINDOW_SECONDS = 24L * 60 * 60;

    private static final long EPOCH_MAR = Instant.parse("2025-03-15T00:00:00Z").toEpochMilli();
    private static final long EPOCH_JUN = Instant.parse("2025-06-15T00:00:00Z").toEpochMilli();
    private static final long EPOCH_FRESH = System.currentTimeMillis();

    /**
     * The business clock, fixed at the session's first publish. Deliberately far
     * from every store epoch below: an assertion that reads the wrong clock cannot
     * pass by coincidence.
     */
    private static final long STARTED_AT = Instant.parse("2025-01-02T03:04:05Z").toEpochMilli();

    private static HazelcastInstance hz;
    private static StratumEventStores stores;
    private static WarmStore warm;
    private static Server server;
    private static ManagedChannel channel;
    private static RecordMapGrpc.RecordMapBlockingStub stub;

    private static AgentSessionState row(String id, long storeEpochMs, SessionStatus status, int tools) {
        return AgentSessionState.newBuilder()
                .setSessionId(id)
                .setSpiffeId("spiffe://cav/tenants/t1/users/joeuser")
                .setStartedAtMs(STARTED_AT)
                .setStatus(status)
                .setToolUseCount(tools)
                .setStoreEpochMs(storeEpochMs)
                .build();
    }

    /** One republish of an existing session — the write path an update takes. */
    private static void publish(AgentSessionState r) throws Exception {
        assertTrue(stores.store(r), "the typed hot store did not claim the record");
        assertTrue(warm.enqueue(r), "the warm tier did not claim a declared RESIDENCY_WARM record");
        warm.awaitWrites(10_000);
        assertEquals(0, warm.writeFailureCount(), "a warm disk write failed");
    }

    /** The GENERATED warm descriptor for this record — the point of the test. */
    private static WarmDataset dataset(String name) {
        return StratumWarmStores.datasets(WINDOW_SECONDS).stream()
                .filter(d -> name.equals(d.name))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "StratumWarmStores.datasets() has no descriptor for " + name
                                + "; the RESIDENCY_WARM declaration did not reach the generator"));
    }

    @BeforeAll
    static void up(@TempDir Path dataRoot) throws Exception {
        Config cfg = new Config();
        cfg.setClusterName("stratum-warm-mutable-e2e-" + UUID.randomUUID());
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().setPortAutoIncrement(true);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(cfg);

        stores = new StratumEventStores();
        stores.init(hz, new HotWrites(hz, 0, 1));
        warm = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));

        // Two aged sessions and one inside the window. All three go through both
        // legs; the partition between them is the code's, not the fixture's.
        publish(row("sess-aged", EPOCH_MAR, SessionStatus.SESSION_STATUS_ACTIVE, 3));
        publish(row("sess-cold", EPOCH_JUN, SessionStatus.SESSION_STATUS_ENDED, 9));
        publish(row("sess-fresh", EPOCH_FRESH, SessionStatus.SESSION_STATUS_ACTIVE, 1));
        assertEquals(3, warm.writtenCount());

        var authorizer = new com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum
                .model.chassis.grpcserver.Authorizer(sink -> { }) {
            @Override
            public TokenWrapper authorize(String fullMethod, Object request) throws ProtoException {
                return null;
            }
        };
        server = NettyServerBuilder.forPort(0)
                .addService(new RecordMap(new StratumRecordMaps(() -> stores, () -> warm), authorizer))
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
        if (warm != null) {
            warm.close();
        }
        if (hz != null) {
            hz.shutdown();
        }
    }

    // ----- the generated RPC family, over an unfiltered handle -----------------

    private static List<AgentSessionState> served() {
        Query.QueryHandle handle = stub.createAgentSessionStateQuery(
                Query.OpenAgentSessionStateQueryRequest.newBuilder()
                        .setFilter(Query.AgentSessionStateFilter.newBuilder().build())
                        .setStanding(false)
                        .build());
        try {
            List<AgentSessionState> rows = new ArrayList<>();
            String pageToken = "";
            while (true) {
                Query.PullAgentSessionStateResponse resp = stub.listAgentSessionState(
                        Query.PullAgentSessionStateRequest.newBuilder()
                                .setQueryId(handle.getQueryId())
                                .setPageSize(100)
                                .setPageToken(pageToken)
                                .build());
                rows.addAll(resp.getItemsList());
                if (resp.getNextPageToken().isEmpty()) {
                    rows.sort(java.util.Comparator.comparing(AgentSessionState::getSessionId));
                    return rows;
                }
                pageToken = resp.getNextPageToken();
            }
        } finally {
            stub.deleteAgentSessionStateQuery(handle);
        }
    }

    private static List<String> servedIds() {
        List<String> ids = new ArrayList<>();
        for (AgentSessionState r : served()) {
            ids.add(r.getSessionId());
        }
        return ids;
    }

    private static AgentSessionState servedRow(String id) {
        for (AgentSessionState r : served()) {
            if (r.getSessionId().equals(id)) {
                return r;
            }
        }
        throw new AssertionError("the generated List RPC did not serve " + id);
    }

    /** The generated merged count over the same unfiltered handle. */
    private static long servedCount() {
        Query.QueryHandle handle = stub.createAgentSessionStateQuery(
                Query.OpenAgentSessionStateQueryRequest.newBuilder()
                        .setFilter(Query.AgentSessionStateFilter.newBuilder().build())
                        .setStanding(false)
                        .build());
        try {
            return stub.getAgentSessionStateCount(handle).getCount();
        } finally {
            stub.deleteAgentSessionStateQuery(handle);
        }
    }

    /** The disk leg alone, for one session. */
    private static long onDisk(String sessionId) throws Exception {
        return warm.count(DATASET, new WarmFilter().eq("session_id", sessionId));
    }

    // -----------------------------------------------------------------------

    /**
     * The declaration reached the generator, and the ONE thing that differs between
     * this record and P4b's is the one flag that is supposed to differ.
     *
     * <p>Asserted against the append-only descriptor in the same generated list, so
     * a generator that hardcoded either value fails here rather than producing a
     * dataset that quietly behaves like the other corner.
     */
    @Test
    @Order(1)
    void theMutableWarmDeclarationReachesTheGeneratedDescriptor() {
        WarmDataset mutable = dataset(DATASET);
        assertFalse(mutable.appendOnly,
                "a record declaring RESIDENCY_WARM and no mutability is MUTABILITY_CRUD;"
                        + " the generated descriptor says it is append-only");
        assertEquals("store_epoch_ms", mutable.epochField);
        assertEquals(WINDOW_SECONDS * 1000L, mutable.windowMs);
        assertTrue(dataset(APPEND_ONLY_DATASET).appendOnly,
                "the append-only record's flag moved with it; the two corners are not distinguished");
    }

    /**
     * Where the boundary actually is, read off both legs rather than assumed.
     *
     * <p>{@code WarmResidency.inMemory} owns {@code epoch >= boundary}; {@code
     * WarmStore.diskRange} makes {@code boundary} the leg's EXCLUSIVE ceiling. So
     * the boundary value itself is memory's and only memory's, {@code boundary - 1}
     * is disk's and only disk's, and there is neither an overlap nor a hole between
     * them. Everything drill 8 asserts about "keeps or resets residency correctly"
     * is measured against this, so it is pinned first.
     */
    @Test
    @Order(2)
    void theBoundaryValueBelongsToMemoryAndTheDiskLegStopsBelowIt() throws Exception {
        WarmResidency res = warm.residency(DATASET);
        long b = res.boundaryMs;

        assertTrue(res.inMemory("k", b), "the boundary value is inside the memory window");
        assertFalse(res.evictable("k", b));
        assertFalse(res.inMemory("k", b - 1), "one millisecond below the boundary is the disk leg's");
        assertTrue(res.evictable("k", b - 1));

        // The disk side of the same cut: a caller whose own epoch floor is at the
        // boundary leaves the leg an empty interval, so it is skipped outright.
        assertEquals(0, warm.count(DATASET, new WarmFilter().range("store_epoch_ms", b, null)),
                "the disk leg answered at or above the boundary, which is memory's side");
        assertEquals(2, warm.count(DATASET, new WarmFilter()),
                "the disk leg must hold exactly the two aged sessions");
    }

    /**
     * Drill 8: a row's store epoch refreshes on update, and that PUTS IT BACK inside
     * the residency window.
     *
     * <p>The aged session is proven to be the disk leg's before the update — that is
     * what makes this a refresh and not a no-op. After the republish the store epoch
     * has moved and {@code started_at_ms} has not: two clocks, only one of them
     * store-owned, which on an append-only record could never be told apart because
     * they are written together and never diverge.
     *
     * <p>"Correctly" here is what the code defines, not what one might expect: the
     * window is measured from the LAST WRITE, so a row that was aged out is inside
     * the window again the instant it is written, and the disk leg — whose ceiling
     * is the boundary — stops answering for it. Eviction agrees, because
     * {@link WarmResidency#evictable} is the exact negation of the same cut.
     */
    @Test
    @Order(3)
    void updatingAnAgedRowRefreshesItsStoreEpochAndPutsItBackInsideTheWindow() throws Exception {
        assertFalse(warm.residency(DATASET).inMemory("sess-aged", EPOCH_MAR),
                "the fixture is only a refresh if the row starts on the disk side");
        assertEquals(1, onDisk("sess-aged"));

        long before = System.currentTimeMillis();
        publish(row("sess-aged", System.currentTimeMillis(), SessionStatus.SESSION_STATUS_IDLE, 7));

        AgentSessionState stored = stores.agentsensorAgentSessionState().get("sess-aged");
        assertTrue(stored.getStoreEpochMs() >= before,
                "the store epoch did not move on update; the residency window measures from it");
        assertEquals(STARTED_AT, stored.getStartedAtMs(),
                "the business clock moved with the store clock; they are two different clocks");
        assertEquals(SessionStatus.SESSION_STATUS_IDLE, stored.getStatus());
        assertEquals(7, stored.getToolUseCount());

        WarmResidency res = warm.residency(DATASET);
        assertTrue(res.inMemory("sess-aged", stored.getStoreEpochMs()),
                "a row written just now is outside its own residency window");
        assertFalse(res.evictable("sess-aged", stored.getStoreEpochMs()));
        assertEquals(0, onDisk("sess-aged"),
                "the disk leg still answers for a row whose epoch is back inside the window");

        // The reclaim sweep reads the same cut, so the refreshed row survives it.
        WarmEviction.sweep(hz, warm);
        assertTrue(stores.agentsensorAgentSessionState().containsKey("sess-aged"),
                "the sweep evicted a row whose update had just put it back inside the window");
    }

    /**
     * Drill 2's remaining half: a row updated near the boundary is counted exactly
     * once — not lost between the legs, not served by both — through the real
     * generated {@code Get<T>Count} / {@code List<T>}.
     *
     * <p>The row under test is the one {@link
     * #updatingAnAgedRowRefreshesItsStoreEpochAndPutsItBackInsideTheWindow()} moved
     * across the boundary: its copy on disk was written when it was aged, and its
     * copy in memory is the update. Both legs hold the key; only one may answer for
     * it. And the row that comes back must be the UPDATED one, or "counted once"
     * would be satisfied by serving the copy the update replaced.
     */
    @Test
    @Order(4)
    void aRowUpdatedAcrossTheBoundaryIsServedAndCountedExactlyOnce() {
        assertEquals(List.of("sess-aged", "sess-cold", "sess-fresh"), servedIds(),
                "an updated row was lost between the legs, or served by both");
        assertEquals(3, servedCount(), "the merged count is not the number of sessions");

        AgentSessionState served = servedRow("sess-aged");
        assertEquals(SessionStatus.SESSION_STATUS_IDLE, served.getStatus(),
                "the pre-update copy was served; the disk leg answered for a row memory owns");
        assertEquals(7, served.getToolUseCount());
    }

    /**
     * The same count, observed on the other side of the asynchronous disk write.
     *
     * <p>A warm write is acked on in-memory replication and commits to disk after
     * it, so between the two the key's disk copy is the PREVIOUS one. That copy is
     * aged and the new one is not, so without {@code WarmIndex.pendingKeys()}
     * excluding the key, both legs would match and the count would be 4. The
     * property is that the answer does not depend on which side of the commit the
     * query lands on, so it is asserted at both.
     */
    @Test
    @Order(5)
    void theCountDoesNotDependOnWhetherTheDiskWriteHasCommittedYet() throws Exception {
        assertEquals(1, onDisk("sess-cold"), "the fixture starts with sess-cold on the disk leg");

        assertTrue(stores.store(row("sess-cold", System.currentTimeMillis(),
                SessionStatus.SESSION_STATUS_ACTIVE, 11)));
        assertTrue(warm.enqueue(stores.agentsensorAgentSessionState().get("sess-cold")));
        assertEquals(3, servedCount(), "the count doubled while the disk write was in flight");

        warm.awaitWrites(10_000);
        assertEquals(3, servedCount(), "the count changed once the disk write committed");
        assertEquals(List.of("sess-aged", "sess-cold", "sess-fresh"), servedIds());
        assertEquals(0, onDisk("sess-cold"),
                "the pre-update copy is still on the disk leg after the update committed");
    }

    /**
     * A CROSS-SLICE update leaves one copy on disk, not two.
     *
     * <p>Slices are named for the store epoch, so an update that moves the epoch
     * into a different month cannot be one {@code updateDocument} — {@code
     * WarmIndex.write} writes the new copy first and then deletes the stale one from
     * the sealed slice. Both epochs here are aged, so BOTH copies would be inside
     * the disk leg's range if the delete did not happen, and the count would be 2.
     *
     * <p>This is the exact contrast with the append-only dataset, where P4b proved
     * the same key written twice correctly leaves two documents.
     */
    @Test
    @Order(6)
    void aCrossSliceUpdateLeavesOneCopyOnDiskNotTwo() throws Exception {
        publish(row("sess-cold", EPOCH_MAR, SessionStatus.SESSION_STATUS_ENDED, 12));

        assertEquals(1, onDisk("sess-cold"),
                "the cross-slice update appended a second copy instead of replacing the first");
        assertEquals(List.of("sess-aged", "sess-cold", "sess-fresh"), servedIds());
        assertEquals(3, servedCount());
        assertEquals(SessionStatus.SESSION_STATUS_ENDED, servedRow("sess-cold").getStatus(),
                "the stale copy answered; the update did not reach the slice the disk leg reads");
        assertEquals(12, servedRow("sess-cold").getToolUseCount());
    }
}
