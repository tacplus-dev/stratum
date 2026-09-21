// Copyright (c) 2026 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotWrites;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.token.TokenWrapper;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.s2sm.internal.grpc.servers.vservices.query.RecordMap;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.query.RecordMapGrpc;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.query.Query;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.errors.ProtoException;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentEvent;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.HookPhase;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorEventType;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorType;

/**
 * End-to-end gate over the GENERATED RecordMap query plane for AgentEvent.
 *
 * Every layer under test is generated: the client stub (RecordMapGrpc), the
 * server wrapper (s2sm .../stratum/RecordMap), the handler
 * (StratumRecordMaps, emitted by stratum-codegen from {@code spec.record} +
 * {@code spec.query_field}), and the typed store (StratumEventStores). The
 * fixtures and the assertions are the only hand-written parts, and the client
 * half is {@link PullAgentEvent#run} itself — the shipped code path, not a copy.
 *
 * What this does NOT prove, stated plainly because the distinction matters:
 *
 *  - No live cluster. The server here is a bare gRPC server in this JVM holding
 *    the generated wrapper. A deployed stratum is brought up by the generated
 *    ServiceManager with TLS, kernel-sourced config and a real key source; none
 *    of that runs here.
 *  - No auth. The wrapper demands an Authorizer and cannot be built without one,
 *    so the harness passes one that authorizes everything. Whether a real caller
 *    is entitled to RecordMap/*AgentEvent is untested here.
 *  - No ingest path. Fixtures are written straight into the generated store; the
 *    EventBus subscribe → store leg is not exercised.
 */
class PullAgentEventE2ETest {

    private static final String SPIFFE_A = "spiffe://cav/tenants/t1/users/a";
    private static final String SPIFFE_B = "spiffe://cav/tenants/t1/users/b";

    private static HazelcastInstance hz;
    private static Server server;
    private static ManagedChannel channel;
    private static RecordMapGrpc.RecordMapBlockingStub stub;

    /**
     * Six events: two sessions, two principals, two agents, timestamps
     * 1000..6000. Chosen so every predicate under test cuts the set somewhere
     * strictly between "nothing" and "everything" — a filter that happens to
     * match all six would pass vacuously.
     */
    private static AgentEvent event(String eventId, String sessionId, String agentId,
                                    String spiffeId, long ts) {
        return AgentEvent.newBuilder()
                .setEventId(eventId)
                .setSessionId(sessionId)
                .setAgentId(agentId)
                .setSpiffeId(spiffeId)
                .setTimestampMs(ts)
                .setEventType(SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE)
                .setSensorType(SensorType.SENSOR_TYPE_CLAUDE_CODE)
                .setHookPhase(HookPhase.HOOK_PHASE_POST)
                .build();
    }

    private static final List<AgentEvent> FIXTURES = List.of(
            event("e1", "s1", "ag1", SPIFFE_A, 1000),
            event("e2", "s1", "ag1", SPIFFE_A, 2000),
            event("e3", "s1", "ag2", SPIFFE_B, 3000),
            event("e4", "s2", "ag2", SPIFFE_B, 4000),
            event("e5", "s2", "ag3", SPIFFE_A, 5000),
            event("e6", "s2", "ag3", SPIFFE_A, 6000));

    @BeforeAll
    static void up() throws Exception {
        // A single-member cluster with both join mechanisms off: this test must
        // not discover, or be discovered by, anything else on the machine.
        Config cfg = new Config();
        cfg.setClusterName("stratum-recordmap-e2e-" + UUID.randomUUID());
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().setPortAutoIncrement(true);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(cfg);

        StratumEventStores stores = new StratumEventStores();
        // Single-member test cluster, so the durability floor is declared as 1
        // rather than degraded to 1 — the same explicit statement a developer
        // laptop makes. Without it every write here would be refused, which is
        // exactly what the gate is for.
        stores.init(hz, new HotWrites(hz, 1));
        // The plane's second leg. AgentEvent is RESIDENCY_HOT (as every record type
        // is until one declares otherwise), so this store holds no dataset, opens no
        // directory and answers no disk leg — but the plane still takes it, which is
        // what keeps the wiring compiled rather than hypothetical.
        for (AgentEvent e : FIXTURES) {
            stores.store(e);
        }
        // store() logs and swallows a record that fails generated validation, so
        // "it returned true" proves nothing — check the store actually holds them.
        assertEquals(FIXTURES.size(), stores.totalCount(),
                "a fixture failed the generated AgentEvent validation and was dropped");

        // The wrapper's authorizer is mandatory by construction. This one allows
        // everything: the auth path is out of scope here and stubbing it is
        // visible rather than silent.
        var authorizer = new com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum
                .model.chassis.grpcserver.Authorizer(sink -> { }) {
            @Override
            public TokenWrapper authorize(String fullMethod, Object request) throws ProtoException {
                // Allow everything. Returning null is the documented "nothing
                // verified an identity on this call" case: the wrapper feeds
                // this to Authorizer.identityClaims, which maps null to an
                // empty claim set rather than dereferencing it.
                return null;
            }
        };

        server = NettyServerBuilder.forPort(0)
                .addService(new RecordMap(
                        // The hot tier's restore gate, open: this fixture has no cold
                        // start to wait for. HotAgentEventE2ETest drives it closed.
                        new StratumRecordMaps(() -> stores, () -> true), authorizer))
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
        if (hz != null) {
            hz.shutdown();
        }
    }

    private static Query.AgentEventFilter.Builder filter() {
        return Query.AgentEventFilter.newBuilder();
    }

    /** Runs the shipped lifecycle with a page size of 2, so paging is walked, not skipped. */
    private static PullAgentEvent.Result pull(Query.AgentEventFilter f) {
        return PullAgentEvent.run(stub, f, null, 2);
    }

    private static List<String> ids(PullAgentEvent.Result r) {
        return r.items.stream().map(AgentEvent::getEventId).sorted().collect(Collectors.toList());
    }

    // -----------------------------------------------------------------------

    @Test
    void unfilteredQueryReturnsEveryRecordAndPages() {
        PullAgentEvent.Result r = pull(filter().build());
        assertEquals(List.of("e1", "e2", "e3", "e4", "e5", "e6"), ids(r));
        assertEquals(6, r.count);
        assertEquals(6, r.totalCount);
        assertEquals(3, r.pages, "six rows at page size 2 must walk three pages");
    }

    @Test
    void eqFilterOnSessionIdReturnsOnlyMatches() {
        PullAgentEvent.Result r = pull(filter().setSessionId("s1").build());
        assertEquals(List.of("e1", "e2", "e3"), ids(r));
        assertTrue(r.items.stream().allMatch(e -> e.getSessionId().equals("s1")));
        assertEquals(3, r.count);
    }

    @Test
    void eqFilterOnSpiffeIdReturnsOnlyMatches() {
        PullAgentEvent.Result r = pull(filter().setSpiffeId(SPIFFE_B).build());
        assertEquals(List.of("e3", "e4"), ids(r));
        assertTrue(r.items.stream().allMatch(e -> e.getSpiffeId().equals(SPIFFE_B)));
        assertEquals(2, r.count);
    }

    @Test
    void eqFilterOnAgentIdReturnsOnlyMatches() {
        PullAgentEvent.Result r = pull(filter().setAgentId("ag3").build());
        assertEquals(List.of("e5", "e6"), ids(r));
        assertEquals(2, r.count);
    }

    @Test
    void eqFiltersCompose() {
        PullAgentEvent.Result r = pull(filter().setSessionId("s2").setSpiffeId(SPIFFE_A).build());
        assertEquals(List.of("e5", "e6"), ids(r));
    }

    /** RANGE, both directions and closed — the same pattern as occurredAtMsFrom/To. */
    @Test
    void rangeFilterOnTimestampMs() {
        assertEquals(List.of("e4", "e5", "e6"), ids(pull(filter().setTimestampMsFrom(4000).build())),
                "from is inclusive");
        assertEquals(List.of("e1", "e2"), ids(pull(filter().setTimestampMsTo(2000).build())),
                "to is inclusive");
        assertEquals(List.of("e2", "e3", "e4"),
                ids(pull(filter().setTimestampMsFrom(2000).setTimestampMsTo(4000).build())));
    }

    @Test
    void rangeAndEqCompose() {
        PullAgentEvent.Result r = pull(filter().setSessionId("s2").setTimestampMsFrom(5000).build());
        assertEquals(List.of("e5", "e6"), ids(r));
    }

    /** Count is the server's own size, so it has to agree with what paging returns. */
    @Test
    void countAgreesWithPagedResultCount() {
        for (Query.AgentEventFilter f : List.of(
                filter().build(),
                filter().setSessionId("s1").build(),
                filter().setAgentId("ag2").build(),
                filter().setSpiffeId(SPIFFE_A).setTimestampMsFrom(5000).build(),
                filter().setTimestampMsFrom(2000).setTimestampMsTo(4000).build())) {
            PullAgentEvent.Result r = pull(f);
            assertEquals(r.count, r.items.size(), "Count disagreed with the paged result for " + f);
            assertEquals(r.count, r.totalCount, "total_count disagreed with Count for " + f);
        }
    }

    /**
     * A predicate that matches nothing must return an empty result, not an error
     * and not everything. Without this the filter assertions above could pass
     * against a plane that silently ignores predicates it does not understand.
     */
    @Test
    void filterMatchingNothingReturnsZeroNotAnError() {
        PullAgentEvent.Result r = pull(filter().setSessionId("no-such-session").build());
        assertEquals(0, r.count);
        assertEquals(0, r.items.size());
        assertEquals(1, r.pages, "an empty result is still one page, not zero and not an error");

        PullAgentEvent.Result byTime = pull(filter().setTimestampMsFrom(99_000).build());
        assertEquals(0, byTime.count);

        PullAgentEvent.Result impossible =
                pull(filter().setTimestampMsFrom(5000).setTimestampMsTo(1000).build());
        assertEquals(0, impossible.count, "from > to is empty, not an error");
    }

    /** Refine replaces the pinned predicate; Count and Pull must follow it. */
    @Test
    void refineNarrowsTheOpenQuery() {
        PullAgentEvent.Result r = PullAgentEvent.run(stub,
                filter().build(),
                filter().setSessionId("s2").setAgentId("ag3").build(),
                2);
        assertEquals(2, r.refinedSize, "RefineAgentEvent reports the refined size");
        assertEquals(2, r.count, "Count after Refine reflects the refined predicate");
        assertEquals(List.of("e5", "e6"), ids(r));
    }

    /** The handle is released: operating on a closed query is NOT_FOUND, not a silent match-all. */
    @Test
    void closeReleasesTheHandle() {
        Query.QueryHandle h = stub.createAgentEventQuery(
                Query.OpenAgentEventQueryRequest.newBuilder().setFilter(filter().build()).build());
        assertEquals(6, stub.getAgentEventCount(h).getCount());
        stub.deleteAgentEventQuery(h);
        StatusRuntimeException e =
                assertThrows(StatusRuntimeException.class, () -> stub.getAgentEventCount(h));
        assertTrue(e.getMessage().contains("unknown query_id"), e.getMessage());
    }

    /**
     * The CLI's own surface: flags land on the generated filter, and nothing is
     * silently defaulted. Everything below this point is the process wrapper —
     * the query legs above are the plane itself.
     */
    @Test
    void argsMapOntoTheGeneratedFilter() {
        PullAgentEvent.Args a = PullAgentEvent.parseArgs(new String[] {
                "--target", "h:1", "--plaintext", "--page-size", "7",
                "--session-id", "s2", "--spiffe-id", SPIFFE_A,
                "--agent-id", "ag3", "--from-ms", "1000", "--to-ms", "9000",
                "--refine-session-id", "s1" });
        assertEquals("h:1", a.target);
        assertTrue(a.plaintext);
        assertEquals(7, a.pageSize);

        Query.AgentEventFilter f = a.filter.build();
        assertEquals("s2", f.getSessionId());
        assertEquals(SPIFFE_A, f.getSpiffeId());
        assertEquals("ag3", f.getAgentId());
        assertEquals(1000, f.getTimestampMsFrom());
        assertEquals(9000, f.getTimestampMsTo());
        assertEquals("s1", a.refine.build().getSessionId());

        // An unmentioned criterion must be ABSENT, not zero: the generated filter
        // fields are optional, and a present-but-default value is a predicate.
        Query.AgentEventFilter empty =
                PullAgentEvent.parseArgs(new String[] {"--target", "h:1"}).filter.build();
        assertFalse(empty.hasSessionId());
        assertFalse(empty.hasTimestampMsFrom());
        assertNull(PullAgentEvent.parseArgs(new String[] {"--target", "h:1"}).refine);
    }

    @Test
    void jsonOutputCarriesTheCountsAndTheRows() throws Exception {
        String json = PullAgentEvent.toJson(pull(filter().setAgentId("ag3").build()),
                PullAgentEvent.filterFieldNames());
        assertTrue(json.contains("\"count\":2"), json);
        assertTrue(json.contains("\"totalCount\":2"), json);
        assertTrue(json.contains("\"refinedSize\":-1"), json);
        assertTrue(json.contains("\"eventId\":\"e5\""), json);
        assertTrue(json.contains("\"sessionId\""), json);
    }

    /**
     * The filter surface is exactly the (spec.query_field) annotations, and
     * nothing else. AgentEvent carries no (spec.redact) field today; if one is
     * ever added it must not become filterable, and this list is where that
     * shows up.
     *
     * storeEpochMsFrom/To are the store-epoch range: (spec.store_epoch) requires
     * QUERY_OP_RANGE on its field, so declaring the epoch necessarily widens this
     * surface by exactly those two bounds.
     */
    @Test
    void filterSurfaceIsExactlyTheAnnotatedFields() {
        assertEquals(
                List.of("spiffeId", "timestampMsFrom", "timestampMsTo", "sessionId", "agentId",
                        "storeEpochMsFrom", "storeEpochMsTo"),
                PullAgentEvent.filterFieldNames(),
                "the generated AgentEventFilter surface changed");
    }
}
