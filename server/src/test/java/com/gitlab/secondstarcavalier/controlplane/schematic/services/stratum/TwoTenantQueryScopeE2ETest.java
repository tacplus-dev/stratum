// Copyright (c) 2026 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotWrites;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmStore;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.grpcserver.Authorizer;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.servicemanager.Auth;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.token.TokenWrapper;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.s2sm.internal.grpc.servers.vservices.query.RecordMap;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.errors.ProtoException;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentEvent;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.HookPhase;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorEventType;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorType;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.query.Query;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.query.RecordMapGrpc;

/**
 * The tenant boundary of the generated record query plane, driven by TWO callers
 * over real gRPC against one store holding both their rows.
 *
 * <p>These are security tests, not feature tests: every RPC shape the plane
 * exposes over a query handle gets a two-tenant negative case, because the bug
 * they pin was one unscoped {@code create<T>Query} reached through six different
 * RPCs. Tenant A opens the broadest query it can — no filter at all — and each
 * shape must answer with A's rows and A's count alone.
 *
 * <p>What that protects, stated concretely. The generated wrapper checks every
 * returned row against the caller's own namespace claim and calls
 * {@code Runtime.getRuntime().halt(1)} when one does not match, so a query that
 * is not scoped at the handle is not a leak on {@code List}/{@code Sample}/
 * {@code Watch} — it is an abrupt JVM kill any entitled caller can trigger on
 * demand. {@code Count}/{@code SubscribeCount} have no such backstop and would
 * simply answer with the other tenant's row count. Both consequences come from
 * the same place, and both are closed by pinning the handle at create.
 *
 * <p>The happy-path half matters just as much: a same-tenant query must still
 * return that tenant's rows. If it did not, this JVM would already have been
 * halted by the backstop and the run would end with no report at all — so the
 * suite completing IS the assertion that the backstop never fired.
 *
 * <p>Auth is stubbed at the one seam a unit test cannot reach: the signature and
 * audience check. Everything past that point — the header, the verified-token
 * slot, the claim extraction, the wrapper, the handler — is the shipped path.
 */
class TwoTenantQueryScopeE2ETest {

    private static final String TENANT_A = "/tenants/a";
    private static final String TENANT_B = "/tenants/b";
    private static final String SYSTEM = "/system";

    private static HazelcastInstance hz;
    private static StratumEventStores stores;
    private static Server server;
    private static ManagedChannel channel;

    /** One stub per caller identity. The namespace claim is the only difference. */
    private static RecordMapGrpc.RecordMapBlockingStub tenantA;
    private static RecordMapGrpc.RecordMapBlockingStub tenantB;
    private static RecordMapGrpc.RecordMapBlockingStub system;
    private static RecordMapGrpc.RecordMapBlockingStub anonymous;

    /**
     * A token carrying one namespace claim, in the shape
     * {@code header.payload.signature} the chassis reads claims out of.
     *
     * Unsigned on purpose: the stub Authorizer below is what would verify a
     * signature in the mesh, and this test is about what the plane does with a
     * claim AFTER verification, not about verification itself.
     */
    private static String token(String namespace) {
        Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
        String header = b64.encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8));
        String payload = b64.encodeToString(("{\"namespace\":\"" + namespace + "\","
                + "\"sub\":\"spiffe://cav" + namespace + "/services/reporter\"}")
                .getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + ".stub-signature";
    }

    private static AgentEvent event(String eventId, String namespace, long timestampMs) {
        return AgentEvent.newBuilder()
                .setEventId(eventId)
                .setSessionId("s1")
                .setAgentId("ag1")
                .setSpiffeId("spiffe://cav" + namespace + "/users/a")
                .setNamespace(namespace)
                .setTimestampMs(timestampMs)
                .setStoreEpochMs(1_700_000_000_000L)
                .setEventType(SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE)
                .setSensorType(SensorType.SENSOR_TYPE_CLAUDE_CODE)
                .setHookPhase(HookPhase.HOOK_PHASE_POST)
                .build();
    }

    @BeforeAll
    static void up() throws Exception {
        Config cfg = new Config();
        cfg.setClusterName("stratum-two-tenant-e2e-" + UUID.randomUUID());
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().setPortAutoIncrement(true);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(cfg);

        stores = new StratumEventStores();
        stores.init(hz, new HotWrites(hz, 1));
        stores.store(event("a-1", TENANT_A, 1000));
        stores.store(event("a-2", TENANT_A, 2000));
        stores.store(event("b-1", TENANT_B, 3000));
        stores.store(event("b-2", TENANT_B, 4000));
        stores.store(event("b-3", TENANT_B, 5000));
        assertEquals(5, stores.totalCount(),
                "a fixture failed the generated AgentEvent validation and was dropped");

        // Verifies nothing; records whatever bearer token the call carried into the
        // slot the real Authorizer fills after it HAS verified one. A call with no
        // authorization header records nothing, which is the unauthenticated case.
        Authorizer authorizer = new Authorizer(sink -> { }) {
            @Override
            public TokenWrapper authorize(String fullMethod, Object request) throws ProtoException {
                String header = Authorizer.AUTH_HEADER_CTX_KEY.get();
                if (header == null || header.isEmpty()) {
                    return null;
                }
                TokenWrapper tw = TokenWrapper.parse(header.replaceFirst("^Bearer ", ""));
                Auth.recordVerifiedToken(tw);
                return tw;
            }
        };

        WarmStore warm = new WarmStore(Path.of("target", "warm-two-tenant-e2e-unused"), List.of());
        server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(
                        new RecordMap(new StratumRecordMaps(() -> stores, () -> warm, () -> true),
                                authorizer),
                        Authorizer.headerCaptureInterceptor()))
                .build()
                .start();
        channel = PullAgentEvent.channel("localhost:" + server.getPort(), true);
        tenantA = PullAgentEvent.stub(channel, token(TENANT_A));
        tenantB = PullAgentEvent.stub(channel, token(TENANT_B));
        system = PullAgentEvent.stub(channel, token(SYSTEM));
        anonymous = PullAgentEvent.stub(channel, null);
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

    /** The broadest query a caller can ask for: no criterion at all. */
    private static Query.QueryHandle openMatchAll(RecordMapGrpc.RecordMapBlockingStub stub) {
        return stub.createAgentEventQuery(Query.OpenAgentEventQueryRequest.newBuilder()
                .setFilter(Query.AgentEventFilter.newBuilder().build())
                .setStanding(false)
                .build());
    }

    private static Query.SampleAgentEventRequest sample(Query.QueryHandle h) {
        return Query.SampleAgentEventRequest.newBuilder()
                .setQueryId(h.getQueryId()).setSeed(1).setN(100).build();
    }

    private static Query.PullAgentEventRequest pull(Query.QueryHandle h) {
        return Query.PullAgentEventRequest.newBuilder()
                .setQueryId(h.getQueryId()).setPageSize(100).build();
    }

    // -----------------------------------------------------------------------
    // The negative case, one per RPC shape that dereferences a handle.
    // -----------------------------------------------------------------------

    /**
     * The read path. Tenant A opens a match-all query; the page it gets back holds
     * A's two rows and neither of B's three.
     *
     * A failure here does not report as a failure: the wrapper's backstop sees the
     * foreign row first and halts the JVM, so the surefire fork dies mid-run.
     */
    @Test
    void listOverAMatchAllQueryReturnsOnlyTheOpeningTenantsRows() {
        Query.PullAgentEventResponse page = tenantA.listAgentEvent(pull(openMatchAll(tenantA)));

        assertEquals(2, page.getItemsCount(), "tenant A sees exactly its own two rows");
        for (AgentEvent row : page.getItemsList()) {
            assertEquals(TENANT_A, row.getNamespace(),
                    "a row from another tenant reached the page: " + row.getEventId());
        }
    }

    /** The same, through the sample path, which pages off its own scan. */
    @Test
    void sampleOverAMatchAllQueryReturnsOnlyTheOpeningTenantsRows() {
        Query.SampleAgentEventResponse s = tenantA.getAgentEventSample(sample(openMatchAll(tenantA)));

        assertEquals(2, s.getItemsCount(), "tenant A samples exactly its own two rows");
        for (AgentEvent row : s.getItemsList()) {
            assertEquals(TENANT_A, row.getNamespace(),
                    "a row from another tenant reached the sample: " + row.getEventId());
        }
    }

    /** The same, through the watch snapshot. */
    @Test
    void watchOverAMatchAllQueryReturnsOnlyTheOpeningTenantsRows() {
        java.util.Iterator<Query.AgentEventWatchEvent> events =
                tenantA.watchAgentEvent(openMatchAll(tenantA));

        int rows = 0;
        while (events.hasNext()) {
            Query.AgentEventWatchEvent e = events.next();
            if (!e.hasSnapshotItem()) {
                continue;
            }
            rows++;
            assertEquals(TENANT_A, e.getSnapshotItem().getNamespace(),
                    "a row from another tenant reached the watch snapshot: "
                            + e.getSnapshotItem().getEventId());
        }
        assertEquals(2, rows, "tenant A watches exactly its own two rows");
    }

    /**
     * The count path — the leg with NO backstop behind it, and so the one where an
     * unscoped query is a silent cross-tenant read rather than a crash. 2, not 5.
     */
    @Test
    void countOverAMatchAllQueryCountsOnlyTheOpeningTenantsRows() {
        assertEquals(2, tenantA.getAgentEventCount(openMatchAll(tenantA)).getCount(),
                "the count leaked rows the caller may not see");
    }

    /** The same count, through the streaming shape. */
    @Test
    void subscribeCountOverAMatchAllQueryCountsOnlyTheOpeningTenantsRows() {
        java.util.Iterator<Query.RecordCount> counts =
                tenantA.subscribeAgentEventCount(openMatchAll(tenantA));

        assertTrue(counts.hasNext(), "the subscription produced no count at all");
        assertEquals(2, counts.next().getCount(), "the subscribed count leaked rows");
    }

    /**
     * The size a refine reports — the third counting site, and the one a caller
     * reaches by widening an already-open handle back to match-all.
     */
    @Test
    void refiningAQueryBackToMatchAllReportsOnlyTheOpeningTenantsSize() {
        Query.QueryHandle h = tenantA.createAgentEventQuery(
                Query.OpenAgentEventQueryRequest.newBuilder()
                        .setFilter(Query.AgentEventFilter.newBuilder().setAgentId("ag1").build())
                        .setStanding(false)
                        .build());

        long size = tenantA.updateAgentEventQuery(Query.RefineAgentEventRequest.newBuilder()
                .setQueryId(h.getQueryId())
                .setFilter(Query.AgentEventFilter.newBuilder().build())
                .build()).getSize();

        assertEquals(2, size, "a refine widened the handle past the tenant that opened it");
    }

    // -----------------------------------------------------------------------
    // Handle ownership: a query_id is a bare UUID, and holding one is not a grant.
    // -----------------------------------------------------------------------

    /**
     * Tenant B holds tenant A's query_id — the leak the handle registry allows by
     * construction, since the id is a bare UUID shared across both caller
     * surfaces. Every RPC that dereferences it must refuse B.
     */
    @Test
    void anotherTenantCannotUseAHandleItDidNotOpen() {
        Query.QueryHandle h = openMatchAll(tenantA);

        assertRefused(() -> tenantB.listAgentEvent(pull(h)), "list");
        assertRefused(() -> tenantB.getAgentEventSample(sample(h)), "sample");
        assertRefused(() -> tenantB.getAgentEventCount(h), "count");
        assertRefused(() -> tenantB.watchAgentEvent(h).hasNext(), "watch");
        assertRefused(() -> tenantB.subscribeAgentEventCount(h).hasNext(), "subscribeCount");
        assertRefused(() -> tenantB.updateAgentEventQuery(Query.RefineAgentEventRequest.newBuilder()
                .setQueryId(h.getQueryId())
                .setFilter(Query.AgentEventFilter.newBuilder().build())
                .build()), "refine");
        assertRefused(() -> tenantB.deleteAgentEventQuery(h), "close");

        // Refused, and still there: B's failed attempts must not have closed or
        // widened the handle out from under A.
        assertEquals(2, tenantA.getAgentEventCount(h).getCount(),
                "tenant B's refused calls disturbed tenant A's open query");
    }

    /** A caller with no verified namespace claim cannot open a query at all. */
    @Test
    void aCallerWithNoNamespaceClaimCannotOpenAQuery() {
        StatusRuntimeException refused = assertThrows(StatusRuntimeException.class,
                () -> openMatchAll(anonymous),
                "an unauthenticated caller opened a query over namespaced rows");
        assertEquals(Status.Code.PERMISSION_DENIED, refused.getStatus().getCode(),
                "expected the claim check to refuse, got: " + refused.getStatus());
    }

    // -----------------------------------------------------------------------
    // The happy paths the fix must not have broken.
    // -----------------------------------------------------------------------

    /**
     * Tenant B's own legitimately-scoped query still answers with B's rows. Without
     * this the negative cases above would pass just as well against a plane that
     * refused everything.
     */
    @Test
    void aTenantsOwnQueryStillSeesItsOwnRows() {
        Query.QueryHandle h = openMatchAll(tenantB);

        assertEquals(3, tenantB.getAgentEventCount(h).getCount());
        Query.PullAgentEventResponse page = tenantB.listAgentEvent(pull(h));
        assertEquals(3, page.getItemsCount());
        for (AgentEvent row : page.getItemsList()) {
            assertEquals(TENANT_B, row.getNamespace());
        }
        assertEquals(h.getQueryId(), tenantB.deleteAgentEventQuery(h).getQueryId());
    }

    /**
     * A /system caller keeps the fleet-wide read this plane has always served —
     * the convention the generated wrapper's own backstop follows, where
     * {@code /system} is the one claim that is not pinned to a tenant.
     */
    @Test
    void aSystemCallerStillReadsEveryTenant() {
        Query.QueryHandle h = openMatchAll(system);

        assertEquals(5, system.getAgentEventCount(h).getCount(),
                "a /system caller lost the fleet-wide read");
        assertEquals(5, system.listAgentEvent(pull(h)).getItemsCount());
    }

    /** A filter the caller does supply still narrows, inside its own tenant. */
    @Test
    void aCallersOwnFilterStillNarrowsWithinItsTenant() {
        Query.QueryHandle h = tenantB.createAgentEventQuery(
                Query.OpenAgentEventQueryRequest.newBuilder()
                        .setFilter(Query.AgentEventFilter.newBuilder()
                                .setTimestampMsFrom(4000)
                                .build())
                        .setStanding(false)
                        .build());

        assertEquals(2, tenantB.getAgentEventCount(h).getCount(),
                "the caller's own range criterion stopped narrowing");
    }

    private static void assertRefused(Runnable call, String rpc) {
        StatusRuntimeException refused = assertThrows(StatusRuntimeException.class, call::run,
                rpc + " served a handle the caller did not open");
        assertEquals(Status.Code.NOT_FOUND, refused.getStatus().getCode(),
                rpc + ": a foreign handle must read as absent, not as forbidden — "
                        + "\"exists, but not yours\" is an existence oracle. Got: "
                        + refused.getStatus());
    }
}
