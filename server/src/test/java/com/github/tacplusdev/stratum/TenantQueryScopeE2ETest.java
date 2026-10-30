// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

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
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.MetadataUtils;

import com.github.tacplusdev.stratum.store.InMemoryRedoLog;
import com.github.tacplusdev.stratum.store.RecordStore;
import com.github.tacplusdev.stratum.model.chassis.grpcserver.Authorizer;
import com.github.tacplusdev.stratum.model.chassis.servicemanager.Auth;
import com.github.tacplusdev.stratum.model.chassis.token.TokenWrapper;
import com.github.tacplusdev.stratum.model.s2sm.internal.grpc.servers.vservices.query.RecordMap;
import com.github.tacplusdev.stratum.model.types.sys.errors.ProtoException;
import com.github.tacplusdev.stratum.model.types.services.letsencrypt.Letsencrypt.CertMagicState;
import com.github.tacplusdev.stratum.model.types.services.letsencrypt.Letsencrypt.LetsEncryptCert;
import com.github.tacplusdev.stratum.model.types.vservices.query.Query;
import com.github.tacplusdev.stratum.model.types.vservices.query.RecordMapGrpc;

/**
 * The tenant boundary of the generated record query plane, over real gRPC
 * against one tenant's instance: it serves tenant {@value #TID} (the namespace
 * of its own identity in production, #18) and its store holds that tenant's
 * rows only, in two namespaces, the tenant's own and {@value #TEAM} below it.
 *
 * <p>Two boundaries are pinned. The instance's: every call carries the tid it
 * is routed to (target_tenant, tacplus#207), and one with none is
 * INVALID_ARGUMENT and one routed to another tenant is PERMISSION_DENIED, both
 * before the handler, {@code /system} callers included. There are no
 * fleet-wide reads any more: Stratum is per tenant. And the handle's, within
 * the instance: a query is pinned to the EXACT namespace of the caller that
 * opened it, so every RPC shape the plane exposes over a query handle gets a
 * negative case, because the bug they pin was one unscoped
 * {@code create<T>Query} reached through six different RPCs. The tenant opens
 * the broadest query it can — no filter at all — and each shape must answer
 * with the tenant's own rows and count alone, never {@value #TEAM}'s.
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
class TenantQueryScopeE2ETest {
	/** The shared registry, in-process here; StratumHandler supplies a Hazelcast map. */
	private static final java.util.Map<String, StratumRecordMaps.Handle> registry = new java.util.concurrent.ConcurrentHashMap<>();


    /** The tid this instance serves, and another tenant's. */
    private static final String TID = "aaaa0001";
    private static final String OTHER_TID = "bbbb0001";

    private static final String TENANT = "/tenants/" + TID;
    private static final String TEAM = TENANT + "/team";
    private static final String OTHER_TENANT = "/tenants/" + OTHER_TID;
    private static final String SYSTEM = "/system";

    private static HazelcastInstance hz;
    private static StratumStores stores;
    private static RecordStore store;
    private static Server server;
    private static ManagedChannel channel;

    /** One stub per caller identity. The namespace claim is the only difference. */
    private static RecordMapGrpc.RecordMapBlockingStub tenant;
    private static RecordMapGrpc.RecordMapBlockingStub team;
    private static RecordMapGrpc.RecordMapBlockingStub otherTenant;
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

    /**
     * A row that passes the generated LetsEncryptCert validation: cert_set_id,
     * certmagic_state and hostnames are required.
     */
    private static LetsEncryptCert cert(String certSetId, String namespace, long expiresAt) {
        return LetsEncryptCert.newBuilder()
                .setCertSetId(certSetId)
                .setCertmagicState(CertMagicState.getDefaultInstance())
                .addHostnames("cav" + namespace.replace('/', '.') + ".example")
                .setExpiresAt(expiresAt)
                .setSpiffeId("spiffe://cav" + namespace + "/services/letsencrypt")
                .setNamespace(namespace)
                .build();
    }

    private static RecordMapGrpc.RecordMapBlockingStub stub(ManagedChannel ch, String token) {
        RecordMapGrpc.RecordMapBlockingStub s = RecordMapGrpc.newBlockingStub(ch);
        if (token == null) {
            return s;
        }
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
        return s.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
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

        stores = new StratumStores(hz);
        store = new RecordStore(hz, new StratumRecordTypes(), new InMemoryRedoLog(), java.time.Duration.ofSeconds(5));
        store.catchUp();
        for (LetsEncryptCert c : List.of(cert("a-1", TENANT, 1000), cert("a-2", TENANT, 2000),
                cert("t-1", TEAM, 3000), cert("t-2", TEAM, 4000), cert("t-3", TEAM, 5000))) {
            store.apply(Query.RecordApplyRequest.newBuilder().addOps(Query.RecordOp.newBuilder()
                    .setPutLetsEncryptCert(Query.PutLetsEncryptCertRequest.newBuilder().setItem(c))).build());
        }
        assertEquals(5, store.rowCount(),
                "a fixture write was refused");

        // Verifies nothing; records whatever bearer token the call carried into the
        // slot the real Authorizer fills after it HAS verified one. A call with no
        // authorization header records nothing, which is the unauthenticated case.
        // The tenant door this instance's authorizer keeps (another tenant's caller
        // refused at authorize) is not reached: authorize is stubbed whole. So
        // otherTenant below gets as far as the route check and the handle check,
        // which is what is under test.
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
        authorizer.setHostNamespace(TENANT);

        server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(
                        new RecordMap(new StratumRecordMaps(() -> stores, () -> store, () -> registry),
                                authorizer),
                        Authorizer.headerCaptureInterceptor()))
                .build()
                .start();
        channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                .negotiationType(NegotiationType.PLAINTEXT).build();
        tenant = stub(channel, token(TENANT));
        team = stub(channel, token(TEAM));
        otherTenant = stub(channel, token(OTHER_TENANT));
        system = stub(channel, token(SYSTEM));
        anonymous = stub(channel, null);
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
        return open(stub, Query.LetsEncryptCertFilter.newBuilder().build(), TID);
    }

    private static Query.QueryHandle open(RecordMapGrpc.RecordMapBlockingStub stub,
            Query.LetsEncryptCertFilter filter, String tid) {
        return routed(stub.createLetsEncryptCertQuery(Query.OpenLetsEncryptCertQueryRequest.newBuilder()
                .setTargetTenant(tid)
                .setFilter(filter)
                .setStanding(false)
                .build()));
    }

    /** The handle as a caller passes it back: routed, like every request. */
    private static Query.QueryHandle routed(Query.QueryHandle h) {
        return h.toBuilder().setTargetTenant(TID).build();
    }

    private static Query.SampleLetsEncryptCertRequest sample(Query.QueryHandle h) {
        return Query.SampleLetsEncryptCertRequest.newBuilder().setTargetTenant(h.getTargetTenant())
                .setQueryId(h.getQueryId()).setSeed(1).setN(100).build();
    }

    private static Query.PullLetsEncryptCertRequest pull(Query.QueryHandle h) {
        return Query.PullLetsEncryptCertRequest.newBuilder().setTargetTenant(h.getTargetTenant())
                .setQueryId(h.getQueryId()).setPageSize(100).build();
    }

    private static Query.RefineLetsEncryptCertRequest refineToMatchAll(Query.QueryHandle h) {
        return Query.RefineLetsEncryptCertRequest.newBuilder().setTargetTenant(h.getTargetTenant())
                .setQueryId(h.getQueryId())
                .setFilter(Query.LetsEncryptCertFilter.newBuilder().build())
                .build();
    }

    // -----------------------------------------------------------------------
    // The instance's boundary: every call is routed to the tenant it serves.
    // -----------------------------------------------------------------------

    /**
     * A call that names no tenant is refused as malformed before the handler,
     * on every RPC shape: open, and each one that dereferences a handle.
     */
    @Test
    void aCallWithNoTargetTenantIsRefusedAsInvalid() {
        int handles = registry.size();
        assertRouteRefused(Status.Code.INVALID_ARGUMENT, () -> open(tenant, Query.LetsEncryptCertFilter.newBuilder().build(), ""), "open");
        assertEquals(handles, registry.size(), "an unrouted open reached the handler");

        Query.QueryHandle unrouted = openMatchAll(tenant).toBuilder().clearTargetTenant().build();
        assertRouteRefusedOverHandle(Status.Code.INVALID_ARGUMENT, tenant, unrouted);
    }

    /**
     * A call routed to another tenant than the one this instance serves is
     * refused before the handler, whoever makes it: that tenant's own caller,
     * this tenant's, and {@code /system} alike. A tenant's rows are on its own
     * instance only, so there is no fleet-wide read to route anywhere.
     */
    @Test
    void aCallRoutedToAnotherTenantIsRefused() {
        int handles = registry.size();
        for (RecordMapGrpc.RecordMapBlockingStub caller : List.of(otherTenant, tenant, system)) {
            assertRouteRefused(Status.Code.PERMISSION_DENIED,
                    () -> open(caller, Query.LetsEncryptCertFilter.newBuilder().build(), OTHER_TID), "open");
        }
        assertEquals(handles, registry.size(), "a misrouted open reached the handler");

        Query.QueryHandle misrouted = openMatchAll(tenant).toBuilder().setTargetTenant(OTHER_TID).build();
        assertRouteRefusedOverHandle(Status.Code.PERMISSION_DENIED, tenant, misrouted);
        assertEquals(2, tenant.getLetsEncryptCertCount(routed(misrouted)).getCount(),
                "a misrouted call disturbed the open query");
    }

    // -----------------------------------------------------------------------
    // The negative case, one per RPC shape that dereferences a handle.
    // -----------------------------------------------------------------------

    /**
     * The read path. The tenant opens a match-all query; the page it gets back
     * holds its own two rows and none of {@value #TEAM}'s three.
     *
     * A failure here does not report as a failure: the wrapper's backstop sees the
     * foreign row first and halts the JVM, so the surefire fork dies mid-run.
     */
    @Test
    void listOverAMatchAllQueryReturnsOnlyTheOpeningTenantsRows() {
        Query.PullLetsEncryptCertResponse page = tenant.listLetsEncryptCert(pull(openMatchAll(tenant)));

        assertEquals(2, page.getItemsCount(), "the tenant sees exactly its own two rows");
        for (LetsEncryptCert row : page.getItemsList()) {
            assertEquals(TENANT, row.getNamespace(),
                    "a row from another namespace reached the page: " + row.getCertSetId());
        }
    }

    /** The same, through the sample path, which pages off its own scan. */
    @Test
    void sampleOverAMatchAllQueryReturnsOnlyTheOpeningTenantsRows() {
        Query.SampleLetsEncryptCertResponse s = tenant.getLetsEncryptCertSample(sample(openMatchAll(tenant)));

        assertEquals(2, s.getItemsCount(), "the tenant samples exactly its own two rows");
        for (LetsEncryptCert row : s.getItemsList()) {
            assertEquals(TENANT, row.getNamespace(),
                    "a row from another namespace reached the sample: " + row.getCertSetId());
        }
    }

    /** The same, through the watch snapshot. */
    @Test
    void watchOverAMatchAllQueryReturnsOnlyTheOpeningTenantsRows() {
        java.util.Iterator<Query.LetsEncryptCertWatchEvent> events =
                tenant.watchLetsEncryptCert(openMatchAll(tenant));

        int rows = 0;
        while (events.hasNext()) {
            Query.LetsEncryptCertWatchEvent e = events.next();
            if (!e.hasSnapshotItem()) {
                continue;
            }
            rows++;
            assertEquals(TENANT, e.getSnapshotItem().getNamespace(),
                    "a row from another namespace reached the watch snapshot: "
                            + e.getSnapshotItem().getCertSetId());
        }
        assertEquals(2, rows, "the tenant watches exactly its own two rows");
    }

    /**
     * The count path — the leg with NO backstop behind it, and so the one where an
     * unscoped query is a silent cross-tenant read rather than a crash. 2, not 5.
     */
    @Test
    void countOverAMatchAllQueryCountsOnlyTheOpeningTenantsRows() {
        assertEquals(2, tenant.getLetsEncryptCertCount(openMatchAll(tenant)).getCount(),
                "the count leaked rows the caller may not see");
    }

    /** The same count, through the streaming shape. */
    @Test
    void subscribeCountOverAMatchAllQueryCountsOnlyTheOpeningTenantsRows() {
        java.util.Iterator<Query.RecordCount> counts =
                tenant.subscribeLetsEncryptCertCount(openMatchAll(tenant));

        assertTrue(counts.hasNext(), "the subscription produced no count at all");
        assertEquals(2, counts.next().getCount(), "the subscribed count leaked rows");
    }

    /**
     * The size a refine reports — the third counting site, and the one a caller
     * reaches by widening an already-open handle back to match-all.
     */
    @Test
    void refiningAQueryBackToMatchAllReportsOnlyTheOpeningTenantsSize() {
        Query.QueryHandle h = open(tenant,
                Query.LetsEncryptCertFilter.newBuilder().setCertSetId("a-1").build(), TID);

        long size = tenant.updateLetsEncryptCertQuery(refineToMatchAll(h)).getSize();

        assertEquals(2, size, "a refine widened the handle past the namespace that opened it");
    }

    // -----------------------------------------------------------------------
    // Handle ownership: a query_id is a bare UUID, and holding one is not a grant.
    // -----------------------------------------------------------------------

    /**
     * Another caller holds the tenant's query_id, correctly routed — the leak
     * the handle registry allows by construction, since the id is a bare UUID
     * shared across every caller of the instance. Every RPC that dereferences it
     * must refuse both {@value #TEAM}, a namespace of the same tenant, and
     * another tenant's caller that got past the door.
     */
    @Test
    void anotherCallerCannotUseAHandleItDidNotOpen() {
        Query.QueryHandle h = openMatchAll(tenant);

        for (RecordMapGrpc.RecordMapBlockingStub caller : List.of(team, otherTenant)) {
            assertRefused(() -> caller.listLetsEncryptCert(pull(h)), "list");
            assertRefused(() -> caller.getLetsEncryptCertSample(sample(h)), "sample");
            assertRefused(() -> caller.getLetsEncryptCertCount(h), "count");
            assertRefused(() -> caller.watchLetsEncryptCert(h).hasNext(), "watch");
            assertRefused(() -> caller.subscribeLetsEncryptCertCount(h).hasNext(), "subscribeCount");
            assertRefused(() -> caller.updateLetsEncryptCertQuery(refineToMatchAll(h)), "refine");
            assertRefused(() -> caller.deleteLetsEncryptCertQuery(h), "close");
        }

        // Refused, and still there: the failed attempts must not have closed or
        // widened the handle out from under the tenant.
        assertEquals(2, tenant.getLetsEncryptCertCount(h).getCount(),
                "the refused calls disturbed the tenant's open query");
    }

    /**
     * A caller with no verified namespace claim cannot open a query at all: the
     * call is routed, so it is the claim check that refuses it.
     */
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
     * {@value #TEAM}'s own legitimately-scoped query still answers with its rows.
     * Without this the negative cases above would pass just as well against a
     * plane that refused everything.
     */
    @Test
    void aNamespacesOwnQueryStillSeesItsOwnRows() {
        Query.QueryHandle h = openMatchAll(team);

        assertEquals(3, team.getLetsEncryptCertCount(h).getCount());
        Query.PullLetsEncryptCertResponse page = team.listLetsEncryptCert(pull(h));
        assertEquals(3, page.getItemsCount());
        for (LetsEncryptCert row : page.getItemsList()) {
            assertEquals(TEAM, row.getNamespace());
        }
        assertEquals(h.getQueryId(), team.deleteLetsEncryptCertQuery(h).getQueryId());
    }

    /**
     * A /system caller routed to this tenant reads every namespace of it — the
     * convention the generated wrapper's own backstop follows, where
     * {@code /system} is the one claim not pinned to a namespace — and that is
     * this tenant's rows only: the instance holds no other tenant's.
     */
    @Test
    void aSystemCallerRoutedToTheTenantReadsThatTenantOnly() {
        Query.QueryHandle h = openMatchAll(system);

        assertEquals(5, system.getLetsEncryptCertCount(h).getCount(),
                "a /system caller lost the read across the tenant's namespaces");
        Query.PullLetsEncryptCertResponse page = system.listLetsEncryptCert(pull(h));
        assertEquals(5, page.getItemsCount());
        for (LetsEncryptCert row : page.getItemsList()) {
            assertTrue(row.getNamespace().equals(TENANT) || row.getNamespace().startsWith(TENANT + "/"),
                    "a row outside tenant " + TID + " reached a /system read: " + row.getNamespace());
        }
    }

    /** A filter the caller does supply still narrows, inside its own namespace. */
    @Test
    void aCallersOwnFilterStillNarrowsWithinItsNamespace() {
        Query.QueryHandle h = open(team,
                Query.LetsEncryptCertFilter.newBuilder().setExpiresAtFrom(4000).build(), TID);

        assertEquals(2, team.getLetsEncryptCertCount(h).getCount(),
                "the caller's own range criterion stopped narrowing");
    }

    /** Every RPC shape that dereferences a handle refuses this one, h, by its route. */
    private static void assertRouteRefusedOverHandle(Status.Code code, RecordMapGrpc.RecordMapBlockingStub caller,
            Query.QueryHandle h) {
        assertRouteRefused(code, () -> caller.listLetsEncryptCert(pull(h)), "list");
        assertRouteRefused(code, () -> caller.getLetsEncryptCertSample(sample(h)), "sample");
        assertRouteRefused(code, () -> caller.getLetsEncryptCertCount(h), "count");
        assertRouteRefused(code, () -> caller.watchLetsEncryptCert(h).hasNext(), "watch");
        assertRouteRefused(code, () -> caller.subscribeLetsEncryptCertCount(h).hasNext(), "subscribeCount");
        assertRouteRefused(code, () -> caller.updateLetsEncryptCertQuery(refineToMatchAll(h)), "refine");
        assertRouteRefused(code, () -> caller.deleteLetsEncryptCertQuery(h), "close");
    }

    private static void assertRouteRefused(Status.Code code, Runnable call, String rpc) {
        StatusRuntimeException refused = assertThrows(StatusRuntimeException.class, call::run,
                rpc + " served a call not routed to tenant " + TID);
        assertEquals(code, refused.getStatus().getCode(), rpc + ": " + refused.getStatus());
        assertTrue(refused.getStatus().getDescription().contains("target_tenant")
                        || refused.getStatus().getDescription().contains("tenant " + OTHER_TID),
                rpc + " was refused, but not by its route: " + refused.getStatus());
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
