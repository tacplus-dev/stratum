// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum;

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

import com.gitlab.seabeam.controlplane.schematic.services.stratum.hot.HotWrites;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.grpcserver.Authorizer;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.servicemanager.Auth;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.token.TokenWrapper;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.s2sm.internal.grpc.servers.vservices.query.RecordMap;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.sys.errors.ProtoException;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.services.letsencrypt.Letsencrypt.CertMagicState;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.services.letsencrypt.Letsencrypt.LetsEncryptCert;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.query.Query;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.query.RecordMapGrpc;

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
	/** The shared registry, in-process here; StratumHandler supplies a Hazelcast map. */
	private static final java.util.Map<String, StratumRecordMaps.Handle> registry = new java.util.concurrent.ConcurrentHashMap<>();


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

        stores = new StratumEventStores();
        stores.init(hz, new HotWrites(hz, 1));
        stores.store(cert("a-1", TENANT_A, 1000));
        stores.store(cert("a-2", TENANT_A, 2000));
        stores.store(cert("b-1", TENANT_B, 3000));
        stores.store(cert("b-2", TENANT_B, 4000));
        stores.store(cert("b-3", TENANT_B, 5000));
        assertEquals(5, stores.totalCount(),
                "a fixture failed the generated LetsEncryptCert validation and was dropped");

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

        server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(
                        new RecordMap(new StratumRecordMaps(() -> stores, () -> true, () -> registry),
                                authorizer),
                        Authorizer.headerCaptureInterceptor()))
                .build()
                .start();
        channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                .negotiationType(NegotiationType.PLAINTEXT).build();
        tenantA = stub(channel, token(TENANT_A));
        tenantB = stub(channel, token(TENANT_B));
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
        return stub.createLetsEncryptCertQuery(Query.OpenLetsEncryptCertQueryRequest.newBuilder()
                .setFilter(Query.LetsEncryptCertFilter.newBuilder().build())
                .setStanding(false)
                .build());
    }

    private static Query.SampleLetsEncryptCertRequest sample(Query.QueryHandle h) {
        return Query.SampleLetsEncryptCertRequest.newBuilder()
                .setQueryId(h.getQueryId()).setSeed(1).setN(100).build();
    }

    private static Query.PullLetsEncryptCertRequest pull(Query.QueryHandle h) {
        return Query.PullLetsEncryptCertRequest.newBuilder()
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
        Query.PullLetsEncryptCertResponse page = tenantA.listLetsEncryptCert(pull(openMatchAll(tenantA)));

        assertEquals(2, page.getItemsCount(), "tenant A sees exactly its own two rows");
        for (LetsEncryptCert row : page.getItemsList()) {
            assertEquals(TENANT_A, row.getNamespace(),
                    "a row from another tenant reached the page: " + row.getCertSetId());
        }
    }

    /** The same, through the sample path, which pages off its own scan. */
    @Test
    void sampleOverAMatchAllQueryReturnsOnlyTheOpeningTenantsRows() {
        Query.SampleLetsEncryptCertResponse s = tenantA.getLetsEncryptCertSample(sample(openMatchAll(tenantA)));

        assertEquals(2, s.getItemsCount(), "tenant A samples exactly its own two rows");
        for (LetsEncryptCert row : s.getItemsList()) {
            assertEquals(TENANT_A, row.getNamespace(),
                    "a row from another tenant reached the sample: " + row.getCertSetId());
        }
    }

    /** The same, through the watch snapshot. */
    @Test
    void watchOverAMatchAllQueryReturnsOnlyTheOpeningTenantsRows() {
        java.util.Iterator<Query.LetsEncryptCertWatchEvent> events =
                tenantA.watchLetsEncryptCert(openMatchAll(tenantA));

        int rows = 0;
        while (events.hasNext()) {
            Query.LetsEncryptCertWatchEvent e = events.next();
            if (!e.hasSnapshotItem()) {
                continue;
            }
            rows++;
            assertEquals(TENANT_A, e.getSnapshotItem().getNamespace(),
                    "a row from another tenant reached the watch snapshot: "
                            + e.getSnapshotItem().getCertSetId());
        }
        assertEquals(2, rows, "tenant A watches exactly its own two rows");
    }

    /**
     * The count path — the leg with NO backstop behind it, and so the one where an
     * unscoped query is a silent cross-tenant read rather than a crash. 2, not 5.
     */
    @Test
    void countOverAMatchAllQueryCountsOnlyTheOpeningTenantsRows() {
        assertEquals(2, tenantA.getLetsEncryptCertCount(openMatchAll(tenantA)).getCount(),
                "the count leaked rows the caller may not see");
    }

    /** The same count, through the streaming shape. */
    @Test
    void subscribeCountOverAMatchAllQueryCountsOnlyTheOpeningTenantsRows() {
        java.util.Iterator<Query.RecordCount> counts =
                tenantA.subscribeLetsEncryptCertCount(openMatchAll(tenantA));

        assertTrue(counts.hasNext(), "the subscription produced no count at all");
        assertEquals(2, counts.next().getCount(), "the subscribed count leaked rows");
    }

    /**
     * The size a refine reports — the third counting site, and the one a caller
     * reaches by widening an already-open handle back to match-all.
     */
    @Test
    void refiningAQueryBackToMatchAllReportsOnlyTheOpeningTenantsSize() {
        Query.QueryHandle h = tenantA.createLetsEncryptCertQuery(
                Query.OpenLetsEncryptCertQueryRequest.newBuilder()
                        .setFilter(Query.LetsEncryptCertFilter.newBuilder().setCertSetId("a-1").build())
                        .setStanding(false)
                        .build());

        long size = tenantA.updateLetsEncryptCertQuery(Query.RefineLetsEncryptCertRequest.newBuilder()
                .setQueryId(h.getQueryId())
                .setFilter(Query.LetsEncryptCertFilter.newBuilder().build())
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

        assertRefused(() -> tenantB.listLetsEncryptCert(pull(h)), "list");
        assertRefused(() -> tenantB.getLetsEncryptCertSample(sample(h)), "sample");
        assertRefused(() -> tenantB.getLetsEncryptCertCount(h), "count");
        assertRefused(() -> tenantB.watchLetsEncryptCert(h).hasNext(), "watch");
        assertRefused(() -> tenantB.subscribeLetsEncryptCertCount(h).hasNext(), "subscribeCount");
        assertRefused(() -> tenantB.updateLetsEncryptCertQuery(Query.RefineLetsEncryptCertRequest.newBuilder()
                .setQueryId(h.getQueryId())
                .setFilter(Query.LetsEncryptCertFilter.newBuilder().build())
                .build()), "refine");
        assertRefused(() -> tenantB.deleteLetsEncryptCertQuery(h), "close");

        // Refused, and still there: B's failed attempts must not have closed or
        // widened the handle out from under A.
        assertEquals(2, tenantA.getLetsEncryptCertCount(h).getCount(),
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

        assertEquals(3, tenantB.getLetsEncryptCertCount(h).getCount());
        Query.PullLetsEncryptCertResponse page = tenantB.listLetsEncryptCert(pull(h));
        assertEquals(3, page.getItemsCount());
        for (LetsEncryptCert row : page.getItemsList()) {
            assertEquals(TENANT_B, row.getNamespace());
        }
        assertEquals(h.getQueryId(), tenantB.deleteLetsEncryptCertQuery(h).getQueryId());
    }

    /**
     * A /system caller keeps the fleet-wide read this plane has always served —
     * the convention the generated wrapper's own backstop follows, where
     * {@code /system} is the one claim that is not pinned to a tenant.
     */
    @Test
    void aSystemCallerStillReadsEveryTenant() {
        Query.QueryHandle h = openMatchAll(system);

        assertEquals(5, system.getLetsEncryptCertCount(h).getCount(),
                "a /system caller lost the fleet-wide read");
        assertEquals(5, system.listLetsEncryptCert(pull(h)).getItemsCount());
    }

    /** A filter the caller does supply still narrows, inside its own tenant. */
    @Test
    void aCallersOwnFilterStillNarrowsWithinItsTenant() {
        Query.QueryHandle h = tenantB.createLetsEncryptCertQuery(
                Query.OpenLetsEncryptCertQueryRequest.newBuilder()
                        .setFilter(Query.LetsEncryptCertFilter.newBuilder()
                                .setExpiresAtFrom(4000)
                                .build())
                        .setStanding(false)
                        .build());

        assertEquals(2, tenantB.getLetsEncryptCertCount(h).getCount(),
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
