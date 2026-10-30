// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import io.grpc.Context;
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

import com.github.tacplusdev.stratum.hazelcast.StratumMapConfig;
import com.github.tacplusdev.stratum.model.chassis.grpcserver.Authorizer;
import com.github.tacplusdev.stratum.model.chassis.servicemanager.Auth;
import com.github.tacplusdev.stratum.model.chassis.token.TokenWrapper;
import com.github.tacplusdev.stratum.model.s2sm.internal.grpc.servers.vservices.query.RecordMap;
import com.github.tacplusdev.stratum.model.types.services.letsencrypt.Letsencrypt.CertMagicState;
import com.github.tacplusdev.stratum.model.types.services.letsencrypt.Letsencrypt.LetsEncryptCert;
import com.github.tacplusdev.stratum.model.types.sys.errors.ProtoException;
import com.github.tacplusdev.stratum.model.types.vservices.query.Query;
import com.github.tacplusdev.stratum.model.types.vservices.query.RecordMapGrpc;
import com.github.tacplusdev.stratum.store.InMemoryRedoLog;
import com.github.tacplusdev.stratum.store.RecordStore;

/**
 * The record write surface over real gRPC, through the generated RecordMap
 * wrapper and the generated StratumRecordMaps, against a store whose redo is in
 * memory: the write answers arrive as gRPC statuses, Get returns the revision a
 * write answered, and Watch<T>Changes streams a snapshot and then only the
 * writes the redo acked.
 *
 * <p>Auth is stubbed at the signature check only, as in
 * {@link TenantQueryScopeE2ETest}; the claim handling and the wrapper's
 * tenant pins are the shipped path. The server serves tenant {@value #TID}, as
 * the instance does in production (its own identity's namespace, #18), and
 * every request is routed to it by target_tenant (tacplus#207).
 */
class RecordWriteSurfaceE2ETest {

    private static final String TID = "acme0001";
    private static final String TENANT = "/tenants/" + TID;

    private static HazelcastInstance hz;
    private static InMemoryRedoLog redo;
    private static RecordStore store;
    private static Server server;
    private static ManagedChannel channel;
    private static RecordMapGrpc.RecordMapBlockingStub tenant;

    private static String token(String namespace) {
        Base64.Encoder b64 = Base64.getUrlEncoder().withoutPadding();
        String header = b64.encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8));
        String payload = b64.encodeToString(("{\"namespace\":\"" + namespace + "\","
                + "\"sub\":\"spiffe://cav" + namespace + "/services/letsencrypt\"}")
                .getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + ".stub-signature";
    }

    private static LetsEncryptCert cert(String id, long expiresAt) {
        return LetsEncryptCert.newBuilder()
                .setCertSetId(id)
                .setCertmagicState(CertMagicState.getDefaultInstance())
                .addHostnames(id + ".example")
                .setExpiresAt(expiresAt)
                .setNamespace(TENANT)
                .build();
    }

    @BeforeAll
    static void up() throws Exception {
        Config cfg = StratumMapConfig.build(TID, List.of("127.0.0.1"), 1, null);
        cfg.setClusterName("stratum-write-e2e-" + UUID.randomUUID());
        cfg.getNetworkConfig().setPort(5851).setPortAutoIncrement(true);
        hz = Hazelcast.newHazelcastInstance(cfg);
        redo = new InMemoryRedoLog();
        store = new RecordStore(hz, new StratumRecordTypes(), redo, Duration.ofSeconds(2));
        store.catchUp();
        StratumStores stores = new StratumStores(hz);

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
        java.util.Map<String, StratumRecordMaps.Handle> registry = new java.util.concurrent.ConcurrentHashMap<>();
        server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(
                        new RecordMap(new StratumRecordMaps(() -> stores, () -> store, () -> registry), authorizer),
                        Authorizer.headerCaptureInterceptor()))
                .build()
                .start();
        channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                .negotiationType(NegotiationType.PLAINTEXT).build();
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token(TENANT));
        tenant = RecordMapGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
    }

    @AfterAll
    static void down() throws Exception {
        if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (server != null) server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (store != null) store.shutdown();
        if (hz != null) hz.shutdown();
    }

    private static Status.Code code(Runnable call) {
        return assertThrows(StatusRuntimeException.class, call::run).getStatus().getCode();
    }

    /** The route is checked before the handler: nothing is written. */
    @Test
    void aWriteWithNoTargetTenantIsRefusedAsInvalid() {
        assertEquals(Status.Code.INVALID_ARGUMENT, code(() -> tenant.putLetsEncryptCert(
                Query.PutLetsEncryptCertRequest.newBuilder().setItem(cert("unrouted", 1)).build())));
        assertEquals(Status.Code.NOT_FOUND, code(() -> tenant.getLetsEncryptCert(
                Query.GetLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setCertSetId("unrouted").build())));
    }

    /** A write routed to another tenant is refused before the handler: this instance serves {@value #TID} only. */
    @Test
    void aWriteRoutedToAnotherTenantIsRefused() {
        assertEquals(Status.Code.PERMISSION_DENIED, code(() -> tenant.putLetsEncryptCert(
                Query.PutLetsEncryptCertRequest.newBuilder().setTargetTenant("other001").setItem(cert("misrouted", 1)).build())));
        assertEquals(Status.Code.PERMISSION_DENIED, code(() -> tenant.getLetsEncryptCert(
                Query.GetLetsEncryptCertRequest.newBuilder().setTargetTenant("other001").setCertSetId("misrouted").build())));
        assertEquals(Status.Code.NOT_FOUND, code(() -> tenant.getLetsEncryptCert(
                Query.GetLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setCertSetId("misrouted").build())));
    }

    @Test
    void theWriteAnswersArriveAsStatuses() {
        Query.RecordWriteResult first = tenant.insertIfAbsentLetsEncryptCert(
                Query.InsertIfAbsentLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setItem(cert("w1", 1)).build());
        assertTrue(first.getRevision() > 0);
        assertEquals(Query.RecordDurability.RECORD_DURABILITY_LOCAL, first.getDurability(),
                "LetsEncryptCert declares no durability: LOCAL, acked by the redo");

        assertEquals(Status.Code.ALREADY_EXISTS, code(() -> tenant.insertIfAbsentLetsEncryptCert(
                Query.InsertIfAbsentLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setItem(cert("w1", 2)).build())));
        assertEquals(Status.Code.ABORTED, code(() -> tenant.replaceIfUnchangedLetsEncryptCert(
                Query.ReplaceIfUnchangedLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setItem(cert("w1", 3))
                        .setExpectedRevision(first.getRevision() + 1000).build())));
        assertEquals(Status.Code.NOT_FOUND, code(() -> tenant.deleteLetsEncryptCert(
                Query.DeleteLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setCertSetId("absent").build())));

        Query.RecordWriteResult replaced = tenant.replaceIfUnchangedLetsEncryptCert(
                Query.ReplaceIfUnchangedLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setItem(cert("w1", 4))
                        .setExpectedRevision(first.getRevision()).build());
        Query.GetLetsEncryptCertResponse got = tenant.getLetsEncryptCert(
                Query.GetLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setCertSetId("w1").build());
        assertEquals(4, got.getItem().getExpiresAt());
        assertEquals(replaced.getRevision(), got.getRevision());

        tenant.deleteLetsEncryptCert(Query.DeleteLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setCertSetId("w1").build());
        assertEquals(Status.Code.NOT_FOUND, code(() -> tenant.getLetsEncryptCert(
                Query.GetLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setCertSetId("w1").build())));
    }

    @Test
    void applyIsAllOrNothingOverTheWire() {
        tenant.putLetsEncryptCert(Query.PutLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setItem(cert("ap-b", 1)).build());
        // Canonical order (the wrapper refuses any other): ap-a before ap-b.
        assertEquals(Status.Code.ALREADY_EXISTS, code(() -> tenant.apply(Query.RecordApplyRequest.newBuilder().setTargetTenant(TID)
                .addOps(Query.RecordOp.newBuilder().setInsertIfAbsentLetsEncryptCert(
                        Query.InsertIfAbsentLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setItem(cert("ap-a", 1))))
                .addOps(Query.RecordOp.newBuilder().setInsertIfAbsentLetsEncryptCert(
                        Query.InsertIfAbsentLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setItem(cert("ap-b", 2))))
                .build())));
        assertEquals(Status.Code.NOT_FOUND, code(() -> tenant.getLetsEncryptCert(
                Query.GetLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setCertSetId("ap-a").build())));
    }

    @Test
    void watchChangesStreamsTheSnapshotThenOnlyAckedWrites() throws Exception {
        tenant.putLetsEncryptCert(Query.PutLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setItem(cert("wa-1", 1)).build());

        Context.CancellableContext ctx = Context.current().withCancellation();
        try {
            Iterator<Query.LetsEncryptCertWatchEvent> events = ctx.call(() -> tenant.watchLetsEncryptCertChanges(
                    Query.WatchLetsEncryptCertChangesRequest.newBuilder().setTargetTenant(TID).build()));
            boolean sawSnapshotRow = false;
            Query.LetsEncryptCertWatchEvent e;
            while (!(e = events.next()).hasSnapshotDone()) {
                sawSnapshotRow |= e.getSnapshotItem().getCertSetId().equals("wa-1");
            }
            assertTrue(sawSnapshotRow);

            // A write the redo refuses is never seen; the next acked one is.
            redo.setReachable(false);
            assertEquals(Status.Code.UNAVAILABLE, code(() -> tenant.putLetsEncryptCert(
                    Query.PutLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setItem(cert("wa-refused", 1)).build())));
            redo.setReachable(true);
            Query.RecordWriteResult acked = tenant.putLetsEncryptCert(
                    Query.PutLetsEncryptCertRequest.newBuilder().setTargetTenant(TID).setItem(cert("wa-2", 2)).build());

            CompletableFuture<Query.LetsEncryptCertWatchChange> delta = CompletableFuture.supplyAsync(() -> {
                while (true) {
                    Query.LetsEncryptCertWatchEvent ev = events.next();
                    if (ev.hasDelta()) {
                        return ev.getDelta();
                    }
                }
            });
            Query.LetsEncryptCertWatchChange change = delta.get(10, TimeUnit.SECONDS);
            assertEquals("wa-2", change.getItem().getCertSetId());
            assertEquals(acked.getRevision(), change.getRevision());
            assertEquals(Query.RecordChangeType.RECORD_CHANGE_TYPE_ADD, change.getType());
            assertFalse(change.getItem().getCertSetId().equals("wa-refused"));
        } finally {
            ctx.cancel(null);
        }
    }
}
