// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.InsecureServerCredentials;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;

import com.github.tacplusdev.stratum.model.chassis.serviceconnection.RouteTenant;
import com.github.tacplusdev.stratum.model.s2sm.internal.grpc.clients.sys.redolog.RedoGrpcClient;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoGrpc;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoAck;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoAppend;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoChange;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoKey;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoOp;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoReplayEntry;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoReplayRequest;
import com.github.tacplusdev.stratum.model.types.sys.spec.Spec.Durability;

/**
 * GrpcRedoLog through the generated client, against a fake redo on loopback:
 * the redo is a {@code /system} service (tacplus-dev/tacplus#258, #54), so
 * neither the append nor the replay names a tenant, in its message or as the
 * call's route tenant.
 */
class GrpcRedoLogTest {

    /** A fake redo: records what it receives, acks each append LOCAL, replays one entry. */
    private static final class FakeRedo extends RedoGrpc.RedoImplBase {
        final List<RedoAppend> appends = new CopyOnWriteArrayList<>();
        final List<RedoReplayRequest> replays = new CopyOnWriteArrayList<>();

        @Override
        public StreamObserver<RedoAppend> append(StreamObserver<RedoAck> acks) {
            return new StreamObserver<>() {
                @Override
                public void onNext(RedoAppend a) {
                    appends.add(a);
                    acks.onNext(RedoAck.newBuilder().setSeq(a.getSeq()).setLevel(Durability.DURABILITY_LOCAL).build());
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                    acks.onCompleted();
                }
            };
        }

        @Override
        public void replay(RedoReplayRequest request, StreamObserver<RedoReplayEntry> out) {
            replays.add(request);
            out.onNext(RedoReplayEntry.newBuilder().setSeq(1).build());
            out.onCompleted();
        }
    }

    /** Records each call's route tenant (RouteTenant.CALL_OPTION), "" when it has none. */
    private static final class RouteRecorder implements ClientInterceptor {
        final List<String> routes = new CopyOnWriteArrayList<>();

        @Override
        public <Q, R> ClientCall<Q, R> interceptCall(MethodDescriptor<Q, R> method, CallOptions options, Channel next) {
            String route = options.getOption(RouteTenant.CALL_OPTION);
            routes.add(method.getBareMethodName() + ":" + (route == null ? "" : route));
            return next.newCall(method, options);
        }
    }

    private final FakeRedo redo = new FakeRedo();
    private final RouteRecorder recorder = new RouteRecorder();
    private Server server;
    private ManagedChannel channel;
    private GrpcRedoLog log;

    @BeforeEach
    void start() throws Exception {
        server = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                .addService(redo)
                .build()
                .start();
        channel = Grpc.newChannelBuilder("127.0.0.1:" + server.getPort(), InsecureChannelCredentials.create())
                .build();
        log = new GrpcRedoLog(new RedoGrpcClient(ClientInterceptors.intercept(channel, recorder), () -> "token"));
    }

    @AfterEach
    void stop() throws Exception {
        log.close();
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void appendNamesNoTenant() throws Exception {
        log.append(RedoAppend.newBuilder()
                .setSeq(1)
                .addChanges(RedoChange.newBuilder().setRecordType("t")
                        .setKey(RedoKey.newBuilder().setStringKey("k")).setOp(RedoOp.REDO_OP_PUT))
                .setDurability(Durability.DURABILITY_LOCAL)
                .build()).get(5, TimeUnit.SECONDS);

        assertEquals(1, redo.appends.size());
        assertEquals("", redo.appends.get(0).getTenant(), "the redo reads the tenant off the caller's identity");
        assertEquals(List.of("Append:"), recorder.routes, "a /system call is routed to no tenant");
    }

    @Test
    void replayNamesNoTenant() {
        List<RedoReplayEntry> entries = new ArrayList<>();
        log.replay(entries::add);

        assertEquals(1, entries.size());
        assertEquals(List.of(RedoReplayRequest.getDefaultInstance()), redo.replays);
        assertTrue(redo.replays.get(0).getTenant().isEmpty());
        assertEquals(List.of("Replay:"), recorder.routes, "a /system call is routed to no tenant");
    }
}
