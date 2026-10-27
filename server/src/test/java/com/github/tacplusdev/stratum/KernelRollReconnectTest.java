// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.grpc.ConnectivityState;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.InsecureServerCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.github.tacplusdev.stratum.model.chassis.grpcserver.Authorizer;
import com.github.tacplusdev.stratum.model.chassis.serviceconnection.ServiceConnection;
import com.github.tacplusdev.stratum.model.types.sys.host.HostOuterClass;
import com.github.tacplusdev.stratum.model.types.sys.jwkview.JwkVerifyingKeysViewServiceGrpc;
import com.github.tacplusdev.stratum.model.types.sys.jwkview.Jwkview;
import com.github.tacplusdev.stratum.model.types.sys.trace.Trace;

/**
 * Defect 11: stratum exited during a kernel roll. The previous container log read
 * kernel GOAWAY, then {@code NullPointerException: channel must not be null}, then
 * "stratum: startup failed — exiting".
 *
 * <p>That message comes from the generated {@code RemoteJwkSource} constructor.
 * The generated {@code ServiceManager.run()} builds its mandatory Authorizer as
 * {@code new Authorizer(Authorizer.remoteSource(stateStoreConn.conn()))}, and the
 * state-store pool's {@code conn()} returns null whenever no endpoint is READY or
 * IDLE. After a GOAWAY, the reconnect that the TLS-cert and WatchConfig streams
 * drive moves the channel to CONNECTING or TRANSIENT_FAILURE while the kernel is
 * still down. {@code run()} then hands null to the Authorizer, and Main exits.
 *
 * <p>This test drives the real generated pieces in the order the chassis drives
 * them, against a fake kernel on a fixed port:
 * <ol>
 *   <li>the kernel serves, and the static state-store pool connects as
 *       {@code ServiceManager.create()} connects it;
 *   <li>the kernel rolls: it shuts down (GOAWAY) and refuses connections for
 *       about 10s. A reconnect attempt is made against it, as the cert and watch
 *       stream retries make one;
 *   <li>{@code run()}'s Authorizer step runs while the kernel is down. It must not
 *       throw;
 *   <li>the kernel comes back on the same port but answers UNAVAILABLE for about
 *       10s (still draining). The JWK watch must back off rather than spin;
 *   <li>the kernel serves again, and the JWK watch must reconnect.
 * </ol>
 */
class KernelRollReconnectTest {

    private static final long REFUSE_MS = 10_000;
    private static final long DRAIN_MS = 10_000;

    /** A fake kernel JWK view: answers UNAVAILABLE while draining, else streams one view and holds. */
    private static final class FakeJwkView extends JwkVerifyingKeysViewServiceGrpc.JwkVerifyingKeysViewServiceImplBase {
        final AtomicBoolean draining = new AtomicBoolean(false);
        final AtomicInteger drainingAttempts = new AtomicInteger();
        final AtomicInteger servedWatches = new AtomicInteger();

        @Override
        public void watch(Trace.TraceRequest request, StreamObserver<Jwkview.JwkViewResponse> out) {
            if (draining.get()) {
                drainingAttempts.incrementAndGet();
                out.onError(Status.UNAVAILABLE.withDescription("kernel draining").asRuntimeException());
                return;
            }
            servedWatches.incrementAndGet();
            out.onNext(Jwkview.JwkViewResponse.getDefaultInstance());
        }
    }

    private static Server startKernel(int port, FakeJwkView view) throws Exception {
        return Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
            .addService(view)
            .build()
            .start();
    }

    @Test
    void stratumSurvivesAKernelRollOnTheStateStoreChannelAndTheJwkWatchReconnects() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }

        FakeJwkView firstView = new FakeJwkView();
        Server kernel = startKernel(port, firstView);
        List<ManagedChannel> channels = new CopyOnWriteArrayList<>();
        ServiceConnection stateStoreConn = null;
        Authorizer authorizer = null;
        try {
            // 1. Connect the static state-store pool as create() does. The channel
            //    factory is plaintext; that is the only difference from the chassis.
            stateStoreConn = ServiceConnection.newStatic(
                "kernel",
                List.of(HostOuterClass.Host.newBuilder().setHost("127.0.0.1").setPort(port).build()),
                (serviceId, target) -> {
                    ManagedChannel ch = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
                    channels.add(ch);
                    return ch;
                });

            // 2. Kernel roll: GOAWAY, then the port refuses. Drive a reconnect
            //    the way the cert and watch stream retries do, until the channel
            //    has seen the refusal.
            kernel.shutdownNow();
            assertTrue(kernel.awaitTermination(5, TimeUnit.SECONDS));
            long rollStart = System.nanoTime();
            ManagedChannel ch = channels.get(channels.size() - 1);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (ch.getState(true) != ConnectivityState.TRANSIENT_FAILURE && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }

            // 3. run()'s Authorizer step while the kernel is down: the exact
            //    expression from the generated ServiceManager.run(). Today this
            //    throws NullPointerException("channel must not be null").
            authorizer = new Authorizer(Authorizer.remoteSource(stateStoreConn.conn()));
            assertNotNull(authorizer);

            // 4. The kernel is back on the same port but still draining.
            long refuseLeft = REFUSE_MS - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - rollStart);
            if (refuseLeft > 0) {
                Thread.sleep(refuseLeft);
            }
            FakeJwkView view = new FakeJwkView();
            view.draining.set(true);
            kernel = startKernel(port, view);
            Thread.sleep(DRAIN_MS);
            int drainingAttempts = view.drainingAttempts.get();

            // 5. The kernel serves again. The JWK watch must reconnect within one
            //    capped backoff step (30s) plus the channel's own reconnect.
            view.draining.set(false);
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (view.servedWatches.get() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            assertTrue(view.servedWatches.get() > 0,
                "the JWK watch never reconnected to the rolled kernel");
            // 0.5s doubling to a 30s cap gives at most ~5 attempts in a 10s
            // window. Without a backoff, the watch re-subscribes on every
            // UNAVAILABLE and hits a draining kernel thousands of times.
            assertTrue(drainingAttempts <= 10,
                "the JWK watch re-subscribed " + drainingAttempts + " times in "
                    + DRAIN_MS + "ms against a draining kernel; expected a backoff");
        } finally {
            if (authorizer != null) {
                authorizer.close();
            }
            if (stateStoreConn != null) {
                stateStoreConn.close();
            }
            kernel.shutdownNow();
        }
    }
}
