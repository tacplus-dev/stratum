// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.protobuf.services.HealthStatusManager;

import com.github.tacplusdev.stratum.model.chassis.grpcserver.GrpcServer;
import com.github.tacplusdev.stratum.model.types.services.stratum.Stratum;
import com.github.tacplusdev.stratum.model.types.sys.grpcconfig.Grpcconfig;

/**
 * Stratum's startup runs two barriers back to back inside
 * {@code EventStore.init} — the writer-quorum wait
 * ({@code StratumMapConfig.withWriterQuorum}, unbounded) and this node's redo
 * replay plus peer catch-up ({@code EventStore.RESTORE_TIMEOUT}, 5 min) — so a
 * slow-but-healthy cold start can sit there for minutes before the handler
 * returns. A pod that answers nothing at all through that window looks
 * indistinguishable from a hung one, which is how the incident behind this
 * barrier work ended: a JVM that had stopped making progress, no listener bound,
 * and a kill with no indication of why.
 *
 * <p>What makes the wait survivable is that the listener a probe talks to is
 * bound BEFORE any of it, and answers {@code NOT_SERVING} rather than refusing
 * the connection. That listener is not stratum's to build: the generated chassis
 * binds a plaintext grpc.health.v1 server in {@code ServiceManager.create()},
 * with the status already set to NOT_SERVING, and only flips it to SERVING via
 * {@code GrpcServer.setReady(true)} once bootstrap has finished — which is after
 * {@code run()} has delivered config to the handler and the barriers have
 * therefore completed. This test pins stratum's half of that contract, driving
 * the real generated pieces in the order the chassis drives them:
 *
 * <ol>
 *   <li>the probe listener is bound and NOT_SERVING before any config reaches
 *       the handler;
 *   <li>{@code handleRecordMapConfig} — the generated entry point
 *       {@code ServiceManager.run()} calls — blocks in the barriers, and a real
 *       health client gets a connected, NOT_SERVING answer at each of them;
 *   <li>nothing on stratum's init path announces SERVING: the status is still
 *       NOT_SERVING when the call returns, and only the chassis's own
 *       {@code setReady(true)} moves it.
 * </ol>
 *
 * <p>The barriers cannot be driven through a real {@link EventStore}: the first
 * one waits on a Hazelcast cluster forming and the second on peers filing their
 * replay reports. {@code newEventStore()} exists for that, and the fake below
 * stops at a latch in each barrier's place, so the window under test is entered
 * deterministically rather than raced for.
 */
class StartupHealthGateTest {

    /**
     * An EventStore that never touches Hazelcast, stopping at two latches where
     * init's two barriers are. {@code eventCount} answers non-zero so the
     * bring-up takes the warm-start branch — the cold-boot branch starts a
     * {@code ColdStartRebuilder} against the ServiceManager, which is not what
     * is being gated here.
     */
    private static class BarrierStore extends EventStore {
        private final CountDownLatch formationEntered;
        private final CountDownLatch releaseFormation;
        private final CountDownLatch replayEntered;
        private final CountDownLatch releaseReplay;

        BarrierStore(CountDownLatch formationEntered, CountDownLatch releaseFormation,
                     CountDownLatch replayEntered, CountDownLatch releaseReplay) {
            this.formationEntered = formationEntered;
            this.releaseFormation = releaseFormation;
            this.replayEntered = replayEntered;
            this.releaseReplay = releaseReplay;
        }

        @Override
        public void init(long cleanupSecs, List<String> seeds) {
            formationEntered.countDown();
            awaitOrFail(releaseFormation, "cluster-formation barrier");
            replayEntered.countDown();
            awaitOrFail(releaseReplay, "redo-replay barrier");
        }

        @Override public long eventCount() { return 1; }
        @Override public int clusterSize() { return 1; }
        @Override public void shutdown() { }
    }

    private static void awaitOrFail(CountDownLatch latch, String what) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test never released the " + what);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted at the " + what, e);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static HealthCheckResponse.ServingStatus probeStatus(HealthGrpc.HealthBlockingStub client) {
        return client.check(HealthCheckRequest.newBuilder().setService("").build()).getStatus();
    }

    @Test
    void theHealthProbeAnswersNotServingAtBothStartupBarriersAndOnlyTheChassisFlipsItToServing()
            throws Exception {
        // What ServiceManager.create() does, before a single line of stratum's own
        // init runs: one HealthStatusManager, NOT_SERVING, behind a plaintext
        // grpc.health.v1 listener.
        HealthStatusManager health = new HealthStatusManager();
        health.setStatus("", HealthCheckResponse.ServingStatus.NOT_SERVING);
        int port = freePort();
        Server probeServer = GrpcServer.startHealthProbeServer(port, health);
        assertNotNull(probeServer, "the generated chassis must bind the plaintext health probe listener");
        // The same manager the main TLS server is handed in run(), so setReady()
        // below is the production flip rather than a restatement of it.
        GrpcServer srv = new GrpcServer(Grpcconfig.Config.getDefaultInstance(), null, null, health);

        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", port)
                .usePlaintext()
                .build();
        try {
            HealthGrpc.HealthBlockingStub probe = HealthGrpc.newBlockingStub(channel);

            CountDownLatch formationEntered = new CountDownLatch(1);
            CountDownLatch releaseFormation = new CountDownLatch(1);
            CountDownLatch replayEntered = new CountDownLatch(1);
            CountDownLatch releaseReplay = new CountDownLatch(1);

            StratumHandler handler = new StratumHandler(null) {
                @Override
                EventStore newEventStore() {
                    return new BarrierStore(formationEntered, releaseFormation,
                            replayEntered, releaseReplay);
                }
            };

            AtomicReference<Throwable> failure = new AtomicReference<>();
            // handleRecordMapConfig, not handleConfig: this is the generated Handler
            // method ServiceManager.run() calls to deliver the first config, and it
            // is the call the barriers run inside of in production.
            Thread configDelivery = new Thread(() -> {
                try {
                    handler.handleRecordMapConfig(Stratum.Config.newBuilder().setRetentionDays(7).build());
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }, "chassis-config-delivery");
            configDelivery.start();

            assertTrue(formationEntered.await(30, TimeUnit.SECONDS),
                    "the bring-up must reach the cluster-formation barrier");
            assertEquals(HealthCheckResponse.ServingStatus.NOT_SERVING, probeStatus(probe),
                    "with the cluster-formation barrier still pending a probe must get a CONNECTED,"
                            + " NOT_SERVING answer — a refused connection is what makes a slow start"
                            + " indistinguishable from a hung process");

            releaseFormation.countDown();
            assertTrue(replayEntered.await(30, TimeUnit.SECONDS),
                    "the bring-up must reach the redo-replay barrier");
            assertEquals(HealthCheckResponse.ServingStatus.NOT_SERVING, probeStatus(probe),
                    "the redo-replay barrier is the second, longer wait and it is still pending");

            releaseReplay.countDown();
            configDelivery.join(30_000);
            assertNull(failure.get(), "the bring-up must not fail: " + failure.get());
            assertFalse(configDelivery.isAlive(), "the bring-up must have returned");

            // Both barriers are done and stratum's init has returned. It must not
            // have announced readiness itself: the status is the chassis's to move,
            // and it moves it after bootstrap — which is why the SERVING flip cannot
            // land while either barrier is pending.
            assertEquals(HealthCheckResponse.ServingStatus.NOT_SERVING, probeStatus(probe),
                    "stratum's init path must not flip the health endpoint to SERVING;"
                            + " the chassis owns that, after bootstrap completes");

            srv.setReady(true);
            assertEquals(HealthCheckResponse.ServingStatus.SERVING, probeStatus(probe),
                    "once both barriers have completed and the chassis finishes bootstrap the same"
                            + " listener must report SERVING");
        } finally {
            channel.shutdownNow();
            probeServer.shutdownNow();
        }
    }
}
