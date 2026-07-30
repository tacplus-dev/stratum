// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.CallCredentials;
import io.grpc.Channel;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.Handler;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.ServiceManager;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.subscribe.AgentsensorAgentEventBusEventStreamSubscriber;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.subscribe.GitlabconnectorGitLabConnectorEventBusEventStreamSubscriber;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.subscribe.LlmsensorLlmEventBusEventStreamSubscriber;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.s2sm.subscribe.ScmsensorScmEventBusEventStreamSubscriber;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.chassis.service_im.IdentityManager;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.chassis.serviceconnection.ServiceConnection;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.services.kernel.Kernel;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.services.kernel.ServiceHealthStateServiceGrpc;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.sys.healthreport.Healthreport.InstanceStatus;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.sys.healthreport.Healthreport.ServiceInstanceHealth;

/**
 * ColdStartRebuilder detects when stratum's in-memory store is empty (cold
 * start / fresh pod) and rebuilds it by replaying all events from eventbus
 * starting at offset 0.
 *
 * On a warm restart (Hazelcast cluster retained data), no replay is triggered.
 *
 * Idempotency: events are keyed by event_id in an IMap (put = upsert), so
 * replaying events that were already stored is safe.
 */
final class ColdStartRebuilder {

    private static final Logger log = LoggerFactory.getLogger(ColdStartRebuilder.class);
    private static final String CONSUMER_GROUP = "stratum-rebuild";

    private final List<Thread> replayThreads = new ArrayList<>();
    private IdentityManager replayIdentity;
    private final AtomicLong restoredCount = new AtomicLong(0);

    /**
     * Returns true if the store is empty (cold start), false if populated (warm).
     */
    static boolean isColdStart(StratumEventStores stores) {
        return isColdStart(stores.totalCount());
    }

    /**
     * Pure decision: total record count of 0 means cold start.
     * Package-private for testing.
     */
    static boolean isColdStart(long totalCount) {
        return totalCount == 0;
    }

    /**
     * Starts cold-start replay from offset 0 for all event-bus topics.
     * Creates background daemon threads that subscribe to eventbus with
     * explicit offset=0 (bypassing committed consumer-group offsets).
     *
     * The replay uses a dedicated consumer group ("stratum-rebuild") so it
     * does not interfere with the normal "stratum" consumer group's offset
     * tracking.
     */
    void startReplay(ServiceManager sm, Handler handler) {
        log.info("COLD START detected: event store is empty, rebuilding from event log (offset=0)");

        try {
            replayIdentity = IdentityManager.create(sm.getConfig());
        } catch (Exception e) {
            log.error("COLD START: failed to create identity for replay — events will not be rebuilt", e);
            return;
        }

        ServiceConnection.ChannelFactory channelFactory;
        try {
            channelFactory = ServiceConnection.channelFactory(sm.getConfig());
        } catch (Exception e) {
            log.error("COLD START: failed to create channel factory for replay", e);
            closeReplayIdentity();
            return;
        }

        Supplier<Channel> channelSupplier = () -> resolveEventbus(sm, channelFactory);
        Supplier<CallCredentials> credsSupplier = () -> replayIdentity.callCredentialsForAudience("eventbus");

        startTopicReplay("agentsensor.event_stream", channelSupplier, credsSupplier,
                new AgentsensorAgentEventBusEventStreamSubscriber(channelSupplier, credsSupplier, CONSUMER_GROUP),
                (sub) -> sub.subscribe((event, meta) -> {
                    handler.handleEventStreamMessage("agentsensor.event_stream", event,
                            new Handler.SubscriptionMeta(meta.offset, meta.timestamp, meta.key, meta.topic, meta.partition));
                    restoredCount.incrementAndGet();
                }, false, false, 0));

        startTopicReplay("gitlabconnector.event_stream", channelSupplier, credsSupplier,
                new GitlabconnectorGitLabConnectorEventBusEventStreamSubscriber(channelSupplier, credsSupplier, CONSUMER_GROUP),
                (sub) -> sub.subscribe((event, meta) -> {
                    handler.handleEventStreamMessage("gitlabconnector.event_stream", event,
                            new Handler.SubscriptionMeta(meta.offset, meta.timestamp, meta.key, meta.topic, meta.partition));
                    restoredCount.incrementAndGet();
                }, false, false, 0));

        startTopicReplay("llmsensor.event_stream", channelSupplier, credsSupplier,
                new LlmsensorLlmEventBusEventStreamSubscriber(channelSupplier, credsSupplier, CONSUMER_GROUP),
                (sub) -> sub.subscribe((event, meta) -> {
                    handler.handleEventStreamMessage("llmsensor.event_stream", event,
                            new Handler.SubscriptionMeta(meta.offset, meta.timestamp, meta.key, meta.topic, meta.partition));
                    restoredCount.incrementAndGet();
                }, false, false, 0));

        startTopicReplay("scmsensor.event_stream", channelSupplier, credsSupplier,
                new ScmsensorScmEventBusEventStreamSubscriber(channelSupplier, credsSupplier, CONSUMER_GROUP),
                (sub) -> sub.subscribe((event, meta) -> {
                    handler.handleEventStreamMessage("scmsensor.event_stream", event,
                            new Handler.SubscriptionMeta(meta.offset, meta.timestamp, meta.key, meta.topic, meta.partition));
                    restoredCount.incrementAndGet();
                }, false, false, 0));

        log.info("COLD START: started {} replay subscription threads from offset 0", replayThreads.size());
    }

    @FunctionalInterface
    interface SubscribeAction<S> {
        void subscribe(S subscriber) throws Exception;
    }

    private <S> void startTopicReplay(String topic, Supplier<Channel> channelSupplier,
            Supplier<CallCredentials> credsSupplier, S subscriber, SubscribeAction<S> action) {
        Thread t = new Thread(() -> {
            try {
                action.subscribe(subscriber);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.warn("COLD START: replay for {} ended: {}", topic, e.getMessage());
            }
            log.info("COLD START: replay thread for {} finished, total restored so far: {}", topic, restoredCount.get());
        }, "cold-start-replay-" + topic);
        t.setDaemon(true);
        t.start();
        replayThreads.add(t);
    }

    /**
     * Resolves the eventbus endpoint via ServiceHealthState, same approach
     * as the generated ServiceManager's resolveService.
     */
    private static io.grpc.ManagedChannel resolveEventbus(ServiceManager sm,
            ServiceConnection.ChannelFactory channelFactory) {
        long backoff = 2000;
        final long maxBackoff = 30_000;
        while (true) {
            try {
                var stub = ServiceHealthStateServiceGrpc.newBlockingStub(sm.conn())
                        .withCallCredentials(new io.grpc.CallCredentials() {
                            @Override
                            public void applyRequestMetadata(RequestInfo requestInfo,
                                    java.util.concurrent.Executor appExecutor,
                                    io.grpc.CallCredentials.MetadataApplier applier) {
                                appExecutor.execute(() -> {
                                    try {
                                        io.grpc.Metadata headers = new io.grpc.Metadata();
                                        headers.put(
                                                io.grpc.Metadata.Key.of("authorization",
                                                        io.grpc.Metadata.ASCII_STRING_MARSHALLER),
                                                "Bearer " + sm.getToken());
                                        applier.apply(headers);
                                    } catch (Exception e) {
                                        applier.fail(io.grpc.Status.UNAUTHENTICATED.withCause(e));
                                    }
                                });
                            }
                        });
                Kernel.InstancesEntriesResponse resp = stub.getAllInstances(
                        Kernel.InstancesGetAllRequest.getDefaultInstance());
                for (ServiceInstanceHealth entry : resp.getEntriesMap().values()) {
                    if ("eventbus".equals(entry.getServiceId())
                            && entry.getStatus() == InstanceStatus.INSTANCE_STATUS_HEALTHY) {
                        var ep = entry.getGrpcEndpoint();
                        if (ep != null && !ep.getHost().isEmpty() && ep.getPort() > 0) {
                            String target = ep.getHost() + ":" + ep.getPort();
                            log.info("COLD START: resolved eventbus endpoint: {}", target);
                            return channelFactory.create("eventbus", target);
                        }
                    }
                }
                log.warn("COLD START: no healthy eventbus found, retrying in {}ms", backoff);
            } catch (Exception e) {
                log.warn("COLD START: failed to resolve eventbus: {}, retrying in {}ms", e.getMessage(), backoff);
            }
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("interrupted while resolving eventbus");
            }
            backoff = Math.min(backoff * 2, maxBackoff);
        }
    }

    long restoredCount() {
        return restoredCount.get();
    }

    void shutdown() {
        for (Thread t : replayThreads) {
            t.interrupt();
        }
        for (Thread t : replayThreads) {
            try {
                t.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        replayThreads.clear();
        closeReplayIdentity();
        log.info("COLD START: replay shutdown complete, total records restored: {}", restoredCount.get());
    }

    private void closeReplayIdentity() {
        if (replayIdentity != null) {
            replayIdentity.close();
            replayIdentity = null;
        }
    }
}
