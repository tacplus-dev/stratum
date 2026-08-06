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

/**
 * ColdStartRebuilder replays all events from eventbus starting at offset 0
 * on every startup, regardless of how many events survived in-memory.
 *
 * Replay is always safe: events are keyed by event_id in an IMap (put =
 * upsert), so replaying events already in the store overwrites them with
 * identical data. A partially-surviving store is converged to the full set
 * from the log — no records are lost whether the store came back empty,
 * partial, or complete.
 */
final class ColdStartRebuilder {

    private static final Logger log = LoggerFactory.getLogger(ColdStartRebuilder.class);
    private static final String CONSUMER_GROUP = "stratum-rebuild";

    private final List<Thread> replayThreads = new ArrayList<>();
    private final AtomicLong restoredCount = new AtomicLong(0);

    /**
     * Always returns true: stratum replays from offset 0 on every startup.
     * The count parameter is accepted (not ignored) so callers can log it,
     * but it does not gate the decision.
     * Package-private for testing.
     */
    static boolean shouldReplay(long totalCount) {
        return true;
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
        log.info("STARTUP REPLAY: replaying from event log (offset=0) to converge in-memory store");

        // Use the ServiceManager's own identity and channel plumbing. This used to
        // stand up a SECOND IdentityManager from sm.getConfig(), which duplicated TLS
        // bootstrap, token exchange and the health monitor for the lifetime of the
        // replay. Drill 7 measured the eventbus KRaft controller being overwhelmed by
        // consumer churn, and a redundant identity plane per replay is that churn,
        // self-inflicted. tacplus ea54a2ea made this surface public precisely so the
        // duplicate could go.
        Supplier<Channel> channelSupplier = () -> sm.resolveService("eventbus");
        Supplier<CallCredentials> credsSupplier = () -> sm.callCredentialsForAudience("eventbus");

        startTopicReplay("agentsensor.event_stream", channelSupplier, credsSupplier,
                new AgentsensorAgentEventBusEventStreamSubscriber(channelSupplier, credsSupplier, CONSUMER_GROUP),
                (sub) -> sub.subscribe((event, meta) -> {
                    handler.handleEventStreamMessage("agentsensor.event_stream", event,
                            new Handler.SubscriptionMeta(meta.offset, meta.timestamp, meta.key, meta.topic, meta.partition));
                    logProgress(restoredCount.incrementAndGet());
                }, false, false, 0));

        startTopicReplay("gitlabconnector.event_stream", channelSupplier, credsSupplier,
                new GitlabconnectorGitLabConnectorEventBusEventStreamSubscriber(channelSupplier, credsSupplier, CONSUMER_GROUP),
                (sub) -> sub.subscribe((event, meta) -> {
                    handler.handleEventStreamMessage("gitlabconnector.event_stream", event,
                            new Handler.SubscriptionMeta(meta.offset, meta.timestamp, meta.key, meta.topic, meta.partition));
                    logProgress(restoredCount.incrementAndGet());
                }, false, false, 0));

        startTopicReplay("llmsensor.event_stream", channelSupplier, credsSupplier,
                new LlmsensorLlmEventBusEventStreamSubscriber(channelSupplier, credsSupplier, CONSUMER_GROUP),
                (sub) -> sub.subscribe((event, meta) -> {
                    handler.handleEventStreamMessage("llmsensor.event_stream", event,
                            new Handler.SubscriptionMeta(meta.offset, meta.timestamp, meta.key, meta.topic, meta.partition));
                    logProgress(restoredCount.incrementAndGet());
                }, false, false, 0));

        startTopicReplay("scmsensor.event_stream", channelSupplier, credsSupplier,
                new ScmsensorScmEventBusEventStreamSubscriber(channelSupplier, credsSupplier, CONSUMER_GROUP),
                (sub) -> sub.subscribe((event, meta) -> {
                    handler.handleEventStreamMessage("scmsensor.event_stream", event,
                            new Handler.SubscriptionMeta(meta.offset, meta.timestamp, meta.key, meta.topic, meta.partition));
                    logProgress(restoredCount.incrementAndGet());
                }, false, false, 0));

        log.info("STARTUP REPLAY: started {} subscription threads from offset 0", replayThreads.size());
    }

    private void logProgress(long count) {
        if (count % 25 == 0) {
            log.info("STARTUP REPLAY: upserted {} records so far", count);
        }
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
                log.warn("STARTUP REPLAY: subscription for {} ended: {}", topic, e.getMessage());
            }
            log.info("STARTUP REPLAY: thread for {} finished, total upserted so far: {}", topic, restoredCount.get());
        }, "startup-replay-" + topic);
        t.setDaemon(true);
        t.start();
        replayThreads.add(t);
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
        log.info("STARTUP REPLAY: shutdown complete, total upserted: {}", restoredCount.get());
    }

}
