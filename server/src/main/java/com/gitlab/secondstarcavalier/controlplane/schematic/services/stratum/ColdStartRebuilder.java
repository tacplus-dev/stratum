// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.CallCredentials;
import io.grpc.Channel;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.s2sm.Handler;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.s2sm.ServiceManager;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.s2sm.subscribe.AgentsensorAgentEventBusEventStreamSubscriber;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.services.eventbus.AdminGrpc;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.services.eventbus.Eventbus;

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
    private final ReplayProgress progress = new ReplayProgress();
    private final java.util.concurrent.atomic.AtomicBoolean caughtUpSignalled =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    /**
     * How far the replay has got against the end of the log as it stood when the
     * replay started, and therefore whether the read gate may open.
     *
     * <p>The target is a WATERMARK, not a quiescence heuristic. eventbus reports
     * each partition's latest offset — the Kafka high watermark, i.e. the offset
     * the next write will take — so the last record already in the log sits at
     * {@code latest - 1}, and delivering it is the exact, non-approximate moment
     * this partition's backlog is drained. "No records for a while" would be the
     * alternative and is not used: on a slow broker it opens the gate early, which
     * is the bug being fixed, and on a busy topic it never opens at all.
     *
     * <p>An empty partition reports {@code latest == 0} and is never outstanding,
     * so a deployment whose topics hold nothing opens immediately rather than
     * waiting for a record that will never come.
     */
    static final class ReplayProgress {
        private final Map<String, Long> outstanding = new HashMap<>();

        private static String key(String topic, int partition) {
            return topic + "#" + partition;
        }

        /** Registers a partition's backlog. A zero watermark is already drained. */
        synchronized void expect(String topic, int partition, long highWatermark) {
            if (highWatermark > 0) {
                outstanding.put(key(topic, partition), highWatermark);
            }
        }

        /** Records a delivery, clearing the partition once it reaches the target. */
        synchronized void observed(String topic, int partition, long offset) {
            String k = key(topic, partition);
            Long target = outstanding.get(k);
            if (target != null && offset >= target - 1) {
                outstanding.remove(k);
            }
        }

        synchronized boolean isCaughtUp() {
            return outstanding.isEmpty();
        }
    }

    /**
     * Replay the event log only on a COLD BOOT.
     *
     * A cold boot is: the store is empty CLUSTER-WIDE. The count is taken from
     * the shared Hazelcast map, so a member that restarts alongside live peers
     * sees their data and skips — which is the point: in production, where the
     * service runs several instances, a single restart or failure is the COMMON
     * case and must not drag the whole event log back through the broker.
     *
     * Member count deliberately does NOT gate this. It used to — "alone in the
     * cluster AND empty" — and that was wrong in the one situation the gate
     * exists for. When the whole StatefulSet restarts, all three members join
     * each other before any of them looks at the store, so every one of them
     * sees clusterSize=3 with an empty map, every one declines to replay, and
     * the store stays empty forever. Verified live: three pods, all logging
     * "0 records held, 3 cluster member(s) — skipping", and a permanently empty
     * event store. The member count answers "am I alone?", which is not the
     * question; "is the data already here?" is, and the count answers it.
     *
     * Every member that observes an empty store replays. That is redundant work
     * on a true cold boot — rare, by design — and it is deliberately preferred
     * to electing one replayer: the upserts are keyed by event_id and therefore
     * idempotent, so concurrent replays converge, and no single member's death
     * mid-replay can leave the store permanently half-filled.
     *
     * Package-private for testing.
     */
    static boolean shouldReplay(long totalCount) {
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
    /**
     * @param onCaughtUp run once, from the replay thread, when every replayed
     *                   topic has been delivered up to where its log ended when
     *                   the replay started. This is what opens the read gate, so
     *                   it must not run before that point — and must still run
     *                   when there is nothing to replay, or the gate never opens.
     */
    void startReplay(ServiceManager sm, Handler handler, Runnable onCaughtUp) {
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

        // Only agentsensor remains. gitlabconnector, llmsensor and scmsensor were
        // vservices defined in proto-app -- the APPLICATION layer -- and a platform
        // service replaying application event streams is exactly the coupling the
        // schematic/application split removes. Their topics move with the app.
        String topic = "agentsensor.event_stream";
        startTopicReplay(topic, channelSupplier, credsSupplier,
                new AgentsensorAgentEventBusEventStreamSubscriber(channelSupplier, credsSupplier, CONSUMER_GROUP),
                (sub) -> sub.subscribe((event, meta) -> {
                    handler.handleEventStreamMessage(topic, event,
                            new Handler.SubscriptionMeta(meta.offset, meta.timestamp, meta.key, meta.topic, meta.partition));
                    logProgress(restoredCount.incrementAndGet());
                    // Keyed on the topic we ASKED for, not meta.topic: the envelope
                    // carries an empty topic when the record predates the meta
                    // envelope, and a progress key that does not match the one
                    // expect() registered would never clear.
                    progress.observed(topic, meta.partition, meta.offset);
                    signalIfCaughtUp(onCaughtUp);
                }, false, false, 0),
                onCaughtUp);

        log.info("STARTUP REPLAY: started {} subscription threads from offset 0", replayThreads.size());
    }

    /** Opens the read gate the first time every topic's backlog is drained. */
    private void signalIfCaughtUp(Runnable onCaughtUp) {
        if (progress.isCaughtUp() && caughtUpSignalled.compareAndSet(false, true)) {
            log.info("STARTUP REPLAY: caught up to the end of the event log ({} record(s) upserted);"
                    + " hot reads may open", restoredCount.get());
            onCaughtUp.run();
        }
    }

    /**
     * Asks eventbus where each partition of {@code topic} currently ends, so the
     * replay knows when it has drained the backlog.
     *
     * <p>Retries indefinitely rather than giving up on a deadline, and the read
     * gate stays CLOSED while it does. That is deliberate: this runs only on a
     * cold boot, where the store is empty cluster-wide, so a store that cannot
     * establish how much it has to recover genuinely cannot answer a query — the
     * same posture as {@code HotRestore.awaitClusterReplay}, which refuses rather
     * than serving a silently incomplete store. A transient eventbus outage
     * therefore delays the gate and resolves itself; it does not open the gate on
     * an unrecovered store.
     *
     * <p>A topic eventbus does not know is not an error: nothing was ever written
     * to it, so there is nothing to replay and the backlog is empty.
     */
    private void awaitWatermarks(String topic, Supplier<Channel> channelSupplier,
            Supplier<CallCredentials> credsSupplier) throws InterruptedException {
        long backoff = 500;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Eventbus.TopicInfo info = AdminGrpc.newBlockingStub(channelSupplier.get())
                        .withCallCredentials(credsSupplier.get())
                        .getTopicInfo(Eventbus.TopicInfoRequest.newBuilder().setTopic(topic).build());
                for (Map.Entry<Integer, Long> e : info.getLatestOffsetsMap().entrySet()) {
                    progress.expect(topic, e.getKey(), e.getValue());
                }
                log.info("STARTUP REPLAY: {} ends at {}", topic, info.getLatestOffsetsMap());
                return;
            } catch (io.grpc.StatusRuntimeException e) {
                if (e.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                    log.info("STARTUP REPLAY: {} does not exist — nothing to replay", topic);
                    return;
                }
                log.warn("STARTUP REPLAY: cannot read {} watermarks ({}); hot reads stay closed,"
                        + " retrying in {}ms", topic, e.getStatus().getCode(), backoff);
                Thread.sleep(backoff);
                backoff = Math.min(backoff * 2, 30_000);
            }
        }
        throw new InterruptedException("interrupted awaiting " + topic + " watermarks");
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
            Supplier<CallCredentials> credsSupplier, S subscriber, SubscribeAction<S> action,
            Runnable onCaughtUp) {
        Thread t = new Thread(() -> {
            try {
                // Before subscribing, not after: the target must be where the log
                // ended BEFORE this replay started reading it, or a topic taking
                // writes throughout the replay moves its own finish line.
                awaitWatermarks(topic, channelSupplier, credsSupplier);
                // An empty topic has no record to arrive and clear the gate, so the
                // drained backlog has to be noticed here as well as on delivery.
                signalIfCaughtUp(onCaughtUp);
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
