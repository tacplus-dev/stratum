// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import com.github.tacplusdev.stratum.hazelcast.StratumMapConfig;
import com.github.tacplusdev.stratum.store.GrpcRedoLog;
import com.github.tacplusdev.stratum.store.RecordStore;
import com.github.tacplusdev.stratum.model.s2sm.ServiceManager;
import com.github.tacplusdev.stratum.model.chassis.collection.CollectionRuntime;
import com.github.tacplusdev.stratum.model.types.services.stratum.Stratum;
import com.github.tacplusdev.stratum.model.types.services.stratum.Helpers;
import com.github.tacplusdev.stratum.model.chassis.servicemanager.Caller;
import com.github.tacplusdev.stratum.model.types.sys.errors.ProtoException;
import com.github.tacplusdev.stratum.model.types.sys.x509.X509;

/**
 * StratumHandler brings the tenant's record store up and wires it to the
 * generated record plane.
 *
 * <p>The store is built once, on the first config (or the plane's lazy init):
 * the Hazelcast member, the record store over the redo service's generated
 * client, and a keeper thread that replays the tenant from the redo whenever
 * this member is the sequencer and does not hold its place in the log — at
 * start, after it takes over from a sequencer that left, and after a write
 * whose ack never came. Until the first replay finishes the plane answers
 * UNAVAILABLE.
 *
 * <p>A later config never rebuilds the store: its rows are memory, and tearing
 * the member down would turn a config change into a replay. The ack timeout
 * applies at once; hazelcast_memory_mb at the next start.
 */
public class StratumHandler extends StratumVServiceBase {

    private static final Logger log = LoggerFactory.getLogger(StratumHandler.class);

    /** The longest the keeper waits between replay attempts while the redo is unreachable. */
    private static final long MAX_REPLAY_BACKOFF_MS = 30_000;

    private final ServiceManager sm;
    private volatile Stratum.Config config;
    // volatile: the record plane reads these from gRPC threads on every call.
    private volatile RecordStore store;
    private volatile StratumStores stores;
    private Thread keeper;
    private StratumRecordMaps recordMaps;

    public StratumHandler(ServiceManager sm) {
        this.sm = sm;
    }

    // -----------------------------------------------------------------------
    // StratumVServiceBase — vservice wiring
    // -----------------------------------------------------------------------

    /**
     * The one record plane implementation, shared by both caller surfaces.
     * Created once: the handle registry lives in the instance, so returning a
     * fresh one per surface would give each surface its own set of query ids.
     */
    @Override
    protected synchronized StratumRecordMaps provideRecordPlane() {
        if (store == null) {
            applyConfig(Helpers.configDefaults(Stratum.Config.getDefaultInstance()).build());
        }
        if (recordMaps == null) {
            // Suppliers, not captured references, so the plane sees the store once
            // it is up. The query registry is a Hazelcast map: a handle opened on one
            // member is served by whichever member the client fails over to.
            recordMaps = new StratumRecordMaps(() -> stores, () -> store,
                    () -> store.hazelcast().getMap(StratumMapConfig.RECORD_QUERIES_MAP));
        }
        return recordMaps;
    }

    @Override
    protected void handleConfig(Stratum.Config cfg) throws Exception {
        applyConfig(cfg);
    }

    @Override
    public void handleStratumConfigStateConfig(Stratum.Config cfg) throws Exception {
        applyConfig(cfg);
    }

    @Override
    public void handleStratumStateConfig(Stratum.Config cfg) throws Exception {
        // StratumState has no config singleton; nothing to do.
    }

    @Override
    public com.github.tacplusdev.stratum.model.s2sm.internal.grpc.servers.services.stratum.StratumStateService.StratumStateServiceHandler getStratumStateGrpcImpl() {
        return new com.github.tacplusdev.stratum.model.s2sm.internal.grpc.servers.services.stratum.StratumStateService.StratumStateServiceHandler() {};
    }

    @Override
    public com.github.tacplusdev.stratum.model.s2sm.internal.grpc.servers.services.stratum.StratumConfigStateService.StratumConfigStateServiceHandler getStratumConfigStateGrpcImpl() {
        return new com.github.tacplusdev.stratum.model.s2sm.internal.grpc.servers.services.stratum.StratumConfigStateService.StratumConfigStateServiceHandler() {
            @Override
            public Stratum.Config getConfig(Caller caller) throws ProtoException {
                Stratum.Config cfg = config;
                return cfg != null ? cfg : Stratum.Config.getDefaultInstance();
            }
            @Override
            public Stratum.Config setConfig(Caller caller, Stratum.Config request) throws ProtoException {
                try {
                    applyConfig(request);
                } catch (RuntimeException e) {
                    throw new ProtoException(ProtoException.INTERNAL, e.getMessage());
                }
                return request;
            }
            @Override
            public void watchConfig(Caller caller, StreamObserver<Stratum.Config> responseObserver) throws ProtoException {
                Stratum.Config cfg = config;
                if (cfg != null) responseObserver.onNext(cfg);
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * Hazelcast seed members, from STRATUM_SEED_MEMBERS: one "host:5001" entry per
     * member of this tenant's store, rendered by the operator from the
     * StatefulSet's stable per-pod DNS. Empty (a plain local run) falls back to
     * multicast, which in Kubernetes would leave every member in a cluster of one.
     */
    private static List<String> seedMembers() {
        String raw = System.getenv("STRATUM_SEED_MEMBERS");
        if (raw == null || raw.isBlank()) {
            log.info("STRATUM_SEED_MEMBERS unset — Hazelcast will use multicast (single-node/dev)");
            return List.of();
        }
        List<String> members = Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(m -> !m.isEmpty())
                .toList();
        log.info("Hazelcast seed members: {}", members);
        return members;
    }

    /**
     * Applies a config: the first one brings the store up, a later one adjusts it.
     * {@code synchronized} because it is reachable from the lazy init, the config
     * deliveries and the setConfig RPC, on different threads, and two of them
     * bringing a member up at once would leave one joined to the cluster with
     * nobody holding it.
     */
    private synchronized void applyConfig(Stratum.Config cfg) {
        Duration ackTimeout = Duration.ofMillis(cfg.getWriteAckTimeoutMs());
        if (store != null) {
            store.setAckTimeout(ackTimeout);
            this.config = cfg;
            log.info("config: write_ack_timeout_ms={} (hazelcast_memory_mb={} applies at the next start)",
                    cfg.getWriteAckTimeoutMs(), cfg.getHazelcastMemoryMb());
            return;
        }
        log.info("config: hazelcast_memory_mb={}, write_ack_timeout_ms={}",
                cfg.getHazelcastMemoryMb(), cfg.getWriteAckTimeoutMs());
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(StratumMapConfig.build(
                seedMembers(), StratumMapConfig.configuredBackupCount(), StratumMapConfig.configuredZone()));
        try {
            RecordStore s = new RecordStore(hz, new StratumRecordTypes(),
                    new GrpcRedoLog(sm.getGrpcClients().sys().redo()), ackTimeout);
            stores = new StratumStores(hz);
            store = s;
            this.config = cfg;
            keeper = new Thread(() -> keepInSync(s), "stratum-replay");
            keeper.setDaemon(true);
            keeper.start();
        } catch (RuntimeException e) {
            // Nothing holds the member: take it out of the cluster rather than leave
            // it owning partitions nobody can reach.
            hz.shutdown();
            throw e;
        }
    }

    /**
     * Replays whenever this member is the sequencer and out of sync with the
     * log, backing off while the redo is unreachable.
     */
    private static void keepInSync(RecordStore s) {
        long backoff = 1_000;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (s.isSequencer() && !s.inSync()) {
                    s.catchUp();
                }
                backoff = 1_000;
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.warn("replay from the redo service failed, retrying in {} ms: {}", backoff, e.getMessage());
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    return;
                }
                backoff = Math.min(backoff * 2, MAX_REPLAY_BACKOFF_MS);
            }
        }
    }

    @Override
    public void newTlsCert(X509.X509CertChain certChain, byte[] privateKeyDer) throws Exception {
        // mTLS for the redo client is handled by the generated ServiceManager.
    }

    @Override
    public synchronized void shutdown() {
        log.info("stratum shutting down");
        if (keeper != null) {
            keeper.interrupt();
        }
        RecordStore s = store;
        if (s != null) {
            s.shutdown();
            s.hazelcast().shutdown();
        }
    }

    // -----------------------------------------------------------------------
    // Pagination helpers
    // -----------------------------------------------------------------------

    static String i64Key(long value) {
        return String.format("%020d", Math.max(0L, value));
    }

    private static int compareKeys(List<String> left, List<String> right) {
        int n = Math.min(left.size(), right.size());
        for (int i = 0; i < n; i++) {
            int cmp = left.get(i).compareTo(right.get(i));
            if (cmp != 0) return cmp;
        }
        return Integer.compare(left.size(), right.size());
    }

    // Package-private: reused by the generated StratumRecordMaps (RecordMap.list<T>).
    static <T> CollectionRuntime.Page<T> pageSorted(
            List<T> all,
            CollectionRuntime.PageParams page,
            Function<T, List<String>> keyFor) throws ProtoException {
        // Rejected, not clamped: a limit no int can hold is a client that has asked
        // for something it cannot mean, and silently serving it a different page
        // hides that. Unchecked, the cast wraps negative and blows up inside
        // subList below as an INTERNAL, which tells the caller nothing.
        if (page.limit > Integer.MAX_VALUE) {
            throw new ProtoException(ProtoException.INVALID_ARGUMENT,
                    "page limit " + page.limit + " exceeds the maximum of " + Integer.MAX_VALUE);
        }
        all.sort((a, b) -> compareKeys(keyFor.apply(a), keyFor.apply(b)));
        int limit = (int) Math.max(1L, page.limit);
        int start = 0;
        if (page.after != null && page.after.keys != null) {
            while (start < all.size() && compareKeys(keyFor.apply(all.get(start)), page.after.keys) <= 0) {
                start++;
            }
        }
        // In long arithmetic: start + limit is an int overflow away from negative
        // for an in-range-but-huge limit, which subList reads as an end BEFORE its
        // start.
        int end = (int) Math.min((long) start + limit, all.size());
        List<T> items = new ArrayList<>(all.subList(start, end));
        CollectionRuntime.Cursor nextAfter = end < all.size() && !items.isEmpty()
                ? new CollectionRuntime.Cursor(keyFor.apply(items.get(items.size() - 1)))
                : null;
        return new CollectionRuntime.Page<>(items, nextAfter, all.size());
    }
}
