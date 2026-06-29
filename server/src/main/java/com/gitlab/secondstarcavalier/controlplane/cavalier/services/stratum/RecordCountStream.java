// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.model.types.services.stratum.Stratum;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

/**
 * Vservice-neutral live record count for the generated RecordMap
 * Subscribe&lt;T&gt;Count handlers. Emits the current count of records matching a
 * predicate, then a new count whenever a matching record is added or removed
 * (eviction / retention / expiry) — backed by a Hazelcast entry listener, so the
 * count stays accurate without polling and without materializing records.
 *
 * Names no vservice: it operates on IMap&lt;String,T&gt; + a caller-supplied
 * Predicate&lt;T&gt; (the generated match&lt;T&gt;), so adding a record type needs no
 * edit here. Cluster-wide: the entry listener fires for matching changes on any
 * member; the count starts from a cluster-wide values() scan.
 */
public final class RecordCountStream {

    private RecordCountStream() {}

    public static <T> void subscribe(
            IMap<String, T> map,
            Predicate<T> match,
            StreamObserver<Stratum.RecordCount> obs) {

        AtomicLong count = new AtomicLong(0);
        for (T v : map.values()) {
            if (match.test(v)) {
                count.incrementAndGet();
            }
        }

        // gRPC stream observers are not safe for concurrent onNext; the initial
        // emit and listener-driven emits are serialized through this lock.
        Object lock = new Object();
        LongConsumer emit = c -> {
            synchronized (lock) {
                obs.onNext(Stratum.RecordCount.newBuilder().setCount(c).build());
            }
        };
        emit.accept(count.get());

        UUID regId = map.addEntryListener(new CountListener<>(match, count, emit), true);

        // Deregister when the client cancels (the stream stays open until then).
        if (obs instanceof ServerCallStreamObserver) {
            ((ServerCallStreamObserver<Stratum.RecordCount>) obs).setOnCancelHandler(() -> {
                try {
                    map.removeEntryListener(regId);
                } catch (Exception ignored) {
                }
            });
        }
    }

    /** +1 on matching add, -1 on matching remove/evict/expire. */
    private static final class CountListener<T> implements
            EntryAddedListener<String, T>,
            EntryRemovedListener<String, T>,
            EntryEvictedListener<String, T>,
            EntryExpiredListener<String, T> {

        private final Predicate<T> match;
        private final AtomicLong count;
        private final LongConsumer emit;

        CountListener(Predicate<T> match, AtomicLong count, LongConsumer emit) {
            this.match = match;
            this.count = count;
            this.emit = emit;
        }

        @Override
        public void entryAdded(EntryEvent<String, T> e) {
            if (e.getValue() != null && match.test(e.getValue())) {
                emit.accept(count.incrementAndGet());
            }
        }

        @Override
        public void entryRemoved(EntryEvent<String, T> e) {
            onGone(e);
        }

        @Override
        public void entryEvicted(EntryEvent<String, T> e) {
            onGone(e);
        }

        @Override
        public void entryExpired(EntryEvent<String, T> e) {
            onGone(e);
        }

        private void onGone(EntryEvent<String, T> e) {
            T v = e.getOldValue() != null ? e.getOldValue() : e.getValue();
            if (v != null && match.test(v)) {
                emit.accept(count.decrementAndGet());
            }
        }
    }
}
