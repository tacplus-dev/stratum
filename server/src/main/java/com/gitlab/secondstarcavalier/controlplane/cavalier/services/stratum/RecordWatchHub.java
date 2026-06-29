// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum;

import com.google.protobuf.Message;

import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Vservice-neutral live fan-out for RecordMap Watch&lt;T&gt;.
 *
 * The ingest path ({@link StratumHandler#handleEventStreamMessage}) calls
 * {@link #publish} for every stored record. Each generated watch&lt;T&gt; handler
 * opens a {@link #changes} subscription with a type/filter predicate and a
 * change-mapper; the generated server wrapper drains that iterator (via the
 * collection WatchHub) and frames it into the typed WatchEvent stream.
 *
 * Names no vservice — it operates on com.google.protobuf.Message + caller-supplied
 * predicate/mapper, so adding a record type needs no edit here.
 */
public final class RecordWatchHub {

    private final CopyOnWriteArrayList<Subscription<?>> subs = new CopyOnWriteArrayList<>();
    private final AtomicLong revision = new AtomicLong();

    /** Monotonic revision, bumped per published record. */
    public long currentRevision() {
        return revision.get();
    }

    /** Fan a stored record out to every matching live subscription. */
    public void publish(Message record) {
        long rev = revision.incrementAndGet();
        for (Subscription<?> s : subs) {
            s.offer(record, rev);
        }
    }

    /**
     * A live change stream: records matching {@code match}, mapped to {@code C}
     * with the publish revision. The iterator blocks for new records; when the
     * draining thread is interrupted (the wrapper tearing the stream down) the
     * subscription deregisters itself.
     */
    public <C> Iterator<C> changes(Predicate<Message> match, BiFunction<Message, Long, C> toChange) {
        Subscription<C> sub = new Subscription<>(match, toChange);
        subs.add(sub);
        return new Iterator<C>() {
            private C pending;

            @Override
            public boolean hasNext() {
                if (pending != null) {
                    return true;
                }
                try {
                    pending = sub.take();
                    return true;
                } catch (InterruptedException e) {
                    subs.remove(sub);
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            @Override
            public C next() {
                C c = pending;
                pending = null;
                return c;
            }
        };
    }

    private static final class Subscription<C> {
        private final Predicate<Message> match;
        private final BiFunction<Message, Long, C> toChange;
        private final LinkedBlockingQueue<C> queue = new LinkedBlockingQueue<>();

        Subscription(Predicate<Message> match, BiFunction<Message, Long, C> toChange) {
            this.match = match;
            this.toChange = toChange;
        }

        void offer(Message record, long revision) {
            if (match.test(record)) {
                queue.offer(toChange.apply(record, revision));
            }
        }

        C take() throws InterruptedException {
            return queue.take();
        }
    }
}
