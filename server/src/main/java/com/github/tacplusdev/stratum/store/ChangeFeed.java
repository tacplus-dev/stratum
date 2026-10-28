// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.grpc.Status;

/**
 * This member's view of committed writes, per record type: what Watch serves.
 *
 * <p>Fed only with writes the store has applied, which it does only once the
 * redo acked them at the level they were owed, so a watcher never sees a write
 * that is not durable at its declared level (and never sees one that was
 * refused). The sequencer publishes each applied operation once, in commit
 * order, and every member, itself included, delivers it here
 * ({@link RecordStore}); revisions therefore arrive in increasing order.
 *
 * <p>Each type keeps its last {@link #HISTORY} changes, so a watcher resuming
 * from a revision this member has seen gets every change after it. One older
 * than that is answered OUT_OF_RANGE by the generated watch runtime, and the
 * caller re-watches from 0 (snapshot, then changes).
 */
public final class ChangeFeed {

    /** Changes kept per record type for watchers that resume. */
    public static final int HISTORY = 4096;

    public enum Kind { ADD, MODIFY, DELETE }

    /** One entry's committed change. */
    public static final class Change implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String type;
        public final String mapKey;
        public final Kind kind;
        /** The row's owning namespace ((spec.store_namespace)). */
        public final String namespace;
        /** The row after the change, or the deleted row for a DELETE, protobuf-encoded. */
        public final byte[] row;
        public final long revision;

        public Change(String type, String mapKey, Kind kind, String namespace, byte[] row, long revision) {
            this.type = type;
            this.mapKey = mapKey;
            this.kind = kind;
            this.namespace = namespace;
            this.row = row;
            this.revision = revision;
        }
    }

    /** One applied operation's changes, as the sequencer publishes them. */
    public static final class Committed implements Serializable {
        private static final long serialVersionUID = 1L;
        public final List<Change> changes;
        /** Set instead of changes when the rows were rebuilt: every watch ends. */
        public final boolean reset;

        public Committed(List<Change> changes) {
            this(changes, false);
        }

        public Committed(List<Change> changes, boolean reset) {
            this.changes = List.copyOf(changes);
            this.reset = reset;
        }
    }

    private final Map<String, Deque<Change>> history = new HashMap<>();
    private final Map<String, Long> lastRevision = new HashMap<>();
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    /** Delivers an applied operation to this member's watchers. */
    public void deliver(Committed op) {
        synchronized (this) {
            for (Change c : op.changes) {
                Deque<Change> h = history.computeIfAbsent(c.type, t -> new ArrayDeque<>());
                if (h.size() == HISTORY) {
                    h.removeFirst();
                }
                h.addLast(c);
                lastRevision.merge(c.type, c.revision, Math::max);
            }
            for (Subscription s : subscriptions) {
                for (Change c : op.changes) {
                    s.offer(c);
                }
            }
        }
    }

    /**
     * The last revision of {@code type} this member has delivered, 0 for none: a
     * watch snapshot taken after reading it misses nothing the feed delivers
     * later.
     */
    public synchronized long lastRevision(String type) {
        return lastRevision.getOrDefault(type, 0L);
    }

    /**
     * Every change of {@code type} held to {@code namespace} (any namespace when
     * null): the retained history first, then each change as it is delivered.
     * The iterator blocks for the next change; closing it, or interrupting the
     * thread reading it, ends it.
     */
    public synchronized Iterator<Change> watch(String type, String namespace) {
        Subscription s = new Subscription(type, namespace);
        for (Change c : history.getOrDefault(type, new ArrayDeque<>())) {
            s.offer(c);
        }
        subscriptions.add(s);
        return s;
    }

    /**
     * Ends every watch. Called when this member may have missed a committed
     * operation, so no watcher waits on a stream with a hole in it: each one
     * re-watches, from its last revision or from a snapshot.
     */
    public void endAll(String why) {
        List<Subscription> ended;
        synchronized (this) {
            ended = new ArrayList<>(subscriptions);
            history.clear();
        }
        for (Subscription s : ended) {
            s.end(why);
        }
    }

    /** Watches currently open on this member. */
    public int watchers() {
        return subscriptions.size();
    }

    private static final Change END = new Change("", "", Kind.DELETE, "", new byte[0], -1);

    private final class Subscription implements Iterator<Change>, AutoCloseable {
        private final String type;
        private final String namespace;
        private final LinkedBlockingQueue<Change> queue = new LinkedBlockingQueue<>();
        private volatile String endedWhy;
        private Change next;

        Subscription(String type, String namespace) {
            this.type = type;
            this.namespace = namespace;
        }

        void offer(Change c) {
            if (c.type.equals(type) && (namespace == null || namespace.equals(c.namespace))) {
                queue.add(c);
            }
        }

        void end(String why) {
            endedWhy = why;
            subscriptions.remove(this);
            queue.add(END);
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            try {
                Change c = queue.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                if (c == END) {
                    queue.add(END);
                    if (endedWhy != null && !endedWhy.isEmpty()) {
                        throw Status.UNAVAILABLE.withDescription(endedWhy).asRuntimeException();
                    }
                    return false;
                }
                next = c;
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                close();
                throw Status.CANCELLED.withDescription("watch closed").asRuntimeException();
            }
        }

        @Override
        public Change next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Change c = next;
            next = null;
            return c;
        }

        @Override
        public void close() {
            if (endedWhy == null) {
                end("");
            }
        }
    }
}
