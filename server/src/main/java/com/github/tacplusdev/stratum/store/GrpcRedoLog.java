// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.github.tacplusdev.stratum.model.s2sm.internal.grpc.clients.sys.redolog.RedoGrpcClient;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoAck;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoAppend;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoReplayEntry;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoReplayRequest;
import com.github.tacplusdev.stratum.model.types.sys.spec.Spec.Durability;

/**
 * The redo service through its generated client: one {@code Append} stream per
 * store, opened on the first append and again after it fails, and a
 * {@code Replay} per catch-up.
 *
 * <p>The redo runs in the reserved {@code restore} tenant, so every call is a
 * tenant-to-tenant call whose route field names that tenant
 * (tacplus-dev/tacplus#170). The tenant a write belongs to is never a field: the
 * redo reads it off this store's identity.
 */
public final class GrpcRedoLog implements RedoLog {

    private static final Logger log = LoggerFactory.getLogger(GrpcRedoLog.class);

    /** The reserved restore tenant, where the redo service runs (Jody, 2026-10-26). */
    public static final String REDO_TENANT = "00000002";

    private static final class Pending {
        final CompletableFuture<Void> acked = new CompletableFuture<>();
        final int owed;

        Pending(int owed) {
            this.owed = owed;
        }
    }

    private final RedoGrpcClient client;
    private final Map<Long, Pending> pending = new ConcurrentSkipListMap<>();
    private StreamObserver<RedoAppend> stream;
    private long generation;

    public GrpcRedoLog(RedoGrpcClient client) {
        this.client = client;
    }

    /** The level an append is owed: unset is LOCAL, the contract's default. */
    static int owedLevel(Durability d) {
        return d == Durability.DURABILITY_UNSPECIFIED ? Durability.DURABILITY_LOCAL_VALUE : d.getNumber();
    }

    @Override
    public synchronized CompletableFuture<Void> append(RedoAppend append) {
        Pending p = new Pending(owedLevel(append.getDurability()));
        pending.put(append.getSeq(), p);
        try {
            if (stream == null) {
                stream = open(++generation);
            }
            stream.onNext(append);
        } catch (RuntimeException e) {
            pending.remove(append.getSeq());
            p.acked.completeExceptionally(e);
            dropStream(e);
        }
        return p.acked;
    }

    private StreamObserver<RedoAppend> open(long gen) {
        return client.append(REDO_TENANT, new StreamObserver<RedoAck>() {
            @Override
            public void onNext(RedoAck ack) {
                Pending p = pending.get(ack.getSeq());
                if (p != null && ack.getLevelValue() >= p.owed) {
                    pending.remove(ack.getSeq());
                    p.acked.complete(null);
                }
            }

            @Override
            public void onError(Throwable t) {
                ended(gen, t);
            }

            @Override
            public void onCompleted() {
                ended(gen, Status.UNAVAILABLE.withDescription("the redo service closed the append stream")
                        .asRuntimeException());
            }
        });
    }

    private synchronized void ended(long gen, Throwable t) {
        if (gen == generation) {
            log.warn("redo append stream ended: {}", t.getMessage());
            stream = null;
            failPending(t);
        }
    }

    private void dropStream(Throwable cause) {
        StreamObserver<RedoAppend> s = stream;
        stream = null;
        generation++;
        failPending(cause);
        if (s != null) {
            try {
                s.onError(Status.CANCELLED.withDescription("store reset its append stream").asRuntimeException());
            } catch (RuntimeException ignored) {
                // Already broken; nothing more to release.
            }
        }
    }

    private void failPending(Throwable t) {
        for (Long seq : pending.keySet()) {
            Pending p = pending.remove(seq);
            if (p != null) {
                p.acked.completeExceptionally(t);
            }
        }
    }

    @Override
    public synchronized void reset() {
        dropStream(Status.UNAVAILABLE.withDescription("append stream reset").asRuntimeException());
    }

    @Override
    public void replay(Consumer<RedoReplayEntry> entry) {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> failed = new AtomicReference<>();
        client.replay(RedoReplayRequest.newBuilder().setTenant(REDO_TENANT).build(),
                new StreamObserver<RedoReplayEntry>() {
                    @Override
                    public void onNext(RedoReplayEntry e) {
                        if (failed.get() == null) {
                            try {
                                entry.accept(e);
                            } catch (RuntimeException ex) {
                                failed.compareAndSet(null, ex);
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        failed.compareAndSet(null, t);
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });
        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withDescription("interrupted during replay").asRuntimeException();
        }
        Throwable t = failed.get();
        if (t != null) {
            throw t instanceof RuntimeException ? (RuntimeException) t : new IllegalStateException(t);
        }
    }

    @Override
    public synchronized void close() {
        dropStream(Status.CANCELLED.withDescription("store shutting down").asRuntimeException());
    }
}
