// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.grpc.Status;

import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoAppend;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoChange;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoReplayEntry;
import com.github.tacplusdev.stratum.model.types.sys.spec.Spec.Durability;

/**
 * A tenant's redo log in memory, with the redo service's rules (redolog.Redo):
 * an append's seq must be exactly one past the log's last, MEMORY is refused,
 * and REMOTE is refused FAILED_PRECONDITION as today's redo refuses it until its
 * second site exists. A test can make it unreachable, and hold its acks: an
 * append is then in the log but its ack waits for {@link #releaseAcks}.
 */
public final class InMemoryRedoLog implements RedoLog {

    private final List<RedoReplayEntry> snapshot = new ArrayList<>();
    private final List<RedoReplayEntry> log = new ArrayList<>();
    private final List<CompletableFuture<Void>> held = new ArrayList<>();
    private final List<CompletableFuture<Void>> open = new ArrayList<>();
    private boolean reachable = true;
    private boolean holdAcks;
    int appends;
    int replays;

    public synchronized void setReachable(boolean reachable) {
        this.reachable = reachable;
        if (!reachable) {
            failOpen(Status.UNAVAILABLE.withDescription("redo unreachable").asRuntimeException());
        }
    }

    public synchronized void holdAcks(boolean hold) {
        this.holdAcks = hold;
    }

    /** Acks every held append, in seq order. */
    public synchronized void releaseAcks() {
        for (CompletableFuture<Void> f : held) {
            f.complete(null);
        }
        open.removeAll(held);
        held.clear();
    }

    /** Seeds the compacted snapshot, as compaction would have left it. */
    synchronized void seedSnapshot(long seq, RedoChange... changes) {
        snapshot.add(RedoReplayEntry.newBuilder().setSeq(seq).setFromSnapshot(true)
                .addAllChanges(List.of(changes)).build());
    }

    /** Seeds a tail entry, as an earlier store would have appended it. */
    synchronized void seedTail(long seq, RedoChange... changes) {
        log.add(RedoReplayEntry.newBuilder().setSeq(seq).addAllChanges(List.of(changes)).build());
    }

    synchronized long lastSeq() {
        long last = 0;
        for (RedoReplayEntry e : snapshot) last = Math.max(last, e.getSeq());
        for (RedoReplayEntry e : log) last = Math.max(last, e.getSeq());
        return last;
    }

    synchronized List<RedoReplayEntry> tail() {
        return List.copyOf(log);
    }

    @Override
    public synchronized CompletableFuture<Void> append(RedoAppend a) {
        appends++;
        CompletableFuture<Void> f = new CompletableFuture<>();
        if (!reachable) {
            f.completeExceptionally(Status.UNAVAILABLE.withDescription("redo unreachable").asRuntimeException());
            return f;
        }
        if (a.getDurability() == Durability.DURABILITY_MEMORY) {
            f.completeExceptionally(Status.INVALID_ARGUMENT.withDescription("MEMORY is never appended").asRuntimeException());
            return f;
        }
        if (a.getDurability() == Durability.DURABILITY_REMOTE) {
            f.completeExceptionally(Status.FAILED_PRECONDITION.withDescription("no second site yet").asRuntimeException());
            return f;
        }
        if (a.getSeq() != lastSeq() + 1) {
            f.completeExceptionally(Status.FAILED_PRECONDITION
                    .withDescription("seq " + a.getSeq() + " does not follow " + lastSeq()).asRuntimeException());
            return f;
        }
        log.add(RedoReplayEntry.newBuilder().setSeq(a.getSeq()).addAllChanges(a.getChangesList()).build());
        open.add(f);
        if (holdAcks) {
            held.add(f);
        } else {
            open.remove(f);
            f.complete(null);
        }
        return f;
    }

    @Override
    public void replay(Consumer<RedoReplayEntry> entry) {
        List<RedoReplayEntry> all;
        synchronized (this) {
            replays++;
            if (!reachable) {
                throw Status.UNAVAILABLE.withDescription("redo unreachable").asRuntimeException();
            }
            all = new ArrayList<>(snapshot);
            all.addAll(log);
        }
        all.forEach(entry);
    }

    @Override
    public synchronized void reset() {
        failOpen(Status.UNAVAILABLE.withDescription("append stream reset").asRuntimeException());
    }

    private void failOpen(RuntimeException e) {
        for (CompletableFuture<Void> f : open) {
            f.completeExceptionally(e);
        }
        open.clear();
        held.clear();
    }

    @Override
    public void close() {
        reset();
    }
}
