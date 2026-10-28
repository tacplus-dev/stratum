// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoAppend;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoReplayEntry;

/**
 * The store's view of the redo service (the sys contract {@code redolog.Redo}):
 * the tenant's log, which is the database. {@link GrpcRedoLog} is the
 * deployed one; tests drive the store against an in-memory log.
 */
public interface RedoLog {

    /**
     * Sends one operation to the log. Calls are made in seq order, one at a
     * time (the store serializes them).
     *
     * @return completes once the redo has acked {@code append.getSeq()} at the
     *         level the append is owed, and exceptionally when that cannot
     *         happen: the redo refused the append, the stream to it failed, or it
     *         is unreachable. A failed append may or may not be in the log; the
     *         store does not apply it and replays before it writes again.
     */
    CompletableFuture<Void> append(RedoAppend append);

    /**
     * Streams the tenant's log, snapshot first and then the tail, to
     * {@code entry}, and returns once it has all been delivered.
     *
     * @throws RuntimeException when the log cannot be read; nothing replayed
     *         so far may be taken for the whole log
     */
    void replay(Consumer<RedoReplayEntry> entry);

    /**
     * Drops the append stream: every append still waiting for its ack fails,
     * and the next append opens a new stream. Called when the store has lost
     * its place in the log.
     */
    void reset();

    /** Releases the stream for good. */
    void close();
}
