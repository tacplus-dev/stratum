// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import java.util.Set;

/**
 * Which leg owns a warm row RIGHT NOW: the memory-side complement of the cut
 * {@link WarmStore#query} applies to the disk side.
 *
 * The two legs are supposed to partition the dataset — memory holds
 * {@code epoch >= boundary}, disk holds {@code epoch < boundary} — and the union
 * is dedup-free only while that actually holds. It does not hold on its own: a row
 * whose epoch has crossed the boundary is still in the Hazelcast map until
 * {@code WarmEviction} next runs, and until then BOTH legs match it and
 * {@code Get<T>Count} counts it twice. This is the cut that closes that window
 * without depending on when the sweep last ran.
 *
 * <h2>Why pending keys are held by memory, not skipped by both</h2>
 * The disk leg already excludes keys whose in-memory copy may be ahead of their
 * on-disk one (see {@link WarmIndex#pendingKeys()}). That is a CLAIM window, not
 * merely a write-in-flight one: it opens when the ingest path claims the key —
 * before the memory put that can make memory's copy the newer one — and closes when
 * the disk write commits, or when the claim is released because no write will
 * follow. If the memory cut were the boundary test alone, a row that is both aged
 * AND claimed would be excluded by both legs and vanish from results until the
 * claim cleared. Ownership is therefore the exact complement of the disk leg's
 * exclusion: memory owns a row while its key is claimed OR its epoch is inside the
 * window, disk owns it otherwise, and every row is owned by exactly one leg at
 * every instant.
 *
 * <h2>Why it is a snapshot</h2>
 * One value, taken once per query, applied to every row of that query. Re-reading
 * the clock per row would let the boundary move mid-scan and put one row on both
 * sides of it — the duplicate this exists to prevent, reintroduced by the
 * prevention.
 */
public final class WarmResidency {

    /** Memory serves {@code epoch >= boundaryMs}; disk serves {@code epoch < boundaryMs}. */
    public final long boundaryMs;

    /** Keys whose disk copy may be behind memory. Live view of the index's set. */
    private final Set<String> pending;

    WarmResidency(long boundaryMs, Set<String> pending) {
        this.boundaryMs = boundaryMs;
        this.pending = pending;
    }

    /**
     * A residency that keeps everything in memory — what a record type with no warm
     * dataset gets, since nothing has tiered it to disk and no other leg can serve it.
     */
    static WarmResidency memoryOnly() {
        return new WarmResidency(Long.MIN_VALUE, Set.of());
    }

    /** True when the memory leg owns this row and the disk leg does not. */
    public boolean inMemory(String key, long epochMs) {
        return epochMs >= boundaryMs || pending.contains(key);
    }

    /**
     * True when the row can be dropped from the Hazelcast map: the disk leg already
     * answers for it. The exact negation of {@link #inMemory}, so eviction cannot
     * remove a row the read path still expects memory to serve.
     */
    public boolean evictable(String key, long epochMs) {
        return !inMemory(key, epochMs);
    }
}
