// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;

/**
 * Stratum's warm disk tier: the store-per-node Lucene indexes under
 * {@code <dataRoot>/warm/<pkg>.<Type>/<yyyy-MM>/}.
 *
 * <h2>Store per node, full local replica</h2>
 * The pod's volume is a per-pod ReadWriteOnce claim and Lucene's write.lock
 * permits one IndexWriter per directory, so a shared index is not available and
 * would need a single elected writer even if it were. Each instance therefore owns
 * its own indexes. That is only CORRECT if each instance sees every warm record,
 * which is what the per-instance consumer group for RESIDENCY_WARM topics
 * (tacplus P1b) delivers: the broker fans the topic out instead of splitting it.
 * A full local replica is also what makes the disk leg answerable entirely on the
 * node serving the RPC — no scatter-gather, no cross-node dedup — and it gives the
 * multi-copy durability property structurally, as three independent commits on
 * three independent volumes.
 *
 * <h2>The disk write is off the ack path</h2>
 * A warm write is acked once it is replicated in memory across instances; the disk
 * commit happens asynchronously after that. The ingest thread therefore does NOT
 * wait on ~2 fsyncs, and the caller's latency is unchanged. What that costs is
 * stated plainly rather than hidden: see {@link WarmIndex#pendingKeys()} for the
 * window it opens and how the disk leg is kept out of it, and
 * {@link #enqueue} for what a crash inside the window loses.
 *
 * <h2>Claim, then write</h2>
 * The window the disk leg has to be kept out of starts BEFORE the disk write, at
 * the moment memory's copy can become the newer one — not at the enqueue. So
 * {@link #claim} and {@link #release} bracket the memory put, and {@link #enqueue}
 * runs inside that bracket. Marking the key at the enqueue instead leaves the
 * interval between the memory put and the enqueue uncovered, and an update to an
 * already-aged row is served by BOTH legs for its duration.
 *
 * <h2>Where the boundary is read</h2>
 * The residency boundary is read from ONE place for both legs: {@link #diskRange}
 * is the disk side of it and {@link #residency} is the memory side. Rows are moved
 * off memory by {@code WarmEviction}, which is a reclaim, not a correctness step —
 * {@link WarmResidency} makes the memory leg stop serving an aged row from the
 * instant the boundary passes it, whether or not a sweep has run since.
 */
public final class WarmStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WarmStore.class);

    /** Where the warm tier lives under the pod's volume. */
    public static final String WARM_SUBDIR = "warm";

    /**
     * Per-slice hit cap. Paging happens in application code after both legs are
     * collected, so the disk leg has to hand back the whole match list; a cap is
     * still needed because Lucene's top-N takes an int. Saturation is LOGGED, never
     * silent — a truncated page that reads as a complete one is the failure mode a
     * cap invites.
     */
    static final int MAX_HITS_PER_SLICE = 100_000;

    private final Path warmRoot;
    private final Map<String, WarmIndex> indexes = new LinkedHashMap<>();

    /**
     * Slice fan-out. A small FIXED pool sized to the container's CPU request — not
     * a pool per query, and deliberately not the gRPC handler threads, which a
     * slow disk would otherwise consume.
     */
    private final ExecutorService searchPool;

    /**
     * The asynchronous disk writer. ONE thread, so writes commit in the order they
     * were acked and a key's own updates cannot reorder against each other.
     * Commit-per-write at this tier's real load leaves it three orders of magnitude
     * of headroom.
     */
    private final ExecutorService writePool;

    private final AtomicLong written = new AtomicLong();
    private final AtomicLong writeFailures = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();

    public WarmStore(Path dataRoot, Collection<WarmDataset> datasets) throws IOException {
        this.warmRoot = dataRoot.resolve(WARM_SUBDIR);
        // No warm-typed record means no warm tier and no directory. Every record is
        // hot until one declares otherwise, so this is the state stratum ships in
        // today and it must not create anything on the volume.
        if (!datasets.isEmpty()) {
            Files.createDirectories(warmRoot);
        }
        for (WarmDataset ds : datasets) {
            indexes.put(ds.name, new WarmIndex(warmRoot.resolve(ds.name), ds));
        }
        this.searchPool = Executors.newFixedThreadPool(4, daemon("stratum-warm-search"));
        this.writePool = Executors.newSingleThreadExecutor(daemon("stratum-warm-writer"));
        if (indexes.isEmpty()) {
            log.info("warm tier: no record type declares RESIDENCY_WARM — nothing is tiered to disk");
        } else {
            log.info("warm tier at {}: {} dataset(s) {}", warmRoot, indexes.size(), indexes.keySet());
        }
    }

    private static ThreadFactory daemon(String prefix) {
        AtomicLong n = new AtomicLong();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /** The datasets this store holds, by name. Empty when no record type declares RESIDENCY_WARM. */
    public Collection<String> datasetNames() {
        return indexes.keySet();
    }

    public boolean isEmpty() {
        return indexes.isEmpty();
    }

    /** Package-visible for tests and for the retention/reconcile sweeps. */
    public WarmIndex index(String datasetName) {
        return indexes.get(datasetName);
    }

    /**
     * Repairs every mutable dataset before the first write.
     *
     * Unconditional and cheap, the way ColdStartRebuilder is: a cross-slice update
     * that was interrupted between its two commits leaves a stale copy that nothing
     * else would ever notice, because the disk leg's epoch clause hides the newer
     * one and the stale one answers forever.
     */
    public void reconcile() {
        for (WarmIndex idx : indexes.values()) {
            try {
                idx.reconcile();
            } catch (IOException e) {
                log.error("warm reconcile failed for {}", idx.dataset().name, e);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Ingest
    // -----------------------------------------------------------------------

    /**
     * Claims a record's key for the memory leg, BEFORE its memory copy is written.
     *
     * This is the half of {@link WarmIndex#pendingKeys()} that {@link #enqueue}
     * cannot cover: the disk leg has to stop answering for the key from the instant
     * memory's copy can be the newer one, and that instant is the memory put, not
     * the enqueue that follows it.
     *
     * A claim is a debt. The caller owes the key a {@link #release} unless a
     * subsequent {@link #enqueue} takes it over — which it does by either submitting
     * the write (the writer clears the key on commit) or rejecting the record (which
     * releases it directly). A claim never released hides the key from the disk leg
     * for the life of the process.
     *
     * The claim is EXCLUSIVE and atomic: when another delivery of the same key is
     * already mid-ingest this returns false without disturbing that claim. For an
     * append-only dataset that answer is also the duplicate decision — see
     * {@code EventStore.ingest}, which is the only caller that can tell the two
     * false cases apart.
     *
     * @return true when a warm dataset claimed the record FOR THIS CALL; false
     *         leaves nothing owed, either because no warm dataset holds the type or
     *         because the key was already claimed.
     */
    public boolean claim(Message record) {
        WarmIndex idx = indexFor(record);
        if (idx == null) {
            return false;
        }
        return idx.markPending(idx.dataset().key(record));
    }

    /** Drops a claim {@link #claim} took when no disk write followed it. */
    public void release(Message record) {
        WarmIndex idx = indexFor(record);
        if (idx != null) {
            idx.clearPending(idx.dataset().key(record));
        }
    }

    /**
     * Hands a record to the asynchronous disk writer and returns immediately.
     *
     * The key is marked pending SYNCHRONOUSLY, on the ingest thread, before the
     * write is submitted — not inside the writer task. Marking it later would leave
     * a gap in which the disk leg still served the row's previous copy while memory
     * already served the new one, which is the duplicate this exists to prevent.
     * The mark is idempotent because the ingest path has normally taken the same
     * claim already (see {@link #claim}); a caller that enqueues without claiming
     * still gets the narrower cover this line has always given.
     *
     * What the asynchrony costs, stated: a process killed between the memory put and
     * the disk commit loses that commit. The row is not lost — the record is still
     * on the event log, and a cold boot replays it — but until then this node's disk
     * copy is behind. That is the accepted consequence of acking on memory
     * replication instead of on the fsync.
     *
     * @return true when a warm dataset claimed the record.
     */
    public boolean enqueue(Message record) {
        WarmIndex idx = indexFor(record);
        if (idx == null) {
            return false;
        }
        // The SAME generated validator the hot store applies before its put. The hot
        // store drops an unservable record and logs it; the disk tier has to make the
        // same decision or the row it kept would outlive the one memory refused, and
        // the read path fails a whole page on one bad record (see
        // StratumEventStores.store). Claimed, then dropped — not rethrown, because
        // this runs on the ingest thread and throwing would retry the poison record
        // forever.
        try {
            idx.dataset().validate(record);
        } catch (Exception e) {
            rejected.incrementAndGet();
            log.warn("warm tier rejected unstorable {}: {}", idx.dataset().name, e.getMessage());
            // No write will follow, so nothing else will ever clear the claim the
            // ingest path took before the memory put. Leaving it would hide the key
            // from the disk leg permanently.
            idx.clearPending(idx.dataset().key(record));
            return true;
        }
        idx.markPending(idx.dataset().key(record));
        writePool.execute(() -> {
            try {
                idx.write(record);
                written.incrementAndGet();
            } catch (Exception e) {
                writeFailures.incrementAndGet();
                log.error("warm write failed for {}: {}", idx.dataset().name, e.getMessage(), e);
            }
        });
        return true;
    }

    /**
     * The warm dataset holding this record's type, or null when no dataset does.
     *
     * The ingest path reads the record's declared attributes — its key and its
     * mutability — from here, so the one place that maps a message to its dataset
     * stays {@link #indexFor}.
     */
    public WarmDataset datasetFor(Message record) {
        WarmIndex idx = indexFor(record);
        return idx == null ? null : idx.dataset();
    }

    private WarmIndex indexFor(Message record) {
        for (WarmIndex idx : indexes.values()) {
            if (idx.dataset().type.isInstance(record)) {
                return idx;
            }
        }
        return null;
    }

    /** Blocks until every enqueued write has been attempted. Tests and shutdown only. */
    public void awaitWrites(long timeoutMs) throws InterruptedException {
        java.util.concurrent.CountDownLatch drained = new java.util.concurrent.CountDownLatch(1);
        writePool.execute(drained::countDown);
        drained.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public long writtenCount()      { return written.get(); }
    public long writeFailureCount() { return writeFailures.get(); }
    public long rejectedCount()     { return rejected.get(); }

    // -----------------------------------------------------------------------
    // Query — the disk leg
    // -----------------------------------------------------------------------

    /**
     * The disk leg's matching rows, as stored proto bytes, or an empty list when
     * the leg is provably empty.
     *
     * Two prunes happen before a single file is opened. The LEG prune: when the
     * caller's own epoch floor is at or above the boundary, every row it can match
     * is inside the memory window and the disk leg is skipped outright — a
     * comparison of two longs, not an estimate. The SLICE prune: a slice whose
     * interval, read off its directory name, cannot intersect the query's epoch
     * range is never opened.
     */
    public List<byte[]> query(String datasetName, WarmFilter filter) throws IOException {
        WarmIndex idx = indexes.get(datasetName);
        if (idx == null) {
            return List.of();
        }
        long[] range = diskRange(idx.dataset(), filter);
        if (range == null) {
            return List.of();
        }
        Query q = WarmQueries.compile(idx.dataset(), filter, range[0], range[1], idx.pendingKeys());
        return idx.search(q, range[0], range[1], MAX_HITS_PER_SLICE, searchPool);
    }

    /** The disk leg's match count. A clean sum of per-slice counts; hits are never materialised. */
    public long count(String datasetName, WarmFilter filter) throws IOException {
        WarmIndex idx = indexes.get(datasetName);
        if (idx == null) {
            return 0;
        }
        long[] range = diskRange(idx.dataset(), filter);
        if (range == null) {
            return 0;
        }
        Query q = WarmQueries.compile(idx.dataset(), filter, range[0], range[1], idx.pendingKeys());
        return idx.count(q, range[0], range[1], searchPool);
    }

    /**
     * The epoch interval the disk leg has to search, as {@code [from, toExclusive)},
     * or null when the leg is provably empty.
     *
     * This IS the leg prune, and it is one comparison of two longs rather than an
     * estimate: the leg's ceiling is the boundary, so a caller whose own epoch floor
     * has reached the boundary leaves an empty interval and the leg is skipped
     * before a directory is even listed. A separate "floor >= boundary" test would
     * be the same test written twice.
     */
    private long[] diskRange(WarmDataset dataset, WarmFilter filter) {
        long boundary = dataset.boundaryMs(System.currentTimeMillis());
        Long callerFrom = filter.lowerBound(dataset.epochField);
        Long callerTo = filter.upperBound(dataset.epochField);
        long from = callerFrom == null ? Long.MIN_VALUE : callerFrom;
        long to = callerTo == null ? boundary : Math.min(callerTo + 1, boundary);
        return to <= from ? null : new long[] { from, to };
    }

    // -----------------------------------------------------------------------
    // Residency — the memory leg's half of the same cut
    // -----------------------------------------------------------------------

    /**
     * Which rows of this dataset the MEMORY leg still owns, at this instant.
     *
     * {@link #diskRange} is this same boundary read from the disk side; handing the
     * memory side its complement from the one place is what keeps the two legs from
     * drifting apart. A dataset this store does not hold is memory-only: nothing has
     * tiered it, so no disk leg can answer for it.
     */
    public WarmResidency residency(String datasetName) {
        WarmIndex idx = indexes.get(datasetName);
        if (idx == null) {
            return WarmResidency.memoryOnly();
        }
        return new WarmResidency(idx.dataset().boundaryMs(System.currentTimeMillis()),
                idx.pendingKeys());
    }

    /**
     * True when this node's disk index already holds the key — the precondition for
     * dropping the row from memory. False for a dataset this store does not hold,
     * which is the safe answer: an unheld dataset has no disk leg, so nothing may be
     * evicted from its memory.
     */
    public boolean isDurableOnDisk(String datasetName, String key) throws IOException {
        WarmIndex idx = indexes.get(datasetName);
        return idx != null && idx.holdsKey(key);
    }

    // -----------------------------------------------------------------------
    // Retention
    // -----------------------------------------------------------------------

    /**
     * Drops every slice of every dataset that lies wholly before the cutoff.
     *
     * The hot tier's sweep deletes rows by predicate; this one deletes directories.
     * They share a cutoff and nothing else, which is why they are separate calls
     * rather than one.
     *
     * @return the dropped slices, by dataset
     */
    public Map<String, List<String>> runRetention(long cutoffMs) {
        Map<String, List<String>> dropped = new LinkedHashMap<>();
        for (WarmIndex idx : indexes.values()) {
            try {
                List<String> slices = idx.dropSlicesEndingBefore(cutoffMs);
                if (!slices.isEmpty()) {
                    dropped.put(idx.dataset().name, slices);
                }
            } catch (IOException e) {
                log.error("warm retention failed for {}", idx.dataset().name, e);
            }
        }
        return dropped;
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    @Override
    public void close() {
        writePool.shutdown();
        try {
            if (!writePool.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("warm writer did not drain in 10s; queued disk writes are lost (memory and the event log still hold them)");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        searchPool.shutdownNow();
        List<String> failed = new ArrayList<>();
        for (WarmIndex idx : indexes.values()) {
            try {
                idx.close();
            } catch (IOException e) {
                failed.add(idx.dataset().name + ": " + e.getMessage());
            }
        }
        if (!failed.isEmpty()) {
            log.warn("warm tier close: {}", failed);
        }
    }
}
