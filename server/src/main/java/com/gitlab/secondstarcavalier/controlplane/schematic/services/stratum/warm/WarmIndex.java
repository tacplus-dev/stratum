// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;

/**
 * One dataset's warm tier: a directory of Lucene slices under
 * {@code /app/data/warm/<pkg>.<Type>/<yyyy-MM>/}.
 *
 * <h2>One searcher per slice, never a MultiReader</h2>
 * Slices stay separate readers so that (a) the outer level of parallelism — one
 * task per slice — exists at all, (b) retention can close exactly one
 * SearcherManager and drop exactly one directory without disturbing anything
 * else, and (c) one unreadable slice fails its own task instead of making the
 * whole dataset unqueryable. Stratum has already been bitten once by a single bad
 * record taking every query down (see StratumEventStores.store); repeating that
 * shape on disk would be re-introducing a known bug.
 *
 * <h2>Commit per write</h2>
 * {@code updateDocument} + {@code commit()} — the only Lucene operation after
 * which a hard kill leaves the document present. There is no batching window: a
 * timer would silently move the guarantee, and at this tier's real load
 * (order 25k rows/year) commit-per-write is free by three orders of magnitude.
 * Visibility follows durability because the searcher is refreshed off the commit,
 * which is a simplification worth having.
 *
 * <h2>Where the ack is</h2>
 * The disk write is NOT on the ack path: an ack means the record is replicated in
 * memory across instances, and the disk commit happens asynchronously after it.
 * That is why {@link #pendingKeys()} exists — see {@link WarmStore}.
 */
public final class WarmIndex implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(WarmIndex.class);

    /** Reserved document field: the record key, the term updates and deletes address. */
    public static final String KEY_FIELD = "_key";
    /** Reserved document field: the store epoch, the field both legs partition on. */
    public static final String EPOCH_FIELD = "_epoch";
    /** Reserved document field: the record itself, stored verbatim. */
    public static final String PROTO_FIELD = "_proto";

    private final Path root;
    private final WarmDataset dataset;

    /** Serialises writes to this dataset. One IndexWriter per directory is Lucene's own rule. */
    private final Object writeLock = new Object();
    private String currentSliceId;
    private IndexWriter writer;
    /** The writer's Directory. IndexWriter.close() does not close it. */
    private FSDirectory writerDir;

    private final Map<String, SearcherManager> searchers = new ConcurrentHashMap<>();
    /**
     * The Directory behind each open SearcherManager. Closing a SearcherManager
     * closes the reader it holds, not the Directory it was opened on, and the
     * retention sweep closes managers for a living — so the handles are tracked
     * here rather than left to the file-descriptor table.
     */
    private final Map<String, FSDirectory> directories = new ConcurrentHashMap<>();

    /**
     * Keys whose in-memory copy MAY BE newer than their on-disk copy.
     *
     * The disk leg excludes them. The window is not the disk write alone: it opens
     * at the CLAIM, before the memory put that makes memory's copy the newer one,
     * and closes at the disk commit that makes the two agree — or at the release,
     * when no disk write will follow. Marking the key only when the write is
     * submitted would leave the interval between the memory put and that submission
     * uncovered, and in it the row's old copy still satisfies the disk leg's epoch
     * predicate while the new copy already satisfies the memory leg's — i.e. a
     * DUPLICATE, which inflates Get&lt;T&gt;Count for the length of the window.
     * Excluding the key makes memory unambiguously the owner for exactly as long as
     * the disk copy may be stale, which is true by construction: the pending write
     * stamps epoch = now, so the memory leg does match it.
     *
     * The set is bounded by the ingests in flight, and every instance holds a full
     * local replica, so an instance's pending set covers its own index exactly.
     */
    private final Set<String> pending = ConcurrentHashMap.newKeySet();

    /**
     * Slices actually opened by queries, cumulative.
     *
     * A first-class counter, not a log line: "a query whose epoch bound lies inside
     * the window demonstrably skips the disk leg" is only checkable if the skip is
     * observable, and this is how it is observed.
     */
    private final AtomicLong slicesSearched = new AtomicLong();

    /**
     * Slice id -> how many queries have selected it as a candidate and have not
     * finished reading it.
     *
     * Candidate selection and retention's delete are two steps apart in time: the
     * query lists the slice, then submits one task per slice to a pool that runs
     * them later. Retention in that gap unlinks the directory, and the read path's
     * "no directory, no slice" answer — {@link #searcherFor} returning null, which
     * {@link #searchSlice}/{@link #countSlice} read as "this slice legitimately holds
     * nothing" — turns the deleted rows into an empty result with no error on it.
     * That is exactly the silent undercount {@link #fanOut} refuses to produce for
     * an unreadable slice, arriving one method earlier by a different door.
     *
     * So the pin is taken WHILE the candidate list is built, under {@link #pinLock},
     * and retention takes the same lock around its own check-and-unlink. A slice is
     * then either pinned before retention looks at it, in which case retention defers
     * it to the next sweep, or unlinked before the query lists it, in which case the
     * query never had it as a candidate. There is no third interleaving, so the race
     * is not detected — it cannot happen.
     */
    private final Map<String, Integer> pins = new HashMap<>();

    /** Guards {@link #pins}, and orders it against retention's unlink. */
    private final Object pinLock = new Object();

    WarmIndex(Path root, WarmDataset dataset) {
        this.root = root;
        this.dataset = dataset;
    }

    public WarmDataset dataset() { return dataset; }

    public Path root() { return root; }

    public long slicesSearched() { return slicesSearched.get(); }

    public Set<String> pendingKeys() { return pending; }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /**
     * Takes the claim on a key, atomically.
     *
     * <p>The return value is the whole point: {@code Set.add} on a
     * {@code ConcurrentHashMap} key set is a compare-and-set, so exactly one of any
     * number of concurrent callers for one key gets true. That is what makes the
     * ingest path's "have I seen this key" decision and its claim one indivisible
     * step rather than two — see {@code EventStore.ingest}.
     *
     * @return true when THIS call took the claim; false when the key was already
     *         claimed, in which case the caller owes no release.
     */
    boolean markPending(String key) {
        return pending.add(key);
    }

    /**
     * Drops a claim no disk write will close.
     *
     * A key left pending forever is HIDDEN from the disk leg forever, which is worse
     * than the duplicate the claim prevents — so every path that claims without
     * writing has to come back through here.
     */
    void clearPending(String key) {
        pending.remove(key);
    }

    /**
     * Commits one record to the slice its store epoch names.
     *
     * Mutable datasets use {@code updateDocument}, never delete-then-add: the
     * delete and the add are one operation and one commit, so there is no window in
     * which the key is absent. When the update moves the row ACROSS slices the two
     * cannot be one commit, and the ordering is new-copy-first: between the two
     * commits the row exists twice on disk but only the old copy is inside the
     * disk leg's epoch range, so exactly one copy is ever served — and a crash
     * between them leaves the row stale rather than gone. Reversing the order
     * would lose it outright.
     */
    void write(Message record) throws IOException {
        String key = dataset.key(record);
        long epochMs = dataset.epochMs(record);
        String sliceId = WarmSlices.sliceIdFor(epochMs);
        Document doc = document(key, epochMs, record);

        synchronized (writeLock) {
            IndexWriter w = writerFor(sliceId);
            if (dataset.appendOnly) {
                w.addDocument(doc);
                w.commit();
            } else {
                String stale = sliceHolding(key, sliceId);
                w.updateDocument(new Term(KEY_FIELD, key), doc);
                w.commit();
                if (stale != null) {
                    deleteFromSealedSlice(stale, key);
                }
            }
            refresh(sliceId);
            pending.remove(key);
        }
    }

    private Document document(String key, long epochMs, Message record) {
        Document doc = new Document();
        doc.add(new StringField(KEY_FIELD, key, org.apache.lucene.document.Field.Store.NO));
        doc.add(new LongPoint(EPOCH_FIELD, epochMs));
        doc.add(new NumericDocValuesField(EPOCH_FIELD, epochMs));
        doc.add(new StoredField(PROTO_FIELD, new BytesRef(record.toByteArray())));
        for (WarmField f : dataset.fields) {
            Object v = f.valueOf.apply(record);
            if (v == null) {
                continue;
            }
            switch (f.kind) {
                case EQ_STRING ->
                        doc.add(new StringField(f.name, (String) v, org.apache.lucene.document.Field.Store.NO));
                case EQ_NUMERIC, RANGE_NUMERIC ->
                        doc.add(new LongPoint(f.name, ((Number) v).longValue()));
            }
        }
        return doc;
    }

    /**
     * One IndexWriter, held open on the current slice only.
     *
     * Rollover happens on the WRITE path, when the first record of a new period
     * arrives — not on a timer, so an idle dataset creates no empty directories.
     */
    private IndexWriter writerFor(String sliceId) throws IOException {
        if (writer != null && sliceId.equals(currentSliceId)) {
            return writer;
        }
        if (writer != null) {
            closeCurrentWriter();
        }
        Path sliceDir = root.resolve(sliceId);
        Files.createDirectories(sliceDir);
        writerDir = FSDirectory.open(sliceDir);
        writer = new IndexWriter(writerDir, new IndexWriterConfig());
        currentSliceId = sliceId;
        return writer;
    }

    private void closeCurrentWriter() throws IOException {
        String sealing = currentSliceId;
        writer.commit();
        writer.close();
        writer = null;
        writerDir.close();
        writerDir = null;
        currentSliceId = null;
        WarmSliceMeta.seal(root.resolve(sealing), sealing);
        refresh(sealing);
    }

    /**
     * True when some slice of this index already holds the key — i.e. the row is
     * committed on THIS node's disk and the disk leg can answer for it.
     *
     * {@code pendingKeys} says a write has not committed YET; this says a copy is
     * there now. Eviction needs the second, not the first: a write that failed
     * outright leaves the key un-pending and absent, and dropping the row from
     * memory on the strength of "no longer pending" would lose it on this node
     * until the event log is replayed.
     */
    public boolean holdsKey(String key) throws IOException {
        return sliceHolding(key, null) != null;
    }

    /**
     * The sealed slice already holding this key, or null. Only slices OTHER than
     * the one being written need checking — {@code updateDocument} handles a
     * same-slice copy itself. A null {@code exceptSlice} excludes nothing, which is
     * what {@link #holdsKey} wants.
     */
    private String sliceHolding(String key, String exceptSlice) throws IOException {
        Query q = new TermQuery(new Term(KEY_FIELD, key));
        for (String sliceId : WarmSlices.list(root)) {
            if (sliceId.equals(exceptSlice)) {
                continue;
            }
            SearcherManager mgr = searcherFor(sliceId);
            if (mgr == null) {
                continue;
            }
            IndexSearcher searcher = mgr.acquire();
            try {
                if (searcher.count(q) > 0) {
                    return sliceId;
                }
            } finally {
                mgr.release(searcher);
            }
        }
        return null;
    }

    /** Reopens a sealed slice just long enough to drop one key, then closes it again. */
    private void deleteFromSealedSlice(String sliceId, String key) throws IOException {
        Path sliceDir = root.resolve(sliceId);
        try (FSDirectory dir = FSDirectory.open(sliceDir);
             IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            w.deleteDocuments(new Term(KEY_FIELD, key));
            w.commit();
        }
        WarmSliceMeta.seal(sliceDir, sliceId);
        refresh(sliceId);
    }

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    /**
     * Runs one query across the candidate slices concurrently and concatenates the
     * hits. Slices whose epoch interval cannot intersect [fromMs, toMsExclusive)
     * are skipped before any reader is opened.
     *
     * Ordering and paging are deliberately NOT done here: the caller merges these
     * hits with the memory leg's and pages the union, which is the model
     * StratumHandler.pageSorted already implements.
     */
    List<byte[]> search(Query query, long fromMs, long toMsExclusive, int maxPerSlice,
                        ExecutorService pool) throws IOException {
        List<String> candidates = candidateSlices(fromMs, toMsExclusive);
        try {
            List<byte[]> out = new ArrayList<>();
            for (List<byte[]> hits : fanOut(candidates, sliceId -> searchSlice(sliceId, query, maxPerSlice), pool)) {
                out.addAll(hits);
            }
            return out;
        } finally {
            unpin(candidates);
        }
    }

    /** Σ over candidate slices of {@code IndexSearcher.count} — never materialises hits. */
    long count(Query query, long fromMs, long toMsExclusive, ExecutorService pool) throws IOException {
        List<String> candidates = candidateSlices(fromMs, toMsExclusive);
        try {
            long n = 0;
            for (Long c : fanOut(candidates, sliceId -> countSlice(sliceId, query), pool)) {
                n += c;
            }
            return n;
        } finally {
            unpin(candidates);
        }
    }

    /**
     * The slices whose interval overlaps the query's, by NAME. The interval is
     * always non-empty here — WarmStore's leg prune returns before calling in when
     * it is not — so this is purely the per-slice cut.
     *
     * Every slice returned is PINNED against retention (see {@link #pins}); the
     * caller owes {@link #unpin} on the same list once its fan-out has finished.
     */
    private List<String> candidateSlices(long fromMs, long toMsExclusive) throws IOException {
        List<String> out = new ArrayList<>();
        synchronized (pinLock) {
            for (String sliceId : WarmSlices.list(root)) {
                if (WarmSlices.intersects(sliceId, fromMs, toMsExclusive)) {
                    out.add(sliceId);
                    pins.merge(sliceId, 1, Integer::sum);
                }
            }
        }
        return out;
    }

    /** Releases the pins {@link #candidateSlices} took, so retention can proceed. */
    private void unpin(List<String> slices) {
        synchronized (pinLock) {
            for (String sliceId : slices) {
                pins.compute(sliceId, (k, n) -> n == null || n <= 1 ? null : n - 1);
            }
        }
    }

    private interface SliceTask<R> {
        R run(String sliceId) throws IOException;
    }

    /**
     * One task per slice on the shared pool, and ALL of them have to answer.
     *
     * A slice that fails to read fails the whole query. The alternative — return
     * what the readable slices held — hands the caller a short list and a short
     * count that are indistinguishable from a complete answer, and the caller has
     * no way to tell "no such rows" from "one slice would not open". That is the
     * same silent-incompleteness rule {@code HotRestore} and
     * {@code ColdStartRebuilder} already refuse to break, applied to the read path.
     */
    private <R> List<R> fanOut(List<String> slices, SliceTask<R> task, ExecutorService pool)
            throws IOException {
        List<Future<R>> futures = new ArrayList<>(slices.size());
        for (String sliceId : slices) {
            Callable<R> c = () -> task.run(sliceId);
            futures.add(pool.submit(c));
        }
        List<R> results = new ArrayList<>(futures.size());
        for (int i = 0; i < futures.size(); i++) {
            try {
                results.add(futures.get(i).get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while searching " + dataset.name, e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                throw new IOException("warm slice " + dataset.name + "/" + slices.get(i)
                        + " failed; refusing to answer from the slices that did read", cause);
            }
        }
        return results;
    }

    private List<byte[]> searchSlice(String sliceId, Query query, int maxPerSlice) throws IOException {
        SearcherManager mgr = searcherFor(sliceId);
        if (mgr == null) {
            return List.of();
        }
        slicesSearched.incrementAndGet();
        IndexSearcher searcher = mgr.acquire();
        try {
            TopDocs top = searcher.search(query, maxPerSlice);
            if (top.totalHits.value() > maxPerSlice) {
                log.warn("warm slice {}/{} matched {} rows, capped at {} — the page is INCOMPLETE",
                        dataset.name, sliceId, top.totalHits.value(), maxPerSlice);
            }
            List<byte[]> out = new ArrayList<>(top.scoreDocs.length);
            var stored = searcher.storedFields();
            for (ScoreDoc sd : top.scoreDocs) {
                BytesRef proto = stored.document(sd.doc, Set.of(PROTO_FIELD)).getBinaryValue(PROTO_FIELD);
                if (proto != null) {
                    out.add(BytesRef.deepCopyOf(proto).bytes);
                }
            }
            return out;
        } finally {
            mgr.release(searcher);
        }
    }

    private Long countSlice(String sliceId, Query query) throws IOException {
        SearcherManager mgr = searcherFor(sliceId);
        if (mgr == null) {
            return 0L;
        }
        slicesSearched.incrementAndGet();
        IndexSearcher searcher = mgr.acquire();
        try {
            return (long) searcher.count(query);
        } finally {
            mgr.release(searcher);
        }
    }

    /**
     * The slice's SearcherManager, opened lazily on first use. Null when the
     * directory holds no committed index yet — a slice whose writer has buffered
     * but not committed is legitimately invisible, which is the same statement as
     * "visibility follows durability".
     */
    private SearcherManager searcherFor(String sliceId) throws IOException {
        SearcherManager existing = searchers.get(sliceId);
        if (existing != null) {
            return existing;
        }
        Path sliceDir = root.resolve(sliceId);
        if (!Files.isDirectory(sliceDir)) {
            return null;
        }
        FSDirectory dir = FSDirectory.open(sliceDir);
        if (!org.apache.lucene.index.DirectoryReader.indexExists(dir)) {
            dir.close();
            return null;
        }
        SearcherManager created = new SearcherManager(dir, null);
        SearcherManager raced = searchers.putIfAbsent(sliceId, created);
        if (raced != null) {
            created.close();
            dir.close();
            return raced;
        }
        directories.put(sliceId, dir);
        return created;
    }

    private void refresh(String sliceId) throws IOException {
        SearcherManager mgr = searchers.get(sliceId);
        if (mgr != null) {
            mgr.maybeRefresh();
        }
    }

    // -----------------------------------------------------------------------
    // Retention
    // -----------------------------------------------------------------------

    /**
     * Drops every slice whose whole epoch interval is older than the cutoff.
     *
     * Retention on this tier is an unlink tree: no tombstones, no merge debt, and
     * nothing to reconcile afterwards. The slice's SearcherManager is closed first
     * so in-flight queries are drained by Lucene's own ref-counting rather than
     * having their files pulled out from under them.
     *
     * A slice a query has already selected as a candidate is DEFERRED, not dropped:
     * see {@link #pins} for why unlinking it instead would answer that query short
     * and silently. Deferral costs the slice one more sweep and nothing else —
     * retention here is a cutoff test on a directory name, so the next pass reaches
     * the same verdict.
     *
     * @return the slice ids dropped
     */
    List<String> dropSlicesEndingBefore(long cutoffMs) throws IOException {
        List<String> dropped = new ArrayList<>();
        List<String> deferred = new ArrayList<>();
        synchronized (writeLock) {
            for (String sliceId : WarmSlices.list(root)) {
                if (WarmSlices.endMsExclusive(sliceId) > cutoffMs) {
                    continue;
                }
                synchronized (pinLock) {
                    if (pins.containsKey(sliceId)) {
                        deferred.add(sliceId);
                        continue;
                    }
                    if (sliceId.equals(currentSliceId)) {
                        closeCurrentWriter();
                    }
                    closeSearcher(sliceId);
                    deleteTree(root.resolve(sliceId));
                }
                dropped.add(sliceId);
            }
        }
        if (!dropped.isEmpty()) {
            log.info("warm retention: dropped {} slice(s) of {} older than {}: {}",
                    dropped.size(), dataset.name, cutoffMs, dropped);
        }
        if (!deferred.isEmpty()) {
            log.info("warm retention: deferred {} slice(s) of {} to the next sweep — a query is reading them: {}",
                    deferred.size(), dataset.name, deferred);
        }
        return dropped;
    }

    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (var walk = Files.walk(dir)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Startup reconciliation
    // -----------------------------------------------------------------------

    /**
     * Keeps the highest-epoch copy of every key and deletes the rest.
     *
     * A cross-slice update is two commits (§new-copy-first above). A process that
     * dies between them leaves the key in two slices, and nothing else would ever
     * notice: the disk leg's epoch clause hides the newer copy, so the stale one
     * answers forever. This runs unconditionally at startup and repairs rather than
     * trusts — the same posture ColdStartRebuilder takes, and for the same reason.
     *
     * Append-only datasets never delete, so the condition cannot arise and this is
     * a no-op for them.
     *
     * @return the number of stale copies deleted
     */
    int reconcile() throws IOException {
        if (dataset.appendOnly) {
            return 0;
        }
        // key -> slice holding the highest epoch seen so far
        Map<String, String> best = new HashMap<>();
        Map<String, Long> bestEpoch = new HashMap<>();
        // key -> every slice it was seen in, in discovery order
        Map<String, List<String>> seen = new LinkedHashMap<>();

        for (String sliceId : WarmSlices.list(root)) {
            for (byte[] proto : allDocs(sliceId)) {
                Message record;
                try {
                    record = dataset.parse(proto);
                } catch (Exception e) {
                    log.warn("warm reconcile: unparseable document in {}/{}: {}",
                            dataset.name, sliceId, e.getMessage());
                    continue;
                }
                String key = dataset.key(record);
                long epoch = dataset.epochMs(record);
                seen.computeIfAbsent(key, k -> new ArrayList<>()).add(sliceId);
                Long current = bestEpoch.get(key);
                if (current == null || epoch > current) {
                    bestEpoch.put(key, epoch);
                    best.put(key, sliceId);
                }
            }
        }

        int deleted = 0;
        for (Map.Entry<String, List<String>> e : seen.entrySet()) {
            String key = e.getKey();
            String keep = best.get(key);
            for (String sliceId : e.getValue()) {
                if (sliceId.equals(keep)) {
                    continue;
                }
                deleteFromSealedSlice(sliceId, key);
                deleted++;
            }
        }
        if (deleted > 0) {
            log.info("warm reconcile: dropped {} stale cross-slice copies from {}", deleted, dataset.name);
        }
        return deleted;
    }

    private List<byte[]> allDocs(String sliceId) throws IOException {
        SearcherManager mgr = searcherFor(sliceId);
        if (mgr == null) {
            return List.of();
        }
        IndexSearcher searcher = mgr.acquire();
        try {
            int n = searcher.getIndexReader().numDocs();
            if (n == 0) {
                return List.of();
            }
            TopDocs top = searcher.search(new MatchAllDocsQuery(), n);
            List<byte[]> out = new ArrayList<>(top.scoreDocs.length);
            var stored = searcher.storedFields();
            for (ScoreDoc sd : top.scoreDocs) {
                BytesRef proto = stored.document(sd.doc, Set.of(PROTO_FIELD)).getBinaryValue(PROTO_FIELD);
                if (proto != null) {
                    out.add(BytesRef.deepCopyOf(proto).bytes);
                }
            }
            return out;
        } finally {
            mgr.release(searcher);
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    @Override
    public void close() throws IOException {
        synchronized (writeLock) {
            if (writer != null) {
                closeCurrentWriter();
            }
        }
        for (String sliceId : List.copyOf(searchers.keySet())) {
            closeSearcher(sliceId);
        }
    }

    private void closeSearcher(String sliceId) throws IOException {
        SearcherManager mgr = searchers.remove(sliceId);
        if (mgr != null) {
            mgr.close();
        }
        FSDirectory dir = directories.remove(sliceId);
        if (dir != null) {
            dir.close();
        }
    }
}
