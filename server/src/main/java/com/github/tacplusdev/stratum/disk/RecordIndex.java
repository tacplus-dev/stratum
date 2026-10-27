// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.disk;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import com.github.tacplusdev.stratum.StratumEventStores;

/**
 * One record type's on-disk leg: a Lucene index under
 * {@code <dataRoot>/records/<pkg>.<Type>/}, keyed by the record key and holding
 * the serialized record.
 *
 * <h2>Why this is not the old warm index</h2>
 * The deleted {@code warm/WarmIndex} was the WARM/HOT tiering model's disk half:
 * monthly slices, a per-slice searcher, an epoch interval per slice so a query
 * could prune legs, and one indexed Lucene field per {@code (spec.query_field)}
 * so the disk leg could evaluate predicates on its own. None of that is this. The
 * 2026-09-20 record lifecycle decision has no tiers to partition against and no
 * disk-side query plane: the memory tier answers every predicate, and this leg
 * exists so that a row Hazelcast evicted for idleness is still THERE — a cache
 * miss rather than a deletion — and so that the daily checkpoint has a complete
 * picture to dump. It is therefore a key/value store that happens to be Lucene,
 * one directory per type, three fields, and no slicing.
 *
 * <p>What is carried over from that code is the part that was about Lucene rather
 * than about tiering: a single {@link IndexWriter} per directory under one lock
 * (Lucene's own rule), {@code updateDocument} rather than delete-then-add so a key
 * is never briefly absent, commit-per-write, and a {@link SearcherManager}
 * refreshed off the commit so visibility follows durability.
 *
 * <h2>The document</h2>
 * Three fields, mirroring {@code Checkpoint}'s line format for the same reason:
 * the key the term addresses, the value's protobuf full name, and the value's
 * protobuf bytes. The full name resolves through the generated registry
 * ({@link StratumEventStores#defaultInstance}), so a row comes back as the
 * concrete type the maps and the query plane expect rather than as a
 * DynamicMessage they could not serve — and no Java class name is stored, so the
 * index outlives a move of the generated code's package.
 *
 * <h2>Opened lazily</h2>
 * The directory, the writer and the searcher are created on the first write or
 * read of this type, not when the index object is constructed. A stratum build
 * declares ten record types and a given deployment may never see a row of some of
 * them; creating ten Lucene directories and ten writer lock files at boot for that
 * is cost with nothing behind it. It also keeps {@code StratumMapConfig.build}
 * side-effect free, which is what lets the config tests build a Config without
 * touching a filesystem.
 */
public final class RecordIndex implements Closeable {

    /** Reserved document field: the record key, the term updates and deletes address. */
    static final String KEY_FIELD = "_key";
    /** Reserved document field: the value's protobuf full name. */
    static final String TYPE_FIELD = "_type";
    /** Reserved document field: the record itself, stored verbatim. */
    static final String PROTO_FIELD = "_proto";

    private final String mapName;
    private final Path dir;

    /** Serialises writes to this index. One IndexWriter per directory is Lucene's own rule. */
    private final Object writeLock = new Object();
    private FSDirectory directory;
    private IndexWriter writer;
    private SearcherManager searchers;

    /** Proto full name -> that type's parser. Resolved once; an index holds one type many times. */
    private final Map<String, Parser<? extends Message>> parsers = new ConcurrentHashMap<>();

    RecordIndex(String mapName, Path dir) {
        this.mapName = mapName;
        this.dir = dir;
    }

    /** The record type this index holds, which is also its Hazelcast map name. */
    public String mapName() {
        return mapName;
    }

    /** This index's directory under the pod's volume. */
    public Path dir() {
        return dir;
    }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /**
     * Writes one row, replacing any copy of the same key.
     *
     * <p>{@code updateDocument} + {@code commit()} — the only Lucene operation
     * after which a hard kill leaves the document present, and one operation rather
     * than a delete followed by an add, so there is no window in which the key is
     * absent from disk while it is also absent from memory. There is no batching
     * window: a timer would silently move the guarantee, and this leg is written
     * off the ack path already (see {@link RecordDisk}), so the commit costs the
     * write nothing it was waiting on.
     */
    public void put(String key, Message value) throws IOException {
        Document doc = document(key, value.getDescriptorForType().getFullName(),
                new BytesRef(value.toByteArray()));
        synchronized (writeLock) {
            IndexWriter w = open();
            w.updateDocument(new Term(KEY_FIELD, key), doc);
            w.commit();
            searchers.maybeRefresh();
        }
    }

    /**
     * Drops one key outright — the disk half of the daily checkpoint's purge step.
     *
     * <p>Not a variant of {@link #put}'s replace: that one swaps a row for a newer
     * copy of itself, this one removes the row with nothing left behind, which is
     * the state the decision calls dead — "never loaded into Hazelcast again, not
     * tiered, not cached briefly, not readable". The only trace left is the
     * tombstone line the purge wrote before calling here.
     */
    public void delete(String key) throws IOException {
        synchronized (writeLock) {
            IndexWriter w = open();
            w.deleteDocuments(new Term(KEY_FIELD, key));
            w.commit();
            searchers.maybeRefresh();
        }
    }

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    /**
     * The row this index holds for {@code key}, or null when it holds none.
     *
     * <p>This is what fires under a memory miss: the record maps carry a
     * {@link RecordDiskLoader} whose {@code load} is this call, so a row Hazelcast
     * evicted on its idle clock comes back transparently on the next {@code get}.
     */
    public Message get(String key) throws IOException {
        SearcherManager mgr = searcherFor();
        IndexSearcher searcher = mgr.acquire();
        try {
            TopDocs top = searcher.search(new TermQuery(new Term(KEY_FIELD, key)), 1);
            if (top.scoreDocs.length == 0) {
                return null;
            }
            return parse(searcher.storedFields(), top.scoreDocs[0].doc);
        } finally {
            mgr.release(searcher);
        }
    }

    /**
     * Every row this index holds, in no particular order.
     *
     * <p>The daily checkpoint's dump source. It materialises the whole type, which
     * is exactly what a dump is: {@code Checkpoint.write} takes an Iterable of every
     * surviving row and writes one line each, so there is nothing to stream to. A
     * {@link LinkedHashMap} rather than a list because the caller unions this with
     * the memory tier by key.
     */
    public Map<String, Message> entries() throws IOException {
        SearcherManager mgr = searcherFor();
        IndexSearcher searcher = mgr.acquire();
        try {
            Map<String, Message> out = new LinkedHashMap<>();
            int total = searcher.getIndexReader().numDocs();
            if (total == 0) {
                return out;
            }
            TopDocs top = searcher.search(new MatchAllDocsQuery(), total);
            StoredFields stored = searcher.storedFields();
            for (ScoreDoc sd : top.scoreDocs) {
                out.put(keyOf(stored, sd.doc), parse(stored, sd.doc));
            }
            return out;
        } finally {
            mgr.release(searcher);
        }
    }

    /** How many rows this index holds. */
    public int size() throws IOException {
        SearcherManager mgr = searcherFor();
        IndexSearcher searcher = mgr.acquire();
        try {
            return searcher.getIndexReader().numDocs();
        } finally {
            mgr.release(searcher);
        }
    }

    // -----------------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------------

    /** One row's document. Shared with {@link ClassNameMigration}, which rewrites rows into it. */
    static Document document(String key, String type, BytesRef proto) {
        Document doc = new Document();
        doc.add(new StringField(KEY_FIELD, key, Field.Store.YES));
        doc.add(new StoredField(TYPE_FIELD, type));
        doc.add(new StoredField(PROTO_FIELD, proto));
        return doc;
    }

    /**
     * The key of one document.
     *
     * <p>{@code KEY_FIELD} is written {@code Store.YES}, i.e. indexed AND stored,
     * because both directions are needed: {@link #get} and {@link #delete} address
     * the key as a term, while {@link #entries} starts from a document id and has
     * to read the key back off it.
     */
    private static String keyOf(StoredFields stored, int docId) throws IOException {
        return stored.document(docId, Set.of(KEY_FIELD)).get(KEY_FIELD);
    }

    private Message parse(StoredFields stored, int docId) throws IOException {
        Document doc = stored.document(docId, Set.of(TYPE_FIELD, PROTO_FIELD));
        String type = doc.get(TYPE_FIELD);
        BytesRef proto = doc.getBinaryValue(PROTO_FIELD);
        return parserFor(type).parseFrom(proto.bytes, proto.offset, proto.length);
    }

    /**
     * The parser for a stored protobuf full name.
     *
     * <p>An unresolvable type FAILS the read rather than being skipped, the same
     * posture {@code Checkpoint.load} takes for the same reason: this leg is what a
     * memory miss and the checkpoint dump are both built on, and quietly dropping
     * rows out of it produces a store that is silently incomplete.
     */
    private Parser<? extends Message> parserFor(String type) {
        return parsers.computeIfAbsent(type, t -> {
            Message defaultInstance = StratumEventStores.defaultInstance(t);
            if (defaultInstance == null) {
                throw new IllegalStateException("the disk leg for " + mapName + " holds rows of " + t
                        + ", which is not a record type this build declares;"
                        + " refusing to answer a read from a partial index");
            }
            return defaultInstance.getParserForType();
        });
    }

    private SearcherManager searcherFor() throws IOException {
        synchronized (writeLock) {
            open();
            return searchers;
        }
    }

    /**
     * Opens the directory, the writer and the searcher on first use.
     *
     * <p>The {@link SearcherManager} is opened ON THE WRITER rather than on the
     * directory. A directory-backed manager cannot see a commit until it reopens
     * and cannot be opened at all until the first commit has happened, which would
     * make a read of a type nothing has written yet an error rather than an empty
     * answer. Opened on the writer, an index with no rows in it is simply an index
     * with no rows in it.
     *
     * <p>Called under {@link #writeLock} by every caller, so the lazy creation does
     * not need a second lock of its own.
     */
    private IndexWriter open() throws IOException {
        if (writer != null) {
            return writer;
        }
        Files.createDirectories(dir);
        directory = FSDirectory.open(dir);
        writer = new IndexWriter(directory, new IndexWriterConfig());
        // applyAllDeletes/writeAllDeletes true: this leg's whole job is to answer
        // for a key, so a delete that a searcher cannot see yet is a row that is
        // readable after it was purged.
        searchers = new SearcherManager(writer, true, true, null);
        return writer;
    }

    @Override
    public void close() {
        synchronized (writeLock) {
            try {
                if (searchers != null) {
                    searchers.close();
                }
                if (writer != null) {
                    writer.close();
                }
                if (directory != null) {
                    directory.close();
                }
            } catch (IOException e) {
                throw new IllegalStateException("the disk leg for " + mapName + " did not close cleanly", e);
            } finally {
                searchers = null;
                writer = null;
                directory = null;
            }
        }
    }
}
