// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentEvent;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.HookPhase;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorEventType;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SensorType;

/**
 * The warm tier, against a real Lucene index on a real filesystem.
 *
 * The dataset descriptor here is hand-built over AgentEvent. That is not a
 * shortcut around the generator: the generator's job is to turn a (spec.record)
 * declaration into exactly such a descriptor, and no record type declares
 * RESIDENCY_WARM yet, so binding one by hand is the only way to exercise the
 * runtime the generator targets. What the generator emits is pinned separately,
 * in the codegen module's WarmStoreGeneratorTest.
 *
 * Epochs are absolute instants, never "now minus something": a test that computed
 * its slices from the clock would land in different months depending on the day
 * it ran, and the whole point of the layout is that the slice a row lands in is a
 * function of its epoch alone.
 */
class WarmTierTest {

    private static final String DATASET = "agentsensor.AgentEvent";
    private static final long DAY_MS = 24L * 60 * 60 * 1000;

    private static long ms(String isoInstant) {
        return Instant.parse(isoInstant).toEpochMilli();
    }

    /**
     * A warm dataset over AgentEvent: keyed by event_id, epoch-stamped on
     * timestamp_ms, filterable on spiffe_id / session_id / event_type.
     */
    private static WarmDataset dataset(boolean appendOnly, long windowMs) {
        return new WarmDataset(DATASET, AgentEvent.class, "timestamp_ms", appendOnly, windowMs,
                List.of(
                        new WarmField("spiffe_id", WarmField.Kind.EQ_STRING,
                                m -> ((AgentEvent) m).getSpiffeId()),
                        new WarmField("session_id", WarmField.Kind.EQ_STRING,
                                m -> ((AgentEvent) m).getSessionId()),
                        new WarmField("event_type", WarmField.Kind.EQ_NUMERIC,
                                m -> (long) ((AgentEvent) m).getEventTypeValue())),
                m -> ((AgentEvent) m).getEventId(),
                m -> ((AgentEvent) m).getTimestampMs(),
                AgentEvent::parseFrom,
                m -> com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types
                        .vservices.agentsensor.Helpers.validateAgentEvent((AgentEvent) m));
    }

    private static AgentEvent event(String id, String isoInstant, String spiffeId) {
        return AgentEvent.newBuilder()
                .setEventId(id)
                .setTimestampMs(ms(isoInstant))
                .setSpiffeId(spiffeId)
                .setSessionId("session-" + spiffeId)
                .setAgentId("agent-" + id)
                .setEventType(SensorEventType.SENSOR_EVENT_TYPE_TOOL_USE)
                .setSensorType(SensorType.SENSOR_TYPE_CLAUDE_CODE)
                .setHookPhase(HookPhase.HOOK_PHASE_POST)
                .build();
    }

    private static List<String> idsOf(List<byte[]> hits) throws IOException {
        List<String> ids = new ArrayList<>();
        for (byte[] b : hits) {
            ids.add(AgentEvent.parseFrom(b).getEventId());
        }
        java.util.Collections.sort(ids);
        return ids;
    }

    /** A window of one day puts every 2025/2026 fixture epoch on the disk side of the boundary. */
    private static WarmStore store(Path dir, boolean appendOnly) throws IOException {
        return new WarmStore(dir, List.of(dataset(appendOnly, DAY_MS)));
    }

    // -----------------------------------------------------------------------

    @Test
    void aWrittenRowComesBackFromDisk(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            assertTrue(warm.enqueue(event("a", "2025-03-15T00:00:00Z", "spiffe://one")));
            warm.awaitWrites(10_000);

            assertEquals(List.of("a"),
                    idsOf(warm.query(DATASET, new WarmFilter().eq("spiffe_id", "spiffe://one"))));
            assertEquals(1, warm.count(DATASET, new WarmFilter().eq("spiffe_id", "spiffe://one")));
            assertEquals(0, warm.count(DATASET, new WarmFilter().eq("spiffe_id", "spiffe://other")));
        }
    }

    @Test
    void aRecordOfAnUnknownTypeIsNotClaimed(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            assertFalse(warm.enqueue(
                    com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types
                            .services.tasks.TasksOuterClass.TaskActivity.getDefaultInstance()));
        }
    }

    /**
     * Acceptance: a query whose own epoch floor is inside the memory window
     * demonstrably skips the disk leg. "Demonstrably" is the counter — asserting
     * only that the result is empty would pass just as well if every slice had been
     * opened and had matched nothing.
     */
    @Test
    void anEpochBoundInsideTheWindowSkipsTheLegWithoutOpeningASlice(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("a", "2025-03-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);
            WarmIndex idx = warm.index(DATASET);

            long opened = idx.slicesSearched();
            WarmFilter insideWindow = new WarmFilter()
                    .range("timestamp_ms", System.currentTimeMillis(), null);

            assertEquals(List.of(), warm.query(DATASET, insideWindow));
            assertEquals(0, warm.count(DATASET, insideWindow));
            assertEquals(opened, idx.slicesSearched(), "the disk leg opened a slice it could prove was empty");
        }
    }

    /** A slice whose interval cannot intersect the query is never opened. */
    @Test
    void slicesOutsideTheQueryRangeAreNeverOpened(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("jan", "2025-01-15T00:00:00Z", "spiffe://one"));
            warm.enqueue(event("mar", "2025-03-15T00:00:00Z", "spiffe://one"));
            warm.enqueue(event("jun", "2025-06-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);
            WarmIndex idx = warm.index(DATASET);

            assertEquals(List.of("2025-01", "2025-03", "2025-06"),
                    WarmSlices.list(dir.resolve(WarmStore.WARM_SUBDIR).resolve(DATASET)));

            long before = idx.slicesSearched();
            List<byte[]> hits = warm.query(DATASET, new WarmFilter()
                    .range("timestamp_ms", ms("2025-03-01T00:00:00Z"), ms("2025-03-31T00:00:00Z")));

            assertEquals(List.of("mar"), idsOf(hits));
            assertEquals(1, idx.slicesSearched() - before, "exactly one slice should have been opened");
        }
    }

    /** The count is a plain sum over the candidate slices. */
    @Test
    void countSumsAcrossEverySliceTheQueryTouches(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("jan", "2025-01-15T00:00:00Z", "spiffe://one"));
            warm.enqueue(event("feb", "2025-02-15T00:00:00Z", "spiffe://one"));
            warm.enqueue(event("mar", "2025-03-15T00:00:00Z", "spiffe://two"));
            warm.awaitWrites(10_000);

            assertEquals(3, warm.count(DATASET, new WarmFilter()));
            assertEquals(2, warm.count(DATASET, new WarmFilter().eq("spiffe_id", "spiffe://one")));
            assertEquals(1, warm.count(DATASET, new WarmFilter()
                    .eq("spiffe_id", "spiffe://one")
                    .range("timestamp_ms", ms("2025-02-01T00:00:00Z"), null)));
        }
    }

    /**
     * An update that moves a row across slices leaves exactly ONE copy on disk.
     * Two would be counted twice, which is the failure the epoch partition exists
     * to prevent.
     */
    @Test
    void anUpdateAcrossSlicesLeavesOneCopy(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("k", "2025-01-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);
            warm.enqueue(event("k", "2025-03-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);

            List<byte[]> hits = warm.query(DATASET, new WarmFilter());
            assertEquals(List.of("k"), idsOf(hits));
            assertEquals(1, warm.count(DATASET, new WarmFilter()));
            assertEquals(ms("2025-03-15T00:00:00Z"), AgentEvent.parseFrom(hits.get(0)).getTimestampMs(),
                    "the surviving copy must be the newer one");
        }
    }

    /** An in-slice update is one updateDocument and one commit, so it replaces rather than adds. */
    @Test
    void anUpdateWithinASliceReplacesTheRow(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("k", "2025-03-01T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);
            warm.enqueue(event("k", "2025-03-20T00:00:00Z", "spiffe://two"));
            warm.awaitWrites(10_000);

            assertEquals(1, warm.count(DATASET, new WarmFilter()));
            assertEquals(0, warm.count(DATASET, new WarmFilter().eq("spiffe_id", "spiffe://one")));
            assertEquals(1, warm.count(DATASET, new WarmFilter().eq("spiffe_id", "spiffe://two")));
        }
    }

    /**
     * The window between the ack (a memory put) and the disk commit is a window in
     * which the disk copy is stale. The disk leg must not serve it, or the row is
     * counted once in memory and once on disk.
     */
    @Test
    void aPendingWriteHidesTheStaleDiskCopy(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("k", "2025-03-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);
            assertEquals(1, warm.count(DATASET, new WarmFilter()));

            // The state between the memory put and the disk commit.
            warm.index(DATASET).markPending("k");
            assertEquals(0, warm.count(DATASET, new WarmFilter()),
                    "the disk leg served a row whose in-memory copy is newer");
            assertEquals(List.of(), warm.query(DATASET, new WarmFilter()));

            warm.index(DATASET).pendingKeys().remove("k");
            assertEquals(1, warm.count(DATASET, new WarmFilter()));
        }
    }

    /**
     * Retention drops directories, not rows. Afterwards a query for the dropped
     * period opens zero slices and returns nothing — observable as "the directory
     * is gone", which is the point of pruning being structural.
     */
    @Test
    void retentionDropsWholeSlicesAndLeavesNoDirectory(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("jan", "2025-01-15T00:00:00Z", "spiffe://one"));
            warm.enqueue(event("feb", "2025-02-15T00:00:00Z", "spiffe://one"));
            warm.enqueue(event("jun", "2025-06-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);

            Path datasetRoot = dir.resolve(WarmStore.WARM_SUBDIR).resolve(DATASET);
            assertEquals(List.of("2025-01", "2025-02", "2025-06"), WarmSlices.list(datasetRoot));

            // Everything wholly before 2025-03-01 goes: 2025-01 and 2025-02.
            assertEquals(List.of("2025-01", "2025-02"),
                    warm.runRetention(ms("2025-03-01T00:00:00Z")).get(DATASET));

            assertEquals(List.of("2025-06"), WarmSlices.list(datasetRoot));
            assertFalse(Files.exists(datasetRoot.resolve("2025-01")));
            assertEquals(List.of("jun"), idsOf(warm.query(DATASET, new WarmFilter())));

            long opened = warm.index(DATASET).slicesSearched();
            assertEquals(0, warm.count(DATASET, new WarmFilter()
                    .range("timestamp_ms", ms("2025-01-01T00:00:00Z"), ms("2025-01-31T00:00:00Z"))));
            assertEquals(opened, warm.index(DATASET).slicesSearched(),
                    "a dropped period has no slice left to open");
        }
    }

    /** Retention must not spare a slice merely because it is the one being written. */
    @Test
    void retentionDropsTheCurrentSliceToo(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("jan", "2025-01-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);

            assertEquals(List.of("2025-01"), warm.runRetention(ms("2025-03-01T00:00:00Z")).get(DATASET));
            assertEquals(0, warm.count(DATASET, new WarmFilter()));

            // ... and the tier still takes writes afterwards.
            warm.enqueue(event("jun", "2025-06-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);
            assertEquals(1, warm.count(DATASET, new WarmFilter()));
        }
    }

    /**
     * The cross-slice update is two commits. A process that dies between them leaves
     * a stale copy in the old slice that nothing would otherwise notice, because the
     * disk leg's epoch clause hides the newer one and the stale one answers forever.
     *
     * The interrupted state is built here with a raw IndexWriter, because that is
     * precisely what "the second commit never happened" looks like on disk.
     */
    @Test
    void reconcileKeepsTheHighestEpochCopyOfACrossSliceKey(@TempDir Path dir) throws Exception {
        Path datasetRoot = dir.resolve(WarmStore.WARM_SUBDIR).resolve(DATASET);
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("k", "2025-01-15T00:00:00Z", "spiffe://stale"));
            warm.awaitWrites(10_000);
        }
        // Leg (1a) of a cross-slice update — the new copy — with leg (1b), the old
        // copy's delete, never reached.
        writeRawDocument(datasetRoot.resolve("2025-03"), event("k", "2025-03-15T00:00:00Z", "spiffe://fresh"));

        try (WarmStore warm = store(dir, false)) {
            assertEquals(2, warm.count(DATASET, new WarmFilter()), "the interrupted state should hold both copies");
            warm.reconcile();

            List<byte[]> hits = warm.query(DATASET, new WarmFilter());
            assertEquals(List.of("k"), idsOf(hits));
            assertEquals("spiffe://fresh", AgentEvent.parseFrom(hits.get(0)).getSpiffeId());
        }
    }

    /** Append-only datasets never delete, so the condition reconcile repairs cannot arise. */
    @Test
    void reconcileLeavesAnAppendOnlyDatasetAlone(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, true)) {
            warm.enqueue(event("k", "2025-01-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);
            warm.enqueue(event("k", "2025-03-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);

            assertEquals(2, warm.count(DATASET, new WarmFilter()), "append-only keeps every row it was given");
            warm.reconcile();
            assertEquals(2, warm.count(DATASET, new WarmFilter()));
        }
    }

    /**
     * Acceptance: an append-only dataset's sealed slice files are never rewritten.
     * The checksum is over the sealed segment files, so a stable checksum across a
     * write load is evidence rather than a claim.
     */
    @Test
    void aSealedAppendOnlySliceIsByteStableAcrossFurtherWrites(@TempDir Path dir) throws Exception {
        Path datasetRoot = dir.resolve(WarmStore.WARM_SUBDIR).resolve(DATASET);
        try (WarmStore warm = store(dir, true)) {
            warm.enqueue(event("jan-1", "2025-01-10T00:00:00Z", "spiffe://one"));
            warm.enqueue(event("jan-2", "2025-01-20T00:00:00Z", "spiffe://one"));
            // Rolling into February seals January.
            warm.enqueue(event("feb-1", "2025-02-10T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);

            Path january = datasetRoot.resolve("2025-01");
            assertTrue(Files.isRegularFile(january.resolve("slice.json")), "a sealed slice carries its manifest");
            String sealedChecksum = WarmSliceMeta.checksum(january);

            for (int i = 0; i < 25; i++) {
                warm.enqueue(event("feb-load-" + i, "2025-02-11T00:00:00Z", "spiffe://one"));
            }
            warm.awaitWrites(10_000);

            assertEquals(sealedChecksum, WarmSliceMeta.checksum(january),
                    "a sealed append-only slice was rewritten under a write load");
            assertEquals(28, warm.count(DATASET, new WarmFilter()));
        }
    }

    /** The manifest is written at seal time and is never consulted to answer a query. */
    @Test
    void sealingWritesTheManifestAndQueriesDoNotNeedIt(@TempDir Path dir) throws Exception {
        Path datasetRoot = dir.resolve(WarmStore.WARM_SUBDIR).resolve(DATASET);
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("jan", "2025-01-15T00:00:00Z", "spiffe://one"));
            warm.enqueue(event("mar", "2025-03-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);

            String manifest = Files.readString(datasetRoot.resolve("2025-01").resolve("slice.json"));
            assertTrue(manifest.contains("\"slice_id\": \"2025-01\""), manifest);
            assertTrue(manifest.contains("\"doc_count\": 1"), manifest);
            assertTrue(manifest.contains("\"min_epoch_ms\": " + ms("2025-01-15T00:00:00Z")), manifest);

            // Delete it: pruning reads the directory NAME, so the query is unaffected.
            Files.delete(datasetRoot.resolve("2025-01").resolve("slice.json"));
            assertEquals(List.of("jan", "mar"), idsOf(warm.query(DATASET, new WarmFilter())));
        }
    }

    /** Rows survive the process. A reopened store answers from what is on disk. */
    @Test
    void aReopenedStoreServesWhatWasCommitted(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("jan", "2025-01-15T00:00:00Z", "spiffe://one"));
            warm.enqueue(event("mar", "2025-03-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);
        }
        try (WarmStore reopened = store(dir, false)) {
            assertEquals(List.of("jan", "mar"), idsOf(reopened.query(DATASET, new WarmFilter())));
        }
    }

    /** An empty warm tier — stratum's state today — writes nothing and touches no volume. */
    @Test
    void aWarmTierWithNoDatasetsCreatesNothing(@TempDir Path dir) throws Exception {
        Path root = dir.resolve("never-created");
        try (WarmStore warm = new WarmStore(root, List.of())) {
            assertTrue(warm.isEmpty());
            assertFalse(warm.enqueue(event("a", "2025-01-15T00:00:00Z", "spiffe://one")));
            assertEquals(0, warm.count(DATASET, new WarmFilter()));
            assertEquals(List.of(), warm.query(DATASET, new WarmFilter()));
        }
        assertFalse(Files.exists(root), "an empty warm tier must not create a directory on the volume");
    }

    /**
     * The claim covers the interval BEFORE the write is enqueued.
     *
     * The duplicate the pending set exists to prevent does not begin at the disk
     * write, it begins at the memory put: from there memory's copy is the newer one
     * while the disk still holds the previous one. So the disk leg has to stop
     * answering for the key at the CLAIM, with nothing else having happened yet —
     * which is exactly what this asserts, since no memory put and no enqueue follow
     * it here.
     */
    @Test
    void aClaimHidesTheStaleDiskCopyBeforeAnythingIsEnqueued(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            warm.enqueue(event("k", "2025-03-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);
            assertEquals(1, warm.count(DATASET, new WarmFilter()));

            AgentEvent update = event("k", "2025-06-15T00:00:00Z", "spiffe://two");
            assertTrue(warm.claim(update), "a warm dataset holds this type; the claim was refused");
            assertEquals(0, warm.count(DATASET, new WarmFilter()),
                    "the disk leg still answers for a key whose memory copy may already be newer");
            assertEquals(List.of(), warm.query(DATASET, new WarmFilter()));

            // Memory owns the key for exactly as long as the claim is held, so a row
            // aged past the boundary is not lost by both legs at once.
            assertTrue(warm.residency(DATASET).inMemory("k", ms("2025-03-15T00:00:00Z")));

            warm.release(update);
            assertEquals(1, warm.count(DATASET, new WarmFilter()),
                    "the released claim still hides the key from the disk leg");
            assertFalse(warm.residency(DATASET).inMemory("k", ms("2025-03-15T00:00:00Z")));
        }
    }

    /** A claim on a type no warm dataset holds is refused, so nothing is owed. */
    @Test
    void aClaimOnAnUnheldTypeIsRefused(@TempDir Path dir) throws Exception {
        try (WarmStore warm = new WarmStore(dir.resolve("empty"), List.of())) {
            assertFalse(warm.claim(event("a", "2025-01-15T00:00:00Z", "spiffe://one")));
        }
    }

    /**
     * The validator-rejection leg releases the claim it was given.
     *
     * Nothing else can: no write is submitted, so no commit will ever clear the key.
     * A leak here is worse than the duplicate the claim prevents — it hides the key
     * from the disk leg for the life of the process — so the proof is that a later
     * legitimate write for the SAME key is served afterwards.
     */
    @Test
    void anUnstorableRecordReleasesTheClaimItWasGiven(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            AgentEvent invalid = AgentEvent.newBuilder()
                    .setEventId("k")
                    .setTimestampMs(ms("2025-03-15T00:00:00Z"))
                    .build(); // event_type unset — the generated validator refuses it

            assertTrue(warm.claim(invalid));
            assertTrue(warm.enqueue(invalid), "the dataset still claims the type");
            warm.awaitWrites(10_000);
            assertEquals(1, warm.rejectedCount());
            assertFalse(warm.index(DATASET).pendingKeys().contains("k"),
                    "the rejected record left its key claimed forever");

            warm.enqueue(event("k", "2025-03-15T00:00:00Z", "spiffe://one"));
            warm.awaitWrites(10_000);
            assertEquals(1, warm.count(DATASET, new WarmFilter()),
                    "a leaked claim is hiding a legitimately written row from the disk leg");
            assertEquals(List.of("k"), idsOf(warm.query(DATASET, new WarmFilter())));
        }
    }

    /**
     * The hot store drops a record that fails the generated validator, because the
     * read path validates every item in a page and one bad row fails the whole
     * response. A disk copy that outlived the memory copy would reintroduce exactly
     * that, permanently.
     */
    @Test
    void anUnstorableRecordIsClaimedAndDropped(@TempDir Path dir) throws Exception {
        try (WarmStore warm = store(dir, false)) {
            AgentEvent invalid = AgentEvent.newBuilder()
                    .setEventId("bad")
                    .setTimestampMs(ms("2025-03-15T00:00:00Z"))
                    .build(); // event_type unset — the generated validator refuses it

            assertTrue(warm.enqueue(invalid), "the dataset still claims the type");
            warm.awaitWrites(10_000);

            assertEquals(1, warm.rejectedCount());
            assertEquals(0, warm.count(DATASET, new WarmFilter()));
        }
    }

    // -----------------------------------------------------------------------

    /** Adds a document straight to a slice, bypassing the tier's own write path. */
    private static void writeRawDocument(Path sliceDir, AgentEvent record) throws IOException {
        Files.createDirectories(sliceDir);
        Document doc = new Document();
        doc.add(new StringField(WarmIndex.KEY_FIELD, record.getEventId(), Field.Store.NO));
        doc.add(new LongPoint(WarmIndex.EPOCH_FIELD, record.getTimestampMs()));
        doc.add(new NumericDocValuesField(WarmIndex.EPOCH_FIELD, record.getTimestampMs()));
        doc.add(new StoredField(WarmIndex.PROTO_FIELD, new BytesRef(record.toByteArray())));
        doc.add(new StringField("spiffe_id", record.getSpiffeId(), Field.Store.NO));
        try (FSDirectory dir = FSDirectory.open(sliceDir);
             IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            w.addDocument(doc);
            w.commit();
        }
    }
}
