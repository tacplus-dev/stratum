// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The daily purge -> dump -> truncate transaction.
 *
 * <p>The rows are of a synthetic type carrying the real marker annotations (see
 * {@link TestRecords}) because no fleet record type declares
 * {@code (spec.deleted_flag)} yet. That is the whole reason the memory tier
 * arrives through {@link DailyCheckpoint.Rows}: over the live Hazelcast maps the
 * purge step could only ever be tested against rows it can never select.
 */
class DailyCheckpointTest {

    private static final String TYPE = TestRecords.TYPE;
    private static final long DAY_MS = 24L * 60 * 60 * 1000;

    /** now = day 100; a row deleted before day 70 is past a 30-day max age. */
    private static final long NOW = 100 * DAY_MS;
    private static final long MAX_AGE_MS = 30 * DAY_MS;

    private final List<String> order = new ArrayList<>();

    private DailyCheckpoint checkpointOver(MapRows rows, Path dataRoot) {
        return new DailyCheckpoint(rows,
                (map, key) -> order.add("disk-purge:" + map + "/" + key),
                new TombstoneLog(dataRoot),
                new Checkpoint(dataRoot),
                () -> order.add("truncate"));
    }

    // ---------------------------------------------------------------- step 1

    /**
     * A row that is deleted AND aged past the cutoff is written to the tombstone
     * log with everything the decision names — type, key, tenant, principal, the
     * delete's store epoch, the purge time and the full payload — and only then
     * removed from memory and from disk.
     */
    @Test
    void thePurgeStepTombstonesEveryRowItRemoves(@TempDir Path dataRoot) throws Exception {
        MapRows rows = new MapRows()
                .put(TYPE, "aged-deleted", TestRecords.row(
                        "aged-deleted", "/acme", "spiffe://example/u1", 60 * DAY_MS, true))
                .put(TYPE, "fresh-deleted", TestRecords.row(
                        "fresh-deleted", "/acme", "spiffe://example/u2", 95 * DAY_MS, true))
                .put(TYPE, "aged-live", TestRecords.row(
                        "aged-live", "/acme", "spiffe://example/u3", 60 * DAY_MS, false));

        DailyCheckpoint.Result result = checkpointOver(rows, dataRoot).run(MAX_AGE_MS, NOW);

        assertEquals(1, result.purged);
        assertEquals(List.of("fresh-deleted", "aged-live"), rows.keys(),
                "only the row that is BOTH deleted and aged may be purged");

        TombstoneLog log = new TombstoneLog(dataRoot);
        List<String> lines = Files.readAllLines(log.fileFor(NOW), StandardCharsets.UTF_8);
        assertEquals(1, lines.size(), "one line per purged row");
        JsonObject line = JsonParser.parseString(lines.get(0)).getAsJsonObject();
        assertEquals(TYPE, line.get("record_type").getAsString());
        assertEquals("aged-deleted", line.get("key").getAsString());
        assertEquals("/acme", line.get("tenant").getAsString());
        assertEquals("spiffe://example/u1", line.get("principal").getAsString());
        assertEquals(60 * DAY_MS, line.get("delete_store_epoch_ms").getAsLong());
        assertEquals(NOW, line.get("purged_at_ms").getAsLong());
        assertTrue(line.get("payload").getAsString().contains("aged-deleted"),
                "the full payload is the only trace left of the row: " + line);

        assertTrue(order.contains("disk-purge:" + TYPE + "/aged-deleted"),
                "the Lucene leg must lose the row too, or a purged row is still queryable");
    }

    /** The day file is named for the purge time, UTC, so a pod's day is the same day. */
    @Test
    void tombstonesLandInTheDayFileOfThePurge(@TempDir Path dataRoot) {
        assertEquals("1970-04-11.jsonl",
                new TombstoneLog(dataRoot).fileFor(NOW).getFileName().toString());
    }

    /**
     * Re-running over the same store purges nothing more and writes no second
     * line — and a row purged by a PARTIAL run is simply purged again, which is
     * what makes the transaction safe to abandon and retry.
     */
    @Test
    void theSweepIsIdempotent(@TempDir Path dataRoot) throws Exception {
        MapRows rows = new MapRows().put(TYPE, "gone", TestRecords.row(
                "gone", "/acme", "spiffe://example/u1", 60 * DAY_MS, true));

        assertEquals(1, checkpointOver(rows, dataRoot).run(MAX_AGE_MS, NOW).purged);
        assertEquals(0, checkpointOver(rows, dataRoot).run(MAX_AGE_MS, NOW).purged);
        assertEquals(1,
                Files.readAllLines(new TombstoneLog(dataRoot).fileFor(NOW)).size());
    }

    /** A type carrying no deleted-flag marker has no soft-delete state to purge. */
    @Test
    void aTypeWithNoDeletedFlagIsNeverPurged(@TempDir Path dataRoot) throws Exception {
        MapRows rows = new MapRows().put("letsencrypt.LetsEncryptCert", "evt-1",
                com.github.tacplusdev.stratum.model.types
                        .services.letsencrypt.Letsencrypt.LetsEncryptCert.newBuilder()
                        .setCertSetId("evt-1").setIssuedAt(1L).build());

        assertEquals(0, checkpointOver(rows, dataRoot).run(MAX_AGE_MS, NOW).purged);
        assertFalse(Files.exists(new TombstoneLog(dataRoot).fileFor(NOW)));
    }

    // ---------------------------------------------------------------- step 2

    /**
     * The dump is committed by one rename: what the checkpoint directory holds
     * afterwards is the finished file and the pointer, and no staging file.
     */
    @Test
    void theDumpIsCommittedByRenameAndLeavesNoStagingFile(@TempDir Path dataRoot) throws Exception {
        MapRows rows = new MapRows()
                .put(TYPE, "a", TestRecords.row("a", "/acme", "p", 95 * DAY_MS, false))
                .put(TYPE, "b", TestRecords.row("b", "/acme", "p", 96 * DAY_MS, false));

        DailyCheckpoint.Result result = checkpointOver(rows, dataRoot).run(MAX_AGE_MS, NOW);

        assertEquals(2, result.dumped);
        Path dir = new Checkpoint(dataRoot).dir();
        try (Stream<Path> files = Files.list(dir)) {
            List<String> names = files.map(p -> p.getFileName().toString()).sorted().toList();
            assertEquals(List.of(Long.toString(NOW), Checkpoint.CURRENT), names,
                    "a staging file left behind means the write was not one rename");
        }
        assertEquals(Long.toString(NOW),
                Files.readString(dir.resolve(Checkpoint.CURRENT)).trim());
        assertEquals(result.checkpoint, new Checkpoint(dataRoot).current());
    }

    /**
     * tmp-then-rename is the atomicity boundary: a dump that fails leaves the
     * PREVIOUS checkpoint still named by {@code current} and still complete. The
     * failure is forced by putting a directory where the commit rename has to land.
     */
    @Test
    void aFailedDumpLeavesThePreviousCheckpointIntact(@TempDir Path dataRoot) throws Exception {
        MapRows rows = new MapRows()
                .put(TYPE, "a", TestRecords.row("a", "/acme", "p", 95 * DAY_MS, false));
        Checkpoint checkpoint = new Checkpoint(dataRoot);
        // A generated type for the standing checkpoint, because the assertion below
        // loads it back and a dump is only loadable as the concrete type it names.
        Path first = checkpoint.write(NOW, List.of(new Checkpoint.Row("letsencrypt.LetsEncryptCert", "evt-1",
                com.github.tacplusdev.stratum.model.types
                        .services.letsencrypt.Letsencrypt.LetsEncryptCert.newBuilder()
                        .setCertSetId("evt-1").setIssuedAt(1L).build())));

        // The next checkpoint's committed name, occupied by a non-empty directory:
        // the rename cannot replace it.
        long next = NOW + 1;
        Files.createDirectories(checkpoint.dir().resolve(Long.toString(next)).resolve("blocker"));

        assertThrows(RuntimeException.class,
                () -> checkpointOver(rows, dataRoot).run(MAX_AGE_MS, next));

        assertEquals(first, new Checkpoint(dataRoot).current(),
                "current must still name the last checkpoint that actually committed");
        assertEquals(1, new Checkpoint(dataRoot).load().size());
        assertFalse(order.contains("truncate"),
                "a failed dump must not truncate: the old checkpoint needs that redo tail");
    }

    /** A checkpoint round-trips every row back as its own concrete type. */
    @Test
    void aCheckpointRoundTripsItsRows(@TempDir Path dataRoot) throws Exception {
        var event = com.github.tacplusdev.stratum.model.types
                .services.letsencrypt.Letsencrypt.LetsEncryptCert.newBuilder()
                .setCertSetId("evt-1").setIssuedAt(7L).setSpiffeId("s1").build();
        Checkpoint checkpoint = new Checkpoint(dataRoot);
        checkpoint.write(NOW, List.of(new Checkpoint.Row("letsencrypt.LetsEncryptCert", "evt-1", event)));

        List<Checkpoint.Row> loaded = new Checkpoint(dataRoot).load();
        assertEquals(1, loaded.size());
        assertEquals("letsencrypt.LetsEncryptCert", loaded.get(0).mapName);
        assertEquals("evt-1", loaded.get(0).key);
        assertEquals(event, loaded.get(0).value,
                "the row must come back as the concrete generated type, not a DynamicMessage");
    }

    /** No checkpoint yet is not an error — it is a first boot. */
    @Test
    void anAbsentCheckpointLoadsNothing(@TempDir Path dataRoot) throws Exception {
        assertNull(new Checkpoint(dataRoot).current());
        assertTrue(new Checkpoint(dataRoot).load().isEmpty());
    }

    // ---------------------------------------------------------------- step 3

    /** Truncate runs last, after the dump's rename, and only once. */
    @Test
    void truncateRunsOnlyAfterTheDumpCommits(@TempDir Path dataRoot) throws Exception {
        MapRows rows = new MapRows()
                .put(TYPE, "gone", TestRecords.row("gone", "/acme", "p", 60 * DAY_MS, true))
                .put(TYPE, "kept", TestRecords.row("kept", "/acme", "p", 95 * DAY_MS, false));

        DailyCheckpoint.Result result = checkpointOver(rows, dataRoot).run(MAX_AGE_MS, NOW);

        assertNotNull(result.checkpoint);
        assertEquals(List.of("disk-purge:" + TYPE + "/gone", "truncate"), order,
                "the purge precedes the dump and the truncate follows it");
        assertEquals(1, order.stream().filter("truncate"::equals).count());
    }

    /**
     * A purge failure aborts before the dump, so nothing is truncated either —
     * the previous checkpoint plus the un-truncated log stay a complete recovery
     * set. The failure is forced by making the tombstone directory unwritable.
     */
    @Test
    void aPurgeFailureAbortsBeforeTheDump(@TempDir Path dataRoot) throws Exception {
        MapRows rows = new MapRows().put(TYPE, "gone",
                TestRecords.row("gone", "/acme", "p", 60 * DAY_MS, true));
        // A regular file where the tombstone DIRECTORY must be created.
        Files.createDirectories(dataRoot);
        Files.write(dataRoot.resolve(TombstoneLog.TOMBSTONE_SUBDIR), new byte[0]);

        assertThrows(IOException.class, () -> checkpointOver(rows, dataRoot).run(MAX_AGE_MS, NOW));

        assertEquals(List.of("gone"), rows.keys(),
                "a row whose tombstone could not be written must not be removed");
        assertFalse(order.contains("truncate"));
        assertNull(new Checkpoint(dataRoot).current());
    }
}
