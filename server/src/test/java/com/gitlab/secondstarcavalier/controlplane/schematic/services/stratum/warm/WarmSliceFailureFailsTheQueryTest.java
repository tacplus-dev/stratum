// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.StratumWarmStores;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.accounting.Accounting;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.accountingrecord.Accountingrecord.AccountingRecord;

/**
 * A slice that cannot be read has to fail the query, not shrink it.
 *
 * The fan-out used to catch a slice task's failure, log it, and go on summing the
 * slices that did answer. The caller then got a short list and a short count with
 * no error on them — "one of your two months would not open" arrived looking
 * exactly like "there is only one month of data", and a retention or billing read
 * over a corrupt slice undercounts silently and forever.
 *
 * The rows here are dated into two fixed, long-past calendar months, so they land
 * in two different slice directories ({@code WarmSlices} is monthly, UTC) and both
 * are well outside any memory-residency window — the query is entirely a disk-leg
 * query, with no clock dependence beyond "2020 is in the past".
 */
class WarmSliceFailureFailsTheQueryTest {

    private static final String DATASET = "accountingrecord.AccountingRecord";
    private static final long WINDOW_SECONDS = 604_800L;

    /** 2020-01-15T00:00:00Z and 2020-02-15T00:00:00Z — two different monthly slices. */
    private static final long JAN_2020 = 1_579_046_400_000L;
    private static final long FEB_2020 = 1_581_724_800_000L;

    private static AccountingRecord row(String id, long epochMs) {
        return AccountingRecord.newBuilder()
                .setAccountingId(id)
                .setRequestId("req-" + id)
                .setSourceId("stratum-0")
                .setTimestampMs(epochMs)
                .setLevel(Accounting.EventLevel.EVENT_LEVEL_INFO)
                .setMethod("query.RecordMap/ListAccountingRecord")
                .setCode(0)
                .setDurationMs(7)
                .setPrincipalSub("joeuser@cav")
                .setSpiffeId("spiffe://cav/tenants/t1/services/stratum")
                .setStoreEpochMs(epochMs)
                .build();
    }

    /** Commits one row per month and returns the dataset's index root. */
    private static Path twoCommittedSlices(Path dataRoot) throws Exception {
        WarmStore warm = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));
        try {
            Path root = warm.index(DATASET).root();
            assertTrue(warm.enqueue(row("acct-jan", JAN_2020)));
            assertTrue(warm.enqueue(row("acct-feb", FEB_2020)));
            // close() drains the single-thread writer and awaits it, so both commits
            // have certainly happened by the time it returns. No sleep, no polling.
            warm.close();
            assertEquals(List.of("2020-01", "2020-02"), WarmSlices.list(root),
                    "the two rows must land in two different monthly slices for this test to mean anything");
            return root;
        } finally {
            warm.close();
        }
    }

    /**
     * Makes one slice unreadable by overwriting its commit point with bytes that are
     * not a segments file. {@code DirectoryReader.indexExists} still says yes (it
     * reads file NAMES), so the reader really is opened and really does throw —
     * which is the corrupt-slice case, not a missing-directory shortcut.
     */
    private static void corruptSlice(Path sliceDir) throws IOException {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(sliceDir, "segments*")) {
            for (Path f : files) {
                Files.write(f, "not a lucene commit point".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        StandardOpenOption.TRUNCATE_EXISTING);
            }
        }
    }

    @Test
    void aCorruptSliceFailsTheQueryInsteadOfUndercountingIt() throws Exception {
        Path dataRoot = Files.createTempDirectory("stratum-warm-badslice");
        Path root = twoCommittedSlices(dataRoot);

        WarmStore intact = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));
        try {
            assertEquals(2, intact.count(DATASET, new WarmFilter()),
                    "both slices are readable to begin with — 2 is the complete answer");
        } finally {
            intact.close();
        }

        corruptSlice(root.resolve("2020-02"));

        WarmStore broken = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));
        try {
            IOException counted = assertThrows(IOException.class,
                    () -> broken.count(DATASET, new WarmFilter()),
                    "a slice that will not open must fail the count — swallowing it returns 1,"
                            + " which the caller cannot tell apart from a complete answer of 1");
            assertTrue(counted.getMessage().contains("2020-02"),
                    "and the error must name the slice that failed: " + counted.getMessage());

            IOException queried = assertThrows(IOException.class,
                    () -> broken.query(DATASET, new WarmFilter()),
                    "the hit list fails for the same reason the count does — one fan-out, one rule");
            assertTrue(queried.getMessage().contains("2020-02"), queried.getMessage());
        } finally {
            broken.close();
        }
    }

    /**
     * The fix is "fail on a slice that fails", not "fail whenever a slice is
     * missing". A dataset whose every slice reads must still answer normally, or
     * the previous test would pass for a store that simply refuses everything.
     */
    @Test
    void everySliceReadableStillAnswers() throws Exception {
        Path dataRoot = Files.createTempDirectory("stratum-warm-goodslices");
        twoCommittedSlices(dataRoot);

        WarmStore warm = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));
        try {
            assertEquals(2, warm.count(DATASET, new WarmFilter()));
            assertEquals(2, warm.query(DATASET, new WarmFilter()).size());
        } finally {
            warm.close();
        }
    }
}
