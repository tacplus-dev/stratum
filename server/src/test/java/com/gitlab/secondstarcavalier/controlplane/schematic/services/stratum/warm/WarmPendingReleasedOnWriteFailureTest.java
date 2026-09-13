// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.StratumWarmStores;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.accounting.Accounting;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.accountingrecord.Accountingrecord.AccountingRecord;

/**
 * A disk write that FAILS owes the key its claim back.
 *
 * The claim ({@code WarmIndex.pendingKeys}) is what makes memory the unambiguous
 * owner of a row while its disk copy may be stale, and only a COMMITTED write
 * clears it. The asynchronous writer used to swallow a failure with a log line and
 * a counter, leaving the key claimed with nothing left to clear it: the disk leg
 * excluded that key for the life of the process, and {@code WarmResidency} pinned
 * its memory copy as un-evictable for just as long. One transient IO error was
 * permanent.
 */
class WarmPendingReleasedOnWriteFailureTest {

    private static final String DATASET = "accountingrecord.AccountingRecord";
    private static final long WINDOW_SECONDS = 604_800L;

    private static AccountingRecord row(String id) {
        return AccountingRecord.newBuilder()
                .setAccountingId(id)
                .setRequestId("req-" + id)
                .setSourceId("stratum-0")
                .setTimestampMs(1000)
                .setLevel(Accounting.EventLevel.EVENT_LEVEL_INFO)
                .setMethod("query.RecordMap/ListAccountingRecord")
                .setCode(0)
                .setDurationMs(7)
                .setPrincipalSub("joeuser@cav")
                .setSpiffeId("spiffe://cav/tenants/t1/services/stratum")
                .setStoreEpochMs(System.currentTimeMillis())
                .build();
    }

    /**
     * Breaks the dataset's index root by replacing the directory the slice would be
     * created under with a regular FILE. {@code Files.createDirectories} then fails
     * for every slice, deterministically and without depending on file modes (which
     * a root-owned CI container would ignore).
     */
    private static void breakIndexRoot(Path root) throws IOException {
        Files.deleteIfExists(root);
        Files.createFile(root);
    }

    @Test
    void aFailedDiskWriteGivesTheClaimBack() throws Exception {
        Path dataRoot = Files.createTempDirectory("stratum-warm-writefail");
        List<WarmDataset> datasets = StratumWarmStores.datasets(WINDOW_SECONDS);
        AccountingRecord record = row("acct-1");

        WarmStore warm = new WarmStore(dataRoot, datasets);
        try {
            WarmIndex idx = warm.index(DATASET);
            breakIndexRoot(idx.root());

            // The ingest path's own sequence: claim, then hand the record to the
            // asynchronous writer.
            assertTrue(warm.claim(record), "the ingest path takes the claim before the memory put");
            assertTrue(idx.pendingKeys().contains("acct-1"), "the claim must be visible while the write is in flight");
            assertTrue(warm.enqueue(record), "a warm-typed record must be accepted by the writer");

            // Deterministic drain: close() shuts the single-thread write pool down
            // and awaits its termination, so the failing task has certainly run.
            warm.close();

            assertFalse(idx.pendingKeys().contains("acct-1"),
                    "a write that failed must release the claim — a key left pending is hidden from"
                            + " the disk leg and un-evictable from memory for the life of the process");
            assertTrue(new WarmResidency(Long.MAX_VALUE, idx.pendingKeys()).evictable("acct-1", 0),
                    "and the residency cut must call it evictable again (WarmEviction still requires a"
                            + " disk copy on top of this, so the row is not dropped while only memory holds it)");
        } finally {
            warm.close();
        }
    }

    /**
     * The claim is still held for the whole of the write's flight — the fix releases
     * it on failure, it does not stop taking it. Without this, the fix could be
     * "never claim at all", which reintroduces the duplicate the claim prevents.
     */
    @Test
    void aSucceedingWriteStillClearsTheClaimOnCommit() throws Exception {
        Path dataRoot = Files.createTempDirectory("stratum-warm-writeok");
        AccountingRecord record = row("acct-2");

        WarmStore warm = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));
        try {
            WarmIndex idx = warm.index(DATASET);
            assertTrue(warm.claim(record));
            assertTrue(warm.enqueue(record));
            warm.close();

            assertFalse(idx.pendingKeys().contains("acct-2"), "a committed write clears its own claim");
            assertTrue(idx.holdsKey("acct-2"), "and the row really is on disk");
        } finally {
            warm.close();
        }
    }
}
