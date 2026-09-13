// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.jupiter.api.Test;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.StratumWarmStores;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.accounting.Accounting;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.accountingrecord.Accountingrecord.AccountingRecord;

/**
 * Retention must not delete a slice a query has already chosen to read.
 *
 * The window is real and not narrow: {@code candidateSlices} lists slice ids, and
 * the tasks that actually open them are submitted to a pool and run LATER. A
 * retention sweep landing in between unlinks the directory, and the read path's
 * only answer for a missing directory is "this slice holds nothing" — the rows
 * come back as an empty result with no exception, no short-read, and nothing in
 * the log. It is the same silent-incompleteness failure {@code fanOut} already
 * refuses for a slice that will not OPEN, reached instead by a slice that stopped
 * EXISTING.
 *
 * The interleaving here is forced, not raced: the query's own executor is the
 * hook. {@code fanOut} calls {@code execute} on the query thread, once per slice,
 * strictly after the candidate list is built and strictly before any slice is
 * opened, so running retention from inside the first {@code execute} puts it in
 * the gap every time. No sleep, no second thread, no timing.
 */
class WarmRetentionDuringQueryTest {

    private static final String DATASET = "accountingrecord.AccountingRecord";
    private static final long WINDOW_SECONDS = 604_800L;

    /** 2020-01-15T00:00:00Z and 2020-02-15T00:00:00Z — two different monthly slices. */
    private static final long JAN_2020 = 1_579_046_400_000L;
    private static final long FEB_2020 = 1_581_724_800_000L;
    /** Past the end of both slices, so a sweep at this cutoff is eligible to drop both. */
    private static final long CUTOFF = WarmSlices.endMsExclusive("2020-02");

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

    /** Commits one row per month into a fresh data root and returns it. */
    private static Path twoCommittedSlices() throws Exception {
        Path dataRoot = Files.createTempDirectory("stratum-warm-retention-race");
        WarmStore warm = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));
        try {
            assertTrue(warm.enqueue(row("acct-jan", JAN_2020)));
            assertTrue(warm.enqueue(row("acct-feb", FEB_2020)));
            // close() drains and awaits the single writer thread, so both commits have
            // certainly happened by the time it returns.
            warm.close();
            assertEquals(List.of("2020-01", "2020-02"),
                    WarmSlices.list(warm.index(DATASET).root()),
                    "the two rows must land in two different monthly slices for this test to mean anything");
        } finally {
            warm.close();
        }
        return dataRoot;
    }

    /**
     * A pool that runs {@code hook} on the caller's thread the first time a task is
     * handed to it — i.e. inside {@code fanOut}'s submit loop, after the candidates
     * are chosen and before any of them is read.
     */
    private static ThreadPoolExecutor hookedPool(Runnable hook) {
        AtomicBoolean fired = new AtomicBoolean();
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable command) {
                if (fired.compareAndSet(false, true)) {
                    hook.run();
                }
                super.execute(command);
            }
        };
    }

    @Test
    void retentionInsideAQueryDefersInsteadOfEmptyingTheAnswer() throws Exception {
        Path dataRoot = twoCommittedSlices();
        WarmStore warm = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));
        WarmIndex idx = warm.index(DATASET);
        List<String> droppedMidQuery = new ArrayList<>();
        ThreadPoolExecutor pool = hookedPool(() -> {
            try {
                droppedMidQuery.addAll(idx.dropSlicesEndingBefore(CUTOFF));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        try {
            long counted = idx.count(new MatchAllDocsQuery(), 0L, Long.MAX_VALUE, pool);
            assertEquals(2L, counted,
                    "retention ran between candidate selection and the slice reads; the query must still"
                            + " see both rows — a 0 or 1 here is a deleted slice reported as an empty one");
            assertEquals(List.of(), droppedMidQuery,
                    "and the sweep must have DEFERRED the pinned slices rather than dropped them");
            assertEquals(List.of("2020-01", "2020-02"), WarmSlices.list(idx.root()),
                    "the directories a pinned query needs must still be on disk");
        } finally {
            pool.shutdown();
            warm.close();
        }
    }

    /** The same interleaving on the hit-list leg — one fan-out, one rule. */
    @Test
    void theHitListLegIsProtectedToo() throws Exception {
        Path dataRoot = twoCommittedSlices();
        WarmStore warm = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));
        WarmIndex idx = warm.index(DATASET);
        ThreadPoolExecutor pool = hookedPool(() -> {
            try {
                idx.dropSlicesEndingBefore(CUTOFF);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        try {
            List<byte[]> hits = idx.search(new MatchAllDocsQuery(), 0L, Long.MAX_VALUE, 100, pool);
            assertEquals(2, hits.size(),
                    "both rows must come back; a short list here is the silent undercount");
        } finally {
            pool.shutdown();
            warm.close();
        }
    }

    /**
     * Deferral is a delay, not an exemption: with no query holding the slices, the
     * very next sweep drops them. Without this the fix could be "retention never
     * deletes anything" and the test above would still pass.
     */
    @Test
    void theNextSweepDropsWhatTheQueryDeferred() throws Exception {
        Path dataRoot = twoCommittedSlices();
        WarmStore warm = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));
        WarmIndex idx = warm.index(DATASET);
        ThreadPoolExecutor pool = hookedPool(() -> {
            try {
                idx.dropSlicesEndingBefore(CUTOFF);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        try {
            assertEquals(2L, idx.count(new MatchAllDocsQuery(), 0L, Long.MAX_VALUE, pool));
            // The query has returned, so its pins are released.
            assertEquals(List.of("2020-01", "2020-02"), idx.dropSlicesEndingBefore(CUTOFF),
                    "the deferred slices must be dropped by the following sweep");
            assertEquals(List.of(), WarmSlices.list(idx.root()));
            assertEquals(0L, idx.count(new MatchAllDocsQuery(), 0L, Long.MAX_VALUE, pool));
        } finally {
            pool.shutdown();
            warm.close();
        }
    }
}
