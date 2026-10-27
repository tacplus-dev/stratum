// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.tacplusdev.stratum.model.s2sm.Handler;
import com.github.tacplusdev.stratum.model.types.services.letsencrypt.Letsencrypt;

/**
 * The cold-start rebuild has to STOP once it has caught up.
 *
 * It subscribes in its own consumer group ("stratum-rebuild") from offset 0, and
 * the generated subscriber it uses never returns on its own — it reconnects and
 * keeps delivering. Past the watermark every record it delivers is a LIVE event
 * that the service's normal "stratum" subscription is already storing, so the
 * rebuild becomes a permanent SECOND writer: every event stored twice, on every
 * replica, for the life of the process. The upserts are idempotent, which is
 * exactly why nothing notices — it is duplicated work and duplicated broker
 * traffic, not duplicated rows.
 *
 * These tests drive the per-record step directly, so the boundary is exact rather
 * than timed.
 */
class ColdStartRebuildStopsAtCatchUpTest {

    private static final String TOPIC = "letsencrypt.LetsEncryptCertBus.stream";

    /**
     * The real handler with its one ingest method recorded instead of stored — so
     * the test asserts on exactly what the rebuild asked the store to write.
     */
    private static final class RecordingHandler extends StratumHandler {
        final List<Long> stored = new ArrayList<>();

        RecordingHandler() {
            super(null);
        }

        @Override
        public void handleEventStreamMessage(String topic, com.google.protobuf.Message event,
                SubscriptionMeta meta) {
            stored.add(meta.offset);
        }
    }

    private static Handler.SubscriptionMeta meta(long offset) {
        return new Handler.SubscriptionMeta(offset, 0L, new byte[0], TOPIC, 0);
    }

    private static void deliver(ColdStartRebuilder rebuilder, Handler handler, long offset,
            Runnable onCaughtUp) throws Exception {
        rebuilder.replayRecord(TOPIC, handler, Letsencrypt.LetsEncryptCert.getDefaultInstance(),
                meta(offset), onCaughtUp);
    }

    @Test
    void theRebuildEndsOnTheLastBackloggedRecordAndNeverSeesALiveOne() throws Exception {
        ColdStartRebuilder rebuilder = new ColdStartRebuilder();
        RecordingHandler handler = new RecordingHandler();
        AtomicInteger gateOpened = new AtomicInteger();

        // The log ended at offset 3 when the replay started, so offsets 0..2 are the
        // backlog and offset 2 is the last record of it.
        rebuilder.progress.expect(TOPIC, 0, 3L);

        deliver(rebuilder, handler, 0, gateOpened::incrementAndGet);
        deliver(rebuilder, handler, 1, gateOpened::incrementAndGet);
        assertEquals(0, gateOpened.get(), "the read gate must stay shut while the backlog remains");

        // The last backlogged record is still stored — and is the one that ends the
        // subscription. InterruptedException is the generated subscriber's stop
        // signal; raising it from the handler cancels the call and ends the replay
        // thread.
        assertThrows(InterruptedException.class, () -> deliver(rebuilder, handler, 2, gateOpened::incrementAndGet));

        assertEquals(List.of(0L, 1L, 2L), handler.stored,
                "every backlogged record must be stored, including the one that ends the replay");
        assertEquals(1, gateOpened.get(), "catching up must open the read gate exactly once");
        assertEquals(3L, rebuilder.restoredCount(), "and all three must be counted");
    }

    /**
     * The bug itself: before the fix, a record delivered past the watermark was
     * handed straight to the store — a second write of an event the live
     * subscription had already stored. Now the replay is over before it can be.
     */
    @Test
    void aLiveEventDeliveredAfterCatchUpIsNotStoredASecondTime() throws Exception {
        ColdStartRebuilder rebuilder = new ColdStartRebuilder();
        RecordingHandler handler = new RecordingHandler();

        rebuilder.progress.expect(TOPIC, 0, 1L);
        assertThrows(InterruptedException.class, () -> deliver(rebuilder, handler, 0, () -> { }));
        assertEquals(List.of(0L), handler.stored);

        // Offset 1 is the first LIVE event. Whatever the subscriber does with it,
        // the rebuild refuses to write it.
        assertThrows(InterruptedException.class, () -> deliver(rebuilder, handler, 1, () -> { }));
        assertEquals(List.of(0L), handler.stored,
                "a live event must have exactly one writer — the normal subscription, not the rebuild");
    }

    /** An empty topic is caught up before the first record, so there is nothing to subscribe for. */
    @Test
    void anEmptyLogIsCaughtUpWithNoBacklogRegistered() {
        ColdStartRebuilder rebuilder = new ColdStartRebuilder();
        rebuilder.progress.expect(TOPIC, 0, 0L);
        assertTrue(rebuilder.progress.isCaughtUp(),
                "a zero watermark is already drained — startTopicReplay must skip the subscribe entirely");
    }
}
