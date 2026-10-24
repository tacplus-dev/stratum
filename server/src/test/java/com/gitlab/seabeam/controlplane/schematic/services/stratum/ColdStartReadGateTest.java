// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;

import org.junit.jupiter.api.Test;

/**
 * The read gate must not open until BOTH recovery paths have finished.
 *
 * <p>The two paths recover disjoint sets of record types. The redo barrier covers
 * the types that carry a redo log; the types that do not are recovered by
 * {@code ColdStartRebuilder} replaying the event log, which runs asynchronously
 * and is still going long after the redo barrier clears. Opening on the redo
 * barrier alone answers queries for those types out of a store that is still
 * filling, and an empty result is indistinguishable to a client from "there is no
 * such data" — the same silent incompleteness the redo barrier exists to refuse.
 *
 * <p>The paired risk is the opposite one: a gate that waits on a rebuild which was
 * never going to run never opens at all. Both directions are asserted here.
 */
class ColdStartReadGateTest {

    /**
     * Sets the redo leg without standing up a Hazelcast cluster and a redo log on
     * disk. {@code EventStore.init} takes its durability floor from the process
     * environment and then blocks until that many members have joined, so driving
     * the real path would need a multi-member cluster to assert a flag that the
     * restore path sets on one line. The cold-start leg is set through its real
     * public entry point; only this one is reached behind the curtain.
     */
    private static void markRedoReplayDone(EventStore store) throws Exception {
        Field f = EventStore.class.getDeclaredField("redoReplayDone");
        f.setAccessible(true);
        f.setBoolean(store, true);
    }

    /** The bug: redo recovery finished, event-log replay still running. */
    @Test
    void theGateStaysClosedWhileColdStartReplayIsStillRunning() throws Exception {
        EventStore store = new EventStore();
        markRedoReplayDone(store);

        assertFalse(store.hotReadable(),
                "redo replay alone must not open reads: the record types with no redo log are"
                        + " still being rebuilt from the event log and would answer as empty");
    }

    /** The gate opens the moment the second leg reports in, and not before. */
    @Test
    void theGateOpensOnceColdStartReplayCatchesUp() throws Exception {
        EventStore store = new EventStore();
        markRedoReplayDone(store);
        assertFalse(store.hotReadable());

        store.coldStartReplayComplete();

        assertTrue(store.hotReadable(), "both legs reported; reads must open");
    }

    /** Neither leg is sufficient on its own — the mirror of the test above. */
    @Test
    void coldStartReplayAloneDoesNotOpenTheGate() {
        EventStore store = new EventStore();

        store.coldStartReplayComplete();

        assertFalse(store.hotReadable(),
                "the event log does not cover the types the redo log recovers; reads must wait");
    }

    /**
     * The no-hang case. A deployment whose topic holds nothing has no record
     * arriving to mark progress, so the drained backlog has to be recognised from
     * the watermark itself — otherwise the gate waits forever on a replay that was
     * never going to deliver anything.
     */
    @Test
    void anEmptyTopicIsAlreadyCaughtUp() {
        ColdStartRebuilder.ReplayProgress progress = new ColdStartRebuilder.ReplayProgress();

        progress.expect("letsencrypt.LetsEncryptCertBus.stream", 0, 0);

        assertTrue(progress.isCaughtUp(),
                "a partition whose log ends at offset 0 holds nothing to replay");
    }

    /** Nothing registered at all — a topic eventbus has never heard of. */
    @Test
    void anAbsentTopicIsAlreadyCaughtUp() {
        assertTrue(new ColdStartRebuilder.ReplayProgress().isCaughtUp(),
                "a topic that does not exist has no backlog and must not hold the gate shut");
    }

    /**
     * The backlog is drained at the watermark and not one record earlier. The log
     * ends at offset 5, so the last record written sits at offset 4; delivering
     * offset 3 leaves a record unreplayed and must not open the gate.
     */
    @Test
    void aPartialReplayIsNotCaughtUp() {
        ColdStartRebuilder.ReplayProgress progress = new ColdStartRebuilder.ReplayProgress();
        progress.expect("letsencrypt.LetsEncryptCertBus.stream", 0, 5);

        for (long offset = 0; offset <= 3; offset++) {
            progress.observed("letsencrypt.LetsEncryptCertBus.stream", 0, offset);
            assertFalse(progress.isCaughtUp(),
                    "offset " + offset + " of a log ending at 5 is still short of the end");
        }

        progress.observed("letsencrypt.LetsEncryptCertBus.stream", 0, 4);

        assertTrue(progress.isCaughtUp(), "offset 4 is the last record of a log ending at 5");
    }

    /** Every partition has to drain, not just the one that happens to finish first. */
    @Test
    void everyPartitionMustDrain() {
        ColdStartRebuilder.ReplayProgress progress = new ColdStartRebuilder.ReplayProgress();
        progress.expect("letsencrypt.LetsEncryptCertBus.stream", 0, 2);
        progress.expect("letsencrypt.LetsEncryptCertBus.stream", 1, 3);

        progress.observed("letsencrypt.LetsEncryptCertBus.stream", 0, 1);
        assertFalse(progress.isCaughtUp(), "partition 1 has not been replayed at all");

        progress.observed("letsencrypt.LetsEncryptCertBus.stream", 1, 2);
        assertTrue(progress.isCaughtUp());
    }
}
