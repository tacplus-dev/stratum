package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Replay is for a COLD BOOT, not for every restart.
 *
 * These tests previously asserted that shouldReplay returns true for every
 * input — 0, 28, 40, 100_000 records alike — which was accurate: it returned a
 * hardcoded true and production never called it. The effect was that a routine
 * restart dragged the entire event log back through the broker (~35s for 50
 * records, linear thereafter) for data the surviving peers were already
 * holding.
 *
 * The count is read from the SHARED cluster map, which is what makes it a
 * sufficient signal on its own: a restarting member sees its peers' data
 * through it, and an empty count means nobody has the data.
 */
class ColdStartRebuilderTest {

    @Test
    void replaysWhenTheStoreIsEmpty() {
        // The only true cold boot: nobody in the cluster has the data.
        assertTrue(ColdStartRebuilder.shouldReplay(0L),
                "an empty cluster-wide store must be rebuilt from the log");
    }

    @Test
    void replaysOnAWholeClusterColdStartWhereEveryMemberIsAlreadyJoined() {
        // The regression this rule exists for. The headless Service publishes
        // not-ready addresses so members find each other before they inspect the
        // store — so on a full StatefulSet restart every member is already joined
        // to the other two while the map is still empty. A rule that also required
        // "alone in the cluster" made all three decline, and the store stayed
        // empty forever. Observed live on three pods.
        assertTrue(ColdStartRebuilder.shouldReplay(0L),
                "an empty store must replay even when every peer is already joined");
    }

    @Test
    void doesNotReplayWhenTheStoreAlreadyHasData() {
        // The common production case: one instance of several restarts and reads
        // the peers' data straight out of the shared map. Nothing to converge.
        assertFalse(ColdStartRebuilder.shouldReplay(28L),
                "a populated store needs no replay");
        assertFalse(ColdStartRebuilder.shouldReplay(100_000L),
                "a large populated store especially needs no replay");
    }
}
