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
 */
class ColdStartRebuilderTest {

    @Test
    void replaysWhenAloneWithAnEmptyStore() {
        // The only true cold boot: nobody else has the data.
        assertTrue(ColdStartRebuilder.shouldReplay(0L, 1),
                "a lone member with an empty store must rebuild from the log");
    }

    @Test
    void doesNotReplayWhenPeersArePresent() {
        // The common production case: one instance of several restarts, and
        // Hazelcast rebalances its partitions back from the peers.
        assertFalse(ColdStartRebuilder.shouldReplay(0L, 2),
                "a member that rejoins peers must not replay — the peers hold the data");
        assertFalse(ColdStartRebuilder.shouldReplay(0L, 3),
                "cluster size is what decides, not how many members there happen to be");
    }

    @Test
    void doesNotReplayWhenTheStoreAlreadyHasData() {
        // Nothing to converge, whether the data survived in-process or arrived
        // from a peer before this ran.
        assertFalse(ColdStartRebuilder.shouldReplay(28L, 1),
                "a populated store needs no replay");
        assertFalse(ColdStartRebuilder.shouldReplay(100_000L, 1),
                "a large populated store especially needs no replay");
    }
}
