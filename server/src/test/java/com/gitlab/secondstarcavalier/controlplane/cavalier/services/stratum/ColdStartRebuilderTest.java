// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ColdStartRebuilder's replay decision.
 *
 * Replay is always triggered on startup regardless of how many records
 * survived in-memory (IMap put = upsert keyed by event_id, so replay is
 * idempotent and converges a partially-surviving store to the full log).
 */
class ColdStartRebuilderTest {

    @Test
    void emptyStore_replays() {
        assertTrue(ColdStartRebuilder.shouldReplay(0L),
                "empty store must replay");
    }

    @Test
    void partialStore_replays() {
        assertTrue(ColdStartRebuilder.shouldReplay(28L),
                "partially-populated store (e.g. 28 of 49 survived) must also replay");
    }

    @Test
    void fullStore_replays() {
        assertTrue(ColdStartRebuilder.shouldReplay(40L),
                "fully-populated store must replay (idempotent upsert converges to same state)");
    }

    @Test
    void largeStore_replays() {
        assertTrue(ColdStartRebuilder.shouldReplay(100_000L),
                "large store must replay");
    }
}
