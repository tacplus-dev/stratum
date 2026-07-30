// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ColdStartRebuilder's cold-vs-warm decision.
 * Tests the pure decision logic (totalCount == 0 → cold, > 0 → warm)
 * without requiring Hazelcast or eventbus.
 */
class ColdStartRebuilderTest {

    @Test
    void coldStart_zeroRecords_returnsTrue() {
        assertTrue(ColdStartRebuilder.isColdStart(0L),
                "empty store (totalCount==0) must be detected as cold start");
    }

    @Test
    void warmStart_manyRecords_returnsFalse() {
        assertFalse(ColdStartRebuilder.isColdStart(40L),
                "populated store (totalCount>0) must NOT be detected as cold start");
    }

    @Test
    void warmStart_singleRecord_returnsFalse() {
        assertFalse(ColdStartRebuilder.isColdStart(1L),
                "store with even one event should be treated as warm start");
    }

    @Test
    void warmStart_largeCount_returnsFalse() {
        assertFalse(ColdStartRebuilder.isColdStart(100_000L),
                "large store is a warm start");
    }
}
