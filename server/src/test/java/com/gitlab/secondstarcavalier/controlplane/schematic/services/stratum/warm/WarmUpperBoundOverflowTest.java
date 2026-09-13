// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Turning the caller's INCLUSIVE epoch ceiling into the disk leg's EXCLUSIVE one
 * must not wrap.
 *
 * {@code WarmStore.diskRange} took {@code callerTo + 1}. At
 * {@code Long.MAX_VALUE} that is {@code Long.MIN_VALUE}, which then fails the
 * {@code to <= from} test and returns null — the leg prune reads "provably empty"
 * and skips the disk entirely, so a query bounded through the largest
 * representable instant comes back with none of the rows it asked for.
 *
 * There is no representable "one past {@code Long.MAX_VALUE}", so the conversion
 * clamps there instead of wrapping. The single instant at {@code Long.MAX_VALUE}
 * is the whole of what that costs.
 */
class WarmUpperBoundOverflowTest {

    @Test
    void theMaximumInclusiveBoundClampsInsteadOfWrapping() {
        assertEquals(Long.MAX_VALUE, WarmStore.exclusiveUpperBound(Long.MAX_VALUE),
                "MAX_VALUE + 1 wraps to MIN_VALUE, which diskRange reads as an empty interval"
                        + " and answers with no disk rows at all");
        assertTrue(WarmStore.exclusiveUpperBound(Long.MAX_VALUE) > 0,
                "and the wrapped value was not merely wrong, it was negative — below every real epoch");
    }

    @Test
    void everyOtherBoundIsStillExactlyOnePast() {
        assertEquals(1L, WarmStore.exclusiveUpperBound(0L));
        assertEquals(1_579_046_400_001L, WarmStore.exclusiveUpperBound(1_579_046_400_000L));
        assertEquals(Long.MAX_VALUE, WarmStore.exclusiveUpperBound(Long.MAX_VALUE - 1),
                "the largest bound that does have a successor still gets it, not the clamp");
        assertEquals(Long.MIN_VALUE + 1, WarmStore.exclusiveUpperBound(Long.MIN_VALUE));
    }
}
