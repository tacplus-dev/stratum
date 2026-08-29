// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Slice pruning is STRUCTURAL, and this pins the arithmetic it rests on.
 *
 * The claim the layout makes is that a slice can be excluded from a query by
 * parsing its directory name — no manifest, no reader, no file. If the interval a
 * name implies were off by so much as a millisecond at either edge, the prune
 * would silently drop rows that were on disk the whole time, and every other test
 * here would still pass because they never query across a month boundary.
 */
class WarmSlicesTest {

    private static long ms(String isoInstant) {
        return Instant.parse(isoInstant).toEpochMilli();
    }

    @Test
    void sliceIdIsTheUtcCalendarMonthOfTheStoreEpoch() {
        assertEquals("2026-08", WarmSlices.sliceIdFor(ms("2026-08-29T18:00:00Z")));
        assertEquals("2026-08", WarmSlices.sliceIdFor(ms("2026-08-01T00:00:00Z")));
        assertEquals("2026-09", WarmSlices.sliceIdFor(ms("2026-09-01T00:00:00Z")));
    }

    @Test
    void theIntervalIsHalfOpenAtBothEnds() {
        assertEquals(ms("2026-08-01T00:00:00Z"), WarmSlices.startMs("2026-08"));
        assertEquals(ms("2026-09-01T00:00:00Z"), WarmSlices.endMsExclusive("2026-08"));
    }

    @Test
    void aSliceTouchingTheQueryOnlyAtTheExclusiveEndDoesNotIntersect() {
        long augStart = ms("2026-08-01T00:00:00Z");
        long sepStart = ms("2026-09-01T00:00:00Z");
        // [sepStart, ...) starts exactly where 2026-08 ends: no overlap.
        assertFalse(WarmSlices.intersects("2026-08", sepStart, sepStart + 1));
        // [augStart-1, augStart) ends exactly where 2026-08 begins: no overlap.
        assertFalse(WarmSlices.intersects("2026-08", augStart - 1, augStart));
        // One millisecond inside, either edge: overlap.
        assertTrue(WarmSlices.intersects("2026-08", sepStart - 1, sepStart));
        assertTrue(WarmSlices.intersects("2026-08", augStart, augStart + 1));
    }

    @Test
    void onlyDirectoriesNamedLikeSlicesAreSlices(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("2026-07"));
        Files.createDirectories(dir.resolve("2026-08"));
        Files.createDirectories(dir.resolve("not-a-slice"));
        Files.writeString(dir.resolve("2026-06"), "a file, not a directory");

        assertEquals(List.of("2026-07", "2026-08"), WarmSlices.list(dir));
    }

    @Test
    void listIsChronologicalBecauseTheNameSortsThatWay(@TempDir Path dir) throws IOException {
        for (String s : List.of("2026-10", "2025-02", "2026-02", "2025-12")) {
            Files.createDirectories(dir.resolve(s));
        }
        assertEquals(List.of("2025-02", "2025-12", "2026-02", "2026-10"), WarmSlices.list(dir));
    }
}
