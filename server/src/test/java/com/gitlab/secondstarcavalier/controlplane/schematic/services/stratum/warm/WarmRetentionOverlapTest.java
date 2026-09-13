// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.StratumWarmStores;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast.StratumEpochExtractor;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.services.stratum.Helpers;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.services.stratum.Stratum;

/**
 * The two windows shipped in Stratum.Config have to OVERLAP, and this is the test
 * that says so.
 *
 * warm_window_seconds is when a row stops being served from memory and starts
 * being served from disk; retention_days is when it is deleted outright. With the
 * window LONGER than retention — a 365-day window against 90-day retention, which
 * is what shipped before warm_window_seconds existed — retention deletes every row
 * before it is ever old enough to age out of memory, and the disk tier the whole
 * warm leg exists to serve is dead code in production. The failure is silent: the
 * disk leg answers correctly, with nothing, forever.
 *
 * So the property under test is not "the numbers are 604800 and 90". It is that
 * SOME age is on the disk side of the residency boundary and still inside
 * retention — i.e. that the disk tier has rows to serve at all.
 */
class WarmRetentionOverlapTest {

    /** Stratum.Config as the service actually receives it: proto defaults applied. */
    private static Stratum.Config defaults() {
        return Helpers.configDefaults(Stratum.Config.getDefaultInstance()).build();
    }

    /** The retention cutoff EventStore.runRetentionCleanup computes, for these defaults. */
    private static long retentionCutoffMs(Stratum.Config cfg, long nowMs) {
        return nowMs - (long) cfg.getRetentionDays() * StratumEpochExtractor.MILLIS_PER_DAY;
    }

    /**
     * A row aged halfway between the two windows: past the residency boundary, so
     * the disk leg owns it, and inside retention, so it still exists.
     */
    @Test
    void someAgeIsDiskServedAndStillRetained() {
        Stratum.Config cfg = defaults();
        long nowMs = System.currentTimeMillis();
        long boundaryMs = nowMs - cfg.getWarmWindowSeconds() * 1000L;
        long cutoffMs = retentionCutoffMs(cfg, nowMs);

        assertTrue(cutoffMs < boundaryMs,
                "retention (" + cfg.getRetentionDays() + "d) must outlast the residency window ("
                        + cfg.getWarmWindowSeconds() + "s) or no row ever reaches the disk tier");

        // Halfway through the overlap — nothing about the test depends on the exact
        // numbers, only on the interval being non-empty.
        long epochMs = (cutoffMs + boundaryMs) / 2;
        assertTrue(epochMs < boundaryMs, "the sample row must be past the residency boundary");
        assertTrue(epochMs >= cutoffMs, "the sample row must still be inside retention");
    }

    /**
     * The same overlap, read through the objects that actually make the call: the
     * GENERATED warm descriptors at the configured default window, and the residency
     * cut the read path applies.
     */
    @Test
    void theGeneratedDescriptorsPutThatRowOnTheDiskLeg() {
        Stratum.Config cfg = defaults();
        long nowMs = System.currentTimeMillis();
        long cutoffMs = retentionCutoffMs(cfg, nowMs);

        List<WarmDataset> datasets = StratumWarmStores.datasets(cfg.getWarmWindowSeconds());
        assertFalse(datasets.isEmpty(),
                "no RESIDENCY_WARM record type — the overlap is untestable against real descriptors");

        for (WarmDataset ds : datasets) {
            long boundaryMs = ds.boundaryMs(nowMs);
            assertTrue(cutoffMs < boundaryMs, ds.name
                    + ": retention deletes rows before they age onto the disk leg");

            long epochMs = (cutoffMs + boundaryMs) / 2;
            WarmResidency residency = new WarmResidency(boundaryMs, java.util.Set.of());
            assertFalse(residency.inMemory("k", epochMs),
                    ds.name + ": a row inside retention but past the boundary must leave memory");
            assertTrue(residency.evictable("k", epochMs),
                    ds.name + ": that same row must be evictable, i.e. the disk leg answers for it");
        }
    }
}
