// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast;

/**
 * StratumEpochExtractor provides constants for the timestampMs-based
 * epoch attribute used in retention cleanup calculations.
 */
public final class StratumEpochExtractor {

    /** Milliseconds per day, used in retention cutoff calculations. */
    public static final long MILLIS_PER_DAY = 86_400_000L;

    private StratumEpochExtractor() {}
}
