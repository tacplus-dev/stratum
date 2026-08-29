// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;

import org.junit.jupiter.api.Test;

/**
 * The hot map's durability settings must be STATED, not inherited.
 *
 * Before this, {@code setBackupCount} was never called, so the map took
 * Hazelcast's default of one synchronous backup — the "two instances" half of the
 * durability story was accidentally true and written down nowhere, and any change
 * to that default would have moved it silently. Nothing anywhere in the platform
 * configured split-brain protection either, so a minority partition accepted
 * writes locally, which is the opposite of an atomic store.
 */
class StratumMapConfigDurabilityTest {

    private static MapConfig hotMap(Config cfg) {
        MapConfig map = cfg.getMapConfigs().get(StratumMapConfig.MAP_NAME);
        assertNotNull(map, "the hot map config must be declared by name, not resolved by wildcard");
        return map;
    }

    @Test
    void backupsAreSynchronousAndSizedToTheFloor() {
        MapConfig map = hotMap(StratumMapConfig.build(List.of(), 2));

        assertEquals(1, map.getBackupCount(), "min_durable_copies=2 means the owner plus one backup");
        assertEquals(0, map.getAsyncBackupCount(),
                "an async backup is not held when the write returns, so counting one would ack"
                        + " a durability that does not exist yet");
    }

    @Test
    void aHigherFloorRaisesTheBackupCount() {
        MapConfig map = hotMap(StratumMapConfig.build(List.of(), 3));
        assertEquals(2, map.getBackupCount());
        assertEquals(0, map.getAsyncBackupCount());
    }

    @Test
    void theSingleNodeFloorKeepsNoBackup() {
        MapConfig map = hotMap(StratumMapConfig.build(List.of(), 1));
        assertEquals(0, map.getBackupCount());
    }

    @Test
    void theHotMapIsUnderSplitBrainProtectionAtTheFloor() {
        Config cfg = StratumMapConfig.build(List.of(), 2);
        MapConfig map = hotMap(cfg);

        assertEquals(StratumMapConfig.SPLIT_BRAIN_PROTECTION_NAME, map.getSplitBrainProtectionName(),
                "the map must reference the protection, or configuring it protects nothing");
        SplitBrainProtectionConfig protection =
                cfg.getSplitBrainProtectionConfig(StratumMapConfig.SPLIT_BRAIN_PROTECTION_NAME);
        assertNotNull(protection);
        assertTrue(protection.isEnabled());
        assertEquals(2, protection.getMinimumClusterSize(),
                "a minority partition that keeps accepting writes is how a store stops being atomic");
    }

    /** The documented default, so an undeclared record is not shipped unprotected. */
    @Test
    void theDefaultFloorIsTwo() {
        assertEquals(2, StratumMapConfig.DEFAULT_MIN_DURABLE_COPIES);
    }
}
