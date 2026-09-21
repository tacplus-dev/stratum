// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hazelcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.StratumEventStores;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotRestore;

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

    /**
     * A single-node deployment keeps no backup on anything that has no record of its own
     * to answer to — which, since the per-record floor landed, is the restore ledger.
     *
     * <p>This used to assert 0 on the hot map too, because backupCount came from the
     * deployment number alone and {@code STRATUM_MIN_DURABLE_COPIES} is documented as an
     * OVERRIDE, not a minimum. Now a record map takes
     * {@code max(deployment, declared)}, so a record declaring 2 keeps one backup even at
     * a declared floor of 1. That is deliberate and harmless: on a one-member cluster
     * Hazelcast has nowhere to place the backup and simply does not,
     * {@code HotWrites.durableCopies()} is {@code min(members=1, backups+1=2) = 1}, and
     * {@code HotWrites.effectiveFloor} is the override 1 — so the laptop still writes.
     * The override still governs the WRITE GATE; it no longer lowers a record map's
     * configured backupCount below what the record asks for.
     */
    @Test
    void theSingleNodeFloorKeepsNoBackupOnTheLedger() {
        Config cfg = StratumMapConfig.build(List.of(), 1);

        MapConfig ledger = cfg.getMapConfigs().get(HotRestore.LEDGER_MAP);
        assertNotNull(ledger);
        assertEquals(0, ledger.getBackupCount());

        MapConfig map = hotMap(cfg);
        assertEquals(Math.max(1, StratumEventStores.minDurableCopies(StratumMapConfig.MAP_NAME)) - 1,
                map.getBackupCount(),
                "a record map holds at its own declared floor, which the override does not lower");
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

    /**
     * EVERY typed record map carries the durability settings, not just the hot map.
     *
     * <p>Only {@link StratumMapConfig#MAP_NAME} used to get them. The other record maps
     * — accountingrecord.AccountingRecord, agentsensor.AgentSessionState,
     * scotty.ScottyActivity — took Hazelcast's defaults instead: one synchronous
     * backup, which happens to agree with a floor of 2 and stops agreeing the moment
     * the floor moves, and NO split-brain protection at all, so a minority partition
     * accepted their writes locally. The "2 durable copies" claim held for one map in
     * four.
     *
     * <p>The name list comes from the GENERATED store, so a record type added or
     * .off-ed moves this assertion with it rather than leaving a map behind.
     */
    @Test
    void everyRecordMapCarriesTheDurabilityFloor() {
        Config cfg = StratumMapConfig.build(List.of(), 3);

        assertTrue(StratumEventStores.mapNames().size() > 1,
                "this build generated one record map; the uniformity claim is untested");
        for (String name : StratumEventStores.mapNames()) {
            MapConfig map = cfg.getMapConfigs().get(name);
            assertNotNull(map, name + " has no MapConfig, so it silently takes Hazelcast's defaults");
            assertEquals(2, map.getBackupCount(), name + " is not at the declared floor");
            assertEquals(0, map.getAsyncBackupCount(), name + " counts an async backup towards the floor");
            assertEquals(StratumMapConfig.SPLIT_BRAIN_PROTECTION_NAME, map.getSplitBrainProtectionName(),
                    name + " accepts writes in a minority partition");
        }
    }

    /**
     * The restore ledger is a map like any other, and carries the same settings.
     *
     * <p>It was outside the loop above — the record maps' own omission, one map
     * later. It is the map {@code HotRestore.awaitClusterReplay} decides the
     * hot-restore barrier on, so on Hazelcast's defaults a minority partition files
     * and counts its own replay reports locally and opens hot reads over a store the
     * majority never agreed was recovered. A barrier against incomplete recovery
     * cannot be a barrier while the ledger it reads is the one map a partition can
     * still write.
     */
    @Test
    void theRestoreLedgerIsProtectedLikeTheRecordMaps() {
        Config cfg = StratumMapConfig.build(List.of(), 3);

        MapConfig ledger = cfg.getMapConfigs().get(HotRestore.LEDGER_MAP);
        assertNotNull(ledger, "the restore ledger has no MapConfig, so it takes Hazelcast's defaults");
        assertEquals(2, ledger.getBackupCount(), "the ledger is not at the declared floor");
        assertEquals(0, ledger.getAsyncBackupCount(),
                "the ledger counts an async backup towards the floor");
        assertEquals(StratumMapConfig.SPLIT_BRAIN_PROTECTION_NAME, ledger.getSplitBrainProtectionName(),
                "a minority partition can file and count its own replay reports");
    }

    /**
     * A record declaring MORE than the deployment floor gets its own higher backupCount.
     *
     * <p>The deployment number is a floor, and it used to be the only input to
     * backupCount. A record declaring {@code (spec.min_durable_copies) = 3} against a
     * deployment floor of 2 therefore got one backup — {@code HotWrites.durableCopies()}
     * could reach only 2 — while {@code HotWrites.effectiveFloor} still held it to its own
     * declared 3, so EVERY write to that record was rejected permanently on a healthy
     * cluster.
     *
     * <p>Driven at a deployment floor of 1 because that is the relationship the real
     * generated fleet can express today: every record declares 2, so a floor of 1 is the
     * case where the record's own declaration is the higher of the two. Nothing here
     * depends on a specific record declaring a specific number — the expectation comes
     * from the generated lookup, so a record raising its declaration moves the assertion
     * with it.
     */
    @Test
    void aRecordDeclaringAboveTheDeploymentFloorGetsItsOwnHigherBackupCount() {
        Config cfg = StratumMapConfig.build(List.of(), 1);

        boolean sawARaisedMap = false;
        for (String name : StratumEventStores.mapNames()) {
            int declared = StratumEventStores.minDurableCopies(name);
            MapConfig map = cfg.getMapConfigs().get(name);
            assertNotNull(map, name + " has no MapConfig");
            assertEquals(Math.max(1, declared) - 1, map.getBackupCount(),
                    name + " declares min_durable_copies=" + declared
                            + " but its map was built from the deployment floor alone");
            sawARaisedMap |= declared > 1;
        }
        assertTrue(sawARaisedMap,
                "no generated record declares above a floor of 1; the raise is untested");
    }

    /** A record at or below the deployment floor is untouched — the floor stays a floor. */
    @Test
    void aRecordAtOrBelowTheDeploymentFloorIsUnaffected() {
        Config cfg = StratumMapConfig.build(List.of(), 3);

        for (String name : StratumEventStores.mapNames()) {
            assertTrue(StratumEventStores.minDurableCopies(name) <= 3,
                    name + " declares above 3; this test no longer covers the at-or-below case");
            MapConfig map = cfg.getMapConfigs().get(name);
            assertNotNull(map, name + " has no MapConfig");
            assertEquals(2, map.getBackupCount(),
                    name + " was lowered below the deployment floor by its own declaration");
        }
    }

    /**
     * The restore ledger stays on the plain deployment floor.
     *
     * <p>It is not a record map: it has no {@code (spec.min_durable_copies)} declaration
     * and nothing in the generated lookup, so the per-record raise must not reach it.
     * Driven at a floor of 1, where every record map IS raised — if the ledger were
     * inside the raise it would show the records' 2 rather than the deployment's 1.
     */
    @Test
    void theRestoreLedgerAlwaysUsesThePlainDeploymentFloor() {
        Config cfg = StratumMapConfig.build(List.of(), 1);

        assertEquals(0, StratumEventStores.minDurableCopies(HotRestore.LEDGER_MAP),
                "the ledger is not a generated record map and must not be in the lookup");
        MapConfig ledger = cfg.getMapConfigs().get(HotRestore.LEDGER_MAP);
        assertNotNull(ledger, "the restore ledger has no MapConfig");
        assertEquals(0, ledger.getBackupCount(),
                "the ledger took a record's per-record floor, which it does not have");
    }

    /** The documented default, so an undeclared record is not shipped unprotected. */
    @Test
    void theDefaultFloorIsTwo() {
        assertEquals(2, StratumMapConfig.DEFAULT_MIN_DURABLE_COPIES);
    }

    /**
     * Every record map carries the memory clock its record declares.
     *
     * <p>{@code setMaxIdleSeconds} was never called, so every record map took
     * Hazelcast's default of 0 — no max-idle, nothing ever leaves memory. That is
     * the one thing the record lifecycle says does not exist: a row untouched for
     * {@code (spec.record).residency.idle_seconds} is evicted from memory and
     * lives on disk, and the next touch reloads it. With the call absent the
     * declaration was inert and the store grew until it was restarted.
     *
     * <p>The expectation comes from the GENERATED lookup rather than a number
     * written here, for the same reason the backupCount assertions do: a record
     * changing its declaration moves this assertion with it instead of leaving a
     * map behind. What is pinned here is that the wiring exists and reaches every
     * map — the codegen side (EventStoreResidencyTest) pins that a DECLARED
     * non-default value is what the lookup answers with.
     */
    @Test
    void everyRecordMapCarriesItsDeclaredIdleClock() {
        Config cfg = StratumMapConfig.build(List.of(), 2);

        for (String name : StratumEventStores.mapNames()) {
            MapConfig map = cfg.getMapConfigs().get(name);
            assertNotNull(map, name + " has no MapConfig");
            int declared = StratumEventStores.idleSeconds(name);
            assertTrue(declared > 0,
                    name + " resolves to no max-idle at all; 0/unset on the annotation means"
                            + " the platform default, never \"never\"");
            // HOTFIX 2026-09-21: the declared clock is deliberately NOT applied until a
            // read-through disk leg exists under the record maps — with none, idle
            // eviction plus the checkpoint's redo truncation was permanent data loss
            // (decisions/stratum-idle-eviction-durability-gap-2026-09-20.md). Flip this
            // back to assertEquals(declared, ...) with the StratumMapConfig line.
            assertEquals(0, map.getMaxIdleSeconds(),
                    name + " has a max-idle applied while no disk leg exists to reload an"
                            + " evicted row from; that is data loss, not cache maintenance");
        }
    }

    /**
     * The restore ledger has no max-idle.
     *
     * <p>It is not a record map and has no lifecycle to age out: it holds the
     * replay reports {@code HotRestore.awaitClusterReplay} counts, and a barrier
     * whose ledger expires out from under it is not a barrier. The generated
     * lookup answers 0 for a name it does not know, which is Hazelcast's own
     * "no max-idle" — so the ledger has to stay outside the lookup, the same way
     * it stays outside the per-record durability raise.
     */
    @Test
    void theRestoreLedgerHasNoMaxIdle() {
        Config cfg = StratumMapConfig.build(List.of(), 2);

        assertEquals(0, StratumEventStores.idleSeconds(HotRestore.LEDGER_MAP),
                "the ledger is not a generated record map and must not be in the lookup");
        MapConfig ledger = cfg.getMapConfigs().get(HotRestore.LEDGER_MAP);
        assertNotNull(ledger, "the restore ledger has no MapConfig");
        assertEquals(0, ledger.getMaxIdleSeconds(),
                "the hot-restore barrier counts reports the ledger would have expired");
    }
}
