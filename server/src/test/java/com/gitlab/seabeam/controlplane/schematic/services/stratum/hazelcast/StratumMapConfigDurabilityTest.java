// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.StratumEventStores;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.disk.RecordDiskLoader;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.hot.HotRestore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
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
    void backupsAreSynchronousAndAtTheConfiguredMemoryRedundancy() {
        MapConfig map = hotMap(StratumMapConfig.build(List.of(), 2));

        assertEquals(StratumMapConfig.DEFAULT_BACKUP_COUNT, map.getBackupCount(),
                "backupCount is memory redundancy and comes from STRATUM_BACKUP_COUNT, not from"
                        + " min_durable_copies");
        assertEquals(0, map.getAsyncBackupCount(),
                "an async backup is not held when the write returns, so a member's death inside"
                        + " that window would cost a row the cluster believed it had twice");
    }

    /**
     * The decoupling, asserted as the thing it is: the durability floor moves and the
     * memory redundancy does not.
     *
     * <p>It used to be {@code floor - 1}, because the durable copies WERE the partition
     * replicas — the redo append rode the replication onto the owner and its
     * synchronous backups. A writer now appends every write cluster-wide whichever
     * member owns the partition, so durability is the writer count and this number
     * answers a different question. Tying them together is also what made search
     * members impossible: a partition whose replicas all landed on volume-less members
     * would have been acked with no durable copy at all.
     */
    @Test
    void aHigherFloorDoesNotRaiseTheBackupCount() {
        for (int floor : new int[] {1, 2, 3}) {
            MapConfig map = hotMap(StratumMapConfig.build(List.of(), floor));
            assertEquals(StratumMapConfig.DEFAULT_BACKUP_COUNT, map.getBackupCount(),
                    "min_durable_copies=" + floor + " moved the memory redundancy");
            assertEquals(0, map.getAsyncBackupCount());
        }
    }

    /** The documented default memory redundancy, so the split is a stated number. */
    @Test
    void theDefaultBackupCountIsOne() {
        assertEquals(1, StratumMapConfig.DEFAULT_BACKUP_COUNT);
    }

    /**
     * A single-node deployment keeps the same memory redundancy as any other, and pays
     * nothing for it: Hazelcast places a backup only where there is a member to place
     * it on. The override governs the WRITE GATE
     * ({@code HotWrites.effectiveFloor}), which is what lets a laptop write at all.
     */
    @Test
    void theSingleNodeFloorLeavesTheMemoryRedundancyAlone() {
        Config cfg = StratumMapConfig.build(List.of(), 1);

        MapConfig ledger = cfg.getMapConfigs().get(HotRestore.LEDGER_MAP);
        assertNotNull(ledger);
        assertEquals(MapConfig.MAX_BACKUP_COUNT, ledger.getBackupCount());

        assertEquals(StratumMapConfig.DEFAULT_BACKUP_COUNT, hotMap(cfg).getBackupCount(),
                "a deliberately single-instance deployment is not a statement about memory"
                        + " redundancy in either direction");
    }

    /**
     * The protection counts WRITERS.
     *
     * <p>Hazelcast's own {@code minimumClusterSize} counts everything in the view, and
     * with search members in the cluster that stopped answering the question the
     * protection exists to ask: one writer and five search members satisfy "six members
     * are here" while holding one durable copy, so a minority partition of that shape
     * would go on accepting writes at the cluster level. The number is set as well as
     * the function, because {@code minimumClusterSize} defaults to 2 on its own and a
     * config that reads as the floor by accident is worse than one that does not.
     */
    @Test
    void theHotMapIsUnderWriterCountedSplitBrainProtection() {
        Config cfg = StratumMapConfig.build(List.of(), 3);
        MapConfig map = hotMap(cfg);

        assertEquals(StratumMapConfig.SPLIT_BRAIN_PROTECTION_NAME, map.getSplitBrainProtectionName(),
                "the map must reference the protection, or configuring it protects nothing");
        SplitBrainProtectionConfig protection =
                cfg.getSplitBrainProtectionConfig(StratumMapConfig.SPLIT_BRAIN_PROTECTION_NAME);
        assertNotNull(protection);
        assertTrue(protection.isEnabled());
        assertEquals(3, protection.getMinimumClusterSize(),
                "the stated number must be the floor, not Hazelcast's default of 2");
        assertTrue(protection.getFunctionImplementation() instanceof WriterCountProtection,
                "the protection counts members, so search members talk a minority partition"
                        + " up to the floor");
        assertEquals(3, ((WriterCountProtection) protection.getFunctionImplementation()).minWriters());
    }

    /**
     * EVERY typed record map carries the replication settings, not just the hot map.
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
    void everyRecordMapCarriesTheReplicationSettings() {
        Config cfg = StratumMapConfig.build(List.of(), 3);

        assertTrue(StratumEventStores.mapNames().size() > 1,
                "this build generated one record map; the uniformity claim is untested");
        for (String name : StratumEventStores.mapNames()) {
            MapConfig map = cfg.getMapConfigs().get(name);
            assertNotNull(map, name + " has no MapConfig, so it silently takes Hazelcast's defaults");
            assertEquals(StratumMapConfig.DEFAULT_BACKUP_COUNT, map.getBackupCount(),
                    name + " is not at the deployment's memory redundancy");
            assertEquals(0, map.getAsyncBackupCount(), name + " counts an async backup towards the floor");
            assertEquals(StratumMapConfig.SPLIT_BRAIN_PROTECTION_NAME, map.getSplitBrainProtectionName(),
                    name + " accepts writes in a minority partition");
        }
    }

    /**
     * The two maps the barrier is decided on carry the record maps' split-brain
     * protection, and MORE replication than the record maps have.
     *
     * <p>The protection is why they stopped taking Hazelcast's defaults: they were
     * outside the loop above — the record maps' own omission, one map later — and on
     * the defaults a minority partition files and counts its own replay reports
     * locally and opens hot reads over a store the majority never agreed was
     * recovered. A barrier against incomplete recovery cannot be a barrier while the
     * ledger it reads is the one map a partition can still write.
     *
     * <p>The replication is the 2026-09-22 fix. These maps are not records and their
     * backup count is not a durability statement; it is how many members have to die
     * at once to destroy the barrier's own knowledge of who has recovered. At the
     * record maps' number that was two of three, and a destroyed report is never
     * re-filed by the serving member it belonged to — see
     * {@code HotRestoreLostReportDeadlockTest} and
     * decisions/2026-09-22-stratum-mutual-restart-deadlock.md.
     */
    @Test
    void theBarriersOwnMapsAreProtectedAndFullyReplicated() {
        Config cfg = StratumMapConfig.build(List.of(), 3);

        for (String name : List.of(HotRestore.LEDGER_MAP, HotRestore.INFLIGHT_MAP)) {
            MapConfig map = cfg.getMapConfigs().get(name);
            assertNotNull(map, name + " has no MapConfig, so it takes Hazelcast's defaults");
            assertEquals(MapConfig.MAX_BACKUP_COUNT, map.getBackupCount(),
                    name + " is replicated to fewer members than are alive to hold it, so the"
                            + " members whose loss the barrier exists to survive can destroy it");
            assertEquals(0, map.getAsyncBackupCount(),
                    name + " counts an async backup, which is not held when the write returns");
            assertEquals(StratumMapConfig.SPLIT_BRAIN_PROTECTION_NAME, map.getSplitBrainProtectionName(),
                    "a minority partition can file and count its own replay reports in " + name);
        }
    }

    /**
     * A record's own {@code (spec.min_durable_copies)} no longer touches its map's
     * replication, above or below the deployment floor.
     *
     * <p>It used to be the input that raised it: the durable copies were the partition
     * replicas, so a record declaring 3 against a deployment floor of 2 needed a third
     * replica or {@code HotWrites.durableCopies()} could never reach its declared floor
     * and every write to it was refused permanently on a healthy cluster. A record
     * declaring more is now declaring it needs more WRITER MEMBERS, which is what the
     * write gate counts and what {@code HotWritesWriterCountTest} asserts — and the
     * replica count is left to say the one thing it was always read as saying.
     *
     * <p>Driven across the floors the generated fleet can express, so a record raising
     * or lowering its declaration cannot move the assertion.
     */
    @Test
    void aRecordsOwnDeclarationDoesNotTouchItsMapsReplication() {
        boolean sawARecordAboveAFloor = false;
        for (int floor : new int[] {1, 2, 3}) {
            Config cfg = StratumMapConfig.build(List.of(), floor);
            for (String name : StratumEventStores.mapNames()) {
                int declared = StratumEventStores.minDurableCopies(name);
                MapConfig map = cfg.getMapConfigs().get(name);
                assertNotNull(map, name + " has no MapConfig");
                assertEquals(StratumMapConfig.DEFAULT_BACKUP_COUNT, map.getBackupCount(),
                        name + " declares min_durable_copies=" + declared + " and at a deployment"
                                + " floor of " + floor + " that moved its replica count");
                sawARecordAboveAFloor |= declared > floor;
            }
        }
        assertTrue(sawARecordAboveAFloor,
                "no generated record declares above any floor tested; the decoupling is untested");
    }

    /**
     * The restore ledger is outside the record maps' replication.
     *
     * <p>It is not a record map: it has no {@code (spec.min_durable_copies)} declaration
     * and nothing in the generated lookup. Its backup count is decided on its own
     * grounds — it is the barrier's knowledge of who has recovered, not a statement
     * about rows — and {@code theBarriersOwnMapsAreProtectedAndFullyReplicated} is where
     * that is asserted. What is asserted here is only that nothing about a record
     * decides it.
     */
    @Test
    void theRestoreLedgerIsOutsideTheRecordMapsReplication() {
        Config cfg = StratumMapConfig.build(List.of(), 1);

        assertEquals(0, StratumEventStores.minDurableCopies(HotRestore.LEDGER_MAP),
                "the ledger is not a generated record map and must not be in the lookup");
        MapConfig ledger = cfg.getMapConfigs().get(HotRestore.LEDGER_MAP);
        assertNotNull(ledger, "the restore ledger has no MapConfig");
        assertEquals(MapConfig.MAX_BACKUP_COUNT, ledger.getBackupCount(),
                "the ledger took a number from a record, which decides nothing about it");
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
     *
     * <p>It also pins that the clock and the read-through leg are applied
     * together. See the comment in the body for why that is one test and not two.
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
            assertEquals(declared, map.getMaxIdleSeconds(),
                    name + " does not carry the memory clock its record declares; the"
                            + " declaration is then inert and the map grows until the pod"
                            + " is restarted");

            // And the clock never travels alone. It was hotfixed off on 2026-09-21
            // precisely because it did: with no read-through leg an evicted row was
            // absent from the next checkpoint dump and its redo entries were
            // truncated behind it, so eviction was permanent loss
            // (decisions/stratum-idle-eviction-durability-gap-2026-09-20.md). A map
            // that carries the clock and no loader is that state exactly, so the
            // two are asserted together rather than in two tests that could pass
            // one at a time.
            MapStoreConfig store = map.getMapStoreConfig();
            assertNotNull(store, name + " carries a max-idle clock and no map store config");
            assertTrue(store.isEnabled(),
                    name + " has a disabled map store config, so nothing reloads what the"
                            + " idle clock evicts");
            assertTrue(store.getImplementation() instanceof RecordDiskLoader,
                    name + " is not backed by the on-disk record leg; an evicted row has"
                            + " nowhere to come back from");
            assertEquals(name, ((RecordDiskLoader) store.getImplementation()).mapName(),
                    "the loader behind " + name + " answers for another record type");
        }
    }

    /**
     * A SEARCH member's record maps carry NEITHER the idle clock NOR the loader.
     *
     * <p>The two travel together, and on a member with no volume the pair is neither.
     * The clock alone would make eviction a DELETION on every partition a search member
     * owned: the row goes off-heap, the loader has no index to answer from, and a Get
     * reports a durable row as absent. So a search member holds its partitions resident
     * for as long as it is a member, which is what "partitions in memory only" means.
     *
     * <p>The role also travels, as a member attribute, because every durability count in
     * stratum is taken over the writers in the cluster view.
     */
    @Test
    void aSearchMembersRecordMapsCarryNoClockAndNoLoader() {
        Config cfg = StratumMapConfig.build(List.of(), 2, 1, StratumRole.SEARCH);

        assertEquals(StratumRole.SEARCH.name(),
                cfg.getMemberAttributeConfig().getAttribute(StratumRole.ATTRIBUTE),
                "a search member that does not publish its role is counted as a durable copy");

        for (String name : StratumEventStores.mapNames()) {
            MapConfig map = cfg.getMapConfigs().get(name);
            assertNotNull(map, name + " has no MapConfig");
            assertEquals(0, map.getMaxIdleSeconds(),
                    name + " carries the idle clock on a member with no disk leg behind it, so"
                            + " eviction there is a deletion");
            assertFalse(map.getMapStoreConfig().isEnabled(),
                    name + " carries an enabled read-through loader on a member with no volume");
            assertNull(map.getMapStoreConfig().getImplementation(),
                    name + " was given a RecordDiskLoader on a member with no index for it to read");
            assertEquals(StratumMapConfig.DEFAULT_BACKUP_COUNT, map.getBackupCount(),
                    name + " holds a different number of replicas on a search member than on a"
                            + " writer; Hazelcast reads the count from whichever member owns the"
                            + " partition");
        }
    }

    /** A WRITER publishes its role too, so absence never has to be guessed at. */
    @Test
    void aWriterPublishesItsRole() {
        Config cfg = StratumMapConfig.build(List.of(), 2, 1, StratumRole.WRITER);
        assertEquals(StratumRole.WRITER.name(),
                cfg.getMemberAttributeConfig().getAttribute(StratumRole.ATTRIBUTE));
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
