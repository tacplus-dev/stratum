// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.disk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Message;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint.Checkpoint;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint.DailyCheckpoint;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint.DurableRows;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint.TombstoneLog;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.hot.HotWrites;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.services.letsencrypt.Letsencrypt;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.services.letsencrypt.Helpers;

/**
 * Idle eviction is a cache miss, not a deletion — the property the whole disk leg
 * exists for, asserted end to end.
 *
 * <p>{@code IdleResidencyEvictionTest} covers the clock's own semantics: a row
 * nothing touches leaves memory, and a read keeps it warm. This covers what
 * happens to a row AFTER it has left. Those were the three ways it could be lost,
 * and each is one test here:
 *
 * <ol>
 *   <li>it is unreadable, because nothing reloads it;</li>
 *   <li>it is absent from the next daily checkpoint, which dumps from memory and
 *       then truncates the redo entries that still named it — the point at which
 *       the loss became permanent;</li>
 *   <li>it is therefore missing from the cold start built on that checkpoint.</li>
 * </ol>
 *
 * <p>Each test carries a CONTROL that is the pre-fix wiring — the same map with no
 * loader, the same transaction dumping from memory alone — so what is asserted is
 * not "the row is there" but "the leg is the reason it is there". Without it these
 * would pass on any store that simply had not evicted yet.
 *
 * <p>The write goes through {@link HotWrites}, i.e. the real ingest path, because
 * "every write lands on disk as well as in memory" is half of what is under test:
 * nothing here puts a row into the index by hand.
 */
class IdleEvictionDurabilityTest {

    private static final String MAP = "letsencrypt.LetsEncryptCert";
    /** The same map shape with no read-through leg: the store as it was before this. */
    private static final String CONTROL_MAP = "letsencrypt.LetsEncryptCertNoLeg";

    /**
     * Wide enough that Hazelcast's own expiry granularity is not what is being
     * measured, short enough to wait out twice. Same reasoning, and the same
     * doubling, as {@code IdleResidencyEvictionTest}.
     */
    private static final int IDLE_SECONDS = 2;

    private HazelcastInstance hz;
    private RecordDisk disk;

    @AfterEach
    void down() {
        if (hz != null) {
            hz.shutdown();
            hz = null;
        }
        if (disk != null) {
            disk.close();
            disk = null;
        }
    }

    /**
     * A member whose record map is wired exactly as {@code StratumMapConfig}
     * wires one — the idle clock plus the {@link RecordDiskLoader} behind it —
     * alongside a control map that has the clock and no loader.
     */
    private IMap<String, Message> member(Path dataRoot) {
        disk = new RecordDisk(dataRoot, List.of(MAP), () -> 30L * 24 * 60 * 60 * 1000);

        Config cfg = new Config();
        cfg.setClusterName("idle-eviction-durability-" + System.nanoTime());
        cfg.getJetConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        cfg.addMapConfig(recordMap(MAP, true));
        cfg.addMapConfig(recordMap(CONTROL_MAP, false));

        hz = Hazelcast.newHazelcastInstance(cfg);
        return hz.getMap(MAP);
    }

    private static MapConfig recordMap(String name, boolean withLeg) {
        MapConfig mapCfg = new MapConfig(name);
        mapCfg.setMaxIdleSeconds(IDLE_SECONDS);
        if (withLeg) {
            MapStoreConfig store = new MapStoreConfig();
            store.setEnabled(true);
            store.setImplementation(new RecordDiskLoader(MAP));
            store.setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY);
            mapCfg.setMapStoreConfig(store);
        }
        return mapCfg;
    }

    private static Letsencrypt.LetsEncryptCert event(String id, long epochMs) {
        return Letsencrypt.LetsEncryptCert.newBuilder()
                .setCertSetId(id)
                .setIssuedAt(epochMs)
                .setSpiffeId("spiffe://example/tenants/t/users/u")
                .build();
    }

    /** One write through the real ingest path, into the map and onto this node's index. */
    private void ingest(IMap<String, Message> map, String mapName, Letsencrypt.LetsEncryptCert e)
            throws Exception {
        new HotWrites(hz, 1).write(map, mapName, e.getCertSetId(), e, 1,
                () -> Helpers.letsEncryptCertToJson(e), e.getIssuedAt());
        // The disk write is queued off the ack path on purpose (see RecordDisk), so
        // a test that reads the index has to wait for the thread the production
        // write path deliberately does not wait for.
        disk.awaitWrites(10_000);
    }

    /** Leaves the row strictly alone for twice its window — see IdleResidencyEvictionTest. */
    private static void waitOutTheIdleWindow() throws InterruptedException {
        Thread.sleep(TimeUnit.SECONDS.toMillis(IDLE_SECONDS) * 2);
    }

    /**
     * Waits out the clock and then settles the record store, so what follows sees
     * the map a scan would see rather than a race.
     *
     * <p>The wait alone is not enough for the two tests that SCAN. Hazelcast
     * expires an idle entry lazily on access — which is what makes the row
     * unreadable, and is all {@code IdleResidencyEvictionTest} and the readback
     * test below need — but the record store itself is cleared by a periodic
     * expiration task, so {@code size()} and {@code entrySet()} can still report an
     * entry the clock has already retired. {@code evict} is that same removal,
     * taken now instead of on the next tick: it drops the entry from memory and,
     * unlike a {@code remove}, does not reach the map store, so the state it leaves
     * is exactly the state max-idle leaves.
     *
     * <p>The clock is still in the path — the row is genuinely past its window
     * before this is called, and the control map below proves it — so what is
     * asserted afterwards is still "an idle-evicted row", not "a row someone
     * deleted from memory".
     */
    private static void evictAndSettle(IMap<String, Message> map, String key)
            throws InterruptedException {
        waitOutTheIdleWindow();
        map.evict(key);
    }

    // ------------------------------------------------------------------ (1)

    /**
     * A row evicted by max-idle is still readable: the loader fires on the miss.
     *
     * <p>No {@code evict} here, and no settling — this is the one test whose claim
     * is about the CLOCK, so the clock has to be what removes the row. The control
     * map is the proof that it did: same window, same wait, same write, and the only
     * difference is the read-through leg. It answering null while the other answers
     * the row is the whole assertion, and it is not satisfiable by a row that
     * simply never left memory.
     */
    @Test
    void anIdleEvictedRowIsStillReadable(@TempDir Path dataRoot) throws Exception {
        IMap<String, Message> map = member(dataRoot);
        IMap<String, Message> control = hz.getMap(CONTROL_MAP);
        Letsencrypt.LetsEncryptCert e = event("evt-1", 1_000L);
        ingest(map, MAP, e);
        ingest(control, CONTROL_MAP, e);

        waitOutTheIdleWindow();

        assertNull(control.get("evt-1"),
                "the control map has the same idle clock and no read-through leg, and still"
                        + " answered; the window has not actually passed, so nothing below is"
                        + " measuring a reload");
        assertEquals(e, map.get("evt-1"),
                "an idle-evicted row did not come back from the disk leg; idle eviction is"
                        + " then a deletion, which is what the 2026-09-21 hotfix turned the"
                        + " clock off to prevent");
    }

    // ------------------------------------------------------------------ (2)

    /**
     * A row evicted by max-idle survives the daily checkpoint that truncates the
     * redo log behind it.
     *
     * <p>This is the failure that made the gap permanent rather than temporary:
     * step 2 dumped what memory held, step 3 discarded the redo entries for
     * everything else, and after one cycle no cold start could reach the row again.
     * The control is the OLD dump source — the same transaction reading memory
     * alone — and it must miss the row, or the union is not what saved it.
     */
    @Test
    void anIdleEvictedRowSurvivesACheckpointAndTruncate(@TempDir Path dataRoot) throws Exception {
        IMap<String, Message> map = member(dataRoot);
        Letsencrypt.LetsEncryptCert e = event("evt-2", 2_000L);
        ingest(map, MAP, e);

        evictAndSettle(map, "evt-2");
        assertEquals(0, map.size(), "the row is still resident, so the dump below would"
                + " have found it in memory whatever the disk leg did");

        Path memoryOnly = dataRoot.resolve("control");
        new DailyCheckpoint(new DurableRows(hz, null, List.of(MAP)),
                DailyCheckpoint.DiskPurge.NONE, new TombstoneLog(memoryOnly),
                new Checkpoint(memoryOnly), () -> { })
                .run(30L * 24 * 60 * 60 * 1000, 3_000L);
        assertTrue(new Checkpoint(memoryOnly).load().isEmpty(),
                "a dump taken from memory alone already held the row, so the union is not"
                        + " what this test is measuring");

        DailyCheckpoint.Result result = new DailyCheckpoint(
                new DurableRows(hz, disk, List.of(MAP)),
                disk::purge, new TombstoneLog(dataRoot), new Checkpoint(dataRoot), () -> { })
                .run(30L * 24 * 60 * 60 * 1000, 3_000L);

        assertEquals(1, result.dumped, "the off-heap row was not dumped");
        List<Checkpoint.Row> dumped = new Checkpoint(dataRoot).load();
        assertEquals(1, dumped.size());
        assertEquals("evt-2", dumped.get(0).key);
        assertEquals(e, dumped.get(0).value);
    }

    // ------------------------------------------------------------------ (3)

    /**
     * A row idle-evicted BEFORE the checkpoint ran is back after a cold start.
     *
     * <p>The two above are the halves; this is the whole thing, and it is the one
     * that would have been observed in production: a pod restarts, loads its
     * checkpoint, and either the row is there or it is gone for good — the redo
     * entries that named it were truncated by the same transaction that wrote the
     * dump, so there is nothing else left to recover it from.
     *
     * <p>The reload is {@code EventStore.loadCheckpoint}'s own step, done here
     * against a fresh member on the same volume: a genuinely new map, so a row that
     * turns up in it came out of the file and not out of the old heap.
     */
    @Test
    void anIdleEvictedRowSurvivesAColdStart(@TempDir Path dataRoot) throws Exception {
        IMap<String, Message> map = member(dataRoot);
        Letsencrypt.LetsEncryptCert e = event("evt-3", 3_000L);
        ingest(map, MAP, e);

        evictAndSettle(map, "evt-3");
        assertEquals(0, map.size(), "the row is still resident, so the checkpoint below"
                + " would have found it in memory whatever the disk leg did");

        new DailyCheckpoint(new DurableRows(hz, disk, List.of(MAP)),
                disk::purge, new TombstoneLog(dataRoot), new Checkpoint(dataRoot), () -> { })
                .run(30L * 24 * 60 * 60 * 1000, 4_000L);

        hz.shutdown();
        hz = null;
        disk.close();
        disk = null;

        IMap<String, Message> restarted = member(dataRoot);
        for (Checkpoint.Row row : new Checkpoint(dataRoot).load()) {
            restarted.putIfAbsent(row.key, row.value);
        }

        assertNotNull(restarted.get("evt-3"),
                "the row was idle-evicted before the checkpoint and did not survive the cold"
                        + " start built on it; after one checkpoint cycle there is nothing left"
                        + " to recover it from");
        assertEquals(e, restarted.get("evt-3"));
    }
}
