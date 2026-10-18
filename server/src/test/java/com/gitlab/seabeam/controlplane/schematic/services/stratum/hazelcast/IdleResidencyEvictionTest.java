// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

/**
 * The memory residency clock itself: a row leaves memory after it has gone
 * {@code (spec.record).residency.idle_seconds} with neither a read nor a write,
 * and a READ resets it.
 *
 * <p>This is the behavioural half of the 2026-09-20 record lifecycle decision's
 * memory tier, and the replacement for what {@code WarmEvictionE2ETest} used to
 * assert. The old model evicted on a sweep that compared each row's
 * {@code (spec.store_epoch)} against a residency boundary — a WRITE clock, swept
 * periodically, so a row nothing had read for a year stayed resident as long as
 * it had been written recently, and a row read constantly was evicted the moment
 * its write aged out. The new model has one clock per type and Hazelcast runs it
 * per entry, which inverts both cases. Those are the two assertions here.
 *
 * <p>{@link StratumMapConfigDurabilityTest#everyRecordMapCarriesItsDeclaredIdleClock}
 * covers the other half — that every record map is actually CONFIGURED with its
 * declared clock. Config without behaviour would pass that test with an eviction
 * that never fires; behaviour without config would pass this one on a map no
 * record type uses. Neither is sufficient alone.
 *
 * <p>Seconds, not milliseconds: {@code maxIdleSeconds} is a second-granularity
 * knob, so the shortest honest window is 1s and the waits are sized off it rather
 * than guessed.
 */
class IdleResidencyEvictionTest {

    private static final String MAP = "idle-residency-probe";
    /**
     * Wide enough that Hazelcast's own expiry granularity is not the thing under
     * test. Its expiration task runs on a periodic tick, so at a 1-second window
     * the "a read keeps it warm" case races that tick and fails on timing rather
     * than on the clock's semantics.
     */
    private static final int IDLE_SECONDS = 5;

    private HazelcastInstance hz;

    private IMap<String, String> mapWithIdleClock(int idleSeconds) {
        Config cfg = new Config();
        cfg.setClusterName("idle-residency-" + System.nanoTime());
        cfg.getJetConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        MapConfig mapCfg = new MapConfig(MAP);
        // The one line under test, as StratumMapConfig sets it from the generated
        // StratumEventStores.idleSeconds(name).
        mapCfg.setMaxIdleSeconds(idleSeconds);
        cfg.addMapConfig(mapCfg);

        hz = Hazelcast.newHazelcastInstance(cfg);
        return hz.getMap(MAP);
    }

    @AfterEach
    void down() {
        if (hz != null) {
            hz.shutdown();
            hz = null;
        }
    }

    /**
     * A row nothing touches leaves memory once the idle clock runs out.
     *
     * <p>The wait does NOT poll with {@code get}. That was the first version of this
     * test and it never went green: {@code get} is precisely what resets the idle
     * clock, so polling for the eviction indefinitely postponed it. The row is left
     * strictly alone for twice its window and read exactly once, at the end —
     * which is also enough on its own, because Hazelcast expires an idle entry
     * lazily on access as well as on its periodic sweep.
     */
    @Test
    void aRowNothingTouchesIsEvictedAfterItsIdleSeconds() throws InterruptedException {
        int idleSeconds = 2;
        IMap<String, String> map = mapWithIdleClock(idleSeconds);
        map.put("k", "v");

        Thread.sleep(TimeUnit.SECONDS.toMillis(idleSeconds) * 2);

        assertNull(map.get("k"),
                "the row was still in memory well past its idle_seconds; the memory residency"
                        + " clock is what bounds every record map's growth now that the retention"
                        + " sweep is gone, so a clock that never fires is unbounded memory");
    }

    /**
     * A READ keeps a row resident — the behaviour the decision is built on
     * ("reads keeping data warm is the behavior users expect").
     *
     * <p>And it must do so WITHOUT becoming a write: this is why the clock lives in
     * Hazelcast rather than in a sweep over {@code (spec.store_epoch)}. Nothing here
     * restamps the row, appends to the redo log or touches disk, and the row is
     * still resident — which a store-epoch boundary could only have achieved by
     * turning the read into a write.
     */
    @Test
    void aReadResetsTheIdleClockWithoutWritingTheRow() throws InterruptedException {
        IMap<String, String> map = mapWithIdleClock(IDLE_SECONDS);
        map.put("k", "v");

        // Read across more than one full idle window, never writing: six reads at
        // two-fifths of the window each is 12s of life on a 5s clock, so a row that
        // survives it survived only because each read reset the clock.
        for (int i = 0; i < 6; i++) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(IDLE_SECONDS) * 2 / 5);
            assertNotNull(map.get("k"),
                    "a row being read went idle anyway on read " + i + "; reads are what keep"
                            + " data warm under the idle clock");
        }

        assertEquals("v", map.get("k"), "the value changed, so the read was not read-only");
    }
}
