// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tacplusdev.stratum.hazelcast.StratumMapConfig;
import com.github.tacplusdev.stratum.hazelcast.StratumRole;
import com.github.tacplusdev.stratum.hot.HotRestore;

/**
 * Defect #14: during a roll a starting writer exited with
 * {@code SplitBrainProtectionException: stratum-hot-min-durable-copies has failed}
 * while its peers were restarting. A cluster below the writer quorum is a state a
 * starting member waits out, not a reason to exit.
 *
 * <p>Drives the real {@link EventStore#init} — the member, the hot maps, the
 * checkpoint decision, the redo replay and the restore barrier — against peers in
 * the same JVM. The store's member binds {@link StratumMapConfig#HAZELCAST_PORT};
 * the peers take the ports after it. STRATUM_DATA_DIR is set by the server pom.
 *
 * <p>A peer here is a writer member that has filed the report a restarting peer
 * files at its own boot (in-flight, nothing durable), which is what lets the
 * starting member's barrier complete without a second real init in the JVM.
 */
class StartupWriterQuorumTest {

    private static final int FIRST_PEER_PORT = StratumMapConfig.HAZELCAST_PORT + 1;
    private static final List<String> SEEDS = List.of(
            "127.0.0.1:" + StratumMapConfig.HAZELCAST_PORT,
            "127.0.0.1:" + FIRST_PEER_PORT,
            "127.0.0.1:" + (FIRST_PEER_PORT + 1),
            "127.0.0.1:" + (FIRST_PEER_PORT + 2));

    private final List<HazelcastInstance> peers = new ArrayList<>();
    private EventStore store;
    private Thread startup;

    @BeforeEach
    void cleanVolume() throws IOException {
        String dir = System.getenv("STRATUM_DATA_DIR");
        assertNotNull(dir, "the server pom sets STRATUM_DATA_DIR for the test JVM");
        deleteRecursively(Paths.get(dir));
    }

    @AfterEach
    void tearDown() throws Exception {
        if (startup != null) {
            startup.interrupt();
            startup.join(10_000);
        }
        if (store != null) {
            store.shutdown();
        }
        for (HazelcastInstance hz : peers) {
            if (hz.getLifecycleService().isRunning()) {
                hz.getLifecycleService().terminate();
            }
        }
    }

    /** Criterion 1: alone below the floor at startup, the floor met later — init waits, then succeeds. */
    @Test
    void startupWaitsForTheWriterQuorumAndThenCompletes() throws Exception {
        AtomicReference<Throwable> failure = startInit();

        // One writer of the two the floor needs. Nothing about that is fatal.
        Thread.sleep(5_000);
        assertNull(failure.get(), "a member below the writer quorum must wait, not fail: " + failure.get());
        assertTrue(startup.isAlive(), "init must still be waiting for the second writer");

        restartingPeer(FIRST_PEER_PORT);

        startup.join(TimeUnit.SECONDS.toMillis(90));
        assertNull(failure.get(), "init must complete once the quorum is met: " + failure.get());
        assertFalse(startup.isAlive(), "init must have returned once the second writer joined");
        // StratumHandler's cold-boot decision, the next protected read of the startup.
        store.eventCount();
    }

    /**
     * The incident: the quorum was there when the member started, then the peers went
     * away for their own restarts while it was still inside its startup — here, in the
     * restore barrier, waiting for a peer that is itself restoring. The protected maps
     * refuse from that moment; init must wait for the writers to come back, not exit.
     */
    @Test
    void startupSurvivesLosingTheWriterQuorumMidRestore() throws Exception {
        HazelcastInstance serving = peer(FIRST_PEER_PORT);
        HazelcastInstance restoring = peer(FIRST_PEER_PORT + 1);
        awaitQuorum(serving);
        awaitQuorum(restoring);
        serving.getMap(HotRestore.LEDGER_MAP).put(uuid(serving), 0);
        restoring.getMap(HotRestore.INFLIGHT_MAP).put(uuid(restoring), Boolean.TRUE);

        AtomicReference<Throwable> failure = startInit();

        // Inside the barrier: its own report is filed, the restoring peer's is not.
        long deadline = System.currentTimeMillis() + 60_000;
        while (serving.getMap(HotRestore.LEDGER_MAP).size() < 2) {
            assertNull(failure.get(), "init must reach the restore barrier: " + failure.get());
            assertTrue(System.currentTimeMillis() < deadline, "init never filed its restore report");
            Thread.sleep(100);
        }

        // Both peers restart at once.
        serving.getLifecycleService().terminate();
        restoring.getLifecycleService().terminate();
        Thread.sleep(5_000);
        assertNull(failure.get(), "losing the writer quorum mid-startup must be waited out: " + failure.get());
        assertTrue(startup.isAlive(), "init must still be waiting for the writers to come back");

        restartingPeer(FIRST_PEER_PORT + 2);

        startup.join(TimeUnit.SECONDS.toMillis(90));
        assertNull(failure.get(), "init must complete once the quorum is back: " + failure.get());
        assertFalse(startup.isAlive(), "init must have returned once a writer rejoined");
        store.eventCount();
    }

    private AtomicReference<Throwable> startInit() {
        store = new EventStore();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        startup = new Thread(() -> {
            try {
                store.init(3600, SEEDS);
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "stratum-startup");
        startup.start();
        return failure;
    }

    private HazelcastInstance peer(int port) {
        Config cfg = StratumMapConfig.build(SEEDS, StratumMapConfig.DEFAULT_MIN_DURABLE_COPIES,
                StratumMapConfig.DEFAULT_BACKUP_COUNT, StratumRole.WRITER);
        cfg.getNetworkConfig().setPort(port);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        peers.add(hz);
        return hz;
    }

    /** A writer that joins and files what a restarting writer files at its own boot. */
    private void restartingPeer(int port) throws InterruptedException {
        HazelcastInstance hz = peer(port);
        awaitQuorum(hz);
        hz.getMap(HotRestore.INFLIGHT_MAP).put(uuid(hz), Boolean.TRUE);
        hz.getMap(HotRestore.LEDGER_MAP).put(uuid(hz), 0);
    }

    private static void awaitQuorum(HazelcastInstance hz) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!hz.getSplitBrainProtectionService()
                .getSplitBrainProtection(StratumMapConfig.SPLIT_BRAIN_PROTECTION_NAME).hasMinimumSize()) {
            assertTrue(System.currentTimeMillis() < deadline, "peer never saw the writer quorum");
            Thread.sleep(100);
        }
    }

    private static String uuid(HazelcastInstance hz) {
        return hz.getCluster().getLocalMember().getUuid().toString();
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }
}
