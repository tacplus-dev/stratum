// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast.StratumMapConfig;

/**
 * A serving peer's ledger report must survive the loss of the members that are
 * restarting, and must come back if it is ever lost anyway.
 *
 * <h2>What went wrong (2026-09-22)</h2>
 * stratum-0 and stratum-2 landed on nodes GKE recreated at the same moment and
 * crash-looped for over an hour, 14 restarts each, against a stratum-1 that had
 * been serving for eleven hours. The barrier needs a report from every LIVE member
 * ({@link HotRestore#awaitClusterReplay}), a member files its report once at its
 * own boot and never again, and stratum-1's had been destroyed when the other two
 * died together: the ledger carried {@code min_durable_copies - 1} = one backup, so
 * every entry lived on two of the three members and the pair that vanished took a
 * third of the map with it. stratum-1 was serving and had no occasion to file
 * another, so the barrier could never again be satisfied by anybody — which is why
 * no restart, in any phase, ever cleared it.
 *
 * <p>The timing story it was first read as — two pods on 5-minute clocks missing
 * each other — is NOT what happened, and
 * {@link #twoPodsRestartingTogetherRecoverWhileTheServingPeerKeepsItsReport}
 * is here to keep that refutation: with the serving peer's report intact, two pods
 * restarting simultaneously both pass, every time.
 */
class HotRestoreLostReportDeadlockTest {

    private static final Duration WAIT = Duration.ofSeconds(8);
    private static final int MIN_DURABLE_COPIES = 2;

    private final List<HazelcastInstance> started = new ArrayList<>();
    private final ExecutorService peerThreads = Executors.newFixedThreadPool(3);

    @AfterEach
    void tearDown() {
        peerThreads.shutdownNow();
        for (HazelcastInstance hz : started) {
            if (hz.getLifecycleService().isRunning()) hz.shutdown();
        }
    }

    /**
     * A member built the way production builds one, so the backup counts under test
     * are the deployed ones and not a config written for the test.
     */
    private HazelcastInstance member() {
        Config cfg = StratumMapConfig.build(
                List.of("127.0.0.1:5881", "127.0.0.1:5882", "127.0.0.1:5883"), MIN_DURABLE_COPIES);
        cfg.setClusterName("stratum-lost-report-deadlock-test");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().setPort(5881).setPortAutoIncrement(true);
        cfg.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        started.add(hz);
        return hz;
    }

    private static String uuid(HazelcastInstance hz) {
        return hz.getCluster().getLocalMember().getUuid().toString();
    }

    /**
     * The loss itself: two of three members die at the same instant and the
     * survivor's coordination state is still there.
     *
     * <p>Three hundred keys rather than one, because the fault is a dice roll per
     * partition and a single key would make this test pass two times in three
     * against the very bug it is here to catch. At one backup the measured loss was
     * 112 of 300 — a third, as the geometry says it must be: of the six ordered
     * owner/backup pairs three members allow, two lie wholly inside the pair that
     * died.
     *
     * <p>Both maps, because the in-flight marks fail in the WORSE direction. A lost
     * ledger report makes a serving member invisible and wedges the barrier; a lost
     * in-flight mark makes a member that is still inside its own restore read as one
     * that has finished, which is the silently-incomplete store the barrier exists
     * to refuse.
     */
    @Test
    void theBarriersCoordinationStateSurvivesLosingTwoOfThreeMembersAtOnce() throws Exception {
        HazelcastInstance survivor = member();
        HazelcastInstance doomedOne = member();
        HazelcastInstance doomedTwo = member();
        assertEquals(3, survivor.getCluster().getMembers().size(), "the three members did not form a cluster");

        IMap<String, Integer> ledger = survivor.getMap(HotRestore.LEDGER_MAP);
        IMap<String, Boolean> inflight = survivor.getMap(HotRestore.INFLIGHT_MAP);
        List<String> members = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            String stand = UUID.randomUUID().toString();
            members.add(stand);
            ledger.put(stand, 100);
            inflight.put(stand, Boolean.TRUE);
        }

        // Not a shutdown: a node that GKE recreates does not hand its partitions over.
        doomedOne.getLifecycleService().terminate();
        doomedTwo.getLifecycleService().terminate();
        long deadline = System.currentTimeMillis() + 60_000;
        while (survivor.getCluster().getMembers().size() > 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        assertEquals(1, survivor.getCluster().getMembers().size(), "the two members did not leave the cluster");

        // One of them comes back, the way k8s brings the pod back. The survivor is
        // read only now because every hot map refuses a read below the split-brain
        // floor, which a cluster of one is — and because what matters is what the
        // returning pod's barrier will find, not what existed mid-outage.
        member();
        deadline = System.currentTimeMillis() + 60_000;
        while (survivor.getCluster().getMembers().size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        assertEquals(2, survivor.getCluster().getMembers().size(), "the replacement member did not join");

        int reportsLost = 0;
        int marksLost = 0;
        for (String stand : members) {
            if (!ledger.containsKey(stand)) reportsLost++;
            if (!inflight.containsKey(stand)) marksLost++;
        }
        assertEquals(0, reportsLost, "ledger reports destroyed by the loss of two of three members,"
                + " out of " + members.size() + "; a destroyed report belonging to a member that is"
                + " still serving can never be re-filed by its own boot and wedges the barrier for"
                + " every other pod");
        assertEquals(0, marksLost, "in-flight marks destroyed by the loss of two of three members,"
                + " out of " + members.size() + "; a destroyed mark reads as a member that has"
                + " finished its restore when it has not");
    }

    /**
     * The crash-loop, end to end: the serving member's report is gone, and a pod
     * restarting against it cannot pass the barrier no matter how long it waits or
     * how many times it tries — until the serving member puts its report back.
     */
    @Test
    void aServingPeerPutsBackAReportThatWentMissingAndUnwedgesTheBarrier() throws Exception {
        HazelcastInstance serving = member();
        HazelcastInstance restarting = member();
        passTheBarrierTogether(serving, restarting);

        // The loss, however it happened: destroyed with the partitions of members
        // that died together, or pruned by a peer whose cluster view was briefly
        // missing this member. Either way nothing else will ever write it again.
        serving.getMap(HotRestore.LEDGER_MAP).delete(uuid(serving));

        HazelcastInstance restarted = restart(restarting);
        IllegalStateException wedged = assertThrows(IllegalStateException.class,
                () -> HotRestore.awaitClusterReplay(restarted, 468, 0, MIN_DURABLE_COPIES, WAIT),
                "with the serving peer's report gone the barrier cannot be satisfied");
        assertTrue(wedged.getMessage().contains("only 1 of 2 expected writer members reported"),
                "the wedge is the barrier's own count, not the contributor check: " + wedged.getMessage());

        assertTrue(HotRestore.refileReport(serving, 468), "the missing report must be put back");
        assertFalse(HotRestore.refileReport(serving, 468),
                "a report that is present must not be re-filed; the entry is a boot snapshot and a"
                        + " repair that revised it would be re-deciding that");

        assertEquals(2, HotRestore.awaitClusterReplay(restarted, 468, 0, MIN_DURABLE_COPIES, WAIT),
                "with the report back the restarting pod passes against the serving peer");
    }

    /**
     * The refutation, kept as a test: two pods restarting at the same moment into a
     * serving third do NOT deadlock on each other's reports. Each files before it
     * waits, so the third report lands while both are still waiting for it.
     */
    @Test
    void twoPodsRestartingTogetherRecoverWhileTheServingPeerKeepsItsReport() throws Exception {
        HazelcastInstance serving = member();
        HazelcastInstance first = member();
        HazelcastInstance second = member();
        CompletableFuture<Integer> a = async(serving, 100);
        CompletableFuture<Integer> b = async(first, 100);
        assertEquals(3, HotRestore.awaitClusterReplay(second, 100, 0, MIN_DURABLE_COPIES, WAIT));
        assertEquals(3, a.get(WAIT.toMillis(), TimeUnit.MILLISECONDS).intValue());
        assertEquals(3, b.get(WAIT.toMillis(), TimeUnit.MILLISECONDS).intValue());

        HazelcastInstance firstAgain = restart(first);
        HazelcastInstance secondAgain = restart(second);
        CompletableFuture<Integer> one = async(firstAgain, 100);
        CompletableFuture<Integer> two = async(secondAgain, 100);
        assertEquals(3, one.get(30, TimeUnit.SECONDS).intValue(),
                "a pod restarting alongside another must not be held up by it");
        assertEquals(3, two.get(30, TimeUnit.SECONDS).intValue(),
                "a pod restarting alongside another must not be held up by it");
    }

    private void passTheBarrierTogether(HazelcastInstance serving, HazelcastInstance other) throws Exception {
        CompletableFuture<Integer> peer = async(serving, 468);
        assertEquals(2, HotRestore.awaitClusterReplay(other, 468, 0, MIN_DURABLE_COPIES, WAIT),
                "the healthy two-member restore must pass before anything is broken");
        assertEquals(2, peer.get(WAIT.toMillis(), TimeUnit.MILLISECONDS).intValue());
    }

    private CompletableFuture<Integer> async(HazelcastInstance hz, int redoRecordsRead) {
        return CompletableFuture.supplyAsync(
                () -> HotRestore.awaitClusterReplay(hz, redoRecordsRead, 0, MIN_DURABLE_COPIES, WAIT),
                peerThreads);
    }

    /** A real restart: the member goes away and comes back with a fresh UUID. */
    private HazelcastInstance restart(HazelcastInstance hz) {
        hz.shutdown();
        started.remove(hz);
        return member();
    }
}
