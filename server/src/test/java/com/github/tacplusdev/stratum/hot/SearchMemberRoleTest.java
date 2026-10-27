// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.github.tacplusdev.stratum.hazelcast.StratumRole;

/**
 * What a SEARCH member is: a full member of the cluster in memory and in the query
 * plane, and nothing at all in the durability plane.
 *
 * <p>Four properties, which are the four the member-roles decision asks for beyond
 * the write gate ({@link HotWritesWriterCountTest}):
 *
 * <ol>
 *   <li>the disk legs of a write go to every WRITER and to no search member, and to
 *       writers that do not own the key's partition — which is what makes durability
 *       independent of where Hazelcast happened to put the partition;</li>
 *   <li>the restore barrier expects writers only, so search members neither satisfy
 *       it nor make it unsatisfiable;</li>
 *   <li>search members take partitions and answer the scatter-gather a predicate
 *       query is;</li>
 *   <li>removing them rebalances and loses nothing.</li>
 * </ol>
 *
 * <p>Every member here lives in one JVM, which is what makes the cluster cheap and
 * is also the one thing to be careful of: {@link HotRedoLogs} and
 * {@code RecordDisk} install ONE node-local registry per JVM, so an append landing
 * on any member in this test would resolve the same log. Property 1 is therefore
 * asserted on the DELIVERY — which members the task is sent to — rather than on the
 * logs, because the logs cannot tell the members apart here and would pass for the
 * old partition-scoped design as readily as for this one.
 */
class SearchMemberRoleTest {

    private static final String MAP = "demo.Event";
    private static final int FIRST_PORT = 5831;

    /** Member UUIDs a {@link MarkerTask} has run on. Static: the members share a JVM. */
    private static final Set<String> DELIVERED = ConcurrentHashMap.newKeySet();

    private final List<HazelcastInstance> cluster = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (HazelcastInstance hz : cluster) {
            if (hz.getLifecycleService().isRunning()) {
                hz.shutdown();
            }
        }
        cluster.clear();
        DELIVERED.clear();
    }

    /** Brings up {@code writers} writers then {@code searchers} search members. */
    private void cluster(int writers, int searchers) {
        int size = writers + searchers;
        List<String> seeds = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            seeds.add("127.0.0.1:" + (FIRST_PORT + i));
        }
        for (int i = 0; i < size; i++) {
            cluster.add(Hazelcast.newHazelcastInstance(memberConfig(FIRST_PORT + i, seeds,
                    i < writers ? StratumRole.WRITER : StratumRole.SEARCH)));
        }
        assertEquals(size, cluster.get(0).getCluster().getMembers().size(),
                "the test needs every member up before the roles can be counted apart");
    }

    private static Config memberConfig(int port, List<String> seeds, StratumRole role) {
        Config cfg = new Config();
        cfg.setClusterName("stratum-search-member-test");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().setPort(port);
        cfg.getNetworkConfig().setPortAutoIncrement(false);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        for (String seed : seeds) {
            cfg.getNetworkConfig().getJoin().getTcpIpConfig().addMember(seed);
        }
        cfg.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        cfg.getMemberAttributeConfig().setAttribute(StratumRole.ATTRIBUTE, role.name());
        // One synchronous backup: the memory redundancy the deployment default carries,
        // and what makes a graceful departure loss-free.
        cfg.addMapConfig(new MapConfig(MAP).setBackupCount(1).setAsyncBackupCount(0));
        return cfg;
    }

    private static String uuid(HazelcastInstance hz) {
        return hz.getCluster().getLocalMember().getUuid().toString();
    }

    // ------------------------------------------------------------- 1. the fan-out

    /**
     * The persist reaches every writer and no search member, and it reaches a writer
     * that does not own the key's partition.
     *
     * <p>The second half is the whole point. Under the design this replaced the append
     * rode the partition's replication, so the members that logged a write were the
     * ones Hazelcast had put the partition on — and with volume-less members able to
     * own a partition, a write whose primary and backup both landed on them was logged
     * nowhere. The selection is {@link StratumRole#writers}, which is what
     * {@code HotWrites.write} submits to.
     */
    @Test
    void thePersistGoesToEveryWriterAndNoSearchMember() throws Exception {
        cluster(2, 3);
        HazelcastInstance ingesting = cluster.get(0);

        // A key whose partition is owned by a SEARCH member: the case the old
        // partition-scoped append could not make durable.
        String key = keyOwnedByASearchMember(ingesting);
        Member owner = ingesting.getPartitionService().getPartition(key).getOwner();
        assertEquals(StratumRole.SEARCH, StratumRole.of(owner),
                "the test needs a key whose partition a search member owns");

        List<Member> writers = StratumRole.writers(ingesting);
        assertEquals(2, writers.size());
        deliver(ingesting, writers);

        Set<String> expected = new HashSet<>();
        expected.add(uuid(cluster.get(0)));
        expected.add(uuid(cluster.get(1)));
        assertEquals(expected, new HashSet<>(DELIVERED),
                "the persist must reach both writers and neither search member");
        assertFalse(DELIVERED.contains(owner.getUuid().toString()),
                "the persist reached the search member that owns the partition, which has no"
                        + " volume to write it to");
        assertTrue(DELIVERED.contains(uuid(cluster.get(1))),
                "the persist did not reach a writer that owns nothing of this key, which is"
                        + " exactly the writer the old partition-scoped append skipped");
    }

    /** Submits the marker to {@code members} on the executor HotWrites uses, and waits. */
    private void deliver(HazelcastInstance from, List<Member> members) throws Exception {
        CountDownLatch done = new CountDownLatch(members.size());
        LATCH.set(done);
        from.getExecutorService(HotWrites.PERSIST_EXECUTOR).executeOnMembers(new MarkerTask(), members);
        assertTrue(done.await(20, TimeUnit.SECONDS),
                "the persist task was not delivered to every member it was submitted to");
    }

    private static final AtomicReference<CountDownLatch> LATCH = new AtomicReference<>();

    /**
     * Stands in for {@link RecordPersistTask} and records which member it ran on.
     *
     * <p>A stand-in rather than the real task because the real one resolves the
     * JVM-wide {@code HotRedoLogs}/{@code RecordDisk} registries, which every member in
     * this test would share — so it could not say which member it had run on, which is
     * the only question here. It is submitted through the same executor name and the
     * same member selection the real one is.
     */
    static final class MarkerTask implements Runnable, Serializable, HazelcastInstanceAware {
        private static final long serialVersionUID = 1L;
        private transient HazelcastInstance hz;

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }

        @Override
        public void run() {
            DELIVERED.add(hz.getCluster().getLocalMember().getUuid().toString());
            CountDownLatch latch = LATCH.get();
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    /** Waits for {@code hz} to see exactly {@code size} members. */
    private static void awaitClusterSize(HazelcastInstance hz, int size) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (hz.getCluster().getMembers().size() != size) {
            if (System.currentTimeMillis() >= deadline) {
                assertEquals(size, hz.getCluster().getMembers().size(),
                        "the cluster view did not converge within 30s");
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

    /** A key Hazelcast places on a partition a search member owns. */
    private static String keyOwnedByASearchMember(HazelcastInstance hz) {
        for (int i = 0; i < 10_000; i++) {
            String key = "k" + i;
            if (StratumRole.of(hz.getPartitionService().getPartition(key).getOwner()) == StratumRole.SEARCH) {
                return key;
            }
        }
        throw new IllegalStateException("no key of 10000 landed on a search member's partition");
    }

    // ------------------------------------------------------------- 2. the barrier

    /**
     * The restore barrier expects WRITERS. Three search members raise the member count
     * and the expectation not at all, and they file nothing — so they neither satisfy
     * the barrier nor wedge it.
     *
     * <p>Counting members here was the failure to avoid: the barrier would have waited
     * for five reports from two members that file them, and every pod's restart would
     * have cost a full timeout for as long as the deployment ran any search members.
     */
    @Test
    void theBarrierExpectsWritersOnlyAndSearchMembersFileNothing() throws Exception {
        cluster(2, 3);
        HazelcastInstance w0 = cluster.get(0);
        HazelcastInstance w1 = cluster.get(1);

        // Both writers are inside their own barrier at once — the simultaneous-restart
        // case — so each has to be satisfied by the other's report.
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread peer = new Thread(() -> {
            try {
                HotRestore.awaitClusterReplay(w1, 12, 0, 2, Duration.ofSeconds(20));
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        peer.start();
        int contributors = HotRestore.awaitClusterReplay(w0, 34, 0, 2, Duration.ofSeconds(20));
        peer.join(30_000);

        assertNull(failure.get(), () -> "the peer writer's barrier failed: " + failure.get());
        assertEquals(2, contributors, "both writers held a durable copy; neither search member does");

        IMap<String, Integer> ledger = w0.getMap(HotRestore.LEDGER_MAP);
        assertEquals(2, ledger.size(), "a search member filed a replay report");
        for (int i = 2; i < cluster.size(); i++) {
            assertNull(ledger.get(uuid(cluster.get(i))),
                    "a search member filed a durable-copy report about a volume it does not have");
        }

        // And a search member passes the same barrier, having filed nothing: the
        // writers are done, so there is nothing left for it to wait for.
        int seen = HotRestore.awaitClusterReplay(cluster.get(2), 0, 0, 2, Duration.ofSeconds(20));
        assertEquals(2, seen, "the search member counted itself or a sibling as a durable copy");
        assertEquals(2, ledger.size(), "the search member filed a report on its way through");
    }

    // ------------------------------------------------- 3 and 4. the query plane

    /**
     * Search members take partitions and answer the query, and giving them back loses
     * nothing.
     *
     * <p>A predicate query is a scatter-gather across every member in parallel and the
     * results are merged, which is the whole reason a search member is worth adding:
     * partitions move to it and its share of the scan runs on its own threads. The
     * count coming back whole is what says the fan-out reached it — a query that only
     * ran where it was issued would return the issuing member's own partitions.
     */
    @Test
    void searchMembersTakePartitionsAnswerQueriesAndGiveThemBackLosslessly() {
        cluster(1, 3);
        HazelcastInstance writer = cluster.get(0);
        IMap<String, String> map = writer.getMap(MAP);

        int rows = 400;
        for (int i = 0; i < rows; i++) {
            map.set("k" + i, "v" + i);
        }

        for (int i = 1; i < cluster.size(); i++) {
            assertFalse(cluster.get(i).<String, String>getMap(MAP).localKeySet().isEmpty(),
                    "search member " + i + " was given no partitions, so it adds no query"
                            + " parallelism at all");
        }
        assertEquals(rows, map.size());
        assertEquals(rows, map.values(Predicates.alwaysTrue()).size(),
                "the predicate query did not fan out across the members");

        // Given back one at a time. A graceful shutdown migrates the departing member's
        // partitions first, which is what makes removing a search member a rebalance
        // rather than a loss — and nothing durable was ever on it to lose.
        for (int i = cluster.size() - 1; i >= 1; i--) {
            cluster.get(i).shutdown();
        }
        // The departure is asynchronous in the surviving member's view, so it is waited
        // for rather than assumed: reading the map before the view has converged would
        // be reading it mid-migration and would say nothing either way.
        awaitClusterSize(writer, 1);
        assertEquals(rows, map.size(),
                "rows went missing when the search members left; a search member holds nothing"
                        + " the cluster cannot get back from a peer");
        assertEquals(rows, map.values(Predicates.alwaysTrue()).size());
    }
}
