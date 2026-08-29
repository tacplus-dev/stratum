// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The one library behaviour the whole redo design rests on, proven rather than
 * cited.
 *
 * The hot write appends to the owner's log in an entry processor's primary body
 * and to the backup's log in {@link EntryProcessor#getBackupProcessor()}. That
 * only gives "two logs by the time the write returns" if Hazelcast runs the
 * backup body as part of the SYNCHRONOUS backup operation — if the backup body
 * ran asynchronously after the call returned, the second log would be a promise,
 * not a fact, and every claim built on it would be wrong.
 *
 * The design document explicitly declined to assert this from documentation. So
 * this runs a real two-member cluster with {@code backupCount=1,
 * asyncBackupCount=0} and counts, from inside both bodies, what has happened by
 * the time {@code executeOnKey} returns.
 *
 * <p>Both members live in one JVM, so the counters below are shared and count
 * EVENTS, not per-node state. That is the right granularity for the question
 * being asked: had the backup body run when the call returned?
 */
class HotRedoBackupTimingTest {

    private static final AtomicInteger PRIMARY = new AtomicInteger();
    private static final AtomicInteger BACKUP = new AtomicInteger();

    private HazelcastInstance a;
    private HazelcastInstance b;

    @BeforeEach
    void setUp() {
        PRIMARY.set(0);
        BACKUP.set(0);
        a = Hazelcast.newHazelcastInstance(config());
        b = Hazelcast.newHazelcastInstance(config());
        // Both members must be in the cluster before the write, or "the backup
        // body ran" would be trivially false for want of anywhere to run it.
        assertEquals(2, a.getCluster().getMembers().size(), "the two members did not form a cluster");
    }

    @AfterEach
    void tearDown() {
        if (a != null) a.shutdown();
        if (b != null) b.shutdown();
    }

    private static Config config() {
        Config cfg = new Config();
        cfg.setClusterName("stratum-backup-timing-test");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        // Loopback TCP/IP only: multicast would pick up whatever else is on the
        // developer's network and make the member count non-deterministic.
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .addMember("127.0.0.1:5801").addMember("127.0.0.1:5802");
        cfg.getNetworkConfig().setPort(5801).setPortAutoIncrement(true);
        cfg.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");

        MapConfig map = new MapConfig("timing");
        map.setBackupCount(1);
        map.setAsyncBackupCount(0);
        cfg.addMapConfig(map);
        return cfg;
    }

    @Test
    void theBackupBodyHasRunByTheTimeTheWriteReturns() {
        IMap<String, String> map = a.getMap("timing");

        map.executeOnKey("k1", new CountingProcessor());

        assertEquals(1, PRIMARY.get(), "the primary body must have run exactly once");
        assertEquals(1, BACKUP.get(),
                "Hazelcast 5.5 ran the backup entry-processor body AFTER executeOnKey returned."
                        + " The redo design's second log would then be a promise, not a fact:"
                        + " a write acked here could survive on one volume only.");
        assertEquals("v1", map.get("k1"));
    }

    /** Ten writes, so one lucky ordering cannot pass for the guarantee. */
    @Test
    void everyWritesBackupBodyHasRunByTheTimeItReturns() {
        IMap<String, String> map = a.getMap("timing");
        for (int i = 0; i < 10; i++) {
            map.executeOnKey("k" + i, new CountingProcessor());
            assertTrue(BACKUP.get() >= i + 1,
                    "backup body lagged the write: after " + (i + 1) + " writes only "
                            + BACKUP.get() + " backup bodies had run");
        }
        assertEquals(10, PRIMARY.get());
        assertEquals(10, BACKUP.get());
    }

    /** Primary body: sets the value and records that it ran. */
    static final class CountingProcessor implements EntryProcessor<String, String, Boolean>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public Boolean process(Map.Entry<String, String> entry) {
            entry.setValue("v1");
            PRIMARY.incrementAndGet();
            return Boolean.TRUE;
        }

        @Override
        public EntryProcessor<String, String, Boolean> getBackupProcessor() {
            return new BackupCountingProcessor();
        }
    }

    /**
     * Backup body, deliberately a DIFFERENT class: returning {@code this} would
     * make the two bodies indistinguishable and the test could not tell which one
     * had run.
     */
    static final class BackupCountingProcessor implements EntryProcessor<String, String, Boolean>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public Boolean process(Map.Entry<String, String> entry) {
            entry.setValue("v1");
            BACKUP.incrementAndGet();
            return Boolean.TRUE;
        }
    }
}
