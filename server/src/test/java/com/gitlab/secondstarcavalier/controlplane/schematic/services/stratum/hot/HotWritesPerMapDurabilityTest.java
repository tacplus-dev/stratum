// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The quorum gate counts EACH MAP'S OWN backups, not one deployment-wide number.
 *
 * <p>StratumMapConfig sizes a record map at
 * {@code max(deployment floor, the record's declared min_durable_copies) - 1}, so
 * a record declaring 3 against a deployment default of 2 gets two synchronous
 * backups and three copies really do exist. HotWrites used to count from one
 * {@code syncBackupCount} handed in at construction — the deployment number — so
 * it reported 2 for that record on a healthy 3-member cluster and
 * {@code QuorumGate.require(name, 2, 3)} refused every write to it, permanently.
 *
 * <p>Three members is the smallest cluster that can tell the two numbers apart:
 * {@code durableCopies} is capped by the visible member count, so on one or two
 * members both the map's count and the deployment's collapse to the same cap.
 */
class HotWritesPerMapDurabilityTest {

    /** The record declaring 3: StratumMapConfig gives it backupCount = 3 - 1. */
    private static final String HIGH_FLOOR_MAP = "demo.HighFloorEvent";

    /** A record at the deployment floor of 2: backupCount = 2 - 1. */
    private static final String DEPLOYMENT_FLOOR_MAP = "demo.Event";

    private static final int MEMBERS = 3;
    private static final int FIRST_PORT = 5801;

    private final List<HazelcastInstance> cluster = new ArrayList<>();

    @BeforeEach
    void setUp() {
        List<String> seeds = new ArrayList<>();
        for (int i = 0; i < MEMBERS; i++) {
            seeds.add("127.0.0.1:" + (FIRST_PORT + i));
        }
        for (int i = 0; i < MEMBERS; i++) {
            cluster.add(Hazelcast.newHazelcastInstance(memberConfig(FIRST_PORT + i, seeds)));
        }
        assertEquals(MEMBERS, cluster.get(0).getCluster().getMembers().size(),
                "the test needs all three members up before it can tell the two counts apart");
    }

    @AfterEach
    void tearDown() {
        for (HazelcastInstance hz : cluster) {
            hz.shutdown();
        }
    }

    private static Config memberConfig(int port, List<String> seeds) {
        Config cfg = new Config();
        cfg.setClusterName("stratum-per-map-durability-test");
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().setPort(port);
        cfg.getNetworkConfig().setPortAutoIncrement(false);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        for (String seed : seeds) {
            cfg.getNetworkConfig().getJoin().getTcpIpConfig().addMember(seed);
        }
        cfg.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        cfg.addMapConfig(new MapConfig(HIGH_FLOOR_MAP).setBackupCount(2).setAsyncBackupCount(0));
        cfg.addMapConfig(new MapConfig(DEPLOYMENT_FLOOR_MAP).setBackupCount(1).setAsyncBackupCount(0));
        return cfg;
    }

    /**
     * The regression: on the same instance, two maps with different backup counts
     * must report different durable-copy counts. The old deployment-wide field
     * reported 2 for both.
     */
    @Test
    void durableCopiesFollowTheMapsOwnBackupCount() {
        HotWrites hot = new HotWrites(cluster.get(0), 0);

        assertEquals(3, hot.durableCopies(HIGH_FLOOR_MAP),
                "a record map configured with two synchronous backups supplies three copies");
        assertEquals(2, hot.durableCopies(DEPLOYMENT_FLOOR_MAP),
                "a record map at the deployment floor still supplies two");
    }

    /**
     * The symptom the audit reported: the write itself. A record declaring 3 on a
     * healthy 3-member cluster must be ACCEPTED — it used to be refused forever,
     * because the gate was told only 2 copies existed.
     */
    @Test
    void aWriteAboveTheDeploymentFloorIsAckedOnAClusterThatCanHoldIt() {
        HotWrites hot = new HotWrites(cluster.get(0), 0);
        IMap<String, String> map = cluster.get(0).getMap(HIGH_FLOOR_MAP);

        hot.write(map, HIGH_FLOOR_MAP, "k1", "v1", 3, () -> "{}", 1L);

        assertEquals("v1", map.get("k1"));
    }
}
