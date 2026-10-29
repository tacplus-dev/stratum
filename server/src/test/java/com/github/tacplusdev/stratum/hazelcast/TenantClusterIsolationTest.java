// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.hazelcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Members of two tenants' stores on one network never form one cluster
 * (stratum#18): the cluster name is {@code stratum-<tid>}, the tid taken from the
 * instance's own SPIFFE identity, and the join is never multicast. This keeps
 * accidental joins apart; it is not admission (stratum#17).
 */
class TenantClusterIsolationTest {

    private final List<HazelcastInstance> members = new ArrayList<>();

    @AfterEach
    void down() {
        members.forEach(HazelcastInstance::shutdown);
    }

    @Test
    void theClusterNameIsTheTenantsOwn() {
        assertEquals("stratum-acme0001",
                StratumMapConfig.build("acme0001", List.of("127.0.0.1"), 1, null).getClusterName());
    }

    @Test
    void theTenantComesFromTheInstancesOwnSpiffeId() {
        assertEquals("acme0001",
                StratumMapConfig.tenantOf("spiffe://td.example/tenants/acme0001/services/stratum"));
    }

    @Test
    void anIdentityThatIsNotATenantsStratumNamesNoTenant() {
        for (String id : List.of(
                "spiffe://td.example/system/services/stratum",
                "spiffe://td.example/tenants/acme0001/services/redo",
                "spiffe://td.example/tenants/acme0001/workspaces/w1/services/stratum",
                "spiffe://td.example/tenants/acme0001/services/stratum/workloads/x",
                "not a spiffe id")) {
            assertThrows(IllegalArgumentException.class, () -> StratumMapConfig.tenantOf(id), id);
        }
    }

    @Test
    void aMemberWithoutATenantRefusesToStart() {
        assertThrows(IllegalArgumentException.class,
                () -> StratumMapConfig.build(null, List.of("127.0.0.1"), 1, null));
        assertThrows(IllegalArgumentException.class,
                () -> StratumMapConfig.build(" ", List.of("127.0.0.1"), 1, null));
    }

    @Test
    void aMemberWithoutSeedsRefusesToStart() {
        IllegalArgumentException none = assertThrows(IllegalArgumentException.class,
                () -> StratumMapConfig.build("acme0001", List.of(), 1, null));
        assertTrue(none.getMessage().contains("STRATUM_SEED_MEMBERS"), none.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> StratumMapConfig.build("acme0001", null, 1, null));
    }

    @Test
    void theJoinIsNeverMulticast() {
        JoinConfig join = StratumMapConfig.build("acme0001", List.of("10.0.0.1:5001", "10.0.0.2:5001"), 1, null)
                .getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertFalse(join.getAutoDetectionConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
        assertEquals(List.of("10.0.0.1:5001", "10.0.0.2:5001"), join.getTcpIpConfig().getMembers());
    }

    /**
     * Four members on loopback, every one seeded with every address: tenant A's two
     * form one cluster (the control: the seeds do reach), tenant B's two another,
     * and no member of one ever sees a member of the other.
     */
    @Test
    void twoTenantsMembersOnOneNetworkNeverFormOneCluster() throws IOException {
        List<Integer> ports = List.of(freePort(), freePort(), freePort(), freePort());
        List<String> seeds = ports.stream().map(p -> "127.0.0.1:" + p).toList();

        HazelcastInstance a1 = member("tenanta1", seeds, ports.get(0));
        HazelcastInstance b1 = member("tenantb1", seeds, ports.get(1));
        HazelcastInstance a2 = member("tenanta1", seeds, ports.get(2));
        HazelcastInstance b2 = member("tenantb1", seeds, ports.get(3));

        for (HazelcastInstance hz : List.of(a1, a2)) {
            assertEquals(2, hz.getCluster().getMembers().size(), "tenant A's members did not find each other");
            assertEquals("stratum-tenanta1", hz.getConfig().getClusterName());
        }
        for (HazelcastInstance hz : List.of(b1, b2)) {
            assertEquals(2, hz.getCluster().getMembers().size(), "tenant B's members did not find each other");
            assertEquals("stratum-tenantb1", hz.getConfig().getClusterName());
        }
        assertTrue(a1.getCluster().getMembers().stream().noneMatch(m ->
                        b1.getCluster().getMembers().contains(m) || b2.getCluster().getMembers().contains(m)),
                "a member of tenant A is in tenant B's cluster");
    }

    private HazelcastInstance member(String tenant, List<String> seeds, int port) {
        Config cfg = StratumMapConfig.build(tenant, seeds, 1, null);
        // One host, so each member needs its own port; production pins 5001 per pod.
        cfg.getNetworkConfig().setPort(port);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        members.add(hz);
        return hz;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
