// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.hazelcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MemberAddressProviderConfig;
import com.hazelcast.spi.MemberAddressProvider;

/**
 * How a member's Hazelcast is placed behind the member port (stratum#17): it
 * binds to the loopback target only, and every member is known by the same
 * loopback alias on every member, which each other member forwards to its
 * member port.
 */
class MemberNetworkTest {

    private static final Predicate<InetAddress> NONE_LOCAL = a -> false;
    private static final InetSocketAddress TARGET = new InetSocketAddress("127.0.0.1", 5701);
    private static final List<String> SEEDS =
            List.of("stratum-0.stratum:5001", "stratum-1.stratum:5001", "stratum-2.stratum:5001");

    @Test
    void thisMemberIsTheSeedNamedByItsPodsHostname() {
        assertEquals(1, MemberNetwork.self(SEEDS, "stratum-1", NONE_LOCAL));
        assertEquals(2, MemberNetwork.self(SEEDS, "stratum-2.stratum.t1.svc.cluster.local", NONE_LOCAL));
    }

    @Test
    void thisMemberIsALoopbackOrOneOfItsOwnAddresses() {
        assertEquals(0, MemberNetwork.self(List.of("127.0.0.1"), "dev", NONE_LOCAL));
        assertEquals(0, MemberNetwork.self(List.of("localhost:5001"), "dev", NONE_LOCAL));
        assertEquals(1, MemberNetwork.self(List.of("10.0.0.1:5001", "10.0.0.2:5001"), "pod",
                a -> a.getHostAddress().equals("10.0.0.2")));
    }

    @Test
    void aSeedListThatDoesNotNameThisMemberExactlyOnceRefusesToStart() {
        IllegalArgumentException none = assertThrows(IllegalArgumentException.class,
                () -> MemberNetwork.self(SEEDS, "stratum-7", NONE_LOCAL));
        assertTrue(none.getMessage().contains("seed_members"), none.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> MemberNetwork.self(List.of("stratum-0.a:5001", "stratum-0.b:5001"), "stratum-0", NONE_LOCAL));
        assertThrows(IllegalArgumentException.class, () -> MemberNetwork.self(List.of(), "stratum-0", NONE_LOCAL));
        assertThrows(IllegalArgumentException.class, () -> MemberNetwork.self(null, "stratum-0", NONE_LOCAL));
    }

    /**
     * Member i is 127.0.2.(i+1) on every member: its own advertised address, and
     * every other member's forward to its member port. Hazelcast binds only to the
     * loopback target, and its join names only the other members' aliases.
     */
    @Test
    void everyMemberKnowsEveryOtherByTheSameAliasForwardedToItsMemberPort() throws IOException {
        List<String[]> forwards = new ArrayList<>();
        MemberNetwork net = MemberNetwork.through(SEEDS, 1, TARGET, 5001, (remote, local) -> {
            forwards.add(new String[] {remote.getHostString() + ":" + remote.getPort(),
                    local.getHostString() + ":" + local.getPort()});
            return local;
        });

        assertEquals(TARGET, net.bind());
        assertEquals(new InetSocketAddress("127.0.2.2", 5701), net.advertised());
        assertEquals(List.of(new InetSocketAddress("127.0.2.1", 5701), new InetSocketAddress("127.0.2.3", 5701)),
                net.peers());
        assertEquals(2, forwards.size());
        assertEquals(List.of("stratum-0.stratum:5001", "127.0.2.1:5701"), List.of(forwards.get(0)));
        assertEquals(List.of("stratum-2.stratum:5001", "127.0.2.3:5701"), List.of(forwards.get(1)));
    }

    @Test
    void aSeedWithoutAPortIsAtTheMemberPortsOwnPort() throws IOException {
        List<InetSocketAddress> remotes = new ArrayList<>();
        MemberNetwork.through(List.of("stratum-0.stratum", "stratum-1.stratum"), 0, TARGET, 5001, (remote, local) -> {
            remotes.add(remote);
            return local;
        });
        assertEquals(5001, remotes.get(0).getPort());
    }

    @Test
    void theTargetMustBeLoopbackAndOutsideTheAliases() {
        assertThrows(IllegalArgumentException.class,
                () -> MemberNetwork.through(SEEDS, 0, new InetSocketAddress("10.0.0.1", 5701), 5001, (r, l) -> l));
        assertThrows(IllegalArgumentException.class,
                () -> MemberNetwork.through(SEEDS, 0, new InetSocketAddress("127.0.2.9", 5701), 5001, (r, l) -> l));
    }

    @Test
    void hazelcastBindsTheTargetAdvertisesItsAliasAndJoinsTheOthersOnly() throws IOException {
        MemberNetwork net = MemberNetwork.through(SEEDS, 0, TARGET, 5001, (r, l) -> l);
        Config cfg = StratumMapConfig.build("acme0001", net, 1, null);

        MemberAddressProviderConfig addresses = cfg.getNetworkConfig().getMemberAddressProviderConfig();
        assertTrue(addresses.isEnabled());
        MemberAddressProvider provider = (MemberAddressProvider) addresses.getImplementation();
        assertEquals(TARGET, provider.getBindAddress());
        assertEquals(new InetSocketAddress("127.0.2.1", 5701), provider.getPublicAddress());
        assertFalse(cfg.getNetworkConfig().isPortAutoIncrement());

        JoinConfig join = cfg.getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertFalse(join.getAutoDetectionConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
        assertEquals(List.of("127.0.2.2:5701", "127.0.2.3:5701"), join.getTcpIpConfig().getMembers());
    }
}
