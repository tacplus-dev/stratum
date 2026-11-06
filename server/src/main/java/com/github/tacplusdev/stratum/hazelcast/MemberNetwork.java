// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.hazelcast;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Where a member's Hazelcast binds and how it reaches the other members of its
 * tenant: through the generated member port only (stratum#17,
 * tacplus-dev/tacplus#212).
 *
 * <p>Open-source Hazelcast can neither encrypt nor authenticate member traffic,
 * so it never crosses the network itself. It binds to the member port's loopback
 * target ({@code grpcconfig.member_port.target_address}); the member port serves
 * TLS on the pod's member port and admits only a Stratum member of this tenant
 * that opens with a valid kernel token. Outbound, every other member is reached
 * through a local forward to its member port.
 *
 * <p>Hazelcast knows a member by one address on every member, so each member
 * has a loopback alias that is the same everywhere: the i-th entry of
 * {@code seed_members} is {@code 127.0.2.<i+1>} at the target's port. A member
 * advertises its own alias, and forwards every other member's alias to that
 * member's port. The seed list must therefore be the same, in the same order, on
 * every member, and the target port the same on every member (the operator
 * renders both once per tenant).
 */
public final class MemberNetwork {

    /** The loopback /24 of the members' aliases: seed i is {@code 127.0.2.<i+1>}. */
    static final String ALIAS_PREFIX = "127.0.2.";

    /** At most this many members: Config.seed_members' own limit. */
    static final int MAX_MEMBERS = 64;

    private static final Pattern IPV4 = Pattern.compile("[0-9.]+");

    /** Tunnels connections to a local loopback address to a remote member port: {@code MemberPort::forward}. */
    @FunctionalInterface
    public interface Forwarder {
        InetSocketAddress forward(InetSocketAddress remote, InetSocketAddress local) throws IOException;
    }

    private final InetSocketAddress bind;
    private final InetSocketAddress advertised;
    private final List<InetSocketAddress> peers;

    private MemberNetwork(InetSocketAddress bind, InetSocketAddress advertised, List<InetSocketAddress> peers) {
        this.bind = bind;
        this.advertised = advertised;
        this.peers = List.copyOf(peers);
    }

    /** Where this member's Hazelcast binds: the member port's loopback target. */
    public InetSocketAddress bind() {
        return bind;
    }

    /** The address the other members know this member by. */
    public InetSocketAddress advertised() {
        return advertised;
    }

    /** The other members, as this member reaches them: the TCP/IP join's members. */
    public List<InetSocketAddress> peers() {
        return peers;
    }

    /** A member with no other member, its Hazelcast on {@code bind}: a test's. */
    public static MemberNetwork alone(InetSocketAddress bind) {
        return direct(bind, List.of());
    }

    /** Members reached directly at {@code peers}, with no member port: the cluster-name tests'. */
    static MemberNetwork direct(InetSocketAddress bind, List<InetSocketAddress> peers) {
        return new MemberNetwork(bind, bind, peers);
    }

    /**
     * Places this member behind its member port.
     *
     * @param seeds      Config.seed_members: every member's member port,
     *                   "host:port" or "host", this member's own included
     * @param self       this member's index in {@code seeds} ({@link #self})
     * @param target     grpcconfig.member_port.target_address, where Hazelcast
     *                   binds: loopback, and outside the aliases
     * @param memberPort the port of a seed that names none
     * @param forwarder  the member port's {@code forward}
     * @throws IllegalArgumentException with no seeds, too many, or a target that
     *                   is not loopback: the member refuses to start
     */
    public static MemberNetwork through(List<String> seeds, int self, InetSocketAddress target, int memberPort,
            Forwarder forwarder) throws IOException {
        requireSeeds(seeds);
        if (self < 0 || self >= seeds.size()) {
            throw new IllegalArgumentException("this member is not in Config.seed_members " + seeds);
        }
        if (target.isUnresolved() || !target.getAddress().isLoopbackAddress()
                || target.getHostString().startsWith(ALIAS_PREFIX)) {
            throw new IllegalArgumentException("grpcconfig.member_port.target_address must be a loopback address outside "
                    + ALIAS_PREFIX + "0/24 (the members' aliases), got " + target);
        }
        List<InetSocketAddress> peers = new ArrayList<>();
        for (int i = 0; i < seeds.size(); i++) {
            if (i == self) {
                continue;
            }
            URI seed = parse(seeds.get(i));
            InetSocketAddress remote = new InetSocketAddress(seed.getHost(),
                    seed.getPort() < 0 ? memberPort : seed.getPort());
            peers.add(forwarder.forward(remote, alias(i, target.getPort())));
        }
        return new MemberNetwork(target, alias(self, target.getPort()), peers);
    }

    /**
     * This member's index in {@code seeds}: the one seed that names it, by a
     * loopback address or "localhost", by one of its own addresses, or by its
     * pod's hostname as the first label of a name (the StatefulSet's
     * stratum-&lt;i&gt;.stratum names pod stratum-&lt;i&gt;). No DNS is consulted.
     *
     * @throws IllegalArgumentException when no seed, or more than one, names it:
     *                                  the member refuses to start
     */
    public static int self(List<String> seeds, String hostname, Predicate<InetAddress> isLocal) {
        requireSeeds(seeds);
        String podName = hostname == null ? "" : firstLabel(hostname);
        int found = -1;
        for (int i = 0; i < seeds.size(); i++) {
            String host = parse(seeds.get(i)).getHost();
            boolean me;
            if (isLiteral(host)) {
                InetAddress a = literal(host);
                me = a.isLoopbackAddress() || isLocal.test(a);
            } else {
                me = host.equalsIgnoreCase("localhost") || (!podName.isEmpty() && firstLabel(host).equals(podName));
            }
            if (me) {
                if (found >= 0) {
                    throw new IllegalArgumentException("Config.seed_members names this member twice ("
                            + seeds.get(found) + ", " + seeds.get(i) + ")");
                }
                found = i;
            }
        }
        if (found < 0) {
            throw new IllegalArgumentException("Config.seed_members " + seeds + " does not name this member (host "
                    + hostname + "): every member is in it, its own entry included");
        }
        return found;
    }

    /** This pod's hostname: $HOSTNAME (the pod name, in Kubernetes), else the JVM's. */
    public static String localHostname() {
        String env = System.getenv("HOSTNAME");
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "";
        }
    }

    /** Whether {@code a} is an address of one of this host's interfaces. */
    public static boolean isLocalAddress(InetAddress a) {
        try {
            return NetworkInterface.getByInetAddress(a) != null;
        } catch (SocketException e) {
            return false;
        }
    }

    static InetSocketAddress alias(int index, int port) {
        return new InetSocketAddress(ALIAS_PREFIX + (index + 1), port);
    }

    private static void requireSeeds(List<String> seeds) {
        if (seeds == null || seeds.isEmpty()) {
            throw new IllegalArgumentException("Config.seed_members is empty: a member refuses to start "
                    + "without its tenant's seed members (multicast join is never used; "
                    + "a single member or a dev run sets it to 127.0.0.1)");
        }
        if (seeds.size() > MAX_MEMBERS) {
            throw new IllegalArgumentException("Config.seed_members names " + seeds.size() + " members, at most "
                    + MAX_MEMBERS);
        }
    }

    private static URI parse(String seed) {
        try {
            URI uri = new URI("tcp://" + seed.trim());
            if (uri.getHost() == null || uri.getPath() != null && !uri.getPath().isEmpty()) {
                throw new URISyntaxException(seed, "not host[:port]");
            }
            return uri;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Config.seed_members: not host[:port]: " + seed, e);
        }
    }

    private static boolean isLiteral(String host) {
        return host.startsWith("[") || IPV4.matcher(host).matches();
    }

    private static InetAddress literal(String host) {
        try {
            // A literal is parsed, never looked up.
            return InetAddress.getByName(host.startsWith("[") ? host.substring(1, host.length() - 1) : host);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Config.seed_members: not an address: " + host, e);
        }
    }

    private static String firstLabel(String host) {
        int dot = host.indexOf('.');
        return (dot < 0 ? host : host.substring(0, dot)).toLowerCase(Locale.ROOT);
    }
}
