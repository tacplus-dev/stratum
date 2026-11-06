// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.hazelcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import com.github.tacplusdev.stratum.model.chassis.jwk.ImmutableJwk;
import com.github.tacplusdev.stratum.model.chassis.key.ImmutableKey;
import com.github.tacplusdev.stratum.model.chassis.key.KeyHelpers;
import com.github.tacplusdev.stratum.model.chassis.memberport.MemberPort;
import com.github.tacplusdev.stratum.model.chassis.token.TokenWrapper;
import com.github.tacplusdev.stratum.model.types.sys.jwk.JwkOuterClass;
import com.github.tacplusdev.stratum.model.types.sys.key.KeyOuterClass;

/**
 * The acceptance test of stratum#17: two tenants' Stratum members on one network,
 * each Hazelcast bound to loopback behind the generated member port
 * (tacplus-dev/tacplus#212) and reaching the others only through
 * {@link MemberNetwork}. A fake kernel stands in for the fleet: one CA for every
 * serving certificate, one signing key for every token, as the real fleet has.
 *
 * <p>Tenant A's members form one cluster; tenant B's member, even seeded with A's
 * member port and given A's cluster name, never joins it; and a peer with no
 * token, another tenant's token, an expired token or no TLS at all reaches no
 * Hazelcast. Outbound, a member refuses a member port that presents another
 * tenant's certificate, or none, before it sends anything.
 */
class TenantMemberAdmissionTest {

    static final String TD = "td.example";
    static final String SVC = "stratum";
    static final String AUD = MemberPort.audienceFor(SVC);
    static final String A = "tenanta1";
    static final String B = "tenantb1";

    Fleet fleet;
    final List<AutoCloseable> closers = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        fleet = new Fleet();
    }

    @AfterEach
    void tearDown() {
        for (int i = closers.size() - 1; i >= 0; i--) {
            try {
                closers.get(i).close();
            } catch (Exception ignored) {
                // Best effort.
            }
        }
    }

    /**
     * A's two members cluster through their member ports. B's member is seeded with
     * its own port and A's first member's port, and is given A's cluster name, so
     * the member port is all that keeps it out: it never joins, and A's data never
     * reaches it.
     */
    @Test
    void twoTenantsMembersNeverFormOneCluster() throws Exception {
        // Each tenant's members share a target port, as every pod of one Stratum
        // does; each member has its own loopback target IP, as each pod has its own
        // loopback. Two tenants' ports differ so their aliases can share this host.
        int portA = freePort();
        int portB = freePort();
        Member a1 = new Member(A, new InetSocketAddress("127.0.1.1", portA));
        Member a2 = new Member(A, new InetSocketAddress("127.0.1.2", portA));
        Member b1 = new Member(B, new InetSocketAddress("127.0.1.3", portB));

        List<String> seedsA = List.of(a1.seed(), a2.seed());
        a1.start(seedsA, 0, null);
        a2.start(seedsA, 1, null);
        waitFor("tenant A's members to form a cluster", 60_000, () -> a1.size() == 2 && a2.size() == 2);
        assertTrue(a1.minter.mints.get() > 0, "A's members talked through the member port");

        b1.start(List.of(b1.seed(), a1.seed()), 0, a1.hz.getConfig().getClusterName());
        Thread.sleep(8_000);
        assertEquals(1, b1.size(), "tenant B's member must not join tenant A's cluster");
        assertEquals(2, a1.size(), "tenant A's cluster is untouched");

        a1.hz.<String, String>getMap("probe").put("k", "from-a1");
        assertEquals("from-a1", a2.hz.<String, String>getMap("probe").get("k"), "A's members share data");
        assertNull(b1.hz.<String, String>getMap("probe").get("k"), "tenant B sees none of it");
    }

    /**
     * Inbound: a peer with no token, another tenant's token, an expired token, or
     * Hazelcast's own bytes in the clear, is dropped, and the member port never even
     * dials the target behind it.
     */
    @Test
    void aMemberWithNoTokenOrAnotherTenantsTokenIsRefused() throws Exception {
        try (CountingTarget target = new CountingTarget()) {
            MemberPort port = memberPort(A);
            InetSocketAddress listen = port.listen(loopback0(), target.address());

            byte[][] openings = {
                frame(MemberPort.FRAME_DATA, "HZC".getBytes(StandardCharsets.US_ASCII)),
                frame(MemberPort.FRAME_AUTH, fleet.mint(B, 300).getBytes(StandardCharsets.UTF_8)),
                frame(MemberPort.FRAME_AUTH, fleet.mint(A, -5).getBytes(StandardCharsets.UTF_8)),
            };
            for (byte[] opening : openings) {
                try (SSLSocket s = tlsClient(listen)) {
                    s.getOutputStream().write(opening);
                    s.getOutputStream().write(frame(MemberPort.FRAME_DATA, "HZC".getBytes(StandardCharsets.US_ASCII)));
                    s.getOutputStream().flush();
                    assertTrue(dropped(s, 5_000), "the member port kept a peer it must refuse");
                } catch (IOException droppedWhileWriting) {
                    // Dropped before the write finished: as good.
                }
            }
            try (Socket plain = new Socket()) {
                plain.connect(listen, 5_000);
                plain.getOutputStream().write("HZC".getBytes(StandardCharsets.US_ASCII));
                plain.getOutputStream().write(new byte[64]);
                assertTrue(dropped(plain, 15_000), "a plaintext member is dropped");
            }
            assertEquals(0, target.accepted.get(), "nothing may reach Hazelcast before a valid token");
        }
    }

    /**
     * Outbound: a member port that presents another tenant's certificate, or no
     * TLS at all, is refused in the handshake, so this member's token and bytes
     * never reach it.
     */
    @Test
    void aMemberPortPresentingAnotherTenantsCertificateOrNoneIsRefused() throws Exception {
        MemberPort port = memberPort(A);
        try (FakeMemberPort foreign = new FakeMemberPort(fleet.leaf(B), true);
                FakeMemberPort plaintext = new FakeMemberPort(null, false)) {
            for (FakeMemberPort remote : List.of(foreign, plaintext)) {
                InetSocketAddress local = port.forward(remote.address());
                try (Socket s = new Socket()) {
                    s.connect(local, 5_000);
                    s.getOutputStream().write("HZC".getBytes(StandardCharsets.US_ASCII));
                    s.getOutputStream().flush();
                    assertTrue(dropped(s, 15_000), "the tunnel to a refused member port stays open");
                } catch (IOException droppedWhileWriting) {
                    // Dropped before the write finished: as good.
                }
                waitFor("the remote to see the connection end", 15_000, () -> remote.ended.get() > 0);
                byte[] seen = remote.received.toByteArray();
                if (remote == foreign) {
                    assertEquals(0, seen.length, "a member sent bytes to a port it must refuse");
                } else {
                    // A plaintext peer sees the TLS ClientHello and nothing else.
                    assertEquals(0x16, seen[0], "a member spoke to a plaintext peer in the clear");
                }
                assertFalse(new String(seen, StandardCharsets.ISO_8859_1).contains("HZC"),
                        "Hazelcast's bytes reached a port the member must refuse");
            }
        }
    }

    // ---- Members ----

    /** One Stratum member: its member port, and its Hazelcast once started. */
    final class Member {
        final String tenant;
        final InetSocketAddress target;
        final Minter minter;
        final MemberPort port;
        final InetSocketAddress portAddress;
        HazelcastInstance hz;

        Member(String tenant, InetSocketAddress target) throws Exception {
            this.tenant = tenant;
            this.target = target;
            this.minter = new Minter(tenant);
            this.port = track(new MemberPort(SVC, TD, List.of(fleet.ca), minter, fleet.jwks::get,
                    fleet.leaf(tenant)::self));
            this.portAddress = port.listen(loopback0(), target);
        }

        /** This member's entry in a seed list: its member port. */
        String seed() {
            return portAddress.getHostString() + ":" + portAddress.getPort();
        }

        /**
         * Starts Hazelcast as StratumHandler does, this member at {@code self} in
         * {@code seeds}; {@code clusterName}, when set, overrides the tenant's own.
         */
        void start(List<String> seeds, int self, String clusterName) throws IOException {
            Config cfg = StratumMapConfig.build(tenant,
                    MemberNetwork.through(seeds, self, target, portAddress.getPort(), port::forward), 1, null);
            if (clusterName != null) {
                cfg.setClusterName(clusterName);
            }
            cfg.setProperty("hazelcast.logging.type", "slf4j");
            cfg.setProperty("hazelcast.max.join.seconds", "10");
            cfg.setProperty("hazelcast.wait.seconds.before.join", "1");
            cfg.getNetworkConfig().getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(3);
            hz = Hazelcast.newHazelcastInstance(cfg);
            closers.add(hz::shutdown);
        }

        int size() {
            return hz.getCluster().getMembers().size();
        }
    }

    MemberPort memberPort(String tenant) throws Exception {
        return track(new MemberPort(SVC, TD, List.of(fleet.ca), new Minter(tenant), fleet.jwks::get,
                fleet.leaf(tenant)::self));
    }

    /** A member's token source, like the ServiceManager's: one token, renewed when half spent. */
    final class Minter implements MemberPort.TokenSource {
        final String tenant;
        final AtomicInteger mints = new AtomicInteger();
        private String cached;

        Minter(String tenant) {
            this.tenant = tenant;
        }

        @Override
        public synchronized String token() {
            if (cached == null || TokenWrapper.parse(cached).getExpiration() - System.currentTimeMillis() / 1000 < 150) {
                cached = fleet.mint(tenant, 300);
                mints.incrementAndGet();
            }
            return cached;
        }
    }

    // ---- The fake kernel ----

    /** The fleet's CA and the kernel's token signing key, with its JWKS. */
    static final class Fleet {
        final ImmutableKey kernel;
        final Map<String, ImmutableJwk> jwks = new ConcurrentHashMap<>();
        final KeyPair caKeys;
        final X509Certificate ca;
        final AtomicLong serial = new AtomicLong(100);

        Fleet() throws Exception {
            kernel = ImmutableKey.from(KeyHelpers.newEd25519Key());
            jwks.put(kernel.getProto().getId(), ImmutableJwk.from(jwkFor(kernel)));
            caKeys = ecKeys();
            X500Name name = new X500Name("CN=fleet-ca");
            X509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(name, BigInteger.ONE,
                    new Date(System.currentTimeMillis() - 60_000), new Date(System.currentTimeMillis() + 86_400_000L),
                    name, caKeys.getPublic());
            b.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
            ca = new JcaX509CertificateConverter().getCertificate(
                    b.build(new JcaContentSignerBuilder("SHA256withECDSA").build(caKeys.getPrivate())));
        }

        /** A Stratum member token of {@code tenant}, for audience member:stratum. */
        String mint(String tenant, long ttlSeconds) {
            TokenWrapper tok = TokenWrapper.create()
                    .setIssuer("kernel")
                    .setAudience(AUD)
                    .setSubType("service")
                    .setSubject(SVC)
                    .setNamespace("/tenants/" + tenant)
                    .setExpiration(System.currentTimeMillis() / 1000 + ttlSeconds);
            tok.sign(kernel);
            return tok.getBase64();
        }

        /** {@code tenant}'s Stratum serving certificate, spiffe://td/tenants/&lt;tid&gt;/services/stratum. */
        Leaf leaf(String tenant) throws Exception {
            KeyPair keys = ecKeys();
            X509v3CertificateBuilder b = new JcaX509v3CertificateBuilder(new X500Name("CN=fleet-ca"),
                    BigInteger.valueOf(serial.incrementAndGet()),
                    new Date(System.currentTimeMillis() - 60_000), new Date(System.currentTimeMillis() + 86_400_000L),
                    new X500Name("CN=" + SVC), keys.getPublic());
            b.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(new GeneralName(
                    GeneralName.uniformResourceIdentifier, "spiffe://" + TD + "/tenants/" + tenant + "/services/" + SVC)));
            X509Certificate cert = new JcaX509CertificateConverter().getCertificate(
                    b.build(new JcaContentSignerBuilder("SHA256withECDSA").build(caKeys.getPrivate())));
            return new Leaf(new MemberPort.Material(new X509Certificate[] {cert, ca}, keys.getPrivate()),
                    new X509Certificate[] {cert, ca}, keys);
        }
    }

    record Leaf(MemberPort.Material material, X509Certificate[] chain, KeyPair keys) {
        MemberPort.Material self() {
            return material;
        }
    }

    static KeyPair ecKeys() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(new ECGenParameterSpec("secp256r1"));
        return g.generateKeyPair();
    }

    static JwkOuterClass.Jwk jwkFor(ImmutableKey key) {
        byte[] spki = key.getPublicKey().getEncoded();
        byte[] raw = new byte[32];
        System.arraycopy(spki, spki.length - 32, raw, 0, 32);
        return JwkOuterClass.Jwk.newBuilder()
                .setKid(key.getProto().getId())
                .setKty(KeyOuterClass.KeyType.KEY_TYPE_OKP)
                .setAlg(key.getAlg())
                .setCrv("Ed25519")
                .setX(Base64.getUrlEncoder().withoutPadding().encodeToString(raw))
                .build();
    }

    // ---- Peers that are not members ----

    /** A loopback listener standing in for Hazelcast: it counts the connections that reach it. */
    static final class CountingTarget implements AutoCloseable {
        final ServerSocket socket = new ServerSocket(0, 64, InetAddress.getLoopbackAddress());
        final AtomicInteger accepted = new AtomicInteger();

        CountingTarget() throws IOException {
            Thread t = new Thread(() -> {
                while (!socket.isClosed()) {
                    try (Socket ignored = socket.accept()) {
                        accepted.incrementAndGet();
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "counting-target");
            t.setDaemon(true);
            t.start();
        }

        InetSocketAddress address() {
            return (InetSocketAddress) socket.getLocalSocketAddress();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    /**
     * A remote "member port" that is not one: TLS with {@code leaf} (another
     * tenant's certificate) and the member ALPN, or plain TCP when {@code tls} is
     * false. It records the application bytes it receives.
     */
    static final class FakeMemberPort implements AutoCloseable {
        final ServerSocket socket;
        final ByteArrayOutputStream received = new ByteArrayOutputStream();
        final AtomicInteger ended = new AtomicInteger();

        FakeMemberPort(Leaf leaf, boolean tls) throws Exception {
            if (tls) {
                KeyStore ks = KeyStore.getInstance("PKCS12");
                ks.load(null, null);
                ks.setKeyEntry("leaf", leaf.keys().getPrivate(), new char[0], leaf.chain());
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, new char[0]);
                SSLContext ctx = SSLContext.getInstance("TLSv1.3");
                ctx.init(kmf.getKeyManagers(), null, null);
                SSLServerSocket s = (SSLServerSocket) ctx.getServerSocketFactory().createServerSocket();
                SSLParameters p = s.getSSLParameters();
                p.setApplicationProtocols(new String[] {MemberPort.ALPN});
                s.setSSLParameters(p);
                socket = s;
            } else {
                socket = new ServerSocket();
            }
            socket.bind(loopback0());
            Thread t = new Thread(() -> {
                while (!socket.isClosed()) {
                    try (Socket in = socket.accept()) {
                        in.setSoTimeout(15_000);
                        InputStream is = in.getInputStream();
                        byte[] buf = new byte[4096];
                        int n;
                        while ((n = is.read(buf)) >= 0) {
                            synchronized (received) {
                                received.write(buf, 0, n);
                            }
                        }
                    } catch (IOException e) {
                        if (socket.isClosed()) {
                            return;
                        }
                    } finally {
                        ended.incrementAndGet();
                    }
                }
            }, "fake-member-port");
            t.setDaemon(true);
            t.start();
        }

        InetSocketAddress address() {
            return (InetSocketAddress) socket.getLocalSocketAddress();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    // ---- Plumbing ----

    <T extends AutoCloseable> T track(T c) {
        closers.add(c);
        return c;
    }

    /** A TLS client of a member port that trusts the fleet CA and presents no token yet. */
    SSLSocket tlsClient(InetSocketAddress addr) throws Exception {
        KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
        ts.load(null, null);
        ts.setCertificateEntry("ca", fleet.ca);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        SSLContext ctx = SSLContext.getInstance("TLSv1.3");
        ctx.init(null, tmf.getTrustManagers(), null);
        SSLSocket s = (SSLSocket) ctx.getSocketFactory().createSocket();
        SSLParameters p = s.getSSLParameters();
        p.setApplicationProtocols(new String[] {MemberPort.ALPN});
        s.setSSLParameters(p);
        s.connect(addr, 5_000);
        s.startHandshake();
        return s;
    }

    /** One member-port frame: a type byte, a 4-byte big-endian length, the payload. */
    static byte[] frame(int type, byte[] payload) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(b);
        out.writeByte(type);
        out.writeInt(payload.length);
        out.write(payload);
        return b.toByteArray();
    }

    /** True once the peer ends the connection within {@code timeoutMillis}. */
    static boolean dropped(Socket s, long timeoutMillis) throws IOException {
        s.setSoTimeout((int) timeoutMillis);
        InputStream in = s.getInputStream();
        byte[] buf = new byte[4096];
        try {
            while (in.read(buf) >= 0) {
                // Drain whatever arrived before the drop.
            }
            return true;
        } catch (SocketTimeoutException e) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }

    static void waitFor(String what, long timeoutMillis, BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(50);
        }
    }

    static InetSocketAddress loopback0() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return s.getLocalPort();
        }
    }
}
