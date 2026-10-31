package com.github.tacplusdev.stratum.hazelcast;

import java.io.FileOutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Properties;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/** Scratch probe for #17: what does OSS Hazelcast 5.7 do with SSLConfig. */
class MemberTlsProbe {
    static final char[] PW = "changeit".toCharArray();

    record Ca(KeyPair kp, X509Certificate cert) {}

    static Ca ca(String cn) throws Exception {
        KeyPair kp = kp();
        X500Name n = new X500Name("CN=" + cn);
        var b = new JcaX509v3CertificateBuilder(n, BigInteger.ONE, new Date(System.currentTimeMillis() - 60000),
                new Date(System.currentTimeMillis() + 3600000), n, kp.getPublic());
        b.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        return new Ca(kp, new JcaX509CertificateConverter().getCertificate(
                b.build(new JcaContentSignerBuilder("SHA256withECDSA").build(kp.getPrivate()))));
    }

    static KeyPair kp() throws Exception {
        KeyPairGenerator g = KeyPairGenerator.getInstance("EC");
        g.initialize(256);
        return g.generateKeyPair();
    }

    static Path stores(Path dir, String name, Ca issuer, Ca trusted) throws Exception {
        KeyPair kp = kp();
        var b = new JcaX509v3CertificateBuilder(new X500Name("CN=" + issuer.cert().getSubjectX500Principal().getName().substring(3)),
                BigInteger.valueOf(System.nanoTime()), new Date(System.currentTimeMillis() - 60000),
                new Date(System.currentTimeMillis() + 3600000), new X500Name("CN=" + name), kp.getPublic());
        b.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(new GeneralName[] {
                new GeneralName(GeneralName.uniformResourceIdentifier, "spiffe://td/tenants/" + name + "/services/stratum"),
                new GeneralName(GeneralName.iPAddress, "127.0.0.1")}));
        X509Certificate leaf = new JcaX509CertificateConverter().getCertificate(
                b.build(new JcaContentSignerBuilder("SHA256withECDSA").build(issuer.kp().getPrivate())));
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("member", kp.getPrivate(), PW, new X509Certificate[] {leaf, issuer.cert()});
        Path k = dir.resolve(name + "-" + System.nanoTime() + "-key.p12");
        try (var o = new FileOutputStream(k.toFile())) { ks.store(o, PW); }
        KeyStore ts = KeyStore.getInstance("PKCS12");
        ts.load(null, null);
        ts.setCertificateEntry("ca", trusted.cert());
        Path t = dir.resolve(name + "-" + System.nanoTime() + "-trust.p12");
        try (var o = new FileOutputStream(t.toFile())) { ts.store(o, PW); }
        return k.resolveSibling(k.getFileName() + "|" + t.getFileName());
    }

    static Config cfg(int port, int peer, Path dir, String stores) {
        Config c = new Config();
        c.setClusterName("probe");
        c.setProperty("hazelcast.phone.home.enabled", "false");
        c.setProperty("hazelcast.max.join.seconds", "8");
        var net = c.getNetworkConfig();
        net.setPort(port).setPortAutoIncrement(false);
        net.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        net.getJoin().getMulticastConfig().setEnabled(false);
        net.getJoin().getAutoDetectionConfig().setEnabled(false);
        net.getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:" + peer);
        if (stores != null) {
            String[] p = stores.split("\\|");
            Properties props = new Properties();
            props.setProperty("keyStore", dir.resolve(p[0]).toString());
            props.setProperty("keyStorePassword", "changeit");
            props.setProperty("keyStoreType", "PKCS12");
            props.setProperty("trustStore", dir.resolve(p[1]).toString());
            props.setProperty("trustStorePassword", "changeit");
            props.setProperty("trustStoreType", "PKCS12");
            props.setProperty("protocol", "TLSv1.3");
            props.setProperty("mutualAuthentication", "REQUIRED");
            net.setSSLConfig(new SSLConfig().setEnabled(true).setProperties(props));
        }
        return c;
    }

    static void pair(String label, Config a, Config b) {
        HazelcastInstance x = null, y = null;
        try {
            x = Hazelcast.newHazelcastInstance(a);
            y = Hazelcast.newHazelcastInstance(b);
            Thread.sleep(3000);
            System.out.println("PROBE " + label + ": sizes " + x.getCluster().getMembers().size() + "/"
                    + y.getCluster().getMembers().size());
        } catch (Throwable t) {
            System.out.println("PROBE " + label + ": threw " + t);
        } finally {
            if (x != null) x.shutdown();
            if (y != null) y.shutdown();
        }
    }

    /** Raw TLS client against a member: does it speak TLS, and does it demand a client cert? */
    static void raw(String label, int port, Path dir, String stores, Ca trust) {
        try {
            KeyStore ts = KeyStore.getInstance("PKCS12");
            ts.load(null, null);
            ts.setCertificateEntry("ca", trust.cert());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            KeyManagerFactory kmf = null;
            if (stores != null) {
                KeyStore ks = KeyStore.getInstance("PKCS12");
                try (var in = Files.newInputStream(dir.resolve(stores.split("\\|")[0]))) { ks.load(in, PW); }
                kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, PW);
            }
            SSLContext ctx = SSLContext.getInstance("TLSv1.3");
            ctx.init(kmf == null ? null : kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            try (SSLSocket s = (SSLSocket) ctx.getSocketFactory().createSocket()) {
                s.connect(new InetSocketAddress("127.0.0.1", port), 2000);
                s.setSoTimeout(3000);
                s.startHandshake();
                // TLS 1.3: a refused client cert shows up on the first read.
                s.getOutputStream().write("CP2".getBytes());
                s.getOutputStream().flush();
                int r = s.getInputStream().read();
                System.out.println("PROBE " + label + ": handshake ok " + s.getSession().getProtocol()
                        + " " + s.getSession().getCipherSuite() + ", read=" + r);
            }
        } catch (Throwable t) {
            System.out.println("PROBE " + label + ": " + t);
        }
    }

    @Test
    void probe() throws Exception {
        System.out.println("PROBE override=" + System.getProperty("hazelcast.internal.override.enterprise")
                + " enterprise=" + com.hazelcast.instance.BuildInfoProvider.getBuildInfo().isEnterprise());
        Path dir = Files.createTempDirectory("tlsprobe");
        Ca caA = ca("ca-a"), caB = ca("ca-b");
        String a1 = stores(dir, "a", caA, caA).getFileName().toString();
        String a2 = stores(dir, "a", caA, caA).getFileName().toString();
        String b1 = stores(dir, "b", caB, caB).getFileName().toString();

        pair("A+A tls", cfg(16701, 16702, dir, a1), cfg(16702, 16701, dir, a2));
        pair("A+B tls", cfg(16711, 16712, dir, a1), cfg(16712, 16711, dir, b1));
        pair("A tls + plain", cfg(16721, 16722, dir, a1), cfg(16722, 16721, dir, null));

        pair("A tls with missing key store + plain", cfg(16741, 16742, dir, "no-such-key.p12|no-such-trust.p12"),
                cfg(16742, 16741, dir, null));

        HazelcastInstance x = null;
        try {
            x = Hazelcast.newHazelcastInstance(cfg(16731, 16739, dir, a1));
            raw("raw client A-cert", 16731, dir, a2, caA);
            raw("raw client B-cert", 16731, dir, b1, caA);
            raw("raw client no-cert", 16731, dir, null, caA);
        } catch (Throwable t) {
            System.out.println("PROBE single A member: threw " + t);
        } finally {
            if (x != null) x.shutdown();
        }
    }
}
