// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.hazelcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;

/**
 * Hazelcast 5.7.0 shades jackson-databind 2.21.2 and 3.1.2 and jackson-core 2.21.2
 * and 3.1.2 into its jar (com.hazelcast.shaded.*), with HIGH advisories, fixed only
 * in jackson releases no Hazelcast carries yet (#28). They are accepted, with an
 * expiry, in security-exceptions.yaml (the build's security job) and
 * .trivyignore.yaml (`just audit-jar`) because none of the vulnerable code can run in a
 * Stratum member; decisions/2026-11-03-shaded-jackson-in-hazelcast.md has the
 * reasoning. This test is that reasoning, kept:
 *
 * <ul>
 * <li>no class outside the shaded jackson packages (Hazelcast's own, jackson-jr,
 *     the other shaded libraries) references either shaded databind or the shaded
 *     jackson 2.x core, so they are dead code in the jar;</li>
 * <li>nothing outside jackson-core 3 itself reaches the code its advisories are in:
 *     the non-blocking parser (CVE-2026-68494), the DataInput parser
 *     (CVE-2026-89425) and NumberInput.looksLikeValidNumber (CVE-2026-89407);
 *     Hazelcast parses JSON (HazelcastJsonValue metadata and queries, Jet's JSON
 *     sources) only with the blocking parsers over byte[], String, Reader and
 *     InputStream;</li>
 * <li>Stratum never touches the shaded packages or HazelcastJsonValue, and its
 *     members keep every JSON-speaking surface off: the REST endpoints (both
 *     APIs), memcache, Jet (and with it SQL), user code deployment and the
 *     advanced network's extra endpoints.</li>
 * </ul>
 *
 * A Hazelcast bump that starts using any of it, or a config change that turns a
 * surface on, fails here; the exception then has to be re-examined, not renewed.
 */
class ShadedJacksonReachabilityTest {

    private static final String SHADED = "com/hazelcast/shaded/";

    /** The shaded jackson 2.x (core and databind) and both databinds: dead code. */
    private static final List<String> DEAD = List.of(
            SHADED + "com/fasterxml/jackson/",
            SHADED + "tools/jackson/databind/");

    private static final String CORE3 = SHADED + "tools/jackson/core/";

    /**
     * Where the jackson-core 3 advisories live, and how a caller of the shaded
     * jackson-core 3 would reach them.
     */
    private static final List<String> CORE3_ADVISORY_ENTRIES = List.of(
            "createNonBlockingByteArrayParser",
            "createNonBlockingByteBufferParser",
            CORE3 + "json/async/",
            "Ljava/io/DataInput;)L" + CORE3 + "JsonParser;",
            "UTF8DataInputJsonParser",
            "looksLikeValidNumber");

    @Test
    void nothingInHazelcastReachesTheShadedDatabindsOrTheAdvisoryCode() throws Exception {
        List<String> found = new ArrayList<>();
        int scanned = 0;
        int shaded = 0;
        try (ZipFile jar = new ZipFile(codeSource(Hazelcast.class).toFile())) {
            Enumeration<? extends ZipEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                ZipEntry e = entries.nextElement();
                String name = e.getName().replaceFirst("^META-INF/versions/\\d+/", "");
                if (!name.endsWith(".class")) {
                    continue;
                }
                if (within(name, DEAD)) {
                    shaded++;
                    continue;
                }
                String pool;
                try (InputStream in = jar.getInputStream(e)) {
                    pool = new String(in.readAllBytes(), StandardCharsets.ISO_8859_1);
                }
                scanned++;
                for (String dead : DEAD) {
                    if (references(pool, dead)) {
                        found.add(name + " -> " + dead);
                    }
                }
                if (!name.startsWith(CORE3) && pool.contains(CORE3)) {
                    for (String entry : CORE3_ADVISORY_ENTRIES) {
                        if (pool.contains(entry)) {
                            found.add(name + " -> " + entry);
                        }
                    }
                }
            }
        }
        assertTrue(scanned > 5_000, "scanned only " + scanned + " classes: is this the Hazelcast jar?");
        assertTrue(shaded > 0, "no shaded jackson 2.x or databind left in the Hazelcast jar: "
                + "drop their entries from security-exceptions.yaml, .trivyignore.yaml and this test's DEAD list");
        assertEquals(List.of(), found, "Hazelcast reaches shaded jackson code its advisories are in (#28)");
    }

    @Test
    void stratumNeverTouchesShadedJacksonOrJsonValues() throws Exception {
        Path classes = codeSource(StratumMapConfig.class);
        assertTrue(Files.isDirectory(classes), "want server/target/classes, got " + classes);
        List<String> found = new ArrayList<>();
        try (Stream<Path> files = Files.walk(classes)) {
            for (Path p : (Iterable<Path>) files.filter(f -> f.toString().endsWith(".class"))::iterator) {
                String pool = new String(Files.readAllBytes(p), StandardCharsets.ISO_8859_1);
                for (String ref : List.of(SHADED, "com/hazelcast/core/HazelcastJsonValue")) {
                    if (references(pool, ref)) {
                        found.add(classes.relativize(p) + " -> " + ref);
                    }
                }
            }
        }
        assertEquals(List.of(), found);
    }

    @Test
    @SuppressWarnings("removal") // the classic REST API can be enabled until it is removed
    void membersKeepEveryJsonSurfaceOff() {
        Config cfg = StratumMapConfig.build("t1", MemberNetwork.alone(new InetSocketAddress("127.0.0.1", 5701)), 1, null);
        assertFalse(cfg.getNetworkConfig().getRestApiConfig().isEnabled(), "REST API (classic) enabled");
        assertFalse(cfg.getRestConfig().isEnabled(), "REST API enabled");
        assertFalse(cfg.getNetworkConfig().getMemcacheProtocolConfig().isEnabled(), "memcache enabled");
        assertFalse(cfg.getAdvancedNetworkConfig().isEnabled(), "advanced network (REST/memcache endpoints) enabled");
        assertFalse(cfg.getJetConfig().isEnabled(), "Jet (JSON sources, SQL JSON functions) enabled");
        assertFalse(cfg.getUserCodeDeploymentConfig().isEnabled(), "user code deployment enabled");
        assertFalse(cfg.getManagementCenterConfig().isScriptingEnabled(), "Management Center scripting enabled");
    }

    private static boolean within(String name, List<String> prefixes) {
        return prefixes.stream().anyMatch(name::startsWith);
    }

    /** A class names another in its constant pool, slashed or (for reflection) dotted. */
    private static boolean references(String pool, String prefix) {
        return pool.contains(prefix) || pool.contains(prefix.replace('/', '.'));
    }

    private static Path codeSource(Class<?> c) throws URISyntaxException, IOException {
        return Path.of(c.getProtectionDomain().getCodeSource().getLocation().toURI());
    }
}
