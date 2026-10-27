// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.hazelcast;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.version.MemberVersion;

/**
 * The Hazelcast on the server's classpath is at or above 5.7.0: 5.5.0 carries
 * GHSA-6v25-8wq6-xq4j (CRITICAL, client-to-member memory access) and
 * GHSA-w294-6q5q-53p8 (HIGH, IMap Predicates authorization bypass), and 5.7.0 is
 * the first release with both fixes (#10). A dependency change that drags it back
 * below fails here rather than in the next advisory scan.
 */
class HazelcastVersionFloorTest {

    private static final MemberVersion FLOOR = MemberVersion.of(5, 7, 0);

    @Test
    void hazelcastIsAtLeastTheFixedRelease() {
        String version = BuildInfoProvider.getBuildInfo().getVersion();
        MemberVersion resolved = MemberVersion.of(version);
        assertTrue(resolved.compareTo(FLOOR) >= 0,
                "Hazelcast " + version + " is below " + FLOOR + ", the first release without the 5.5.0 advisories");
    }
}
