// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.util.ClassicVersionUtil;
import ch.qos.logback.core.util.CoreVersionUtil;
import org.junit.jupiter.api.Test;

/**
 * The logback on the server's classpath is at or above 1.5.34. 1.5.29 carried
 * GHSA-p47f-322f-whfh (LOW, deserialization of untrusted data, fixed in 1.5.33)
 * and GHSA-jhq6-gfmj-v8fx (LOW, object injection through
 * HardenedObjectInputStream, fixed in 1.5.34) (#12). Both artifacts are checked:
 * classic and core are pinned separately and must move together. A dependency
 * change that drags logback back fails here rather than in the next advisory scan.
 */
class LogbackVersionFloorTest {

    private static final int[] FLOOR = {1, 5, 34};

    @Test
    void logbackCoreIsAtLeastTheFixedRelease() {
        assertAtLeastFloor("logback-core", CoreVersionUtil.getCoreVersionBySelfDeclaredProperties());
    }

    @Test
    void logbackClassicIsAtLeastTheFixedRelease() {
        assertAtLeastFloor("logback-classic", ClassicVersionUtil.getVersionBySelfDeclaredProperties());
    }

    private static void assertAtLeastFloor(String artifact, String version) {
        assertNotNull(version, artifact + " declares no version");
        assertTrue(compare(parse(version), FLOOR) >= 0,
                artifact + " " + version + " is below 1.5.34, the first 1.5 release without the 1.5.29 advisories");
    }

    private static int[] parse(String version) {
        String[] parts = version.split("[.-]");
        int[] numbers = new int[3];
        for (int i = 0; i < numbers.length && i < parts.length; i++) {
            numbers[i] = Integer.parseInt(parts[i]);
        }
        return numbers;
    }

    private static int compare(int[] a, int[] b) {
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return Integer.compare(a[i], b[i]);
            }
        }
        return 0;
    }
}
