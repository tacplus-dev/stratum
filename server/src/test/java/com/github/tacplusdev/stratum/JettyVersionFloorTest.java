// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.eclipse.jetty.util.Jetty;
import org.junit.jupiter.api.Test;

/**
 * The Jetty on the server's classpath (brought by the generated chassis's Javalin)
 * is at or above 12.1.10. Javalin 6.7.0 brought Jetty 11.0.25, which carries
 * GHSA-355h-qmc2-wpwf (HIGH, chunked-extension request smuggling) and
 * GHSA-2fvj-hgj9-j2gr (HIGH, Digest auth bypass); Jetty 11 is out of community
 * support and some of its advisories are fixed only on 12. 12.1.10 is the first
 * 12.1 release with every fix (#11). A dependency change that drags Jetty back
 * fails here rather than in the next advisory scan.
 */
class JettyVersionFloorTest {

    private static final int[] FLOOR = {12, 1, 10};

    @Test
    void jettyIsAtLeastTheFixedRelease() {
        String version = Jetty.VERSION;
        assertTrue(compare(parse(version), FLOOR) >= 0,
                "Jetty " + version + " is below 12.1.10, the first 12.1 release without the 11.0.25 advisories");
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
