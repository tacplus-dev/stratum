// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Every core OpenTelemetry module on the server's classpath is at or above
 * 1.67.0. google-cloud-storage brings 1.62.0, whose W3CTraceContextPropagator
 * spends quadratic CPU on a crafted tracestate header (GHSA-ccrj-gw8r-854m,
 * MODERATE, fixed in 1.67.0) (#19). The modules are released together and the
 * opentelemetry-bom pins them together, so each one is checked: a dependency
 * change that drags any of them back fails here rather than in the next
 * advisory scan.
 */
class OpenTelemetryVersionFloorTest {

    private static final int[] FLOOR = {1, 67, 0};

    /**
     * Each core module google-cloud-storage resolves ships a version.properties
     * in its own package. The model jar shades them in, so the jars' manifests
     * are gone but these resources are not.
     */
    private static final List<String> MODULE_PACKAGES = List.of(
            "io/opentelemetry/api",
            "io/opentelemetry/common",
            "io/opentelemetry/context",
            "io/opentelemetry/sdk",
            "io/opentelemetry/sdk/common",
            "io/opentelemetry/sdk/trace",
            "io/opentelemetry/sdk/logs",
            "io/opentelemetry/sdk/metrics",
            "io/opentelemetry/sdk/autoconfigure/spi");

    @Test
    void everyCoreModuleIsAtLeastTheFixedRelease() throws IOException {
        for (String pkg : MODULE_PACKAGES) {
            String resource = pkg + "/version.properties";
            Properties properties = new Properties();
            try (InputStream in = getClass().getClassLoader().getResourceAsStream(resource)) {
                assertNotNull(in, resource + " is not on the classpath");
                properties.load(in);
            }
            String version = properties.getProperty("sdk.version");
            assertNotNull(version, resource + " declares no sdk.version");
            assertTrue(compare(parse(version), FLOOR) >= 0,
                    "opentelemetry " + version + " (" + pkg + ") is below 1.67.0,"
                            + " the first release without GHSA-ccrj-gw8r-854m");
        }
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
