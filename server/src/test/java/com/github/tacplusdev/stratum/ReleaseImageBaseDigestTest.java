// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Every {@code FROM} of the release Dockerfile names its image by digest,
 * {@code <image>:<tag>@sha256:<digest>}, never by a tag alone: a floating tag lets a
 * rebuild of the same commit ship a different, unscanned base (#45; tacplus-dev/roadmap
 * decisions/security-gates-ship-only-2026-11-23.md, point 2). A {@code FROM} of an
 * earlier stage of the same Dockerfile is no image, so it needs none. Nightly
 * maintenance moves the digest (tacplus-dev/looper#234).
 */
class ReleaseImageBaseDigestTest {

    private static final Pattern FROM = Pattern.compile(
            "^FROM\\s+(?:--\\S+\\s+)*(\\S+)(?:\\s+AS\\s+(\\S+))?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PINNED = Pattern.compile("^[^@\\s]+:[^@\\s/]+@sha256:[0-9a-f]{64}$");

    @Test
    void everyFromIsPinnedByDigest() throws IOException {
        List<String> froms = new ArrayList<>();
        Set<String> stages = new HashSet<>();
        for (String line : Files.readAllLines(dockerfile())) {
            Matcher m = FROM.matcher(line.strip());
            if (!m.matches()) {
                continue;
            }
            String image = m.group(1);
            if (!stages.contains(image.toLowerCase(Locale.ROOT))) {
                froms.add(image);
            }
            if (m.group(2) != null) {
                stages.add(m.group(2).toLowerCase(Locale.ROOT));
            }
        }
        assertFalse(froms.isEmpty(), "the release Dockerfile has no FROM");
        for (String image : froms) {
            assertTrue(PINNED.matcher(image).matches(),
                    "FROM " + image + " is not pinned by digest: name it <image>:<tag>@sha256:<digest> (#45)");
        }
    }

    /** The repository's Dockerfile: the test runs in server/, the module's directory. */
    private static Path dockerfile() {
        for (Path dir = Path.of("").toAbsolutePath(); dir != null; dir = dir.getParent()) {
            Path candidate = dir.resolve("Dockerfile");
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        throw new AssertionError("no Dockerfile above " + Path.of("").toAbsolutePath());
    }
}
