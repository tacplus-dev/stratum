// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Slice identity: the directory name IS the slice id, and its epoch interval is
 * derivable from that name alone.
 *
 * That is the whole reason pruning is free. A query knows its own epoch bounds
 * before it opens anything, so a slice whose interval cannot intersect them is
 * skipped by parsing a string off a directory listing — no manifest is read, no
 * reader is opened, no file is touched. A manifest would have to be opened to be
 * consulted, which is the cost the layout exists to avoid.
 *
 * Granularity is calendar-monthly, UTC. It is deliberately a naming decision and
 * nothing else: moving to weekly or daily changes this class and no other. The
 * design records that the granularity is unmeasured, so keeping the blast radius
 * to one file is the point.
 */
public final class WarmSlices {

    private WarmSlices() {}

    /** The slice a row with this store epoch belongs to, e.g. "2026-08". */
    public static String sliceIdFor(long epochMs) {
        return YearMonth.from(Instant.ofEpochMilli(epochMs).atOffset(ZoneOffset.UTC)).toString();
    }

    /** Inclusive start of the slice's epoch interval. */
    public static long startMs(String sliceId) {
        return YearMonth.parse(sliceId).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    /** Exclusive end of the slice's epoch interval. */
    public static long endMsExclusive(String sliceId) {
        return YearMonth.parse(sliceId).plusMonths(1).atDay(1)
                .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    /** True when the name parses as a slice id. Anything else in the dataset directory is not ours. */
    public static boolean isSliceId(String name) {
        try {
            YearMonth.parse(name);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    /**
     * True when the slice's epoch interval overlaps [fromMs, toMsExclusive).
     * A query outside every slice's interval opens zero readers.
     */
    public static boolean intersects(String sliceId, long fromMs, long toMsExclusive) {
        return startMs(sliceId) < toMsExclusive && endMsExclusive(sliceId) > fromMs;
    }

    /**
     * The dataset's slice ids, oldest first. Lexicographic order on "yyyy-MM" is
     * chronological order, so no parse is needed to sort them.
     */
    public static List<String> list(Path datasetRoot) throws IOException {
        List<String> ids = new ArrayList<>();
        if (!Files.isDirectory(datasetRoot)) {
            return ids;
        }
        try (DirectoryStream<Path> children = Files.newDirectoryStream(datasetRoot)) {
            for (Path child : children) {
                String name = child.getFileName().toString();
                if (Files.isDirectory(child) && isSliceId(name)) {
                    ids.add(name);
                }
            }
        }
        Collections.sort(ids);
        return ids;
    }
}
