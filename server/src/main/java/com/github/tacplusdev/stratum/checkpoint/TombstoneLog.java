// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.checkpoint;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import com.google.gson.JsonObject;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

/**
 * The pod's write-only record of every row the purge step removed.
 *
 * <p>One JSON line per purged row under {@code <dataRoot>/tombstones/<yyyy-MM-dd>.jsonl}.
 * Nothing in the platform reads it: it is a historical trace that can be shipped
 * offline, and it is the ONLY trace left once the row is gone from memory, from
 * Lucene and — after the next checkpoint — from the redo log. That is why the
 * append happens BEFORE the removal in {@link DailyCheckpoint}: a crash between
 * the two leaves a line for a row that is still present, which the next sweep
 * writes again and which is harmless, while the other order can lose the row
 * without a trace.
 *
 * <p>The payload is the full record as protobuf JSON, so the line is
 * self-describing without the .proto. Tenant and principal are lifted out of it
 * into their own keys as well, because they are what an offline collector
 * partitions by and digging them back out of a payload whose field name differs
 * per type is exactly the per-type knowledge this package does not have.
 *
 * <p>The file is opened, appended and closed per line. A checkpoint purges a
 * bounded number of rows once a day, off every request path, so the open cost is
 * irrelevant next to not holding a descriptor open for the life of the process.
 */
public final class TombstoneLog {

    /** Where tombstone logs live under the pod's volume. */
    public static final String TOMBSTONE_SUBDIR = "tombstones";

    private static final DateTimeFormatter DAY =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    private final Path dir;
    private final JsonFormat.Printer printer = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .preservingProtoFieldNames();

    public TombstoneLog(Path dataRoot) {
        this.dir = dataRoot.resolve(TOMBSTONE_SUBDIR);
    }

    /**
     * The file a row purged at {@code purgedAtMs} is appended to. UTC, so a pod's
     * day boundary does not move with the node's timezone.
     */
    public Path fileFor(long purgedAtMs) {
        return dir.resolve(DAY.format(Instant.ofEpochMilli(purgedAtMs)) + ".jsonl");
    }

    /**
     * Appends one purged row.
     *
     * @param recordType the Hazelcast map name, which is the record type's name
     * @param key        the row's record key
     * @param row        the full record, as it stood when it was purged
     * @param purgedAtMs when this sweep removed it
     */
    public void append(String recordType, String key, Message row, long purgedAtMs)
            throws IOException {
        JsonObject line = new JsonObject();
        line.addProperty("record_type", recordType);
        line.addProperty("key", key);
        line.addProperty("tenant",
                RecordAnnotations.stringAt(row, RecordAnnotations.storeNamespace(row.getDescriptorForType())));
        line.addProperty("principal",
                RecordAnnotations.stringAt(row, RecordAnnotations.storePrincipal(row.getDescriptorForType())));
        line.addProperty("delete_store_epoch_ms", RecordAnnotations.storeEpochMs(row));
        line.addProperty("purged_at_ms", purgedAtMs);
        line.addProperty("payload", printer.print(row));

        Files.createDirectories(dir);
        Files.write(fileFor(purgedAtMs),
                (line.toString() + "\n").getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
