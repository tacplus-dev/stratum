// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.StratumEventStores;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.FsUtil;

/**
 * The pod's full dump of every row of every record type, and the pointer that
 * names the newest complete one.
 *
 * <h2>Layout</h2>
 * <pre>
 *   &lt;dataRoot&gt;/checkpoint/&lt;ts&gt;          one dump, NDJSON, one line per row
 *   &lt;dataRoot&gt;/checkpoint/current       a text file holding one &lt;ts&gt;
 * </pre>
 *
 * <p>{@code current} is a POINTER FILE, not a symlink. Both are updated by the
 * same atomic rename ({@link FsUtil#atomicWrite}), so they are equally safe; a
 * regular file is simply readable by anything that can read the directory,
 * including an offline collector on a volume snapshot where a symlink's target
 * may not have come along.
 *
 * <h2>Why tmp-then-rename is the whole atomicity story</h2>
 * The dump is staged by {@link FsUtil#writeTemp} — a fresh sibling temp,
 * fsync'd — and committed by {@link FsUtil#promote}, one atomic rename. A crash
 * before the rename leaves a stray temp and the PREVIOUS checkpoint still
 * pointed at; a crash between the rename and the pointer update leaves a complete
 * dump nothing points at, and the previous checkpoint still pointed at. Neither
 * is a partial checkpoint, which is what lets {@link DailyCheckpoint} treat the
 * rename as the point after which the redo logs may be truncated.
 *
 * <p>The staging name is {@code <ts>.tmp.<pid>} rather than a bare {@code <ts>.tmp}
 * because that is what {@code FsUtil} writes: the pid keeps two processes sharing
 * a volume from colliding on the staging name, and the temp is in the same
 * directory, so the commit rename stays on one filesystem and is therefore atomic.
 *
 * <h2>The line format</h2>
 * One JSON object per line: the Hazelcast map name, the record key, the value's
 * protobuf full name ({@code type}, e.g. {@code accountingrecord.AccountingRecord})
 * and the value's protobuf bytes, base64. The load leg resolves the full name
 * through the generated registry ({@link StratumEventStores#defaultInstance}), so
 * the row comes back as the concrete type the maps and the query plane expect
 * rather than as a DynamicMessage they could not serve. A Java class name is never
 * written: it would tie the durable format to the generated code's package layout.
 */
public final class Checkpoint {

    /** Where checkpoints live under the pod's volume. */
    public static final String CHECKPOINT_SUBDIR = "checkpoint";
    /** The pointer file naming the newest complete checkpoint. */
    public static final String CURRENT = "current";

    /** One dumped row. */
    public static final class Row {
        /** The Hazelcast map name, which is the record type's name. */
        public final String mapName;
        public final String key;
        public final Message value;

        public Row(String mapName, String key, Message value) {
            this.mapName = mapName;
            this.key = key;
            this.value = value;
        }
    }

    private final Path dir;
    /** Proto full name -> that type's parser. Resolved once; a dump holds one type many times. */
    private final Map<String, Parser<? extends Message>> parsers = new ConcurrentHashMap<>();

    public Checkpoint(Path dataRoot) {
        this.dir = dataRoot.resolve(CHECKPOINT_SUBDIR);
    }

    /** The checkpoint directory. */
    public Path dir() {
        return dir;
    }

    /**
     * Writes one checkpoint and repoints {@code current} at it.
     *
     * @param ts   the checkpoint's name — the sweep's own clock reading, so the
     *             directory sorts oldest-first by name
     * @param rows every row of every type that survived the purge step
     * @return the committed checkpoint's path
     */
    public Path write(long ts, Iterable<Row> rows) {
        StringBuilder buf = new StringBuilder();
        for (Row row : rows) {
            JsonObject line = new JsonObject();
            line.addProperty("map", row.mapName);
            line.addProperty("key", row.key);
            line.addProperty("type", row.value.getDescriptorForType().getFullName());
            line.addProperty("proto", Base64.getEncoder().encodeToString(row.value.toByteArray()));
            buf.append(line).append('\n');
        }
        Path target = dir.resolve(Long.toString(ts));
        Path staged = FsUtil.writeTemp(target, buf.toString().getBytes(StandardCharsets.UTF_8), 0600);
        FsUtil.promote(staged, target);
        // Only now: a pointer written first would name a dump that does not exist
        // yet, which is the one state the rename ordering exists to rule out.
        FsUtil.atomicWrite(dir.resolve(CURRENT), target.getFileName().toString()
                .getBytes(StandardCharsets.UTF_8), 0600);
        return target;
    }

    /**
     * The checkpoint {@code current} names, or null when this pod has never
     * completed one — a first boot, or a boot after a volume was replaced.
     */
    public Path current() throws IOException {
        Path pointer = dir.resolve(CURRENT);
        if (!Files.exists(pointer)) {
            return null;
        }
        String name = new String(Files.readAllBytes(pointer), StandardCharsets.UTF_8).trim();
        if (name.isEmpty()) {
            return null;
        }
        Path target = dir.resolve(name);
        return Files.exists(target) ? target : null;
    }

    /**
     * How many rows the checkpoint {@code current} names holds; 0 when this pod has
     * never completed one.
     *
     * <p>This is the size of THIS POD'S DURABLE BASELINE, and it is what
     * {@code EventStore.restoreHotTier} reports to the hot-restore ledger alongside
     * the redo records it read. A pod whose redo log is empty because
     * {@link DailyCheckpoint} truncated it to the dump point has lost nothing — the
     * rows moved from the tail into the file this counts — and counting only the
     * tail would report that pod as holding nothing durable at all.
     *
     * <p>Counts lines rather than parsing them, deliberately: the caller only needs
     * to know whether this volume holds a durable copy, and it asks on EVERY boot,
     * including the ones where {@code loadCheckpoint} skips the dump because the
     * peers already hold the data. Parsing a dump that is not going to be loaded
     * would make every rolling restart pay the cold-start cost to answer a
     * yes/no question.
     *
     * <p>Trusting the line count without parsing is sound for exactly the reason
     * {@link #write} is atomic: {@code current} is only ever repointed after the
     * dump's commit rename, so a checkpoint it names is a complete dump by
     * construction. A dump whose line CONTENT is unreadable still fails
     * {@link #load}, which is the path where the bytes are the answer.
     */
    public long rowCount() throws IOException {
        Path target = current();
        if (target == null) {
            return 0;
        }
        try (Stream<String> lines = Files.lines(target, StandardCharsets.UTF_8)) {
            return lines.filter(line -> !line.isBlank()).count();
        }
    }

    /**
     * Every row of the checkpoint {@code current} names; empty when there is none.
     *
     * <p>A line whose type cannot be resolved or whose bytes do not parse FAILS
     * the load rather than being skipped: a checkpoint is the baseline a cold start
     * is built on, and quietly dropping rows out of it produces a store that is
     * silently incomplete — the failure the whole cold-start gate exists to refuse.
     */
    public List<Row> load() throws IOException {
        Path target = current();
        if (target == null) {
            return List.of();
        }
        List<Row> rows = new ArrayList<>();
        for (String line : Files.readAllLines(target, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            JsonObject obj = JsonParser.parseString(line).getAsJsonObject();
            String type = obj.get("type").getAsString();
            byte[] proto = Base64.getDecoder().decode(obj.get("proto").getAsString());
            rows.add(new Row(obj.get("map").getAsString(), obj.get("key").getAsString(),
                    parserFor(type).parseFrom(proto)));
        }
        return rows;
    }

    private Parser<? extends Message> parserFor(String type) {
        return parsers.computeIfAbsent(type, t -> {
            Message defaultInstance = StratumEventStores.defaultInstance(t);
            if (defaultInstance == null) {
                throw new IllegalStateException("checkpoint holds rows of " + t
                        + ", which is not a record type this build declares;"
                        + " refusing to load a partial baseline");
            }
            return defaultInstance.getParserForType();
        });
    }
}
