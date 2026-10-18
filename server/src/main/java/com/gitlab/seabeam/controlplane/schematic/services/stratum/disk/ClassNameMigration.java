// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.disk;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.Message;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.StratumEventStores;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint.Checkpoint;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.diskstore.FsUtil;

/**
 * One-time rewrite of a volume written before the durable format named rows by
 * their protobuf full name.
 *
 * <p>Checkpoint dumps used to carry a {@code class} property and record indexes a
 * {@code _class} stored field, each holding the value's generated Java class —
 * under {@code com.gitlab.secondstarcavalier} for data written before the
 * 2026-09-28 org rename, {@code com.gitlab.seabeam} after it. This rewrites both
 * into the {@code type} / {@code _type} full name the readers now expect. It runs
 * at a writer's boot, before {@link RecordDisk} opens the indexes, and is a no-op
 * on a volume that holds no legacy row, so it can run on every boot until it is
 * deleted.
 *
 * <p>The class names are matched against the generated record types, never
 * loaded: a legacy name that is not one of this build's record types FAILS the
 * boot, the same posture the readers take, because dropping or guessing at a row
 * of the durable baseline produces a store that is silently incomplete.
 *
 * <p>Each rewrite is atomic on its own — a dump by tmp-then-rename, an index by
 * one Lucene commit — so a crash mid-migration leaves every file either old or
 * new, and the next boot finishes the rest.
 */
public final class ClassNameMigration {

    private static final Logger log = LoggerFactory.getLogger(ClassNameMigration.class);

    private static final String LEGACY_ORG = "com.gitlab.secondstarcavalier.";
    private static final String ORG = "com.gitlab.seabeam.";
    /** The checkpoint line property that held the Java class name. */
    static final String LEGACY_CHECKPOINT_PROPERTY = "class";
    /** The record-index stored field that held the Java class name. */
    static final String LEGACY_INDEX_FIELD = "_class";

    private ClassNameMigration() {}

    /**
     * Rewrites every legacy checkpoint dump and record index under {@code dataRoot}.
     *
     * @param mapNames every record map name, i.e. {@code StratumEventStores.mapNames()}
     */
    public static void run(Path dataRoot, List<String> mapNames) throws IOException {
        // Generated Java class name -> protobuf full name, for this build's record types.
        Map<String, String> types = new HashMap<>();
        for (String name : mapNames) {
            Message defaultInstance = StratumEventStores.defaultInstance(name);
            types.put(defaultInstance.getClass().getName(), name);
        }
        int dumps = migrateCheckpoints(dataRoot.resolve(Checkpoint.CHECKPOINT_SUBDIR), types);
        int rows = 0;
        for (String name : mapNames) {
            rows += migrateIndex(dataRoot.resolve(RecordDisk.RECORDS_SUBDIR).resolve(name), types);
        }
        if (dumps > 0 || rows > 0) {
            log.info("durable format: rewrote {} checkpoint dump(s) and {} record-index row(s)"
                    + " from Java class names to protobuf full names", dumps, rows);
        }
    }

    private static String typeOf(String className, Map<String, String> types, Path where) {
        String current = className.startsWith(LEGACY_ORG)
                ? ORG + className.substring(LEGACY_ORG.length())
                : className;
        String type = types.get(current);
        if (type == null) {
            throw new IllegalStateException(where + " holds rows of " + className
                    + ", which is not a record type this build declares; refusing to migrate"
                    + " a partial baseline");
        }
        return type;
    }

    /** Rewrites every committed dump carrying a class name; returns how many were rewritten. */
    private static int migrateCheckpoints(Path dir, Map<String, String> types) throws IOException {
        if (!Files.isDirectory(dir)) {
            return 0;
        }
        List<Path> dumps;
        // A committed dump is named by its all-digit timestamp; "current" is the
        // pointer and anything else is an uncommitted stage nothing reads.
        try (Stream<Path> files = Files.list(dir)) {
            dumps = files.filter(p -> p.getFileName().toString().matches("\\d+")).toList();
        }
        int rewritten = 0;
        for (Path dump : dumps) {
            List<String> lines = Files.readAllLines(dump, StandardCharsets.UTF_8);
            StringBuilder buf = new StringBuilder();
            boolean legacy = false;
            for (String line : lines) {
                if (line.isBlank()) {
                    continue;
                }
                JsonObject obj = JsonParser.parseString(line).getAsJsonObject();
                if (obj.has(LEGACY_CHECKPOINT_PROPERTY)) {
                    legacy = true;
                    JsonObject out = new JsonObject();
                    out.add("map", obj.get("map"));
                    out.add("key", obj.get("key"));
                    out.addProperty("type",
                            typeOf(obj.get(LEGACY_CHECKPOINT_PROPERTY).getAsString(), types, dump));
                    out.add("proto", obj.get("proto"));
                    obj = out;
                }
                buf.append(obj).append('\n');
            }
            if (legacy) {
                Path staged = FsUtil.writeTemp(dump, buf.toString().getBytes(StandardCharsets.UTF_8), 0600);
                FsUtil.promote(staged, dump);
                rewritten++;
            }
        }
        return rewritten;
    }

    /** Rewrites every row of one index carrying a class name; returns how many were rewritten. */
    private static int migrateIndex(Path dir, Map<String, String> types) throws IOException {
        if (!Files.isDirectory(dir)) {
            return 0;
        }
        try (FSDirectory directory = FSDirectory.open(dir)) {
            if (!DirectoryReader.indexExists(directory)) {
                return 0;
            }
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                int rewritten = 0;
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    StoredFields stored = reader.storedFields();
                    Bits live = MultiBits.getLiveDocs(reader);
                    for (int i = 0; i < reader.maxDoc(); i++) {
                        if (live != null && !live.get(i)) {
                            continue;
                        }
                        Document doc = stored.document(i, Set.of(RecordIndex.KEY_FIELD,
                                LEGACY_INDEX_FIELD, RecordIndex.PROTO_FIELD));
                        String className = doc.get(LEGACY_INDEX_FIELD);
                        if (className == null) {
                            continue;
                        }
                        String key = doc.get(RecordIndex.KEY_FIELD);
                        writer.updateDocument(new Term(RecordIndex.KEY_FIELD, key),
                                RecordIndex.document(key, typeOf(className, types, dir),
                                        doc.getBinaryValue(RecordIndex.PROTO_FIELD)));
                        rewritten++;
                    }
                }
                if (rewritten > 0) {
                    writer.commit();
                }
                return rewritten;
            }
        }
    }
}
