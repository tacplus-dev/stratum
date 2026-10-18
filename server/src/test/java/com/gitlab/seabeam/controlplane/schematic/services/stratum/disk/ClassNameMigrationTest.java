// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.disk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.StratumEventStores;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.checkpoint.Checkpoint;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor;

/**
 * The durable format names rows by protobuf full name; a volume written when it
 * named them by Java class is rewritten once, and a name that is not a record type
 * is refused on both paths.
 */
class ClassNameMigrationTest {

    private static final String MAP = "agentsensor.AgentEvent";
    private static final String CLASS =
            "com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor$AgentEvent";
    private static final String PRE_RENAME_CLASS =
            "com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor$AgentEvent";

    private static Agentsensor.AgentEvent event(String id) {
        return Agentsensor.AgentEvent.newBuilder().setEventId(id).setStoreEpochMs(42).build();
    }

    /** Writes a dump in the pre-migration line format and points {@code current} at it. */
    private static Path legacyDump(Path dataRoot, String className, Agentsensor.AgentEvent e) throws IOException {
        Path dir = dataRoot.resolve(Checkpoint.CHECKPOINT_SUBDIR);
        Files.createDirectories(dir);
        Path dump = dir.resolve("1000");
        Files.writeString(dump, "{\"map\":\"" + MAP + "\",\"key\":\"" + e.getEventId()
                + "\",\"class\":\"" + className + "\",\"proto\":\""
                + Base64.getEncoder().encodeToString(e.toByteArray()) + "\"}\n");
        Files.writeString(dir.resolve(Checkpoint.CURRENT), "1000");
        return dump;
    }

    private static void migrate(Path dataRoot) throws IOException {
        ClassNameMigration.run(dataRoot, StratumEventStores.mapNames());
    }

    @Test
    void aPreRenameDumpRestoresAfterMigration(@TempDir Path dataRoot) throws IOException {
        Path dump = legacyDump(dataRoot, PRE_RENAME_CLASS, event("e1"));

        migrate(dataRoot);

        List<Checkpoint.Row> rows = new Checkpoint(dataRoot).load();
        assertEquals(1, rows.size());
        assertEquals(event("e1"), rows.get(0).value);
        String text = Files.readString(dump, StandardCharsets.UTF_8);
        assertTrue(text.contains("\"type\":\"" + MAP + "\""), text);
        assertFalse(text.contains("com.gitlab"), "no Java class name may survive the rewrite:\n" + text);
    }

    @Test
    void aPostRenameClassNameDumpRestoresAfterMigration(@TempDir Path dataRoot) throws IOException {
        legacyDump(dataRoot, CLASS, event("e1"));

        migrate(dataRoot);

        assertEquals(event("e1"), new Checkpoint(dataRoot).load().get(0).value);
    }

    @Test
    void aFullNameDumpRestoresAndNamesNoJavaClass(@TempDir Path dataRoot) throws IOException {
        Checkpoint checkpoint = new Checkpoint(dataRoot);
        Path dump = checkpoint.write(2000, List.of(new Checkpoint.Row(MAP, "e1", event("e1"))));
        String before = Files.readString(dump, StandardCharsets.UTF_8);

        migrate(dataRoot);

        assertEquals(before, Files.readString(dump, StandardCharsets.UTF_8), "nothing legacy to rewrite");
        assertTrue(before.contains("\"type\":\"" + MAP + "\""), before);
        assertFalse(before.contains("com.gitlab"), before);
        assertEquals(event("e1"), checkpoint.load().get(0).value);
    }

    @Test
    void anUnknownClassNameRefusesTheMigration(@TempDir Path dataRoot) throws IOException {
        Path dump = legacyDump(dataRoot, "com.example.NotARecord", event("e1"));
        String before = Files.readString(dump, StandardCharsets.UTF_8);

        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> migrate(dataRoot));
        assertTrue(refused.getMessage().contains("com.example.NotARecord"), refused.getMessage());
        assertEquals(before, Files.readString(dump, StandardCharsets.UTF_8), "a refused dump is left untouched");
    }

    @Test
    void anUnknownFullNameRefusesTheLoad(@TempDir Path dataRoot) throws IOException {
        Path dir = dataRoot.resolve(Checkpoint.CHECKPOINT_SUBDIR);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("1000"), "{\"map\":\"" + MAP + "\",\"key\":\"e1\",\"type\":\"nope.NotARecord\","
                + "\"proto\":\"" + Base64.getEncoder().encodeToString(event("e1").toByteArray()) + "\"}\n");
        Files.writeString(dir.resolve(Checkpoint.CURRENT), "1000");

        IllegalStateException refused =
                assertThrows(IllegalStateException.class, () -> new Checkpoint(dataRoot).load());
        assertTrue(refused.getMessage().contains("nope.NotARecord"), refused.getMessage());
    }

    @Test
    void aLegacyIndexRowIsReadableAfterMigration(@TempDir Path dataRoot) throws IOException {
        Path dir = dataRoot.resolve(RecordDisk.RECORDS_SUBDIR).resolve(MAP);
        Files.createDirectories(dir);
        try (FSDirectory directory = FSDirectory.open(dir);
             IndexWriter w = new IndexWriter(directory, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField(RecordIndex.KEY_FIELD, "e1", Field.Store.YES));
            doc.add(new StoredField(ClassNameMigration.LEGACY_INDEX_FIELD, PRE_RENAME_CLASS));
            doc.add(new StoredField(RecordIndex.PROTO_FIELD, new BytesRef(event("e1").toByteArray())));
            w.addDocument(doc);
            w.commit();
        }

        migrate(dataRoot);
        migrate(dataRoot); // a second boot finds nothing left to rewrite

        RecordIndex index = new RecordIndex(MAP, dir);
        try {
            assertEquals(1, index.size());
            assertEquals(event("e1"), index.get("e1"));
        } finally {
            index.close();
        }
    }

    @Test
    void anIndexRowWrittenNowNamesTheFullName(@TempDir Path dataRoot) throws IOException {
        Path dir = dataRoot.resolve(RecordDisk.RECORDS_SUBDIR).resolve(MAP);
        RecordIndex index = new RecordIndex(MAP, dir);
        try {
            index.put("e1", event("e1"));
        } finally {
            index.close();
        }
        try (FSDirectory directory = FSDirectory.open(dir);
             DirectoryReader reader = DirectoryReader.open(directory)) {
            Document doc = reader.storedFields().document(0);
            assertEquals(MAP, doc.get(RecordIndex.TYPE_FIELD));
            assertNull(doc.get(ClassNameMigration.LEGACY_INDEX_FIELD));
        }
    }
}
