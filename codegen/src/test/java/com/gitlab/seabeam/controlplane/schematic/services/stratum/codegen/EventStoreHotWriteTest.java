// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The generated hot write must be the durability path, not a bare put.
 *
 * {@code imap.put(key, value)} buys the second in-memory copy and nothing else:
 * no gate below the floor, and a redo append (if one were bolted on) would pick
 * its own second node rather than the one Hazelcast chose. Routing the write
 * through {@code HotWrites} is what makes the two logs that recorded a write the
 * two instances that hold it, by construction.
 */
class EventStoreHotWriteTest {

    private static String generate(Path dir, String eventBody) throws IOException {
        Path protoDir = dir.resolve("proto");
        Path vservices = protoDir.resolve("vservices");
        Files.createDirectories(vservices);
        Files.writeString(vservices.resolve("demo.proto"), """
                syntax = "proto3";
                package demo;
                option java_package = "com.example.demo";
                option java_outer_classname = "Demo";

                message DemoBus {
                  option (spec.service_event) = {label: "eventbus"};
                  DemoEvent event_stream = 1 [(spec.topic) = true];
                }

                message DemoEvent {
                """ + eventBody + """
                }
                """);
        List<EventTopicDiscovery.TopicEvent> events = EventTopicDiscovery.discover(protoDir);
        assertEquals(1, events.size(), "expected one topic event, got " + events);

        Path out = dir.resolve("out");
        EventStoreGenerator.generate(out, events);
        return Files.readString(out.resolve(
                "com/gitlab/seabeam/controlplane/schematic/services/stratum/StratumEventStores.java"));
    }

    private static final String EVENT = """
              option (spec.record) = {
                key: "demo_id"
                retention: {timestamp_field: "timestamp_ms" duration_seconds: 7776000}
              };
              string demo_id = 1;
              int64 timestamp_ms = 2;
              int64 store_epoch_ms = 3 [(spec.store_epoch) = true];
            """;

    @Test
    void theWriteGoesThroughTheQuorumGatedHotPath(@TempDir Path dir) throws IOException {
        String src = generate(dir, EVENT);

        assertFalse(src.contains(".put(e.getDemoId(), e);"),
                "the bare put has no gate and no durability leg:\n" + src);
        assertTrue(src.contains("hot.write(demoDemoEvent, \"demo.DemoEvent\", e.getDemoId(), e,"), src);
        assertTrue(src.contains("MIN_DURABLE_COPIES_DEMO_DEMO_EVENT"), src);
        assertTrue(src.contains("com.example.demo.Helpers.demoEventToJson(e)"), src);
        assertTrue(src.contains("e.getStoreEpochMs()"),
                "the redo record must carry the row's declared store epoch:\n" + src);
    }

    @Test
    void theDeclaredFloorIsEmittedAsAConstant(@TempDir Path dir) throws IOException {
        String src = generate(dir, EVENT);
        assertTrue(src.contains("public static final int MIN_DURABLE_COPIES_DEMO_DEMO_EVENT = 2;"), src);
    }

    @Test
    void aDeclaredFloorOfThreeReachesTheGeneratedConstant(@TempDir Path dir) throws IOException {
        String src = generate(dir, """
                  option (spec.record) = {key: "demo_id"
                    retention: {timestamp_field: "timestamp_ms" duration_seconds: 100}};
                  string demo_id = 1;
                  int64 timestamp_ms = 2;
                  string payload = 3 [(spec.quorum_write) = true, (spec.min_durable_copies) = 3];
                  int64 store_epoch_ms = 4 [(spec.store_epoch) = true];
                """);
        assertTrue(src.contains("public static final int MIN_DURABLE_COPIES_DEMO_DEMO_EVENT = 3;"), src);
    }

    /**
     * A record with no store epoch still gets the gate — the floor is about
     * in-memory replication and is independent of whether the type is replayable —
     * but has no epoch to stamp, so 0 is emitted rather than a substitute clock.
     */
    @Test
    void aRecordWithoutAStoreEpochStillGetsTheGate(@TempDir Path dir) throws IOException {
        String src = generate(dir, """
                  option (spec.record) = {key: "demo_id"
                    retention: {timestamp_field: "timestamp_ms" duration_seconds: 100}};
                  string demo_id = 1;
                  int64 timestamp_ms = 2;
                """);
        assertTrue(src.contains("hot.write(demoDemoEvent,"), src);
        assertTrue(src.contains("MIN_DURABLE_COPIES_DEMO_DEMO_EVENT"), src);
        assertTrue(src.contains(", 0L);"), "no store epoch means no epoch to stamp:\n" + src);
    }

    @Test
    void initTakesTheHotWritePath(@TempDir Path dir) throws IOException {
        String src = generate(dir, EVENT);
        assertTrue(src.contains("public void init(HazelcastInstance hz, HotWrites hot) {"), src);
        assertTrue(src.contains("import com.gitlab.seabeam.controlplane.schematic"
                + ".services.stratum.hot.HotWrites;"), src);
    }

    /**
     * StratumMapConfig needs the per-record floor keyed by Hazelcast map name — it only
     * has the name, not the record type, when it walks mapNames() to build the Config.
     */
    @Test
    void minDurableCopiesIsKeyedByMapName(@TempDir Path dir) throws IOException {
        String src = generate(dir, """
                  option (spec.record) = {key: "demo_id"
                    retention: {timestamp_field: "timestamp_ms" duration_seconds: 100}};
                  string demo_id = 1;
                  int64 timestamp_ms = 2;
                  string payload = 3 [(spec.quorum_write) = true, (spec.min_durable_copies) = 3];
                """);
        assertTrue(src.contains("public static int minDurableCopies(String mapName) {"), src);
        assertTrue(src.contains(
                "case \"demo.DemoEvent\": return MIN_DURABLE_COPIES_DEMO_DEMO_EVENT;"), src);
        assertTrue(src.contains("default: return 0;"), src);
    }

    /**
     * The durable format names a row by its protobuf full name; the registry that
     * turns that name back into a generated type is keyed by it, not by a Java class.
     */
    @Test
    void defaultInstanceIsKeyedByProtoFullName(@TempDir Path dir) throws IOException {
        String src = generate(dir, EVENT);
        assertTrue(src.contains("public static Message defaultInstance(String protoFullName) {"), src);
        assertTrue(src.contains("case \"demo.DemoEvent\": return "), src);
        assertTrue(src.contains(".DemoEvent.getDefaultInstance();"), src);
        assertTrue(src.contains("default: return null;"), src);
    }
}
