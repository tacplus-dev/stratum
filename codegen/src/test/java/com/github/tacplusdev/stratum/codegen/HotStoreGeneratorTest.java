// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.codegen;

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
 * The hot tier's durability is opted into by proto declarations and nothing else.
 *
 * Two declarations decide it: (spec.min_durable_copies) is the floor a write must
 * reach before it is acked, and (spec.store_epoch) is what a per-node redo log can
 * be unioned back by. Both have defaults that must be the DOCUMENTED ones — a
 * floor that silently reads as 1, or a redo log emitted for a record with no
 * epoch, are exactly the failures that look green at build time.
 */
class HotStoreGeneratorTest {

    private static Path proto(Path dir, String eventBody) throws IOException {
        Path vservices = dir.resolve("vservices");
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
        return dir;
    }

    private static EventTopicDiscovery.TopicEvent only(Path dir) throws IOException {
        List<EventTopicDiscovery.TopicEvent> events = EventTopicDiscovery.discover(dir);
        assertEquals(1, events.size(), "expected exactly one topic event, got " + events);
        return events.get(0);
    }

    private static final String EPOCH_EVENT = """
              option (spec.record) = {
                key: "demo_id"
                retention: {timestamp_field: "timestamp_ms" duration_seconds: 7776000}
              };
              string demo_id = 1;
              int64 timestamp_ms = 2;
              int64 store_epoch_ms = 3 [(spec.store_epoch) = true];
            """;

    /**
     * What used to be declared as a warm record: the retired
     * {@code residency: {window: RESIDENCY_WARM}} preset, on a type that carries a
     * store epoch. Kept as a fixture because protos in the wild may still carry the
     * retired clause, and the point of the test below is that it no longer changes
     * the answer.
     */
    private static final String RETIRED_WARM_PRESET_EVENT = """
              option (spec.record) = {
                key: "demo_id"
                residency: {window: RESIDENCY_WARM}
              };
              string demo_id = 1;
              int64 timestamp_ms = 2;
              int64 store_epoch_ms = 3 [(spec.store_epoch) = true];
            """;

    private static final String NO_EPOCH_EVENT = """
              option (spec.record) = {
                key: "demo_id"
                retention: {timestamp_field: "timestamp_ms" duration_seconds: 7776000}
              };
              string demo_id = 1;
              int64 timestamp_ms = 2;
            """;

    // ------------------------------------------------------------ the floor

    /**
     * spec.proto documents the floor as "Default 2 when unset". A record that says
     * nothing must therefore require two nodes, not one: defaulting to 1 would make
     * every undeclared record ship with the guarantee already switched off.
     */
    @Test
    void anUndeclaredRecordGetsTheDocumentedFloorOfTwo(@TempDir Path dir) throws IOException {
        assertEquals(2, only(proto(dir, EPOCH_EVENT)).minDurableCopies);
    }

    @Test
    void aDeclaredFloorIsCarriedThrough(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, """
                  option (spec.record) = {key: "demo_id"};
                  string demo_id = 1;
                  string payload = 2 [(spec.quorum_write) = true, (spec.min_durable_copies) = 3];
                  int64 store_epoch_ms = 3 [(spec.store_epoch) = true];
                """));
        assertEquals(3, ev.minDurableCopies);
    }

    /**
     * A row is written whole, so the strictest declared floor governs. Taking the
     * last one seen would make the guarantee depend on field order.
     */
    @Test
    void theStrictestDeclaredFloorGoverns(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, """
                  option (spec.record) = {key: "demo_id"};
                  string demo_id = 1;
                  string a = 2 [(spec.quorum_write) = true, (spec.min_durable_copies) = 3];
                  string b = 3 [(spec.quorum_write) = true, (spec.min_durable_copies) = 2];
                  int64 store_epoch_ms = 4 [(spec.store_epoch) = true];
                """));
        assertEquals(3, ev.minDurableCopies);
    }

    // ------------------------------------------------------------ descriptors

    @Test
    void aRecordWithAStoreEpochGetsAReplayableDescriptor(@TempDir Path dir) throws IOException {
        String src = HotStoreGenerator.render(List.of(only(proto(dir, EPOCH_EVENT))));

        assertTrue(src.contains("new HotDataset("), src);
        assertTrue(src.contains("\"demo.DemoEvent\""), src);
        assertTrue(src.contains("m -> ((com.example.demo.Demo.DemoEvent) m).getDemoId()"), src);
        assertTrue(src.contains("m -> ((com.example.demo.Demo.DemoEvent) m).getStoreEpochMs()"), src);
        assertTrue(src.contains("\"store_epoch_ms\""), src);
        assertTrue(src.contains("com.example.demo.Helpers.demoEventToJson"), src);
        assertTrue(src.contains("com.example.demo.Helpers.demoEventFromJson"), src);
        assertTrue(src.contains("                2,"), "the declared floor must reach the descriptor:\n" + src);
    }

    /**
     * A record with no store epoch has nothing to resolve a cross-node union by:
     * the author's business timestamp is a different clock, and file position is
     * causal only WITHIN one node's log. It keeps its in-memory replication and its
     * quorum gate and gets no redo log — rather than a log that cannot be merged
     * back safely, which would only show at recovery.
     */
    @Test
    void aRecordWithoutAStoreEpochGetsNoRedoDescriptor(@TempDir Path dir) throws IOException {
        String src = HotStoreGenerator.render(List.of(only(proto(dir, NO_EPOCH_EVENT))));

        assertFalse(src.contains("new HotDataset("), src);
        assertTrue(src.contains("public static List<HotDataset> datasets()"), src);
    }

    /**
     * The store epoch is now the WHOLE predicate: a record carrying one gets a redo
     * descriptor, whatever residency preset it declares.
     *
     * <p>This inverts the assertion this test used to make. Under the HOT/WARM model
     * a warm record was excluded here, because it recovered through its own Lucene
     * tier instead and a redo log on top would have replayed back into memory
     * exactly the rows the eviction sweep had just removed. The 2026-09-20 record
     * lifecycle decision removed that tier and the window with it: there is one
     * memory clock now, eviction is Hazelcast max-idle, and recovery for every type
     * is the checkpoint plus its redo tail. So the exclusion had nothing left to
     * protect, and a type that declares the retired preset must not silently lose
     * the redo log its recovery now depends on.
     */
    @Test
    void aRecordWithAStoreEpochGetsARedoDescriptorEvenUnderTheRetiredWarmPreset(@TempDir Path dir)
            throws IOException {
        EventTopicDiscovery.TopicEvent e = only(proto(dir, RETIRED_WARM_PRESET_EVENT));
        assertEquals("store_epoch_ms", e.storeEpochField,
                "the fixture must carry the epoch, or this is the no-epoch test again");

        String src = HotStoreGenerator.render(List.of(e));

        assertTrue(src.contains("new HotDataset("), src);
        assertTrue(src.contains("public static List<HotDataset> datasets()"), src);
    }

    @Test
    void noRecordsStillGeneratesTheWiring(@TempDir Path dir) throws IOException {
        String src = HotStoreGenerator.render(List.of());
        assertTrue(src.contains("class StratumHotStores"), src);
        assertTrue(src.contains("return out;"), src);
        assertFalse(src.contains("new HotDataset("), src);
    }
}
