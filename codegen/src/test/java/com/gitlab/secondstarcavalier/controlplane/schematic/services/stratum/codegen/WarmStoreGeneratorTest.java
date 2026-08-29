// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The warm tier is opted into by a proto declaration and by nothing else.
 *
 * That makes discovery the whole contract: a record type that declares
 * RESIDENCY_WARM must get a descriptor, one that declares nothing must get
 * neither a descriptor nor a disk leg, and a declaration the tier cannot honour
 * must fail the BUILD rather than generate something that compiles and silently
 * does the wrong thing at run time. Stratum has been here before — the retention
 * sweep's hard-coded attribute name was also invisible at build time and only
 * failed inside a scheduled job.
 */
class WarmStoreGeneratorTest {

    /** Writes one vservice proto declaring an eventbus topic over `body`. */
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

    private static final String WARM_EVENT = """
              option (spec.record) = {
                key: "accounting_id"
                owner: "spiffe_id"
                retention: {timestamp_field: "timestamp_ms" duration_seconds: 7776000}
                residency: {window: RESIDENCY_WARM window_seconds: 31536000}
                mutability: MUTABILITY_APPEND_ONLY
              };
              string accounting_id = 1;
              string spiffe_id = 2 [(spec.query_field) = {op: QUERY_OP_EQ}];
              int64 timestamp_ms = 3 [(spec.query_field) = {op: QUERY_OP_RANGE}];
              Method method = 4 [(spec.query_field) = {op: QUERY_OP_EQ}];
              int64 store_epoch_ms = 5 [(spec.store_epoch) = true, (spec.query_field) = {op: QUERY_OP_RANGE}];
            """;

    // ---------------------------------------------------------------- discovery

    @Test
    void residencyMutabilityAndStoreEpochAreDiscovered(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, WARM_EVENT));

        assertTrue(ev.isWarm());
        assertEquals("RESIDENCY_WARM", ev.residency);
        assertEquals(31536000L, ev.residencyWindowSeconds);
        assertTrue(ev.isAppendOnly());
        assertEquals("store_epoch_ms", ev.storeEpochField);
    }

    /**
     * Absence is RESIDENCY_HOT and MUTABILITY_CRUD. Every record proto in the fleet
     * relies on that: if absence meant anything else, they would all change meaning
     * the day the annotations landed.
     */
    @Test
    void aRecordDeclaringNoneOfThemIsHotAndMutable(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, """
                  option (spec.record) = {
                    key: "event_id"
                    retention: {timestamp_field: "timestamp_ms" duration_seconds: 100}
                  };
                  string event_id = 1;
                  int64 timestamp_ms = 2;
                """));

        assertFalse(ev.isWarm());
        assertEquals("", ev.residency);
        assertEquals(0L, ev.residencyWindowSeconds);
        assertFalse(ev.isAppendOnly());
        assertEquals("", ev.storeEpochField);
    }

    /**
     * The residency clause is read from inside the record option's own braces, the
     * same bounding retention gets. An unbounded scan would pick up a residency
     * clause belonging to some other annotation further down the message, which is
     * the coupling that bounding exists to prevent.
     */
    @Test
    void residencyOutsideTheRecordOptionIsNotTheRecordsResidency(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, """
                  option (spec.record) = {
                    key: "event_id"
                    retention: {timestamp_field: "timestamp_ms" duration_seconds: 100}
                  };
                  option (spec.transformation) = {
                    name: "Projection"
                    residency: {window: RESIDENCY_WARM}
                  };
                  string event_id = 1;
                  int64 timestamp_ms = 2;
                """));

        assertFalse(ev.isWarm(), "a residency clause on another annotation is not the record's");
    }

    /** A commented-out declaration is not a declaration, on this axis as on the others. */
    @Test
    void aCommentedOutResidencyIsNotParsed(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, """
                  option (spec.record) = {
                    key: "event_id"
                    retention: {timestamp_field: "timestamp_ms" duration_seconds: 100}
                    // residency: {window: RESIDENCY_WARM}
                  };
                  string event_id = 1;
                  // int64 store_epoch_ms = 3 [(spec.store_epoch) = true];
                  int64 timestamp_ms = 2;
                """));

        assertFalse(ev.isWarm());
        assertEquals("", ev.storeEpochField);
    }

    // ---------------------------------------------------------------- generation

    @Test
    void aWarmRecordGetsADescriptorCarryingItsKeyEpochAndFields(@TempDir Path dir) throws IOException {
        String src = WarmStoreGenerator.render(List.of(only(proto(dir, WARM_EVENT))));

        assertTrue(src.contains("\"demo.DemoEvent\","), src);
        assertTrue(src.contains("com.example.demo.Demo.DemoEvent.class,"), src);
        assertTrue(src.contains("\"store_epoch_ms\","), src);
        assertTrue(src.contains("true,"), "MUTABILITY_APPEND_ONLY should reach the descriptor:\n" + src);
        assertTrue(src.contains("31536000L * 1000L,"), src);
        assertTrue(src.contains("getAccountingId()"), src);
        assertTrue(src.contains("getStoreEpochMs()"), src);
        assertTrue(src.contains("com.example.demo.Demo.DemoEvent::parseFrom"), src);

        assertTrue(src.contains("new WarmField(\"spiffe_id\", WarmField.Kind.EQ_STRING"), src);
        assertTrue(src.contains("new WarmField(\"timestamp_ms\", WarmField.Kind.RANGE_NUMERIC"), src);
        assertTrue(src.contains("new WarmField(\"method\", WarmField.Kind.EQ_NUMERIC"), src);
        assertTrue(src.contains("getMethodValue()"), "an enum criterion is indexed by its wire number:\n" + src);
    }

    /**
     * The store-epoch field is indexed once, under the reserved name the leg cut
     * uses. Indexing it a second time under its proto name would put the caller's
     * bound and the leg's own bound on two different Lucene fields.
     */
    @Test
    void theStoreEpochFieldIsNotAlsoEmittedAsAnOrdinaryCriterion(@TempDir Path dir) throws IOException {
        String src = WarmStoreGenerator.render(List.of(only(proto(dir, WARM_EVENT))));

        assertFalse(src.contains("new WarmField(\"store_epoch_ms\""),
                "the store-epoch field must not be indexed twice:\n" + src);
    }

    /** A hot record type gets no descriptor at all. */
    @Test
    void aHotRecordGetsNoDescriptor(@TempDir Path dir) throws IOException {
        String src = WarmStoreGenerator.render(List.of(only(proto(dir, """
                  option (spec.record) = {
                    key: "event_id"
                    retention: {timestamp_field: "timestamp_ms" duration_seconds: 100}
                  };
                  string event_id = 1;
                  int64 timestamp_ms = 2 [(spec.query_field) = {op: QUERY_OP_RANGE}];
                """))));

        assertFalse(src.contains("new WarmDataset("), src);
        assertTrue(src.contains("List<WarmDataset> out = new ArrayList<>();"), src);
    }

    /**
     * A warm record with no (spec.store_epoch) field cannot be tiered: the store
     * epoch is what the two legs partition on and there is no substitute. Refusing
     * it at build time is the point — the alternative is a tier that silently puts
     * rows on the wrong side of the boundary.
     */
    @Test
    void aWarmRecordWithoutAStoreEpochFieldFailsGeneration(@TempDir Path dir) throws IOException {
        List<EventTopicDiscovery.TopicEvent> events = List.of(only(proto(dir, """
                  option (spec.record) = {
                    key: "event_id"
                    retention: {timestamp_field: "timestamp_ms" duration_seconds: 100}
                    residency: {window: RESIDENCY_WARM}
                  };
                  string event_id = 1;
                  int64 timestamp_ms = 2;
                """)));

        IllegalStateException boom = assertThrows(IllegalStateException.class,
                () -> WarmStoreGenerator.render(events));
        assertTrue(boom.getMessage().contains("(spec.store_epoch)"), boom.getMessage());
    }

    /** A warm record declaring no window_seconds falls back to the configured default. */
    @Test
    void anUnsetWindowFallsBackToTheConfiguredDefault(@TempDir Path dir) throws IOException {
        String src = WarmStoreGenerator.render(List.of(only(proto(dir, """
                  option (spec.record) = {
                    key: "event_id"
                    retention: {timestamp_field: "timestamp_ms" duration_seconds: 100}
                    residency: {window: RESIDENCY_WARM}
                  };
                  string event_id = 1;
                  int64 timestamp_ms = 2;
                  int64 store_epoch_ms = 3 [(spec.store_epoch) = true];
                """))));

        assertTrue(src.contains("defaultWindowSeconds * 1000L,"), src);
    }
}
