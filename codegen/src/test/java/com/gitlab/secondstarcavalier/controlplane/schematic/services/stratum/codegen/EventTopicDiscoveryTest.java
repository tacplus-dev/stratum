// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins stratum's regex parser to tacplus's descriptor-based behaviour. The two
 * sides have to agree about what a record is and what its key is: tacplus emits
 * the query plane's RPC family from its answer and stratum implements the family
 * from this one, so any disagreement is an abstract-method gap javac rejects.
 */
class EventTopicDiscoveryTest {

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

    /**
     * F2 — a record that omits `key:` is still a record. tacplus keys the plane
     * off the annotation's presence and defaults the key to event_id
     * (model.DefaultRecordKeyField), so stratum must default it identically
     * rather than treat the record as absent.
     */
    @Test
    void recordWithoutDeclaredKeyDefaultsToEventId(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, """
                  option (spec.record) = {owner: "owner_id"};
                  string event_id = 1;
                  string owner_id = 2;
                """));

        assertTrue(ev.isRecord, "a (spec.record) message with no key: is still a record");
        assertEquals("event_id", ev.recordKey);
    }

    /** A message declaring no (spec.record) is a plain topic, not a record. */
    @Test
    void plainTopicIsNotARecord(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, """
                  string event_id = 1;
                """));

        assertTrue(!ev.isRecord, "a message with no (spec.record) is not a record");
        assertEquals("", ev.recordKey);
    }

    /**
     * F3 — a commented-out annotation is not a declaration. Left unstripped, the
     * commented (spec.query_field) below produced a filter field on stratum's
     * side that tacplus's descriptor-based side never emits.
     */
    @Test
    void commentedOutAnnotationsAreNotParsed(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, """
                  option (spec.record) = {key: "event_id"};
                  string event_id = 1;
                  // string spiffe_id = 2 [(spec.query_field) = {op: QUERY_OP_EQ}];
                  int64 timestamp_ms = 3 [(spec.query_field) = {op: QUERY_OP_RANGE}];
                """));

        assertEquals(List.of("timestamp_ms"), ev.queryFields.stream().map(f -> f.name).toList());
    }

    /**
     * F4 — `key:` is read only from inside the record option's own braces, and
     * only as a whole field name.
     *
     * The old DOTALL regex anchored at "(spec.record) = {" scanned past that
     * literal's closing brace for any later `key:`, and matched the tail of
     * sort_key: as well. Here both traps are present: the record option declares
     * no key of its own but does declare sort_key:, and a later message declares
     * key: "wrong_field".
     */
    @Test
    void recordKeyIgnoresSuffixedFieldsAndLaterMessages(@TempDir Path dir) throws IOException {
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
                  option (spec.record) = {owner: "owner_id", sort_key: "occurred_at_ms"};
                  string event_id = 1;
                  string owner_id = 2;
                  int64 occurred_at_ms = 3;
                }

                message OtherRecord {
                  option (spec.record) = {key: "wrong_field"};
                  string wrong_field = 1;
                }
                """);

        EventTopicDiscovery.TopicEvent ev = only(dir);
        assertEquals("event_id", ev.recordKey);
    }
}
