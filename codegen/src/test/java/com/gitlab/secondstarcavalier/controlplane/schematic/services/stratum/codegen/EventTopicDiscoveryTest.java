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

        // event_id (the record's own key) is folded in as an implicit EQ
        // criterion even though it carries no (spec.query_field) of its own —
        // see EventTopicDiscovery.withKeyCriterion.
        assertEquals(List.of("timestamp_ms", "event_id"), ev.queryFields.stream().map(f -> f.name).toList());
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

    /**
     * A record's key is always an implicit EQ query field, whether or not the
     * author also annotated it with (spec.query_field) — this is what makes a
     * point read by key possible at all. Without it, RecordMapImplGenerator's
     * match<T>() never checks the key a caller filtered on, and a read for a
     * wrong/never-created id matches every row instead of none.
     */
    @Test
    void recordKeyIsAlwaysAnImplicitQueryField(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, """
                  option (spec.record) = {key: "spiffe_id"};
                  string spiffe_id = 1;
                  string owner_id = 2;
                """));

        List<EventTopicDiscovery.QueryField> fields = ev.queryFields;
        assertEquals(1, fields.size(), "expected only the folded-in key: " + fields.stream().map(f -> f.name).toList());
        assertEquals("spiffe_id", fields.get(0).name);
        assertEquals("string", fields.get(0).protoType);
        assertTrue(!fields.get(0).range, "the key criterion is EQ, not RANGE");
    }

    /** An explicit (spec.query_field) on the key is not duplicated by the fold. */
    @Test
    void explicitQueryFieldOnKeyIsNotDuplicated(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, """
                  option (spec.record) = {key: "spiffe_id"};
                  string spiffe_id = 1 [(spec.query_field) = {op: QUERY_OP_EQ}];
                """));

        assertEquals(List.of("spiffe_id"), ev.queryFields.stream().map(f -> f.name).toList());
    }

    /** A repeated or map-typed key cannot be an EQ criterion, so it is skipped. */
    @Test
    void repeatedKeyIsNotFoldedIn(@TempDir Path dir) throws IOException {
        EventTopicDiscovery.TopicEvent ev = only(proto(dir, """
                  option (spec.record) = {key: "tags"};
                  repeated string tags = 1;
                """));

        assertTrue(ev.queryFields.isEmpty(), "a repeated key must not become a filter criterion: " + ev.queryFields.stream().map(f -> f.name).toList());
    }
}
