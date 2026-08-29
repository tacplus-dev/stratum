// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.codegen;

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
 * The query plane's second leg.
 *
 * A warm record type is served by memory AND disk; a hot one is served by memory
 * exactly as it always was. Both halves matter: the disk leg has to appear at
 * every site that scans the store (miss one and that RPC quietly under-reports),
 * and it has to appear at NO site for a hot type (emit one and every record in the
 * fleet grows a leg it has no index for).
 */
class RecordMapWarmLegTest {

    private static final String HOST_PKG =
            "com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum";

    /** The plane declaration, plus one record whose body the caller supplies. */
    private static Path tree(Path dir, String eventBody) throws IOException {
        Path vservices = dir.resolve("vservices");
        Files.createDirectories(vservices);
        Files.writeString(vservices.resolve("query.proto"), """
                syntax = "proto3";
                package demoquery;
                option java_package = "com.example.model.types.vservices.demoquery";
                option java_outer_classname = "Demoquery";

                option (spec.trait) = {
                  id: 40
                  name: "query"
                  host: "stratum"
                  s2s_service: "DemoRecordMap"
                  c2s_service: "DemoRecordQuery"
                };

                // tacplus:generated-service recordmap
                service DemoRecordMap {
                  option (spec.service_s2s) = {label: "stratum"};
                  rpc CreateDemoEventQuery(OpenDemoEventQueryRequest) returns (QueryHandle);
                }

                // tacplus:generated-service recordmap
                service DemoRecordQuery {
                  option (spec.service_c2s) = {auth_entitled_users: true, label: "stratum"};
                  rpc CreateDemoEventQuery(OpenDemoEventQueryRequest) returns (QueryHandle);
                }
                """);
        Files.writeString(vservices.resolve("demo.proto"), """
                syntax = "proto3";
                package demo;
                option java_package = "com.example.model.types.vservices.demo";
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

    private static String generate(Path dir, String eventBody) throws IOException {
        Path protoRoot = tree(dir.resolve("proto"), eventBody);
        VServiceDiscovery.RecordPlane plane = VServiceDiscovery.discoverRecordPlane(protoRoot, "stratum");
        List<EventTopicDiscovery.TopicEvent> records = EventTopicDiscovery.discover(protoRoot);
        assertEquals(1, records.size(), "expected exactly one record, got " + records);

        Path out = dir.resolve("out");
        RecordMapImplGenerator.generate(out, plane.s2s, records);
        return Files.readString(out.resolve(HOST_PKG).resolve("StratumRecordMaps.java"));
    }

    private static final String WARM_BODY = """
              option (spec.record) = {
                key: "event_id"
                retention: {timestamp_field: "timestamp_ms" duration_seconds: 100}
                residency: {window: RESIDENCY_WARM}
              };
              string event_id = 1;
              string spiffe_id = 2 [(spec.query_field) = {op: QUERY_OP_EQ}];
              int64 timestamp_ms = 3 [(spec.query_field) = {op: QUERY_OP_RANGE}];
              int64 store_epoch_ms = 4 [(spec.store_epoch) = true, (spec.query_field) = {op: QUERY_OP_RANGE}];
            """;

    private static final String HOT_BODY = """
              option (spec.record) = {
                key: "event_id"
                retention: {timestamp_field: "timestamp_ms" duration_seconds: 100}
              };
              string event_id = 1;
              string spiffe_id = 2 [(spec.query_field) = {op: QUERY_OP_EQ}];
              int64 timestamp_ms = 3 [(spec.query_field) = {op: QUERY_OP_RANGE}];
            """;

    @Test
    void aWarmTypeGetsADiskLegAtEverySiteThatScansTheStore(@TempDir Path dir) throws IOException {
        String src = generate(dir, WARM_BODY);

        assertTrue(src.contains("private static WarmFilter warmFilterDemoEvent("), src);
        assertTrue(src.contains("private java.util.List<com.example.model.types.vservices.demo.Demo.DemoEvent> warmDemoEvent("), src);
        assertTrue(src.contains("private long warmCountDemoEvent("), src);

        // getCount, updateQuery and subscribeCount count; getSample, list and watch collect.
        assertEquals(3, count(src, "n += warmCountDemoEvent(f);"),
                "every counting RPC must add the disk leg:\n" + src);
        assertEquals(3, count(src, "matches.addAll(warmDemoEvent(f));"),
                "every collecting RPC must add the disk leg:\n" + src);
    }

    /** The filter translation uses the same set/unset test the in-memory match uses. */
    @Test
    void theFilterTranslationMirrorsTheInMemoryMatch(@TempDir Path dir) throws IOException {
        String src = generate(dir, WARM_BODY);

        assertTrue(src.contains("if (f.hasSpiffeId()) wf.eq(\"spiffe_id\", f.getSpiffeId());"), src);
        assertTrue(src.contains("if (f.hasTimestampMsFrom() || f.hasTimestampMsTo()) wf.range(\"timestamp_ms\","), src);
        assertTrue(src.contains("f.hasTimestampMsFrom() ? f.getTimestampMsFrom() : null,"), src);
        assertTrue(src.contains("if (f.hasStoreEpochMsFrom() || f.hasStoreEpochMsTo()) wf.range(\"store_epoch_ms\","),
                "the caller's own epoch bound is what the leg prune reads:\n" + src);
    }

    /**
     * A hot record type — which is every record type today — gets no disk leg and no
     * dataset name. If this ever fails, the emitted code names a Lucene dataset that
     * was never created.
     */
    @Test
    void aHotTypeGetsNoDiskLegAtAll(@TempDir Path dir) throws IOException {
        String src = generate(dir, HOT_BODY);

        assertFalse(src.contains("warmDemoEvent"), src);
        assertFalse(src.contains("warmCountDemoEvent"), src);
        assertFalse(src.contains("warmFilterDemoEvent"), src);
        assertFalse(src.contains("\"demo.DemoEvent\""), src);
    }

    /** The plane always takes the warm store, so the wiring compiles before any proto opts in. */
    @Test
    void thePlaneAlwaysTakesTheWarmStore(@TempDir Path dir) throws IOException {
        String src = generate(dir, HOT_BODY);

        assertTrue(src.contains("Supplier<WarmStore> warmSupplier"), src);
        assertTrue(src.contains("private WarmStore warm() { return warmSupplier.get(); }"), src);
    }

    private static int count(String haystack, String needle) {
        int n = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
            n++;
        }
        return n;
    }
}
