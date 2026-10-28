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
 * The query plane's soft-delete default exclusion.
 *
 * A record type that declares (spec.deleted_flag) has its flagged rows dropped
 * from every default query result; one that does not is generated exactly as it
 * was before the marker existed. Both halves matter: miss the clause at one
 * scanning site and that RPC quietly returns deleted rows, and emit it for an
 * unmarked type and the generated source names a getter the record has no field
 * for, which fails javac fleet-wide.
 *
 * include_deleted is the escape hatch, and it is fixed at create&lt;T&gt;Query and
 * replaced at update&lt;T&gt;Query because every other call in the lifecycle carries
 * only the handle.
 */
class RecordSoftDeleteTest {

    private static final String HOST_PKG =
            "com/github/tacplusdev/stratum";

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

    private static RecordDiscovery.RecordDecl discover(Path dir, String eventBody) throws IOException {
        List<RecordDiscovery.RecordDecl> records =
                RecordDiscovery.discover(tree(dir.resolve("proto"), eventBody));
        assertEquals(1, records.size(), "expected exactly one record, got " + records);
        return records.get(0);
    }

    private static String generate(Path dir, String eventBody) throws IOException {
        Path protoRoot = tree(dir.resolve("proto"), eventBody);
        VServiceDiscovery.RecordPlane plane = VServiceDiscovery.discoverRecordPlane(protoRoot, "stratum");
        List<RecordDiscovery.RecordDecl> records = RecordDiscovery.discover(protoRoot);
        assertEquals(1, records.size(), "expected exactly one record, got " + records);

        Path out = dir.resolve("out");
        RecordMapImplGenerator.generate(out, plane.s2s, records);
        return Files.readString(out.resolve(HOST_PKG).resolve("StratumRecordMaps.java"));
    }

    /** A filterable record carrying the marker. */
    private static final String MARKED_BODY = """
              option (spec.record) = {key: "event_id"};
              string event_id = 1;
              string spiffe_id = 2 [(spec.query_field) = {op: QUERY_OP_EQ}];
              string namespace = 3 [(spec.store_namespace) = true];
              bool is_deleted = 4 [(spec.deleted_flag) = true];
            """;

    /** The same record without the marker — today's fleet, every type of it. */
    private static final String UNMARKED_BODY = """
              option (spec.record) = {key: "event_id"};
              string event_id = 1;
              string spiffe_id = 2 [(spec.query_field) = {op: QUERY_OP_EQ}];
              string namespace = 3 [(spec.store_namespace) = true];
              bool is_deleted = 4;
            """;

    /** A record with no (spec.query_field) and no s2s_read: no match<T> to hide in. */
    private static final String FILTERLESS_MARKED_BODY = """
              option (spec.record) = {key: "event_id"};
              string event_id = 1;
              string namespace = 2 [(spec.store_namespace) = true];
              bool is_deleted = 3 [(spec.deleted_flag) = true];
            """;

    // ----- (a) detection -----

    @Test
    void aMarkedBoolFieldIsDiscovered(@TempDir Path dir) throws IOException {
        assertEquals("is_deleted", discover(dir, MARKED_BODY).deletedFlagField);
    }

    @Test
    void anUnmarkedRecordHasNoDeletedFlagField(@TempDir Path dir) throws IOException {
        assertEquals("", discover(dir, UNMARKED_BODY).deletedFlagField);
    }

    @Test
    void aCommentedOutMarkerIsNotADeclaration(@TempDir Path dir) throws IOException {
        assertEquals("", discover(dir, """
                  option (spec.record) = {key: "event_id"};
                  string event_id = 1;
                  string namespace = 2 [(spec.store_namespace) = true];
                  bool is_deleted = 3; // [(spec.deleted_flag) = true]
                """).deletedFlagField);
    }

    /**
     * DF-2/DF-3, the shapes tacplus refuses before generation.
     *
     * Stratum does not re-validate them — tacplus's ValidateDeletedFlagAnnotations
     * has already rejected the file — but it has to ANSWER them the way tacplus's
     * own parser.DeletedFlagField does, i.e. "no marker", so that a malformed
     * declaration that somehow reached here generates the pre-marker query plane
     * rather than one that excludes on the wrong field.
     */
    @Test
    void aMalformedOrDuplicatedMarkerReadsAsNoMarker(@TempDir Path dir) throws IOException {
        assertEquals("", discover(dir.resolve("two"), """
                  option (spec.record) = {key: "event_id"};
                  string event_id = 1;
                  string namespace = 2 [(spec.store_namespace) = true];
                  bool is_deleted = 3 [(spec.deleted_flag) = true];
                  bool also_deleted = 4 [(spec.deleted_flag) = true];
                """).deletedFlagField, "DF-3: two markers");

        assertEquals("", discover(dir.resolve("repeated"), """
                  option (spec.record) = {key: "event_id"};
                  string event_id = 1;
                  string namespace = 2 [(spec.store_namespace) = true];
                  repeated bool is_deleted = 3 [(spec.deleted_flag) = true];
                """).deletedFlagField, "DF-2: repeated");

        assertEquals("", discover(dir.resolve("nonbool"), """
                  option (spec.record) = {key: "event_id"};
                  string event_id = 1;
                  string namespace = 2 [(spec.store_namespace) = true];
                  string is_deleted = 3 [(spec.deleted_flag) = true];
                """).deletedFlagField, "DF-2: not a bool");
    }

    // ----- (b) the generated clause -----

    @Test
    void aMarkedTypeExcludesFlaggedRowsFromMatch(@TempDir Path dir) throws IOException {
        String src = generate(dir, MARKED_BODY);

        assertTrue(src.contains(
                "private static boolean matchDemoEvent(Demoquery.DemoEventFilter f,"
                        + " boolean includeDeleted, com.example.model.types.vservices.demo.Demo.DemoEvent ev) {"),
                src);
        assertTrue(src.contains("        if (!includeDeleted && ev.getIsDeleted()) return false;"), src);
    }

    /**
     * The design's claim, checked rather than taken on faith: all five scanning
     * RPCs route through match&lt;T&gt;, so the one clause above covers all of them.
     */
    @Test
    void allFiveScanningOpsGoThroughMatch(@TempDir Path dir) throws IOException {
        String src = generate(dir, MARKED_BODY);
        for (String op : List.of("getDemoEventCount", "getDemoEventSample", "listDemoEvent",
                "watchDemoEvent", "subscribeDemoEventCount")) {
            String body = bodyOf(src, op);
            assertTrue(body.contains("matchDemoEvent(f, h.includeDeleted, ev)"),
                    op + " does not scan through match<T>:\n" + body);
        }
    }

    @Test
    void anUnmarkedTypeEmitsNoExtraClause(@TempDir Path dir) throws IOException {
        String src = generate(dir, UNMARKED_BODY);

        assertTrue(src.contains(
                "private static boolean matchDemoEvent(Demoquery.DemoEventFilter f,"
                        + " boolean includeDeleted, com.example.model.types.vservices.demo.Demo.DemoEvent ev) {"),
                src);
        assertFalse(src.contains("getIsDeleted()"),
                "an unmarked record has no soft-delete state to exclude on:\n" + src);
    }

    /**
     * A filterless record type still gets count/sample/list, and so still needs the
     * exclusion — it just has no match&lt;T&gt; for the clause to live in.
     */
    @Test
    void aFilterlessMarkedTypeStillExcludes(@TempDir Path dir) throws IOException {
        String src = generate(dir, FILTERLESS_MARKED_BODY);

        assertFalse(src.contains("private static boolean matchDemoEvent("),
                "a filterless type has no filter to match:\n" + src);
        assertTrue(src.contains(
                "if (inNamespace(h, ev.getNamespace()) && (h.includeDeleted || !ev.getIsDeleted())) n++;"),
                src);
        assertTrue(src.contains(
                "if (inNamespace(h, ev.getNamespace()) && (h.includeDeleted || !ev.getIsDeleted()))"
                        + " matches.add(ev);"),
                src);
    }

    // ----- (c) include_deleted reaches the handle, and only from Open/Refine -----

    @Test
    void includeDeletedIsFixedAtOpenAndReplacedAtRefine(@TempDir Path dir) throws IOException {
        String src = generate(dir, MARKED_BODY);

        assertTrue(src.contains("        final boolean includeDeleted;"), src);
        // The handle lives in a map every node shares, so its filter is bytes and
        // it is typed by the record it was opened over.
        assertTrue(src.contains(
                "String qid = openHandle(\"" + "marked.Marked" + "\", ns, req.getFilter().toByteArray(), req.getIncludeDeleted());")
                || src.contains("ns, req.getFilter().toByteArray(), req.getIncludeDeleted());"), src);
        assertTrue(src.contains(
                "queries().put(req.getQueryId(), new Handle(h.type, h.namespace, f.toByteArray(), req.getIncludeDeleted()));"), src);
    }

    /**
     * There is no Get&lt;T&gt;-by-key RPC on this plane — a point read is an EQ
     * criterion on the record key — so "Get stays unfiltered" has no separate site
     * to leave alone, and the deleted row a caller wants by key comes back through
     * include_deleted. What this pins is the narrower fact the plane does own: the
     * exclusion is applied at the row scan and NOWHERE in the handle lifecycle, so
     * delete&lt;T&gt;Query and create&lt;T&gt;Query stay row-blind.
     */
    @Test
    void theHandleLifecycleOpsNeverTouchTheFlaggedRow(@TempDir Path dir) throws IOException {
        String src = generate(dir, MARKED_BODY);
        for (String op : List.of("createDemoEventQuery", "deleteDemoEventQuery")) {
            assertFalse(bodyOf(src, op).contains("getIsDeleted()"),
                    op + " must not filter rows:\n" + bodyOf(src, op));
        }
    }

    /** The emitted text of one generated method, from its signature to the next one. */
    private static String bodyOf(String src, String method) {
        int at = src.indexOf(" " + method + "(");
        assertTrue(at >= 0, method + " not generated:\n" + src);
        int end = src.indexOf("\n    @Override", at);
        return src.substring(at, end < 0 ? src.length() : end);
    }
}
