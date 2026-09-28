// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Get-by-key ignores the deleted flag.
 *
 * <p>This plane has no Get&lt;T&gt; RPC: a point read is a &lt;T&gt;Filter that pins the
 * record key (the criterion tacplus folds in for an s2s_read record) and sets
 * nothing else. The record lifecycle decision says such a read returns the row
 * deleted or not, until it is purged — so for that one filter shape the
 * soft-delete exclusion is skipped whatever {@code include_deleted} says, while
 * every other shape keeps the default exclusion {@link RecordSoftDeleteTest}
 * pins.
 *
 * <p>"Still within age" here means "still in the store": the
 * deleted_max_age_seconds purge is a later slice, and a row that has not been
 * purged is simply a row the scan sees.
 *
 * <p>The behaviour half is not asserted on the emitted TEXT: the generated
 * match&lt;T&gt; is extracted, compiled against stub filter/record classes shaped
 * like the protobuf ones it reflects on, and CALLED. The predicate under test is
 * the real generated code, not a restatement of it.
 */
class RecordGetByKeyTest {

    private static final String HOST_PKG =
            "com/gitlab/seabeam/controlplane/schematic/services/stratum";

    /** Marked, and s2s_read — so tacplus folds event_id into DemoEventFilter. */
    private static final String MARKED_READABLE_BODY = """
              option (spec.record) = {key: "event_id", s2s_read: {auth_services: ["demo"]}};
              string event_id = 1;
              string spiffe_id = 2 [(spec.query_field) = {op: QUERY_OP_EQ}];
              string namespace = 3 [(spec.store_namespace) = true];
              bool is_deleted = 4 [(spec.deleted_flag) = true];
            """;

    /** Marked, but no s2s_read: no key criterion exists, so no shape to exempt. */
    private static final String MARKED_UNREADABLE_BODY = """
              option (spec.record) = {key: "event_id"};
              string event_id = 1;
              string spiffe_id = 2 [(spec.query_field) = {op: QUERY_OP_EQ}];
              string namespace = 3 [(spec.store_namespace) = true];
              bool is_deleted = 4 [(spec.deleted_flag) = true];
            """;

    /** The same readable record with no marker — today's fleet, every type of it. */
    private static final String UNMARKED_READABLE_BODY = """
              option (spec.record) = {key: "event_id", s2s_read: {auth_services: ["demo"]}};
              string event_id = 1;
              string spiffe_id = 2 [(spec.query_field) = {op: QUERY_OP_EQ}];
              string namespace = 3 [(spec.store_namespace) = true];
              bool is_deleted = 4;
            """;

    // ----- the emitted guard -----

    @Test
    void aKeyOnlyFilterIsExemptFromTheExclusion(@TempDir Path dir) throws IOException {
        String src = generate(dir, MARKED_READABLE_BODY);

        assertTrue(src.contains("        boolean pointRead = f.hasEventId() && !f.hasSpiffeId();"), src);
        assertTrue(src.contains(
                "        if (!includeDeleted && !pointRead && ev.getIsDeleted()) return false;"), src);
    }

    /**
     * A range criterion is two filter fields, and a point read has neither set —
     * pinning the key and a half-open range is a scan.
     */
    @Test
    void aRangeCriterionCountsAsBothItsEnds(@TempDir Path dir) throws IOException {
        String src = generate(dir, """
                  option (spec.record) = {key: "event_id", s2s_read: {auth_services: ["demo"]}};
                  string event_id = 1;
                  int64 timestamp_ms = 2 [(spec.query_field) = {op: QUERY_OP_RANGE}];
                  string namespace = 3 [(spec.store_namespace) = true];
                  bool is_deleted = 4 [(spec.deleted_flag) = true];
                """);

        assertTrue(src.contains("        boolean pointRead = f.hasEventId()"
                + " && !f.hasTimestampMsFrom() && !f.hasTimestampMsTo();"), src);
    }

    @Test
    void aRecordWithNoKeyCriterionKeepsThePlainClause(@TempDir Path dir) throws IOException {
        String src = generate(dir, MARKED_UNREADABLE_BODY);

        assertFalse(src.contains("pointRead"),
                "with no key folded into the filter there is no by-key read to exempt:\n" + src);
        assertTrue(src.contains("        if (!includeDeleted && ev.getIsDeleted()) return false;"), src);
    }

    @Test
    void anUnmarkedTypeIsUntouched(@TempDir Path dir) throws IOException {
        String src = generate(dir, UNMARKED_READABLE_BODY);

        assertFalse(src.contains("pointRead"), src);
        assertFalse(src.contains("getIsDeleted()"), src);
    }

    // ----- the predicate, compiled and run -----

    /**
     * Both halves of the decision, against the generated predicate itself: the
     * deleted row comes back for a bare key-EQ read with include_deleted=false,
     * and stays excluded from a scan under that same request.
     */
    @Test
    void theGeneratedPredicateReturnsADeletedRowByKeyAndHidesItFromAScan(@TempDir Path dir)
            throws Exception {
        Predicate match = compileMatch(dir, MARKED_READABLE_BODY);

        Object deleted = match.event("k1", "sp-1", true);
        Object live = match.event("k1", "sp-1", false);

        // include_deleted = false throughout: the request asks for no deleted rows.
        assertTrue(match.call(match.filter("k1", null), false, deleted),
                "a key-EQ point read must return the deleted row");
        assertFalse(match.call(match.filter(null, "sp-1"), false, deleted),
                "a non-key scan must still exclude the deleted row");

        // The exemption is the BARE key filter only — pinning the key alongside
        // another criterion is a scan that happens to be narrow.
        assertFalse(match.call(match.filter("k1", "sp-1"), false, deleted),
                "key + another criterion is a scan, not a point read");

        // And the exemption does not make the key criterion itself stop applying.
        assertFalse(match.call(match.filter("k2", null), false, deleted),
                "a point read on a different key must not return this row");

        // Sanity: nothing above changed for a row that is not deleted.
        assertTrue(match.call(match.filter("k1", null), false, live));
        assertTrue(match.call(match.filter(null, "sp-1"), false, live));

        // include_deleted = true is still the way a SCAN sees deleted rows.
        assertTrue(match.call(match.filter(null, "sp-1"), true, deleted));
    }

    // ----- fixture (mirrors RecordSoftDeleteTest) -----

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

    // ----- compiling the generated predicate -----

    /** The compiled match&lt;T&gt;, plus the stub types it takes. */
    private record Predicate(Method method, Class<?> filterCls, Class<?> eventCls) {

        Object filter(String eventId, String spiffeId) throws Exception {
            Object f = filterCls.getConstructor().newInstance();
            if (eventId != null) {
                filterCls.getMethod("eventId", String.class).invoke(f, eventId);
            }
            if (spiffeId != null) {
                filterCls.getMethod("spiffeId", String.class).invoke(f, spiffeId);
            }
            return f;
        }

        Object event(String eventId, String spiffeId, boolean deleted) throws Exception {
            return eventCls.getConstructor(String.class, String.class, boolean.class)
                    .newInstance(eventId, spiffeId, deleted);
        }

        boolean call(Object filter, boolean includeDeleted, Object event) throws Exception {
            return (boolean) method.invoke(null, filter, includeDeleted, event);
        }
    }

    /**
     * Lifts match&lt;T&gt; out of the generated source and compiles it, against stubs
     * carrying the protobuf accessors it reflects on. The stubs stand in for the
     * generated model classes (which this module does not depend on); the method
     * body is the generator's own output, byte for byte.
     */
    private static Predicate compileMatch(Path dir, String eventBody) throws Exception {
        String match = matchSource(generate(dir, eventBody));

        Path src = dir.resolve("probe-src");
        write(src, "com/example/model/types/vservices/demoquery/Demoquery.java", """
                package com.example.model.types.vservices.demoquery;
                public final class Demoquery {
                    public static final class DemoEventFilter {
                        private String eventId;
                        private String spiffeId;
                        public void eventId(String v) { this.eventId = v; }
                        public void spiffeId(String v) { this.spiffeId = v; }
                        public boolean hasEventId() { return eventId != null; }
                        public String getEventId() { return eventId == null ? "" : eventId; }
                        public boolean hasSpiffeId() { return spiffeId != null; }
                        public String getSpiffeId() { return spiffeId == null ? "" : spiffeId; }
                    }
                }
                """);
        write(src, "com/example/model/types/vservices/demo/Demo.java", """
                package com.example.model.types.vservices.demo;
                public final class Demo {
                    public static final class DemoEvent {
                        private final String eventId;
                        private final String spiffeId;
                        private final boolean deleted;
                        public DemoEvent(String eventId, String spiffeId, boolean deleted) {
                            this.eventId = eventId;
                            this.spiffeId = spiffeId;
                            this.deleted = deleted;
                        }
                        public String getEventId() { return eventId; }
                        public String getSpiffeId() { return spiffeId; }
                        public boolean getIsDeleted() { return deleted; }
                    }
                }
                """);
        write(src, "probe/Probe.java",
                "package probe;\n"
                        + "import com.example.model.types.vservices.demoquery.Demoquery;\n"
                        + "public final class Probe {\n"
                        + match.replace("private static boolean", "public static boolean")
                        + "}\n");

        Path classes = dir.resolve("probe-classes");
        Files.createDirectories(classes);
        compile(src, classes);

        try (URLClassLoader cl = new URLClassLoader(new URL[] {classes.toUri().toURL()},
                RecordGetByKeyTest.class.getClassLoader())) {
            Class<?> filterCls = cl.loadClass(
                    "com.example.model.types.vservices.demoquery.Demoquery$DemoEventFilter");
            Class<?> eventCls = cl.loadClass(
                    "com.example.model.types.vservices.demo.Demo$DemoEvent");
            Method m = cl.loadClass("probe.Probe")
                    .getMethod("matchDemoEvent", filterCls, boolean.class, eventCls);
            return new Predicate(m, filterCls, eventCls);
        }
    }

    /** The emitted match&lt;T&gt; method, signature line through its closing brace. */
    private static String matchSource(String generated) {
        int at = generated.indexOf("    private static boolean matchDemoEvent(");
        assertTrue(at >= 0, "match<T> not generated:\n" + generated);
        int end = generated.indexOf("\n    }\n", at);
        assertTrue(end >= 0, "match<T> has no closing brace:\n" + generated.substring(at));
        return generated.substring(at, end + "\n    }\n".length());
    }

    private static void write(Path root, String rel, String body) throws IOException {
        Path file = root.resolve(rel);
        Files.createDirectories(file.getParent());
        Files.writeString(file, body);
    }

    private static void compile(Path srcRoot, Path classes) throws IOException {
        JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
        assertNotNull(javac, "no system java compiler — the tests must run on a JDK");
        List<Path> sources = new ArrayList<>();
        try (var walk = Files.walk(srcRoot)) {
            walk.filter(p -> p.toString().endsWith(".java")).forEach(sources::add);
        }
        DiagnosticCollector<JavaFileObject> problems = new DiagnosticCollector<>();
        try (StandardJavaFileManager fm = javac.getStandardFileManager(problems, null, null)) {
            boolean ok = javac.getTask(null, fm, problems,
                    List.of("-d", classes.toString()), null,
                    fm.getJavaFileObjectsFromPaths(sources)).call();
            assertTrue(ok, "the generated predicate does not compile: " + problems.getDiagnostics());
        }
    }
}
