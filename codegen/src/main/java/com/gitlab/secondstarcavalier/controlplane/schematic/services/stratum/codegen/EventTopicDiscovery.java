// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.codegen;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Discovers the EventBus topic event types defined across the vservice protos.
 *
 * An event type is a message whose vservice declares an EventBus topic for it:
 *
 *   message AgentEventBus {
 *     option (spec.service_event) = {tag: "eventbus", ...};
 *     AgentEvent event_stream = 1 [(spec.topic) = true];   // <- AgentEvent is the topic type
 *   }
 *
 * Stratum subscribes to every such topic and stores the event. This discovery
 * feeds {@link EventStoreGenerator}, which emits the typed per-event store —
 * so stratum's hand-written code never names a vservice. Adding/removing a
 * vservice (or .off-ing its proto) regenerates the store with no hand-edit.
 *
 * Cross-package topic types (e.g. trace.TraceEvent) are skipped: stratum stores
 * vservice sensor events, not framework topics, and the Java binding for a
 * same-package event is unambiguous.
 */
public final class EventTopicDiscovery {

    /** A (spec.query_field) filter criterion on a record field. */
    public static final class QueryField {
        public final String name;      // proto field name, e.g. "spiffe_id"
        public final String protoType; // scalar proto type, e.g. "string", "int64"
        public final boolean range;    // true = RANGE (from/to), false = EQ

        public QueryField(String name, String protoType, boolean range) {
            this.name = name;
            this.protoType = protoType;
            this.range = range;
        }
    }

    /** A discovered EventBus topic event type. */
    public static final class TopicEvent {
        /** Proto package of the vservice, e.g. "agentsensor". */
        public final String protoPackage;
        /** Simple event message name, e.g. "AgentEvent". */
        public final String eventType;
        /** Relative proto path, e.g. "vservices/agent-sensor.proto". */
        public final String protoRelPath;
        /** Record key field (spec.record key), e.g. "event_id" — "" if unknown. */
        public final String recordKey;
        /** Queryable fields (spec.query_field) — empty if the record declares none. */
        public final List<QueryField> queryFields;
        /** Java package from option java_package, e.g. "...model.types.vservices.agentsensor". */
        public final String javaPackage;
        /** Java outer class name from option java_outer_classname, e.g. "Agentsensor". */
        public final String outerClassName;

        public TopicEvent(String protoPackage, String eventType, String protoRelPath,
                          String recordKey, List<QueryField> queryFields,
                          String javaPackage, String outerClassName) {
            this.protoPackage = protoPackage;
            this.eventType = eventType;
            this.protoRelPath = protoRelPath;
            this.recordKey = recordKey;
            this.queryFields = queryFields;
            this.javaPackage = javaPackage;
            this.outerClassName = outerClassName;
        }

        @Override
        public String toString() {
            return protoPackage + "." + eventType + " (" + protoRelPath + ")"
                    + (queryFields.isEmpty() ? "" : " query=" + queryFields.size());
        }
    }

    private static final Pattern PACKAGE_DECL = Pattern.compile("^package\\s+(\\S+);");
    private static final Pattern JAVA_PACKAGE = Pattern.compile(
            "option\\s+java_package\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern JAVA_OUTER_CLASSNAME = Pattern.compile(
            "option\\s+java_outer_classname\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern MESSAGE_START = Pattern.compile("^message\\s+(\\w+)\\s*\\{");
    private static final Pattern EVENTBUS_TAG = Pattern.compile(
            "option\\s*\\(spec\\.service_event\\)\\s*=\\s*\\{[^}]*tag:\\s*\"eventbus\"");
    // Field carrying the topic, e.g.  AgentEvent event_stream = 1 [(spec.topic) = true];
    private static final Pattern TOPIC_FIELD = Pattern.compile(
            "([\\w.]+)\\s+\\w+\\s*=\\s*\\d+\\s*\\[[^\\]]*\\(spec\\.topic\\)\\s*=\\s*true");
    // spec.record key, e.g.  option (spec.record) = { key: "event_id" ... }
    private static final Pattern RECORD_KEY = Pattern.compile(
            "\\(spec\\.record\\)\\s*=\\s*\\{.*?key:\\s*\"(\\w+)\"", Pattern.DOTALL);
    // A field carrying (spec.query_field), e.g.
    //   string spiffe_id = 4 [(spec.query_field) = {op: QUERY_OP_EQ}];
    private static final Pattern QUERY_FIELD = Pattern.compile(
            "(\\w[\\w.]*)\\s+(\\w+)\\s*=\\s*\\d+\\s*\\[[^\\]]*\\(spec\\.query_field\\)\\s*=\\s*\\{([^}]*)\\}");

    private EventTopicDiscovery() {}

    /** Scans all .proto files under baseDir for EventBus topic event types. */
    public static List<TopicEvent> discover(Path baseDir) throws IOException {
        List<TopicEvent> result = new ArrayList<>();
        if (!Files.isDirectory(baseDir)) {
            return result;
        }
        try (Stream<Path> files = Files.walk(baseDir)) {
            files.filter(p -> p.toString().endsWith(".proto"))
                 .sorted()
                 .forEach(proto -> {
                     try {
                         scanProto(proto, baseDir, result);
                     } catch (IOException e) {
                         throw new RuntimeException("failed to scan " + proto, e);
                     }
                 });
        }
        return result;
    }

    private static void scanProto(Path proto, Path baseDir, List<TopicEvent> result) throws IOException {
        List<String> lines = Files.readAllLines(proto);
        String relPath = baseDir.relativize(proto).toString();

        String protoPackage = "";
        String javaPackage = "";
        String outerClassName = "";
        for (String line : lines) {
            String trimmed = line.trim();
            Matcher m = PACKAGE_DECL.matcher(trimmed);
            if (m.matches()) {
                protoPackage = m.group(1);
            }
            Matcher jp = JAVA_PACKAGE.matcher(trimmed);
            if (jp.find()) {
                javaPackage = jp.group(1);
            }
            Matcher joc = JAVA_OUTER_CLASSNAME.matcher(trimmed);
            if (joc.find()) {
                outerClassName = joc.group(1);
            }
        }

        // First pass: collect every message's brace-balanced body by name, so a
        // wrapper can be resolved to its (possibly earlier or later) event message.
        java.util.Map<String, String> bodies = new java.util.LinkedHashMap<>();
        int i = 0;
        while (i < lines.size()) {
            Matcher msgStart = MESSAGE_START.matcher(lines.get(i).trim());
            if (msgStart.find()) {
                String name = msgStart.group(1);
                StringBuilder block = new StringBuilder();
                int depth = 0;
                int j = i;
                while (j < lines.size()) {
                    String l = lines.get(j);
                    block.append(l).append('\n');
                    for (char c : l.toCharArray()) {
                        if (c == '{') depth++;
                        else if (c == '}') depth--;
                    }
                    j++;
                    if (depth == 0 && j > i) break;
                }
                bodies.put(name, block.toString());
                i = j;
            } else {
                i++;
            }
        }

        // Second pass: each wrapper's (spec.topic) field names the event type; the
        // event message (same package) carries the spec.record key + any
        // spec.query_field filter criteria.
        for (String body : bodies.values()) {
            if (!EVENTBUS_TAG.matcher(body).find()) {
                continue;
            }
            Matcher topic = TOPIC_FIELD.matcher(body);
            if (!topic.find()) {
                continue;
            }
            String type = topic.group(1);
            if (type.contains(".")) {
                continue; // same-package events only (skip e.g. trace.TraceEvent)
            }
            String eventBody = bodies.getOrDefault(type, "");
            result.add(new TopicEvent(protoPackage, type, relPath,
                    parseRecordKey(eventBody), parseQueryFields(eventBody),
                    javaPackage, outerClassName));
        }
    }

    private static String parseRecordKey(String eventBody) {
        Matcher m = RECORD_KEY.matcher(eventBody);
        return m.find() ? m.group(1) : "";
    }

    private static List<QueryField> parseQueryFields(String eventBody) {
        List<QueryField> fields = new ArrayList<>();
        Matcher m = QUERY_FIELD.matcher(eventBody);
        while (m.find()) {
            String protoType = m.group(1);
            String name = m.group(2);
            boolean range = m.group(3).contains("QUERY_OP_RANGE");
            fields.add(new QueryField(name, protoType, range));
        }
        return fields;
    }
}
