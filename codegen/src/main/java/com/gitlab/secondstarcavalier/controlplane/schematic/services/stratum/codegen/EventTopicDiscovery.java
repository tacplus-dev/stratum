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
 *     option (spec.service_event) = {label: "eventbus", ...};
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
        /**
         * Whether the event message declares {@code option (spec.record)} — i.e.
         * whether tacplus emits the query plane's RPC family for it.
         *
         * This, not a non-empty {@link #recordKey}, is what "is a record" means.
         * tacplus keys the whole plane off the annotation's presence and defaults
         * a missing {@code key:} to {@value EventTopicDiscovery#DEFAULT_RECORD_KEY}; a record that
         * omits {@code key:} therefore gets RPCs in the generated interface, and
         * dropping it here would leave StratumRecordMaps with an abstract-method
         * gap that fails javac.
         */
        public final boolean isRecord;
        /**
         * Record key field from {@code (spec.record).key}, defaulted to
         * {@value EventTopicDiscovery#DEFAULT_RECORD_KEY} when the record declares none — the same
         * default tacplus applies (model.DefaultRecordKeyField). "" only when the
         * message is not a record at all.
         */
        public final String recordKey;
        /** Queryable fields (spec.query_field) — empty if the record declares none. */
        public final List<QueryField> queryFields;
        /**
         * The field the record's retention age is measured from, taken from
         * (spec.record).retention.timestamp_field — e.g. "timestamp_ms" on
         * AgentEvent, "occurred_at_ms" on TaskActivity. "" when the record
         * declares no retention policy.
         *
         * Retention is DELIVERY-side state. It used to ride on the record's
         * timeseries projection annotation; BIG GOAL 3 acceptance item 3 split
         * transformation from delivery and moved it onto (spec.record), which is
         * the declaration that actually says this message is stored and delivered.
         */
        public final String retentionTimestampField;
        /**
         * (spec.record).residency.window — "RESIDENCY_WARM" when the record type
         * declares the warm window, "" otherwise.
         *
         * Absence is HOT, which is what every record does today, so an empty string
         * here and RESIDENCY_HOT are the same statement. The disk tier is generated
         * only for the warm ones, which is why this is a string and not a boolean:
         * RESIDENCY_COLD is a declared-but-refused value in tacplus, and swallowing
         * it into "not warm" would silently accept a declaration tacplus rejects.
         */
        public final String residency;
        /**
         * (spec.record).residency.window_seconds — the memory-residency window
         * override, or 0 for "use the configured default".
         */
        public final long residencyWindowSeconds;
        /**
         * (spec.record).mutability — "MUTABILITY_APPEND_ONLY" when the record type
         * is written once, "" otherwise (i.e. CRUD, today's behaviour).
         *
         * On the warm tier this is not decoration: an append-only dataset's slices
         * are pure appends, so its sealed segments are byte-stable and it needs no
         * cross-slice reconciliation, because it never deletes.
         */
        public final String mutability;
        /**
         * The record field carrying (spec.store_epoch) — Unix millis of the row's
         * last write, stamped by the generated wrapper. "" when the record declares
         * none.
         *
         * This is the field the two legs partition on: memory serves
         * epoch >= now - window, disk serves epoch < now - window. A warm record
         * without one cannot be tiered at all, which is why WarmStoreGenerator
         * refuses it rather than picking a substitute clock.
         */
        public final String storeEpochField;
        /** Java package from option java_package, e.g. "...model.types.vservices.agentsensor". */
        public final String javaPackage;
        /** Java outer class name from option java_outer_classname, e.g. "Agentsensor". */
        public final String outerClassName;

        public TopicEvent(String protoPackage, String eventType, String protoRelPath,
                          boolean isRecord, String recordKey, List<QueryField> queryFields,
                          String retentionTimestampField,
                          String residency, long residencyWindowSeconds, String mutability,
                          String storeEpochField,
                          String javaPackage, String outerClassName) {
            this.protoPackage = protoPackage;
            this.eventType = eventType;
            this.protoRelPath = protoRelPath;
            this.isRecord = isRecord;
            this.recordKey = recordKey;
            this.queryFields = queryFields;
            this.retentionTimestampField = retentionTimestampField;
            this.residency = residency;
            this.residencyWindowSeconds = residencyWindowSeconds;
            this.mutability = mutability;
            this.storeEpochField = storeEpochField;
            this.javaPackage = javaPackage;
            this.outerClassName = outerClassName;
        }

        /** True when the record type declares the warm memory-residency window. */
        public boolean isWarm() {
            return RESIDENCY_WARM.equals(residency);
        }

        /** True when the record type is written once and never updated or deleted. */
        public boolean isAppendOnly() {
            return MUTABILITY_APPEND_ONLY.equals(mutability);
        }

        @Override
        public String toString() {
            return protoPackage + "." + eventType + " (" + protoRelPath + ")"
                    + (queryFields.isEmpty() ? "" : " query=" + queryFields.size())
                    + (residency.isEmpty() ? "" : " " + residency)
                    + (mutability.isEmpty() ? "" : " " + mutability);
        }
    }

    /**
     * The record key tacplus assumes when a record declares no {@code key:}
     * (its model.DefaultRecordKeyField). Both sides must agree: tacplus emits
     * the RPC family with this as the Pull/Watch order_by, so stratum has to
     * implement it under the same name.
     */
    public static final String DEFAULT_RECORD_KEY = "event_id";

    /** The residency preset that puts a record type on the warm disk tier. */
    public static final String RESIDENCY_WARM = "RESIDENCY_WARM";
    /** The mutability preset that makes a record type write-once. */
    public static final String MUTABILITY_APPEND_ONLY = "MUTABILITY_APPEND_ONLY";

    private static final Pattern PACKAGE_DECL = Pattern.compile("^package\\s+(\\S+);");
    private static final Pattern JAVA_PACKAGE = Pattern.compile(
            "option\\s+java_package\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern JAVA_OUTER_CLASSNAME = Pattern.compile(
            "option\\s+java_outer_classname\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern MESSAGE_START = Pattern.compile("^message\\s+(\\w+)\\s*\\{");
    private static final Pattern EVENTBUS_TAG = Pattern.compile(
            "option\\s*\\(spec\\.service_event\\)\\s*=\\s*\\{[^}]*label:\\s*\"eventbus\"");
    // Field carrying the topic, e.g.  AgentEvent event_stream = 1 [(spec.topic) = true];
    private static final Pattern TOPIC_FIELD = Pattern.compile(
            "([\\w.]+)\\s+\\w+\\s*=\\s*\\d+\\s*\\[[^\\]]*\\(spec\\.topic\\)\\s*=\\s*true");
    // The record key inside the (spec.record) literal, e.g. { key: "event_id" ... }.
    // Matched only against recordOptionBlock's brace-balanced text, and only on a
    // bare "key:" — "\\(spec\\.record\\)...key:" with DOTALL would run past the
    // literal's closing brace to find any key: later in the file, and a bare
    // "key:" also matches the tail of sort_key:/partition_key:/store_key:.
    private static final Pattern RECORD_KEY = Pattern.compile(
            "(?<!\\w)key:\\s*\"(\\w+)\"");
    // Start of the (spec.record) annotation literal. The retention clause is read
    // only from INSIDE that literal's balanced braces (see recordOptionBlock):
    // retention is delivery-side state and only counts where delivery is declared.
    // A regex alone cannot do this — ".*?" between "(spec.record) = {" and
    // "retention:" runs straight past the literal's closing brace and would happily
    // pick up a retention clause on a (spec.transformation) further down the
    // message, which is exactly the coupling BIG GOAL 3 item 3 removed.
    private static final Pattern RECORD_OPTION_START = Pattern.compile(
            "\\(spec\\.record\\)\\s*=\\s*\\{");
    // The retention clause, matched only within the record option literal, e.g.
    //   retention: {timestamp_field: "occurred_at_ms" duration_seconds: 7776000}
    private static final Pattern RETENTION_TIMESTAMP_FIELD = Pattern.compile(
            "retention:\\s*\\{[^}]*timestamp_field:\\s*\"(\\w+)\"");

    // The residency clause, matched only within the record option literal, e.g.
    //   residency: {window: RESIDENCY_WARM window_seconds: 31536000}
    private static final Pattern RESIDENCY_WINDOW = Pattern.compile(
            "residency:\\s*\\{[^}]*window:\\s*(RESIDENCY_\\w+)");
    private static final Pattern RESIDENCY_WINDOW_SECONDS = Pattern.compile(
            "residency:\\s*\\{[^}]*window_seconds:\\s*(\\d+)");
    // The mutability clause, e.g.  mutability: MUTABILITY_APPEND_ONLY
    // It is a bare enum value on the record option, not a nested literal.
    private static final Pattern MUTABILITY = Pattern.compile(
            "(?<!\\w)mutability:\\s*(MUTABILITY_\\w+)");
    // The field carrying (spec.store_epoch), e.g.
    //   int64 store_epoch_ms = 9 [(spec.store_epoch) = true, (spec.query_field) = {op: QUERY_OP_RANGE}];
    private static final Pattern STORE_EPOCH_FIELD = Pattern.compile(
            "\\w[\\w.]*\\s+(\\w+)\\s*=\\s*\\d+\\s*\\[[^\\]]*\\(spec\\.store_epoch\\)\\s*=\\s*true");

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
        // Every parse below runs over comment-free text. A commented-out
        // annotation is not a declaration: left in, a commented (spec.query_field)
        // produced a phantom filter field stratum implemented and tacplus's
        // descriptor-based side never emitted, and a "{" or "}" in prose threw off
        // the brace-balanced message scan.
        List<String> lines = stripLineComments(Files.readAllLines(proto));
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
            String recordOption = recordOptionBlock(eventBody);
            boolean isRecord = !recordOption.isEmpty();
            result.add(new TopicEvent(protoPackage, type, relPath,
                    isRecord, parseRecordKey(recordOption), parseQueryFields(eventBody),
                    parseRetentionTimestampField(recordOption),
                    parseResidency(recordOption), parseResidencyWindowSeconds(recordOption),
                    parseMutability(recordOption), parseStoreEpochField(eventBody),
                    javaPackage, outerClassName));
        }
    }

    /**
     * The record's key field, read only from inside its own (spec.record)
     * literal and defaulted the way tacplus defaults it. "" when the message
     * declares no (spec.record) — the one case where there is no key because
     * there is no record.
     */
    private static String parseRecordKey(String recordOption) {
        if (recordOption.isEmpty()) {
            return "";
        }
        Matcher m = RECORD_KEY.matcher(recordOption);
        return m.find() ? m.group(1) : DEFAULT_RECORD_KEY;
    }

    /**
     * The single field this record's retention age is measured from, read from the
     * record's own (spec.record).retention clause.
     *
     * There is exactly one (spec.record) per message, so there is exactly one
     * answer to "how old is this event" — no reconciliation across several
     * declarations is needed or possible. That is the point of putting retention
     * on the delivery declaration: a record may declare any number of
     * transformations, and none of them can disagree about its lifetime.
     */
    private static String parseRetentionTimestampField(String recordOption) {
        if (recordOption.isEmpty()) {
            return "";
        }
        Matcher m = RETENTION_TIMESTAMP_FIELD.matcher(recordOption);
        return m.find() ? m.group(1) : "";
    }

    /**
     * The record's declared residency preset, read only from inside its own
     * (spec.record) literal — the same bounding {@link #recordOptionBlock} gives
     * retention, and for the same reason: residency is a property of the DELIVERY
     * declaration, and an unbounded scan would happily pick up a clause belonging
     * to some other annotation further down the message.
     */
    private static String parseResidency(String recordOption) {
        if (recordOption.isEmpty()) {
            return "";
        }
        Matcher m = RESIDENCY_WINDOW.matcher(recordOption);
        return m.find() ? m.group(1) : "";
    }

    /** The record's window_seconds override, or 0 for "use the configured default". */
    private static long parseResidencyWindowSeconds(String recordOption) {
        if (recordOption.isEmpty()) {
            return 0;
        }
        Matcher m = RESIDENCY_WINDOW_SECONDS.matcher(recordOption);
        return m.find() ? Long.parseLong(m.group(1)) : 0;
    }

    /** The record's declared mutability, "" when it declares none (i.e. CRUD). */
    private static String parseMutability(String recordOption) {
        if (recordOption.isEmpty()) {
            return "";
        }
        Matcher m = MUTABILITY.matcher(recordOption);
        return m.find() ? m.group(1) : "";
    }

    /**
     * The field carrying (spec.store_epoch). Unlike residency and mutability this
     * is a FIELD annotation, not a clause on the record option, so it is read from
     * the message body — the same place (spec.query_field) is read from.
     */
    private static String parseStoreEpochField(String eventBody) {
        Matcher m = STORE_EPOCH_FIELD.matcher(eventBody);
        return m.find() ? m.group(1) : "";
    }

    /**
     * The text of the message's (spec.record) option literal, brace-balanced, or ""
     * when the message declares none. Bounding the scan to this literal is what
     * makes "retention lives on the delivery declaration" enforceable rather than
     * merely documented.
     */
    private static String recordOptionBlock(String eventBody) {
        Matcher start = RECORD_OPTION_START.matcher(eventBody);
        if (!start.find()) {
            return "";
        }
        int open = start.end() - 1; // index of the literal's opening brace
        int depth = 0;
        for (int i = open; i < eventBody.length(); i++) {
            char c = eventBody.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return eventBody.substring(open, i + 1);
                }
            }
        }
        return ""; // unbalanced: protoc will complain about it first
    }

    /** Drops // line comments so commented-out proto cannot be parsed as declared. */
    static List<String> stripLineComments(List<String> lines) {
        List<String> out = new ArrayList<>(lines.size());
        for (String line : lines) {
            int idx = line.indexOf("//");
            out.add(idx >= 0 ? line.substring(0, idx) : line);
        }
        return out;
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
