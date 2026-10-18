// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.codegen;

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
         * (spec.record).residency.idle_seconds — how long a row may go with NO
         * read and NO write before Hazelcast evicts it from memory. Always a
         * value: {@value EventTopicDiscovery#DEFAULT_RESIDENCY_IDLE_SECONDS}
         * when the record declares none or declares 0.
         *
         * 0/unset means "apply the platform default", never "never" — that is
         * tacplus's own rule (parser.DefaultResidencyIdleSeconds), and the
         * default is applied here rather than left for a consumer to guess,
         * exactly as {@link #minDurableCopies} defaults spec.proto's number.
         * Under the one-clock lifecycle every record ages out of memory; a
         * long-lived, config-shaped type just declares a long idle_seconds.
         *
         * The row is not lost when it is evicted: it lives on disk beneath and
         * the next touch reloads it. This clock is MEMORY only — a read never
         * restamps (spec.store_epoch) and never writes the event log.
         */
        public final long residencyIdleSeconds;
        /**
         * (spec.record).residency.deleted_max_age_seconds — how long a row
         * carrying a set {@link #deletedFlagField} survives after the delete
         * write before the sweep purges it. Always a value:
         * {@value EventTopicDiscovery#DEFAULT_RESIDENCY_DELETED_MAX_AGE_SECONDS}
         * when the record declares none or declares 0, on tacplus's same
         * "0 means the platform default" rule.
         *
         * Measured from the delete write's (spec.store_epoch), which is why
         * store_epoch keeps its meaning as the last WRITE and is not the
         * residency clock.
         */
        public final long residencyDeletedMaxAgeSeconds;
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
        /**
         * The record field carrying (spec.store_namespace) — the row's owning
         * TENANT, stamped by the generated wrapper at publish time. "" when the
         * message declares none.
         *
         * This is the field the query plane scopes a caller's handle to, and the
         * same field the generated wrapper's egress backstop compares against
         * before it halts the JVM. tacplus makes it mandatory on every
         * (spec.record), so "" on a record means the declaration did not reach
         * this discovery — which {@link RecordMapImplGenerator} refuses rather
         * than emitting an unscoped query for.
         */
        public final String storeNamespaceField;
        /**
         * The strictest (spec.min_durable_copies) declared on any field of the
         * record, or {@value EventTopicDiscovery#DEFAULT_MIN_DURABLE_COPIES} when
         * the record declares none — spec.proto's documented default.
         *
         * This is the IN-MEMORY replica count a hot write must reach before it is
         * acked, not a redo-log commit count: the disk leg follows the ack. Below
         * it the write is refused rather than acked locally, so a cluster that has
         * lost members cannot pass for a single-node development tree.
         */
        public final int minDurableCopies;
        /**
         * The record field carrying (spec.deleted_flag) — the one bool that records
         * a row's soft-delete state. "" when the record declares no marker.
         *
         * A flagged row is excluded from List / Watch / Count / Sample by default;
         * a caller opts back in with the query plane's include_deleted. "" means the
         * record type has no soft-delete state at all, and the generated query plane
         * is then byte-identical to the pre-marker one — which is why this is a
         * plain absence and not an error: tacplus emits include_deleted on every
         * record's Open/Refine message whether or not the type carries a marker, so
         * the fleet annotation pass can land one type at a time.
         *
         * The detection rule mirrors tacplus's own parser.DeletedFlagField
         * (pkg/internal/common/parser/deleted_flag_validate.go): exactly one
         * non-repeated bool field carrying the marker, and "" for anything else.
         * tacplus refuses to generate a file that breaks DF-1..DF-3, so the
         * malformed cases cannot reach here; answering "" for them generates the
         * pre-marker plane rather than a wrong one, exactly as tacplus does.
         */
        public final String deletedFlagField;
        /**
         * (spec.record).private — true when the record belongs to exactly one
         * principal and every read pins owner == caller. tacplus emits NO S2S
         * RecordMap RPCs for such a type (record_map.go skips it: a peer calls
         * with its own service identity and the owner pin has no caller
         * principal to bind to), so the generated s2s impl methods for it must
         * not be declared as overrides of an interface that does not carry them.
         * The C2S plane is untouched and still delegates to those methods.
         */
        public final boolean isPrivate;
        /** Java package from option java_package, e.g. "...model.types.vservices.agentsensor". */
        public final String javaPackage;
        /** Java outer class name from option java_outer_classname, e.g. "Agentsensor". */
        public final String outerClassName;

        public TopicEvent(String protoPackage, String eventType, String protoRelPath,
                          boolean isRecord, String recordKey, List<QueryField> queryFields,
                          long residencyIdleSeconds, long residencyDeletedMaxAgeSeconds,
                          String mutability,
                          String storeEpochField, String storeNamespaceField, int minDurableCopies,
                          String deletedFlagField, boolean isPrivate,
                          String javaPackage, String outerClassName) {
            this.protoPackage = protoPackage;
            this.eventType = eventType;
            this.protoRelPath = protoRelPath;
            this.isRecord = isRecord;
            this.recordKey = recordKey;
            this.queryFields = queryFields;
            this.residencyIdleSeconds = residencyIdleSeconds;
            this.residencyDeletedMaxAgeSeconds = residencyDeletedMaxAgeSeconds;
            this.mutability = mutability;
            this.storeEpochField = storeEpochField;
            this.storeNamespaceField = storeNamespaceField;
            this.minDurableCopies = minDurableCopies;
            this.deletedFlagField = deletedFlagField;
            this.isPrivate = isPrivate;
            this.javaPackage = javaPackage;
            this.outerClassName = outerClassName;
        }

        /** True when the record type is written once and never updated or deleted. */
        public boolean isAppendOnly() {
            return MUTABILITY_APPEND_ONLY.equals(mutability);
        }

        @Override
        public String toString() {
            return protoPackage + "." + eventType + " (" + protoRelPath + ")"
                    + (queryFields.isEmpty() ? "" : " query=" + queryFields.size())
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

    /**
     * The in-memory replica count a hot write must reach when the record declares
     * no (spec.min_durable_copies) of its own — spec.proto's documented default.
     */
    public static final int DEFAULT_MIN_DURABLE_COPIES = 2;

    /**
     * The effective (spec.record).residency.idle_seconds for a record that
     * declares none or declares 0 — one day with neither a read nor a write
     * before the row leaves memory.
     *
     * This mirrors tacplus's parser.DefaultResidencyIdleSeconds
     * (pkg/internal/common/parser/residency_validate.go), which is the platform
     * authority on the number. It is restated here for the same reason
     * {@link #DEFAULT_MIN_DURABLE_COPIES} and {@link #DEFAULT_RECORD_KEY} are:
     * stratum's generator reads .proto TEXT, so it has to apply the same
     * defaults tacplus applies or the two sides disagree about what an
     * undeclared knob means.
     */
    public static final long DEFAULT_RESIDENCY_IDLE_SECONDS = 86400L;

    /**
     * The effective (spec.record).residency.deleted_max_age_seconds for a record
     * that declares none or declares 0 — thirty days between the delete write
     * and the sweep that makes the row gone.
     *
     * Mirrors tacplus's parser.DefaultResidencyDeletedMaxAgeSeconds, same source
     * and same reason as {@link #DEFAULT_RESIDENCY_IDLE_SECONDS}.
     */
    public static final long DEFAULT_RESIDENCY_DELETED_MAX_AGE_SECONDS = 2592000L;
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
    // The two lifecycle clocks on the same residency clause, e.g.
    //   residency: {idle_seconds: 3600 deleted_max_age_seconds: 2592000}
    //
    // "(?<!\\w)" on idle_seconds keeps it off window_seconds and off any other
    // *_seconds knob that ends in the same letters; deleted_max_age_seconds is
    // already unambiguous. Both are read from inside the record option literal
    // only, the same bound every other clause here is read under.
    private static final Pattern RESIDENCY_IDLE_SECONDS = Pattern.compile(
            "residency:\\s*\\{[^}]*(?<!\\w)idle_seconds:\\s*(\\d+)");
    private static final Pattern RESIDENCY_DELETED_MAX_AGE_SECONDS = Pattern.compile(
            "residency:\\s*\\{[^}]*(?<!\\w)deleted_max_age_seconds:\\s*(\\d+)");
    // The mutability clause, e.g.  mutability: MUTABILITY_APPEND_ONLY
    // It is a bare enum value on the record option, not a nested literal.
    private static final Pattern MUTABILITY = Pattern.compile(
            "(?<!\\w)mutability:\\s*(MUTABILITY_\\w+)");
    // Whether the record option declares an s2s_read grant at all, e.g.
    //   s2s_read: {auth_services: ["scotty"]}
    // Presence, not content, is what gates the key criterion below — this
    // mirrors tacplus's own gate (record_map.go's declaresRead), which folds
    // the key into <T>Filter only when the record declares a reader.
    private static final Pattern S2S_READ = Pattern.compile(
            "(?<!\\w)s2s_read:\\s*\\{");
    // The field carrying (spec.store_epoch), e.g.
    //   int64 store_epoch_ms = 9 [(spec.store_epoch) = true, (spec.query_field) = {op: QUERY_OP_RANGE}];
    private static final Pattern STORE_EPOCH_FIELD = Pattern.compile(
            "\\w[\\w.]*\\s+(\\w+)\\s*=\\s*\\d+\\s*\\[[^\\]]*\\(spec\\.store_epoch\\)\\s*=\\s*true");

    // The field carrying (spec.store_namespace) — the row's owning tenant, e.g.
    //   string namespace = 26 [(spec.store_namespace) = true, (spec.assert_max_size) = 256];
    private static final Pattern STORE_NAMESPACE_FIELD = Pattern.compile(
            "\\w[\\w.]*\\s+(\\w+)\\s*=\\s*\\d+\\s*\\[[^\\]]*\\(spec\\.store_namespace\\)\\s*=\\s*true");

    // A field carrying (spec.min_durable_copies), e.g.
    //   string payload = 3 [(spec.quorum_write) = true, (spec.min_durable_copies) = 3];
    private static final Pattern MIN_DURABLE_COPIES = Pattern.compile(
            "\\(spec\\.min_durable_copies\\)\\s*=\\s*(\\d+)");

    // The field carrying (spec.deleted_flag), e.g.
    //   bool deleted = 7 [(spec.deleted_flag) = true];
    // Read from the message body for the same reason (spec.store_epoch) is: it is a
    // FIELD annotation, not a clause on the record option. The leading
    // "(repeated )?<type>" is captured rather than hard-coded to "bool" so the
    // malformed declarations tacplus refuses (DF-2/DF-3) can be told apart from an
    // absent marker below.
    private static final Pattern DELETED_FLAG_FIELD = Pattern.compile(
            "(?<!\\w)(repeated\\s+)?(\\w[\\w.]*)\\s+(\\w+)\\s*=\\s*\\d+\\s*\\[[^\\]]*\\(spec\\.deleted_flag\\)\\s*=\\s*true");

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
            String recordKey = parseRecordKey(recordOption);
            List<QueryField> queryFields = parseQueryFields(eventBody);
            if (isRecord && S2S_READ.matcher(recordOption).find()) {
                queryFields = withKeyCriterion(queryFields, recordKey, eventBody);
            }
            result.add(new TopicEvent(protoPackage, type, relPath,
                    isRecord, recordKey, queryFields,
                    parseResidencyIdleSeconds(recordOption),
                    parseResidencyDeletedMaxAgeSeconds(recordOption),
                    parseMutability(recordOption), parseStoreEpochField(eventBody),
                    parseStoreNamespaceField(eventBody), parseMinDurableCopies(eventBody),
                    parseDeletedFlagField(eventBody), parsePrivate(recordOption),
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
     * The record's idle_seconds, defaulted to
     * {@link #DEFAULT_RESIDENCY_IDLE_SECONDS} the way tacplus defaults it.
     *
     * The default is applied HERE, not at the consumer, because 0 is not a
     * legal answer on this clock: tacplus documents 0/unset as "apply the
     * platform default", so a consumer handed a 0 would have to re-know the
     * default to make sense of it. That is the mistake
     * {@link #parseMinDurableCopies} already avoids.
     */
    private static long parseResidencyIdleSeconds(String recordOption) {
        if (recordOption.isEmpty()) {
            return 0;
        }
        Matcher m = RESIDENCY_IDLE_SECONDS.matcher(recordOption);
        long declared = m.find() ? Long.parseLong(m.group(1)) : 0;
        return declared > 0 ? declared : DEFAULT_RESIDENCY_IDLE_SECONDS;
    }

    /**
     * The record's deleted_max_age_seconds, defaulted to
     * {@link #DEFAULT_RESIDENCY_DELETED_MAX_AGE_SECONDS} — same rule, same
     * reason as {@link #parseResidencyIdleSeconds}.
     */
    private static long parseResidencyDeletedMaxAgeSeconds(String recordOption) {
        if (recordOption.isEmpty()) {
            return 0;
        }
        Matcher m = RESIDENCY_DELETED_MAX_AGE_SECONDS.matcher(recordOption);
        long declared = m.find() ? Long.parseLong(m.group(1)) : 0;
        return declared > 0 ? declared : DEFAULT_RESIDENCY_DELETED_MAX_AGE_SECONDS;
    }

    /**
     * The strictest (spec.min_durable_copies) on the record's fields, defaulted to
     * {@link #DEFAULT_MIN_DURABLE_COPIES}.
     *
     * A record is written whole, so when several fields declare a floor the
     * strictest one governs — a per-field floor would need a per-field write, and
     * the hot store writes rows.
     */
    private static int parseMinDurableCopies(String eventBody) {
        int min = DEFAULT_MIN_DURABLE_COPIES;
        Matcher m = MIN_DURABLE_COPIES.matcher(eventBody);
        while (m.find()) {
            min = Math.max(min, Integer.parseInt(m.group(1)));
        }
        return min;
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
     * The field carrying (spec.store_namespace) — the row's owning tenant. Read
     * from the message body for the same reason (spec.store_epoch) is: it is a
     * FIELD annotation, not a clause on the record option.
     *
     * "" only when the message declares none, which for a record tacplus itself
     * refuses to generate (ValidateRecordNamespaceAnnotations).
     */
    private static String parseStoreNamespaceField(String eventBody) {
        Matcher m = STORE_NAMESPACE_FIELD.matcher(eventBody);
        return m.find() ? m.group(1) : "";
    }

    /**
     * The field carrying (spec.deleted_flag), or "" when the record declares none.
     *
     * Mirrors tacplus's parser.DeletedFlagField exactly, including its answers to
     * the cases tacplus's own validator refuses first: a repeated or non-bool
     * marked field (DF-2) and a second marked field (DF-3) both yield "", which
     * generates the pre-marker query plane rather than a wrong one. This is a
     * LOOKUP, not a second opinion — well-formedness is enforced once, by tacplus,
     * before any of this runs.
     */
    private static String parseDeletedFlagField(String eventBody) {
        Matcher m = DELETED_FLAG_FIELD.matcher(eventBody);
        String found = "";
        while (m.find()) {
            if (m.group(1) != null || !"bool".equals(m.group(2))) {
                return ""; // DF-2: not a non-repeated bool
            }
            if (!found.isEmpty()) {
                return ""; // DF-3: a row has one soft-delete state
            }
            found = m.group(3);
        }
        return found;
    }

    /**
     * The text of the message's (spec.record) option literal, brace-balanced, or ""
     * when the message declares none. Bounding the scan to this literal is what
     * makes "retention lives on the delivery declaration" enforceable rather than
     * merely documented.
     */
    // Matched only against recordOptionBlock's brace-balanced text.
    private static final Pattern RECORD_PRIVATE = Pattern.compile("\\bprivate\\s*:\\s*true\\b");

    /** True when the (spec.record) literal declares {@code private: true}. */
    static boolean parsePrivate(String recordOption) {
        return RECORD_PRIVATE.matcher(recordOption).find();
    }

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

    // A field declaration for a given name, e.g. "string spiffe_id = 4" or
    // "repeated string tags = 5" or "map<string, string> labels = 6" — used to
    // look up the record key's own proto type below.
    private static Pattern fieldDeclPattern(String name) {
        return Pattern.compile("(?<!\\w)(repeated\\s+)?([\\w.]+(?:\\s*<[^>]*>)?)\\s+"
                + Pattern.quote(name) + "\\s*=\\s*\\d+");
    }

    /**
     * Folds a record's key field into its query fields as an implicit EQ
     * criterion, when it is not already an explicit one.
     *
     * tacplus's own generated Rust reader sets the key as a filter criterion
     * for a point read whenever the record declares (spec.record).s2s_read
     * (pkg/internal/proto/record_map.go, appendKeyFilterCriterion, gated on
     * declaresRead) — the plane's <T>Filter message carries a field for it
     * even when the record's author never annotated the key with
     * (spec.query_field). Stratum's match<T>() is generated from this
     * class's OWN independently-parsed query field list, not from that
     * generated message, so without this it never checked the key criterion
     * a reader sent: a point read for a never-created or wrong id matched
     * every row and returned an arbitrary existing one instead of "not
     * found". Callers gate this on the same s2s_read presence tacplus does —
     * a record with no reader has no <T>Filter to match, so folding the key
     * in anyway just fabricates a field the generated Filter message never
     * has. Also skipped when the key's own declaration is repeated or a
     * map — the same "not a plain scalar" exclusion tacplus applies — since
     * neither can be an EQ criterion.
     */
    private static List<QueryField> withKeyCriterion(List<QueryField> fields, String key, String eventBody) {
        for (QueryField qf : fields) {
            if (qf.name.equals(key)) {
                return fields;
            }
        }
        Matcher m = fieldDeclPattern(key).matcher(eventBody);
        if (!m.find() || m.group(1) != null) {
            return fields; // key field not found, or declared repeated
        }
        String type = m.group(2).trim();
        if (type.startsWith("map")) {
            return fields;
        }
        List<QueryField> withKey = new ArrayList<>(fields);
        withKey.add(new QueryField(key, type, false));
        return withKey;
    }
}
