// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.codegen;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Discovers the record types the store holds: every message, in every proto
 * of the generation, that declares {@code option (spec.record)}.
 *
 * <p>A record is a record by that annotation alone. It used to be found through
 * the event-bus topic that carried it, because the store ingested records from
 * the bus; records are now written through the store's generated write surface
 * and the store declares {@code no_bus_ingest} (stratum#2), so a record needs no
 * topic to be held here.
 *
 * <p>This discovery feeds every stratum generator, so stratum's hand-written
 * code never names a record type. It reads .proto TEXT, so it applies the same
 * defaults tacplus applies wherever a declaration leaves something out.
 */
public final class RecordDiscovery {

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

    /** A discovered record type. */
    public static final class RecordDecl {
        /** Proto package, e.g. "letsencrypt". */
        public final String protoPackage;
        /** Simple message name, e.g. "LetsEncryptCert". */
        public final String typeName;
        /** Relative proto path, e.g. "services/letsencrypt.proto". */
        public final String protoRelPath;
        /**
         * Record key field from {@code (spec.record).key}, defaulted to
         * {@value RecordDiscovery#DEFAULT_RECORD_KEY} when the record declares
         * none — the same default tacplus applies (model.DefaultRecordKeyField).
         */
        public final String recordKey;
        /**
         * The key field's proto type: "string", or an integer type ("int64",
         * "uint64", ...). The redo contract keys an entry by a string or an
         * integer (redolog.RedoKey), so this picks the case.
         */
        public final String keyProtoType;
        /** Queryable fields (spec.query_field) — empty if the record declares none. */
        public final List<QueryField> queryFields;
        /**
         * (spec.record).mutability — "MUTABILITY_APPEND_ONLY" when the record type
         * is written once, "" otherwise (i.e. CRUD).
         */
        public final String mutability;
        /**
         * The record field carrying (spec.store_namespace) — the row's owning
         * TENANT. tacplus makes it mandatory on every (spec.record), so "" means
         * the declaration did not reach this discovery, which the generators
         * refuse rather than emit an unscoped query for.
         */
        public final String storeNamespaceField;
        /**
         * The record field carrying (spec.deleted_flag) — the one bool that records
         * a row's soft-delete state. "" when the record declares no marker.
         *
         * The detection rule mirrors tacplus's own parser.DeletedFlagField: exactly
         * one non-repeated bool field carrying the marker, and "" for anything else.
         */
        public final String deletedFlagField;
        /**
         * (spec.record).private — tacplus emits NO S2S RecordMap RPCs for such a
         * type, so the generated s2s impl methods for it must not be declared as
         * overrides of an interface that does not carry them.
         */
        public final boolean isPrivate;
        /**
         * (spec.record).durability — the level a write of this type is acked at:
         * "DURABILITY_MEMORY", "DURABILITY_LOCAL" or "DURABILITY_REMOTE". Always a
         * value: {@value RecordDiscovery#DEFAULT_DURABILITY} when the record
         * declares none, tacplus's default.
         */
        public final String durability;
        /** Java package from option java_package, e.g. "...model.types.services.letsencrypt". */
        public final String javaPackage;
        /** Java outer class name from option java_outer_classname, e.g. "Letsencrypt". */
        public final String outerClassName;

        public RecordDecl(String protoPackage, String typeName, String protoRelPath,
                          String recordKey, String keyProtoType, List<QueryField> queryFields,
                          String mutability, String storeNamespaceField, String deletedFlagField,
                          boolean isPrivate, String durability,
                          String javaPackage, String outerClassName) {
            this.protoPackage = protoPackage;
            this.typeName = typeName;
            this.protoRelPath = protoRelPath;
            this.recordKey = recordKey;
            this.keyProtoType = keyProtoType;
            this.queryFields = queryFields;
            this.mutability = mutability;
            this.storeNamespaceField = storeNamespaceField;
            this.deletedFlagField = deletedFlagField;
            this.isPrivate = isPrivate;
            this.durability = durability;
            this.javaPackage = javaPackage;
            this.outerClassName = outerClassName;
        }

        /** True when the record type is written once and never updated or deleted. */
        public boolean isAppendOnly() {
            return MUTABILITY_APPEND_ONLY.equals(mutability);
        }

        /** The record's full proto name, e.g. "letsencrypt.LetsEncryptCert". */
        public String fullName() {
            return protoPackage + "." + typeName;
        }

        /** True when the key is a string; false for an integer key. */
        public boolean stringKey() {
            return "string".equals(keyProtoType);
        }

        @Override
        public String toString() {
            return fullName() + " (" + protoRelPath + ") key=" + recordKey + " " + durability
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

    /** The durability tacplus gives a record that declares none (spec.Durability). */
    public static final String DEFAULT_DURABILITY = "DURABILITY_LOCAL";

    /** The mutability preset that makes a record type write-once. */
    public static final String MUTABILITY_APPEND_ONLY = "MUTABILITY_APPEND_ONLY";

    private static final Pattern PACKAGE_DECL = Pattern.compile("^package\\s+(\\S+);");
    private static final Pattern JAVA_PACKAGE = Pattern.compile(
            "option\\s+java_package\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern JAVA_OUTER_CLASSNAME = Pattern.compile(
            "option\\s+java_outer_classname\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern MESSAGE_START = Pattern.compile("^message\\s+(\\w+)\\s*\\{");
    // The record key inside the (spec.record) literal, e.g. { key: "event_id" ... }.
    // Matched only against recordOptionBlock's brace-balanced text, and only on a
    // bare "key:" — a bare "key:" also matches the tail of sort_key:/partition_key:,
    // which the lookbehind excludes.
    private static final Pattern RECORD_KEY = Pattern.compile(
            "(?<!\\w)key:\\s*\"(\\w+)\"");
    // Start of the (spec.record) annotation literal. Every clause is read only from
    // INSIDE that literal's balanced braces (see recordOptionBlock): a regex alone
    // would run past the literal's closing brace into a later message.
    private static final Pattern RECORD_OPTION_START = Pattern.compile(
            "\\(spec\\.record\\)\\s*=\\s*\\{");
    // The mutability clause, e.g.  mutability: MUTABILITY_APPEND_ONLY
    private static final Pattern MUTABILITY = Pattern.compile(
            "(?<!\\w)mutability:\\s*(MUTABILITY_\\w+)");
    // The durability clause, e.g.  durability: DURABILITY_REMOTE
    private static final Pattern DURABILITY = Pattern.compile(
            "(?<!\\w)durability:\\s*(DURABILITY_\\w+)");
    // Whether the record option declares an s2s_read grant at all. Presence, not
    // content, is what gates the key criterion below — this mirrors tacplus's own
    // gate (record_map.go's declaresRead).
    private static final Pattern S2S_READ = Pattern.compile(
            "(?<!\\w)s2s_read:\\s*\\{");
    // The field carrying (spec.store_namespace) — the row's owning tenant, e.g.
    //   string namespace = 26 [(spec.store_namespace) = true, (spec.assert_max_size) = 256];
    private static final Pattern STORE_NAMESPACE_FIELD = Pattern.compile(
            "\\w[\\w.]*\\s+(\\w+)\\s*=\\s*\\d+\\s*\\[[^\\]]*\\(spec\\.store_namespace\\)\\s*=\\s*true");
    // The field carrying (spec.deleted_flag), e.g.
    //   bool deleted = 7 [(spec.deleted_flag) = true];
    // "(repeated )?<type>" is captured so the malformed declarations tacplus
    // refuses (DF-2/DF-3) can be told apart from an absent marker below.
    private static final Pattern DELETED_FLAG_FIELD = Pattern.compile(
            "(?<!\\w)(repeated\\s+)?(\\w[\\w.]*)\\s+(\\w+)\\s*=\\s*\\d+\\s*\\[[^\\]]*\\(spec\\.deleted_flag\\)\\s*=\\s*true");
    // A field carrying (spec.query_field), e.g.
    //   string spiffe_id = 4 [(spec.query_field) = {op: QUERY_OP_EQ}];
    private static final Pattern QUERY_FIELD = Pattern.compile(
            "(\\w[\\w.]*)\\s+(\\w+)\\s*=\\s*\\d+\\s*\\[[^\\]]*\\(spec\\.query_field\\)\\s*=\\s*\\{([^}]*)\\}");
    // Matched only against recordOptionBlock's brace-balanced text.
    private static final Pattern RECORD_PRIVATE = Pattern.compile("\\bprivate\\s*:\\s*true\\b");

    private RecordDiscovery() {}

    /** Scans all .proto files under baseDir for record types. */
    public static List<RecordDecl> discover(Path baseDir) throws IOException {
        List<RecordDecl> result = new ArrayList<>();
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

    private static void scanProto(Path proto, Path baseDir, List<RecordDecl> result) throws IOException {
        // Every parse below runs over comment-free text. A commented-out
        // annotation is not a declaration, and a "{" or "}" in prose would throw
        // off the brace-balanced message scan.
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

        // Every top-level message's brace-balanced body, in file order.
        int i = 0;
        while (i < lines.size()) {
            Matcher msgStart = MESSAGE_START.matcher(lines.get(i).trim());
            if (!msgStart.find()) {
                i++;
                continue;
            }
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
            i = j;

            String body = block.toString();
            String recordOption = recordOptionBlock(body);
            if (recordOption.isEmpty()) {
                continue;
            }
            String recordKey = parseRecordKey(recordOption);
            List<QueryField> queryFields = parseQueryFields(body);
            if (S2S_READ.matcher(recordOption).find()) {
                queryFields = withKeyCriterion(queryFields, recordKey, body);
            }
            result.add(new RecordDecl(protoPackage, name, relPath,
                    recordKey, keyType(recordKey, body), queryFields,
                    parseMutability(recordOption), parseStoreNamespaceField(body),
                    parseDeletedFlagField(body), parsePrivate(recordOption),
                    parseDurability(recordOption), javaPackage, outerClassName));
        }
    }

    /**
     * The record's key field, read only from inside its own (spec.record)
     * literal and defaulted the way tacplus defaults it.
     */
    private static String parseRecordKey(String recordOption) {
        Matcher m = RECORD_KEY.matcher(recordOption);
        return m.find() ? m.group(1) : DEFAULT_RECORD_KEY;
    }

    /** The key field's declared proto type; "string" when it cannot be found. */
    private static String keyType(String key, String body) {
        Matcher m = fieldDeclPattern(key).matcher(body);
        return m.find() && m.group(1) == null ? m.group(2).trim() : "string";
    }

    /** The record's declared mutability, "" when it declares none (i.e. CRUD). */
    private static String parseMutability(String recordOption) {
        Matcher m = MUTABILITY.matcher(recordOption);
        return m.find() ? m.group(1) : "";
    }

    /**
     * The record's declared durability, {@link #DEFAULT_DURABILITY} when it
     * declares none or declares DURABILITY_UNSPECIFIED (which spec.proto defines
     * as LOCAL).
     */
    static String parseDurability(String recordOption) {
        Matcher m = DURABILITY.matcher(recordOption);
        if (!m.find() || "DURABILITY_UNSPECIFIED".equals(m.group(1))) {
            return DEFAULT_DURABILITY;
        }
        return m.group(1);
    }

    /**
     * The field carrying (spec.store_namespace) — the row's owning tenant. "" only
     * when the message declares none, which tacplus itself refuses for a record
     * (ValidateRecordNamespaceAnnotations).
     */
    private static String parseStoreNamespaceField(String body) {
        Matcher m = STORE_NAMESPACE_FIELD.matcher(body);
        return m.find() ? m.group(1) : "";
    }

    /**
     * The field carrying (spec.deleted_flag), or "" when the record declares none.
     * Mirrors tacplus's parser.DeletedFlagField exactly, including its answers to
     * the cases tacplus's own validator refuses first: a repeated or non-bool
     * marked field (DF-2) and a second marked field (DF-3) both yield "".
     */
    private static String parseDeletedFlagField(String body) {
        Matcher m = DELETED_FLAG_FIELD.matcher(body);
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

    /** True when the (spec.record) literal declares {@code private: true}. */
    static boolean parsePrivate(String recordOption) {
        return RECORD_PRIVATE.matcher(recordOption).find();
    }

    /**
     * The text of the message's (spec.record) option literal, brace-balanced, or ""
     * when the message declares none.
     */
    private static String recordOptionBlock(String body) {
        Matcher start = RECORD_OPTION_START.matcher(body);
        if (!start.find()) {
            return "";
        }
        int open = start.end() - 1; // index of the literal's opening brace
        int depth = 0;
        for (int i = open; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return body.substring(open, i + 1);
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

    private static List<QueryField> parseQueryFields(String body) {
        List<QueryField> fields = new ArrayList<>();
        Matcher m = QUERY_FIELD.matcher(body);
        while (m.find()) {
            String protoType = m.group(1);
            String name = m.group(2);
            boolean range = m.group(3).contains("QUERY_OP_RANGE");
            fields.add(new QueryField(name, protoType, range));
        }
        return fields;
    }

    // A field declaration for a given name, e.g. "string spiffe_id = 4" or
    // "repeated string tags = 5" or "map<string, string> labels = 6".
    private static Pattern fieldDeclPattern(String name) {
        return Pattern.compile("(?<!\\w)(repeated\\s+)?([\\w.]+(?:\\s*<[^>]*>)?)\\s+"
                + Pattern.quote(name) + "\\s*=\\s*\\d+");
    }

    /**
     * Folds a record's key field into its query fields as an implicit EQ
     * criterion, when it is not already an explicit one.
     *
     * tacplus folds the key into the plane's {@code <T>Filter} whenever the record
     * declares (spec.record).s2s_read (record_map.go, appendKeyFilterCriterion,
     * gated on declaresRead), even when the key carries no (spec.query_field).
     * Stratum's match<T>() is generated from this class's OWN query field list,
     * so without this it never checked the key criterion a reader sent and a
     * point read for a wrong id matched every row. Skipped when the key's own
     * declaration is repeated or a map, as tacplus skips it.
     */
    private static List<QueryField> withKeyCriterion(List<QueryField> fields, String key, String body) {
        for (QueryField qf : fields) {
            if (qf.name.equals(key)) {
                return fields;
            }
        }
        Matcher m = fieldDeclPattern(key).matcher(body);
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
