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
 * Discovers the services stratum has to implement, on BOTH caller surfaces.
 *
 * Two kinds of service land here:
 *
 *   1. Hand-written vservices hosted by stratum — a service declaring
 *      option (spec.service_s2s) = {label: "stratum"} in a vservices/ proto.
 *      {@link VServiceWiringGenerator} turns each into an abstract provider on
 *      StratumVServiceBase.
 *
 *   2. The synthesized record query plane — the two sibling services tacplus
 *      appends for a (spec.trait) whose host is a record store. They are marked
 *      in the proto by a {@code // tacplus:generated-service recordmap} sentinel
 *      and are implemented by generated code (StratumRecordMaps /
 *      StratumRecordQueries), not by a hand-written provider.
 *
 * The plane's service NAMES, proto package and Java outer class all come off
 * the declaration that produced it — they used to be constants here, which is
 * why moving the plane out of services/stratum.proto and onto a trait would
 * otherwise have needed a hand-edit in three files.
 */
public class VServiceDiscovery {

    /** Which caller surface a service declares. A service declares exactly one. */
    public enum Surface { S2S, C2S }

    /** tacplus's synthesized-service kind for the record query plane. */
    public static final String KIND_RECORD_MAP = "recordmap";

    /** Describes a discovered service stratum has to serve. */
    public static class HostedVService {
        /** Proto package name (e.g. "claudesensor"). */
        public final String protoPackage;
        /** gRPC service name (e.g. "StratumQuery"). */
        public final String serviceName;
        /** Relative path of the proto file (e.g. "vservices/claude-sensor.proto"). */
        public final String protoRelPath;
        /** First path segment of protoRelPath — "services" or "vservices". */
        public final String parentDir;
        /** Caller surface the service declares. */
        public final Surface surface;
        /** Routing label from the surface option, "" when it declares none. */
        public final String hostLabel;
        /** tacplus synthesized-service kind, "" for a hand-written service. */
        public final String synthesizedKind;
        /** Java package from option java_package. */
        public final String javaPackage;
        /** Java outer class name from option java_outer_classname. */
        public final String outerClassName;

        public HostedVService(String protoPackage, String serviceName, String protoRelPath,
                              String parentDir, Surface surface, String hostLabel,
                              String synthesizedKind, String javaPackage, String outerClassName) {
            this.protoPackage = protoPackage;
            this.serviceName = serviceName;
            this.protoRelPath = protoRelPath;
            this.parentDir = parentDir;
            this.surface = surface;
            this.hostLabel = hostLabel;
            this.synthesizedKind = synthesizedKind;
            this.javaPackage = javaPackage;
            this.outerClassName = outerClassName;
        }

        /**
         * FQN of the generated server wrapper class whose nested
         * {@code <Name>Handler} interface the implementation implements, e.g.
         * {@code ...model.s2sm.internal.grpc.servers.vservices.taskquery.RecordMap}.
         */
        public String wrapperFqn(String modelBase, String smPrefix) {
            return modelBase + "." + smPrefix + ".internal.grpc.servers."
                    + parentDir + "." + protoPackage + "." + serviceName;
        }

        /** FQN of the generated message outer class for this service's proto file. */
        public String messagesFqn() {
            return javaPackage + "." + outerClassName;
        }

        @Override
        public String toString() {
            return protoPackage + "." + serviceName + " (" + protoRelPath + ", " + surface + ")";
        }
    }

    /** The record query plane: the two sibling services tacplus emits for a trait. */
    public static final class RecordPlane {
        public final HostedVService s2s;
        public final HostedVService c2s;

        public RecordPlane(HostedVService s2s, HostedVService c2s) {
            this.s2s = s2s;
            this.c2s = c2s;
        }

        @Override
        public String toString() {
            return s2s.serviceName + " (s2s) + " + c2s.serviceName + " (c2s) in " + s2s.protoRelPath;
        }
    }

    private static final Pattern SERVICE_START = Pattern.compile("^service\\s+(\\w+)\\s*\\{");
    private static final Pattern SERVICE_S2S = Pattern.compile(
            "option\\s*\\(spec\\.service_s2s\\)\\s*=\\s*\\{([^}]*)\\}");
    private static final Pattern SERVICE_C2S = Pattern.compile(
            "option\\s*\\(spec\\.service_c2s\\)\\s*=\\s*\\{([^}]*)\\}");
    private static final Pattern SURFACE_LABEL = Pattern.compile("label:\\s*\"([^\"]+)\"");
    private static final Pattern GENERATED_SENTINEL = Pattern.compile(
            "^//\\s*tacplus:generated-service\\s+(\\w+)");
    private static final Pattern PACKAGE_DECL = Pattern.compile("^package\\s+(\\S+);");
    private static final Pattern JAVA_PACKAGE = Pattern.compile(
            "option\\s+java_package\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern JAVA_OUTER_CLASSNAME = Pattern.compile(
            "option\\s+java_outer_classname\\s*=\\s*\"([^\"]+)\"");

    /**
     * Hand-written vservices hosted by this label: S2S surface, declared label,
     * no synthesized-service sentinel. The record plane is deliberately excluded
     * — it is implemented by generated code, not by a provider method.
     */
    public static List<HostedVService> discover(Path protoRoot, String targetLabel) throws IOException {
        List<HostedVService> result = new ArrayList<>();
        for (HostedVService svc : scanAll(protoRoot)) {
            if (svc.surface == Surface.S2S
                    && svc.synthesizedKind.isEmpty()
                    && targetLabel.equals(svc.hostLabel)) {
                result.add(svc);
            }
        }
        return result;
    }

    /**
     * The record query plane this service hosts, or null when the proto set
     * declares none.
     *
     * A plane carrying a label names its host explicitly — that is the
     * trait-driven shape, where the plane lives in the trait's own proto and
     * both siblings carry {@code label: "<host>"}. A plane carrying no label is
     * the pre-trait shape, where tacplus appended it straight into the record
     * store's own services/ proto and the store implements it directly; the only
     * such proto in this service's view is stratum's own.
     */
    public static RecordPlane discoverRecordPlane(Path protoRoot, String targetLabel) throws IOException {
        HostedVService s2s = null;
        HostedVService c2s = null;
        for (HostedVService svc : scanAll(protoRoot)) {
            if (!KIND_RECORD_MAP.equals(svc.synthesizedKind)) {
                continue;
            }
            if (!svc.hostLabel.isEmpty() && !targetLabel.equals(svc.hostLabel)) {
                continue; // a plane hosted by some other service
            }
            if (svc.surface == Surface.S2S) {
                s2s = svc;
            } else {
                c2s = svc;
            }
        }
        if (s2s == null && c2s == null) {
            return null;
        }
        if (s2s == null || c2s == null) {
            throw new IOException("record query plane is half-declared: found only the "
                    + (s2s != null ? "s2s" : "c2s") + " sibling");
        }
        return new RecordPlane(s2s, c2s);
    }

    /** Every service declared under protoRoot, on either caller surface. */
    private static List<HostedVService> scanAll(Path protoRoot) throws IOException {
        List<HostedVService> result = new ArrayList<>();
        if (!Files.isDirectory(protoRoot)) {
            return result;
        }
        try (Stream<Path> files = Files.walk(protoRoot)) {
            files.filter(p -> p.toString().endsWith(".proto"))
                 .sorted()
                 .forEach(proto -> {
                     try {
                         scanProto(proto, protoRoot, result);
                     } catch (IOException e) {
                         throw new RuntimeException("failed to scan " + proto, e);
                     }
                 });
        }
        return result;
    }

    private static void scanProto(Path proto, Path protoRoot, List<HostedVService> result)
            throws IOException {
        List<String> lines = Files.readAllLines(proto);
        String relPath = protoRoot.relativize(proto).toString();
        int slash = relPath.indexOf('/');
        String parentDir = slash > 0 ? relPath.substring(0, slash) : "";

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

        // Find service blocks and read each one's surface option. We track brace
        // depth to find the boundaries of each service block.
        String pendingKind = "";
        int i = 0;
        while (i < lines.size()) {
            String line = lines.get(i).trim();
            Matcher sentinel = GENERATED_SENTINEL.matcher(line);
            if (sentinel.find()) {
                // tacplus writes the sentinel on the line immediately above the
                // service it marks.
                pendingKind = sentinel.group(1);
                i++;
                continue;
            }
            Matcher svcStart = SERVICE_START.matcher(line);
            if (svcStart.find()) {
                String serviceName = svcStart.group(1);
                // Collect the service block lines
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
                String body = block.toString();
                Surface surface = null;
                String options = "";
                Matcher s2s = SERVICE_S2S.matcher(body);
                Matcher c2s = SERVICE_C2S.matcher(body);
                if (s2s.find()) {
                    surface = Surface.S2S;
                    options = s2s.group(1);
                } else if (c2s.find()) {
                    surface = Surface.C2S;
                    options = c2s.group(1);
                }
                if (surface != null) {
                    Matcher label = SURFACE_LABEL.matcher(options);
                    result.add(new HostedVService(protoPackage, serviceName, relPath, parentDir,
                            surface, label.find() ? label.group(1) : "", pendingKind,
                            javaPackage, outerClassName));
                }
                pendingKind = "";
                i = j;
            } else {
                if (!line.isEmpty()) {
                    pendingKind = "";
                }
                i++;
            }
        }
    }
}
