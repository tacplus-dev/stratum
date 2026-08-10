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
 * Discovers virtual services hosted by stratum.
 *
 * Scans proto-annotated/vservices/ for proto files that contain a service with
 * option (spec.service_s2s) = {tag: "<targetTag>"}.
 *
 * When a new vservice is added with {tag: "stratum"}, this discovery
 * automatically picks it up on the next build.
 */
public class VServiceDiscovery {

    /** Describes a discovered hosted vservice. */
    public static class HostedVService {
        /** Proto package name (e.g. "claudesensor"). */
        public final String protoPackage;
        /** gRPC service name (e.g. "StratumQuery"). */
        public final String serviceName;
        /** Relative path of the proto file (e.g. "vservices/claude-sensor.proto"). */
        public final String protoRelPath;

        public HostedVService(String protoPackage, String serviceName, String protoRelPath) {
            this.protoPackage = protoPackage;
            this.serviceName = serviceName;
            this.protoRelPath = protoRelPath;
        }

        @Override
        public String toString() {
            return protoPackage + "." + serviceName + " (" + protoRelPath + ")";
        }
    }

    // Matches: service <Name> { ... option (spec.service_s2s) = {tag: "<tag>"}; ... }
    // We do a simplified two-pass: first find service blocks, then check for the tag.
    private static final Pattern SERVICE_START = Pattern.compile("^service\\s+(\\w+)\\s*\\{");
    private static final Pattern SERVICE_S2S_TAG = Pattern.compile(
            "option\\s*\\(spec\\.service_s2s\\)\\s*=\\s*\\{[^}]*tag:\\s*\"([^\"]+)\"");
    private static final Pattern PACKAGE_DECL = Pattern.compile("^package\\s+(\\S+);");

    /**
     * Scans all .proto files under vservicesDir and returns services tagged with targetTag.
     */
    public static List<HostedVService> discover(Path vservicesDir, String targetTag) throws IOException {
        List<HostedVService> result = new ArrayList<>();
        if (!Files.isDirectory(vservicesDir)) {
            return result;
        }
        try (Stream<Path> files = Files.walk(vservicesDir)) {
            files.filter(p -> p.toString().endsWith(".proto"))
                 .sorted()
                 .forEach(proto -> {
                     try {
                         scanProto(proto, vservicesDir, targetTag, result);
                     } catch (IOException e) {
                         throw new RuntimeException("failed to scan " + proto, e);
                     }
                 });
        }
        return result;
    }

    private static void scanProto(Path proto, Path baseDir, String targetTag,
                                  List<HostedVService> result) throws IOException {
        List<String> lines = Files.readAllLines(proto);
        String relPath = baseDir.getParent().relativize(proto).toString();

        // Extract proto package
        String protoPackage = "";
        for (String line : lines) {
            Matcher m = PACKAGE_DECL.matcher(line.trim());
            if (m.matches()) {
                protoPackage = m.group(1);
                break;
            }
        }

        // Find service blocks and check each for the target tag.
        // We track brace depth to find the boundaries of each service block.
        int i = 0;
        while (i < lines.size()) {
            String line = lines.get(i).trim();
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
                // Check if this service block has the target tag
                Matcher tagMatcher = SERVICE_S2S_TAG.matcher(block.toString());
                if (tagMatcher.find() && targetTag.equals(tagMatcher.group(1))) {
                    result.add(new HostedVService(protoPackage, serviceName, relPath));
                }
                i = j;
            } else {
                i++;
            }
        }
    }
}
