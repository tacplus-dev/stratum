// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.stratum.codegen;

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

    /** A discovered EventBus topic event type. */
    public static final class TopicEvent {
        /** Proto package of the vservice, e.g. "agentsensor". */
        public final String protoPackage;
        /** Simple event message name, e.g. "AgentEvent". */
        public final String eventType;
        /** Relative proto path, e.g. "vservices/agent-sensor.proto". */
        public final String protoRelPath;

        public TopicEvent(String protoPackage, String eventType, String protoRelPath) {
            this.protoPackage = protoPackage;
            this.eventType = eventType;
            this.protoRelPath = protoRelPath;
        }

        @Override
        public String toString() {
            return protoPackage + "." + eventType + " (" + protoRelPath + ")";
        }
    }

    private static final Pattern PACKAGE_DECL = Pattern.compile("^package\\s+(\\S+);");
    private static final Pattern MESSAGE_START = Pattern.compile("^message\\s+(\\w+)\\s*\\{");
    private static final Pattern EVENTBUS_TAG = Pattern.compile(
            "option\\s*\\(spec\\.service_event\\)\\s*=\\s*\\{[^}]*tag:\\s*\"eventbus\"");
    // Field carrying the topic, e.g.  AgentEvent event_stream = 1 [(spec.topic) = true];
    private static final Pattern TOPIC_FIELD = Pattern.compile(
            "([\\w.]+)\\s+\\w+\\s*=\\s*\\d+\\s*\\[[^\\]]*\\(spec\\.topic\\)\\s*=\\s*true");

    private EventTopicDiscovery() {}

    /** Scans all .proto files under vservicesDir for EventBus topic event types. */
    public static List<TopicEvent> discover(Path vservicesDir) throws IOException {
        List<TopicEvent> result = new ArrayList<>();
        if (!Files.isDirectory(vservicesDir)) {
            return result;
        }
        try (Stream<Path> files = Files.walk(vservicesDir)) {
            files.filter(p -> p.toString().endsWith(".proto"))
                 .sorted()
                 .forEach(proto -> {
                     try {
                         scanProto(proto, vservicesDir, result);
                     } catch (IOException e) {
                         throw new RuntimeException("failed to scan " + proto, e);
                     }
                 });
        }
        return result;
    }

    private static void scanProto(Path proto, Path baseDir, List<TopicEvent> result) throws IOException {
        List<String> lines = Files.readAllLines(proto);
        String relPath = baseDir.getParent().relativize(proto).toString();

        String protoPackage = "";
        for (String line : lines) {
            Matcher m = PACKAGE_DECL.matcher(line.trim());
            if (m.matches()) {
                protoPackage = m.group(1);
                break;
            }
        }

        int i = 0;
        while (i < lines.size()) {
            Matcher msgStart = MESSAGE_START.matcher(lines.get(i).trim());
            if (msgStart.find()) {
                // Collect the message block (brace-balanced).
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
                if (EVENTBUS_TAG.matcher(body).find()) {
                    Matcher topic = TOPIC_FIELD.matcher(body);
                    if (topic.find()) {
                        String type = topic.group(1);
                        // Same-package events only (skip e.g. trace.TraceEvent).
                        if (!type.contains(".")) {
                            result.add(new TopicEvent(protoPackage, type, relPath));
                        }
                    }
                }
                i = j;
            } else {
                i++;
            }
        }
    }
}
