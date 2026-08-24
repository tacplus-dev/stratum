// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.codegen;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Maven mojo that generates vservice wiring for stratum.
 *
 * Reads proto-annotated vservices/ to discover virtual services with
 * option (spec.service_s2s) = {tag: "stratum"}, then generates
 * StratumVServiceBase.java — an abstract bridge class that stratum's
 * StratumHandler extends.
 *
 * When a new vservice is declared with {tag: "stratum"}, rebuilding
 * stratum automatically adds the new abstract method to StratumVServiceBase.
 */
@Mojo(name = "generate-vservice-wiring", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class StratumCodegenMojo extends AbstractMojo {

    /**
     * Directory containing the annotated proto files written by tacplus.
     * This is the proto-annotated/ directory produced during code generation.
     */
    @Parameter(property = "protoDir",
               defaultValue = "${project.basedir}/../model/target/generated-sources/model/proto-annotated")
    private File protoDir;

    /**
     * Output directory for generated Java sources.
     */
    @Parameter(property = "outputDir",
               defaultValue = "${project.build.directory}/generated-sources/stratum-codegen/src/main/java")
    private File outputDir;

    @Override
    public void execute() throws MojoExecutionException {
        getLog().info("Stratum codegen: scanning for hosted vservices in " + protoDir);

        Path vservicesDir = protoDir.toPath().resolve("vservices");
        if (!vservicesDir.toFile().exists()) {
            getLog().warn("vservices directory not found: " + vservicesDir + " — skipping codegen");
            return;
        }

        try {
            List<VServiceDiscovery.HostedVService> services =
                    VServiceDiscovery.discover(vservicesDir, "stratum");

            getLog().info("Discovered " + services.size() + " hosted vservice(s):");
            for (VServiceDiscovery.HostedVService svc : services) {
                getLog().info("  " + svc);
            }

            Path out = outputDir.toPath();
            VServiceWiringGenerator.generate(out, services);
            getLog().info("Generated StratumVServiceBase.java");

            List<EventTopicDiscovery.TopicEvent> allEvents =
                    EventTopicDiscovery.discover(protoDir.toPath());
            // The event store and RecordMap query plane operate on record types
            // (spec.record, identified by a non-empty recordKey). Plain EventBus
            // topics (e.g. TaskEvent) are consumed by other subscribers but are not
            // stored or queryable via stratum.
            List<EventTopicDiscovery.TopicEvent> events = new ArrayList<>();
            for (EventTopicDiscovery.TopicEvent ev : allEvents) {
                if (!ev.recordKey.isEmpty()) {
                    events.add(ev);
                }
            }
            getLog().info("Discovered " + events.size() + " record type(s) of "
                    + allEvents.size() + " EventBus topic(s):");
            for (EventTopicDiscovery.TopicEvent ev : events) {
                getLog().info("  " + ev);
            }
            EventStoreGenerator.generate(out, events);
            getLog().info("Generated StratumEventStores.java");

            // The record map plane (RecordMap) is backed by the same record types
            // the hot store holds, so it reuses the record-type list.
            RecordMapImplGenerator.generate(out, events);
            getLog().info("Generated StratumRecordMaps.java");

            // The c2s half (RecordQuery) is the same lifecycle under governed
            // verbs, so its impl is a delegation shim onto the s2s one.
            RecordQueryImplGenerator.generate(out, events);
            getLog().info("Generated StratumRecordQueries.java");

        } catch (IOException e) {
            throw new MojoExecutionException("Stratum codegen failed", e);
        }
    }
}
