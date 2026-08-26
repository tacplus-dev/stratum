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
 * option (spec.service_s2s) = {label: "stratum"}, then generates
 * StratumVServiceBase.java — an abstract bridge class that stratum's
 * StratumHandler extends.
 *
 * When a new vservice is declared with {label: "stratum"}, rebuilding
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

        Path protoRoot = protoDir.toPath();
        if (!protoRoot.toFile().isDirectory()) {
            getLog().warn("proto directory not found: " + protoRoot + " — skipping codegen");
            return;
        }

        try {
            List<VServiceDiscovery.HostedVService> services =
                    VServiceDiscovery.discover(protoRoot, "stratum");

            getLog().info("Discovered " + services.size() + " hosted vservice(s):");
            for (VServiceDiscovery.HostedVService svc : services) {
                getLog().info("  " + svc);
            }

            // The record query plane's two sibling services. Their names, proto
            // package and message outer class all come off the declaration that
            // produced them, so moving the plane from the record store's own proto
            // onto a (spec.trait) needs no edit here.
            VServiceDiscovery.RecordPlane plane =
                    VServiceDiscovery.discoverRecordPlane(protoRoot, "stratum");
            getLog().info(plane == null
                    ? "No record query plane declared"
                    : "Discovered record query plane: " + plane);

            Path out = outputDir.toPath();
            VServiceWiringGenerator.generate(out, services, plane);
            getLog().info("Generated StratumVServiceBase.java");

            List<EventTopicDiscovery.TopicEvent> allEvents =
                    EventTopicDiscovery.discover(protoRoot);
            // The event store and the query plane operate on record types —
            // messages declaring (spec.record). Plain EventBus topics (e.g.
            // TaskEvent) are consumed by other subscribers but are not stored or
            // queryable via stratum.
            //
            // The test is the annotation's PRESENCE, matching tacplus: it emits the
            // plane's RPC family for every (spec.record) message and defaults a
            // missing key: to event_id. Requiring a declared key here dropped such a
            // record from the impl while its RPCs stayed in the generated interface,
            // which is an abstract-method gap javac rejects.
            List<EventTopicDiscovery.TopicEvent> events = new ArrayList<>();
            for (EventTopicDiscovery.TopicEvent ev : allEvents) {
                if (ev.isRecord) {
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

            if (plane != null) {
                // The s2s half is backed by the same record types the hot store
                // holds, so it reuses the record-type list.
                RecordMapImplGenerator.generate(out, plane.s2s, events);
                getLog().info("Generated StratumRecordMaps.java");

                // The c2s half is the same lifecycle under governed verbs, so its
                // impl is a delegation shim onto the s2s one.
                RecordQueryImplGenerator.generate(out, plane.c2s, events);
                getLog().info("Generated StratumRecordQueries.java");
            }

        } catch (IOException e) {
            throw new MojoExecutionException("Stratum codegen failed", e);
        }
    }
}
