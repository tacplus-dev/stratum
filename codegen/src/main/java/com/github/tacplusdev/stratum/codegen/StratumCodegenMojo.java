// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.codegen;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
            // Everything under the output dir is this generator's: clear it, so a
            // class an earlier build generated and this one no longer does is gone
            // rather than compiled against types that no longer exist.
            deleteTree(out);
            VServiceWiringGenerator.generate(out, services, plane);
            getLog().info("Generated StratumVServiceBase.java");

            List<RecordDiscovery.RecordDecl> records = RecordDiscovery.discover(protoRoot);
            getLog().info("Discovered " + records.size() + " record type(s):");
            for (RecordDiscovery.RecordDecl r : records) {
                getLog().info("  " + r);
            }
            RecordStoreGenerator.generate(out, records);
            getLog().info("Generated StratumStores.java and StratumRecordTypes.java");

            if (plane != null) {
                // The s2s half is backed by the same record types the hot store
                // holds, so it reuses the record-type list.
                RecordMapImplGenerator.generate(out, plane.s2s, records);
                getLog().info("Generated StratumRecordMaps.java");

                if (plane.c2s != null) {
                    // The c2s half is the same lifecycle under governed verbs, so
                    // its impl is a delegation shim onto the s2s one.
                    RecordQueryImplGenerator.generate(out, plane.c2s, records);
                    getLog().info("Generated StratumRecordQueries.java");
                }
            }

        } catch (IOException e) {
            throw new MojoExecutionException("Stratum codegen failed", e);
        }
    }

    /** Removes a directory tree; absent is fine. */
    private static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(dir)) {
            for (Path p : walk.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }
}
