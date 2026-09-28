// Copyright (c) 2026 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Commented-out proto does not declare a service.
 *
 * <p>{@link EventTopicDiscovery} strips {@code //} comments before it parses a
 * record; {@link VServiceDiscovery} used to parse the raw lines, so the same text
 * meant one thing to one discovery tool and another to its sibling. What that
 * bought was a PHANTOM PROVIDER: a service whose surface option is commented out
 * still matched {@code (spec.service_s2s)} inside the comment, was discovered as
 * an s2s service hosted by this label, and {@link VServiceWiringGenerator} emitted
 * an abstract provider on StratumVServiceBase for a service the proto does not
 * declare on any surface — a compile error in hand-written code, caused by a
 * comment.
 *
 * <p>The other direction is the same bug read backwards: a {@code "}"} inside a
 * comment closed the block early, so the surface option below it was never seen
 * and a REAL service went undiscovered.
 *
 * <p>The one comment that must still be read is tacplus's
 * {@code // tacplus:generated-service} sentinel — it is written as a comment and
 * means what it says, so stripping must not take it with the rest.
 */
class VServiceDiscoveryCommentTest {

    private static Path protoRoot(Path dir, String body) throws IOException {
        Path vservices = dir.resolve("vservices");
        Files.createDirectories(vservices);
        Files.writeString(vservices.resolve("demo.proto"), """
                syntax = "proto3";
                package demo;
                option java_package = "com.example.model.types.vservices.demo";
                option java_outer_classname = "Demo";

                """ + body);
        return dir;
    }

    /** The phantom: a commented-out service block must declare nothing. */
    @Test
    void aCommentedOutServiceBlockRegistersNoProvider(@TempDir Path dir) throws IOException {
        Path root = protoRoot(dir, """
                service Live {
                  option (spec.service_s2s) = {label: "stratum"};
                  rpc Ping(PingRequest) returns (PingResponse);
                }

                // Removed in the c2s split, kept here for reference:
                // service Ghost {
                //   option (spec.service_s2s) = {label: "stratum"};
                //   rpc Ping(PingRequest) returns (PingResponse);
                // }
                """);

        List<VServiceDiscovery.HostedVService> found = VServiceDiscovery.discover(root, "stratum");

        assertEquals(1, found.size(), "found: " + found);
        assertEquals("Live", found.get(0).serviceName);
    }

    /**
     * The phantom, in the shape that actually reached the generator: the block is
     * real, its surface option is commented out. Reading the raw lines made the
     * comment the declaration.
     */
    @Test
    void aCommentedOutSurfaceOptionDoesNotDeclareASurface(@TempDir Path dir) throws IOException {
        Path root = protoRoot(dir, """
                service Ghost {
                  // option (spec.service_s2s) = {label: "stratum"};
                  rpc Ping(PingRequest) returns (PingResponse);
                }
                """);

        List<VServiceDiscovery.HostedVService> found = VServiceDiscovery.discover(root, "stratum");

        assertTrue(found.isEmpty(),
                "a service that declares no surface must not become a provider; found: " + found);
    }

    /** The same bug backwards: a brace inside a comment must not close the block. */
    @Test
    void aBraceInsideACommentDoesNotEndTheServiceBlock(@TempDir Path dir) throws IOException {
        Path root = protoRoot(dir, """
                service Live {
                  // the closing } of this block is below, not here
                  option (spec.service_s2s) = {label: "stratum"};
                  rpc Ping(PingRequest) returns (PingResponse);
                }
                """);

        List<VServiceDiscovery.HostedVService> found = VServiceDiscovery.discover(root, "stratum");

        assertEquals(1, found.size(), "a real service was dropped by a comment; found: " + found);
        assertEquals("Live", found.get(0).serviceName);
    }

    /** Stripping must not take tacplus's sentinel, which is a comment that declares. */
    @Test
    void theGeneratedServiceSentinelIsStillRead(@TempDir Path dir) throws IOException {
        Path root = protoRoot(dir, """
                // tacplus:generated-service recordmap
                service DemoRecordMap {
                  option (spec.service_s2s) = {label: "stratum"};
                  rpc CreateDemoEventQuery(OpenDemoEventQueryRequest) returns (QueryHandle);
                }
                """);

        VServiceDiscovery.RecordPlane plane = VServiceDiscovery.discoverRecordPlane(root, "stratum");

        assertNotNull(plane, "the sentinel is a comment, but it is a declaration");
        assertEquals("DemoRecordMap", plane.s2s.serviceName);
        // and it is NOT offered as a hand-written provider
        assertTrue(VServiceDiscovery.discover(root, "stratum").isEmpty());
    }
}
