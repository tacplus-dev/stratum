// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins stratum's side of the trait-driven record query plane.
 *
 * tacplus no longer emits the plane into the record store's own services/ proto
 * under the fixed names RecordMap/RecordQuery: it emits it into the proto that
 * declares the (spec.trait), under the service names that declaration carries,
 * with {@code label: "<host>"} on both surfaces. Everything stratum needs — the
 * two service names, the wrapper package, the message outer class — therefore
 * has to be read off the emitted proto, which is what this fixture proves.
 */
class RecordPlaneTraitTest {

    private static final String MODEL_BASE =
            "com.gitlab.seabeam.controlplane.schematic.services.stratum.model";

    /**
     * The shape tacplus emits for a trait declaring
     * {@code host: "stratum", s2s_service: "TaskRecordMap", c2s_service: "TaskRecordQuery"}.
     * Names deliberately differ from RecordMap/RecordQuery so a leftover
     * hardcode cannot pass.
     */
    private static Path traitTree(@TempDir Path dir) throws IOException {
        Path vservices = dir.resolve("vservices");
        Files.createDirectories(vservices);
        Files.writeString(vservices.resolve("query.proto"), """
                syntax = "proto3";
                package taskquery;
                option java_package = "com.example.model.types.vservices.taskquery";
                option java_outer_classname = "Taskquery";

                option (spec.trait) = {
                  id: 40
                  name: "query"
                  host: "stratum"
                  s2s_service: "TaskRecordMap"
                  c2s_service: "TaskRecordQuery"
                  auth_services: ["reporter"]
                };

                // tacplus:generated-service recordmap
                service TaskRecordMap {
                  option (spec.service_s2s) = {label: "stratum"};

                  rpc CreateDemoEventQuery(OpenDemoEventQueryRequest) returns (QueryHandle) {
                    option (spec.rpc) = {principal: RPC_PRINCIPAL_SERVICE, auth_services: ["reporter"]};
                  }
                }

                // tacplus:generated-service recordmap
                service TaskRecordQuery {
                  option (spec.service_c2s) = {auth_entitled_users: true, label: "stratum"};

                  rpc CreateDemoEventQuery(OpenDemoEventQueryRequest) returns (QueryHandle);
                }
                """);
        Files.writeString(vservices.resolve("demo.proto"), """
                syntax = "proto3";
                package demo;
                option java_package = "com.example.model.types.vservices.demo";
                option java_outer_classname = "Demo";

                message DemoBus {
                  option (spec.service_event) = {label: "eventbus"};
                  DemoEvent event_stream = 1 [(spec.topic) = true];
                }

                message DemoEvent {
                  option (spec.record) = {key: "event_id"};
                  string event_id = 1;
                  string spiffe_id = 2 [(spec.query_field) = {op: QUERY_OP_EQ}];
                  string namespace = 3 [(spec.store_namespace) = true];
                }
                """);
        return dir;
    }

    @Test
    void discoversBothSurfacesOfTheTraitPlane(@TempDir Path dir) throws IOException {
        VServiceDiscovery.RecordPlane plane =
                VServiceDiscovery.discoverRecordPlane(traitTree(dir), "stratum");

        assertNotNull(plane, "the trait's plane must be discovered");
        assertEquals("TaskRecordMap", plane.s2s.serviceName);
        assertEquals(VServiceDiscovery.Surface.S2S, plane.s2s.surface);
        assertEquals("TaskRecordQuery", plane.c2s.serviceName);
        assertEquals(VServiceDiscovery.Surface.C2S, plane.c2s.surface);
        assertEquals("stratum", plane.c2s.hostLabel);
        assertEquals(MODEL_BASE + ".s2sm.internal.grpc.servers.vservices.taskquery.TaskRecordQuery",
                plane.c2s.wrapperFqn(MODEL_BASE, "s2sm"));
        assertEquals("com.example.model.types.vservices.taskquery.Taskquery",
                plane.c2s.messagesFqn());
    }

    /**
     * The plane's two services must NOT also come back as hand-written hosted
     * vservices: they are implemented by generated code, and a duplicate
     * provider method on StratumVServiceBase would not compile.
     */
    @Test
    void planeServicesAreNotHandWrittenVServices(@TempDir Path dir) throws IOException {
        List<VServiceDiscovery.HostedVService> hosted =
                VServiceDiscovery.discover(traitTree(dir), "stratum");

        assertEquals(List.of(), hosted.stream().map(s -> s.serviceName).toList());
    }

    /** No trait, no plane — the surface must disappear with the declaration. */
    @Test
    void noPlaneWithoutTheDeclaration(@TempDir Path dir) throws IOException {
        Path vservices = dir.resolve("vservices");
        Files.createDirectories(vservices);
        Files.writeString(vservices.resolve("demo.proto"), """
                syntax = "proto3";
                package demo;
                message DemoEvent {
                  string event_id = 1;
                }
                """);

        assertNull(VServiceDiscovery.discoverRecordPlane(dir, "stratum"));
    }

    /** A plane hosted by some other service is not stratum's to implement. */
    @Test
    void planeHostedElsewhereIsIgnored(@TempDir Path dir) throws IOException {
        Path vservices = dir.resolve("vservices");
        Files.createDirectories(vservices);
        Files.writeString(vservices.resolve("query.proto"), """
                syntax = "proto3";
                package otherquery;

                // tacplus:generated-service recordmap
                service OtherRecordMap {
                  option (spec.service_s2s) = {label: "timedb"};
                }

                // tacplus:generated-service recordmap
                service OtherRecordQuery {
                  option (spec.service_c2s) = {auth_entitled_users: true, label: "timedb"};
                }
                """);

        assertNull(VServiceDiscovery.discoverRecordPlane(dir, "stratum"));
    }

    /**
     * The emitted impls must speak the trait's names end to end: the class
     * declaration, the wrapper import and the message outer class.
     */
    @Test
    void generatedImplsUseTheTraitDeclaredNames(@TempDir Path dir, @TempDir Path out)
            throws IOException {
        Path protoRoot = traitTree(dir);
        VServiceDiscovery.RecordPlane plane =
                VServiceDiscovery.discoverRecordPlane(protoRoot, "stratum");
        List<EventTopicDiscovery.TopicEvent> records =
                EventTopicDiscovery.discover(protoRoot).stream().filter(e -> e.isRecord).toList();
        assertEquals(1, records.size());

        RecordMapImplGenerator.generate(out, plane.s2s, records);
        RecordQueryImplGenerator.generate(out, plane.c2s, records);
        VServiceWiringGenerator.generate(out, List.of(), plane);

        Path pkg = out.resolve(
                "com/gitlab/seabeam/controlplane/schematic/services/stratum");
        String maps = Files.readString(pkg.resolve("StratumRecordMaps.java"));
        String queries = Files.readString(pkg.resolve("StratumRecordQueries.java"));
        String base = Files.readString(pkg.resolve("StratumVServiceBase.java"));

        assertTrue(maps.contains(
                "public final class StratumRecordMaps implements TaskRecordMap.TaskRecordMapHandler {"),
                maps);
        assertTrue(queries.contains(
                "public final class StratumRecordQueries implements TaskRecordQuery.TaskRecordQueryHandler {"),
                queries);
        assertTrue(queries.contains("import " + MODEL_BASE
                + ".s2sm.internal.grpc.servers.vservices.taskquery.TaskRecordQuery;"), queries);
        assertTrue(queries.contains("import com.example.model.types.vservices.taskquery.Taskquery;"),
                queries);
        assertTrue(queries.contains("public Taskquery.QueryHandle createDemoEventQuery("), queries);
        assertTrue(base.contains("public final TaskRecordMap.TaskRecordMapHandler getTaskRecordMapGrpcImpl()"),
                base);
        assertTrue(base.contains(
                "public final TaskRecordQuery.TaskRecordQueryHandler getTaskRecordQueryGrpcImpl()"), base);
        assertTrue(base.contains("protected abstract StratumRecordMaps provideRecordPlane();"), base);

        // No leftover hardcode from the pre-trait shape.
        assertTrue(!queries.contains("services.stratum.RecordQuery"), queries);
        assertTrue(!maps.contains("types.services.stratum.Stratum"), maps);
    }
}
