// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.codegen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The memory clock a record declares must reach the Hazelcast map that holds it.
 *
 * <p>Under the one-clock lifecycle a row leaves MEMORY after
 * {@code (spec.record).residency.idle_seconds} with neither a read nor a write,
 * and the store has exactly one place to say so: the per-map
 * {@code MapConfig.setMaxIdleSeconds}. {@code StratumMapConfig} builds those
 * configs from a map NAME, so the declaration has to arrive as a name-keyed
 * lookup on the generated store — the same shape, and for the same reason, as
 * {@code minDurableCopies(String)}.
 *
 * <p>The other half of this file is the RETIRED gate: generation used to refuse
 * any stored record that declared no
 * {@code (spec.record).retention.timestamp_field}. tacplus dropped
 * {@code (spec.record).retention} on 2026-09-20 with the HOT/WARM/COLD residency
 * model, so that gate now refuses every converted proto for missing a field that
 * no longer exists to declare.
 */
class EventStoreResidencyTest {

    private static String generate(Path dir, String eventBody) throws IOException {
        Path protoDir = dir.resolve("proto");
        Path vservices = protoDir.resolve("vservices");
        Files.createDirectories(vservices);
        Files.writeString(vservices.resolve("demo.proto"), """
                syntax = "proto3";
                package demo;
                option java_package = "com.example.demo";
                option java_outer_classname = "Demo";

                message DemoBus {
                  option (spec.service_event) = {label: "eventbus"};
                  DemoEvent event_stream = 1 [(spec.topic) = true];
                }

                message DemoEvent {
                """ + eventBody + """
                }
                """);
        List<EventTopicDiscovery.TopicEvent> events = EventTopicDiscovery.discover(protoDir);
        assertEquals(1, events.size(), "expected one topic event, got " + events);

        Path out = dir.resolve("out");
        EventStoreGenerator.generate(out, events);
        return Files.readString(out.resolve(
                "com/gitlab/seabeam/controlplane/schematic/services/stratum/StratumEventStores.java"));
    }

    /** A record on tacplus's current annotation surface: two clocks, no retention. */
    private static final String DECLARED = """
              option (spec.record) = {
                key: "demo_id"
                residency: {idle_seconds: 3600 deleted_max_age_seconds: 604800}
              };
              string demo_id = 1;
              int64 store_epoch_ms = 2 [(spec.store_epoch) = true];
            """;

    @Test
    void theDeclaredIdleSecondsReachesTheNameKeyedLookup(@TempDir Path dir) throws IOException {
        String src = generate(dir, DECLARED);

        assertTrue(src.contains("public static int idleSeconds(String mapName) {"),
                "StratumMapConfig has only the map name in hand:\n" + src);
        assertTrue(src.contains("case \"demo.DemoEvent\": return 3600;"),
                "the declared idle window did not reach the generated store:\n" + src);
    }

    /**
     * A name that is not a record map answers 0 — Hazelcast's "no max-idle".
     *
     * <p>{@code durableMap} is called for the hot-restore ledger too, and the
     * ledger holds the replay reports the restore barrier counts. A barrier whose
     * ledger expires is not a barrier, so the default arm must be 0 and not the
     * platform residency default.
     */
    @Test
    void aNameThatIsNotARecordMapHasNoMaxIdle(@TempDir Path dir) throws IOException {
        String src = generate(dir, DECLARED);

        int lookup = src.indexOf("public static int idleSeconds(String mapName) {");
        assertTrue(lookup > 0, src);
        assertTrue(src.indexOf("default: return 0;", lookup) > 0,
                "a non-record name must take Hazelcast's no-max-idle, not a record's clock:\n" + src);
    }

    /**
     * 0/unset means "apply the platform default", never "never".
     *
     * <p>The number is tacplus's {@code parser.DefaultResidencyIdleSeconds},
     * mirrored as {@link EventTopicDiscovery#DEFAULT_RESIDENCY_IDLE_SECONDS}. The
     * expectation reads it from the constant rather than restating 86400, so the
     * two sides cannot drift apart silently through this test.
     */
    @Test
    void anUndeclaredIdleClockTakesThePlatformDefault(@TempDir Path dir) throws IOException {
        String src = generate(dir, """
                  option (spec.record) = {key: "demo_id"};
                  string demo_id = 1;
                  int64 store_epoch_ms = 2 [(spec.store_epoch) = true];
                """);

        assertTrue(src.contains("case \"demo.DemoEvent\": return "
                        + EventTopicDiscovery.DEFAULT_RESIDENCY_IDLE_SECONDS + ";"),
                "an undeclared clock must resolve to the platform default, not to 0"
                        + " — 0 would mean 'never leaves memory', which this lifecycle does"
                        + " not have:\n" + src);
    }

    /** A record declaring the clocks explicitly at 0 is the same statement as omitting them. */
    @Test
    void anExplicitZeroIsTheSameAsUndeclared(@TempDir Path dir) throws IOException {
        String src = generate(dir, """
                  option (spec.record) = {key: "demo_id"
                    residency: {idle_seconds: 0 deleted_max_age_seconds: 0}};
                  string demo_id = 1;
                """);

        assertTrue(src.contains("case \"demo.DemoEvent\": return "
                + EventTopicDiscovery.DEFAULT_RESIDENCY_IDLE_SECONDS + ";"), src);
    }

    /**
     * The retired preset shape must not be read as a clock.
     *
     * <p>{@code window_seconds} occupies the same {@code residency:} clause and
     * ends in the same letters as {@code idle_seconds}; a loose pattern would take
     * a warm type's 31536000-second disk window as its memory clock, which is a
     * three-hundred-times-too-long max-idle arrived at silently.
     */
    @Test
    void theRetiredWindowSecondsIsNotMistakenForTheIdleClock(@TempDir Path dir) throws IOException {
        String src = generate(dir, """
                  option (spec.record) = {key: "demo_id"
                    residency: {window: RESIDENCY_WARM window_seconds: 31536000}};
                  string demo_id = 1;
                  int64 store_epoch_ms = 2 [(spec.store_epoch) = true];
                """);

        assertFalse(src.contains("return 31536000;"),
                "the retired preset window was read as the memory clock:\n" + src);
        assertTrue(src.contains("case \"demo.DemoEvent\": return "
                + EventTopicDiscovery.DEFAULT_RESIDENCY_IDLE_SECONDS + ";"), src);
    }

    /**
     * The retired retention gate is gone: a record declaring no
     * {@code retention.timestamp_field} generates.
     *
     * <p>It used to throw {@code IllegalStateException} and fail generation for the
     * whole fleet. Every proto converted to tacplus's current surface declares no
     * such field — there is no such field any more — so the gate would refuse the
     * conversion it exists to serve.
     */
    @Test
    void aRecordWithNoRetentionClauseGenerates(@TempDir Path dir) {
        assertDoesNotThrow(() -> generate(dir, DECLARED));
    }

    /**
     * The retention sweep is GONE from the generated store entirely — not merely
     * emitted with no lines in it.
     *
     * <p>Retention was the old model's only bound on growth, and it measured from a
     * per-record business timestamp declared as
     * {@code (spec.record).retention.timestamp_field}. The 2026-09-20 record
     * lifecycle decision retired that annotation and replaced the sweep with two
     * clocks: {@code idleSeconds} ages a row out of MEMORY (Hazelcast max-idle), and
     * the daily checkpoint purges a flagged-deleted row past
     * {@code deleted_max_age_seconds}. Neither needs a business timestamp, and no
     * proto declares one any more — so a {@code runRetention} left on the generated
     * store would be a method the caller could only call to do nothing, and
     * {@code EventStore} no longer has a caller for it at all.
     */
    @Test
    void noRetentionSweepIsGeneratedAtAll(@TempDir Path dir) throws IOException {
        String src = generate(dir, DECLARED);

        assertFalse(src.contains("runRetention"),
                "the retired retention sweep is still generated:\n" + src);
        assertFalse(src.contains("Predicates.lessThan("),
                "a retention predicate survived:\n" + src);
    }

    /**
     * ...including for a proto that still carries the retired clause. The clause is
     * inert now, not a trigger: reading it would resurrect the sweep for exactly the
     * types slowest to be re-annotated.
     */
    @Test
    void aSurvivingRetentionClauseGeneratesNoSweepLine(@TempDir Path dir) throws IOException {
        String src = generate(dir, """
                  option (spec.record) = {key: "demo_id"
                    retention: {timestamp_field: "occurred_at_ms" duration_seconds: 100}};
                  string demo_id = 1;
                  int64 occurred_at_ms = 2;
                """);

        assertFalse(src.contains("runRetention"), src);
        assertFalse(src.contains("occurredAtMs"), src);
    }
}
