// Copyright (c) 2026 Seabeam LLC. All rights reserved.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.chassis.backoff.Backoff;

/**
 * {@link StratumMapConfig#withWriterQuorum}: the wait a starting member does below
 * the writer quorum (defect #14). The end-to-end startup is
 * {@code StartupWriterQuorumTest}; this pins the policy.
 */
class WriterQuorumWaitTest {

    private static final String MAP = "letsencrypt.LetsEncryptCert";
    private static final int FIRST_PORT = 5851;
    private static final List<String> SEEDS = List.of("127.0.0.1:" + FIRST_PORT, "127.0.0.1:" + (FIRST_PORT + 1));

    private final List<HazelcastInstance> cluster = new ArrayList<>();
    private final Logger logger = (Logger) LoggerFactory.getLogger(StratumMapConfig.class);
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();

    @AfterEach
    void tearDown() {
        logger.detachAppender(logs);
        for (HazelcastInstance hz : cluster) {
            if (hz.getLifecycleService().isRunning()) {
                hz.getLifecycleService().terminate();
            }
        }
    }

    /** Criterion 3: one log line per attempt, on the chassis 0.5s → 30s schedule, never a hot loop. */
    @Test
    void belowTheQuorumItWaitsOnTheReconnectScheduleWithOneLogLinePerAttempt() throws Exception {
        logs.start();
        logger.addAppender(logs);
        HazelcastInstance first = member(FIRST_PORT, 2);
        List<Duration> sleeps = new CopyOnWriteArrayList<>();
        // Records the schedule and sleeps briefly, so the test does not wait it out.
        Backoff.Sleeper recording = delay -> {
            sleeps.add(delay);
            Thread.sleep(50);
        };
        AtomicInteger runs = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                StratumMapConfig.withWriterQuorum(first, "the test step",
                        () -> first.getMap(MAP).size() + runs.incrementAndGet(), recording);
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        waiter.start();

        while (sleeps.size() < 8) {
            Thread.sleep(20);
        }
        assertEquals(0, runs.get(), "the step must not run below the quorum");
        member(FIRST_PORT + 1, 2);
        waiter.join(60_000);

        assertFalse(waiter.isAlive(), "the wait must end once the second writer joins");
        assertEquals(null, failure.get(), "the step must succeed once the quorum is met");
        assertEquals(1, runs.get(), "the step runs once, after the quorum is met");
        Duration expected = Backoff.RECONNECT_INITIAL_BACKOFF;
        for (Duration d : sleeps) {
            assertEquals(expected, d, "the wait follows the chassis reconnect schedule: " + sleeps);
            expected = Backoff.nextReconnectBackoff(expected);
        }
        assertEquals(Backoff.RECONNECT_MAX_BACKOFF, sleeps.get(7), "capped at 30s: " + sleeps);
        long lines = logs.list.stream()
                .filter(e -> e.getFormattedMessage().startsWith("the test step: waiting for 2 WRITER members"))
                .count();
        assertEquals(sleeps.size(), lines, "exactly one log line per attempt");
    }

    /** Criterion 2: a failure that is not the quorum's is the step's own, and fails at once. */
    @Test
    void aFailureOtherThanTheQuorumIsRethrownOnTheFirstAttempt() throws Exception {
        HazelcastInstance first = member(FIRST_PORT, 2);
        member(FIRST_PORT + 1, 2);
        awaitProtection(first);
        IllegalStateException real = new IllegalStateException("hot restore: equal-epoch divergence");
        AtomicInteger runs = new AtomicInteger();
        List<Duration> sleeps = new CopyOnWriteArrayList<>();

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () ->
                StratumMapConfig.withWriterQuorum(first, "the test step", () -> {
                    runs.incrementAndGet();
                    throw real;
                }, sleeps::add));

        assertSame(real, thrown, "the step's own failure must reach the caller unchanged");
        assertEquals(1, runs.get(), "a non-quorum failure is not retried");
        assertTrue(sleeps.isEmpty(), "and not waited on");
    }

    /**
     * The member count is not the gate. A fresh member's protection refuses until it
     * takes the view into account, seconds after the count already reads "enough" —
     * shown here with the single-instance floor of 1, where the count is satisfied
     * from the first instant and the old count-based gate let {@code getMap} throw.
     */
    @Test
    void itWaitsForTheProtectionItselfNotForTheMemberCount() {
        HazelcastInstance only = member(FIRST_PORT, 1);
        assertTrue(StratumRole.writerCount(only) >= 1, "the count is satisfied immediately");

        int size = StratumMapConfig.withWriterQuorum(only, "the test step",
                () -> only.getMap(MAP).size(), Backoff.THREAD_SLEEPER);

        assertEquals(0, size);
    }

    private HazelcastInstance member(int port, int minDurableCopies) {
        Config cfg = StratumMapConfig.build(SEEDS, minDurableCopies, StratumMapConfig.DEFAULT_BACKUP_COUNT,
                StratumRole.WRITER);
        cfg.setClusterName("stratum-writer-quorum-test");
        cfg.getNetworkConfig().setPort(port);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        cluster.add(hz);
        return hz;
    }

    private static void awaitProtection(HazelcastInstance hz) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!hz.getSplitBrainProtectionService()
                .getSplitBrainProtection(StratumMapConfig.SPLIT_BRAIN_PROTECTION_NAME).hasMinimumSize()) {
            assertTrue(System.currentTimeMillis() < deadline, "the cluster never reached the quorum");
            Thread.sleep(100);
        }
    }
}
