// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.services.stratum.Stratum;

/**
 * A config reload tears the Hazelcast member down and stands a new one up. Two
 * properties of that sequence are asserted here, both of which it lacked.
 *
 * <p><b>It has to be serialised.</b> The reload entry point is reachable from the
 * lazy init in {@code provideRecordPlane}, from {@code handleConfig}, from the
 * config-state plane's delivery and from the plane's {@code setConfig} RPC. Only
 * the first held a monitor, so two deliveries landing on different kernel-callback
 * and gRPC threads each ran the whole sequence: both shut the same previous store
 * down and both built a member, leaving the field pointing at one while the other
 * stayed joined to the cluster owning partitions nobody could reach.
 *
 * <p><b>A failed bring-up has to leave nothing behind.</b> The new store was
 * assigned to the shared field before its init returned, so an init that threw
 * partway left a member that had joined the cluster but had not opened its redo
 * logs or warm directory — dropping every write and answering every read
 * UNAVAILABLE, for the life of the pod.
 *
 * <p>Neither can be driven through a real {@link EventStore}: its init blocks on
 * Hazelcast cluster formation and cannot be told to fail. The handler exposes
 * {@code newEventStore()} for exactly this, and the fakes below block or throw on
 * demand in its place.
 */
class ConfigReloadSerializationTest {

    /** Distinguishes a deliberate test failure from any other breakage. */
    private static final class InitFailed extends RuntimeException {
        InitFailed() { super("forced init failure"); }
    }

    /**
     * An EventStore that never touches Hazelcast. {@code eventCount} answers
     * non-zero so the reload takes the warm-start branch: the cold-boot branch
     * starts a {@code ColdStartRebuilder} against the ServiceManager, which is a
     * different code path from the one under test.
     */
    private static class FakeStore extends EventStore {
        final int retentionDays;
        volatile boolean shutdown;
        FakeStore(int retentionDays) { this.retentionDays = retentionDays; }
        @Override public void init(int days, long cleanupSecs, List<String> seeds) { }
        @Override public long eventCount() { return 1; }
        @Override public int clusterSize() { return 1; }
        @Override public void shutdown() { shutdown = true; }
    }

    private static Stratum.Config cfg(int retentionDays) {
        return Stratum.Config.newBuilder().setRetentionDays(retentionDays).build();
    }

    private static EventStore liveStore(StratumHandler handler) throws Exception {
        Field f = StratumHandler.class.getDeclaredField("eventStore");
        f.setAccessible(true);
        return (EventStore) f.get(handler);
    }

    private static Stratum.Config liveConfig(StratumHandler handler) throws Exception {
        Field f = StratumHandler.class.getDeclaredField("config");
        f.setAccessible(true);
        return (Stratum.Config) f.get(handler);
    }

    /**
     * Two reloads delivered at once, on the two paths that really are unrelated
     * threads in production: the kernel's {@code handleConfig} callback and the
     * config-state plane's delivery.
     *
     * <p>The overlap is forced deterministically rather than raced for. Each
     * bring-up announces itself at a two-party latch and waits there: if the lock
     * does not hold, both parties arrive, the latch opens immediately and the two
     * bring-ups are provably inside at the same time; if it does hold, the first
     * party waits the latch out alone and the counter never reaches two. There is
     * no interval in which the outcome depends on scheduling.
     */
    @Test
    void twoOverlappingReloadsDoNotInterleave() throws Exception {
        CountDownLatch bothInside = new CountDownLatch(2);
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger peakConcurrent = new AtomicInteger();
        StratumHandler handler = new StratumHandler(null) {
            @Override
            EventStore newEventStore() {
                return new FakeStore(0) {
                    @Override
                    public void init(int days, long cleanupSecs, List<String> seeds) {
                        peakConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
                        bothInside.countDown();
                        try {
                            bothInside.await(2, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        concurrent.decrementAndGet();
                    }
                };
            }
        };
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread kernelDelivery = new Thread(() -> {
            try { handler.handleConfig(cfg(7)); } catch (Throwable t) { failure.compareAndSet(null, t); }
        }, "kernel-config");
        Thread planeDelivery = new Thread(() -> {
            try { handler.handleStratumConfigStateConfig(cfg(9)); } catch (Throwable t) { failure.compareAndSet(null, t); }
        }, "plane-config");

        kernelDelivery.start();
        planeDelivery.start();
        kernelDelivery.join(30_000);
        planeDelivery.join(30_000);

        assertNull(failure.get(), "neither reload may fail: " + failure.get());
        assertEquals(1, peakConcurrent.get(),
                "two bring-ups were inside initFromConfig at once — the reload sequence is not serialised,"
                        + " so both shut the same store down and both built a Hazelcast member");

        EventStore live = liveStore(handler);
        assertNotNull(live, "one of the two reloads must be the live state");
        assertTrue(live instanceof FakeStore);
        assertFalse(((FakeStore) live).shutdown, "the live store must not have been shut down");
        assertEquals(0, concurrent.get(), "every bring-up has returned");

        // Exactly one reload's result is live, and the config field agrees with it:
        // the losing reload must have been fully superseded, not half-applied.
        Stratum.Config cfg = liveConfig(handler);
        assertNotNull(cfg);
        assertTrue(cfg.getRetentionDays() == 7 || cfg.getRetentionDays() == 9,
                "the live config must be one of the two delivered, not a mixture: " + cfg.getRetentionDays());
    }

    /** No store has ever come up, and the first bring-up fails. */
    @Test
    void aFailedFirstInitLeavesNothingBehind() throws Exception {
        AtomicReference<FakeStore> attempted = new AtomicReference<>();
        StratumHandler handler = new StratumHandler(null) {
            @Override
            EventStore newEventStore() {
                FakeStore s = new FakeStore(3) {
                    @Override
                    public void init(int days, long cleanupSecs, List<String> seeds) { throw new InitFailed(); }
                };
                attempted.set(s);
                return s;
            }
        };

        assertThrows(InitFailed.class, () -> handler.handleConfig(cfg(3)),
                "a failed bring-up must propagate, not be swallowed");

        assertNull(liveStore(handler),
                "the half-built store must not be left assigned: it would drop every write and"
                        + " answer every read UNAVAILABLE");
        assertTrue(attempted.get().shutdown,
                "the half-built Hazelcast member must be shut down, not orphaned");
        assertNull(liveConfig(handler),
                "no config is in effect, so none may be advertised");
    }

    /**
     * A store is already live and the reload's bring-up fails.
     *
     * <p>The previous member is gone by then — it is released before the
     * replacement is built, deliberately, because keeping it joined while the new
     * one forms is the two-members-at-once state that made reads and writes land on
     * different owners. So the guarantee here is the clean uninitialised state, and
     * the config reverting to the one that is actually in effect: nothing
     * half-built survives, and the plane does not advertise a config whose
     * bring-up failed.
     */
    @Test
    void aFailedReloadLeavesACleanUninitialisedHandler() throws Exception {
        AtomicInteger attempt = new AtomicInteger();
        List<FakeStore> stores = new ArrayList<>();
        StratumHandler handler = new StratumHandler(null) {
            @Override
            EventStore newEventStore() {
                FakeStore s = attempt.incrementAndGet() == 1
                        ? new FakeStore(7)
                        : new FakeStore(9) {
                            @Override
                            public void init(int days, long cleanupSecs, List<String> seeds) { throw new InitFailed(); }
                        };
                stores.add(s);
                return s;
            }
        };

        handler.handleConfig(cfg(7));
        FakeStore first = stores.get(0);
        assertSame(first, liveStore(handler));
        assertEquals(7, liveConfig(handler).getRetentionDays());

        assertThrows(InitFailed.class, () -> handler.handleConfig(cfg(9)));

        FakeStore second = stores.get(1);
        assertTrue(second.shutdown, "the half-built replacement must be shut down");
        assertTrue(first.shutdown, "the previous member is released before the replacement is built");
        assertNull(liveStore(handler), "no half-built store may be left serving");
        assertEquals(7, liveConfig(handler).getRetentionDays(),
                "the config must revert: getConfig may not report a config whose bring-up failed");
    }

    /**
     * The reload after a failure has to actually run. The "config unchanged"
     * short-circuit keys off the config field, so a failed reload that left the new
     * config recorded would make the retry a no-op and strand the handler with no
     * store at all.
     */
    @Test
    void redeliveringTheSameConfigAfterAFailureRetries() throws Exception {
        AtomicInteger attempt = new AtomicInteger();
        List<FakeStore> stores = new ArrayList<>();
        StratumHandler handler = new StratumHandler(null) {
            @Override
            EventStore newEventStore() {
                FakeStore s = attempt.incrementAndGet() == 1
                        ? new FakeStore(5) {
                            @Override
                            public void init(int days, long cleanupSecs, List<String> seeds) { throw new InitFailed(); }
                        }
                        : new FakeStore(5);
                stores.add(s);
                return s;
            }
        };

        assertThrows(InitFailed.class, () -> handler.handleConfig(cfg(5)));
        assertNull(liveStore(handler));

        handler.handleConfig(cfg(5));

        assertEquals(2, attempt.get(), "the retry must build a store rather than short-circuit as unchanged");
        assertSame(stores.get(1), liveStore(handler));
        assertEquals(5, liveConfig(handler).getRetentionDays());
    }
}
