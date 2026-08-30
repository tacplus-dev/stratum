// Copyright (c) 2026 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotWrites;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmStore;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.accounting.Accounting;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.accountingrecord.Accountingrecord.AccountingRecord;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.AgentSessionState;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor.SessionStatus;

/**
 * The APPEND_ONLY unique-key contract, enforced at ingest.
 *
 * <p>A record type declaring {@code (spec.record).mutability = MUTABILITY_APPEND_ONLY}
 * says its key may not repeat. The two legs disagree about what a repeat MEANS — the
 * Hazelcast map upserts, the Lucene index appends — so a republished key used to be
 * accepted by both and leave them permanently disagreeing once the first copy aged
 * past the residency boundary. {@link EventStore#ingest} refuses it instead.
 *
 * <p>Both halves are here: the legitimate write (a fresh key, which must behave exactly
 * as before) and the violation (a republished key, which must not reach either leg).
 * The MUTABLE control — {@code AgentSessionState}, the other declared warm record — is
 * what says the guard is keyed on the declaration and not on being warm-tiered at all.
 */
class AppendOnlyKeyGuardTest {

    private static final String APPEND_ONLY_DATASET = "accountingrecord.AccountingRecord";

    /** A one-day residency window, as the other warm tests use. */
    private static final long WINDOW_SECONDS = 24L * 60 * 60;

    private static HazelcastInstance hz;
    private static StratumEventStores stores;
    private static WarmStore warm;

    @BeforeAll
    static void up(@TempDir Path dataRoot) throws Exception {
        Config cfg = new Config();
        cfg.setClusterName("stratum-append-only-guard-" + UUID.randomUUID());
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().setPortAutoIncrement(true);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(cfg);

        stores = new StratumEventStores();
        stores.init(hz, new HotWrites(hz, 0, 1));

        // The GENERATED descriptor list — the mutability attribute under test arrives
        // through it, so a hand-built descriptor would prove nothing.
        warm = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));
    }

    @AfterAll
    static void down() {
        if (warm != null) {
            warm.close();
        }
        if (hz != null) {
            hz.shutdown();
        }
    }

    private static AccountingRecord accounting(String id, long durationMs, long storeEpochMs) {
        return AccountingRecord.newBuilder()
                .setAccountingId(id)
                .setRequestId("req-" + id)
                .setSourceId("stratum-0")
                .setTimestampMs(1000)
                .setLevel(Accounting.EventLevel.EVENT_LEVEL_INFO)
                .setMethod("query.RecordMap/ListAccountingRecord")
                .setCode(0)
                .setDurationMs(durationMs)
                .setPrincipalSub("joeuser@cav")
                .setSpiffeId("spiffe://cav/tenants/t1/services/stratum")
                .setStoreEpochMs(storeEpochMs)
                .build();
    }

    private static AgentSessionState session(String id, int tools, long storeEpochMs) {
        return AgentSessionState.newBuilder()
                .setSessionId(id)
                .setSpiffeId("spiffe://cav/tenants/t1/users/joeuser")
                .setStartedAtMs(1000)
                .setStatus(SessionStatus.SESSION_STATUS_ACTIVE)
                .setToolUseCount(tools)
                .setStoreEpochMs(storeEpochMs)
                .build();
    }

    /** The memory leg as {@link EventStore#store} reads it: the map the generated store writes. */
    private static boolean memoryHolds(String datasetName, String key) {
        return hz.getMap(datasetName).containsKey(key);
    }

    /**
     * A fresh key is stored exactly as before — the happy path is untouched.
     *
     * <p>Everything in production respects the contract today, so this is the case the
     * guard must not disturb: both legs take the record and the memory put runs.
     */
    @Test
    void aFreshAppendOnlyKeyIsStoredThroughBothLegs() throws Exception {
        // The write count is the store's, shared with every other test in this class,
        // so the baseline is only meaningful once the writer has drained.
        warm.awaitWrites(10_000);
        long writtenBefore = warm.writtenCount();
        AtomicBoolean memoryPutRan = new AtomicBoolean();

        AccountingRecord r = accounting("acct-fresh-key", 7, System.currentTimeMillis());
        assertTrue(EventStore.ingest(warm, r, AppendOnlyKeyGuardTest::memoryHolds, () -> {
            memoryPutRan.set(true);
            return stores.store(r);
        }), "the typed hot store did not claim the record");

        assertTrue(memoryPutRan.get(), "the memory put was skipped for a key that does not exist");
        warm.awaitWrites(10_000);
        assertEquals(writtenBefore + 1, warm.writtenCount(),
                "the warm tier did not write a legitimate append-only record");
        assertEquals(0, warm.writeFailureCount());
        assertEquals(7, stores.accountingrecordAccountingRecord().get("acct-fresh-key").getDurationMs());
    }

    /**
     * A republished key is REFUSED, and neither leg is touched by it.
     *
     * <p>Without the guard this call upserts the memory copy and appends a second
     * document to the disk index. The assertions are therefore about both: the memory
     * value is still the first write's, and the disk write count did not move. The
     * memory put is not even invoked — the refusal is a decision about the record, not
     * a rollback after the fact.
     *
     * <p>Refused, not thrown: this runs on the EventBus subscription thread, where a
     * throw would redeliver the offending record forever. {@code ingest} returns true —
     * handled — the same answer the validator-rejection path in {@link WarmStore#enqueue}
     * gives.
     */
    @Test
    void aRepublishedAppendOnlyKeyIsRejectedAndOverwritesNothing() throws Exception {
        long epoch = System.currentTimeMillis();
        AccountingRecord first = accounting("acct-dup", 7, epoch);
        assertTrue(EventStore.ingest(warm, first, AppendOnlyKeyGuardTest::memoryHolds,
                () -> stores.store(first)));
        warm.awaitWrites(10_000);
        long writtenBefore = warm.writtenCount();
        long rejectedBefore = warm.rejectedCount();
        AtomicBoolean memoryPutRan = new AtomicBoolean();

        // The same accounting_id, different content — a republish, which the record's
        // own declaration says cannot happen.
        AccountingRecord republished = accounting("acct-dup", 99, epoch + 1);
        assertTrue(EventStore.ingest(warm, republished, AppendOnlyKeyGuardTest::memoryHolds, () -> {
            memoryPutRan.set(true);
            return stores.store(republished);
        }), "a refused record must read as handled, or the subscription retries it forever");

        assertFalse(memoryPutRan.get(), "the duplicate reached the memory put");
        assertEquals(7, stores.accountingrecordAccountingRecord().get("acct-dup").getDurationMs(),
                "the duplicate silently overwrote the memory leg's copy");
        warm.awaitWrites(10_000);
        assertEquals(writtenBefore, warm.writtenCount(),
                "the duplicate appended a second document to the disk leg");
        assertEquals(rejectedBefore, warm.rejectedCount(),
                "the duplicate went down the validator-rejection path; it is a contract refusal,"
                        + " not an unstorable record");

        // Nothing was claimed, so nothing is owed a release: an aged copy of this key
        // must still be the disk leg's, not hidden in memory's half forever.
        assertFalse(warm.residency(APPEND_ONLY_DATASET).inMemory("acct-dup", 0L),
                "the refused write leaked a claim; the disk leg is hiding the key it holds");
    }

    /**
     * The control: a MUTABLE warm record still republishes, unchanged.
     *
     * <p>AgentSessionState declares RESIDENCY_WARM and no mutability — it is
     * MUTABILITY_CRUD, and an update to an existing session_id is its normal write.
     * Nothing about being warm-tiered triggers the guard; the declaration does.
     */
    @Test
    void aRepublishedMutableKeyIsStillAccepted() throws Exception {
        AgentSessionState first = session("sess-update", 3, System.currentTimeMillis());
        assertTrue(EventStore.ingest(warm, first, AppendOnlyKeyGuardTest::memoryHolds,
                () -> stores.store(first)));
        warm.awaitWrites(10_000);
        long writtenBefore = warm.writtenCount();
        AgentSessionState update = session("sess-update", 4, System.currentTimeMillis() + 1);
        assertTrue(EventStore.ingest(warm, update, AppendOnlyKeyGuardTest::memoryHolds,
                () -> stores.store(update)));
        warm.awaitWrites(10_000);

        assertEquals(4, stores.agentsensorAgentSessionState().get("sess-update").getToolUseCount(),
                "a mutable record's update was refused; the guard is firing on the wrong datasets");
        assertEquals(writtenBefore + 1, warm.writtenCount(),
                "the mutable update did not reach the disk leg");
        assertFalse(warm.datasetFor(update).appendOnly,
                "the control record is append-only too; this test contrasts nothing");
    }
}
