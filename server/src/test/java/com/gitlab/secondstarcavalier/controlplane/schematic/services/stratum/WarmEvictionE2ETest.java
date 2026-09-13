// Copyright (c) 2026 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotWrites;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmFilter;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmResidency;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmStore;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.token.TokenWrapper;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.s2sm.internal.grpc.servers.vservices.query.RecordMap;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.errors.ProtoException;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.accounting.Accounting;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.accountingrecord.Accountingrecord.AccountingRecord;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.query.Query;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.query.RecordMapGrpc;

/**
 * Memory eviction at the warm residency boundary, over the same real declared
 * record {@link WarmAccountingRecordE2ETest} uses.
 *
 * <p>Acceptance item 3 — "force-evict past the window, query, get the rows from
 * disk" — is the sequence in
 * {@link #evictionReleasesAgedRowsFromMemoryAndLosesNothing()}: the rows are proven
 * memory-resident, the sweep is forced, the map is proven smaller, and the same
 * generated RPC returns the same rows. The two legs are fed by one loop and the
 * partition between them is the code's, not the fixture's.
 *
 * <p>The other two tests are the sweep's refusals, which are the half that can lose
 * data: it must not take the row that is still inside its window, and it must not
 * take the row this node's disk does not actually hold.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WarmEvictionE2ETest {

    private static final String DATASET = "accountingrecord.AccountingRecord";

    /** A one-day residency window, so the 2025 fixtures are aged without waiting. */
    private static final long WINDOW_SECONDS = 24L * 60 * 60;

    private static final long EPOCH_MAR = Instant.parse("2025-03-15T00:00:00Z").toEpochMilli();
    private static final long EPOCH_JUN = Instant.parse("2025-06-15T00:00:00Z").toEpochMilli();
    private static final long EPOCH_FRESH = System.currentTimeMillis();

    private static HazelcastInstance hz;
    private static StratumEventStores stores;
    private static WarmStore warm;
    private static Server server;
    private static ManagedChannel channel;
    private static RecordMapGrpc.RecordMapBlockingStub stub;

    private static AccountingRecord row(String id, long storeEpochMs) {
        return AccountingRecord.newBuilder()
                .setAccountingId(id)
                .setRequestId("req-" + id)
                .setSourceId("stratum-0")
                .setTimestampMs(1000)
                .setLevel(Accounting.EventLevel.EVENT_LEVEL_INFO)
                .setMethod("query.RecordMap/ListAccountingRecord")
                .setCode(0)
                .setDurationMs(7)
                .setPrincipalSub("joeuser@cav")
                .setSpiffeId("spiffe://cav/tenants/t1/services/stratum")
                .setStoreEpochMs(storeEpochMs)
                .build();
    }

    @BeforeAll
    static void up(@TempDir Path dataRoot) throws Exception {
        Config cfg = new Config();
        cfg.setClusterName("stratum-warm-eviction-e2e-" + UUID.randomUUID());
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().setPortAutoIncrement(true);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(cfg);

        stores = new StratumEventStores();
        stores.init(hz, new HotWrites(hz, 1));
        warm = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));

        // Both legs, in EventStore.store's order. Two aged rows and one inside the
        // window; all three are memory-resident when the sweep runs, which is the
        // state eviction exists to resolve.
        for (AccountingRecord r : List.of(row("acct-mar", EPOCH_MAR),
                                          row("acct-jun", EPOCH_JUN),
                                          row("acct-fresh", EPOCH_FRESH))) {
            assertTrue(stores.store(r));
            assertTrue(warm.enqueue(r));
        }
        // The row whose disk write never happened: aged, memory only. Not an
        // artificial case — it is what a failed asynchronous write leaves behind,
        // which WarmStore.writeFailureCount counts and the event log repairs.
        assertTrue(stores.store(row("acct-orphan", EPOCH_MAR)));

        warm.awaitWrites(10_000);
        assertEquals(0, warm.writeFailureCount(), "a warm disk write failed");
        assertEquals(3, warm.writtenCount());

        var authorizer = new com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum
                .model.chassis.grpcserver.Authorizer(sink -> { }) {
            @Override
            public TokenWrapper authorize(String fullMethod, Object request) throws ProtoException {
                return null;
            }
        };
        server = NettyServerBuilder.forPort(0)
                .addService(new RecordMap(
                        // The hot tier's restore gate, open: this fixture has no cold
                        // start to wait for. HotAgentEventE2ETest drives it closed.
                        new StratumRecordMaps(() -> stores, () -> warm, () -> true), authorizer))
                .build()
                .start();
        channel = PullAgentEvent.channel("localhost:" + server.getPort(), true);
        stub = PullAgentEvent.stub(channel, null);
    }

    @AfterAll
    static void down() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (warm != null) {
            warm.close();
        }
        if (hz != null) {
            hz.shutdown();
        }
    }

    /** The generated Create/List/Delete lifecycle, unfiltered. */
    private static List<String> servedIds() {
        Query.QueryHandle handle = stub.createAccountingRecordQuery(
                Query.OpenAccountingRecordQueryRequest.newBuilder()
                        .setFilter(Query.AccountingRecordFilter.newBuilder().build())
                        .setStanding(false)
                        .build());
        try {
            List<String> ids = new ArrayList<>();
            String pageToken = "";
            while (true) {
                Query.PullAccountingRecordResponse resp = stub.listAccountingRecord(
                        Query.PullAccountingRecordRequest.newBuilder()
                                .setQueryId(handle.getQueryId())
                                .setPageSize(100)
                                .setPageToken(pageToken)
                                .build());
                for (AccountingRecord r : resp.getItemsList()) {
                    ids.add(r.getAccountingId());
                }
                if (resp.getNextPageToken().isEmpty()) {
                    java.util.Collections.sort(ids);
                    return ids;
                }
                pageToken = resp.getNextPageToken();
            }
        } finally {
            stub.deleteAccountingRecordQuery(handle);
        }
    }

    /** The generated merged count over the same unfiltered handle. */
    private static long servedCount() {
        Query.QueryHandle handle = stub.createAccountingRecordQuery(
                Query.OpenAccountingRecordQueryRequest.newBuilder()
                        .setFilter(Query.AccountingRecordFilter.newBuilder().build())
                        .setStanding(false)
                        .build());
        try {
            return stub.getAccountingRecordCount(handle).getCount();
        } finally {
            stub.deleteAccountingRecordQuery(handle);
        }
    }

    // -----------------------------------------------------------------------

    /**
     * Acceptance item 3: force-evict past the window, query, get the rows from disk.
     *
     * <p>Three claims in one sequence, because the claim IS a sequence. Before: the
     * aged rows are in the Hazelcast map (so the eviction has something to do) and
     * the answer is already correct (so eviction is not what makes it correct).
     * The sweep: exactly the two aged, disk-held rows go. After: the same generated
     * RPC returns the same rows and the same count, now with the aged ones coming
     * off disk only.
     *
     * <p>acct-orphan is absent from the served set throughout, before the sweep as
     * well as after. That is the design and not eviction's doing: a row past the
     * boundary belongs to the disk leg, and this node's disk does not have it.
     */
    @Test
    @Order(1)
    void evictionReleasesAgedRowsFromMemoryAndLosesNothing() {
        assertEquals(4, stores.accountingrecordAccountingRecord().size());
        assertTrue(stores.accountingrecordAccountingRecord().containsKey("acct-mar"),
                "the aged row must be memory-resident before the sweep or this proves nothing");
        assertEquals(List.of("acct-fresh", "acct-jun", "acct-mar"), servedIds());
        assertEquals(3, servedCount());

        Map<String, Long> evicted = WarmEviction.sweep(hz, warm);

        assertEquals(Map.of(DATASET, 2L), evicted, "the sweep took the wrong rows");
        assertEquals(2, stores.accountingrecordAccountingRecord().size());
        assertFalse(stores.accountingrecordAccountingRecord().containsKey("acct-mar"),
                "the aged, disk-held row was not released from memory");
        assertFalse(stores.accountingrecordAccountingRecord().containsKey("acct-jun"));

        assertEquals(List.of("acct-fresh", "acct-jun", "acct-mar"), servedIds(),
                "a row evicted from memory is no longer queryable from disk");
        assertEquals(3, servedCount(), "the count changed when rows moved between legs");
    }

    /**
     * The sweep's first refusal, and item 3's control: the row inside the residency
     * window stays in memory. Without it "evicts the aged rows" would be satisfied
     * by a sweep that empties the map.
     */
    @Test
    @Order(2)
    void theRowInsideTheWindowIsNotEvicted() {
        WarmEviction.sweep(hz, warm);
        assertTrue(stores.accountingrecordAccountingRecord().containsKey("acct-fresh"),
                "a row inside its residency window was evicted; only the disk leg would answer"
                        + " for it, and the disk leg's ceiling is the boundary");
        assertEquals(EPOCH_FRESH,
                stores.accountingrecordAccountingRecord().get("acct-fresh").getStoreEpochMs());
    }

    /**
     * The sweep's second refusal: a row past the boundary whose copy is NOT on this
     * node's disk is left in the map.
     *
     * <p>"No longer pending" is true of a write that failed as well as of one that
     * committed, so pending alone is not the precondition — the disk index is asked
     * whether it actually holds the key. Evicting here would delete the last copy
     * this node has of a row it already cannot serve, and turn a repairable gap into
     * a permanent one.
     */
    @Test
    @Order(3)
    void evictionRefusesARowThisNodesDiskDoesNotHold() throws Exception {
        assertFalse(warm.isDurableOnDisk(DATASET, "acct-orphan"),
                "the fixture is only a control while the disk really lacks the row");
        assertTrue(warm.residency(DATASET)
                        .evictable("acct-orphan", EPOCH_MAR),
                "the boundary alone would have taken it; the disk check is the only thing left");

        WarmEviction.sweep(hz, warm);

        assertTrue(stores.accountingrecordAccountingRecord().containsKey("acct-orphan"),
                "eviction removed the only copy of a row the disk leg cannot answer for");
    }

    /**
     * The residency cut and the disk leg agree on one boundary. A row on the disk
     * side is excluded from memory and included in the disk leg's own count; the
     * fresh row is the reverse. If the two ever read different boundaries, one of
     * these is off by exactly the rows between them.
     */
    @Test
    @Order(4)
    void theMemoryCutAndTheDiskLegShareOneBoundary() throws Exception {
        WarmResidency residency = warm.residency(DATASET);
        assertFalse(residency.inMemory("acct-mar", EPOCH_MAR));
        assertTrue(residency.inMemory("acct-fresh", EPOCH_FRESH));
        assertEquals(2, warm.count(DATASET, new WarmFilter()),
                "the disk leg must answer for exactly the rows the memory cut excluded");
    }
}
