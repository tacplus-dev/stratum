// Copyright (c) 2026 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.hot.HotWrites;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmDataset;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmField;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmFilter;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.warm.WarmStore;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.chassis.token.TokenWrapper;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.s2sm.internal.grpc.servers.vservices.query.RecordMap;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.errors.ProtoException;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.sys.accounting.Accounting;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.accountingrecord.Accountingrecord.AccountingRecord;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.query.Query;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum.model.types.vservices.query.RecordMapGrpc;

/**
 * The warm tier and the append-only attribute over a REAL declared record:
 * AccountingRecord, which carries
 * {@code (spec.record).residency = {window: RESIDENCY_WARM}} and
 * {@code mutability: MUTABILITY_APPEND_ONLY} as of BIG GOAL 4 P4b.
 *
 * <p>Everything under test is generated from that one declaration and taken as
 * generated: the warm dataset descriptor ({@link StratumWarmStores}), the typed
 * memory store ({@link StratumEventStores}), the two-leg query handler
 * ({@code StratumRecordMaps}), the server wrapper and the client stub. That is the
 * distinction from {@code WarmTierTest}, which drives the same warm runtime through
 * a HAND-BUILT descriptor over AgentEvent and so cannot show that a declaration
 * produces one.
 *
 * <h2>What it does NOT cover, stated plainly</h2>
 * <p><b>Memory eviction at the residency boundary does not exist yet.</b> Nothing
 * removes a row from the Hazelcast map when it ages past the window
 * ({@code WarmDataset.boundaryMs} has exactly one caller, the disk leg's prune in
 * {@code WarmStore.diskRange}), and the generated memory leg applies no epoch cut.
 * So the two-leg partition this test asserts on — memory holds
 * {@code epoch >= boundary}, disk holds {@code epoch < boundary} — is STAGED here:
 * the aged rows are written through the warm ingest only and the fresh row through
 * the memory store only, which is the state an eviction sweep would produce. Once
 * eviction lands, both legs are fed by the one {@code EventStore.store} call and
 * this staging goes away. Until then, a warm row that ages past the window while
 * still resident is served by BOTH legs and counted twice — see the final report
 * for the gap.
 *
 * <p>The stamp itself is also not covered, for the same reason
 * {@code HotAgentEventE2ETest} does not cover it: the store epoch is stamped in the
 * generated ServiceManager's subscription dispatch, which needs a bootstrapped
 * service and a broker. Here the fixtures arrive already stamped.
 */
class WarmAccountingRecordE2ETest {

    private static final String DATASET = "accountingrecord.AccountingRecord";

    /**
     * A one-day residency window. Every aged fixture below is stamped in 2025, so
     * the boundary puts it on the disk side without the test having to wait.
     */
    private static final long WINDOW_SECONDS = 24L * 60 * 60;

    /** Aged store epochs, in two different months so slice pruning has something to prune. */
    private static final long EPOCH_MAR = Instant.parse("2025-03-15T00:00:00Z").toEpochMilli();
    private static final long EPOCH_JUN = Instant.parse("2025-06-15T00:00:00Z").toEpochMilli();

    /** A store epoch inside the window: this row is memory-resident, never on disk. */
    private static final long EPOCH_FRESH = System.currentTimeMillis();

    private static HazelcastInstance hz;
    private static StratumEventStores stores;
    private static WarmStore warm;
    private static Server server;
    private static ManagedChannel channel;
    private static RecordMapGrpc.RecordMapBlockingStub stub;

    /**
     * A row as the subscription dispatch hands it over: store epoch stamped by the
     * store, business timestamp written by the emitting service. They are
     * deliberately far apart so an assertion cannot pass by reading the wrong clock.
     */
    private static AccountingRecord row(String id, long timestampMs, long storeEpochMs) {
        return AccountingRecord.newBuilder()
                .setAccountingId(id)
                .setRequestId("req-" + id)
                .setSourceId("stratum-0")
                .setTimestampMs(timestampMs)
                .setLevel(Accounting.EventLevel.EVENT_LEVEL_INFO)
                .setMethod("query.RecordMap/ListAccountingRecord")
                .setCode(0)
                .setDurationMs(7)
                .setPrincipalSub("joeuser@cav")
                .setSpiffeId("spiffe://cav/tenants/t1/services/stratum")
                .setStoreEpochMs(storeEpochMs)
                .build();
    }

    /** The GENERATED warm descriptor for AccountingRecord — the point of the test. */
    private static WarmDataset accountingDataset() {
        return StratumWarmStores.datasets(WINDOW_SECONDS).stream()
                .filter(d -> DATASET.equals(d.name))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "StratumWarmStores.datasets() has no descriptor for " + DATASET
                                + "; the RESIDENCY_WARM declaration did not reach the generator"));
    }

    @BeforeAll
    static void up(@TempDir Path dataRoot) throws Exception {
        Config cfg = new Config();
        cfg.setClusterName("stratum-warm-accounting-e2e-" + UUID.randomUUID());
        cfg.setProperty("hazelcast.phone.home.enabled", "false");
        cfg.getNetworkConfig().setPortAutoIncrement(true);
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        cfg.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        hz = Hazelcast.newHazelcastInstance(cfg);

        stores = new StratumEventStores();
        stores.init(hz, new HotWrites(hz, 0, 1));

        // The GENERATED descriptor list, at a window this test can cross.
        warm = new WarmStore(dataRoot, StratumWarmStores.datasets(WINDOW_SECONDS));

        // Aged rows: warm ingest only — the state eviction would leave (see class doc).
        assertTrue(warm.enqueue(row("acct-mar", 1000, EPOCH_MAR)),
                "the warm tier did not claim a declared RESIDENCY_WARM record");
        assertTrue(warm.enqueue(row("acct-jun", 2000, EPOCH_JUN)));
        warm.awaitWrites(10_000);
        assertEquals(0, warm.writeFailureCount(), "a warm disk write failed");
        assertEquals(0, warm.rejectedCount(), "a fixture failed the generated AccountingRecord validation");
        assertEquals(2, warm.writtenCount());

        // The in-window row: memory only.
        assertTrue(stores.store(row("acct-fresh", 3000, EPOCH_FRESH)));
        assertEquals(1, stores.accountingrecordAccountingRecord().size(),
                "the fresh fixture failed the generated validation and was dropped");

        var authorizer = new com.gitlab.secondstarcavalier.controlplane.schematic.services.stratum
                .model.chassis.grpcserver.Authorizer(sink -> { }) {
            @Override
            public TokenWrapper authorize(String fullMethod, Object request) throws ProtoException {
                return null;
            }
        };
        server = NettyServerBuilder.forPort(0)
                .addService(new RecordMap(new StratumRecordMaps(() -> stores, () -> warm), authorizer))
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

    // -----------------------------------------------------------------------
    // Query-plane helpers — the generated lifecycle, nothing hand-framed.
    // -----------------------------------------------------------------------

    /** Runs the generated Create/Count/List/Delete lifecycle and returns the rows. */
    private static List<AccountingRecord> pull(Query.AccountingRecordFilter filter) {
        Query.QueryHandle handle = stub.createAccountingRecordQuery(
                Query.OpenAccountingRecordQueryRequest.newBuilder()
                        .setFilter(filter)
                        .setStanding(false)
                        .build());
        try {
            List<AccountingRecord> items = new ArrayList<>();
            String pageToken = "";
            while (true) {
                Query.PullAccountingRecordResponse resp = stub.listAccountingRecord(
                        Query.PullAccountingRecordRequest.newBuilder()
                                .setQueryId(handle.getQueryId())
                                .setPageSize(100)
                                .setPageToken(pageToken)
                                .build());
                items.addAll(resp.getItemsList());
                if (resp.getNextPageToken().isEmpty()) {
                    return items;
                }
                pageToken = resp.getNextPageToken();
            }
        } finally {
            stub.deleteAccountingRecordQuery(handle);
        }
    }

    /** The generated Get&lt;T&gt;Count over the same handle — the merged, two-leg count. */
    private static long count(Query.AccountingRecordFilter filter) {
        Query.QueryHandle handle = stub.createAccountingRecordQuery(
                Query.OpenAccountingRecordQueryRequest.newBuilder()
                        .setFilter(filter)
                        .setStanding(false)
                        .build());
        try {
            return stub.getAccountingRecordCount(handle).getCount();
        } finally {
            stub.deleteAccountingRecordQuery(handle);
        }
    }

    private static List<String> idsOf(List<AccountingRecord> rows) {
        List<String> ids = new ArrayList<>();
        for (AccountingRecord r : rows) {
            ids.add(r.getAccountingId());
        }
        java.util.Collections.sort(ids);
        return ids;
    }

    // -----------------------------------------------------------------------

    /**
     * The declaration produced a warm descriptor, and it reads the STORE's clock.
     * A descriptor pointed at timestamp_ms would partition the two legs on the
     * emitting service's clock — the failure the epoch field exists to prevent —
     * and appendOnly is the mutability attribute arriving through the same seam.
     */
    @Test
    void theDeclarationGeneratesAWarmDescriptorReadingTheStoreEpoch() {
        WarmDataset d = accountingDataset();
        assertEquals("store_epoch_ms", d.epochField);
        assertTrue(d.appendOnly, "MUTABILITY_APPEND_ONLY did not reach the generated descriptor");
        assertEquals(WINDOW_SECONDS * 1000L, d.windowMs, "window_seconds is unset, so the default applies");
        assertEquals(AccountingRecord.class, d.type);

        AccountingRecord probe = row("probe", 1000, EPOCH_JUN);
        assertEquals("probe", d.key(probe), "the descriptor must key on accounting_id");
        assertEquals(EPOCH_JUN, d.epochMs(probe),
                "the descriptor must read store_epoch_ms, not the business timestamp");
        assertNotEquals(1000, d.epochMs(probe));
    }

    /**
     * The disk index is built from the record's own (spec.query_field) set, with
     * the store epoch excluded because it is indexed once under the reserved
     * {@code _epoch} field. A field that is filterable in memory is filterable on
     * disk by construction; this pins that the two lists have not drifted.
     */
    @Test
    void theWarmIndexCarriesExactlyTheAnnotatedFilterFieldsLessTheEpoch() {
        List<String> names = new ArrayList<>();
        for (WarmField f : accountingDataset().fields) {
            names.add(f.name);
        }
        assertEquals(List.of("request_id", "source_id", "timestamp_ms", "method", "spiffe_id"), names);
    }

    /**
     * Acceptance item 1: a warm row older than the window is queryable through the
     * SAME RPC as a fresh one, and the merged result spans both legs with no
     * duplication and no loss. The count is the same number by the independent
     * Get&lt;T&gt;Count path, which sums the legs rather than materialising them.
     */
    @Test
    void agedRowsComeBackFromDiskThroughTheSameGeneratedRpcAsTheFreshOne() {
        List<AccountingRecord> all = pull(Query.AccountingRecordFilter.newBuilder().build());
        assertEquals(List.of("acct-fresh", "acct-jun", "acct-mar"), idsOf(all));
        assertEquals(3, count(Query.AccountingRecordFilter.newBuilder().build()),
                "the merged count must equal the true row count across both legs");

        assertEquals(redacted(row("acct-mar", 1000, EPOCH_MAR)), served(all, "acct-mar"),
                "the row must round-trip through the warm tier's stored proto unchanged"
                        + " but for the egress redaction");
    }

    /**
     * Acceptance item 10, redaction half: a REDACT_MODE_MAY_REDACT field is redacted
     * in a DISK-served row exactly as in a memory-served one. The disk leg reparses
     * its own stored bytes, so it is a second path to the same egress sanitizer and
     * the one that could silently miss it.
     *
     * <p>The control is the stored form: the row on disk still HOLDS principal_sub.
     * Redaction is an egress property, and a test that could not tell "redacted on
     * the way out" from "never written" would prove the wrong thing.
     */
    @Test
    void aRedactedFieldIsRedactedIdenticallyOnBothLegs() throws Exception {
        List<AccountingRecord> all = pull(Query.AccountingRecordFilter.newBuilder().build());
        AccountingRecord fromDisk = served(all, "acct-mar");
        AccountingRecord fromMemory = served(all, "acct-fresh");

        assertEquals("", fromDisk.getPrincipalSub(), "a disk-served row leaked principal_sub");
        assertTrue(fromDisk.getPrincipalSubRedacted());
        assertEquals(fromMemory.getPrincipalSub(), fromDisk.getPrincipalSub());
        assertEquals(fromMemory.getPrincipalSubRedacted(), fromDisk.getPrincipalSubRedacted());

        List<byte[]> stored = warm.query(DATASET, new WarmFilter().eq("request_id", "req-acct-mar"));
        assertEquals(1, stored.size());
        assertEquals("joeuser@cav", AccountingRecord.parseFrom(stored.get(0)).getPrincipalSub(),
                "the warm tier must store the raw value; redaction happens on egress");
    }

    /** The named row out of a served page. */
    private static AccountingRecord served(List<AccountingRecord> rows, String id) {
        return rows.stream()
                .filter(r -> id.equals(r.getAccountingId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(id + " was not served"));
    }

    /** A fixture as the egress sanitizer hands it back to a caller holding no data grant. */
    private static AccountingRecord redacted(AccountingRecord r) {
        return r.toBuilder().clearPrincipalSub().setPrincipalSubRedacted(true).build();
    }

    /**
     * Acceptance item 1's paired control: an epoch bound inside the memory window
     * DEMONSTRABLY skips the disk leg. "Demonstrably" is the whole point —
     * asserting only that the disk rows are absent would pass just as well if every
     * slice had been opened and had matched nothing.
     */
    @Test
    void anEpochBoundInsideTheWindowSkipsTheDiskLeg() {
        long opened = warm.index(DATASET).slicesSearched();
        Query.AccountingRecordFilter insideWindow = Query.AccountingRecordFilter.newBuilder()
                .setStoreEpochMsFrom(EPOCH_FRESH)
                .build();

        assertEquals(List.of("acct-fresh"), idsOf(pull(insideWindow)));
        assertEquals(1, count(insideWindow));
        assertEquals(opened, warm.index(DATASET).slicesSearched(),
                "the disk leg opened a slice it could prove was empty");
    }

    /**
     * The caller's typed store-epoch range reaches the slice prune: a bound over
     * March alone opens exactly one slice. The generated filter translation
     * (warmFilterAccountingRecord) is the piece under test — a range that did not
     * reach the disk leg would open both slices and still return the right row.
     */
    @Test
    void aStoreEpochRangeOverOneMonthOpensOnlyThatSlice() {
        long opened = warm.index(DATASET).slicesSearched();
        List<AccountingRecord> march = pull(Query.AccountingRecordFilter.newBuilder()
                .setStoreEpochMsFrom(Instant.parse("2025-03-01T00:00:00Z").toEpochMilli())
                .setStoreEpochMsTo(Instant.parse("2025-03-31T00:00:00Z").toEpochMilli())
                .build());

        assertEquals(List.of("acct-mar"), idsOf(march));
        assertEquals(1, warm.index(DATASET).slicesSearched() - opened,
                "exactly one slice should have been opened");
    }

    /**
     * Acceptance item 6, generation end: the record query plane emits no
     * row-mutating RPC for an append-only record. The surface is PINNED rather than
     * merely scanned for Update/Delete, because Update&lt;T&gt;Query and
     * Delete&lt;T&gt;Query legitimately exist — they mutate the query predicate and
     * the query's lifetime, never a stored row — so a scan for the verb alone would
     * either fire on those or have to special-case them and prove nothing.
     */
    @Test
    void theGeneratedPlaneEmitsNoRowMutatingRpcForTheAppendOnlyRecord() {
        Descriptors.ServiceDescriptor svc = Query.getDescriptor().findServiceByName("RecordMap");
        List<String> mentioning = new ArrayList<>();
        for (Descriptors.MethodDescriptor m : svc.getMethods()) {
            if (m.getName().contains("AccountingRecord")) {
                mentioning.add(m.getName());
            }
            assertNotEquals("accountingrecord.AccountingRecord", m.getInputType().getFullName(),
                    m.getName() + " carries the append-only record as its request message");
            assertNotEquals("accountingrecord.AccountingRecord", m.getOutputType().getFullName(),
                    m.getName() + " carries the append-only record as its response message");
        }
        java.util.Collections.sort(mentioning);
        assertEquals(List.of(
                        "CreateAccountingRecordQuery",
                        "DeleteAccountingRecordQuery",
                        "GetAccountingRecordCount",
                        "GetAccountingRecordSample",
                        "ListAccountingRecord",
                        "SubscribeAccountingRecordCount",
                        "UpdateAccountingRecordQuery",
                        "WatchAccountingRecord"),
                mentioning,
                "the AccountingRecord query surface changed");
    }

    /**
     * Acceptance item 6, runtime end: a row mutation over the append-only record is
     * refused by the running server. The call is framed by hand ONLY because the
     * generated stub has no such method to call — which is the claim.
     *
     * <p>The control is the second half: the same hand-framed mechanism against an
     * RPC that DOES exist must not come back UNIMPLEMENTED, or this test would pass
     * on a mistyped service name and prove nothing.
     */
    @Test
    void aRowMutationOverTheAppendOnlyRecordIsRefusedByTheRunningServer() {
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
                () -> ClientCalls.blockingUnaryCall(channel, unaryMethod("DeleteAccountingRecord"),
                        CallOptions.DEFAULT, Query.QueryHandle.getDefaultInstance()));
        assertEquals(Status.Code.UNIMPLEMENTED, e.getStatus().getCode(),
                "the server hosts a row-deleting RPC for an append-only record");

        assertNotEquals(Status.Code.UNIMPLEMENTED, probe("DeleteAccountingRecordQuery"),
                "an RPC the plane does emit came back UNIMPLEMENTED; the probe is not reaching the service");
    }

    /** The status one hand-framed probe comes back with; OK when it does not fail. */
    private static Status.Code probe(String method) {
        try {
            ClientCalls.blockingUnaryCall(channel, unaryMethod(method), CallOptions.DEFAULT,
                    Query.QueryHandle.getDefaultInstance());
            return Status.Code.OK;
        } catch (StatusRuntimeException e) {
            return e.getStatus().getCode();
        }
    }

    /** A unary call against query.RecordMap by name, with QueryHandle on both sides. */
    private static MethodDescriptor<Query.QueryHandle, Query.QueryHandle> unaryMethod(String name) {
        return MethodDescriptor.<Query.QueryHandle, Query.QueryHandle>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(RecordMapGrpc.SERVICE_NAME + "/" + name)
                .setRequestMarshaller(ProtoUtils.marshaller(Query.QueryHandle.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(Query.QueryHandle.getDefaultInstance()))
                .build();
    }

    /**
     * Acceptance item 6, disk end: the declared append-only dataset APPENDS. Writing
     * the same accounting_id twice leaves both copies on disk, because the warm
     * index calls addDocument rather than updateDocument for an append-only
     * descriptor. A CRUD descriptor over the same rows would leave one.
     *
     * <p>Its own store, so the shared fixtures above are untouched.
     */
    @Test
    void writingTheSameKeyTwiceAppendsRatherThanRewrites(@TempDir Path dir) throws Exception {
        try (WarmStore own = new WarmStore(dir, StratumWarmStores.datasets(WINDOW_SECONDS))) {
            assertTrue(own.enqueue(row("acct-dup", 1000, EPOCH_MAR)));
            assertTrue(own.enqueue(row("acct-dup", 2000, EPOCH_MAR + 1)));
            own.awaitWrites(10_000);
            assertEquals(0, own.writeFailureCount());

            assertEquals(2, own.count(DATASET, new WarmFilter()),
                    "an append-only dataset replaced a row instead of appending to it");
        }
    }
}
