// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import com.github.tacplusdev.stratum.StratumRecordTypes;
import com.github.tacplusdev.stratum.hazelcast.StratumMapConfig;
import com.github.tacplusdev.stratum.model.types.services.letsencrypt.Letsencrypt.CertMagicState;
import com.github.tacplusdev.stratum.model.types.services.letsencrypt.Letsencrypt.LetsEncryptCert;
import com.github.tacplusdev.stratum.model.types.sys.errors.ProtoException;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoChange;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoOp;
import com.github.tacplusdev.stratum.model.types.vservices.query.Query;

/**
 * The record store's write path against a real Hazelcast member and an
 * in-memory redo log with the redo service's rules: the write answers, lock
 * order, apply-after-ack, refusal while the redo is unreachable, replay at start
 * and Watch delivering only acked writes (stratum#2's acceptance).
 */
class RecordStoreTest {

    private static final String TYPE = "letsencrypt.LetsEncryptCert";
    private static final String NS = "/tenants/acme0001";

    private final List<HazelcastInstance> members = new ArrayList<>();
    private final List<RecordStore> stores = new ArrayList<>();
    private final String cluster = "stratum-test-" + UUID.randomUUID();

    @AfterEach
    void down() {
        stores.forEach(RecordStore::shutdown);
        members.forEach(HazelcastInstance::shutdown);
    }

    private HazelcastInstance member() {
        Config cfg = StratumMapConfig.build("acme0001", List.of("127.0.0.1"), 1, null);
        cfg.setClusterName(cluster);
        cfg.getNetworkConfig().setPort(5801).setPortAutoIncrement(true);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        members.add(hz);
        return hz;
    }

    private RecordStore store(HazelcastInstance hz, RedoLog redo, Duration ackTimeout) {
        return store(hz, redo, ackTimeout, new EntryLocks());
    }

    private RecordStore store(HazelcastInstance hz, RedoLog redo, Duration ackTimeout, EntryLocks locks) {
        RecordStore s = new RecordStore(hz, new StratumRecordTypes(), redo, ackTimeout, locks);
        stores.add(s);
        return s;
    }

    /** A replayed store of one member over {@code redo}. */
    private RecordStore started(InMemoryRedoLog redo) throws ProtoException {
        RecordStore s = store(member(), redo, Duration.ofSeconds(5));
        s.catchUp();
        return s;
    }

    static LetsEncryptCert cert(String id, long expiresAt) {
        return cert(id, NS, expiresAt);
    }

    static LetsEncryptCert cert(String id, String ns, long expiresAt) {
        return LetsEncryptCert.newBuilder()
                .setCertSetId(id)
                .setCertmagicState(CertMagicState.getDefaultInstance())
                .addHostnames(id + ".example")
                .setExpiresAt(expiresAt)
                .setNamespace(ns)
                .build();
    }

    static Query.RecordOp put(LetsEncryptCert c) {
        return Query.RecordOp.newBuilder().setPutLetsEncryptCert(
                Query.PutLetsEncryptCertRequest.newBuilder().setItem(c)).build();
    }

    static Query.RecordOp insert(LetsEncryptCert c) {
        return Query.RecordOp.newBuilder().setInsertIfAbsentLetsEncryptCert(
                Query.InsertIfAbsentLetsEncryptCertRequest.newBuilder().setItem(c)).build();
    }

    static Query.RecordOp replace(LetsEncryptCert c, long expectedRevision) {
        return Query.RecordOp.newBuilder().setReplaceIfUnchangedLetsEncryptCert(
                Query.ReplaceIfUnchangedLetsEncryptCertRequest.newBuilder().setItem(c)
                        .setExpectedRevision(expectedRevision)).build();
    }

    static Query.RecordOp replaceIfItem(LetsEncryptCert c, LetsEncryptCert expected) {
        return Query.RecordOp.newBuilder().setReplaceIfUnchangedLetsEncryptCert(
                Query.ReplaceIfUnchangedLetsEncryptCertRequest.newBuilder().setItem(c)
                        .setExpectedItem(expected)).build();
    }

    static Query.RecordOp delete(String id) {
        return Query.RecordOp.newBuilder().setDeleteLetsEncryptCert(
                Query.DeleteLetsEncryptCertRequest.newBuilder().setCertSetId(id).setNamespace(NS)).build();
    }

    static Query.RecordApplyRequest ops(Query.RecordOp... ops) {
        return Query.RecordApplyRequest.newBuilder().addAllOps(List.of(ops)).build();
    }

    static long write(RecordStore s, Query.RecordOp op) throws ProtoException {
        return s.apply(ops(op)).getResults(0).getRevision();
    }

    static int code(Runnable r) {
        return assertThrows(ProtoException.class, r::run).getCode();
    }

    private static IMap<String, LetsEncryptCert> rows(RecordStore s) {
        return s.hazelcast().getMap(TYPE);
    }

    // -----------------------------------------------------------------------
    // The write answers
    // -----------------------------------------------------------------------

    @Test
    void insertIfAbsentOnAPresentEntryIsAlreadyExists() throws Exception {
        RecordStore s = started(new InMemoryRedoLog());
        long rev = write(s, insert(cert("a", 1)));
        assertTrue(rev > 0);

        assertEquals(ProtoException.ALREADY_EXISTS, code(() -> write(s, insert(cert("a", 2)))));
        assertEquals(1, rows(s).get("a").getExpiresAt(), "the refused insert changed nothing");
    }

    @Test
    void replaceIfUnchangedAnswersChangedUnderneathAndNotFound() throws Exception {
        RecordStore s = started(new InMemoryRedoLog());
        long r1 = write(s, put(cert("a", 1)));
        long r2 = write(s, replace(cert("a", 2), r1));
        assertTrue(r2 > r1, "revisions increase");

        // The revision it read is gone: changed underneath.
        assertEquals(ProtoException.ABORTED, code(() -> write(s, replace(cert("a", 3), r1))));
        // By value: the row it expected is not the stored one.
        assertEquals(ProtoException.ABORTED, code(() -> write(s, replaceIfItem(cert("a", 3), cert("a", 1)))));
        write(s, replaceIfItem(cert("a", 4), cert("a", 2)));
        assertEquals(4, rows(s).get("a").getExpiresAt());

        assertEquals(ProtoException.NOT_FOUND, code(() -> write(s, replace(cert("nope", 1), 1))));
    }

    @Test
    void deleteOfAnAbsentEntryIsNotFound() throws Exception {
        RecordStore s = started(new InMemoryRedoLog());
        write(s, put(cert("a", 1)));
        write(s, delete("a"));
        assertNull(rows(s).get("a"));
        assertEquals(ProtoException.NOT_FOUND, code(() -> write(s, delete("a"))));
    }

    @Test
    void getAnswersTheRowAndItsRevisionAndNotFound() throws Exception {
        RecordStore s = started(new InMemoryRedoLog());
        long rev = write(s, put(cert("a", 7)));
        RecordStore.Stored got = s.get(TYPE, StratumRecordTypes.letsEncryptCertKey("a"), NS);
        assertEquals(7, ((LetsEncryptCert) got.row).getExpiresAt());
        assertEquals(rev, got.revision);

        assertEquals(ProtoException.NOT_FOUND,
                code(() -> s.get(TYPE, StratumRecordTypes.letsEncryptCertKey("b"), NS)));
        // Another namespace's entry answers as absent.
        assertEquals(ProtoException.NOT_FOUND,
                code(() -> s.get(TYPE, StratumRecordTypes.letsEncryptCertKey("a"), "/tenants/other01")));
    }

    @Test
    void anEntryHeldToAnotherNamespaceIsNotOverwritten() throws Exception {
        RecordStore s = started(new InMemoryRedoLog());
        write(s, put(cert("a", 1)));
        assertEquals(ProtoException.PERMISSION_DENIED,
                code(() -> write(s, put(cert("a", "/tenants/other01", 2)))));
        assertEquals(NS, rows(s).get("a").getNamespace());
    }

    @Test
    void anApplyIsAllOrNothing() throws Exception {
        InMemoryRedoLog redo = new InMemoryRedoLog();
        RecordStore s = started(redo);
        write(s, put(cert("b", 1)));
        int appended = redo.appends;

        // The second op fails its condition: the first is not written either.
        ProtoException e = assertThrows(ProtoException.class,
                () -> s.apply(ops(insert(cert("a", 1)), insert(cert("b", 2)))));
        assertEquals(ProtoException.ALREADY_EXISTS, e.getCode());
        assertTrue(e.getMessage().startsWith("op 1:"), e.getMessage());
        assertNull(rows(s).get("a"));
        assertEquals(appended, redo.appends, "a refused operation is never appended");

        Query.RecordApplyResponse ok = s.apply(ops(insert(cert("a", 1)), put(cert("b", 3))));
        assertEquals(2, ok.getResultsCount());
        assertEquals(2, redo.tail().get(redo.tail().size() - 1).getChangesCount(),
                "the whole Apply is ONE log entry");
    }

    @Test
    void anOperationNamingAnEntryTwiceIsRefused() throws Exception {
        RecordStore s = started(new InMemoryRedoLog());
        assertEquals(ProtoException.INVALID_ARGUMENT,
                code(() -> s.apply(ops(put(cert("a", 1)), put(cert("a", 2))))));
    }

    // -----------------------------------------------------------------------
    // Lock order
    // -----------------------------------------------------------------------

    @Test
    void entryLocksAreTakenInCanonicalOrderWhateverTheOpOrder() throws Exception {
        List<EntryId> taken = java.util.Collections.synchronizedList(new ArrayList<>());
        InMemoryRedoLog redo = new InMemoryRedoLog();
        RecordStore s = store(member(), redo, Duration.ofSeconds(5), new EntryLocks(taken::add));
        s.catchUp();

        s.apply(ops(put(cert("c", 1)), put(cert("a", 1)), put(cert("b", 1))));
        assertEquals(List.of("a", "b", "c"), taken.stream().map(EntryId::mapKey).toList());
    }

    @Test
    void canonicalKeyOrderIsUtf8ByteOrder() {
        // U+FFFF sorts BEFORE U+1F600 in UTF-8 (EF BF BF < F0 9F 98 80) but AFTER it
        // in UTF-16 code units (FFFF > D83D): the order is the bytes'.
        EntryId bmp = new EntryId(TYPE, StratumRecordTypes.letsEncryptCertKey("￿"));
        EntryId astral = new EntryId(TYPE, StratumRecordTypes.letsEncryptCertKey("😀"));
        assertTrue(bmp.compareTo(astral) < 0);
        // Type first, then key.
        EntryId otherType = new EntryId("a.Z", StratumRecordTypes.letsEncryptCertKey("zzz"));
        assertTrue(otherType.compareTo(bmp) < 0);
    }

    @Test
    void opposedMultiEntryWritesDoNotDeadlock() throws Exception {
        RecordStore s = started(new InMemoryRedoLog());
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> ab = pool.submit(() -> {
                for (int i = 0; i < 200; i++) {
                    s.apply(ops(put(cert("a", i)), put(cert("b", i))));
                }
                return null;
            });
            Future<?> ba = pool.submit(() -> {
                for (int i = 0; i < 200; i++) {
                    s.apply(ops(put(cert("b", i)), put(cert("a", i))));
                }
                return null;
            });
            ab.get(60, TimeUnit.SECONDS);
            ba.get(60, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
    }

    // -----------------------------------------------------------------------
    // Apply after ack
    // -----------------------------------------------------------------------

    @Test
    void aWriteIsInvisibleUntilTheRedoAcksIt() throws Exception {
        InMemoryRedoLog redo = new InMemoryRedoLog();
        RecordStore s = started(redo);
        Iterator<ChangeFeed.Change> watch = s.feed().watch(TYPE, null);
        redo.holdAcks(true);

        CompletableFuture<Long> w = CompletableFuture.supplyAsync(() -> write(s, put(cert("a", 1))));
        waitFor(() -> redo.tail().size() == 1);

        // In the log, not acked: not in the maps, not in the feed.
        assertNull(rows(s).get("a"));
        assertFalse(w.isDone());
        assertEquals(0, s.feed().lastRevision(TYPE));

        redo.releaseAcks();
        long rev = w.get(10, TimeUnit.SECONDS);
        assertEquals(1, rows(s).get("a").getExpiresAt());
        ChangeFeed.Change c = watch.next();
        assertEquals(rev, c.revision);
        assertEquals(ChangeFeed.Kind.ADD, c.kind);
    }

    @Test
    void aWriteTheRedoNeverAcksIsRefusedAndAppliedOnlyByTheNextReplay() throws Exception {
        InMemoryRedoLog redo = new InMemoryRedoLog();
        RecordStore s = store(member(), redo, Duration.ofMillis(300));
        s.catchUp();
        redo.holdAcks(true);

        assertEquals(ProtoException.UNAVAILABLE, code(() -> write(s, put(cert("a", 1)))));
        assertNull(rows(s).get("a"), "an unacked write is not applied");
        assertFalse(s.inSync(), "the store lost its place in the log");

        // It is in the log, though: the log is the database, and the next write's
        // replay applies it before writing.
        redo.holdAcks(false);
        write(s, put(cert("b", 1)));
        assertEquals(1, rows(s).get("a").getExpiresAt());
        assertEquals(2, redo.lastSeq());
    }

    // -----------------------------------------------------------------------
    // Refusal while the redo is unreachable
    // -----------------------------------------------------------------------

    @Test
    void writesAreRefusedWhileTheRedoIsUnreachable() throws Exception {
        InMemoryRedoLog redo = new InMemoryRedoLog();
        RecordStore s = started(redo);
        Iterator<ChangeFeed.Change> watch = s.feed().watch(TYPE, null);
        redo.setReachable(false);

        assertEquals(ProtoException.UNAVAILABLE, code(() -> write(s, put(cert("a", 1)))));
        assertEquals(ProtoException.UNAVAILABLE, code(() -> write(s, put(cert("a", 1)))),
                "still refused: the replay before the write cannot reach the redo either");
        assertNull(rows(s).get("a"));
        assertEquals(0, redo.lastSeq());

        redo.setReachable(true);
        long rev = write(s, put(cert("a", 2)));
        assertEquals(rev, watch.next().revision, "the first change a watcher sees is the acked one");
        assertEquals(1, redo.lastSeq());
    }

    @Test
    void remoteIsRefusedWhileTheRedoHasNoSecondSite() throws Exception {
        RecordStore s = started(new InMemoryRedoLog());
        Query.RecordApplyRequest remote = ops(put(cert("a", 1))).toBuilder()
                .setDurability(Query.RecordDurability.RECORD_DURABILITY_REMOTE).build();
        assertEquals(ProtoException.FAILED_PRECONDITION, code(() -> s.apply(remote)));
        assertNull(rows(s).get("a"));
    }

    @Test
    void readsAndWritesAreRefusedUntilTheStoreHasReplayed() throws Exception {
        InMemoryRedoLog redo = new InMemoryRedoLog();
        redo.setReachable(false);
        RecordStore s = store(member(), redo, Duration.ofSeconds(5));
        assertThrows(ProtoException.class, s::catchUp);

        assertFalse(s.readable());
        assertEquals(ProtoException.UNAVAILABLE, code(s::requireReadable));
        assertEquals(ProtoException.UNAVAILABLE, code(() -> write(s, put(cert("a", 1)))));
    }

    // -----------------------------------------------------------------------
    // Replay at start
    // -----------------------------------------------------------------------

    private static RedoChange putChange(LetsEncryptCert c, long revision) {
        return RedoChange.newBuilder().setRecordType(TYPE)
                .setKey(StratumRecordTypes.letsEncryptCertKey(c.getCertSetId()))
                .setOp(RedoOp.REDO_OP_PUT).setValue(c.toByteString()).setRevision(revision).build();
    }

    private static RedoChange deleteChange(String id, long revision) {
        return RedoChange.newBuilder().setRecordType(TYPE)
                .setKey(StratumRecordTypes.letsEncryptCertKey(id))
                .setOp(RedoOp.REDO_OP_DELETE).setRevision(revision).build();
    }

    @Test
    void startReplaysTheSnapshotThenTheTail() throws Exception {
        InMemoryRedoLog redo = new InMemoryRedoLog();
        redo.seedSnapshot(10, putChange(cert("a", 1), 4), putChange(cert("b", 1), 9));
        redo.seedTail(11, putChange(cert("a", 2), 11));
        redo.seedTail(12, deleteChange("b", 12), putChange(cert("c", 3), 13));

        RecordStore s = store(member(), redo, Duration.ofSeconds(5));
        assertFalse(s.readable());
        s.catchUp();

        assertTrue(s.readable());
        assertEquals(2, rows(s).get("a").getExpiresAt());
        assertNull(rows(s).get("b"));
        assertEquals(3, rows(s).get("c").getExpiresAt());
        assertEquals(11, s.get(TYPE, StratumRecordTypes.letsEncryptCertKey("a"), NS).revision,
                "a replay restores revisions exactly");

        // The store continues the log where it ends, with higher revisions.
        assertEquals(13, s.nextSeq());
        long rev = write(s, put(cert("d", 1)));
        assertTrue(rev > 13);
        assertEquals(13, redo.lastSeq());
    }

    @Test
    void aRestartedStoreHoldsEveryAckedWrite() throws Exception {
        InMemoryRedoLog redo = new InMemoryRedoLog();
        RecordStore first = started(redo);
        write(first, put(cert("a", 1)));
        long rev = write(first, put(cert("b", 2)));
        write(first, delete("a"));
        first.shutdown();
        members.get(0).shutdown();

        // A full restart: a new cluster, nothing in memory, the same log.
        RecordStore second = store(member(), redo, Duration.ofSeconds(5));
        second.catchUp();
        assertNull(rows(second).get("a"));
        assertEquals(2, rows(second).get("b").getExpiresAt());
        assertEquals(rev, second.get(TYPE, StratumRecordTypes.letsEncryptCertKey("b"), NS).revision);
    }

    // -----------------------------------------------------------------------
    // The sequencer
    // -----------------------------------------------------------------------

    @Test
    void anotherMemberForwardsItsWritesAndReadsToTheSequencer() throws Exception {
        InMemoryRedoLog redo = new InMemoryRedoLog();
        RecordStore sequencer = started(redo);
        RecordStore other = store(member(), redo, Duration.ofSeconds(5));
        waitFor(() -> members.get(1).getCluster().getMembers().size() == 2);
        assertFalse(other.isSequencer());

        long rev = write(other, insert(cert("a", 1)));
        assertEquals(ProtoException.ALREADY_EXISTS, code(() -> write(other, insert(cert("a", 2)))));
        assertEquals(rev, other.get(TYPE, StratumRecordTypes.letsEncryptCertKey("a"), NS).revision);
        assertEquals(1, redo.lastSeq(), "one writer of the log");
        assertTrue(other.readable());
        assertEquals(1, rows(sequencer).get("a").getExpiresAt());
    }

    @Test
    void theNextSequencerReplaysTheTailItsPredecessorNeverApplied() throws Exception {
        InMemoryRedoLog redo = new InMemoryRedoLog();
        RecordStore first = started(redo);
        RecordStore second = store(member(), redo, Duration.ofSeconds(5));
        waitFor(() -> members.get(1).getCluster().getMembers().size() == 2);
        write(first, put(cert("a", 1)));

        // An append the old sequencer made and never applied.
        redo.seedTail(2, putChange(cert("z", 9), 100));
        first.shutdown();
        members.get(0).shutdown();
        waitFor(second::isSequencer);

        write(second, put(cert("b", 1)));
        assertEquals(9, rows(second).get("z").getExpiresAt());
        assertEquals(1, rows(second).get("a").getExpiresAt());
        assertEquals(3, redo.lastSeq());
        assertTrue(write(second, put(cert("c", 1))) > 100, "revisions stay above everything in the log");
    }

    // -----------------------------------------------------------------------
    // Watch
    // -----------------------------------------------------------------------

    @Test
    void watchDeliversOnlyAckedWritesInRevisionOrder() throws Exception {
        InMemoryRedoLog redo = new InMemoryRedoLog();
        RecordStore s = started(redo);
        Iterator<ChangeFeed.Change> watch = s.feed().watch(TYPE, NS);

        long r1 = write(s, put(cert("a", 1)));
        redo.setReachable(false);
        code(() -> write(s, put(cert("b", 1))));           // refused: never seen
        redo.setReachable(true);
        code(() -> write(s, insert(cert("a", 2))));        // refused by its condition
        write(s, put(cert("x", "/tenants/other01", 1)));    // another namespace
        long r2 = write(s, put(cert("a", 3)));
        long r3 = write(s, delete("a"));

        ChangeFeed.Change c1 = watch.next();
        ChangeFeed.Change c2 = watch.next();
        ChangeFeed.Change c3 = watch.next();
        assertEquals(List.of(r1, r2, r3), List.of(c1.revision, c2.revision, c3.revision));
        assertEquals(List.of(ChangeFeed.Kind.ADD, ChangeFeed.Kind.MODIFY, ChangeFeed.Kind.DELETE),
                List.of(c1.kind, c2.kind, c3.kind));
        assertEquals(3, LetsEncryptCert.parseFrom(c3.row).getExpiresAt(), "a delete carries the deleted row");
    }

    @Test
    void aWatcherOnAnotherMemberSeesTheSequencersCommits() throws Exception {
        InMemoryRedoLog redo = new InMemoryRedoLog();
        RecordStore sequencer = started(redo);
        RecordStore other = store(member(), redo, Duration.ofSeconds(5));
        waitFor(() -> members.get(1).getCluster().getMembers().size() == 2);
        Iterator<ChangeFeed.Change> watch = other.feed().watch(TYPE, null);

        long rev = write(sequencer, put(cert("a", 1)));
        CompletableFuture<ChangeFeed.Change> next = CompletableFuture.supplyAsync(watch::next);
        assertEquals(rev, next.get(10, TimeUnit.SECONDS).revision);
    }

    private static void waitFor(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition not reached in 30s");
            }
            Thread.sleep(20);
        }
    }
}
