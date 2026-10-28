// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.store;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;
import com.hazelcast.topic.ITopic;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.github.tacplusdev.stratum.model.types.sys.errors.ProtoException;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoAppend;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoChange;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoKey;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoOp;
import com.github.tacplusdev.stratum.model.types.sys.redolog.RedoOuterClass.RedoReplayEntry;
import com.github.tacplusdev.stratum.model.types.sys.spec.Spec.Durability;
import com.github.tacplusdev.stratum.model.types.vservices.query.Query;

/**
 * A tenant's record store: its rows in memory, its writes made durable by the
 * redo service before they are applied.
 *
 * <h2>Where state lives</h2>
 * Rows are in one Hazelcast map per record type (the generated query plane
 * scans them), each entry's revision in {@value #REVISIONS_MAP}, and the
 * store's place in the redo log in {@value #META_MAP}. All of it is memory,
 * replicated across the members for availability. None of it is durable: the
 * redo log is (decisions: stratum-per-tenant-2026-10-25).
 *
 * <h2>The sequencer</h2>
 * The redo log is one sequence per tenant, and an append is refused unless its
 * seq follows the last one exactly, so the store has exactly one writer of it:
 * the oldest member of the cluster, the sequencer. Every write and every point
 * read runs there; another member forwards it ({@link SequencerCall}). The
 * entry locks are therefore JVM locks in the sequencer.
 *
 * <h2>A write</h2>
 * <ol>
 *   <li>Take the entries' locks, in canonical order ({@link EntryLocks}).</li>
 *   <li>Check every op's condition against the stored entries: InsertIfAbsent
 *       on a present entry is ALREADY_EXISTS, ReplaceIfUnchanged on a changed one
 *       ABORTED, ReplaceIfUnchanged or Delete on an absent one NOT_FOUND. One
 *       failure refuses the whole operation and nothing is written.</li>
 *   <li>Assign the revisions and the next seq, and append the operation to the
 *       redo at the level it is owed: the highest of its types' levels and what
 *       it asked for. A MEMORY write is not appended.</li>
 *   <li>Wait for the redo's ack at that level. No ack in time, a refusal or an
 *       unreachable redo refuses the write (UNAVAILABLE): nothing is applied and
 *       nothing is published, and the store replays before it writes again.</li>
 *   <li>Apply it to the maps, in seq order, publish it to the change feed, and
 *       answer.</li>
 * </ol>
 * Nothing reads or watches a write before its ack, because nothing is in the
 * maps or the feed before it.
 *
 * <h2>Start</h2>
 * Every start replays the tenant from the redo ({@link #catchUp}): the snapshot,
 * then the tail. Reads and writes are refused (UNAVAILABLE) until it has. A
 * member that becomes the sequencer replays too, applying only what the store
 * has not applied yet, so a write the old sequencer appended but never applied
 * is not lost: the log is the database.
 */
public final class RecordStore {

    private static final Logger log = LoggerFactory.getLogger(RecordStore.class);

    /** Each entry's revision, keyed by {@link #revisionKey}. */
    public static final String REVISIONS_MAP = "stratum.revisions";
    /** The store's place in the redo log and whether it has replayed. */
    public static final String META_MAP = "stratum.meta";
    /** Applied operations, published by the sequencer in commit order. */
    public static final String CHANGES_TOPIC = "stratum.changes";
    /** Writes and point reads forwarded to the sequencer. */
    public static final String SEQUENCER_EXECUTOR = "stratum.sequencer";

    static final String APPLIED_SEQ = "applied_seq";
    static final String LAST_REVISION = "last_revision";
    static final String REPLAYED = "replayed";

    private static final String USER_CONTEXT_KEY = "stratum.record-store";

    private final HazelcastInstance hz;
    private final RecordTypes types;
    private final RedoLog redo;
    private volatile long ackTimeoutMs;
    private final EntryLocks locks;
    private final ChangeFeed feed = new ChangeFeed();
    private final IMap<String, Long> revisions;
    private final IMap<String, Long> meta;
    private final ITopic<ChangeFeed.Committed> changes;
    private final UUID changesListener;

    /** Writes hold it shared; a catch-up holds it exclusively. */
    private final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();

    // The sequencer's place, meaningful only while inSync. Guarded by `order`.
    private final Object order = new Object();
    private long nextSeq;
    private long nextRevision;
    private long nextTicket;
    private long turn;
    private volatile boolean inSync;
    private volatile boolean readable;

    public RecordStore(HazelcastInstance hz, RecordTypes types, RedoLog redo, Duration ackTimeout) {
        this(hz, types, redo, ackTimeout, new EntryLocks());
    }

    RecordStore(HazelcastInstance hz, RecordTypes types, RedoLog redo, Duration ackTimeout, EntryLocks locks) {
        this.hz = hz;
        this.types = types;
        this.redo = redo;
        this.ackTimeoutMs = ackTimeout.toMillis();
        this.locks = locks;
        this.revisions = hz.getMap(REVISIONS_MAP);
        this.meta = hz.getMap(META_MAP);
        this.changes = hz.getReliableTopic(CHANGES_TOPIC);
        this.changesListener = changes.addMessageListener(new ChangeListener(feed));
        hz.getUserContext().put(USER_CONTEXT_KEY, this);
        hz.getCluster().addMembershipListener(new com.hazelcast.cluster.MembershipListener() {
            @Override
            public void memberAdded(com.hazelcast.cluster.MembershipEvent e) {
                // A cluster merge can make another member the oldest. This one then
                // stops writing the log, and must replay if it is ever its writer again.
                if (!isSequencer()) {
                    inSync = false;
                }
            }

            @Override
            public void memberRemoved(com.hazelcast.cluster.MembershipEvent e) {
                // The sequencer may have left. If this member is the new one it
                // does not know the log's tail: it replays before it writes.
                if (isSequencer() && inSync && !e.getMember().localMember()) {
                    log.info("member {} left; this member is the sequencer and replays before its next write",
                            e.getMember().getAddress());
                    inSync = false;
                }
            }
        });
    }

    public HazelcastInstance hazelcast() { return hz; }

    /** How long a write waits for the redo's ack before it is refused. */
    public void setAckTimeout(Duration ackTimeout) {
        this.ackTimeoutMs = ackTimeout.toMillis();
    }

    public RecordTypes types() { return types; }

    public ChangeFeed feed() { return feed; }

    /** Whether this member is the one that writes the redo log. */
    public boolean isSequencer() {
        Set<Member> members = hz.getCluster().getMembers();
        return !members.isEmpty() && members.iterator().next().localMember();
    }

    /**
     * Whether the store has replayed the tenant since the cluster started, which
     * is what makes a read answerable. Raised once by the sequencer's first
     * catch-up and never lowered: a later sequencer's catch-up only adds a tail.
     */
    public boolean readable() {
        if (!readable) {
            Long r = meta.get(REPLAYED);
            readable = r != null && r == 1L;
        }
        return readable;
    }

    /** UNAVAILABLE until {@link #readable}. */
    public void requireReadable() throws ProtoException {
        if (!readable()) {
            throw new ProtoException(ProtoException.UNAVAILABLE,
                    "the record store has not replayed its tenant from the redo service yet");
        }
    }

    /** Rows held across every record type. */
    public long rowCount() {
        long n = 0;
        for (RecordType t : types.all()) {
            n += map(t).size();
        }
        return n;
    }

    private IMap<String, Message> map(RecordType t) {
        return hz.getMap(t.fullName());
    }

    static String revisionKey(EntryId id) {
        return id.type() + '\u0000' + id.mapKey();
    }

    // -----------------------------------------------------------------------
    // Start: replay
    // -----------------------------------------------------------------------

    /**
     * Brings the sequencer's place in the log up to date: replays the tenant and
     * applies whatever the store has not applied. Does nothing on a member that
     * is not the sequencer, or on one already in sync.
     *
     * @throws ProtoException UNAVAILABLE when the redo cannot be replayed
     */
    public void catchUp() throws ProtoException {
        if (!isSequencer() || inSync) {
            return;
        }
        gate.writeLock().lock();
        try {
            if (!isSequencer() || inSync) {
                return;
            }
            replayLocked();
        } finally {
            gate.writeLock().unlock();
        }
    }

    private void replayLocked() throws ProtoException {
        long applied = meta.getOrDefault(APPLIED_SEQ, 0L);
        long lastRevision = meta.getOrDefault(LAST_REVISION, 0L);
        boolean fresh = applied == 0 && !readable();
        redo.reset();
        Replay r = new Replay(applied, fresh);
        long started = System.currentTimeMillis();
        try {
            redo.replay(r::entry);
            r.end();
        } catch (RuntimeException e) {
            throw new ProtoException(ProtoException.UNAVAILABLE,
                    "replaying the tenant from the redo service failed: " + e.getMessage(), e);
        }
        synchronized (order) {
            nextSeq = r.lastSeq + 1;
            nextRevision = Math.max(r.maxRevision, lastRevision) + 1;
            nextTicket = 0;
            turn = 0;
        }
        meta.set(APPLIED_SEQ, r.lastSeq);
        meta.set(LAST_REVISION, nextRevision - 1);
        meta.set(REPLAYED, 1L);
        readable = true;
        inSync = true;
        log.info("replayed the tenant from the redo service in {} ms: log at seq {}, {} operation(s) applied"
                        + " (store had applied seq {}), next revision {}",
                System.currentTimeMillis() - started, r.lastSeq, r.appliedOps, applied, nextRevision);
    }

    /** One replay's progress. */
    private final class Replay {
        final long applied;
        final boolean fresh;
        long lastSeq;
        long maxRevision;
        long appliedOps;
        boolean inSnapshot = true;
        boolean resync;
        final Map<String, Set<String>> snapshotKeys = new HashMap<>();

        Replay(long applied, boolean fresh) {
            this.applied = applied;
            this.fresh = fresh;
            this.lastSeq = applied;
        }

        void entry(RedoReplayEntry e) {
            if (e.getFromSnapshot()) {
                if (e.getSeq() <= applied) {
                    // The maps already hold everything this snapshot folds in.
                    noteRevisions(e);
                    return;
                }
                resync = true;
                apply(e, false);
                for (RedoChange c : e.getChangesList()) {
                    snapshotKeys.computeIfAbsent(c.getRecordType(), t -> new HashSet<>())
                            .add(EntryId.mapKey(c.getKey()));
                }
            } else {
                endSnapshot();
                if (e.getSeq() <= applied) {
                    noteRevisions(e);
                    return;
                }
                apply(e, !fresh && !resync);
            }
            lastSeq = Math.max(lastSeq, e.getSeq());
        }

        void end() {
            endSnapshot();
            if (resync && !fresh) {
                // The rows were rebuilt underneath any open watch: end them all, on
                // every member, so each re-watches from a snapshot.
                changes.publish(new ChangeFeed.Committed(List.of(), true));
            }
        }

        /**
         * A snapshot newer than what the maps hold replaces them: an entry the
         * snapshot does not carry was deleted inside it.
         */
        private void endSnapshot() {
            if (!inSnapshot) {
                return;
            }
            inSnapshot = false;
            if (!resync || fresh) {
                return;
            }
            // A whole-type operation: the map locks, the rare case they exist for.
            List<String> logged = new ArrayList<>();
            for (RecordType t : types.all()) {
                // A MEMORY type's rows are never in the log, so the snapshot's
                // silence about them is not a delete.
                if (t.durability() != Durability.DURABILITY_MEMORY) {
                    logged.add(t.fullName());
                }
            }
            try (EntryLocks.Hold held = locks.lockMaps(logged)) {
                for (String name : logged) {
                    RecordType t = types.byName(name);
                    Set<String> keep = snapshotKeys.getOrDefault(name, Set.of());
                    for (String k : new ArrayList<>(map(t).keySet())) {
                        if (!keep.contains(k)) {
                            map(t).delete(k);
                            revisions.delete(name + '\u0000' + k);
                        }
                    }
                }
            }
        }

        private void noteRevisions(RedoReplayEntry e) {
            for (RedoChange c : e.getChangesList()) {
                maxRevision = Math.max(maxRevision, c.getRevision());
            }
        }

        private void apply(RedoReplayEntry e, boolean publish) {
            List<ChangeFeed.Change> out = new ArrayList<>();
            for (RedoChange c : e.getChangesList()) {
                maxRevision = Math.max(maxRevision, c.getRevision());
                RecordType t = types.byName(c.getRecordType());
                if (t == null) {
                    // A type this build no longer holds: its rows stay in the log.
                    continue;
                }
                EntryId id = new EntryId(t.fullName(), c.getKey());
                if (c.getOp() == RedoOp.REDO_OP_DELETE) {
                    Message old = map(t).remove(id.mapKey());
                    revisions.delete(revisionKey(id));
                    if (old != null) {
                        out.add(new ChangeFeed.Change(t.fullName(), id.mapKey(), ChangeFeed.Kind.DELETE,
                                t.namespaceOf(old), old.toByteArray(), c.getRevision()));
                    }
                } else {
                    Message row;
                    try {
                        row = t.parse(c.getValue());
                    } catch (InvalidProtocolBufferException ex) {
                        throw new IllegalStateException("redo entry seq " + e.getSeq() + " holds a "
                                + t.fullName() + " that does not parse", ex);
                    }
                    Message old = map(t).put(id.mapKey(), row);
                    revisions.set(revisionKey(id), c.getRevision());
                    out.add(new ChangeFeed.Change(t.fullName(), id.mapKey(),
                            old == null ? ChangeFeed.Kind.ADD : ChangeFeed.Kind.MODIFY,
                            t.namespaceOf(row), c.getValue().toByteArray(), c.getRevision()));
                }
            }
            appliedOps++;
            if (publish && !out.isEmpty()) {
                changes.publish(new ChangeFeed.Committed(out));
            }
        }
    }

    // -----------------------------------------------------------------------
    // Writes
    // -----------------------------------------------------------------------

    /**
     * Applies an operation (one write, or an Apply of several), all or nothing,
     * once the redo has acked it. Runs on the sequencer; any other member
     * forwards it.
     */
    public Query.RecordApplyResponse apply(Query.RecordApplyRequest request) throws ProtoException {
        if (!isSequencer()) {
            byte[] reply = forward(SequencerCall.apply(request));
            try {
                return Query.RecordApplyResponse.parseFrom(reply);
            } catch (InvalidProtocolBufferException e) {
                throw new ProtoException(ProtoException.INTERNAL, "sequencer reply: " + e.getMessage(), e);
            }
        }
        return applyHere(request);
    }

    Query.RecordApplyResponse applyHere(Query.RecordApplyRequest request) throws ProtoException {
        List<WriteOp> ops = new ArrayList<>();
        for (Query.RecordOp op : request.getOpsList()) {
            ops.add(types.decode(op));
        }
        if (ops.isEmpty()) {
            throw new ProtoException(ProtoException.INVALID_ARGUMENT, "the operation carries no write");
        }
        Set<EntryId> seen = new HashSet<>();
        List<EntryId> entries = new ArrayList<>();
        Durability owed = Durability.forNumber(Math.max(Durability.DURABILITY_MEMORY_VALUE,
                request.getDurabilityValue()));
        for (WriteOp op : ops) {
            if (!seen.add(op.entry)) {
                throw new ProtoException(ProtoException.INVALID_ARGUMENT,
                        "the operation names " + op.entry + " twice; each entry once");
            }
            entries.add(op.entry);
            owed = higher(higher(owed, op.durability), op.type.durability());
        }

        catchUp();
        gate.readLock().lock();
        try {
            if (!inSync || !isSequencer()) {
                throw new ProtoException(ProtoException.UNAVAILABLE,
                        "the record store is not in sync with the redo service; retry");
            }
            try (EntryLocks.Hold held = locks.lockEntries(entries)) {
                return commit(ops, owed);
            }
        } finally {
            gate.readLock().unlock();
        }
    }

    private static Durability higher(Durability a, Durability b) {
        if (b == null || b == Durability.UNRECOGNIZED) {
            return a;
        }
        return b.getNumber() > a.getNumber() ? b : a;
    }

    /** Steps 2-5 of a write, with every entry it touches locked. */
    private Query.RecordApplyResponse commit(List<WriteOp> ops, Durability owed) throws ProtoException {
        List<Message> stored = new ArrayList<>(ops.size());
        for (int i = 0; i < ops.size(); i++) {
            WriteOp op = ops.get(i);
            Message row = map(op.type).get(op.entry.mapKey());
            check(op, row, i, ops.size());
            checkUnchanged(op, row, i, ops.size());
            stored.add(row);
        }

        boolean logged = owed != Durability.DURABILITY_MEMORY;
        long ticket;
        long seq = 0;
        long firstRevision;
        CompletableFuture<Void> acked;
        List<RedoChange> redoChanges = new ArrayList<>(ops.size());
        synchronized (order) {
            ticket = nextTicket++;
            firstRevision = nextRevision;
            nextRevision += ops.size();
            for (int i = 0; i < ops.size(); i++) {
                WriteOp op = ops.get(i);
                RedoChange.Builder c = RedoChange.newBuilder()
                        .setRecordType(op.type.fullName())
                        .setKey(op.entry.key())
                        .setRevision(firstRevision + i);
                if (op.kind == WriteOp.Kind.DELETE) {
                    c.setOp(RedoOp.REDO_OP_DELETE);
                } else {
                    c.setOp(RedoOp.REDO_OP_PUT).setValue(op.item.toByteString());
                }
                redoChanges.add(c.build());
            }
            if (logged) {
                seq = nextSeq++;
                acked = redo.append(RedoAppend.newBuilder()
                        .setTenant(GrpcRedoLog.REDO_TENANT)
                        .setSeq(seq)
                        .addAllChanges(redoChanges)
                        .setDurability(owed)
                        .build());
            } else {
                acked = CompletableFuture.completedFuture(null);
            }
        }

        try {
            acked.get(ackTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // The append may or may not be in the log. Nothing is applied, and the
            // store replays (applying it if it is there) before it writes again.
            inSync = false;
            redo.reset();
            awaitTurn(ticket);
            endTurn(ticket);
            throw refusal(e, seq, owed);
        }

        awaitTurn(ticket);
        if (!inSync) {
            // An earlier write's ack never came, so the store no longer knows the
            // log: this one may follow a write that is in the log and not in the
            // maps. It is not applied here; the replay applies it, in order.
            endTurn(ticket);
            throw new ProtoException(ProtoException.UNAVAILABLE, "an earlier write was refused and the store"
                    + " replays before it applies more; this write's outcome is decided by that replay");
        }
        try {
            List<ChangeFeed.Change> out = new ArrayList<>(ops.size());
            Query.RecordApplyResponse.Builder response = Query.RecordApplyResponse.newBuilder();
            Query.RecordDurability level = Query.RecordDurability.forNumber(owed.getNumber());
            for (int i = 0; i < ops.size(); i++) {
                WriteOp op = ops.get(i);
                long revision = firstRevision + i;
                String key = op.entry.mapKey();
                if (op.kind == WriteOp.Kind.DELETE) {
                    Message old = stored.get(i);
                    map(op.type).delete(key);
                    revisions.delete(revisionKey(op.entry));
                    out.add(new ChangeFeed.Change(op.type.fullName(), key, ChangeFeed.Kind.DELETE,
                            op.type.namespaceOf(old), old.toByteArray(), revision));
                } else {
                    map(op.type).set(key, op.item);
                    revisions.set(revisionKey(op.entry), revision);
                    out.add(new ChangeFeed.Change(op.type.fullName(), key,
                            stored.get(i) == null ? ChangeFeed.Kind.ADD : ChangeFeed.Kind.MODIFY,
                            op.namespace, op.item.toByteArray(), revision));
                }
                response.addResults(Query.RecordWriteResult.newBuilder()
                        .setRevision(revision)
                        .setDurability(level));
            }
            if (logged) {
                meta.set(APPLIED_SEQ, seq);
            }
            meta.set(LAST_REVISION, firstRevision + ops.size() - 1);
            changes.publish(new ChangeFeed.Committed(out));
            return response.build();
        } finally {
            endTurn(ticket);
        }
    }

    /** Refuses op {@code i} unless its condition holds against {@code row}. */
    private static void check(WriteOp op, Message row, int i, int n) throws ProtoException {
        String at = n == 1 ? "" : "op " + i + ": ";
        // An entry held to another namespace is not this caller's to see, so it
        // answers as absent wherever "absent" is an answer.
        boolean foreign = row != null && !op.namespace.isEmpty()
                && !op.namespace.equals(op.type.namespaceOf(row));
        switch (op.kind) {
            case PUT:
                if (foreign) {
                    throw new ProtoException(ProtoException.PERMISSION_DENIED,
                            at + op.entry + " is held to another namespace");
                }
                return;
            case INSERT_IF_ABSENT:
                if (row != null) {
                    throw new ProtoException(ProtoException.ALREADY_EXISTS, at + op.entry + " already exists");
                }
                return;
            case REPLACE_IF_UNCHANGED:
                if (row == null || foreign) {
                    throw new ProtoException(ProtoException.NOT_FOUND, at + op.entry + " not found");
                }
                return;
            case DELETE:
                if (row == null || foreign) {
                    throw new ProtoException(ProtoException.NOT_FOUND, at + op.entry + " not found");
                }
                return;
            default:
                throw new ProtoException(ProtoException.INVALID_ARGUMENT, at + "unknown write " + op.kind);
        }
    }

    /**
     * ReplaceIfUnchanged's comparison, made against the stored revision as well
     * as the row; called with the entry locked.
     */
    private void checkUnchanged(WriteOp op, Message row, int i, int n) throws ProtoException {
        if (op.kind != WriteOp.Kind.REPLACE_IF_UNCHANGED) {
            return;
        }
        String at = n == 1 ? "" : "op " + i + ": ";
        if (op.expectedRevision != null) {
            Long current = revisions.get(revisionKey(op.entry));
            if (current == null || current.longValue() != op.expectedRevision) {
                throw new ProtoException(ProtoException.ABORTED, at + op.entry + " changed: revision "
                        + current + ", expected " + op.expectedRevision);
            }
        } else if (op.expectedItem != null && !op.expectedItem.equals(row)) {
            throw new ProtoException(ProtoException.ABORTED, at + op.entry + " changed: the stored row"
                    + " is not the expected one");
        }
    }

    private static ProtoException refusal(Exception e, long seq, Durability owed) {
        Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        if (e instanceof TimeoutException) {
            return new ProtoException(ProtoException.UNAVAILABLE,
                    "the redo service did not ack the write at " + owed + " in time; it was not applied", e);
        }
        Status s = cause instanceof StatusRuntimeException ? ((StatusRuntimeException) cause).getStatus() : null;
        String why = s != null ? s.getCode() + ": " + s.getDescription() : String.valueOf(cause.getMessage());
        int code = ProtoException.UNAVAILABLE;
        if (s != null && s.getCode() == Status.Code.FAILED_PRECONDITION && owed == Durability.DURABILITY_REMOTE) {
            // Today's redo has no second site and refuses REMOTE outright.
            code = ProtoException.FAILED_PRECONDITION;
        }
        return new ProtoException(code, "the redo service did not ack the write at " + owed
                + " (seq " + seq + "): " + why + "; it was not applied", cause);
    }

    private void awaitTurn(long ticket) {
        synchronized (order) {
            while (turn != ticket) {
                try {
                    order.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Status.CANCELLED.withDescription("interrupted awaiting commit order").asRuntimeException();
                }
            }
        }
    }

    private void endTurn(long ticket) {
        synchronized (order) {
            turn = ticket + 1;
            order.notifyAll();
        }
    }

    // -----------------------------------------------------------------------
    // Point reads
    // -----------------------------------------------------------------------

    /** A stored row and its revision. */
    public static final class Stored {
        public final Message row;
        public final long revision;

        Stored(Message row, long revision) {
            this.row = row;
            this.revision = revision;
        }
    }

    /**
     * The entry's row and revision, read with the entry locked on the
     * sequencer, so the pair is one committed state: a Get after a write's
     * answer sees it, and a Get during a write waits for its ack.
     *
     * @param namespace the namespace the read is held to; "" for a fleet-wide caller
     * @throws ProtoException NOT_FOUND when absent or held to another namespace
     */
    public Stored get(String type, RedoKey key, String namespace) throws ProtoException {
        if (!isSequencer()) {
            byte[] reply = forward(SequencerCall.get(type, key, namespace));
            try {
                RedoChange c = RedoChange.parseFrom(reply);
                RecordType t = types.byName(type);
                return new Stored(t.parse(c.getValue()), c.getRevision());
            } catch (InvalidProtocolBufferException e) {
                throw new ProtoException(ProtoException.INTERNAL, "sequencer reply: " + e.getMessage(), e);
            }
        }
        return getHere(type, key, namespace);
    }

    Stored getHere(String type, RedoKey key, String namespace) throws ProtoException {
        requireReadable();
        RecordType t = types.byName(type);
        if (t == null) {
            throw new ProtoException(ProtoException.INVALID_ARGUMENT, "unknown record type " + type);
        }
        EntryId id = new EntryId(type, key);
        try (EntryLocks.Hold held = locks.lockEntries(List.of(id))) {
            Message row = map(t).get(id.mapKey());
            if (row == null || (namespace != null && !namespace.isEmpty() && !namespace.equals(t.namespaceOf(row)))) {
                throw new ProtoException(ProtoException.NOT_FOUND, id + " not found");
            }
            Long revision = revisions.get(revisionKey(id));
            return new Stored(row, revision == null ? 0L : revision);
        }
    }

    // -----------------------------------------------------------------------
    // Forwarding to the sequencer
    // -----------------------------------------------------------------------

    private byte[] forward(SequencerCall call) throws ProtoException {
        Member sequencer = hz.getCluster().getMembers().iterator().next();
        SequencerCall.Reply reply;
        try {
            Future<SequencerCall.Reply> f = hz.getExecutorService(SEQUENCER_EXECUTOR).submitToMember(call, sequencer);
            reply = f.get(ackTimeoutMs * 2, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProtoException(ProtoException.CANCELLED, "interrupted forwarding to the sequencer", e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            throw new ProtoException(ProtoException.UNAVAILABLE,
                    "the store's sequencer " + sequencer.getAddress() + " did not answer: " + e.getMessage(), e);
        }
        // Outside the try: the sequencer's own refusal (ALREADY_EXISTS, ...) is the
        // answer, not a failure to reach it.
        return reply.unwrap();
    }

    /** A write or point read sent to the sequencer member. */
    static final class SequencerCall implements Callable<SequencerCall.Reply>, Serializable, HazelcastInstanceAware {
        private static final long serialVersionUID = 1L;

        /** The sequencer's answer: the reply's bytes, or the refusal it threw. */
        static final class Reply implements Serializable {
            private static final long serialVersionUID = 1L;
            final byte[] bytes;
            final int code;
            final String message;

            Reply(byte[] bytes, int code, String message) {
                this.bytes = bytes;
                this.code = code;
                this.message = message;
            }

            byte[] unwrap() throws ProtoException {
                if (code != 0) {
                    throw new ProtoException(code, message);
                }
                return bytes;
            }
        }

        private final boolean isApply;
        private final byte[] payload;
        private final String namespace;
        private transient HazelcastInstance hz;

        private SequencerCall(boolean isApply, byte[] payload, String namespace) {
            this.isApply = isApply;
            this.payload = payload;
            this.namespace = namespace;
        }

        static SequencerCall apply(Query.RecordApplyRequest request) {
            return new SequencerCall(true, request.toByteArray(), "");
        }

        static SequencerCall get(String type, RedoKey key, String namespace) {
            return new SequencerCall(false,
                    RedoChange.newBuilder().setRecordType(type).setKey(key).build().toByteArray(), namespace);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }

        @Override
        public Reply call() {
            RecordStore store = (RecordStore) hz.getUserContext().get(USER_CONTEXT_KEY);
            try {
                if (store == null || !store.isSequencer()) {
                    throw new ProtoException(ProtoException.UNAVAILABLE,
                            "this member is no longer the store's sequencer; retry");
                }
                if (isApply) {
                    return new Reply(store.applyHere(Query.RecordApplyRequest.parseFrom(payload)).toByteArray(), 0, null);
                }
                RedoChange c = RedoChange.parseFrom(payload);
                Stored s = store.getHere(c.getRecordType(), c.getKey(), namespace);
                return new Reply(RedoChange.newBuilder().setValue(s.row.toByteString())
                        .setRevision(s.revision).build().toByteArray(), 0, null);
            } catch (ProtoException e) {
                return new Reply(null, e.getCode(), e.getMessage());
            } catch (InvalidProtocolBufferException e) {
                return new Reply(null, ProtoException.INVALID_ARGUMENT, e.getMessage());
            }
        }
    }

    /** Delivers what the sequencer publishes to this member's feed. */
    private static final class ChangeListener implements com.hazelcast.topic.ReliableMessageListener<ChangeFeed.Committed> {
        private final ChangeFeed feed;
        private long lastSequence = -1;

        ChangeListener(ChangeFeed feed) {
            this.feed = feed;
        }

        @Override
        public void onMessage(com.hazelcast.topic.Message<ChangeFeed.Committed> m) {
            ChangeFeed.Committed op = m.getMessageObject();
            if (op.reset) {
                feed.endAll("the record store was rebuilt from the redo service; watch again");
            } else {
                feed.deliver(op);
            }
        }

        @Override
        public long retrieveInitialSequence() {
            return -1;
        }

        @Override
        public void storeSequence(long sequence) {
            if (lastSequence >= 0 && sequence != lastSequence + 1) {
                // A gap: this member missed committed operations, so no watch on it
                // may go on as if its stream were whole.
                feed.endAll("this member missed committed changes; watch again");
            }
            lastSequence = sequence;
        }

        @Override
        public boolean isLossTolerant() {
            return true;
        }

        @Override
        public boolean isTerminal(Throwable failure) {
            return false;
        }
    }

    public void shutdown() {
        try {
            changes.removeMessageListener(changesListener);
            hz.getUserContext().remove(USER_CONTEXT_KEY, this);
        } catch (RuntimeException ignored) {
            // The member is already down, and took both with it.
        }
        feed.endAll("the record store is shutting down");
        redo.close();
    }

    /** Test seam: the sequencer's next seq. */
    long nextSeq() {
        synchronized (order) {
            return nextSeq;
        }
    }

    /**
     * Whether this member, as the sequencer, holds its place in the log; false
     * on a member that is not the sequencer.
     */
    public boolean inSync() {
        return inSync;
    }
}
