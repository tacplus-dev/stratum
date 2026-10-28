# Stratum durability contract

Decision: tacplus-dev/roadmap `decisions/stratum-per-tenant-2026-10-25.md`. Issue: stratum#2.

## What is durable, and where

**The redo service is durability. Memory is availability.**

- A tenant's Stratum holds its rows in memory only: one Hazelcast `IMap` per record type, each
  entry's revision in `stratum.revisions`, and the store's place in the log in `stratum.meta`
  (`server/src/main/java/com/github/tacplusdev/stratum/store/RecordStore.java`). Nothing is written to
  disk. There are no volumes, no member roles, no checkpoint, no disk index and no idle eviction.
- Every write that is owed LOCAL or REMOTE is appended to the redo service and acked there **before**
  it is applied in memory. The log is the database: every start rebuilds memory from it.
- The redo service runs in the reserved `restore` tenant (`00000002`). Stratum reaches it through the
  generated client for `redolog.Redo`, as a tenant-to-tenant call whose route field names `00000002`
  (`store/GrpcRedoLog.java`, tacplus-dev/tacplus#170). The tenant a write belongs to is never a request
  field: the redo reads it off Stratum's identity, and keeps one log per tenant. The contract is
  `sys/redo.proto` (generated into `model/target/generated-sources/model/proto/sys/redo.proto`).
- The redo log is one sequence per tenant. An append is refused unless its seq is exactly one more than
  the last seq the log holds. Replay is therefore exact.

## Durability levels

Declared per record type in the proto, as `(spec.record).durability`. Unset is LOCAL.

| Level | Acked when | Survives |
|---|---|---|
| `DURABILITY_MEMORY` | applied in memory on the members; **not appended** | a member loss (with backups); not a full restart |
| `DURABILITY_LOCAL` (default) | the local redo has it on disk (a regional disk, replicated across two zones) | a full restart, a zone loss |
| `DURABILITY_REMOTE` | the local redo and the second site have it | a region loss |

- A write may ask for a higher level than its type declares, never a lower one. Asking lower is not an
  error; the type's level wins.
- An `Apply` over several entries is owed the highest level of its types and of what it asked for. It is
  one append, kept by the log all or nothing.
- The log is a prefix: waiting for REMOTE on one write makes every earlier write remote too.
- The answer carries the level the write was acked at (`RecordWriteResult.durability`).
- **REMOTE today:** the redo's second-site step (redo#1 step 2, waiting on tacplus-dev/tacplus#204) has
  not landed. Until it does the redo refuses a REMOTE append with FAILED_PRECONDITION, and Stratum passes
  that code through to the caller. Nothing is applied.

## The write path

All writes (`Put`, `InsertIfAbsent`, `ReplaceIfUnchanged`, `Delete`, and `Apply` of several) run on one
member, the **sequencer**: the oldest member of the Hazelcast cluster. Another member forwards the write
to it (`RecordStore.SequencerCall`, executor `stratum.sequencer`). The log has one writer, so the entry
locks are plain JVM locks in the sequencer (`store/EntryLocks.java`).

1. **Locks.** Take the lock of every entry the operation touches, including entries that do not exist
   yet, in the canonical order (record type full name, then key; `store/EntryId.java`). Every operation
   uses that order, so two operations over the same entries queue rather than deadlock. Each entry
   may appear once per operation.
2. **Conditions.** Check every op against the stored rows. One failure refuses the whole operation and
   nothing is written.
3. **Append.** Assign the revisions and the next seq, and append the operation to the redo at the level
   it is owed. A MEMORY operation is not appended and takes no seq.
4. **Ack.** Wait for the redo's ack at that level, up to `write_ack_timeout_ms` (default 10000).
5. **Apply.** Write the maps. Operations are applied in the order they took their seq (a ticket per
   operation), so revisions are applied in increasing order.
6. **Publish.** Publish the applied changes to the change feed.
7. **Answer.** Return the revision and the acked level per op.

Nothing can read or watch a write before its ack, because nothing is in the maps or the feed before it.

### The answers

| Answer | When |
|---|---|
| `ALREADY_EXISTS` | `InsertIfAbsent` on a present entry |
| `ABORTED` | `ReplaceIfUnchanged` on an entry whose revision (or stored row) is not the expected one |
| `NOT_FOUND` | `ReplaceIfUnchanged` or `Delete` on an absent entry, or on one held to another namespace |
| `PERMISSION_DENIED` | `Put` over an entry held to another namespace |
| `INVALID_ARGUMENT` | an empty operation, or one naming an entry twice |
| `FAILED_PRECONDITION` | a REMOTE write the redo refuses (see above) |
| `UNAVAILABLE` | the redo did not ack in time, refused, or is unreachable; the store has not replayed yet; the sequencer did not answer a forwarded call |

**UNAVAILABLE after an append means the outcome is unknown.** The append may be in the log. It is not
applied now, but if it is in the log it is applied at the next replay. So a retry can see its own earlier
attempt: a retried `InsertIfAbsent` can answer `ALREADY_EXISTS`, and a retried `ReplaceIfUnchanged`
can answer `ABORTED`. A caller that retries should read the entry back when that matters.

### While the redo is unreachable

Writes owed LOCAL or REMOTE are refused (UNAVAILABLE). After a failed ack the sequencer drops its append
stream and marks itself out of sync; it does not write again until it has replayed. The keeper thread in
`StratumHandler` retries the replay with backoff (1 s, doubling to 30 s). Reads of what is already in
memory keep working. MEMORY writes are refused too while the sequencer is out of sync.

## Start: replay

Every start replays the tenant from the redo (`RecordStore.catchUp`): first the compacted snapshot (one
PUT per live entry), then the tail behind it in seq order. A full restart, a start on another cluster
and a restore after a region loss are the same operation. Restore is to the latest state.

Until the first replay finishes, reads and writes answer UNAVAILABLE (`RecordStore.readable()`, checked
by every generated read, write and watch). The flag lives in `stratum.meta`, so it is raised once per
cluster lifetime and not lowered by a later takeover.

Each entry's revision is restored from the log, and the next revision continues past the highest seen.

### Sequencer takeover

When the sequencer leaves, the next oldest member becomes the sequencer. It does not know the log's tail,
so it replays before it writes, and it applies **only seq > applied** (`applied_seq` in `stratum.meta`,
which survives in memory with the backups). A write the old sequencer appended but never applied is
therefore applied now, not lost. Writes in the meantime answer UNAVAILABLE; the caller retries.

If the redo's snapshot already folds in seqs past `applied` (the store fell behind compaction), the
snapshot replaces the maps: any entry it does not carry is removed. Every open watch is then ended, on
every member, so watchers start again from a snapshot.

A cluster merge can make another member the oldest. The member that stops being the sequencer marks
itself out of sync, and replays if it ever becomes the sequencer again.

### Two sequencers

A split brain gives each half its own oldest member, and so two sequencers. The redo's strict seq is the
fence: an append succeeds only if it follows the log's last seq exactly. The half that appends second is
refused, replays (taking in the other half's writes), and checks conditions again against the whole log.
So the log stays one sequence and no condition is decided against a state the log has moved past. Reads
in either half can be stale while the split lasts.

## Members, zones, footprint

- Every member is the same: it holds partitions (primaries and backups) of every record map and answers
  queries over them (`hazelcast/StratumMapConfig.java`). A query (`List`, `Count`) is a scatter-gather
  across the members. A point `Get` is read on the sequencer with the entry locked, so it sees a write
  as soon as the write is answered.
- `STRATUM_BACKUP_COUNT` (default 1) synchronous backups per map. Backups are memory, for availability.
- `STRATUM_ZONE` (set by the operator from the node's `topology.kubernetes.io/zone`) makes backups
  zone-aware: a partition's backup is never in its primary's zone, so a zone loss costs no row while
  another zone holds a member. Unset, backups are not zone-aware.
- No eviction. Each tenant's whole dataset stays resident, and the memory request is sized for it
  (`hazelcast_memory_mb`).
- **A single member is allowed.** It trades availability, not durability: a restart is downtime until
  the replay finishes, and nothing acked at LOCAL or REMOTE is lost. MEMORY writes are lost on any full
  restart, whatever the member count.
- Members are found through `STRATUM_SEED_MEMBERS`, rendered by the operator.

## Watch

`Watch<T>Changes` exists on every record type (`ChangeFeed.java`, and the watch method emitted by
`RecordMapImplGenerator.writeSurface`).

- **Only applied writes.** The feed is fed with what the sequencer applied, which is only what the redo
  acked at the level owed. A refused write is never delivered.
- **Revision order.** The sequencer publishes each applied operation once, in commit order, on a
  Hazelcast reliable topic (`stratum.changes`); every member delivers it to its own watchers.
- **Snapshot, then changes.** A watch from 0 sends the rows in memory, then the changes after the feed
  revision read before the scan. A change made during the scan can appear both in the snapshot and as a
  change; applying it again is a no-op.
- **Resume.** Each member keeps the last 4096 changes per record type (`ChangeFeed.HISTORY`). A watch
  resuming from a revision inside that history gets every change after it. One older than that is
  answered `OUT_OF_RANGE`, and the caller re-watches from 0.
- **Watches end** (UNAVAILABLE, "watch again") when a member misses changes on the topic (a gap in the
  topic's sequence; the ring buffer holds 65536 operations), when the store is rebuilt from a snapshot
  (above), and when the store shuts down. The member's history is cleared at the same time.
- A watch is held to the caller's namespace; a fleet-wide (`/system`) caller sees every namespace.

## Tenant deletion

Decision: `decisions/tenant-deletion-2026-10-27.md`. Deleting a tenant deletes all its data. Its
Stratum, and so its in-memory rows, go with its namespace (`tac-<deployment>-<tid>`), and the redo
service deletes its log at both sites (redo#1). Stratum itself does nothing on deletion: it holds nothing
that outlives its pods.

## Known gaps

- **Soft-deleted rows are not purged.** A row with `(spec.deleted_flag)` set stays in memory, and in the
  redo's snapshot, until someone deletes it. The daily checkpoint used to purge such rows after
  `deleted_max_age`; it was removed with the rest of the disk path and nothing replaces it yet. Queries
  still exclude flagged rows unless `include_deleted` is set.
- **Revisions are store-assigned**, one int64 counter across all record types, not the redo seq. A MEMORY
  write takes a revision but no seq, and one operation takes one seq but a revision per op. The revision
  is kept in the log (`RedoChange.revision`) so a replay restores it exactly.
- **MEMORY writes are per cluster lifetime.** They are not in the log, so a full restart loses them by
  definition.
- **Point-in-time restore** is done by editing the redo log by hand.
- **REMOTE** is refused until redo#1 step 2 lands (above).

## Verification

- `server/src/test/java/com/github/tacplusdev/stratum/store/RecordStoreTest.java`: the write answers,
  Apply all or nothing, lock order (canonical, UTF-8 byte order) and no deadlock between opposed
  multi-entry writes, apply-after-ack, a write whose ack never comes (refused, applied only by the next
  replay), refusal while the redo is unreachable, REMOTE refused, reads refused until the replay, replay
  of snapshot and tail at start, a full restart, forwarding to the sequencer, a takeover that applies its
  predecessor's tail, and Watch delivering only acked writes on the sequencer and on another member.
- `.../store/EntryLocksTest.java`: lock order and map locks excluding their entries.
- `.../RecordWriteSurfaceE2ETest.java`: the same answers over gRPC through the generated wrapper, Get's
  revision, Apply, and `Watch<T>Changes` (snapshot, then only acked writes).

The redo service is an in-memory log with the redo's rules (`store/InMemoryRedoLog.java`); no test runs
against a deployed redo.
