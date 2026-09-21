# Stratum durability contract

## What stratum is

Stratum is a **record store with three durability legs**, not the single in-memory
map this document used to describe. A record stratum holds is recoverable from:

1. **In-memory replication.** Every record map is a Hazelcast `IMap<key, Message>`
   with a declared `min_durable_copies` floor and split-brain protection. A write
   is acked only once that many distinct instances hold it; a below-quorum write
   throws rather than storing locally (`HotWrites`).
2. **This pod's own volume.** A checkpoint plus a redo log, per pod, under
   `<dataRoot>` — the subject of the rest of this document. Record types tiered to
   disk additionally have a node-local Lucene index (`WarmStore`).
3. **The event log.** Records are event-sourced: they are written only by republish
   on the record's bus, and the record plane is read-only. A pod with no volume and
   no peers rebuilds from offset zero (`ColdStartRebuilder`).

The write path is unchanged by any of this: nothing below ever writes a record, and
a read never restamps `store_epoch`, never appends to the event log and never
touches disk.

## The daily checkpoint (per pod, one transaction)

Each pod runs one transaction on the existing `cleanup_interval_seconds` cadence
(`DailyCheckpoint`). It is **not** leader-gated — it writes this pod's own volume,
and one pod doing it for the cluster would leave every other pod with no baseline.

1. **Purge.** Every row whose `(spec.deleted_flag)` field is true and whose delete
   `(spec.store_epoch)` is older than the deleted-max-age is appended to the pod's
   tombstone log and then removed from memory and from this node's Lucene index.
   The tombstone line is written **before** the removal: a crash between the two
   leaves a line for a row that is still present, which the next sweep writes again
   and which is harmless, while the other order can lose the row with no trace.
2. **Dump.** Every row that survived step 1 is written to
   `<dataRoot>/checkpoint/<ts>`, staged as a fsync'd sibling temp and committed by
   one atomic rename (`FsUtil.writeTemp` + `FsUtil.promote`). `current`, a pointer
   file naming that checkpoint, is then rewritten by the same atomic-rename
   primitive.
3. **Truncate.** Only once that rename has succeeded, every redo log is rolled to
   an empty baseline (`HotRedoLogs.truncateToCheckpoint`). The checkpoint file IS
   the baseline now; what has to survive the roll is exactly the tail, and that is
   what `RedoLog.roll` folds back in.

**Failure at any step leaves the previous checkpoint plus an un-truncated redo log,
which is a complete recovery set.** The transaction does not roll back and has
nothing to roll back: it aborts, and the outcome of an aborted run is "try again on
the next tick", never data loss. That holds only while the truncate is strictly
last. The sweep is idempotent for the same reason — a row purged in a partial run
and seen again on retry is simply purged again.

### Layout

```
<dataRoot>/checkpoint/<ts>              one dump, NDJSON, one line per row
<dataRoot>/checkpoint/current           a text file holding one <ts>
<dataRoot>/hot/…/<map>.log              this pod's redo log, the tail since <ts>
<dataRoot>/tombstones/<yyyy-MM-dd>.jsonl   every row this pod has purged, UTC days
<dataRoot>/warm/<dataset>/<slice>/      the Lucene leg, for disk-tiered types
```

`current` is a **pointer file, not a symlink**. Both are updated by the same atomic
rename, so they are equally safe; a regular file is readable by anything that can
read the directory, including an offline collector working off a volume snapshot
where a symlink's target may not have come along.

The tombstone log is **write-only**. Nothing in the platform reads it. It is a
historical record that can be shipped offline; each line carries the record type,
the key, the tenant, the principal, the delete's `store_epoch`, the purge time and
the full payload as protobuf JSON, so it is readable without the `.proto`.

## Cold start

Cold start is **the checkpoint plus the redo tail**, not a replay from offset zero:

- `EventStore.init` loads `checkpoint/current` into the maps before the redo
  replay, and only on a cold boot — the gate is `ColdStartRebuilder.shouldReplay`,
  i.e. the store is empty cluster-wide. A member restarting alongside live peers
  gets its partitions back from them, and loading a stale local dump over that
  would be one pod overwriting the cluster's newer rows with its own older ones.
- `HotRestore.replay` then applies the redo tail on top, resolved per key by
  highest `store_epoch`, so a row rewritten after the dump wins over the dump's copy.
- The full event-log replay is not removed; it is simply no longer reached by a pod
  that has a checkpoint, because the store is no longer empty by the time
  `StratumHandler` asks. A pod with no checkpoint — a first boot, a replaced volume
  — still replays, which is what should happen.

Recovery therefore costs the size of the data plus one cycle of writes, not the
size of the history. **A purged row can never come back**: it is absent from the
dump, and the redo tail that still mentioned it was discarded by the same
transaction's truncate.

## What is NOT cut over yet

Stated because the code has two mechanisms where the 2026-09-20 record lifecycle
decision has one:

- **The old retention sweep still runs**, on the same scheduler tick, leader-gated
  (`EventStore.runCleanupCycle`). No fleet record type carries
  `(spec.deleted_flag)` yet — the annotation is on tacplus's unmerged branch — so
  the purge step selects nothing, and removing retention today would simply let
  every store grow without bound. Retention goes when the fleet annotation pass has
  run.
- **`deleted_max_age_seconds` is a flat platform default**, not the per-type
  annotation the decision specifies: 30 days, overridable with
  `STRATUM_DELETED_MAX_AGE_SECONDS`. `Config` is a tacplus-owned proto in another
  repository, so this knob has the same shape `STRATUM_MIN_DURABLE_COPIES` already
  has. The per-type read replaces it at one call site once the annotation lands.
- **`WarmResidency` / `WarmEviction`'s memory-residency window is untouched.** It
  is replaced by Hazelcast per-entry max-idle driven by
  `(spec.record).residency.idle_seconds`, which is the same blocked annotation.
  Removing the window before the idle clock exists would leave the two legs of a
  disk-tiered read with no partition between them.

## What stratum does NOT guarantee

- **It is not the system of record.** A record's presence in stratum is not proof
  it happened, and its absence is not proof it did not. The event log is the truth.
- **It does not survive a lost event log AND lost volumes.** With every checkpoint
  and every redo log gone, the recoverable history is whatever the broker still
  holds — and stratum's effective history is then
  `min(what was ever published, broker retention)`.
- **A redo append is queued, not awaited.** The ack is the in-memory replication.
  A process killed inside that window loses that append; the row survives on the
  peers that queued the same append, and a simultaneous loss of every replica
  inside that window is the named, accepted cost of acking on replication rather
  than on the disk write.
- **N pods write N identical dumps.** That is the accepted cost until the
  "collect from one pod and compress" process exists (Jody, 2026-09-20).

## Verification

- `DailyCheckpointTest` — the tombstone log's contents, the dump's
  one-rename commit, a failed dump leaving the previous checkpoint still named by
  `current`, truncate running only after that commit, and the sweep's idempotence.
- `CheckpointColdStartTest` — a cold start that loads `checkpoint/current` and
  replays only the tail, and a purged row that nothing left on the volume can bring
  back.
- `RecordAnnotationsTest` — the markers resolved off the descriptor, including a
  real generated type's `(spec.store_epoch)`.
- `HotRedoRestoreTest`, `HotRestoreLedgerTest` — the redo leg and the cross-node
  restore barrier. `ColdStartRebuilderTest` — the event-log replay decision.
