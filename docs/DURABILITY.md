# Stratum durability contract

## What stratum is

Stratum is a **record store with three durability legs**, not the single in-memory
map this document used to describe. A record stratum holds is recoverable from:

1. **In-memory replication.** Every record map is a Hazelcast `IMap<key, Message>`
   with `backupCount` synchronous replicas and split-brain protection. A write is
   acked once the primary and those replicas hold it.
2. **The writers' volumes.** A checkpoint, a redo log and a per-type record index,
   per **writer** member, under `<dataRoot>` — the subject of the rest of this
   document. A write is refused unless `min_durable_copies` writers are in the
   cluster to receive it (`HotWrites`).
3. **The event log.** Records are event-sourced: they are written only by republish
   on the record's bus, and the record plane is read-only. A pod with no volume and
   no peers rebuilds from offset zero (`ColdStartRebuilder`).

The write path is unchanged by any of this: nothing below ever writes a record, and
a read never restamps `store_epoch`, never appends to the event log and never
touches disk.

## Members, partitions and queries

Every record map is a **partitioned** Hazelcast `IMap`: each entry has one primary
and `backupCount` backups on other members, and primaries are spread nearly evenly.
No member holds the whole dataset in memory, and **a predicate query — `List`,
`Count`, a watch's fan-in — is a scatter-gather across ALL members in parallel whose
results are merged**. Adding a member rebalances partitions onto it and adds that
much query parallelism.

## Member roles: writer and search

Every member holds partitions and answers that scatter-gather. The **role** says what
a member does besides, and the whole of the difference is disk. It travels as the
Hazelcast member attribute `stratum.role` (`hazelcast/StratumRole`), declared per pod
by `STRATUM_MEMBER_ROLE`; a member with no attribute reads as a **writer**, the
direction that cannot invent durability.

| | **writer** | **search** |
|---|---|---|
| partitions, queries | yes | yes |
| volume | yes | **no** |
| redo log, record index, checkpoint | yes | **no** |
| counted for `min_durable_copies` | yes | **never** |
| restore-barrier contributor | yes | **never** |
| idle-clock eviction | yes | no — nothing to reload from |
| count | static, sized for durability (2 or 3) | dynamic, sized for query throughput |
| operator field | `stratum_nodes` | `stratum_search_nodes` (default 0) |

**A writer holds the full dataset on disk.** Every write is sent to every writer —
`RecordPersistTask`, submitted to `StratumRole.writers` — regardless of which member
owns the partition, so a writer's redo log and record index cover the whole store and
a writer can restore it alone. That is what makes durability independent of where
Hazelcast happened to put a partition, and it is what lets a volume-less member own
one safely.

**`backupCount` is memory redundancy and nothing else** (`STRATUM_BACKUP_COUNT`,
default 1). Until the roles landed it was `min_durable_copies - 1`, because the
durable copies WERE the partition's replicas: the append rode the replication onto the
owner and its synchronous backups. Tying them together is also what made search
members impossible — a partition whose primary and backup both landed on volume-less
members would have been acked with no durable copy at all.

**Search members are a query-throughput knob, not a durability one.** The write gate
counts writers (`HotWrites.durableCopies`), the split-brain minimum counts writers
(`hazelcast/WriterCountProtection`), the cluster-formation wait counts writers
(`StratumMapConfig.withWriterQuorum`) and the hot-restore barrier counts
writers (`HotRestore.awaitClusterReplay`). So one writer plus five search members with
`min_durable_copies = 2` **refuses writes**, and scaling search members from 0 to 4
and back changes nothing durable — it rebalances partitions and nothing else.

A search member does still wait at the cluster-formation and hot-restore barriers: it
is handed partitions the moment it joins, and answering a query off a half-recovered
store is the silent incompleteness those barriers exist to refuse. It files no report
of its own on the way through.

A search member's record maps carry **neither the idle clock nor the read-through
loader**. The two are wired together so a map cannot get the clock without the leg
that answers for what the clock evicts, and on a member with no volume the pair is
neither — the clock alone would make eviction a deletion. A search member therefore
holds its partitions resident for as long as it is a member, which is what its memory
request is sized for.

## The daily checkpoint (per writer, one transaction)

Each **writer** runs one transaction on the existing `cleanup_interval_seconds`
cadence (`DailyCheckpoint`). It is **not** leader-gated — it writes this pod's own
volume, and one pod doing it for the cluster would leave every other writer with no
baseline. A search member runs no cycle at all: nothing to dump, nothing to truncate,
no tombstone log to purge into.

1. **Purge.** Every row whose `(spec.deleted_flag)` field is true and whose delete
   `(spec.store_epoch)` is older than the deleted-max-age is appended to the pod's
   tombstone log and then removed from memory **and from this pod's record
   index**. Removing it from memory alone would leave the index's copy for step 2
   to dump straight back into the next checkpoint — tombstoned and resurrected,
   every day, forever.
   The tombstone line is written **before** the removal: a crash between the two
   leaves a line for a row that is still present, which the next sweep writes again
   and which is harmless, while the other order can lose the row with no trace.
2. **Dump.** Every row that survived step 1 is written to
   `<dataRoot>/checkpoint/<ts>`. The rows are the **union of this pod's record
   indexes and its resident memory** (`DurableRows`), never memory alone: a row
   evicted on its idle clock is off-heap, and a dump taken from memory would omit
   it and then have step 3 truncate the redo entries that still named it — the
   failure that hotfixed the idle clock off on 2026-09-21. Disk alone is not the
   answer either, because the index write is queued off the ack path, so a row
   written moments before the sweep is in memory and not yet indexed. The dump is
   staged as a fsync'd sibling temp and committed by
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
<dataRoot>/records/<pkg>.<Type>/        this pod's Lucene index for one record type
<dataRoot>/tombstones/<yyyy-MM-dd>.jsonl   every row this pod has purged, UTC days
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
- What each pod reports to the restore ledger is **its checkpoint's row count plus
  the redo records it read**, not the redo records alone. The durability floor in
  `HotRestore.awaitClusterReplay` counts a pod as a contributor when that number is
  non-zero, so a pod sitting just after its own truncate still counts: its rows
  moved from the tail into the dump, and reading it as an empty volume refused
  restarts that had lost nothing
  (`decisions/2026-09-21-stratum0-checkpoint-quorum-crashloop.md`).

Recovery therefore costs the size of the data plus one cycle of writes, not the
size of the history. **A purged row can never come back**: it is absent from the
dump, and the redo tail that still mentioned it was discarded by the same
transaction's truncate.

## Memory residency

One clock per record type, and Hazelcast runs it: per-entry **max-idle** set from
`(spec.record).residency.idle_seconds` (`StratumMapConfig`, from the generated
`StratumEventStores.idleSeconds`). A row stays resident while it is read or
written and leaves memory once it has gone `idle_seconds` with neither. A read
resets the clock without restamping `(spec.store_epoch)`, appending to the redo
log or touching disk.

This replaced, on 2026-09-20:

- **The HOT/WARM/COLD residency classes** and the per-query residency boundary
  (`WarmResidency`) that partitioned a warm type's memory and disk legs. There is
  one leg now, so there is nothing to partition and nothing to deduplicate.
- **`WarmEviction`**, the periodic sweep that dropped rows whose `store_epoch` had
  aged past that boundary. It measured a WRITE clock on a sweep interval; max-idle
  measures reads and writes, per entry, continuously.
- **The `retention_days` sweep** (`EventStore.runRetentionCleanup`, the generated
  `StratumEventStores.runRetention`). It measured each record's declared
  `(spec.record).retention.timestamp_field`, an annotation retired with the same
  decision — no proto declares one, so the sweep had become a no-op before it was
  removed.
- **The node-local Lucene tier** (`WarmStore`/`WarmIndex`), which was keyed on the
  retired `residency: {window: RESIDENCY_WARM}` preset and generated an empty
  dataset list once no proto declared it.

### The disk leg under the clock

Eviction is a **cache miss, not a deletion**, because every record map has a
read-through leg beneath it:

- **One Lucene index per record type**, on this pod's volume, under
  `<dataRoot>/records/<pkg>.<Type>/` (`disk/RecordIndex`). Keyed by the record key,
  holding the record's protobuf full name (e.g. `accountingrecord.AccountingRecord`)
  and its protobuf bytes — the same line shape the checkpoint uses, and for the same
  reason: the full name resolves through the generated registry
  (`StratumEventStores.defaultInstance`), so a row comes back as the type the maps and
  the query plane expect. No Java class name is stored, on disk or on the Hazelcast
  wire (`hazelcast/ProtoMessageSerializer`); volumes written in the older class-name
  format are rewritten once at boot by `disk/ClassNameMigration`.
- **Every write lands on every writer's index as well as in memory.** The index write
  rides `RecordPersistTask`, the same task that queues the redo append, so it runs on
  every writer member and each writer's index holds the whole store. It is queued to
  one node-local thread and is **not** on the ack path, exactly like the redo append.
  Because each writer is reached over its own connection, two writes to one key can
  arrive in either order, so `RecordDisk.writeAsync` refuses a row whose
  `store_epoch` is older than the one the index already holds. The redo log needs no
  such guard — `HotRestore` resolves its records by the same epoch on the way in.
- **A memory miss loads from it.** Each record map on a WRITER carries a Hazelcast
  `MapLoader` (`disk/RecordDiskLoader`), wired in `StratumMapConfig.durableMap` in the
  same branch that applies the max-idle clock, so a map cannot get the clock without
  the leg — and a search member, having no leg, gets neither. `loadAllKeys()` answers null: this is read-through only, and preloading the
  index would both start from the wrong file and undo eviction every time a map is
  opened.
- **A dead row is not loadable.** A flagged-deleted row past its deleted-max-age
  answers null even while the index still holds it, because the decision is explicit
  that such a row is never loaded into Hazelcast again; it is on disk only because
  the next sweep has not reached it.

This is **not** the warm tier returning. `WarmStore`/`WarmIndex` were the WARM/HOT
model's disk half — monthly slices, per-slice searchers, epoch intervals so a query
could prune legs, and an indexed Lucene field per `(spec.query_field)` so disk could
evaluate predicates. None of that is here: there are no tiers to partition against
and no disk-side query plane. This is a key/value store that happens to be Lucene.

The redo log is **still** the crash-recovery record and is not replaced by this. It
is ordered, append-only and epoch-stamped, and `HotRestore` merges on that epoch;
the index is an unordered latest-value store per key. Neither can stand in for the
other.

### Open: `List`/`Watch` do not see an evicted row

The read-through leg answers a **point read**. It does not, and with Hazelcast
cannot, extend to a scan: the generated query plane (`StratumRecordMaps`) iterates
`IMap.values()`, and `values()` — like `size()` — runs over the resident entries
only. So with the clock live:

- a `Get` by key returns an idle-evicted row (the loader fires);
- `List`/`Watch` do not return it, and `Count` does not count it, until something
  `Get`s it back into memory.

For `Count` that is the decision's own rule ("a dashboard counting a dataset" must
not keep it hot). For `List`/`Watch` it is a real behaviour change at
`idle_seconds`, and closing it would mean giving the generated scan a second,
disk-side leg — structurally the two-leg scan that was deleted with the warm model.
Raised for decision in `looper/schematic/inbox.md` (2026-09-22); not blocking, and
stated here rather than left to be discovered.

### Cold start keeps its event-log fallback

`ColdStartRebuilder` is retained deliberately; see "Cold start" above. The
checkpoint makes it unreachable for a pod that has one, which is the cutover the
decision asked for. A pod with no checkpoint — a first boot, a replaced volume —
still replays, and deleting that path would leave such a pod with no recovery route
when the `stratum` consumer group's offsets have already advanced past the history.

## What stratum does NOT guarantee

- **It is not the system of record.** A record's presence in stratum is not proof
  it happened, and its absence is not proof it did not. The event log is the truth.
- **It does not survive a lost event log AND lost volumes.** With every checkpoint
  and every redo log gone, the recoverable history is whatever the broker still
  holds — and stratum's effective history is then
  `min(what was ever published, broker retention)`.
- **A redo append is queued, not awaited.** The ack is the in-memory replication.
  A process killed inside that window loses that append; the row survives on the
  other writers that queued the same append, and a simultaneous loss of every writer
  inside that window is the named, accepted cost of acking on replication rather
  than on the disk write. The persist now crosses a network hop before it is queued,
  so the window is one hop wider than it was when the append rode the replication.
- **N writers write N identical dumps**, and since every writer holds the whole store
  they are now identical in full. That is the accepted cost until the "collect from
  one pod and compress" process exists (Jody, 2026-09-20).

## Verification

- `DailyCheckpointTest` — the tombstone log's contents, the dump's
  one-rename commit, a failed dump leaving the previous checkpoint still named by
  `current`, truncate running only after that commit, and the sweep's idempotence.
- `CheckpointColdStartTest` — a cold start that loads `checkpoint/current` and
  replays only the tail, and a purged row that nothing left on the volume can bring
  back.
- `disk/IdleEvictionDurabilityTest` — the three properties the leg exists for,
  each against a control that is the pre-leg wiring: an idle-evicted row is still
  readable, it still appears in the next checkpoint dump, and it survives a cold
  start built on that dump.
- `StratumMapConfigDurabilityTest.everyRecordMapCarriesItsDeclaredIdleClock` — every
  record map carries its declared clock AND the loader behind it, asserted together
  so neither can be applied without the other.
- `RecordAnnotationsTest` — the markers resolved off the descriptor, including a
  real generated type's `(spec.store_epoch)`.
- `HotRedoRestoreTest`, `HotRestoreLedgerTest` — the redo leg and the cross-node
  restore barrier. `ColdStartRebuilderTest` — the event-log replay decision.
- `HotRestoreDurabilityFloorTest` — a recovery under the floor refuses rather than
  logs; `HotRestoreCheckpointContributorTest` — a peer whose log is empty because
  its checkpoint just truncated it still counts toward that floor, and a peer with
  neither a tail nor a dump still does not.
- `hot/HotWritesWriterCountTest` — one writer and five search members refuse a write
  needing two copies, the same write is acked once a second writer joins, and a
  member with no role attribute counts as a writer.
- `hot/SearchMemberRoleTest` — the persist goes to every writer and no search member
  (including to a writer that owns nothing of the key, which is the property the old
  partition-scoped append did not have); the restore barrier expects writers only and
  search members file nothing; search members take partitions, answer the query, and
  give them back losslessly.
- `hazelcast/StratumMapConfigDurabilityTest` — `backupCount` does not move with
  `min_durable_copies` and a record's own declaration does not move it either; the
  protection counts writers; a search member's maps carry neither the clock nor the
  loader.
- `StratumMapConfigDurabilityTest.everyRecordMapCarriesItsDeclaredIdleClock` — every
  record map is configured with its declared clock; `IdleResidencyEvictionTest` —
  that the clock actually evicts, and that a read resets it without writing.
