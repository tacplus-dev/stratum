# Stratum — Design

## Purpose

Stratum is the platform's **record store**. It holds a tenant's tacplus-declared records
(`option (spec.record)`) and serves the generated record plane over them: reads, conditional writes and
a change feed. tacplus declares the record contract; Stratum decides how the rows are held.

Records are written and read through Stratum's own generated API. Stratum does not ingest from the event
bus (`option (spec.record_store) = {no_bus_ingest: true}` in `proto/services/stratum.proto`).

Decision: tacplus-dev/roadmap `decisions/stratum-per-tenant-2026-10-25.md`. Issue: stratum#2.

## Current shape (per tenant, in memory, 2026-10-28)

- **Per tenant.** `(spec.service) = {tenant_owned: true}`: one Stratum per tenant, `people` included,
  placed by config. No tenant is special in code.
- **In memory.** Rows live in Hazelcast maps, one per record type, spread across the tenant's members.
  Backups are zone-aware (`STRATUM_ZONE`). Nothing is evicted; the whole dataset stays resident. Memory
  is for availability, not durability.
- **Durability is the redo service.** A write is applied only after the redo service acked it at the
  record type's durability level (MEMORY, LOCAL, REMOTE). Every start replays the tenant from the redo.
  See [docs/DURABILITY.md](docs/DURABILITY.md).
- **Language:** Java. `tacplus.yaml` sets `records: all`: the generated surface covers every
  `(spec.record)` in the fleet.

### The record plane

Per record type, generated:

- **Query plane:** `create<T>Query`, `update<T>Query`, `list<T>`, `get<T>Count`, `get<T>Sample`,
  `subscribe<T>Count`, `delete<T>Query`, with keyset paging and the `(spec.query_field)` filters, on the
  S2S surface and again under the C2S verbs (`open<T>Query`, `pull<T>`, ...). Query handles live in the
  Hazelcast map `stratum.record-queries`, so a handle opened on one member is served by any member. Rows flagged by `(spec.deleted_flag)` are excluded
  unless the query asks for them.
- **Write surface:** `Put<T>`, `InsertIfAbsent<T>`, `ReplaceIfUnchanged<T>` (by revision or by expected
  row), `Delete<T>`, and `Apply` for several ops over several entries, all or nothing. An append-only
  type (`MUTABILITY_APPEND_ONLY`) gets `InsertIfAbsent<T>` only. A private type
  (`(spec.record).private`) gets no S2S RPCs.
- **`Get<T>`:** a point read with the entry's revision.
- **`Watch<T>Changes`:** a snapshot, then the committed changes, resumable by revision.

All of it is emitted by `stratum-codegen` (`codegen/`, a Maven plugin that reads the `.proto` text of the
generation) from the `(spec.record)` messages found by `RecordDiscovery`:

- `RecordMapImplGenerator` emits `StratumRecordMaps`, the plane's implementation; `writeSurface` emits
  Get, the writes and Watch for one type.
- `RecordStoreGenerator` emits `StratumStores` (one typed `IMap` per record type, which the query plane
  scans) and `StratumRecordTypes` (each type's name, durability, key and namespace accessors, and the
  decoding of a `RecordOp` into a `WriteOp`).
- `RecordQueryImplGenerator` emits `StratumRecordQueries`, the C2S query plane, delegating to the S2S one.
- `VServiceWiringGenerator` emits `StratumVServiceBase`, which `StratumHandler` extends.

Stratum's hand-written code names no record type.

## Components

| Path | What it does |
|---|---|
| `server/.../StratumHandler.java` | Brings the Hazelcast member and the record store up on the first config; runs the keeper thread that replays whenever this member is the sequencer and out of sync; applies `write_ack_timeout_ms` live. |
| `server/.../store/RecordStore.java` | The store: sequencer (oldest member) and forwarding, the write path (locks, conditions, append, ack, apply, publish), point reads, replay and takeover, the read gate. |
| `server/.../store/EntryLocks.java`, `EntryId.java` | Entry locks in canonical order (record type, then key); map locks. |
| `server/.../store/WriteOp.java`, `RecordType.java`, `RecordTypes.java` | One decoded write; what the store knows of a record type. |
| `server/.../store/RedoLog.java`, `GrpcRedoLog.java` | The redo service through its generated client: one `Append` stream per store, a `Replay` per catch-up, route tenant `00000002`. |
| `server/.../store/ChangeFeed.java` | Each member's view of committed changes: what Watch serves, with 4096 changes of history per type. |
| `server/.../hazelcast/StratumMapConfig.java` | Hazelcast config: cluster `stratum-<tid>` (the tid from the instance's own SPIFFE identity), TCP/IP join to the seed members only (never multicast), one map per record type, backups, zone-aware grouping, the change topic, the sequencer executor. No eviction. |
| `server/.../hazelcast/ProtoMessageSerializer.java` | Rows travel as (proto full name, proto bytes), never as a Java class name. |
| `proto/services/stratum.proto` | Config (`hazelcast_memory_mb`, `write_ack_timeout_ms`), state, health, and the `redolog.Redo` client declaration. |

`server/...` is `server/src/main/java/com/github/tacplusdev/stratum`.

Environment: `STRATUM_SEED_MEMBERS` (Hazelcast join; required, a single member or a dev run sets `127.0.0.1`), `STRATUM_BACKUP_COUNT` (default 1), `STRATUM_ZONE`.

## What was removed (stratum#2)

The writer and search member roles, the volumes, Stratum's own redo logs, the daily checkpoint and its
tombstone log, the per-type disk index behind idle eviction, event-bus ingest and the event-log cold-start
rebuild (`EventStore`, `ColdStartRebuilder`, `checkpoint/`, `disk/`, `hot/`), and the working-set map
config. Config fields 2 and 4-8 are reserved in `stratum.proto`.

## Notes from huddles

- **2026-10-21 (identity graph huddle, `roadmap/identity-graph-huddle-2026-10-21.md`):** the identity
  graph (XIDs, TIDs, login mappings, tenant names) needs a Stratum above the clusters, with a conditional
  write, a revision per record and a change feed.
- **2026-10-25 (Stratum huddle, `decisions/stratum-per-tenant-2026-10-25.md`):** settled that need
  differently. Stratum runs per tenant; the identity graph lives in the `people` tenant's Stratum, owned
  by a people service there, not in kernel state. The primitives it needed are this design's:
  InsertIfAbsent / ReplaceIfUnchanged (an XID is minted by insert-if-absent, retried on collision;
  uniqueness on another attribute is made per entry by keying a row by it), entry locks in a fixed order
  for a few known entries, revisions, and Watch. Map locks exist and are rare. Region failover (active and
  passive clusters, a witness, fenced terms) is kept as the direction, not built.
- **2026-10-26 (Jody):** the redo service runs in the reserved `restore` tenant (`00000002`) and is
  reached by a tenant-to-tenant call, not as a platform service.
- **2026-10-27 (`decisions/tenant-deletion-2026-10-27.md`):** deleting a tenant deletes its Stratum with
  its namespace and its redo log at both sites.
- **2026-10-28 (Jody, redo#1):** the second site is redo to redo (`redo.RedoSite`). Stratum builds
  against LOCAL now; REMOTE is refused until that link is deployed.
