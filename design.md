# Stratum — Design

## Purpose

Stratum is the platform's **record store**: it hosts tacplus-declared records
(`option (spec.record)`) for fast lookup and serves the query planes built on
them. How records are physically stored — in-memory maps, disk, archival tiers —
is stratum's own concern; tacplus declares the record contract, stratum decides
the storage strategy.

Stratum is also the Supervisor LLM's stateful SQL workspace: a dynamic MCP tool
surface over the records it holds (distributed working-set maps, a durable
scratchpad, async job management, a SQL engine across loaded data). The
Supervisor is a persistent single brain — not a stateless request handler — and
stratum provides the infrastructure for that persistence.

## Current shape (post StratumQuery implementation, 2026-07-29)

- Language: Java. Declares `record_store` in `stratum.proto`; generates from
  `[proto-base, proto-app]` because it hosts records for base and app vservices
  alike (agentsensor, llmsensor, scmsensor, gitlabconnector).
- **RecordMap plane** (generated): typed record maps per record type with keyset
  paging; stratum's service-local codegen (`stratum-codegen`, a registry.json
  consumer — only tacplus reads protos) emits the store wiring
  (`StratumRecordMaps`).
- **StratumQuery vservice** (hosted here, real implementation):
  - **Storage**: `EventStore` — a Hazelcast `IMap<event_id, AgentEvent>` with
    SORTED indexes on epoch/timestampMs and HASH indexes on spiffe_id,
    sessionId, agentId, eventType.
  - **Member roles** (`hazelcast/StratumRole`): one cluster, two kinds of member,
    carried as the member attribute `stratum.role` and declared per pod by
    `STRATUM_MEMBER_ROLE`.
    - **writer** — holds partitions like any member AND persists every write:
      every write cluster-wide goes to every writer's redo log and record index
      (`hot/RecordPersistTask`), whichever member owns the partition, so **a writer
      holds the full dataset on disk** and can restore it alone. Writers take part
      in the daily checkpoint, and they are the only members counted for
      `min_durable_copies`, for the split-brain minimum, for the cluster-formation
      wait and for the hot-restore barrier. A static count (2 or 3), one volume
      each — the operator's `stratum_nodes`.
    - **search** — holds partitions (primaries and backups) and answers queries
      like any member, and nothing else: no volume, no redo log, no record index,
      no checkpoint, and no idle clock (the clock is wired to the disk leg it does
      not have, and without the leg eviction would be deletion). Never a durable
      copy, never a restore contributor. A dynamic count scaled for query
      throughput — the operator's `stratum_search_nodes`, default 0 — and a search
      member that leaves loses nothing durable.
    `backupCount` (`STRATUM_BACKUP_COUNT`, default 1) is memory redundancy and is
    independent of `min_durable_copies`, which is durability. They were the same
    number while the redo append rode the partition replication; separating them is
    what lets a volume-less member own a partition safely. **A predicate query is a
    scatter-gather across all members in parallel whose results are merged**, which
    is what a search member is for. See `docs/DURABILITY.md`.
  - **Record lifecycle** (`DailyCheckpoint`): on the `cleanup_interval_seconds`
    tick every WRITER runs one transaction over its own volume — purge
    flagged-deleted rows past their max age into a write-only tombstone log (and
    out of the record index, not memory alone), dump every surviving row to
    `<dataRoot>/checkpoint/<ts>` committed by one atomic rename, then truncate the
    redo logs to that dump point. The dump reads the **union of this pod's record
    indexes and its resident memory** (`DurableRows`), so a row that is off-heap on
    its idle clock is still in the checkpoint that step 3 truncates the redo log
    behind. Cold start is that checkpoint plus the redo tail, not a replay from
    offset zero.
  - **Memory residency**: one clock per record type, run by Hazelcast as
    per-entry max-idle from `(spec.record).residency.idle_seconds`. A row stays
    resident while it is read or written and leaves memory once it has gone that
    long with neither. This replaced the HOT/WARM/COLD residency classes, the
    `retention_days` sweep and the node-local Lucene tier, all removed on
    2026-09-20.
  - **The disk leg** (`disk/RecordDisk`, `disk/RecordIndex`,
    `disk/RecordDiskLoader`): one Lucene index per record type under
    `<dataRoot>/records/<pkg>.<Type>/`, keyed by the record key and holding the
    serialized record. Every write lands on every writer's index as well as in
    memory, riding the same task that queues the redo append, so a writer's index
    holds the whole store; a memory miss loads back through a Hazelcast
    `MapLoader` wired in the same branch of `StratumMapConfig.durableMap` that
    applies the clock, so a map cannot carry the clock without the leg — and a
    search member, having no leg, gets neither. This is
    what makes idle eviction a cache miss rather than a deletion, and it is what
    the clock was hotfixed off for lacking on 2026-09-21. It is NOT the warm tier
    returning: no slices, no epoch pruning, no disk-side query plane. One
    consequence is open and documented — a point `Get` reloads an evicted row,
    but `List`/`Watch`/`Count` scan resident entries only and so do not see it.
    See `docs/DURABILITY.md` for the mechanism and the layout.
  - **Session aggregation** (`StratumQueryImpl.buildSessionSummaries`): folds
    AgentEvent records grouped by `session_id` into `SessionSummary`. Each
    summary tracks first/last timestamps, event count, max call depth,
    latest event type, and active/idle state (idle = now − last_seen_ms >
    `active_session_idle_seconds`, default 900s). Aggregation is deterministic
    and stateless — recomputed per query from the Hazelcast map, not cached.
  - **Paged queries**: `listActiveSessions`, `listSessionEvents`,
    `listUserSessions`, `listEventsByTimeRange`, `listEventsByActor` — all use
    Hazelcast predicate queries + `pageSorted` keyset pagination (composite
    keys: `[i64(timestamp), event_id]` for events,
    `[i64(last_seen_ms), session_id]` for sessions). Fail-closed: missing
    required params → INVALID_ARGUMENT.
  - **Live streaming** (`subscribeLiveSessions`): server-streaming driven by
    the ingest path (`handleEventStreamMessage`). Each incoming AgentEvent is
    pushed to registered watchers via an in-process `CopyOnWriteArrayList`
    fan-out. Watchers receive `WatchLiveSessionsEvent.session_updated`
    (quick single-event snapshot) and optionally `live_event` (raw event).
    Stream stays open until client disconnects.
  - **Map-plane RPCs** (createMap, deleteMap, listMaps, getMapDetail,
    getMapSize, updateMapByIds, updateMapBySql, updateMapFilter, merge,
    executeMapQuery): delegated to `WorkingSetStore`, which manages
    Hazelcast-backed working-set maps with idle TTL eviction. SQL filtering
    and querying via `CalciteEngine`.
- **Events in**: record-bearing topics arrive via eventbus subscriptions;
  `handleEventStreamMessage` stores records and fans out AgentEvents to live
  session watchers. The 1.2-era RecordWatchHub fan-out is excised (no consumer).

## Rewrite note

The pre-cutover version of this document described a retired storage-tier
architecture and no longer reflected the system. This stub is intentionally
minimal and truthful; the P3 stratum pass (StratumQuery implementation) should
expand it with the storage-tier design it actually builds.
