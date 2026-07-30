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
    sessionId, agentId, eventType. Retention cleanup deletes events older than
    `retention_days` on a configurable interval.
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
