# Stratum — Design

## Purpose

Stratum is the Supervisor LLM's stateful SQL workspace. It is not a store — it is an intelligence layer that sits above the storage tier (hotdb, colddb) and exposes a dynamic, stateful MCP tool surface to the Supervisor.

The Supervisor is a **persistent single brain** — not a stateless request handler. It maintains global awareness across all concurrent investigations. Stratum provides the infrastructure for that persistence: distributed working-set maps, a durable scratchpad, async job management, and a Calcite SQL engine to query across loaded data.

## Where Stratum Fits

```
EventBus (Kafka)
  ├── → hotdb     (30-day Hazelcast IMap, indexed, in-memory)
  ├── → colddb    (Iceberg/Parquet, partitioned, long-term archive)
  ├── → timedb    (ClickHouse projections — small metadata only)
  └── → graphdb   (Dgraph projections — small metadata only)

Supervisor LLM
  ↕ MCP tools
  stratum
    ├── queries hotdb  via gRPC  (load_hot_data)
    ├── queries colddb via gRPC  (load_cold_data)
    ├── own Hazelcast cluster (working-set maps + scratchpad + job buffers)
    ├── Apache Calcite SQL engine (query_map)
    └── publishes to EventBus → timedb/graphdb (load_map_into_*)
```

Stratum does **not** subscribe to EventBus for storage. It only publishes to EventBus (to drive timedb/graphdb population at the Supervisor's direction).

## Service Identity

| Property | Value |
|----------|-------|
| Service name | `stratum` |
| Service ID | 34 |
| Proto | `proto/services/stratum.proto` |
| vService | `proto/vservices/stratum.proto` (MCP tool surface) |
| Language | Java (consistent with hotdb, colddb) |
| Pattern | Same tacplus/additionalServices() pattern as hotdb/colddb |

## The Storage Tier (Context — Not Stratum's Responsibility)

| Service | Was | Role | Retention |
|---------|-----|------|-----------|
| `hotdb` | hazelcast-hot | In-memory IMap, EventBus subscriber | 30 days |
| `colddb` | (new) | Iceberg cold archive | 365+ days (configurable) |
| `timedb` | clickhouse | Timeseries projections | 90 days |
| `graphdb` | dgraph | Graph projections | 90 days |

The Supervisor is told how much data exists in each tier. It decides whether to pull from hot, cold, or both. It decides whether to promote data into timedb or graphdb.

## Stratum's Own Hazelcast Cluster

Stratum runs its own embedded Hazelcast cluster (separate from hotdb's cluster) with three logical namespaces:

### 1. Working-Set Maps (`ws:<map_id>`)

- Type: `IMap<String event_id, byte[] serialized_SensorEvent>`
- Keyed by `event_id` — deduplication is automatic (same event from hot and cold overwrites)
- In-memory only (no disk persistence) — rebuilt on demand if lost
- Idle TTL eviction (configurable, default 2 hours)
- Multiple stratum nodes share one distributed cluster — a map created on node 1 is queryable from node 2

### 2. Scratchpad (single named IMap, disk-persistent)

- Type: `IMap<String key, byte[] value>`
- Fully owned by the Supervisor — it sets keys, schemas, and values
- Persists across Supervisor restarts and stratum restarts (Hazelcast Hot Restart / Persistence enabled)
- This is the Supervisor's long-term memory: investigation state, job IDs, conclusions, pattern notes

### 3. Job Buffers (`job:<job_id>`)

- Type: `IList<byte[] serialized_JobEvent>` (append-only, sequenced)
- Holds streaming results so the Supervisor can reconnect and resume from a sequence number
- TTL eviction after job completes (configurable, default 30 minutes)

## MCP Tool Surface

### Synchronous Tools

```
create_map(purpose: string)
    → map_id: string
    Creates a new working-set map with a human-readable purpose label.

delete_map(map_id: string)
    → ok
    Deletes a map and frees its Hazelcast memory.

list_active_maps()
    → [{map_id, purpose, event_count, memory_bytes, created_ms, last_accessed_ms}]
    Returns all open maps with metadata for resource self-awareness.

describe_map(map_id: string)
    → {query_summary, event_type_breakdown, time_range_ms, memory_bytes}
    Deeper inspection of a specific map.

get_resource_usage()
    → {total_maps, total_events_loaded, hazelcast_memory_used_bytes,
       hazelcast_memory_total_bytes, cold_queries_in_flight, jobs_active}
    Supervisor self-awareness of its Stratum footprint.

get_job_status(job_id: string)
    → {state: PENDING|RUNNING|DONE|FAILED, progress_pct, events_processed, error}

scratchpad_read(key: string)
    → value: bytes

scratchpad_write(key: string, value: bytes)
    → ok

scratchpad_delete(key: string)
    → ok

scratchpad_list()
    → [{key, size_bytes, last_updated_ms}]

scratchpad_clear()
    → ok (nuclear option — wipes entire scratchpad)
```

### Async Tools (Submit → Status/Subscribe)

All heavy operations return a `job_id` immediately and execute in the background.

```
load_hot_data(map_id: string, sql: string)
    → job_id: string
    Executes SQL against hotdb's gRPC query API, loads matching SensorEvents
    into the specified map. SQL dialect: Calcite (same as query_map).

load_cold_data(map_id: string, sql: string)
    → job_id: string
    Executes SQL against colddb's gRPC query API (Iceberg scan),
    loads results into the map. May be slow for large time ranges.

query_map(map_id: string, sql: string)
    → job_id: string
    Executes Calcite SQL over the Hazelcast IMap contents.
    Returns paginated SensorEvent results via the job buffer.

load_map_into_graphdb(map_id: string)
    → job_id: string
    Republishes all events in the map to EventBus → graphdb picks them up.

load_map_into_timedb(map_id: string)
    → job_id: string
    Republishes all events in the map to EventBus → timedb picks them up.

subscribe_to_job(job_id: string, from_sequence: int)
    → stream<JobEvent>
    Streams results from the job buffer. from_sequence=0 means from start.
    Supports reconnect: if the Supervisor disconnects mid-stream, it can
    resubscribe with the last received sequence to resume without loss.
```

### JobEvent Structure

```protobuf
message JobEvent {
  int64 sequence = 1;                      // monotonic, for reconnect
  JobProgress progress = 2;                // events_processed, pct
  repeated SensorEvent data = 3;           // batched results
  bool is_eos = 4;                         // true on final message
  string error = 5;                        // set if job failed
}

message JobProgress {
  int64 events_processed = 1;
  int32 progress_pct = 2;                  // 0-100, -1 if unknown
}
```

## Supervisor Startup Context Injection

Every Supervisor invocation is prepended with a resource context block before the actual request. Stratum provides a `GetSupervisorContext` RPC that returns this as structured data. The MCP server formats it as:

```
Resource state:
  Active maps (3):
    - map_abc123: "investigating honeypot trip session X" — 4,201 events, 18MB, 14m ago
    - map_def456: "commit lineage ABCD" — 892 events, 4MB, 2h ago (IDLE — consider cleaning up)
    - map_ghi789: "periodic sweep 2026-04-29" — 12,004 events, 56MB, 3m ago

  Scratchpad (4 keys):
    - "investigating": 1.2KB, 3m ago
    - "active_jobs": 0.8KB, 3m ago
    - "known_honeypots": 4.1KB, 1h ago
    - "session_patterns": 12KB, 2h ago

  Memory: 78MB / 2GB used
  Active jobs: 1 (job_xyz running load_cold_data for map_abc123)

You are a single persistent brain. Before responding to the new request:
1. Review active maps and scratchpad for relevant prior work.
2. Clean up maps older than 2h with no recent access — call delete_map.
3. Do not duplicate work already captured in an existing map.
4. Check scratchpad key "active_jobs" for in-flight work you may want to resume.

New request: [...]
```

## Async Job Pattern — Implementation Notes

- Each async operation creates a Hazelcast `IList<byte[]>` named `job:<job_id>` before starting work
- The background thread appends `JobEvent` entries (serialized proto) as results stream in
- The final entry always has `is_eos=true` (and `error` set if failed)
- `subscribe_to_job` is a gRPC server-streaming RPC that reads from the IList starting at `from_sequence`, then long-polls for new entries until EOS
- Job buffer TTL: configurable, default 30 minutes after EOS is written
- Job state map: `job_state:<job_id>` → `{state, progress, submitted_ms, completed_ms, error}`

## SQL Query Language

Apache Calcite throughout:

- `load_hot_data(map_id, sql)` — SQL sent to hotdb's `ExecuteSql` RPC (hotdb exposes Calcite-compatible SQL over its Hazelcast IMap schema)
- `load_cold_data(map_id, sql)` — SQL sent to colddb's `ExecuteSql` RPC (Calcite over Iceberg via the Iceberg Calcite adapter)
- `query_map(map_id, sql)` — Calcite over the in-memory Hazelcast IMap (stratum registers the map as a Calcite schema)

The LLM writes SQL. SQL is expressive, well-understood by LLMs, and maps cleanly to the SensorEvent schema.

## Interrupted Work Pattern

The Supervisor persists investigation state to the scratchpad:
```json
"investigating": {
  "trigger": "honeypot",
  "session_id": "abc123",
  "map_id": "ws:xyz789",
  "job_id": "job:load_cold_abc",
  "status": "in_progress",
  "started_ms": 1746000000000
}
```

On the next invocation, the Supervisor reads this, checks if `map_id` still exists via `list_active_maps()`, checks if `job_id` is still running via `get_job_status()`, and decides: resume or restart.

## Trigger Model (Outside Stratum)

The trigger mechanism is **not** Stratum's concern. A separate orchestration layer (TBD) watches EventBus for trigger events and invokes the Supervisor with the appropriate model and context:

| Trigger | Model | Urgency |
|---------|-------|---------|
| Periodic sweep | Cheap model (Haiku) | Low — runs on schedule |
| Commit event | Mid-tier model (Sonnet) | Normal |
| Honeypot trip | Thinking model (Opus) | High — immediate |

Stratum provides the `GetSupervisorContext` RPC that the orchestration layer calls to build the startup context injection.

## Proto Definition Sketch

`proto/services/stratum.proto` — service ID 34:

```protobuf
syntax = "proto3";
package stratum;

import "libs/spec.proto";
import "libs/trace.proto";
import "vservices/claude-sensor.proto";

option (spec.service) = {id: 34};
option (spec.int_const) = {name: "DEFAULT_PORT", value: 8443};

message Config {
  option (spec.grpc_config) = {service_name: "stratum", id: 34};

  // hazelcast_memory_mb is the Hazelcast cluster memory limit in MB.
  int32 hazelcast_memory_mb = 1 [(spec.default_int) = 2048, (spec.assert_ge) = 256];

  // map_idle_ttl_seconds evicts idle working-set maps. Default: 7200 (2h).
  int32 map_idle_ttl_seconds = 2 [(spec.default_int) = 7200, (spec.assert_ge) = 60];

  // job_buffer_ttl_seconds keeps job result buffers after completion. Default: 1800 (30m).
  int32 job_buffer_ttl_seconds = 3 [(spec.default_int) = 1800, (spec.assert_ge) = 60];

  // max_maps limits concurrent working-set maps. Default: 20.
  int32 max_maps = 4 [(spec.default_int) = 20, (spec.assert_ge) = 1];
}

message StratumState {
  option (spec.service_state) = {tag: "kernel", auto_tls: TLS_KEY_TYPE_ED25519};
  // Stratum depends on hotdb and colddb for data loading
  option (spec.grpc_s2s_client) = {service: "hotdb"};
  option (spec.grpc_s2s_client) = {service: "colddb"};
}
```

`proto/vservices/stratum.proto` — MCP tool surface (all RPCs listed in MCP Tool Surface section above).

## Implementation Steps for Agent

1. **Write `proto/services/stratum.proto`** (service ID 34) — Config, StratumState with grpc_s2s_client deps on hotdb and colddb.

2. **Write `proto/vservices/stratum.proto`** — all sync and async MCP RPCs, JobEvent, JobProgress, MapInfo, ResourceUsage messages.

3. **Create `services/stratum/`** following the hotdb/colddb pattern:
   - `tacplus.yaml` — image name `registry.gitlab.com/secondstarcavalier/schematic/platform/services/stratum/stratum`
   - `pom.xml` — parent, modules (codegen, server)
   - `Dockerfile` — COPY grpc_health_probe (pre-downloaded arm64 binary), stratum entrypoint script, same pattern as hotdb
   - `stratum` — entrypoint shell script: `exec java $JAVA_OPTS -jar /app/stratum.jar "$@"`
   - `justfile` — build/run targets matching hotdb pattern

4. **Run `tacplus build`** from `services/stratum/` to generate Java types.

5. **Implement `StratumHandler.java`** with `additionalServices()` pattern:
   - Fetches config from kernel via `StratumConfigStateServiceGrpc`
   - Starts embedded Hazelcast cluster with three map configs:
     - `ws:*` — in-memory, `map_idle_ttl_seconds` TTL, no persistence
     - `scratchpad` — disk-persistent (Hazelcast Hot Restart Store)
     - `job:*` — in-memory, `job_buffer_ttl_seconds` TTL, no persistence
   - Returns `List.of(new StratumQueryImpl().bindService())`

6. **Implement async job execution**: thread pool (or virtual threads), Hazelcast IList append, long-poll subscribe.

7. **Implement Calcite SQL engine** over Hazelcast IMap for `query_map`.

8. **Wire gRPC clients to hotdb and colddb** for `load_hot_data` and `load_cold_data`.

9. **Add `stratum` to `mock/local/build/settings.yaml`** topology and `docker-compose.yml`.

10. **Add `stratum_config` to `proto/services/services_config.proto`** (generated — update the source, regenerate).

11. **Run `cavalier -c build`** to generate bootstrap0.yaml entry, then verify kernel starts with stratum config.

## Key Design Constraints

- **Stratum does not subscribe to EventBus.** It publishes to EventBus (to drive timedb/graphdb), but storage is hotdb/colddb's job.
- **colddb stays dumb.** No async, no working sets — just EventBus ingest and synchronous gRPC reads.
- **One brain.** The Supervisor is a single persistent process, not per-request. Stratum's resource tools enable self-awareness and deduplication of work.
- **Working-set maps are in-memory only.** If lost, the Supervisor rebuilds them. The scratchpad is the only thing that must persist.
- **SQL everywhere.** Calcite for hot, cold, and map queries. LLMs write SQL well.
