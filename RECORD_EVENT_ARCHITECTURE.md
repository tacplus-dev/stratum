# Stratum — the record store

Stratum is a **generic, app-agnostic, typed, stateful database**. It holds the
**full records** of the data plane; timedb and graphdb hold **projections**
(offshoots) of those records. Stratum's hand-written code names **no vservice** —
every per-record-type surface is generated from the tacplus contract.

> Authoritative annotation contract: `schematic/tacplus/docs/RECORD_EVENT_MODEL.md`.

> **2026-08-25 — timedb and graphdb are in the boneyard** (BIG GOAL 3, first move
> #1): out of the fleet build, protos deleted, no images published. Every mention
> of them below describes the world before that. Stratum is now the only store on
> the bus. `timeseries_projection` survives as a record's **retention** declaration;
> `graph_projection` and the two `*_store` annotations have no host at all.

## Role

```
producer ─[event]→ eventbus ─┬─→ STRATUM   stores the full record   ← this service
                             ├─→ timedb    timeseries projections
                             └─→ graphdb   graph projections
```

- A message annotated `record { key, owner, … }` is stored here in full, keyed by
  `key`, owned by `owner`. Stratum (annotated `record_store`) auto-subscribes to the
  events that carry records and stores them.
- Records are held **distributed and in-memory** today; the hot/cold split will later
  merge into a single "fast-store" tier. The backing engine is an **implementation
  detail** and never appears in the proto/tacplus contract.

## Query plane — typed result-set maps (C2S, fronted by argus)

For every `record` type, tacplus generates a **typed map API**. A caller:

1. `NewMap<T>() → map_id` — create a working-set map for record type `T`.
2. Operates on it by id: **add** (by ids / by query / by graphdb federation / by
   timedb federation), **merge**, **delete-from**, **modify/sanitize** entries,
   **filter**, **watch**, and **cleanup**.
3. **pull** — only this returns data, and it returns **fully-typed `T` entities**.
   Nothing generic is ever returned.

Key properties:

- **Data stays on stratum.** Result sets live as maps here; operations run
  **server-side** (a distributed executor moves compute to the data; python3
  transformers are planned). The map is mutated in place; the caller pulls only when
  it actually needs the rows.
- **Federation:** a timedb/graphdb-backed `add` makes stratum call those services
  (typed **S2S**), receive **keys**, and **hydrate** the full records by key into the
  map. timedb/graphdb never return full records — only keys + projection data.
- **tacplus knows the typed query shape, not its transform.** How a filter/query
  string becomes execution (SQL/predicate) is impl-side here; tacplus only generates
  the typed RPC surface.
- **Caller identity is known**, so maps are size-bound per caller and callers must
  clean up. **Clients never call stratum directly** — a service such as **argus**
  fronts stratum, drives the maps, and is responsible for deleting maps it no longer
  needs. Stratum will additionally reap **orphaned** maps (planned).

## What is generated vs hand-written

- **Generated:** the typed per-record store, ingest/subscription routing, and the
  typed map query API (one set of RPCs per `record` type).
- **Hand-written (vservice-neutral):** the generic map engine, the distributed-executor
  operations, the federation/hydration plumbing, and the query→execution transform.

Changing a vservice (`record`/projections) only requires regenerating + rebuilding
stratum — no hand-edits.
