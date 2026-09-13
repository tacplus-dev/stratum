# stratum Security & Quality Audit — Consolidated Report
2026-09-12 — multimodal review: Fable 5.1 (Claude), Codex CLI (GPT), Grok 4.6 (via GitHub Copilot CLI)
Scope: `server/src` (40 Java files) + `codegen/src` (15 files, stratum-specific codegen). `model/`
(tacplus-generated) and `target/` excluded from the main review, except where cited below to
confirm a mechanism that only exists in the generated output.

Stratum is the distributed (Hazelcast-based) state layer backing generated store/state services
platform-wide. It had a real incident days ago (whole-namespace restart → cluster-of-one →
split-brain protection threw → zombied JVM with no listener bound; fixed by waiting for quorum
before opening hot maps and exiting on startup failure instead of zombieing). This audit found
**several more members of that same failure family still present**, plus a genuine, more nuanced
tenant-isolation problem than initially reported.

---

## Critical — the tenant-isolation issue, fully characterized after direct verification

### `create<T>Query` never scopes the caller's filter to their own tenant [VERIFIED, all three tools + me]
**File:** `codegen/.../codegen/RecordMapImplGenerator.java:90-115, 225-240`
I read this directly: `create<T>Query(Caller caller, Open<T>QueryRequest req)` accepts the verified
`caller` parameter but the method body never references it —
`queries.put(qid, hasFilter ? req.getFilter() : Boolean.TRUE)` stores exactly what the caller
supplied, including an empty/match-all filter for filterless record types. All three tools found
this independently; Codex and Grok characterized the consequence as "any entitled caller can list
every tenant's records."

**What actually happens when that unscoped query executes — this refines the severity, don't skip
it.** I traced further into the *generated* wrapper (`model/target/generated-sources/.../vservices
/query/RecordMap.java`, excluded from the main review scope but checked specifically to confirm
this mechanism) and confirmed Fable's more complete finding:
- **`List`/`Sample`/`Watch` have a "fail-loud" backstop.** Every response row is checked against the
  caller's namespace, and if the underlying (broken) query returns a row from a different tenant,
  the generated code logs `"FATAL: (spec.store_namespace) backstop fired... the store's own query
  is leaking across tenants"` and calls **`Runtime.getRuntime().halt(1)`** — an immediate, abrupt
  JVM kill, no graceful shutdown. So the practical effect of the root bug for these three
  operations is **not a silent data leak — it's a trivially repeatable denial of service**: any
  caller with query entitlement on a namespaced record type can crash the hosting pod on demand
  simply by opening a broad query against a multi-tenant record type. Repeated across replicas
  (or triggered by an automated retry loop hitting the same broken query), this could drop the
  cluster below quorum — the same failure family as the original incident, just reached a
  different way.
- **`Count`/`size`/`SubscribeCount` have no such backstop** (confirmed by Fable, consistent with
  what I found — no per-row namespace check exists in those code paths). These genuinely do leak
  real cross-tenant information silently: an entitled caller gets an accurate count of another
  tenant's rows with no crash, no log, no signal to anyone.

**Bottom line:** this is one root cause (unscoped query filter) with two distinct, both-serious
consequences depending on which RPC is called — a live DoS vector on the read/watch path, and a
genuine (if narrower) information leak on the count path. Fix the root cause once
(scope the filter to the caller's own namespace at `create<T>Query`, or reject non-conforming
filters outright) and both consequences go away.

### Query handles are unscoped, shared bearer UUIDs [Codex + Fable cross-confirmed]
`codegen/.../RecordMapImplGenerator.java:113` (registry), same file's per-op methods — a query
handle is a bare UUID with no binding to the caller, record type, or C2S-vs-S2S surface (the two
surfaces deliberately share one handle registry). Anyone who obtains another caller's `query_id`
can read, widen (via `update<T>Query`), watch, or close that query.

---

## High — real correctness bugs in the same failure family as the original incident

All of these are distinct siblings of the class of bug that caused the whole-namespace-restart
incident (cluster-membership/quorum/recovery races), found by one or more tools and, where marked,
independently checked by me reading the cited code.

- **Durability floor is configured globally, not per-record** (Codex) — every map gets the
  deployment-default `minDurableCopies` regardless of what a generated record actually declares;
  a record declaring 3 copies against a 2-copy default floor can never satisfy its own durability
  requirement, permanently rejecting every write for that record type even on a healthy 3-node
  cluster.
- **Restore quorum counts durability contribution in aggregate across all datasets, not per
  dataset** (Codex) — a node holding only dataset A's data still counts as a contributor toward
  dataset B's quorum, so B can appear to meet quorum while genuinely under-replicated.
- **Recovery below the durability floor is logged, not rejected** (Codex) — `HotRestore.java`
  proceeds and flips the store readable even when fewer than the minimum durable copies
  contributed to replay.
- **The restore ledger never prunes departed members** (Codex + Fable + Grok — all three) — keyed
  by a UUID that changes every restart, with no cleanup on membership change. Fable's detailed
  trace: two of three pods die, the survivor keeps 3 stale entries, the recovering pod adds a 4th,
  sees 4 ≥ expected-3, and opens reads before the third pod has actually replayed anything —
  exactly the "wrong set satisfies the barrier" failure the original incident was about.
- **Event-log cold-start rebuild is hardcoded to one topic** (Codex + Fable) — `agentsensor.event_
  stream` only, while codegen discovers and stores records from every EventBus topic; a whole-
  cluster loss leaves every other record type's data unrecoverable from the event log, and (per
  Fable) rebuild is also gated so it never runs at all once any map shows a nonzero count, even if
  that count came from a different type's redo log.
- **The read gate opens before cold-start replay begins, not after it completes** (Codex + Fable)
  — `hotReadable` flips as soon as redo recovery clears, but asynchronous event-log replay (the
  fallback for un-logged types) hasn't started yet; clients can read empty/partial results as if
  they were complete.
- **Config-reload race: concurrent `initFromConfig` calls aren't serialized** (Codex + Grok) —
  callable from multiple RPC/callback paths with only one of several call sites synchronized;
  concurrent deliveries can shut down and replace the shared Hazelcast member state out from under
  each other.
- **A failed init during config reload can leave an orphaned Hazelcast member with no disk backing**
  (Fable) — the new store is assigned before init completes; on failure, the exception is swallowed,
  the member is never shut down, and it silently drops every subsequent write (no disk persistence)
  while every read on it returns `UNAVAILABLE`.
- **Non-atomic append-only duplicate check** (Codex) — the "have I seen this key" check happens
  before the Hazelcast mutation and disk enqueue, with a window for two concurrent deliveries of a
  supposedly-new key to both pass and both append, permanently corrupting warm-tier counts.
- **Redo logs never compact; retention writes no tombstones** (Fable) — every write appends
  forever with an fsync; on restart, retention-deleted rows resurrect from the redo log until the
  next retention sweep runs, and restore time scales with total lifetime writes rather than
  current live data.
- **Retention and disk-serving windows don't overlap at all** (Fable) — default 90-day retention
  vs. a 365-day "serve from disk after this age" boundary means no row ever survives long enough
  to reach the disk tier the code exists to serve from; the "still queryable, only slower" promise
  in the record's own proto comment doesn't hold with current defaults.
- **Schema drift between the redo-log JSON encoding and the live wire type causes a permanent crash
  loop** (Fable) — the redo payload is a lossy field-by-field JSON helper that silently drops
  unknown fields, while the peer-comparison check includes them; adding a new field to a record
  type before stratum picks up the change causes every affected key to report DIVERGED, which
  throws during init, which restarts the pod into the identical failure.
- **Unauthenticated Hazelcast cluster-join port with Java deserialization exposure** (Grok, not
  independently re-verified by me) — TCP port for cluster membership has no TLS/member-auth/
  serialization filtering, and at least two custom classes on that path are `Serializable`. Worth
  checking network exposure (likely mitigated by k8s network policy, but verify rather than
  assume).

## Medium
- Cold-start rebuild's event subscriber never unsubscribes after catching up, so every event is
  written twice per replica for the life of the process (Fable).
- A failed warm-tier disk write leaves its key permanently marked "pending," hiding it from the
  disk leg and making it un-evictable for the process lifetime (Codex + Fable cross-confirmed).
- Warm-tier slice query failures are logged and silently dropped rather than failing the request —
  a corrupt or transiently-unreadable slice produces an undercount that looks like a complete,
  successful result (Codex + Grok cross-confirmed).
- A `long` page limit is cast to `int` with no bounds check — a large client-supplied value can
  wrap negative and throw inside `subList` rather than validate or clamp (Codex).
- Sequential startup timeouts (2 min + 5 min) before the health listener binds can exceed the
  container/sidecar liveness window, potentially killing the pod for being slow rather than
  broken (Fable) — same shape of problem as the original incident's zombie-JVM symptom.
- Retention can close a Lucene searcher out from under an in-flight query, surfacing as a dropped
  slice / undercount with no error signal to the caller (Fable).
- Integer-overflow edge case converting an inclusive upper time bound to exclusive form at
  `Long.MAX_VALUE` (Codex).

## Low / anti-patterns / dead code
- `Watch`/`SubscribeCount` are documented as live streams but are actually one-shot snapshots —
  the doc references a `RecordWatchHub` component that doesn't exist (Fable + Codex-adjacent).
- Query-handle registry has no cap, TTL, or per-caller limit — unbounded heap growth is possible
  for any entitled caller (Fable).
- A substantial dead code path: a hand-rolled SQL parser and a Calcite-based query executor with
  zero callers, still fully maintained (five indexes kept up to date on every write) for a reader
  that doesn't exist (Fable) — flagged only, not touched, per the read-only scope of this audit.
- The restore ledger map itself isn't included in the same split-brain-protection map list that
  was the actual fix for the original incident — the fix covers data maps but not this
  coordination map (Grok).
- Minor discovery-tooling inconsistency: one file's proto-comment parser reads text that a sibling
  discovery tool strips, generating a phantom provider for a commented-out service block (Fable).

## Recommended next step
Fix the Critical root cause first (#1) — it's a single, well-localized fix (scope `create<T>Query`'s
stored filter to the caller's own namespace, or reject a filter that doesn't) that eliminates both
the DoS and the information-leak consequence at once. Then the restore-ledger staleness and
per-dataset-quorum issues (both in the same incident-adjacent family as what was already fixed) —
those are the ones most likely to cause a second version of the original outage.

## Raw per-tool output
- Codex: `codex.out.md` (18 findings)
- Fable 5.1: `fable.out.md` (16 findings — the deepest trace of the three, confirmed the
  `Runtime.halt` backstop mechanism directly in generated output)
- Grok 4.6: `grok.out.md` (completed in full on the first pass, no timeout this time — 13 findings)
