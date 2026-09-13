# Record queries are pinned to the opening caller's namespace

2026-09-12. Fixes the Critical finding of `decisions/security-audit-2026-09-12/CONSOLIDATED_REPORT.md`
— `create<T>Query` never scoping the caller's filter to their own tenant — and, in the same
change, the "query handles are unscoped, shared bearer UUIDs" item recorded alongside it.
Code-only change in `codegen/` plus tests; no proto, config or deploy change. It is generator
code, so it changes what every future stratum build emits.

## The bug

`RecordMapImplGenerator` emitted, for every record type:

```java
public Query.QueryHandle createAgentEventQuery(Caller caller, Query.OpenAgentEventQueryRequest req) {
    String qid = UUID.randomUUID().toString();
    queries.put(qid, req.getFilter());
    return Query.QueryHandle.newBuilder().setQueryId(qid).build();
}
```

`caller` is the verified identity of who is calling and the body never reads it. Whatever the
caller supplied — including an empty, match-all filter — was stored verbatim under a fresh
UUID, and every RPC that dereferences that UUID (`List`, `Sample`, `Watch`, `Count`, the size a
`Refine` reports, `SubscribeCount`) scanned the whole multi-tenant store through it.

One root cause, two live consequences, both confirmed here rather than taken from the report:

- **`List`/`Sample`/`Watch`: a denial of service, not a leak.** The generated wrapper
  (`model/.../vservices/query/RecordMap.java`) checks every returned row against
  `Auth.callerNamespaceFromContext()` and, on a mismatch, prints
  `FATAL: (spec.store_namespace) backstop fired` and calls `Runtime.getRuntime().halt(1)` — an
  abrupt JVM kill, no graceful shutdown. Any caller with query entitlement on a namespaced
  record type could drop the pod by opening a match-all query. Reproduced directly: a control
  run of the new test against a deliberately unscoped generator ends with surefire reporting
  `The forked VM terminated without properly saying goodbye` and zero tests run.
- **`Count`/`SubscribeCount`/refine-size: a genuine information leak.** Those paths have no
  backstop; they returned an accurate count of another tenant's rows, silently.

## The fix

The filter is scoped at `create<T>Query`, so the backstop is never reached by construction.
Three emitted pieces, all shared across record types rather than per-RPC:

- `Handle` — the registry value is no longer the bare filter but `{namespace, pinned, filter}`.
  The namespace is the **opening** caller's verified claim and is never read off the request:
  `(spec.store_namespace)` is deliberately not a `(spec.query_field)`, so the boundary is not a
  predicate the caller gets to write. `pinned` is `!namespace.startsWith("/system")` — the same
  `/system`-sees-everything convention the generated wrapper's own backstop uses, and the same
  EXACT-equality rule, so the two seams cannot disagree about what a tenant is.
- `callerNamespace(Caller)` — fail-closed: `caller == null` or an empty claim is
  `PERMISSION_DENIED`, matching the wrapper's "no verified namespace, no rows". This is what
  closes the `Count` leak, which the wrapper never guarded.
- `inNamespace(Handle, String rowNamespace)` — joined into every store scan ahead of the
  caller's own predicate: `if (inNamespace(h, ev.getNamespace()) && matchAgentEvent(f, ev))`.

`update<T>Query` carries `h.namespace` onto the replacement handle: a refine replaces the
predicate, never the boundary, so widening back to match-all stays inside the opening tenant.

The namespace field is not hand-named anywhere. `EventTopicDiscovery` learned
`storeNamespaceField`, read from the record's own `(spec.store_namespace)` annotation the same
way `storeEpochField` is read from `(spec.store_epoch)`. tacplus makes that annotation
mandatory on every `(spec.record)` (`ValidateRecordNamespaceAnnotations`), so a record reaching
this generator without one means the declaration did not reach discovery — `RecordMapImplGenerator`
throws rather than emitting an unscoped query, the way `WarmStoreGenerator` refuses a warm record
with no store epoch instead of picking a substitute clock.

### Handle ownership

Included because it was small and falls out of the same `Handle`: `requireQuery` now takes the
`Caller` and refuses a handle whose namespace is not the requesting caller's, on every RPC that
dereferences one — `List`, `Sample`, `Watch`, `Count`, `SubscribeCount`, `Refine` and `Close`.
`NOT_FOUND`, not `PERMISSION_DENIED`: a `query_id` is a bare UUID shared across the C2S and S2S
surfaces, and answering "exists, but not yours" turns the registry into an existence oracle for
other tenants' handles.

### The warm disk leg

A warm record type is served by two legs and both are scoped. The memory leg gets the same
`inNamespace` predicate; the disk leg's hits are parsed protos already, so they are filtered on
the way into `matches`. `warmCount<T>` is the one place with a cost: the tier indexes
`(spec.query_field)` criteria only and the namespace is deliberately not one, so a **pinned**
handle counts off the parsed hits instead of summing per slice. A `/system` handle keeps the
existing materialise-nothing count. That set is the same one `list<T>` already pages, so this
adds no new scan shape.

## What was NOT changed

- The `halt(1)` backstop in the generated wrapper. It is a legitimate last-resort net and it
  stays; the fix is making sure it is never reached, not removing it.
- The unbounded handle registry (no cap, TTL or per-caller limit) — a separate Low item in the
  same report, untouched.
- The report's High items (durability floor, per-dataset restore quorum, restore-ledger
  staleness, cold-start rebuild topic) — out of scope here, all still open.
- An unused local `matchCast` in `RecordMapImplGenerator` (dead since the getter-based match
  landed). Noted, not deleted.

## Verification

`TwoTenantQueryScopeE2ETest` (11 tests) — two callers over real gRPC against one store holding
tenant A's 2 rows and tenant B's 3, driving the generated stub, the generated wrapper and the
generated handler. Every handle-taking RPC shape gets a two-tenant negative case: A opens a
match-all query and `List`, `Sample`, `Watch`, `Count`, `SubscribeCount` and the refine size all
answer with A's 2 rows / count 2, never B's; B holding A's `query_id` is refused `NOT_FOUND` on
all seven shapes and leaves A's handle intact; a caller with no namespace claim cannot open a
query at all. The happy paths are asserted too — B's own query returns B's 3 rows, B's own range
criterion still narrows, `/system` still reads all 5 — and the suite *completing at all* is the
assertion that the backstop never fired, since a same-tenant query that leaked would halt the
JVM instead of failing an assertion.

That is not hypothetical: reverting the scoping in the generator and re-running this test
produces exactly that — `Tests run: 0`, `The forked VM terminated without properly saying
goodbye`. With the fix, 11/11 pass.

`mvn -o test`: **codegen 40/40 pass.** Server: 91 run, 2 failures, 23 errors — against a
baseline, measured before this change, of 80 run, 1 failure, 23 errors. The 23 errors are the
same pre-existing set and are all one cause: the stratum E2E suite drives the plane with **no
bearer token at all**, and the tacplus-generated wrapper now refuses a namespaced read from a
caller with no verified namespace claim (`ListAgentEvent returns rows owned by a namespace and
the caller presented no verified namespace claim`). Those tests were already red at HEAD.

Two honest deltas inside that:

- `PullAgentEventE2ETest.closeReleasesTheHandle` passed before and now fails. It is the same
  tokenless cause as its 11 already-red siblings in the same class, and it passed before only
  because `Count` answered a caller with no namespace claim — which is precisely the leak this
  change closes.
- `HotAgentEventE2ETest.theLiveReadPathRefusesUntilTheRestoreGateOpens` and
  `WarmMutableSessionStateE2ETest` shuffle one test each between the "failure" and "error"
  columns. Same already-red tests, refused one RPC earlier than before.

Two test files do not compile at HEAD and were parked (and restored) to measure the baseline at
all: `StratumEventStoresRetentionTest` and `warm/WarmTierTest` both import
`model.types.services.tasks.TasksOuterClass`, which the platform model no longer generates —
`tasks.proto` lives in `testapp/proto`, not in the `platform/proto` tree `tacplus.yaml` points
at. Pre-existing and unrelated; not touched.
