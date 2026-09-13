# HotWrites counts each map's OWN backups, closing the write-gate half of the per-record floor

Date: 2026-09-13
Branch: `d3-stratum-c2s-split`
Closes **residual gap 1** of
`2026-09-13-per-record-floor-wired-into-mapconfig-two-residual-gaps.md` (read that first — it
states the bug and why the `StratumMapConfig` fix at `6b5da3c` did not close it). The original
analysis is `2026-09-12-per-record-durability-floor-needs-a-generated-lookup.md`; the generator
half is `2026-09-13-eventstoregenerator-mindurablecopies-lookup.md`.

## The bug

`StratumMapConfig.build` now sizes each record map at
`max(deployment floor, StratumEventStores.minDurableCopies(name)) - 1` backups, so a record
declaring 3 against a deployment default of 2 physically gets two synchronous replicas and three
copies really can exist. `HotWrites` did not believe it. It was constructed once with a single
deployment-wide `syncBackupCount` (`EventStore.init`: `new HotWrites(hz, minDurableCopies - 1,
floorOverride)`) and `durableCopies()` returned `min(members, syncBackupCount + 1)` — no map name
anywhere, even though `write(...)` already had one and passed it to `QuorumGate.require`.

On a healthy 3-member cluster that record therefore hit
`QuorumGate.require(name, 2, effectiveFloor(3) = 3)` and every write to it was refused,
permanently. Reproduced before the fix, with the audit's exact message:

    DiskStore store demo.HighFloorEvent: write needs 3 durable copies, cluster can supply 2

## What changed

`durableCopies()` → `durableCopies(String mapName)`, reading the map's own live backup count:

```java
public int durableCopies(String mapName) {
    int members = hz.getCluster().getMembers().size();
    return Math.min(members, hz.getConfig().findMapConfig(mapName).getBackupCount() + 1);
}
```

The cluster-member cap is unchanged and still the outer bound — a partition cannot make replicas
out of members that are not there. `effectiveFloor`, `floorOverride`, the laptop override,
`QuorumGate`, `StratumMapConfig` and `HotRestore`'s ledger write path are all untouched.

### Why the live Hazelcast config, not `StratumEventStores.minDurableCopies(mapName)`

The prior doc suggested recomputing `max(syncBackupCount, minDurableCopies(mapName) - 1)` here.
That would be a second copy of a number `StratumMapConfig` already computed, free to drift from
it. The map's configured `backupCount` is the physical truth the put actually waits on, and it is
where `StratumMapConfig` wrote the decision. One source, read back.

### Why `findMapConfig`, verified empirically

Both APIs were probed against a live member (Hazelcast 5.5.0) with
`agentsensor.AgentEvent` explicitly configured at `backupCount=2`:

    cfg class = com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig
    agentsensor.AgentEvent -> name=agentsensor.AgentEvent backupCount=2 async=0
       findMapConfig -> name=agentsensor.AgentEvent backupCount=2

So the named entry really is read back, not the DEFAULT config — the question the task asked to
confirm, because `StratumMapConfig.durableMap` deliberately names every map rather than using a
wildcard. `findMapConfig` was chosen over `getMapConfig` because `getMapConfig` INSERTS a
default-derived config for a name it does not find — the probe's static config grew a
`not.Configured` entry purely from being asked about it. A lookup on the hot write path must not
mutate cluster config. `findMapConfig` returns a `MapConfigReadOnly` and adds nothing. Cost is
0.44 µs/call versus 0.02 µs (200k iterations), which is noise beside the `executeOnKey` it guards.

### The now-dead constructor parameter was removed

With `durableCopies` reading the map, the `syncBackupCount` field had no remaining reader. It was
removed rather than left in place: a stale second statement of a durability number inside the
class that enforces durability is the exact hazard this change exists to remove. `HotWrites(hz,
floorOverride)` — `EventStore.java:181` plus one argument dropped in each of nine test files, no
behaviour change at any call site (every one of them passed a count that the maps' own configs
already imply).

## Verification

- `mvn -o -pl server generate-sources -Dtacplus.skip=true` → BUILD SUCCESS.
- `mvn -o -pl server -DskipTests compile` → BUILD SUCCESS.
- New `hot/HotWritesPerMapDurabilityTest` — three in-JVM members on 127.0.0.1:5801-5803, which is
  the smallest cluster that can tell the two numbers apart (`durableCopies` is capped by the member
  count, so on one or two members the map's count and the deployment's collapse to the same cap):
  - `durableCopiesFollowTheMapsOwnBackupCount` — 3 for a `backupCount=2` map, 2 for a
    `backupCount=1` one, on the same instance.
  - `aWriteAboveTheDeploymentFloorIsAckedOnAClusterThatCanHoldIt` — the audit's symptom: the
    floor-3 write is acked.
  Both FAIL with the pre-fix body simulated (`min(members, 1 + 1)`, the deployment-wide count):
  `expected: <3> but was: <2>` and the `needs 3 durable copies, cluster can supply 2` refusal.
  Both pass with the change.
- `HotQuorumGateTest.durableCopiesAreCappedByTheVisibleCluster` rewritten for the new signature and
  given a `demo.HighFloor` map at `backupCount=2` in its config: on one member both maps still
  report 1, so the cluster cap is still asserted.
- `mvn -o -pl server -Dtest='HotWritesPerMapDurabilityTest,HotQuorumGateTest,HotRedoRestoreTest,
  HotRestoreDurabilityFloorTest,HotRestoreLedgerTest,StratumMapConfigDurabilityTest,
  AppendOnlyKeyGuardTest,TwoTenantQueryScopeE2ETest' test` → all pass (2, 4, 15, 3, 3, 10, 6, 11).

### Pre-existing failures, confirmed not caused by this change

The module's test phase still cannot compile `StratumEventStoresRetentionTest` and
`warm/WarmTierTest` (`tasks.TaskActivity` missing from generated sources — the known gap from the
prior doc). Those two files were moved aside to run anything, then moved back; only their
`new HotWrites` argument is edited in the committed tree.

With them aside, five E2E classes fail: 2 failures + 23 errors, nearly all
`PERMISSION_DENIED: the record query plane reads rows owned by a namespace and the caller
presented no verified namespace claim` (see
`2026-09-12-record-queries-are-pinned-to-the-opening-callers-namespace.md`). These are on the READ
plane, not the write gate. To prove it rather than assert it, HEAD (`6b5da3c`, without this
change) was checked out into a throwaway worktree with the generated sources copied in and the
same five classes run: **identical 2 failures / 23 errors, same tests**. Pre-existing, untouched
here, and still needing their own fix.

## Still open — NOT addressed here

Residual gap 2 of the prior doc stands unchanged: the shared `SplitBrainProtectionConfig` still
uses the deployment floor as its `minimumClusterSize`, so a record declaring more than the
deployment gets a proxy, and reads, on a cluster smaller than its own floor. That is a redesign of
the protection scheme (one config per distinct floor, a new naming scheme replacing the public
`SPLIT_BRAIN_PROTECTION_NAME`, and a matching change to `awaitProtectedClusterSize`), explicitly
out of scope for this task, and still wants scoping as its own item.
