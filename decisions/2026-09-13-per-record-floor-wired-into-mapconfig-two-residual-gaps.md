# Per-record durability floor: wired into StratumMapConfig, and two residual gaps it does not close

Date: 2026-09-13
Branch: `d3-stratum-c2s-split`
Closes the consumer half of `2026-09-12-per-record-durability-floor-needs-a-generated-lookup.md`
(read that first — full bug analysis). Generator half landed at `stratum@e4cd0a2`
(`2026-09-13-eventstoregenerator-mindurablecopies-lookup.md`).

## What landed

`StratumMapConfig.build` now sizes each record map's `backupCount` from
`Math.max(deploymentFloor, StratumEventStores.minDurableCopies(name))` instead of the
deployment number alone, consuming the generated name-keyed accessor added for exactly this.
`durableMap`'s parameter is renamed to `effectiveMinDurableCopies` to say which number it is.
The restore ledger keeps the plain deployment floor — it is not a record map and the generated
lookup returns 0 for it, asserted in the test.

Tests in `StratumMapConfigDurabilityTest` (the class already existed, contrary to the task's
assumption): a record declaring above the deployment floor gets its own higher `backupCount`;
a record at or below it is unaffected; the ledger always takes the plain deployment floor. All
expectations are derived from `StratumEventStores.minDurableCopies(name)` rather than hardcoded,
so a record changing its declaration moves the assertions with it.

### One existing test's expectation changed, deliberately

`theSingleNodeFloorKeepsNoBackup` asserted `backupCount == 0` on the hot map at a deployment
floor of 1. That is now 1, because every generated record declares 2 and `max()` no longer lets
the deployment number lower a record map below what the record asks for. This is the semantic
collision the prior doc flagged in advance: `STRATUM_MIN_DURABLE_COPIES` is documented as an
OVERRIDE, and `max()` treats it as a minimum. The laptop case still works — on a one-member
cluster Hazelcast has nowhere to place a backup and does not, `durableCopies()` is
`min(1, 2) = 1`, and `effectiveFloor` is still the override 1 — so the override still governs
the write gate; it just no longer shrinks a record map's configured backup count. The test was
renamed `theSingleNodeFloorKeepsNoBackupOnTheLedger` and now asserts both halves.

## Residual gap 1 (BLOCKING — the audit finding is NOT closed by this change alone)

**`HotWrites` counts backups from the deployment floor, so the raised `backupCount` is never
counted and the write is still rejected.**

`EventStore.init:181` constructs `new HotWrites(hz, minDurableCopies - 1, floorOverride)` with
one deployment-wide `syncBackupCount`, and `HotWrites.durableCopies()` is
`min(clusterMembers, syncBackupCount + 1)` — no map name, no per-record value, even though
`HotWrites.write()` already receives `mapName` and the record's declared `minDurableCopies` and
passes both to `QuorumGate.require`.

So for a record declaring 3 against a deployment default of 2, on a healthy 3-node cluster,
AFTER this change:

- map `backupCount` = `max(2, 3) - 1` = **2** (fixed here — Hazelcast really does place 3 copies)
- `durableCopies()` = `min(3, (2 - 1) + 1)` = **2** (still the deployment number)
- `QuorumGate.require(name, 2, effectiveFloor(3) = 3)` → **write still rejected, permanently**

The original audit symptom therefore survives this change. What this change fixes is that the
copies now physically exist; what still fails is the gate's count of them.

The fix is the same `max()` one line lower, in `HotWrites`: make `durableCopies` take the map
name and use `min(members, max(syncBackupCount, StratumEventStores.minDurableCopies(mapName) - 1) + 1)`
— or, equivalently, pass the per-map effective floor down from `EventStore` the way
`StratumMapConfig` now computes it. `durableCopies()` has exactly one production call site
(`HotWrites:97`) and two test call sites (`HotQuorumGateTest:102-103`), so it is a contained
change, ~5 lines plus a test.

**Not done here.** The task scoped the diff explicitly to `StratumMapConfig.java` plus tests plus
this doc, and this is a different file in a different package. Flagging rather than assuming, per
that scope. **Manager: this needs authorizing as a follow-up, or the landmine stays armed** —
the map config is now correct and the write path still refuses to believe it.

## Residual gap 2 (non-blocking, genuinely out of scope for the audit finding)

**The shared `SplitBrainProtectionConfig` still uses the deployment floor as its
`minimumClusterSize`, so a record declaring more opens on a smaller cluster than its own floor
needs.**

`SPLIT_BRAIN_PROTECTION_NAME` is one enabled protection created once in `build()` at
`minimumClusterSize = minDurableCopies`, referenced by name from every map. A record declaring 3
against a deployment floor of 2 therefore gets a map that Hazelcast will hand out a proxy for,
and serve READS from, on a 2-member cluster — fewer members than that record's own declared
durability requires.

Judged out of scope for this finding, but real and worth recording:

- The audit finding was about `backupCount` and permanent write rejection. The write path is
  unaffected by this: `durableCopies()` is capped by the live member count, so a 2-member
  cluster rejects a floor-3 write at `QuorumGate` regardless of what protection allows. The
  exposure is reads (and proxy creation) on an under-sized cluster, which is a different
  property from the one audited.
- Fixing it is NOT a one-liner like gap 1. It means one protection config per distinct floor
  value rather than one shared object — a new naming scheme replacing the public constant
  `SPLIT_BRAIN_PROTECTION_NAME`, which four existing assertions in
  `StratumMapConfigDurabilityTest` reference by identity, and a matching change to
  `awaitProtectedClusterSize` (which likewise waits for the deployment floor only, and would
  have to wait for the highest declared floor instead). That is a redesign of the protection
  scheme, and the task said to flag rather than scope-creep into one.

Recommend treating it as a new, separate audit finding rather than folding it into this one.

## Verification

- `mvn -o -pl server generate-sources -Dtacplus.skip=true` → BUILD SUCCESS, accessor emitted for
  all 8 record types.
- `mvn -o -pl server -DskipTests compile` → BUILD SUCCESS (the main change compiles).
- The pre-existing server test-compile break from the prior doc is STILL PRESENT and unfixed
  (`StratumEventStoresRetentionTest` / `WarmTierTest` reference `tasks.TaskActivity`, which the
  model module no longer generates — 8 errors, none in files touched here), so
  `mvn -pl server -Dtest=...` cannot run. The two affected classes were compiled and run directly
  against the module's own classpath instead: `StratumMapConfigDurabilityTest` (10 tests) and
  `StartupHealthGateTest` (1 test), 11/11 pass. That break still needs its own fix before this
  module's test phase runs in this environment.
