# Per-record durability floor: the values are generated, the lookup is not

Date: 2026-09-12
Branch: `d3-stratum-c2s-split`
Origin: security audit 2026-09-12, High tier — "Durability floor is configured globally,
not per-record" (`decisions/security-audit-2026-09-12/CONSOLIDATED_REPORT.md`).

**Outcome: no code change. Stopped at a scope boundary (below), plus the server test module
does not compile on this branch for an unrelated, pre-existing reason.**

## The bug is real, and the mechanism is exact

`StratumMapConfig.build(seedMembers, minDurableCopies)` (server/src/.../hazelcast/StratumMapConfig.java:155-157)
loops `StratumEventStores.mapNames()` and calls `durableMap(name, minDurableCopies)` with one
deployment-wide number; `durableMap` sets `mapCfg.setBackupCount(minDurableCopies - 1)` (line 244).
No per-record value is consulted.

The write path, meanwhile, enforces the record's OWN declared floor. `HotWrites.effectiveFloor`
(server/src/.../hot/HotWrites.java:65-67) is `floorOverride > 0 ? floorOverride : declared`, and
`HotWrites.durableCopies()` is `min(clusterMembers, syncBackupCount + 1)`. With no
`STRATUM_MIN_DURABLE_COPIES` set — the documented production configuration — a record declaring
`(spec.min_durable_copies) = 3`:

- map `backupCount` = `DEFAULT_MIN_DURABLE_COPIES - 1` = 1 → `durableCopies()` = min(3, 2) = **2**
- `QuorumGate.require(mapName, 2, effectiveFloor(3) = 3)` → **every write rejected, permanently**,
  on an otherwise healthy 3-node cluster.

The audit's claim holds. Note the bug bites only when no override is set: with an override,
`effectiveFloor` returns the override for every record and `backupCount` is derived from that same
override, so the two agree.

Today no record actually declares more than 2, so nothing is failing in production right now — it
fails the moment anyone writes `(spec.min_durable_copies) = 3` on a field, which is a supported,
documented, tested-in-codegen declaration (`EventStoreHotWriteTest:91-94`).

## Scope boundary: codegen emits the values but no name-keyed accessor

Per-record floors ARE threaded through to generated output, as one public constant per record in
`server/target/generated-sources/stratum-codegen/.../StratumEventStores.java:33-48`:

```java
public static final int MIN_DURABLE_COPIES_AGENTSENSOR_AGENT_EVENT = 2;
```

emitted by `EventStoreGenerator` lines 75-82, named by `floorConst()` (line 201-205), from
`EventTopicDiscovery.TopicEvent.minDurableCopies` (the strictest `(spec.min_durable_copies)` on any
field, defaulting to 2 — `EventTopicDiscovery:451-462`).

What does NOT exist anywhere in generated output is a lookup **keyed by Hazelcast map name**.
`StratumEventStores.mapNames()` returns names; the floors are separately-named constants. There is
no `minDurableCopies(String mapName)`, no map, no enum. Verified by grepping all of
`server/target/generated-sources/` for `durable` — the only hits are those eight constants and
their eight use sites inside `store()`.

`StratumHotStores.datasets()` does carry `(mapName, minDurableCopies)` pairs on `HotDataset`, but
only for records carrying `(spec.store_epoch)` — 1 of the 8 maps on this branch. Using it would
silently leave 7 maps on the global value.

So `StratumMapConfig` cannot consume the per-record floor without one of:

1. **A codegen addition** (the correct fix). ~8 lines in `EventStoreGenerator`, beside the existing
   constant emission:

   ```java
   w.println("    /** The declared durability floor for a map name, or 0 if unknown. */");
   w.println("    public static int minDurableCopies(String mapName) {");
   w.println("        switch (mapName) {");
   for (EventTopicDiscovery.TopicEvent e : events) {
       w.println("            case \"" + mapName(e) + "\": return " + floorConst(e) + ";");
   }
   w.println("            default: return 0;");
   w.println("        }");
   w.println("    }");
   ```

   `StratumMapConfig.build` then becomes, per map:
   `cfg.addMapConfig(durableMap(name, Math.max(minDurableCopies, StratumEventStores.minDurableCopies(name))));`
   with `durableMap`'s parameter renamed to the effective floor. The shared
   `SplitBrainProtectionConfig` stays on the deployment number untouched (per the task's rule 4);
   that is correct — protection is about cluster membership, backups are about copies.

2. **Hand-rolling a map-name → floor table in `StratumMapConfig`**, or reflecting on the constant
   names by re-deriving `floorConst()`'s snake-case transformation. Both duplicate what the
   generator owns, and option 2b is a hack. Rejected.

Option 1 is a codegen change, which the task for this fix explicitly excludes ("don't hand-roll a
codegen change as part of this fix — flag it as a follow-up instead"). The task's stated diff scope
(`StratumMapConfig.java` plus tests, nothing else) confirms the exclusion. **Default proceeded on:
stop, change nothing, flag the codegen addition above as the follow-up.** It is small, fully
specified here, and the `stratum-codegen` plugin runs at `generate-sources` during an ordinary
`mvn test` (server/pom.xml:126-138) — `-Dtacplus.skip=true` does not suppress it — so the
regenerated accessor would be available to the server build immediately.

### One semantic subtlety for whoever does the follow-up

`STRATUM_MIN_DURABLE_COPIES` is documented as an **override**, not a minimum: `HotWrites`
deliberately lets a laptop declare 1 and have it replace each record's declared floor. Using
`max(deployment, declared)` for `backupCount` therefore raises `backupCount` above a 1-override
(to `max(1,2)-1 = 1`). That is harmless — on a one-member cluster Hazelcast simply places no
backup, and `durableCopies()` = min(1, 2) = 1 still clears `effectiveFloor(2)` = 1 — so the
laptop case keeps working. Flagging it because `max()` and the override's documented semantics
point in opposite directions, and the reason they don't collide is not obvious.

## Second blocker: server tests do not compile on this branch (pre-existing)

`mvn -o -pl server -Dtacplus.skip=true test -Dtest=StratumMapConfigDurabilityTest` fails in
`default-testCompile`, on a **pristine tree** (`git status --short` showed only `?? decisions/`
before any work). It is not caused by this task and was not introduced here:

```
[ERROR] StratumEventStoresRetentionTest.java:[15,104] package ...model.types.services.tasks does not exist
[ERROR] StratumEventStoresRetentionTest.java:[74,31] cannot find symbol: method tasksTaskActivity()
[ERROR] WarmTierTest.java:[124,60] package ...model.types.services.tasks.TasksOuterClass does not exist
```

`StratumEventStoresRetentionTest` and `WarmTierTest` reference a `tasks.TaskActivity` record that
the model module no longer generates: `model/target/generated-sources/.../types/services/` has 20
vservices and no `tasks`. The model is generated by tacplus from a remote proto repo
(`tacplus.yaml`: `src: https://gitlab.com/secondstarcavalier/schematic/platform/proto.git`), so the
offline build uses whatever the local cache holds. Confirmed this is not a stale-artifact problem
on my side: rebuilding and installing `model` (both with and without `-Dtacplus.skip=true`,
`mvn -o -pl model -DskipTests install`, exit 0) still produces no `tasks` package, and the server
test compile fails identically afterwards.

Consequence: **no server test can be run on this branch in this environment**, so neither the
existing `StratumMapConfigDurabilityTest` nor any new test for this fix can be verified here. That
needs resolving (refresh the proto cache online, or `.off` the `tasks` references in those two
tests if the vservice was genuinely removed) before the follow-up fix above can be verified.

## Open question for the manager

Authorize the `EventStoreGenerator` accessor (option 1)? It is the only non-duplicating route to
this fix, and the fix cannot be done in `StratumMapConfig.java` alone. If yes, the resulting diff
touches `codegen/.../EventStoreGenerator.java`, a codegen test, `StratumMapConfig.java`, and
`StratumMapConfigDurabilityTest.java` — wider than the scope this task was given, which is why it
is being asked rather than assumed.
