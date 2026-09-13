# EventStoreGenerator: add `minDurableCopies(String mapName)` lookup

Date: 2026-09-13
Branch: `d3-stratum-c2s-split`
Follow-up to: `2026-09-12-per-record-durability-floor-needs-a-generated-lookup.md` (read that first —
it has the full bug analysis, the exact scope boundary, and the manager authorization question
this closes out).

## What landed

`EventStoreGenerator.generate()` now emits, right after `mapNames()`:

```java
public static int minDurableCopies(String mapName) {
    switch (mapName) {
        case "mailbox.MailboxEntry": return MIN_DURABLE_COPIES_MAILBOX_MAILBOX_ENTRY;
        ...
        default: return 0;
    }
}
```

One `case` per `EventTopicDiscovery.TopicEvent`, delegating to the existing per-record
`floorConst()` constant — no new data, just a name-keyed accessor over what was already emitted.

New test `minDurableCopiesIsKeyedByMapName` in `EventStoreHotWriteTest` proves the switch, one
matched case, and the `default: return 0` fallback. `mvn -o -pl codegen -Dtacplus.skip=true test`:
6/6 pass. Real-fleet regen (`mvn -o -pl server generate-sources -Dtacplus.skip=true`) confirmed the
accessor emits correctly for all 8 real record types in `StratumEventStores.java`.

## Scope

This is the generator addition only — option 1 from the prior doc, authorized by this looper
(tacplus) as the manager decision the prior doc's "Open question for the manager" asked for. It is
a small, fully-specified, non-duplicating accessor over data the generator already owns; nothing
about it needed a human call (not destructive, not outward-facing, no spend, no credentials).

**Not done here, per the prior doc's own division of labor**: the `StratumMapConfig.build`
consumer change (`Math.max(deploymentFloor, StratumEventStores.minDurableCopies(name))` for
`backupCount`) and its test (`StratumMapConfigDurabilityTest`, does not exist yet). Schematic's
inbox message that requested this generator addition said it would wire that consumer itself once
this landed — pinged back via tacplus's reply to schematic's inbox.

The pre-existing, unrelated server-test-compile break (`tasks.TaskActivity` missing from the model
module's generated sources — see the prior doc's "Second blocker") was not touched and still blocks
running `server`'s test module in this environment; not this task's scope to fix.
