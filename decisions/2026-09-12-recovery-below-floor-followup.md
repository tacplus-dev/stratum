# Follow-up: recovery below the durability floor is now a refusal

Context: security audit 2026-09-12, High finding "Recovery below the durability floor is logged,
not rejected". Fix applied in `HotRestore.awaitClusterReplay`
(`server/src/main/java/.../hot/HotRestore.java`) plus a new test class
`server/src/test/java/.../hot/HotRestoreDurabilityFloorTest.java`. Branch `d3-stratum-c2s-split`.

## What was asked vs. what was written

The assignment said: throw when `contributors < minDurableCopies`.

What was written throws when `contributors > 0 && contributors < minDurableCopies`.

## Why the `contributors > 0` carve-out (the one deviation)

`contributors == 0` means **no live instance had a redo log at all**. Two ways to get there:

1. A first-ever deployment. Nothing has ever been written, so no volume holds a log.
2. A total loss of every volume in the cluster.

Neither is the failure the finding is about. The finding is about a *partial* recovery — some
instances came back with their logs and fewer of them than the declared floor, so the assembled
store can be missing rows whose only surviving copies were on the instances that contributed
nothing. With zero contributors nothing was replayed thinly; nothing was replayed at all, and the
hot tier is *empty*, not under-durable. Its recovery path is the event-log cold-start rebuild,
which `EventStore.hotReadable()` already waits for as a separate leg of the read gate.

A literal `contributors < minDurableCopies` throw would make **every first-ever bring-up
crash-loop**: `EventStore.init` calls `restoreHotTier` unconditionally on a cluster with no data,
`contributors` is 0, `minDurableCopies` is >= 1 by construction, and init would throw forever.

This is not speculative — the pre-existing test
`HotRestoreLedgerTest.staleEntriesDoNotCountAsDurabilityContributors` (line ~98) calls
`awaitClusterReplay(hz, 0, 1, ...)` and asserts a normal return of 0. That test encodes the
no-log-anywhere case as legitimate, and a literal throw breaks it.

**Default proceeded on:** the carve-out, documented in a block comment at the check and in the
`@throws` javadoc, and pinned by
`HotRestoreDurabilityFloorTest.noContributorsAnywhereIsAnEmptyStoreNotAnUnderDurableOne`.

**Open question for the reviewer, if they disagree:** is a *total volume loss* (case 2) something
this method should refuse rather than delegate to the cold-start rebuild? Refusing it would need a
way to distinguish "first deployment" from "we had data and lost all of it" — stratum has no such
marker today (the ledger is per-boot, keyed by a UUID minted at restart). Adding one is a separate
change, not this fix.

## Explicitly not done (per the assignment)

Nothing per-dataset. `restoreHotTier` still sums `read` across all datasets and calls
`awaitClusterReplay` once with one aggregate `minDurableCopies`. That aggregation is its own filed
finding, blocked on a tacplus codegen gap
(`decisions/2026-09-12-per-record-durability-floor-needs-a-generated-lookup.md`,
`tacplus/looper/inbox.md`). `restoreHotTier`'s loop and `minDurableCopies`'s derivation are
untouched.

## Verification actually run

Two pre-existing test files do not compile against current generated sources
(`StratumEventStoresRetentionTest.java`, `warm/WarmTierTest.java` — the known, unrelated
`TasksOuterClass` / `tasksTaskActivity()` generation gap). They were moved out of the tree for the
runs below and restored byte-for-byte afterwards; `git status` and `git diff` show no change to
either.

- `mvn -o -pl server -Dtacplus.skip=true -Dtest=HotRestoreDurabilityFloorTest test`
  → **Tests run: 3, Failures: 0, Errors: 0, Skipped: 0. BUILD SUCCESS.**
- Same test class against the *unmodified* `HotRestore.java` (`git show HEAD:...`)
  → **Tests run: 3, Failures: 1** — the new test genuinely catches the bug rather than passing
  vacuously.
- Full server suite, `mvn -o -pl server -Dtacplus.skip=true test`
  → **Tests run: 108, Failures: 2, Errors: 23.** All 25 are pre-existing E2E failures in
  `WarmAccountingRecordE2ETest` (5 errors), `WarmMutableSessionStateE2ETest` (1F/4E),
  `PullAgentEventE2ETest` (12 errors), `WarmEvictionE2ETest` (1 error), `HotAgentEventE2ETest`
  (1F/1E). Confirmed pre-existing by re-running those five classes against the unmodified
  `HotRestore.java`: identical 2 failures / 23 errors.
- Every hot-tier and restore-adjacent class passes clean: `HotRedoRestoreTest` 13/13,
  `HotRestoreLedgerTest` 3/3, `HotRedoBackupTimingTest` 2/2, `HotQuorumGateTest` 4/4,
  `ColdStartReadGateTest` 7/7, `ColdStartRebuilderTest` 3/3, `StratumMapConfigDurabilityTest` 6/6,
  `ConfigReloadSerializationTest` 4/4.

No generated code was touched; no tacplus plumbing was hand-rolled.
