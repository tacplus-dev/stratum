# Stale restore-ledger entries + premature read-gate opening

Date: 2026-09-12
Branch: `d3-stratum-c2s-split`
Source: `decisions/security-audit-2026-09-12/CONSOLIDATED_REPORT.md`, High tier —
"The restore ledger never prunes departed members" and "The read gate opens before
cold-start replay begins, not after it completes".

Both are siblings of the whole-namespace-restart incident: a stale or wrong set of
members satisfies a readiness barrier, and reads open on a store that is silently
incomplete.

## Finding 1 — the restore ledger counted entries from members that had departed

`HotRestore.awaitClusterReplay` broke its wait on `ledger.size()`. The ledger is
keyed by Hazelcast member UUID, which is minted fresh on every restart, so an
entry outlives the pod that filed it and the raw size answers "how many reports
have ever been filed?" rather than "how many of the instances that are here now
have replayed?".

Fixed by counting only reports whose key is in the current cluster view
(`hz.getCluster().getMembers()`), recomputed on every pass of the wait, and by
counting the durability contributors over that same filtered set — otherwise a
dead pod's leftover could still talk the store up to `min_durable_copies`.

### Decision: check-time filtering, not a `MembershipListener`

The report offered both. Filtering at check time is the mechanism; pruning is
housekeeping that happens on the way past.

A `MembershipListener` evicting on `memberRemoved` was rejected because it does
not remove the need for the filter. The listener races the barrier — the check can
run between a member leaving and the event being delivered — so a correct
implementation would need the check-time filter anyway, and would additionally
have to be registered, unregistered and reasoned about across the EventStore's
init / shutdown / config-reload lifecycle. One mechanism that is sufficient on its
own beats two that are each necessary. The cost of filtering is a cluster-view
read per pass, which is a local field read.

The ledger is still pruned, because without it the map grows by one dead entry per
pod per restart for the life of the cluster. That is a side effect of the filter
pass, not the thing correctness rests on.

### Consequence for existing tests (deliberate, please review)

`HotRedoRestoreTest` simulated a peer with `peerReported(hz, "peer-a", n)` — a
ledger entry under an invented UUID, because its `member()` helper disables both
join mechanisms and two such instances cannot form one cluster.

That simulation is exactly the stale entry the fix must ignore, so three tests went
red. They were **not** "fixed" by weakening the barrier. `peerReported` now brings
up a real second member over loopback TCP (`joinedMember` / `twoMemberCluster`) and
files the report under the UUID the cluster actually sees it by. The three tests
assert the same things as before against a faithful two-member cluster.

`theFirstPodUpDoesNotPassTheBarrierAlone` still uses the single-member `member()`
helper and is unchanged.

## Finding 2 — the read gate opened on the redo barrier alone

`hotReadable` was one flag, set the moment `awaitClusterReplay` returned.
`ColdStartRebuilder`'s event-log replay — the recovery path for record types with
no redo log — is started asynchronously by `StratumHandler` *after*
`eventStore.init` has already returned and already set that flag. Queries for
those types were answered out of a store that was still filling, and an empty
result is indistinguishable to a client from "there is no such data".

Fixed by splitting the gate into two legs that are ANDed:

- `redoReplayDone` — set by `restoreHotTier`, as before.
- `coldStartReplayDone` — set through the new public `coldStartReplayComplete()`.

`requireHotReadable()` names which leg is closed, so the two causes are
distinguishable in an error.

The caller sets the second leg, because only the caller knows whether a replay was
needed. `StratumHandler` either starts a rebuild that signals on catch-up, or
declares immediately that none was required. **Every branch must end by telling the
store**, since the gate is closed until told — a branch that says nothing leaves
reads refused forever. That includes the warm-start branch and the
already-completed-before-a-reload branch.

### Decision: a watermark, not a quiescence heuristic

"Cold-start replay has completed" has no natural definition: the generated
subscriber opens a live stream that never ends, and keeps consuming new events
after the backlog is drained.

The completion signal is therefore the **Kafka high watermark**, read once per
topic through the generated `eventbus` `AdminGrpc.getTopicInfo`, whose
`latest_offsets` map is populated from `OffsetSpec.latest()` (verified in
`platform/services/eventbus/.../kafka/TopicManager.java:131-161`). That is the
offset the next write will take, so the last record already in the log sits at
`latest - 1`, and delivering it is the exact moment the backlog is drained.
`ReplayProgress` tracks this per partition.

Idle-based detection ("no records for N seconds") was rejected as a hack: on a slow
broker it opens the gate early, which is the bug being fixed, and on a busy topic
it never opens at all.

The watermark is captured **before** subscribing, so a topic taking writes
throughout the replay does not move its own finish line.

### No-deadlock cases

The requirement that the gate must not wait forever on a rebuild that was never
going to run is met at three points:

- No cold-start replay needed (`shouldReplay` false, the common warm start) —
  `StratumHandler` signals the leg immediately; the gate opens on the redo barrier
  alone, as promptly as before.
- Topic does not exist — `getTopicInfo` returns `NOT_FOUND`, treated as "nothing
  was ever written, nothing to replay", and the leg is signalled.
- Topic exists but is empty — every partition reports `latest == 0`, nothing is
  registered as outstanding, and the drained backlog is recognised from the
  watermark itself rather than waiting for a record that will never arrive.

## Open question for review — watermark probe failure keeps the gate CLOSED

If `getTopicInfo` fails with anything other than `NOT_FOUND` (`UNAVAILABLE`,
`PERMISSION_DENIED`), the probe retries indefinitely with capped backoff and the
read gate stays closed while it does.

**Default proceeded on:** stay closed and stay loud.

Reasoning: this path runs only on a cold boot, where the store is empty
cluster-wide. A store that cannot establish how much it has to recover genuinely
cannot answer a query, so refusing matches `awaitClusterReplay`, which throws
rather than serving a silently incomplete store. A transient eventbus outage then
delays the gate and resolves itself rather than opening it on an unrecovered store.

**What a reviewer should decide:** whether stratum is actually authorised to call
the eventbus `Admin` service S2S. It already calls `AgentEventBusStateService` for
the subscription, but `Admin` is a different service on the same target and I could
not verify the authz policy from this repo. If stratum is not authorised, the probe
will retry forever on `PERMISSION_DENIED` and a cold boot will never open its read
gate — availability failure, not a data-correctness one, and loud in the logs, but
it would need either an authz grant or a different completion signal.

## Not touched

`StratumMapConfig.java` — flagged as possibly being edited concurrently by a
sibling for a different High-tier finding. It was clean when I started (`git
status` showed only an untracked `decisions/`) and it is **not** in my diff.

The other High-tier findings in the same area are deliberately out of scope, in
particular "event-log cold-start rebuild is hardcoded to one topic". The read gate
is built per topic (`ReplayProgress` is keyed by topic and partition), so widening
the rebuild to every topic will extend the gate rather than having to rework it.

## Pre-existing breakage found (NOT caused by this work, NOT fixed here)

`mvn -o -pl server -Dtacplus.skip=true test` does not compile its tests at HEAD on
this branch, independently of this change:

    StratumEventStoresRetentionTest.java: package ...model.types.services.tasks does not exist
    WarmTierTest.java:                    package ...services.tasks.TasksOuterClass does not exist

The `tasks` vservice is absent from the generated model (the nearest thing present
is `work`), so those two test files reference a type the generator no longer
produces. Verified pre-existing by reverting this entire change and re-running:
identical errors.

To get a real signal from the rest of the suite I moved those two files aside for
the duration of the run and **restored them afterwards** — they are untouched in
the final tree. Numbers below are from runs with them aside.

## Verification

    Baseline (this change fully reverted):  Tests run: 91, Failures: 2, Errors: 23
    With this change:                       Tests run: 101, Failures: 2, Errors: 23

+10 tests, all of them new here, and the failure and error counts are unchanged —
per-class as well as in total. The remaining 2 failures / 23 errors are
pre-existing and are namespace-claim errors from the C2S split
(`PERMISSION_DENIED: ... the caller presented no verified namespace claim`) in
`WarmAccountingRecordE2ETest`, `WarmMutableSessionStateE2ETest`,
`PullAgentEventE2ETest`, `WarmEvictionE2ETest` and `HotAgentEventE2ETest`.

Targeted, run individually:

    HotRestoreLedgerTest   Tests run: 3,  Failures: 0, Errors: 0
    ColdStartReadGateTest  Tests run: 7,  Failures: 0, Errors: 0
    HotRedoRestoreTest     Tests run: 13, Failures: 0, Errors: 0

`HotRestoreLedgerTest` was confirmed non-vacuous: against the unfixed
`HotRestore.java` all 3 fail (the barrier passes on two dead entries, and the
contributor count is inflated by a departed member's report).
