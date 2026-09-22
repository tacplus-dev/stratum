# stratum-0/stratum-2 crash-loop: a serving peer's ledger report destroyed by the restart it was supposed to survive

Investigated 2026-09-22 (~16:15-17:30 MDT). The reported diagnosis — two pods on 5-minute
clocks missing each other's ledger reports — is **refuted**, with a test that keeps the
refutation. The real cause is a different one in the same barrier, and unlike a timing race it
explains why nothing ever cleared: the state the barrier waits on had been **destroyed**, and
nothing in the system could ever write it again.

Code changed in `StratumMapConfig`, `HotRestore`, `EventStore`; new test
`HotRestoreLostReportDeadlockTest`; three assertions in `StratumMapConfigDurabilityTest` updated
deliberately (§6). Committed locally, **not deployed** — no kubectl was run for this
investigation at all.

## 1. What was observed

```
stratum-0   0/1   CrashLoopBackOff   14 restarts   ~85m
stratum-1   1/1   Running             0 restarts    11h
stratum-2   0/1   CrashLoopBackOff   14 restarts   ~85m
```

stratum-0 and stratum-2 both sat on nodes GKE recreated by auto-repair at ~14:43 MDT and had
been failing to rejoin ever since. `min_durable_copies` is 2 (unset in the StatefulSet,
`StratumMapConfig.DEFAULT_MIN_DURABLE_COPIES`). stratum-1 alone stayed 1/1 and serving
throughout, so this was reduced redundancy, not an outage.

A live `kubectl scale sts stratum --replicas=2` mitigation was reverted within ~2 minutes by
`schematic-operator`, which reconciles the StatefulSet's replica count. No further live
mitigation was attempted and none is needed for the fix below.

**No pod logs were preserved**, so this investigation is built on the code and on real-Hazelcast
experiments rather than on a stack trace. Where that limits the conclusion it is said so (§3).

## 2. The reported diagnosis, and why it is wrong

The incident was first read as a mutual timing deadlock: `expected = max(live cluster size,
min_durable_copies)` reaches 3 once both pods join Hazelcast, so each needs the OTHER's ledger
report, and their 5-minute timeouts plus CrashLoopBackOff were landing out of phase.

Three real Hazelcast members say otherwise.

**A. Two pods restarting together into a serving third both pass, every time.** Each member
files its ledger entry on ENTRY to `awaitClusterReplay`, before it waits — so the third report
lands while both are still waiting for it. First seen in a throwaway probe, then kept as
`twoPodsRestartingTogetherRecoverWhileTheServingPeerKeepsItsReport`:

```
PROBE-A after restart, cluster size 3 ledger=[...3 entries...]
PROBE-A RESULT zero=OK contributors=3 two=OK contributors=3
```

**B. Anti-phase does not deadlock either.** If the pods genuinely never overlap, then while one
is in its barrier the other is not a cluster member, `expected` falls back to
`min_durable_copies` = 2, and `{self, stratum-1}` satisfies it. The out-of-phase case is the
case that *works*.

So the timing story cannot produce a self-perpetuating loop in any phase, which is exactly what
14 restarts over 85 minutes rules out. Something had to be *missing*, not merely *late*.

## 3. Root cause: the barrier waits on a fact that can be destroyed and can never be re-written

Two properties of `HotRestore` combine into a permanent wedge.

**The barrier needs a report from every live member.** `awaitClusterReplay` waits for
`liveReports(hz, ledger).size() >= expected`, where `expected` is at least the current cluster
size. A live member with no ledger entry is a member the barrier waits for forever.

**A member files its report exactly once, at its own boot, and nothing can ever write it
again.** `LEDGER_MAP` is documented as a boot snapshot and `396d952` deliberately settled that a
serving peer counts on the strength of the barrier it already passed rather than on its number
being current. What was not noticed is the corollary: a **serving** member has no occasion to
file a report at all, so if its entry goes away it stays away for as long as that member keeps
serving — and the member is serving precisely because it is *not* restarting.

**And the entry is destroyed by exactly the event the barrier exists to survive.** The ledger
was configured with the record maps' backup count, `min_durable_copies - 1` = **one backup**.
On three members each entry therefore lives on two of the three, so losing two members *at the
same instant* destroys outright every entry whose owner and backup were both among them. That
is not a tail risk; it is a third of the map. Measured with the production config, 300 keys,
`terminate()` on two of three:

```
before: 300
after 2-of-3 terminate: surviving entries=188 of 300  LOST=112     (37%)
```

`101 of 300` on a second run. The geometry says why: of the six ordered owner/backup pairs three
members allow, two lie wholly inside the pair that died.

Put together, this is the incident. When both nodes were recreated at 14:43, there was roughly a
one-in-three chance that stratum-1's ledger entry was destroyed with them. It was, and stratum-1
— serving, eleven hours up, never restarting — could not file another. From then on the barrier
demanded three reports and only two members were able to produce any, so **every** restart of
**either** pod timed out, in every phase, forever. The message the pods would have exited on —
taken from a live three-member reproduction, whose only difference from production is the
timeout the test drives it at:

```
hot restore: only 2 of 3 expected instances reported their redo replay within PT8S
(cluster size 3, min_durable_copies 2); refusing to serve a silently incomplete hot store
```

and with only one of them up, the same wedge one lower:

```
hot restore: only 1 of 2 expected instances reported their redo replay within PT8S
```

Reproduced end to end in `aServingPeerPutsBackAReportThatWentMissingAndUnwedgesTheBarrier`.

**What is not provable without the logs.** That stratum-1's entry was specifically the one
destroyed, rather than pruned by `liveReports` during a brief cluster view that was missing it
(the second route to the same state; §4.2 is the part of the fix aimed at it). Both routes end in "a serving member has no
ledger entry", both are permanent for the same reason, and the fix closes both. What IS proven
is that the state is destroyable at a third per double-member loss, that it can never come back
on its own, and that the resulting barrier is unsatisfiable in every phase — which is the only
hypothesis examined here that matches an 85-minute loop that never once cleared.

## 4. The fix

Two changes. The first stops the loss; the second stops any loss from being permanent. Neither
touches `min_durable_copies`, the contributor rule, or the zero-contributors carve-out.

### 4.1 The barrier's own maps are replicated to every member

`StratumMapConfig.coordinationMap` now gives `LEDGER_MAP` and `INFLIGHT_MAP`
`MapConfig.MAX_BACKUP_COUNT` instead of `min_durable_copies - 1`, keeping the record maps'
split-brain protection unchanged.

The record maps' backup count is a **durability** statement: a write is acked once that many
members hold it. These two maps hold no records. They hold the barrier's knowledge of which
members have recovered, and the event that knowledge has to survive is precisely the one the
record maps' number is chosen against — several members dying at once. Using the durability
number for it was a category error, visible in the original comment's own words ("it carries the
same backup count ... as the record maps").

`MAX_BACKUP_COUNT` is Hazelcast's ceiling of 6 and is a maximum, not a requirement: a cluster of
three places two backups. So no cluster size this deployment can reach leaves a coordination
entry on fewer members than are alive to hold it, and in particular **a member's own entries now
last as long as the member does**: destroying them takes killing every member that holds a copy,
which includes the member the entries are about.

It is affordable here and nowhere else: one entry per member, written twice per boot, against
maps that hold the store.

The in-flight marks get the same treatment and need it more, because they fail in the worse
direction. A lost ledger report makes a serving member invisible and wedges the barrier — loud.
A lost in-flight mark makes a member that is *still inside its own restore* read as one that has
finished, which is the silently-incomplete store the barrier exists to refuse.

### 4.2 A serving member puts its report back if it goes missing

`HotRestore.refileReport(hz, durableRecords)` does a `putIfAbsent` of this member's own ledger
entry. `EventStore` calls it from the existing cleaner thread every 30 seconds
(`REPORT_REPAIR_SECONDS`), after the barrier and never before it.

`putIfAbsent`, never `put`: this replaces nothing. The entry is a boot snapshot by design and
`396d952` settled that; a repair that revised the number would be re-deciding it. `EventStore`
keeps the number it filed (`bootDurableRecords`) rather than recomputing one, for the same
reason.

30 seconds is well inside the 5-minute `RESTORE_TIMEOUT`, deliberately: a report that comes back
within a fraction of a peer's wait costs that peer a slower start and nothing else. The
alternative — a report that never comes back — cost this incident every restart of every other
pod, permanently.

## 5. Alternatives rejected

**Weaken the barrier when several members are absent at once** (the brief's own suggestion:
treat "the only other non-self member is also mid-restore" as not needing to clear the full
barrier). Rejected, and it would not have helped: with the serving peer's report intact this
case already passes (§2A), and with it destroyed the pods would have been credited a durability
nobody had. That is the exact silent-incomplete-store failure `396d952` and the checkpoint-quorum
incident were about.

**Count every live member as a contributor, dropping the ledger requirement.** Rejected: a
member that has joined Hazelcast but has not yet reached the barrier is live, has no report, and
has no in-flight mark. Counting it would let a pod open hot reads before its peer had replayed
anything.

**Publish a positive "finished" marker (`INFLIGHT` = FALSE on completion) and account live
members off that instead of off the ledger.** Rejected as machinery that buys nothing: the
marker is keyed by the same member UUID as the ledger entry, so Hazelcast puts it in the **same
partition**, and both `liveReports` and `liveInflight` prune on the same predicate. Every way
the report is lost loses the marker with it. It would have added a state to the lifecycle map
and closed no hole.

**Stop pruning `liveReports` so a wrongly-pruned entry cannot be lost.** Rejected for the reason
the original javadoc gives: the ledger is keyed by a UUID minted on every restart, so a map that
is never pruned grows by one dead entry per pod per restart for the life of the cluster. §4.2 is
the answer to the prune route instead — repair it, don't stop housekeeping.

**Ask the peer directly whether it is serving** (an `IExecutorService` task returning that JVM's
own completion flag). This is the only genuinely unloseable answer and it was the first design.
Rejected as disproportionate: it needs a serializable task, a per-instance completion registry
(a static flag is wrong the moment two members share a JVM, which every test in this package
does), and a defined behaviour when the peer runs an image without the class. §4.1 plus §4.2
reduce the failure to "every member holding the entry died", which means the member itself died,
which means it was not serving.

**Raise `min_durable_copies` / lower the barrier timeout.** Neither addresses a barrier that is
unsatisfiable rather than slow.

## 6. Test coverage

New `HotRestoreLostReportDeadlockTest`, three tests, real Hazelcast members built from
`StratumMapConfig.build(...)` so the backup counts under test are the deployed ones:

- `theBarriersCoordinationStateSurvivesLosingTwoOfThreeMembersAtOnce` — 300 stand-in member
  entries in both maps, `terminate()` (not `shutdown()`: a recreated node does not hand its
  partitions over) on two of three, a replacement member joins the way k8s brings the pod back,
  and every entry is still there. Three hundred keys rather than one because the fault is a dice
  roll per partition and a single key would pass two times in three against the very bug it
  catches. **Fails against the unfixed tree: `expected: <0> but was: <101>`.**
- `aServingPeerPutsBackAReportThatWentMissingAndUnwedgesTheBarrier` — the crash-loop end to end.
  Two members pass the barrier, the serving one's report is deleted, the other restarts and
  cannot pass (`only 1 of 2 expected instances reported`), `refileReport` puts it back, and the
  same pod then passes with 2 contributors. Also asserts the repair is a no-op when the entry is
  present.
- `twoPodsRestartingTogetherRecoverWhileTheServingPeerKeepsItsReport` — the refutation of §2,
  kept as a test so the timing story is not re-diagnosed next time.

Three assertions in `StratumMapConfigDurabilityTest` said the ledger's backup count IS the
deployment floor — the bug, written down as an expectation — and were changed deliberately:

- `theRestoreLedgerIsProtectedLikeTheRecordMaps` → `theBarriersOwnMapsAreProtectedAndFullyReplicated`,
  now covering `INFLIGHT_MAP` as well and keeping every split-brain assertion unchanged.
- `theRestoreLedgerAlwaysUsesThePlainDeploymentFloor` → `theRestoreLedgerIsOutsideThePerRecordRaise`.
  Its real invariant — the ledger is not in the generated per-record lookup and no record's
  number decides anything about it — is unchanged and still asserted.
- `theSingleNodeFloorKeepsNoBackupOnTheLedger` → `theSingleNodeFloorDoesNotLowerARecordMapBelowItsOwnDeclaration`,
  which is what it was actually about. A `STRATUM_MIN_DURABLE_COPIES=1` deployment pays nothing
  for full replication: Hazelcast places a backup only where there is a member to place it on.

No other existing test changed.

Unchanged and still green, deliberately: `HotRestoreServingPeerContributorTest`
`.twoPodsRestartingTogetherDoNotCreditEachOther` — two pods restarting onto empty volumes with
no serving peer still refuse at "only 1 of 2 contributed a durable copy". The floor semantics are
untouched.

## 7. Not deployed

Build and test only, per the dispatch. The fix ships as a config change plus a 30-second repair
task; it needs a roll of all three pods to take effect, and the two crash-looping pods will pick
it up on their next restart while stratum-1 needs a deliberate restart to start repairing. Note
the ordering: **stratum-1 restarting is itself the thing that clears the current wedge**, because
a restarting stratum-1 files a fresh report. That is a valid manual mitigation for the live
incident today, independent of this fix, and it is the reason the incident is not urgent.

## 8. Suite result

`mvn -o -pl server test` on the final tree:

```
Tests run: 122, Failures: 1, Errors: 13, Skipped: 0
```

All 14 are the known pre-existing `PERMISSION_DENIED: ... no verified namespace claim` baseline
— 12 in `PullAgentEventE2ETest`, 2 in `HotAgentEventE2ETest` — untouched by this change and not
in scope. Every other test passes, including the three new ones. Zero new failures.

Note on the build: `-am` could not be used. `model/target/generated-sources` was already absent
when this work started, and the locally installed `tacplus` refuses this repo's standalone
generation (`residency annotation errors: ... no (spec.trait) hosts a record query plane`) — the
generator-pin problem a previous child hit. **No tacplus pin was moved.** The existing
`model/target/stratum-model-1.2.0-SNAPSHOT.jar` (built 2026-09-21 13:06 by the `stratum-disk-leg`
child, newer than the one in `~/.m2`) was installed with `install:install-file` and the server
module built against it, so nothing was regenerated and no generated source changed.
