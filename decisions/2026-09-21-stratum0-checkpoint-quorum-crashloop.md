# stratum-0 CrashLoopBackOff: a checkpoint-truncated redo log read as an empty volume

Investigated 2026-09-21 (~11:00 MDT) against the live `schematic` namespace. Root cause
**confirmed**, with one correction to the reported diagnosis that changes the prognosis from
"self-heals" to "does not self-heal". Code changed in `HotRestore`, `Checkpoint`, `EventStore`;
new test `HotRestoreCheckpointContributorTest`. Tree left uncommitted for review.

## 1. What the cluster actually showed

```
$ kubectl get pods -n schematic -l app=stratum
stratum-0   0/1   CrashLoopBackOff   7 (4m21s ago)   17m
stratum-1   1/1   Running            0               10h
stratum-2   1/1   Running            0               130m
```

`kubectl logs -n schematic stratum-0 --previous` ends exactly as reported:

```
java.lang.IllegalStateException: hot restore: only 1 of 3 instances contributed a redo log,
below min_durable_copies 2. ... refusing to serve a silently incomplete hot store
  at ...hot.HotRestore.awaitClusterReplay(HotRestore.java:265)
  at ...EventStore.restoreHotTier(EventStore.java:277)
  at ...EventStore.init(EventStore.java:208)
```

and the two lines before it show the whole shape of the failure:

```
16:51:30.032 HotRedoLogs  hot redo logs opened under /app/data/hot: [...6 datasets...]
16:51:30.094 EventStore   checkpoint: not a cold boot (298 row(s) held cluster-wide)
                          — the peers hold the data, this pod's checkpoint is not loaded
```

`min_durable_copies` is **2** and is not configured anywhere: the StatefulSet sets no
`STRATUM_MIN_DURABLE_COPIES` (`kubectl get sts stratum -o jsonpath='{...env}'` lists only
`BOOTSTRAP_CONFIG`, `NODE_NAME`, `POD_NAME`, `JDK_JAVA_OPTIONS`, the two seed-member lists and
the kernel token file), so `EventStore.init` falls through to
`StratumMapConfig.DEFAULT_MIN_DURABLE_COPIES = 2`.

## 2. Where the 1 came from — and the correction

The peers' own logs say what each of them filed in the ledger, and **the ledger is written
exactly once per member, at that member's `awaitClusterReplay` call, and never revised**
(`ledger.put` is the only write in `HotRestore`; `grep -rn LEDGER_MAP --include=*.java` finds no
other writer outside tests):

| pod | booted | what it read at ITS boot | its permanent ledger entry |
|---|---|---|---|
| stratum-1 | 06:33:30 | `hot tier restored: 371 record(s) read` | **371** |
| stratum-2 | 14:47:22 | `hot tier restored: 0 record(s) read` | **0** |
| stratum-0 | 16:51:30 | 0 (volume's tail already truncated) | 0 |

So contributors = 1 (stratum-1 alone), expected = 3, floor = 2 → refusal.

**Correction to the reported diagnosis.** The prompt attributes stratum-2's zero to its
16:47:22 checkpoint, four minutes before stratum-0's attempt. It is not that checkpoint:
stratum-2's ledger entry has been 0 since **14:47:22**, when it booted onto a volume whose tail a
*previous* checkpoint had already truncated. Its logs show it clearing the same barrier then with
`hot restore complete: 2 of 3 instances contributed a redo log` — it passed only because
stratum-0 and stratum-1 still carried non-zero entries from their own boots.

This matters for the prognosis. **The crash-loop does not self-heal.** The reported
self-healing path — "once stratum-1 also checkpoints, contributors drops to 0, which is allowed"
— does not exist: stratum-1 checkpoints hourly (07:33, 08:33, … all present in its log) and its
ledger entry has stayed 371 throughout, because nothing recomputes it. The entry only changes
when stratum-1 itself restarts. stratum-0 would loop until that happened.

**Checkpoints are hourly, not daily** (`Config.cleanup_interval_seconds`; stratum-1 at
07:33/08:33/09:33…, stratum-2 at 15:47/16:47). Every pod truncates its tail to empty every hour,
so the "has a non-empty redo log" property is false for every pod almost all of the time. What
has been keeping this barrier passing is stale ledger numbers from pods' boots, not current
durability — the mechanism has been accidentally, not structurally, correct.

## 3. Is a checkpoint-truncated log equivalent to a missing one? No.

`DailyCheckpoint.run` is purge → dump → **truncate**, and the truncate is last precisely because
the dump is what makes the tail disposable: the rows move from the redo log into
`checkpoint/<ts>`, committed by one atomic rename before anything is discarded. `DURABILITY.md`
states it directly — "The checkpoint file **IS** the durable copy" — and `DailyCheckpoint`'s own
javadoc calls the previous checkpoint plus an un-truncated log "a complete recovery set".

So the two states the barrier was conflating are not the same state at all:

- **Tail empty, checkpoint present** — this pod's rows are durable on this pod's disk, in the
  dump. Nothing has been lost.
- **Tail empty, no checkpoint** — a first boot or a replaced/wiped volume. This pod genuinely
  holds nothing, and it is what the `contributors > 0` carve-out and the refusal are both about.

Answering the prompt's sub-question about a peer's checkpoint: **there was no notion of one
anywhere in the hot-restore path.** `EventStore.loadCheckpoint` consults only *this pod's own*
dump, and it deliberately skips loading it whenever `ColdStartRebuilder.shouldReplay` says the
peers already hold the data — which is every rolling restart, i.e. precisely the boots where the
barrier decides. `HotRestore` counted redo rows and nothing else. The peer's checkpoint was
durable, restorable, and invisible to the check that was refusing on its account.

## 4. The fix

Report to the ledger **what this volume holds durably** — the redo records replayed *plus* the
committed checkpoint's row count — instead of the redo records alone. Three files:

- **`checkpoint/Checkpoint.java`** — new `rowCount()`. Counts non-blank lines of the dump
  `current` names; 0 when there is none. It does not parse: the caller asks on every boot,
  including the ones where the dump is never loaded, and parsing it there would make every
  rolling restart pay the cold-start cost to answer a yes/no question. Trusting the count is
  sound for the same reason `write` is atomic — `current` is only repointed after the commit
  rename, so a checkpoint it names is a complete dump by construction.
- **`hot/HotRestore.java`** — `awaitClusterReplay` takes a `checkpointRecords` argument, files
  `redoRecordsRead + checkpointRecords` (saturated, so an overflowed sum can never report a full
  volume as an empty one), and the contributor comment block now says what a contributor is in
  those terms. The refusal message names the real condition: instances that "held neither a redo
  log nor a checkpoint".
- **`EventStore.java`** — `restoreHotTier` passes `checkpoint.rowCount()`, via a
  `checkpointRows()` helper that turns an `IOException` into a refusal to start rather than a
  zero, matching `loadCheckpoint`'s posture. A pod that cannot tell whether it has a baseline
  must not file the one report that can talk the cluster below its floor.

**What was deliberately NOT changed.** `min_durable_copies` semantics, the `contributors > 0`
carve-out (restated in the new terms, not weakened), the barrier's expected-count logic, the
live-member filter, and the per-dataset aggregation question already filed in
`2026-09-12-recovery-below-floor-followup.md`.

**Alternatives weighed and rejected.** (a) *Refresh each pod's ledger entry after every
checkpoint* — it makes the number current rather than a boot snapshot, but on its own it makes
things strictly worse: refreshing a redo-only count would write 0 for every pod every hour and
turn today's accident into a guarantee. It is only coherent on top of this change, and then it
is unnecessary (see the open question below). (b) *Have the restoring pod read its peers'
checkpoints* — peers' volumes are not reachable from a pod; the ledger is the only cross-node
channel, which is why the fix is in what gets reported rather than in what gets consulted.
(c) *Drop the floor to 1 / set `STRATUM_MIN_DURABLE_COPIES=1`* — that is turning off the check
the audit added, on a cluster that really does ack on two nodes.

## 5. Test

`server/src/test/java/.../hot/HotRestoreCheckpointContributorTest.java`, three live Hazelcast
members because the bug lives strictly between the two carve-outs — with the checkpointed peer
read as empty the count lands on **one**, which two members cannot express. The just-checkpointed
peer's state is produced by running the **real** `DailyCheckpoint` transaction over a `@TempDir`
volume and asserting its tail came out empty and its dump countable, so "empty because of
checkpoint truncation" is reproduced rather than asserted. Each member files its report through
`awaitClusterReplay` itself, so what is under test is the number production actually files.

- `aJustCheckpointedPeerStillCountsTowardTheDurabilityFloor` — the production scenario; must
  return 2 contributors and not throw.
- `aPeerWithNoTailAndNoCheckpointIsStillNotAContributor` — the paired negative, so the fix
  cannot be "count everyone": an empty volume is still not a contributor and one lone
  contributor against a floor of two is still refused.

Commands run, from `platform/services/stratum`:

```
$ mvn -o -pl server -Dtacplus.skip=true -Dtest=HotRestoreCheckpointContributorTest test
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0 -- in ...HotRestoreCheckpointContributorTest
```

Re-run with `HotRestore`'s sum temporarily reverted to the pre-fix
`long durableRecords = (long) redoRecordsRead;` — the test reproduces the production stack, so it
is not passing vacuously:

```
Tests run: 2, Failures: 0, Errors: 1
aJustCheckpointedPeerStillCountsTowardTheDurabilityFloor -- ERROR!
java.lang.IllegalStateException: hot restore: only 1 of 3 instances contributed a durable copy,
below min_durable_copies 2. ...
```

(the sum was restored immediately afterwards; `grep` confirms
`long durableRecords = (long) redoRecordsRead + checkpointRecords;`)

Full server suite, `mvn -o -pl server -Dtacplus.skip=true test` — it **does** compile here, so
the `platform/proto` llmproxy blocker named in the assignment did not bite:

- Everything restore-, checkpoint- and durability-adjacent passes: `HotRedoRestoreTest` 15/15,
  `HotRestoreLedgerTest` 3/3, `HotRestoreDurabilityFloorTest` 3/3, `HotQuorumGateTest` 4/4,
  `HotRedoBackupTimingTest` 2/2, `HotWritesPerMapDurabilityTest` 2/2, `DailyCheckpointTest`
  10/10, `CheckpointColdStartTest` 2/2, `RecordAnnotationsTest` 4/4,
  `StratumMapConfigDurabilityTest` 12/12, `ColdStartReadGateTest` 7/7, `ColdStartRebuilderTest`
  3/3, `IdleResidencyEvictionTest` 2/2, `ConfigReloadSerializationTest` 4/4.
- Two classes fail, both pre-existing and unrelated: `PullAgentEventE2ETest` (12 errors) and
  `HotAgentEventE2ETest` (1 failure, 1 error). Every one is
  `PERMISSION_DENIED: the record query plane reads rows owned by a namespace and the caller
  presented no verified namespace claim` — the E2E harness presenting no namespace claim, nothing
  on the restore path. The same two classes with the same counts are recorded as pre-existing in
  `decisions/2026-09-12-recovery-below-floor-followup.md`.

The existing call sites in `HotRedoRestoreTest`, `HotRestoreLedgerTest` and
`HotRestoreDurabilityFloorTest` gained a `0` checkpoint argument — each already sets up a pod
described purely by a redo count, so 0 is the faithful translation. An overloaded two-argument
form was deliberately not added: it would let a future caller silently re-create this bug.

## 6. Next steps (not mine to run)

- **Nothing was done to the live cluster.** No pod, PVC or StatefulSet was touched. stratum-0 is
  still crash-looping and, per §2, will not recover on its own.
- **Immediate unblock, if it is wanted before the fix ships:** restarting **stratum-1**
  (`kubectl delete pod -n schematic stratum-1`) drops its stale 371 to 0, which takes the cluster
  to zero contributors — the allowed carve-out — and lets stratum-0 start. Reads/writes stay
  served throughout by the other two. It is a legitimate unblock, not a fix, and it re-arms the
  same trap for the next unlucky restart. 2/3 is up and serving, so waiting for the build is also
  a defensible call; this is the manager's to make.
- Normal build + deploy of `platform/services/stratum` once this is reviewed and committed.

## 7. Open questions

- **Should the ledger entry be refreshed after each checkpoint, rather than staying a boot
  snapshot?** With this fix, staleness can now only make a pod *understate* what it holds (its
  checkpoint only grows rows it already reported as tail), and the check reads the number only as
  `> 0`, so understating cannot cause a false pass and a stale non-zero cannot become false
  without the volume being lost — at which point the pod restarts and re-reports. **Default
  proceeded on:** leave it a boot snapshot; the refresh is an extra mechanism that fixes nothing
  this does not. Reopen it if a future check ever reads the ledger value as a quantity rather
  than as a yes/no.
- **`loadCheckpoint`'s skip is still a real hole, and this change does not close it.** A pod
  whose dump is skipped because *some other* peer has already made the store non-empty has rows
  in neither memory nor the maps, only in its unread file. This fix makes such a pod *count* as a
  contributor, which is a strictly more accurate statement about its durability, but "durable on
  disk" and "present in the cluster" are not the same claim and `shouldReplay`'s cluster-wide
  emptiness test cannot tell them apart. **Default proceeded on:** out of scope — it is a
  cold-start-gate question, it predates this bug, and conflating it with the ledger fix would put
  two arguments in one change. It should be filed and answered separately.
- **Every pod writing an identical hourly dump** is already the accepted cost noted in the
  2026-09-20 lifecycle decision; this change now reads those dumps' line counts on every boot,
  which is one extra file read per pod per start. Named here so it is not a surprise, not
  proposed as a problem.

## 8. Not deployed — build blocked; incident self-cleared via the zero-contributor carve-out

**2026-09-21 ~11:30 MDT.** No image was built, pushed or deployed. `scotty/backend/build.sh
stratum` cannot run on this host: its preflight fails the 45GB free-disk gate (10GB available,
98% full volume), and `docker builder prune -f` — the fix the script itself prescribes — reclaims
space *inside* Docker Desktop's VM disk while returning **zero** bytes to macOS (10.1GB free
before and after). No docker pruning can move that number. Escalated to `looper/schematic/
inbox.md` as a question with three options; nothing was overridden. The commit under test is
`9fa7ebc`; the hot-restore suite re-ran green here (23/23, `HotRestoreCheckpointContributorTest`
2/2) with `-Dtacplus.skip=true` against the module's pre-built classes.

**The live symptom cleared on its own at 17:17:49Z, and how it cleared matters more than that it
did.** `stratum-0` (restart #12, still the old image
`sha256:501b2eabcc0b418ee0a90ff58d92d5e146dc48ee978e70efb54372a2440a2769`) booted successfully
and logged:

```
HotRestore.java:272  hot restore complete: 0 of 3 instances contributed a redo log
```

Not a recovered quorum — the **zero-contributor carve-out**. Section 1's "only 1 of 3" became
"0 of 3" because the last peer still holding a live redo tail checkpointed and truncated it too,
so every member now reports nothing and the first-boot/total-volume-loss escape hatch let the pod
through without any durability check at all. Effect here is benign (it joined a live 3-member
Hazelcast cluster whose partitions already hold the data, rather than restoring from nothing), but
the guard is now demonstrated to swing between both failure directions on the same unchanged
cluster: refusing a healthy boot at 1 contributor, then waving one through unverified at 0. That
is a second live argument for the fix in section 4, not a reason to consider the incident closed.

Fleet at the time of writing: 22/22 Running, `stratum-0/1/2` all 1/1. The bug is latent again and
will resurface on the next restart that lands while exactly one peer holds a tail.

---

# Second bug, same barrier: a serving peer's ledger entry is a boot snapshot, so a mixed-version restart is refused

**2026-09-21 ~12:50-13:00 MDT, live incident.** `stratum-1` and `stratum-2` both in
`CrashLoopBackOff`; `stratum-0` up and serving alone (1/3, degraded HA, not an outage). This is a
DIFFERENT bug from sections 1-7 above, in the same check, and `9fa7ebc` is what exposed it.

## 9. What the cluster showed

```
$ kubectl get pods -n schematic -l app=stratum
stratum-0   1/1   Running            12 (95m ago)   129m
stratum-1   0/1   Error              5 (90s ago)    3m30s
stratum-2   0/1   CrashLoopBackOff   4 (86s ago)    3m30s
```

Both peers exit identically:

```
java.lang.IllegalStateException: hot restore: only 1 of 2 instances contributed a durable copy,
below min_durable_copies 2. ...
	at ...hot.HotRestore.awaitClusterReplay(HotRestore.java:295)
```

**It is a mixed-version cluster, and that is confirmed, not inferred.** The three pods are not
running the same image, despite the shared `:local` tag:

```
$ kubectl get pod stratum-0 stratum-1 -n schematic -o jsonpath='...containerStatuses...imageID'
stratum-0  stratum@sha256:501b2eabcc0b418ee0a90ff58d92d5e146dc48ee978e70efb54372a2440a2769
stratum-1  stratum@sha256:de1113c8242387eca9d1f2f02e5ad9c026c1d9760cc4ca9bff66bf44a6c18588
```

`501b2ea` is the pre-`9fa7ebc` image named in section 8. Its own log confirms the old code by the
old wording and the old line number:

```
$ kubectl logs stratum-0 -n schematic | grep -i 'hot restore complete'
17:17:51.736 HotRestore  HotRestore.java:272  hot restore complete: 0 of 3 instances contributed
                                              a redo log
17:17:51.692 EventStore  checkpoint: not a cold boot (468 row(s) held cluster-wide)
```

So: **stratum-0 filed a permanent ledger entry of 0 at 17:17:51Z** — redo tail only, because that
is all its image knows how to count — and has been serving the cluster's 468 rows ever since.
stratum-1 and stratum-2 boot on the fixed image, correctly count their own `checkpointRecords`
(non-zero), and the barrier sums live entries: their own 1 + stratum-0's stale 0 = **1 of 2**,
below the floor of 2. Refused, forever, on every retry.

The trigger was a bulk roll: the StatefulSet is `updateStrategy: RollingUpdate` (generation 27),
and the events show stratum-1 and stratum-2 recreated within seconds of each other, which is why
neither could satisfy the barrier off the other either.

## 10. Why this does not self-heal, and why `9fa7ebc` is not the thing to revert

Nothing ever revises a ledger entry — that is established in section 2 and is unchanged. So
stratum-0's 0 stands until stratum-0 itself restarts, and stratum-0 is the only serving replica:
restarting it to clear the entry is the one action that converts a degraded cluster into an
outage. The two crash-looping pods cannot rescue each other either, because each sees only its
own fresh report plus the stale 0.

`9fa7ebc` is correct and stays. It made the *reported number* accurate. What this incident shows
is that an accurate number filed ONCE, AT BOOT, is still the wrong thing for the barrier to
decide on — the check asks "who holds a durable copy right now?" and the ledger can only answer
"who held one when they started, under whatever code they were running then". Reverting `9fa7ebc`
would restore the section-1 bug without fixing this one.

## 11. The fix: a peer that is already serving counts, whatever its number says

New map `HotRestore.INFLIGHT_MAP` (`stratum.hot.__restore_inflight`), member UUID -> TRUE,
**put on entry to `awaitClusterReplay`, before the ledger entry is filed, and deleted only on the
success path**. A live member that has a ledger entry and is NOT in that map has passed this same
barrier and is holding the record maps in memory; it is counted as a contributor regardless of
what its `durableRecords` number says.

Three properties, each load-bearing:

- **Absence is the serving state, not presence.** A member on an image that predates this map
  writes nothing to it and is therefore read as serving — which is exactly right for it, because
  an already-running peer on an older image is precisely the member that cannot file a fresh
  report. This is what makes the fix resolve the CURRENT incident rather than only future ones:
  it works against a stratum-0 that is never redeployed.
- **The mark precedes the ledger write**, so the two are never observable in the order that would
  let a sibling read a member still at the barrier as a serving one.
- **The mark is NOT cleared on the refusal path.** A member that refuses is exiting; its entries
  stop counting when it leaves the cluster view, which `liveReports` already handles. Clearing it
  on the way out of a refusal is the one direction that is unsafe — it would advertise a member
  that never finished as one that had.

`liveInflight` filters and prunes by the current cluster view exactly as `liveReports` does, for
the same reason: a departed member's UUID must not decide anything. `StratumMapConfig` gives the
new map the same `durableMap` backup count and split-brain protection as the ledger — it is read
in the same breath as the ledger, so a minority partition that could keep a private copy of "who
is still recovering" could talk itself up to the floor.

**What this does NOT do.** It is not a staleness-versioning scheme, the ledger entry stays a boot
snapshot, `min_durable_copies` semantics are untouched, the zero-contributor carve-out is
untouched, and the `9fa7ebc` counting rule is untouched. Simultaneous restarts cannot credit each
other, which is the case a naive "count every live member" would have broken.

**One behavior change, named rather than hidden.** A cluster where the only serving peer is up
but every restarting pod holds an empty volume now lands on 1 contributor and REFUSES, where
before the fix all-zero reports landed on 0 and passed through the zero-contributor carve-out
unverified. That is the carve-out swing section 8 flagged, now resolving in the strict direction.
It is the correct direction (2-of-3 volumes genuinely wiped should not open reads silently), and
it is covered by `twoPodsRestartingTogetherDoNotCreditEachOther`.

## 12. Test

`server/src/test/java/.../hot/HotRestoreServingPeerContributorTest.java`, two live Hazelcast
members, because the production shape IS two — one serving peer, one pod restarting alone into
it. The serving peer is put into that state by running the real `awaitClusterReplay` to
completion, and the restarting pod is a real `shutdown()` plus a real new member with a fresh
UUID, so nothing about the state under test is hand-written.

- `aServingPeerWithAStaleLedgerEntryStillCountsTowardTheDurabilityFloor` — the production
  scenario: the peer completes a healthy restore, its entry is then overwritten with the
  redo-tail-only 0 a pre-`9fa7ebc` member files for a checkpointed volume, and the sibling
  restarts alone. Must return 2 contributors and not throw.
- `twoPodsRestartingTogetherDoNotCreditEachOther` — the paired negative: a peer inside its own
  restore is not serving anything yet, so two pods restarting together onto empty volumes still
  refuse at "only 1 of 2".

```
$ mvn -o -pl server -Dtacplus.skip=true -Dtest=HotRestoreServingPeerContributorTest test
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0 -- in ...HotRestoreServingPeerContributorTest
```

Re-run with the serving leg temporarily disabled (`boolean servingPeer = false;`) — it reproduces
the live stack, so it is not passing vacuously:

```
Tests run: 2, Failures: 0, Errors: 1
aServingPeerWithAStaleLedgerEntryStillCountsTowardTheDurabilityFloor -- ERROR!
java.lang.IllegalStateException: hot restore: only 1 of 2 instances contributed a durable copy,
below min_durable_copies 2. ...
```

(the leg was restored immediately afterwards; `grep` confirms
`boolean servingPeer = !member.equals(self) && !restoring.contains(member);`)

**Two existing tests needed their peer fixture made faithful, and the reason is the fix's own
semantics.** `HotRedoRestoreTest.peerReported` and `HotRestoreDurabilityFloorTest.peerReports`
each wrote a peer's ledger entry directly, without that peer ever entering `awaitClusterReplay` —
so under the new rule the peer they described was not "a member reporting from inside its own
restore" but "a member that finished one and is serving", which is a contributor by design. Both
helpers already document themselves as filing "as that member's own replay would", and that
replay marks in-flight before it writes the ledger; adding the mark is what makes the fixture
match the sentence. The assertions themselves are untouched — the floor still refuses at one
contributor, and `aRecoveryBelowTheDurabilityFloorRefusesToOpenTheStore` and
`aNodeWithNoRedoLogStillContributesNothing` still fail if the serving leg is applied to a peer
that has not finished.

Restore-, checkpoint- and durability-adjacent suite, all green:

```
$ mvn -o -pl server -Dtacplus.skip=true -Dtest='HotRedoRestoreTest,HotRestoreDurabilityFloorTest,
    HotRestoreLedgerTest,HotRestoreCheckpointContributorTest,HotRestoreServingPeerContributorTest,
    HotQuorumGateTest,HotRedoBackupTimingTest,HotWritesPerMapDurabilityTest,
    StratumMapConfigDurabilityTest,DailyCheckpointTest,CheckpointColdStartTest,
    ColdStartReadGateTest,StartupHealthGateTest' test
Tests run: 65, Failures: 0, Errors: 0, Skipped: 0
```

Full server suite (`mvn -o -pl server -Dtacplus.skip=true test`): 116 tests, the only failures
being `PullAgentEventE2ETest` (12 errors) and `HotAgentEventE2ETest` (1 failure, 1 error) — every
one `PERMISSION_DENIED: the record query plane reads rows owned by a namespace and the caller
presented no verified namespace claim`, the same two classes with the same counts recorded as
pre-existing in section 5 and in `2026-09-12-recovery-below-floor-followup.md`. Nothing on the
restore path.
