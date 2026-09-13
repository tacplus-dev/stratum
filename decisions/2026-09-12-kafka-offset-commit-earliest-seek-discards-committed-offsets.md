# Kafka offset commit: the generated subscriber seeks to earliest on every reconnect, discarding the group's committed offsets

Date: 2026-09-12
Branch: `d3-stratum-c2s-split`
Origin: scotty's live cluster investigation "assertion-11" — *"stratum's Kafka/eventbus consumer
group never commits offsets (0 commits observed over 42 seek-to-earliest events)"*, described as
the "likely smallest, highest-value fix" of a batch of three findings.

**Outcome: no code change in stratum. The defect is entirely in tacplus-generated code and in the
eventbus proto contract. Zero stratum files edited; ask filed at
`/Users/jody/workspace/loopers/_retired/tacplus/inbox.md`.**

> tacplus's looper is **retired and will not reply**. The inbox append is a durable record of the
> defect and the generator change it needs, not an active escalation. Nobody is waiting on a
> response, and nothing in stratum is blocked on one.

## The symptom is real; the stated mechanism is off by one step

scotty's observable — every eventbus restart or rebalance makes stratum replay its topic from the
beginning, so artifacts land correctly but minutes-to-tens-of-minutes late — is real and fully
explained by the code. The mechanism is *not* a missing commit call. Offsets **are** committed.
They are then **thrown away on the next connect**, because the client unconditionally asks the
broker to seek to the beginning.

Correcting the finding matters, because "add a commit call" is not the fix and there is nowhere in
stratum to add one.

### 1. eventbus does commit, after every non-empty poll

`platform/services/eventbus/server/src/main/java/.../kafka/KafkaSubscriber.java:619`

```java
if (records.isEmpty()) {
    continue;
}
try {
    consumer.commitSync();
} catch (CommitFailedException e) { ... }
```

with `ENABLE_AUTO_COMMIT_CONFIG = "false"` and `AUTO_OFFSET_RESET_CONFIG = "earliest"`
(`KafkaSubscriber.java:647-648`). The commit is reachable on every poll that delivered records —
the `records.isEmpty()` guard only skips empty polls, deliberately (its comment explains the
`__consumer_offsets` flood it was added to prevent). So the committed offset for group `stratum`
does advance while a subscription is running.

I could not reproduce scotty's "0 commits" counter from this side, and I did not have the live
cluster to check what it was counting. Whether the counter was measuring commit *calls* or
committed-offset *advancement across restarts*, the behaviour below makes the committed offset
useless either way, and it is what produces the observed replay.

### 2. The generated client asks for `earliest` on every single connect

`model/target/generated-sources/.../s2sm/subscribe/AgentsensorAgentEventBusEventStreamSubscriber.java:101-102`

```java
public void subscribe(Handler handler) throws InterruptedException {
    subscribe(handler, true, false, -1);   // earliest=true
}
```

That one-arg overload is the **only** entry point the generated wiring uses. All eight subscribers
in `model/target/generated-sources/.../s2sm/subscribe/` carry the identical line 102, and it comes
straight from the template at
`tacplus/pkg/templates/java/types/Subscriber.java.tmpl:102`.

`earliest=true` is encoded into the `SubscribeRequest` (`...Subscriber.java:351-353`, proto field
4), sent once, and then the stream is half-closed (`:194-196`) — so the client can never send a
follow-up message on the bidi stream even though the proto is designed to allow it.

Crucially, the request is re-sent on **every** reconnect: `subscribe(...)`'s retry loop
(`...Subscriber.java:111-152`) calls `subscribeOnce(channel, handler, earliest, latest, offset)`
with the same captured `earliest=true` each time round.

### 3. The server honours it, and that wipes out the committed position

`platform/services/eventbus/server/src/main/java/.../grpc/SubscribeHandler.java:387-388` maps
`EARLIEST` → `SeekMode.EARLIEST`; `KafkaSubscriber.runLoop` then runs
`applySeek(consumer, partitions, initial)` (`KafkaSubscriber.java:458`) once per session, and
`applySeek`'s `case EARLIEST` is `consumer.seekToBeginning(partitions)`
(`KafkaSubscriber.java:716-717`).

Every gRPC reconnect creates a fresh session with a fresh `KafkaConsumer` (`createSession`,
`:639`), so every reconnect seeks to offset 0 for the whole assignment. The committed offset is
never read. That is scotty's 42 seek-to-earliest events, one per reconnect/rebalance, and it is
exactly the full-replay-per-restart symptom.

### 4. There is no way to express "resume from my committed offset"

`platform/proto/services/eventbus.proto:46-54` — `SubscribeRequest.start_from` is a oneof of
`offset` / `timestamp` / `latest` / `earliest`. There is no `resume` member. And leaving the oneof
unset does not mean resume either: `SubscribeHandler.toSeekParams`'s `default:` case
(`SubscribeHandler.java:390-392`) maps `START_FROM_NOT_SET` to `SeekMode.LATEST`, i.e. skip the
backlog entirely — which is worse than replaying it.

So the contract offers exactly two positions and neither is the one a durable consumer group
wants. **This is why a hand-fix in stratum is impossible, not merely discouraged**: there is no
wire representation of the desired behaviour to send.

## Why this is generator-owned, not stratum-owned

The live `stratum` subscription is wired entirely inside generated code
(`model/target/generated-sources/.../s2sm/ServiceManager.java:769-806`, header:
`CODE GENERATED - DO NOT EDIT`):

```java
String serviceLabel = "stratum";
AgentsensorAgentEventBusEventStreamSubscriber subscriber =
    new AgentsensorAgentEventBusEventStreamSubscriber(
        () -> resolveService("eventbus"),
        () -> identity.callCredentialsForAudience("eventbus"),
        serviceLabel);
...
subscriber.subscribe((event, meta) -> { ... });   // one-arg → earliest=true
```

Grepping all of `server/src/main` for subscriber construction or consumer-group config finds
exactly one hand-written hit, and it is a different, deliberate consumer:

- `ColdStartRebuilder.java:36` — `CONSUMER_GROUP = "stratum-rebuild"`, subscribing with
  `subscribe(handler, false, false, 0)` (`:167`), i.e. an explicit offset 0.

**`ColdStartRebuilder` is correct as it stands and I changed nothing in it.** Replaying from 0 is
its entire purpose, it runs on cold boot only (`shouldReplay(totalCount) == totalCount == 0`), and
it uses a *separate* group specifically so it does not perturb the `stratum` group's offsets. Its
recent stop-condition change (`a1db5ec`, iter14859 — throw `InterruptedException` once the
watermark backlog is drained) is orthogonal to this finding and, if anything, mitigates it: the
rebuild subscription now ends rather than lingering. The task framing anticipated the fix would
land in this class; it does not.

There is likewise no stratum-side config knob defaulting the wrong way. `tacplus.yaml` is four
lines (language/pkg/output-dir/src) and carries no subscription settings, and nothing in
`server/src/main` reads a start-position or auto-commit setting.

Per the house rule — never hand-roll plumbing a generator already produces — the correct move is to
document and file, not to work around it. Working around it would in any case require reaching past
the generated `ServiceManager` to stand up a parallel subscription for the same topic under the same
group, which is precisely the duplicate-writer failure `a1db5ec` just finished removing.

## The generator change needed

Three coordinated pieces. Filed in full at the tacplus inbox; summarised here.

1. **Proto** (`platform/proto/services/eventbus.proto`) — add a `resume` member to the
   `start_from` oneof, meaning "start from this consumer group's committed offset; fall back to
   `auto.offset.reset` (`earliest`) if the group has none". A new oneof member is wire-compatible;
   no existing field changes.

2. **eventbus server** — add `SeekMode.RESUME` (`KafkaSubscriber.java:228`), map it in
   `SubscribeHandler.toSeekParams` (`:374-393`), and make `applySeek`'s new `case RESUME` a
   **no-op**: Kafka's own group-subscribe already positions the consumer at the committed offset,
   and `auto.offset.reset=earliest` already covers the no-committed-offset case. Separately,
   `toSeekParams`'s `default:` for `START_FROM_NOT_SET` should arguably become `RESUME` rather than
   `LATEST` — a client that expressed no preference wants its group's position, not a silent
   backlog skip — but that is a behaviour change for existing callers (Go's template sends no
   `start_from` at all today, see below) and should be decided deliberately, not folded in.

3. **Subscriber templates** — change the convenience overload's default from earliest to resume:

   - `tacplus/pkg/templates/java/types/Subscriber.java.tmpl:102` —
     `subscribe(handler, true, false, -1)` currently hardcodes `earliest=true`. This is the one
     stratum is hitting.
   - `tacplus/pkg/templates/rust/types/subscriber.rs.tmpl:155` — same defect:
     `self.subscribe_with(handler, SubscribeOptions::earliest())`.
   - `tacplus/pkg/templates/go/types/subscriber.go.tmpl:128-129` — sends **no** `start_from` unless
     `WithEarliest()`/etc. is passed, so today it lands on the server's `LATEST` default. It needs
     the same `resume` default, for the opposite reason.
   - `tacplus/pkg/templates/typescript/types/subscriber.ts.tmpl:223` — check alongside; same shape.

The explicit `subscribe(handler, earliest, latest, offset)` overload must keep working unchanged —
`ColdStartRebuilder` depends on it to force offset 0.

## Blast radius if this is fixed

Every tacplus-generated Java and Rust service that subscribes to an event bus under a durable
consumer group is replaying its whole topic on every reconnect today, not just stratum. Eight
subscribers in stratum's own generated model alone. The cost is invisible in correctness terms
wherever the write path is idempotent (stratum's is: `event_id`-keyed IMap upserts) and shows up
only as latency and broker load — which is exactly why it survived this long.

## Open questions for the manager

Recorded here because tacplus's looper is retired and cannot answer them.

1. **Who owns the eventbus half?** Pieces 1 and 2 above are in `platform/proto` and
   `platform/services/eventbus`, not in tacplus at all. scotty's other two findings from this batch
   are also in eventbus. If someone is already opening eventbus for those, the `resume` mode is a
   natural companion and the tacplus template change (piece 3) is then the only orphaned part.
   **Default proceeded on:** filed the whole three-part ask in the tacplus inbox as one record,
   since the pieces are useless separately, and left the eventbus work unstarted — it is outside
   stratum's write scope.

2. **Should `START_FROM_NOT_SET` become `RESUME`?** See piece 2. It would fix Go-generated clients
   without touching their template, and "unset means resume" is the behaviour a durable group
   naturally wants. It is also a silent semantic change for anyone relying on today's LATEST
   default. **Default proceeded on:** flagged, not decided.

3. **Is scotty's "0 commits" counter worth chasing?** The code shows commits happening
   (`KafkaSubscriber.java:619`). If the live counter really was zero rather than
   zero-net-progress, there is a second, separate defect in eventbus that this doc does not
   explain. **Default proceeded on:** treated the seek-to-earliest path as the sufficient
   explanation for the observed replay symptom, and noted the discrepancy rather than assuming the
   counter was wrong.

## Verification

No stratum source files were modified, so there is no new test. `git status` shows only this
untracked decision doc, and `git diff` is empty.

Baseline recorded anyway, so the next person knows what this branch looks like before they touch it.
`mvn -o -pl server -am test` with the two known-broken files parked
(`StratumEventStoresRetentionTest.java`, `warm/WarmTierTest.java` — both still failing to compile on
the missing `model.types.services.tasks` package, unchanged from the note in
`2026-09-12-per-record-durability-floor-needs-a-generated-lookup.md`):

```
Tests run: 133, Failures: 2, Errors: 23, Skipped: 0
BUILD FAILURE
```

**All 25 are the same pre-existing regression**, not this work: every one is
`PERMISSION_DENIED: the record query plane reads rows owned by a namespace and the caller presented
no verified namespace claim`, thrown from the generated `RecordMapBlockingStub`, across
`PullAgentEventE2ETest` (12), `WarmAccountingRecordE2ETest` (5), `WarmMutableSessionStateE2ETest`
(5), `HotAgentEventE2ETest` (2) and `WarmEvictionE2ETest` (1). These E2E tests do not present a
namespace claim and were not updated when queries were pinned to the opening caller's namespace
(`a32ec20`, see `2026-09-12-record-queries-are-pinned-to-the-opening-callers-namespace.md`). Since
this task changed no source at all, they are the branch's baseline by construction.

The tests covering the class this finding was expected to touch both pass:
`ColdStartRebuildStopsAtCatchUpTest` 3/3 and `ColdStartRebuilderTest` 3/3.

Both parked files were then restored and confirmed byte-identical (`git diff` empty).
