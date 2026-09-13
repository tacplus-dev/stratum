Publishing was declined, so the report lives at the local file `/private/tmp/claude-501/-Users-jody-workspace-loopers/5475af40-be0a-442b-88c2-a4219a5828de/scratchpad/stratum-audit.html` and in full below. Nothing in the repo was modified.

## Security

**1. Critical. The generated query plane ignores the caller's tenant.** `codegen/.../RecordMapImplGenerator.java:246-289, 329-357`. Six stored record types carry `(spec.store_namespace)`, and the tacplus wrapper expects the handler to return only the caller's namespace. Any other row is treated as a leak and the wrapper calls `Runtime.halt(1)` on List, Sample and Watch. The emitted bodies never read `caller` on either leg. Failure: a `/tenants/<t>` peer with a QUERY grant calls ListAgentEvent while another tenant's rows exist, and the pod halts. Repeat per replica and the Hazelcast cluster drops below quorum. Count, Update size and SubscribeCount have no backstop, so the same caller receives fleet-wide counts.

## Correctness

**2. High. Warm rows inside the residency window, and four hot types, have no recovery after a cluster-wide restart.** `HotStoreGenerator.java:76`, `ColdStartRebuilder.java:90-100`, `StratumHandler.java:221-233`. Warm types get no redo log, yet the disk leg refuses rows newer than the boundary. Message, ScottyActivity, AgentCycle and WorkActivity have no store epoch, so no redo log either. Event-log replay runs only when every map is empty and covers only the agentsensor topic. After a full restart, AgentEvent's redo restore makes the count nonzero, replay is skipped, and every MailboxEntry, AccountingRecord and AgentSessionState younger than a year plus all rows of the four un-logged types are gone.

**3. High. Retention deletes warm slices before the disk leg would ever serve them.** `EventStore.java:472-484`, `WarmStore.java:88, 349-356`, `WarmIndex.java:476-490`. Defaults are 90-day retention and a 365-day window. Disk serves only rows older than a year; slices are dropped at 90 days. Nothing reaches the boundary, so every Lucene commit is wasted, WarmEviction never reclaims memory, and the "still queryable, only slower" promise in accounting-record.proto is false.

**4. High. The restore ledger accumulates stale member UUIDs.** `hot/HotRestore.java:162-183, 206-211`. Never cleared, keyed by a UUID that changes each restart. Two of three pods die, the survivor keeps three entries, the first pod back puts a fourth, sees four against an expected three, and opens hot reads before the third pod's redo log is replayed. That is the exact incomplete-store condition the barrier exists to refuse.

**5. High. Redo logs never compact and retention writes no tombstones.** `hot/HotRedoLogs.java` (no `roll`), `EventStore.java:479`, `HotRestore.java:98-115`. Every write appends with an fsync forever. On restart every row ever written replays as APPLIED into the empty map, so retention-deleted events resurrect until the next sweep, and restore time scales with lifetime writes.

**6. High. A failed init during config reload leaves an orphan Hazelcast member and an unreadable store.** `StratumHandler.java:197-212`, `EventStore.java:135-155`. The new store is assigned before init runs. Formation timeout, divergence or interruption throws after the member is created without shutting it down, and ServiceManager swallows the exception. The member owns partitions with no HotRedoLogs installed, so appends on it silently skip disk. Every read is UNAVAILABLE and every ingest NPEs into the subscriber reconnect loop. Same shape as the incident, on the path Main's exit does not cover.

**7. Medium. initFromConfig is unsynchronized and reachable from three threads.** `StratumHandler.java:168-234, 124-131, 96-103`. Two overlapping distinct configs both pass the equality guard, both tear down and both rebuild. The second HotRedoLogs install throws or two members join, which is the "three members" failure the comment says was fixed.

**8. Medium. Lossy JSON redo payloads turn schema skew into a DIVERGED crash loop.** `EpochMergeProcessor.java:76`, `HotWrites.java:103-111`, `EventStore.java:345-349`. The payload is the field-by-field Gson helper, which drops unknown fields, while `equals` on the peer's wire object includes them. Ship a producer with a new AgentEvent field before stratum, then roll stratum: every such key reports DIVERGED, init throws, and the pod restarts into the same refusal.

**9. Medium. Cold-start replay never ends.** `ColdStartRebuilder.java:94-100`. The subscribe call blocks until interrupted, so after catching up the rebuild group stays a live second consumer. Every AgentEvent is then written twice per replica for the life of the process.

**10. Medium. A failed warm disk write leaves the key claimed forever.** `WarmStore.java:253-261`, `WarmIndex.java:193`. The pending set is cleared only on success. The key is hidden from the disk leg and never evictable, the case the class comment at `WarmIndex.java:150-159` calls worse than the duplicate.

**11. Medium. Startup wait budget exceeds the sidecar window.** `StratumMapConfig.java:171` (2 min) then `EventStore.java:105` (5 min), sequential, before the listener binds. A slow peer volume means the sidecar that killed the incident pod at five minutes fires first, again with no terminal log line.

**12. Medium. Slice retention can close a searcher under an in-flight query.** `WarmIndex.java:618-627, 383-389, 361-381`. The task throws AlreadyClosed, fanOut logs and drops that slice, and the caller gets a short page or undercount with no signal.

## Anti-patterns and drift

**13. Low. Watch and SubscribeCount are one-shot but documented as live.** `RecordMapImplGenerator.java:21-28, 338-341, 355-357`. The doc cites a RecordWatchHub that does not exist.

**14. Low. Query-handle registry has no cap, TTL or owner.** `RecordMapImplGenerator.java:115, 239`. Any entitled peer can grow heap without bound.

**15. Low. Dead read path with a hand-rolled SQL parser and Calcite executor.** `EventStore.java:387-430, 509-575`, `CalciteEngine.java`, `StratumHandler.java:273`. No callers exist. The five indexes and OBJECT format at `StratumMapConfig.java:252-279` are maintained on every put for this nonexistent reader. Flagged only, per your read-only instruction.

**16. Low. VServiceDiscovery parses comments that EventTopicDiscovery strips.** `VServiceDiscovery.java:208` vs `EventTopicDiscovery.java:292`. A commented-out service block generates an abstract provider nobody implements.

Findings 1 through 3 are the ones I would fix first. The tenant issue is the only one that is externally triggerable, and 2 and 3 together mean the warm tier neither survives a full restart nor ever serves from disk.
