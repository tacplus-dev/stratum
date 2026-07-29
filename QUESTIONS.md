# stratum QUESTIONS.md

1. **tacplus gap — watch<T> / subscribe<T>Count dropped from RecordMap.RecordMapHandler**
   The old RecordMapImplGenerator generated `watch<T>` (returning `CollectionRuntime.WatchSource<T, Stratum.<T>WatchChange>`) and `subscribe<T>Count` (returning a live count stream) methods. After the tacplus-1.2 → main migration these methods are absent from the generated `RecordMap.RecordMapHandler` interface, and the `Stratum.<T>WatchChange` message types were never added to stratum.proto.
   - **Old API**: `RecordMapHandler.watchAgentEvent(Caller, QueryHandle)` → `WatchSource<AgentEvent, Stratum.AgentEventWatchChange>`; `subscribeAgentEventCount(Caller, QueryHandle, StreamObserver<RecordCount>)`.
   - **Current API**: neither method exists in `RecordMap.RecordMapHandler`.
   - **Default taken**: removed watch<T>/subscribeCount generation from RecordMapImplGenerator and dropped `watchHubSupplier` from StratumRecordMaps constructor. `RecordWatchHub` remains wired in `StratumHandler.handleEventStreamMessage` (still publishes) but has no current consumer.
   - **Question**: will tacplus re-add these as streaming RPCs (server-streaming Watch<T> and SubscribeCount in stratum.proto)? If yes, a `Stratum.<T>WatchChange` proto message is needed per record type. If the watch plane moves entirely to RecordMap streaming RPCs, the RecordWatchHub fan-out in StratumHandler should be rewired to the generated handler.

2. **StratumQuery vservice still tagged stratum but implementation removed (Phase 4)**
   `agent-sensor.proto` declares `StratumQuery` with `option (spec.service_s2s) = {tag: "stratum"}`, so the generated `StratumVServiceBase` requires `provideStratumQueryHandler()`. The previous `StratumQueryImpl` was removed in Phase 4 (commit 4b8b622).
   - **Default taken**: implemented `provideStratumQueryHandler()` as a stub returning UNIMPLEMENTED for all 18 methods, with a comment that the map-plane moved to RecordMap.
   - **Question**: should the `StratumQuery` service be removed from the proto (tag removed / service deleted) since its function is now in RecordMap? Or is the plan to re-implement it on top of RecordMap queries?

## ANSWERS (control agent, 2026-07-29)
1. **Record watch plane**: current tacplus record contract has NO watch — the 1.2 watch<T>/subscribeCount surface is gone deliberately, not missing. Decision: do NOT re-add in tacplus now; revisit only if the P6 e2e demands live record subscriptions. The now-consumerless RecordWatchHub fan-out in StratumHandler is dead code — excise it in the P3 stratum pass (queued in control STATUS).
2. **StratumQuery**: KEEP the stratum tag. argus now delegates its session queries (incl. subscribe_live_sessions + the paged list methods) to StratumQuery — the vservice query plane hosted by stratum is exactly the pattern. Your UNIMPLEMENTED stubs are acceptable ONLY as a compile bridge: implementing StratumQuery over the record store is a queued P3 work item.
