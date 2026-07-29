# Stratum — new-tacplus migration status

Status (2026-07-15): **DONE.** `mvn -pl server -am package` BUILD SUCCESS against
the patched dev tacplus (`tacplus-0-0-recordmapfix`). stratum has no unit tests, so
compile is the verification. Generated `StratumRecordMaps.java` implements the new
query-handle RecordMap API (8 methods × 4 record types; 0 old ephemeral-map refs).

Follow-up (not blocking): `WorkingSetStore` is now idle — the query-handle model
doesn't materialize working-set maps, so its create/put/merge/getWorkingSetMap API
is no longer called (it's still lifecycle-wired in `StratumHandler`). Consider
retiring that Hazelcast working-set cluster in a later cleanup.

## Done

1. **tacplus generation unblocked (2 tacplus fixes, on branch
   `fix/recordmap-synthesized-sentinel`):**
   - RecordMap service now carries the `tacplus:generated-service` sentinel so the
     EnforceVerbs gate skips its S2S query verbs (commit `66833196`).
   - Java watch wrapper no longer assumes `from_revision` on handle-based watch
     requests (RecordMap's `QueryHandle`) — commit `b96ffe4e`. Without this the
     generated `RecordMap.java` failed with `cannot find symbol getFromRevision`.
2. **`libs`→`chassis` drift** in stratum's codegen: `RecordMapImplGenerator.java`
   constants `CALLER`/`COLLECTION_RUNTIME` repointed `.libs.` → `.chassis.`
   (`servicemanager.Caller`, `collection.CollectionRuntime`).

3. **RecordMapImplGenerator rewritten** to the query-handle API. It now emits a
   `StratumRecordMaps` that keeps a `query_id → filter` registry (instance field)
   and, per record type T: `open<T>Query` (register filter → handle), `count<T>`,
   `sample<T>` (seeded), `pull<T>` (paged via `pageSorted`), `close<T>`,
   `refine<T>` (replace filter; filterable types only), `watch<T>` (snapshot +
   live via `RecordWatchHub`), `subscribe<T>Count` (via `RecordCountStream`) — all
   over the typed `StratumEventStores` using the getter-based `match<T>` predicate.
   The old ephemeral-map path (create/update-by-query/list/delete + WorkingSetStore)
   is gone; the constructor dropped its `WorkingSetStore` supplier and
   `StratumHandler.getRecordMapGrpcImpl` was updated to the 2-arg form.

Verify with:
`JAVA_HOME=/Users/jody/.local/jdk/jdk/Contents/Home mvn -pl server -am
-Dtacplus.skip=true -Dmaven.test.skip=true package`.
