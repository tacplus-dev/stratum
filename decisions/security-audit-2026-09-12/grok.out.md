Launching a security review of `stratum` and reading `server/src` and `codegen/src` in parallel for cluster-race and correctness issues.

● Security-review (requested model: grok-4.6) Stratum security audit
  └ Agent started in background with agent_id: a39f300b-66f4-4a4b-9975-987d1dcc15...

/ Search (glob)
  │ "**/*.{java,kt}" in
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src
  └ 15 files found

/ Search (glob)
  │ "**/*.{java,kt}" in
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src
  └ 47 files found

/ Search (glob)
  │ "**/*" in ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum
  └ 289 files found

Security review is running. I’ll inspect cluster bootstrap, auth, and map/query paths for sibling races.

● Read Main.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/Main.java
  └ 36 lines read

● Read StratumHandler.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/StratumHandler.java
  └ 306 lines read

/ Search (grep)
  │ "tenant|auth|authorize|grant|permission|namespace" in **/*.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ No matches found

/ Search (grep)
  │ "SQL|sql|query|deserialize|ObjectInput|readObject|Calcite" in **/*.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ 15 files found

/ Search (grep)
  │ "quorum|split.?brain|cluster.?of.?one|waitFor|memberAdded|membership" in **/*.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src)
  └ 9 files found

● Read EventStore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/EventStore.java
  └ 1 line read

● Read HotWrites.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hot/HotWrites.java
  └ 113 lines read

● Read HotRestore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hot/HotRestore.java
  └ 223 lines read

● Read CalciteEngine.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/CalciteEngine.java
  └ 297 lines read

● Read StratumMapConfig.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hazelcast/StratumMapConfig.
  │ java
  └ 281 lines read

● Read ColdStartRebuilder.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/ColdStartRebuilder.java
  └ 153 lines read

● Read EventStore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/EventStore.java
  └ L1:250 (250 lines read)

● Read EpochMergeProcessor.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hot/EpochMergeProcessor.jav
  │ a
  └ 95 lines read

● Read HotRedoLogs.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hot/HotRedoLogs.java
  └ 201 lines read

● Read RedoAppendProcessor.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hot/RedoAppendProcessor.jav
  │ a
  └ 83 lines read

● Read EventStore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/EventStore.java
  └ L250:550 (301 lines read)

✗ Read WarmStore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/schematic/services/stratum/warm/WarmStore.java
  └ Path does not exist

● Read HotQuorumGateTest.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/test/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hot/HotQuorumGateTest.java
  └ 105 lines read

/ Search (grep)
  │ "CalciteEngine|setConfig|initFromConfig|hotReadable|synchronized" in **/*.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum)
  └ 8 files found

● Read EventStore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/EventStore.java
  └ L550:750 (27 lines read)

● Read WarmStore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmStore.java
  └ L1:200 (200 lines read)

● Read RecordMapImplGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/RecordMapImplGener
  │ ator.java
  └ L1:150 (150 lines read)

● Read StratumAnyGrantProbe.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/test/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/model/chassis/grpcserver/St
  │ ratumAnyGrantProbe.java
  └ L1:80 (80 lines read)

/ Search (grep)
  │ "hotReadable|requireHotReadable|ROOT_TENANT|tenant" in **/*.{java,java.ftl}
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum)
  └ 13 files found

● Read RecordQueryImplGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/RecordQueryImplGen
  │ erator.java
  └ L1:120 (120 lines read)

● Read EventStoreGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/EventStoreGenerato
  │ r.java
  └ L1:200 (199 lines read)

● Read WarmQueries.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmQueries.java
  └ L1:200 (76 lines read)

/ Search (grep)
  │ "query_id|caller|tenant|SPIFFE|requireHotReadable|watch" in **/RecordMapImplGenerator.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src)
  └ 1 file found

/ Search (grep)
  │ "MembershipListener|memberRemoved|ledger" in **/*.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src)
  └ 2 files found

● Read RecordMapImplGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/RecordMapImplGener
  │ ator.java
  └ L150:450 (301 lines read)

● Read WarmEviction.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/WarmEviction.java
  └ 125 lines read

● Read WarmIndex.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmIndex.java
  └ L1:180 (180 lines read)

● Read HotStoreGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/HotStoreGenerator.
  │ java
  └ L1:150 (150 lines read)

/ Search (grep)
  │ "JavaSerialization|setClassLoader|UserCodeDeployment|enableXml|hazelcast.security|symmetric" in
  │ **/*.{java,xml,yaml} (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum)
  └ No matches found

● Read WarmStore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmStore.java
  └ L200:450 (246 lines read)

● Read WarmResidency.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmResidency.java
  └ 71 lines read

● Read EventStoreGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/EventStoreGenerato
  │ r.java
  └ L200:350 (104 lines read)

● Read HotRedoRestoreTest.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/test/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hot/HotRedoRestoreTest.java
  └ L1:120 (120 lines read)

/ Search (grep)
  │ "SqlParser|load_by_sql|CalciteEngine" in **/*.{java,md}
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum)
  └ 3 files found

● Read WarmIndex.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmIndex.java
  └ L180:380 (201 lines read)

● Read StratumEpochExtractor.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hazelcast/StratumEpochExtra
  │ ctor.java
  └ 14 lines read

● Read VServiceWiringGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/VServiceWiringGene
  │ rator.java
  └ L1:150 (150 lines read)

● Read WarmFilter.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmFilter.java
  └ 66 lines read

/ Search (grep)
  │ "create.*Query|queries\.put|caller" in **/RecordMapImplGenerator.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen)
  └ 1 file found

● Read WarmSlices.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmSlices.java
  └ 89 lines read

● Read EventTopicDiscovery.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/EventTopicDiscover
  │ y.java
  └ L1:120 (120 lines read)

● Read WarmIndex.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmIndex.java
  └ L380:520 (141 lines read)

● Read HotDataset.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hot/HotDataset.java
  └ 59 lines read

/ Search (grep)
  │ "minDurableCopies|isWarm|MUTABILITY|appendOnly" in **/EventTopicDiscovery.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src)
  └ 1 file found

● Read EventTopicDiscovery.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/EventTopicDiscover
  │ y.java
  └ L120:350 (231 lines read)

● Read WarmIndex.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmIndex.java
  └ L520:700 (109 lines read)

/ Search (grep)
  │ "getBackupProcessor|writeObject|readObject" in **/*.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ 1 file found

● Read StratumEventTypeExtractor.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hazelcast/StratumEventTypeE
  │ xtractor.java
  └ 20 lines read

● Read Dockerfile
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/Dockerfile
  └ 38 lines read

● Read WarmStoreGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/WarmStoreGenerator
  │ .java
  └ L1:80 (80 lines read)

/ Search (grep)
  │ "phone.home|cluster-name|5001" in **/*.{java,yml,yaml,xml}
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum)
  └ 12 files found

Security specialist is still running. Correctness pass on cluster/restore/query paths is done; I’ll merge both into the ranked report when that review lands.

● Read (Security-review agent — Stratum security audit)
  └ idle

Security review returned one high-confidence issue. I’ll pin exact line numbers, then merge that with the cluster-race findings.

/ Search (grep)
  │ "cfg\.setClusterName|writePool\.execute" in **/*.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ 3 files found

/ Search (grep)
  │ "queries\.put\(qid|Row filtering|emptyIterator|ledger\.size\(\)|hotReadable = true|if
  │ \(this\.config != null" in **/*.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum)
  └ 5 files found

/ Search (grep)
  │ "setClusterName" in **/StratumMapConfig.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ 1 line found

/ Search (grep)
  │ "hotReadable = true" in **/EventStore.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ 1 line found

/ Search (grep)
  │ "onCompleted" in **/RecordMapImplGenerator.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen)
  └ 1 line found

/ Search (grep)
  │ "writeFailures" in **/WarmStore.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ 3 lines found

/ Search (grep)
  │ "queries\.put\(qid" in **/RecordMapImplGenerator.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen)
  └ 1 line found

/ Search (grep)
  │ "ExecutionException" in **/WarmIndex.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ 2 lines found

/ Search (grep)
  │ "ledger\.size" in **/HotRestore.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ 2 lines found

/ Search (grep)
  │ "coldStartHandled" in **/StratumHandler.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ 2 lines found

/ Search (grep)
  │ "entry\.setValue" in **/RedoAppendProcessor.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ 1 line found

/ Search (grep)
  │ "emptyIterator" in **/RecordMapImplGenerator.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen)
  └ 1 line found

/ Search (grep)
  │ "this\.config != null && this\.config.equals" in **/StratumHandler.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main)
  └ 1 line found

/ Search (grep)
  │ "Row filtering" in **/RecordQueryImplGenerator.java
  │ (~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen)
  └ 1 line found

● Read RecordQueryImplGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/RecordQueryImplGen
  │ erator.java
  └ L38:45 (8 lines read)

● Read StratumMapConfig.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hazelcast/StratumMapConfig.
  │ java
  └ L140:150 (11 lines read)

● Read HotRestore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hot/HotRestore.java
  └ L210:280 (14 lines read)

● Read RecordMapImplGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/RecordMapImplGener
  │ ator.java
  └ L230:250 (21 lines read)

● Read StratumHandler.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/StratumHandler.java
  └ L188:275 (88 lines read)

● Read EventStore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/EventStore.java
  └ L348:365 (18 lines read)

● Read RedoAppendProcessor.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/hot/RedoAppendProcessor.jav
  │ a
  └ L70:80 (11 lines read)

● Read WarmStore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmStore.java
  └ L238:258 (21 lines read)

● Read RecordMapImplGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/RecordMapImplGener
  │ ator.java
  └ L330:370 (41 lines read)

● Read WarmIndex.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/warm/WarmIndex.java
  └ L318:345 (28 lines read)

● Read EventStore.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/server/src/main/java/c
  │ om/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/EventStore.java
  └ L218:250 (33 lines read)

● Read RecordMapImplGenerator.java
  │ ~/workspace/gitlab/secondstarcavalier/schematic/platform/services/stratum/codegen/src/main/java/
  │ com/gitlab/secondstarcavalier/controlplane/schematic/services/stratum/codegen/RecordMapImplGener
  │ ator.java
  └ L298:320 (23 lines read)

Read-only audit. Quorum wait + `System.exit` on startup look like the intended incident fix. Remaining issues below are ones I’m confident are real.

## Security

| # | Severity | File | Lines | Vulnerability | Confidence |
|---|----------|------|-------|---------------|------------|
| 1 | 🟠 HIGH | server/.../hazelcast/StratumMapConfig.java | 145 | Unauthenticated Hazelcast join on TCP 5001 (`stratum-events`); no TLS, member auth, or Java serialization filter. `RedoAppendProcessor` / `EpochMergeProcessor` are `Serializable`. | 9/10 |
| 2 | 🟠 HIGH | codegen/.../RecordQueryImplGenerator.java, RecordMapImplGenerator.java | 40-43, 238-243 | C2S row/tenant filtering is deliberately absent; `create*Query` stores the caller’s filter only. Any `QUERY_ANY` user can list every tenant’s records (e.g. all `AgentEvent`s). | 9/10 |
| 3 | 🟡 MEDIUM | codegen/.../RecordMapImplGenerator.java | 238-243, 308-314 | Query handles are unscoped bearer UUIDs (`Caller` ignored). `update*Query` can widen to match-all. Anyone who sees a `query_id` can dump or reshape that query. | 8/10 |

## Correctness (cluster / restore siblings)

**4. 🟠 Restore barrier counts ghost members** — `HotRestore.java` (`ledger.size() >= expected`). Ledger IMap is never pruned on membership change. After rolling restarts, dead UUIDs still count. A replacement pod can pass the barrier before *current* members have replayed — same class as the cluster-of-one incident: the wait is satisfied by the wrong set.

**5. 🟠 Thin restore still opens reads** — `HotRestore.java` (~212-220), `EventStore.java` (~355). `contributors < min_durable_copies` is `log.error` only; `hotReadable = true` anyway. Fresh volumes on a formed cluster: everyone reports `0`, error is logged, queries are served incomplete.

**6. 🟠 Event-log replay is after the read gate** — `EventStore.java` (~355) then `StratumHandler.java` (~255-269). Redo barrier flips `hotReadable`, *then* `ColdStartRebuilder` starts daemon replay with no completion gate. Whole-namespace restart, empty maps+volumes: clients get empty results until (if) replay catches up. Closest sibling to the incident.

**7. 🟠 `initFromConfig` is racy** — `StratumHandler.java` (~188-269). Six call sites; only `provideRecordPlane` is `synchronized`. Concurrent config can start two Hazelcast members (the orphan-member bug they already hit). `eventStore = null` then `new EventStore().init()` (up to 2+5 min) races ingest: `handleEventStreamMessage` calls `eventStore.store` with no null check.

**8. 🟠 Cold replay vs live ingest, last-put-wins** — `RedoAppendProcessor` `entry.setValue(value)` (no epoch). Rebuild group `stratum-rebuild` from offset 0 plus the live subscriber both call `store()`. Mutable keys (session state): an old replayed row can overwrite a newer live row. Append-only: `rejectsAppendOnlyDuplicate` is not atomic with enqueue — two threads can both pass the check; warm `addDocument` then duplicates on disk (count jumps after residency cut).

## Other high-confidence

**9. 🟡 Watch/subscribe are snapshots** — `RecordMapImplGenerator.java` (~340-360). `watch*` returns `emptyIterator()`; `subscribe*Count` `onNext` then `onCompleted`. RPCs and comments promise a live stream; callers hang on a stale snapshot.

**10. 🟡 Failed warm write leaves the key pending forever** — `WarmStore.java` (~252-258). `clearPending` only on success inside `WarmIndex.write`. Disk error: key stays in `pending`, hidden from the disk leg, ineligible for eviction, stuck in heap for process life.

**11. 🟡 Warm search treats slice failure as success** — `WarmIndex.fanOut` `ExecutionException` is logged and skipped. One bad slice → undercount / missing rows with a successful RPC.

**12. 🟡 Every member full-replays on cold boot** — `ColdStartRebuilder` (deliberate). Their own comments: this overwhelmed EventBus/KRaft. Still the production path for a true empty cluster.

**13. ⚪ Pattern drift** — Restore ledger map `stratum.hot.__restore_ledger` is *not* in `StratumEventStores.mapNames()`, so no split-brain protection (the bug they fixed for data maps). `StratumEpochExtractor` / `StratumEventTypeExtractor` are constant husks. Dockerfile/Main still describe a Calcite SQL workspace; `CalciteEngine` is dead. Proto discovery is regex, not descriptors (they’ve already shipped phantom-filter bugs from that).

---

This pass was read-only (no files changed). If you want follow-up:

1. **Fix highest severity issues**
2. **Fix all issues**
3. **Commit a summary of findings** (`SECURITY-REVIEW.md`)

Reply with 1, 2, or 3.

