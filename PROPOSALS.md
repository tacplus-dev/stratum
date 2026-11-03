# Proposals: stratum

Written by loopers. Each proposal: what, why, links. Jody promotes accepted ones into ROADMAP.md (via an agent);
the looper then marks it `promoted` or `declined` here with the date.

## Open
- **<title>** (<date>, <issue/run link>): <what and why>
- **tacplus Java chassis: Javalin 7, or no Javalin** (2026-10-27, #11): stratum now pins Javalin 7.2.4
  (Jetty 12.1.14) over the 6.7.0 that tacplus's dependency list carries, because Javalin 6 brings Jetty 11
  (end of community support, two HIGH advisories, fixes only on 12). Stratum's generated chassis only names
  the type: `GrpcServer.httpServer` is never assigned (only `stop()` on null) and
  `ServiceImplementation.registerRESTEndpoints(Javalin, String)` is never called, so it compiles against 7
  unchanged. tacplus should move the Java chassis to Javalin 7 for every Java service, or drop the unused
  Javalin from it (and the Jetty/Kotlin stack it puts in each image). Needs a tacplus issue.
- **A dependency audit in `just gates`** (2026-10-27, #13): the gates are generation, the tracked-output
  check, the build and the tests. #11, #12 and the Hazelcast/Bouncy Castle/jackson bumps were all found by
  the nightly scan, not on the change that would have let them in. An audit step (OWASP dependency-check
  or `osv-scanner` over the resolved tree, failing on HIGH and above) would catch them before the push;
  #13's triage did not decide it, so it needs its own issue and a decision on the threshold.
- **An image smoke test** (2026-10-27, #14): no gate builds the release image. #14 moved the base to
  `java21-debian13` and was checked by hand: `docker build` with the real jar and launcher, then the
  container started the same on debian12 and debian13 (gRPC health on the plaintext probe port 8444 answers
  `NOT_SERVING`, 8443 is not up without a kernel state-store endpoint in the bootstrap config). A recipe that
  builds the image and starts it against a stub state store until 8443 answers `SERVING` would turn that
  into a check, and would cover the next base or JDK move (java25-debian13 is published).
- **One SLF4J provider in the jar** (2026-10-27, #14): the server pulls in both logback and `slf4j-simple`
  (root `pom.xml` dependencyManagement, `model/pom.xml`, `server/pom.xml`), so every start prints
  `Class path contains multiple SLF4J providers`. Logback wins today, but the choice is classpath order. If
  `slf4j-simple` is only there for tacplus's generated model, it should be test scope or excluded; check
  what tacplus's dependency list expects first.
- **The operator renders the per-tenant Stratum** (2026-10-28, #2): stratum.proto now declares
  `tenant_owned: true` and holds no durable state. The operator (tacplus-dev/operator) has to run one
  Stratum per tenant with no volume, set `Config.seed_members` to that tenant's members (#23; required since #18: a member without seeds refuses to start, and there is no multicast fallback; a one-member tenant names itself), set
  `STRATUM_ZONE` from the node's `topology.kubernetes.io/zone` (zone-aware backups), and size memory for
  the tenant's whole dataset (nothing is evicted). The member count per tenant is the footprint knob; one
  is allowed. Needs an operator issue.
- **Purge soft-deleted rows again** (2026-10-28, #2): the daily checkpoint purged rows whose
  `(spec.deleted_flag)` was set after `deleted_max_age_seconds`; it went with the disk path, and nothing
  purges them now (docs/DURABILITY.md, Known gaps). A sweep on the sequencer that issues `Delete`
  through the write path (so the redo logs it) would restore it; it needs a delete timestamp, which
  `(spec.store_epoch)` gave.
- **Drop google-cloud-storage** (2026-10-28, #2): `proto/dependencies.yaml` pins it only so tacplus
  emits `GcsArchiveSink` for the hot redo archive, which #2 removed. Dropping it (and the jackson-bom
  and opentelemetry-bom pins it needs, #8 and #19) removes a dependency tree with a history of HIGH
  advisories. Check that nothing in the generated model still needs it first.
- **Split brain** (2026-10-28, #2): the redo's strict seq keeps the log single-writer, so a split
  cannot fork the database; but a minority side keeps answering reads from what it holds. Hazelcast
  split-brain protection on reads (or refusing reads without the sequencer) would close that.
- **tacplus#205: a depends cycle merges the project's published proto** (2026-10-28, #2): letsencrypt
  depends on stratum, so stratum cannot generate a proto change before it is on `main`. #2 was verified
  with an explicit `--src` merge and the gates re-run after the push.
- **Pin the exact tacplus version (`tacplus.lock`)** (2026-10-30, nightly scan, #20): `tacplus.yaml` names only
  the `2.5` line, and `just generate` runs whatever `tacplus` is on the PATH. So tacplus v2.5.3 (tacplus#207)
  broke `just gates` on main overnight, with no stratum commit. The decision tacplus-dev/roadmap
  decisions/deterministic-builds-2026-10-27.md says a build is fully determined by its commit: the repository pins
  the exact tacplus version and build-image digest, and moving the pin is a commit. tacplus#193 defines the pin
  (`tacplus.lock`, `tacplus pin`), and it ships in 2.6.0 (tacplus#213). After that, stratum should commit a
  `tacplus.lock` and have the justfile run the pinned binary. A new patch then reaches main as a looper commit
  that passes the gates, not as a red nightly.
- **E2E tests through the instance's tenant door** (2026-10-30, #20): `TenantQueryScopeE2ETest` and
  `RecordWriteSurfaceE2ETest` stub `Authorizer.authorize` whole to skip the token signature check, so the door that
  refuses another tenant's caller on a tenant-owned instance (`setHostNamespace`, tacplus#207) is never exercised over
  the wire here; only the route check and the handle check behind it are. Signing the test tokens with a test JWK
  source instead would put the shipped `authorize` on the path.
- **`STRATUM_BACKUP_COUNT` and `STRATUM_ZONE` out of env too** (2026-11-01, #23): #23 moved the seed
  members into `Config.seed_members`; stratum still reads these two from env
  (`StratumMapConfig.configuredBackupCount`, `configuredZone`). Under the uni-config's interim rule
  (tacplus-dev/operator#17: no env but downward-API pod identity) both have to go: `backup_count` fits
  `Config`; the zone is per-node, so it is either node identity (allowed with `NODE_NAME`, if stratum
  looks the zone up itself) or a downward-API value Jody has to add to the allowlist. Also: the operator
  still renders `STRATUM_WS_SEED_MEMBERS` and `STRATUM_MEMBER_ROLE`, which nothing has read since #2;
  operator#17 can drop them now, and `STRATUM_SEED_MEMBERS` once it renders `seed_members`.
- **A gate on the plugins' own classpath** (2026-11-02, #25): the OSV check covers the project's resolved tree,
  not the realms Maven runs plugins in, so maven-shade-plugin on plexus-utils 3.5.1 (GHSA-6fmv-xxpf-w3cw) was
  found by hand, and nothing here would catch the root pom's override being dropped while shade still asks
  for an affected plexus-utils. A check reading the `Populating class realm plugin>...` blocks of
  `mvn -X package`, like fleet-dev's `checks/shade.sh` (tacplus-dev/fleet-dev#17), or scanning those realms'
  jars, would; it costs another package build per gates run unless it reads the gates' own build log.
  #30 added antrun's override (plexus-utils 4.0.3) and moved exec and build-helper to clean releases, found
  the same way (tacplus-dev/fleet-dev#18, whose `checks/plugins.sh` is the model for those three realms).
  exec-maven-plugin has no execution in the gates' build, so its realm only shows up under a goal run
  directly (`mvn -X -pl model exec:help`).
- **model's antrun step and `just generate` run different compilers** (2026-11-03, #30): `just generate`
  runs `tacplus build --tag latest`, while model's antrun target runs plain `tacplus build` when the
  generated sources are missing (a bare `mvn install`). Running that target produced different sources
  from the gates' (eventbus and kernel gRPC stubs, `annotations/` missing), so a bare `mvn install` from
  a clean checkout does not build what the gates build. Passing `--tag latest` there too, or failing
  with "run just generate", would close the gap.
- **Audit the release jar, and publish the result** (2026-11-03, nightly scan, #28): an OSV check of the
  resolved pom tree came back clean (164 packages, every scope), while `trivy rootfs` over `server/target/stratum.jar`
  found jackson 2.21.2 and 3.1.2 with HIGH advisories shaded inside Hazelcast 5.7.0 (`com.hazelcast.shaded.*`),
  which no tree can show. #28 put the jar scan in the gates (`just audit-jar`); the image (with the image smoke
  test) is still not scanned. And CI runs only `planning-files`, so there is no security report for the
  nightly scan to read: the README has no "Security checks" section and code scanning has no analysis. A workflow
  that runs that audit on push and uploads SARIF (or a README section that says where the report lives) would
  give the scan one to read.
- **Bump Hazelcast when it re-shades a fixed jackson** (2026-11-03, #28): the eight jackson advisories shaded in
  Hazelcast 5.7.0 are accepted as unreachable until 2026-12-03 (`.trivyignore.yaml`,
  decisions/2026-11-03-shaded-jackson-in-hazelcast.md). Hazelcast ships no OSS patch releases, so the fix comes
  with its next minor; asked on hazelcast/hazelcast#26597. When it ships: bump `hazelcast.version`, delete the
  entries, and keep `ShadedJacksonReachabilityTest` while any shaded jackson remains. If it has not shipped by
  2026-12-03, `just gates` fails on main, and the exception needs re-examining, with the test still green, or
  another route: an unaffiliated fork that re-shades jackson 2.21.7 / 3.1.7 ("Opencast", its own Maven
  repository) is advertised in those threads, and taking it would change the provenance of the store's core
  dependency, which is Jody's call.
- **Gates that depend on the advisory database** (2026-11-03, #28): `just audit-jar` runs trivy against its
  current database, so a new advisory turns `just gates` red on main, and on every land, with no stratum
  commit, like the tacplus patch did (#20). That is the point of the audit, but a land blocked by an advisory
  in an unrelated jar may want a looper rule: whether a newly red audit blocks unrelated lands, or is
  triaged as its own bug first.
- **Stored rows keep the write's `trace_id`** (2026-11-03, #29): the trace decision
  (tacplus-dev/roadmap decisions/trace-propagation-2026-11-03.md) clears field 200 on every store, the record
  plane and redo included. Records already carry it (`LetsEncryptCert.trace_id = 200`), and
  `RecordStore.commit` puts `op.item` into the map and its bytes into the `RedoChange` value as they came.
  tacplus-dev/tacplus#313 clears it on generated storage paths. Once it lands, check with a raw map read
  and a redo tail whether the generated query plane clears it before the `RecordOp`. If it doesn't,
  `commit` (and `Replay.apply`, for rows logged before) should clear it, with a test.
- **Does a watch event carry the write's trace?** (2026-11-03, #29): a commit publishes
  `ChangeFeed.Committed` (a Java object with no trace) on the Hazelcast topic, and each watcher turns it
  into a `...WatchEvent`, which has field 200. That event is caused by the write but sent on the watcher's
  stream. The decision doesn't say whose trace it carries: the write's (copy it through `Committed`), the
  watch request's, or none. It needs a ruling before stratum sets it.
- **A forwarded Get drops its `trace_id`** (2026-11-03, #29): `RecordStore.get` forwards to the sequencer
  as `SequencerCall.get(type, key, namespace)`, and the trace isn't in it. That's harmless today, because
  the sequencer's `getHere` makes no further call. If it ever does, carry the request's `trace_id` in the
  call, the way `SequencerCall.apply` carries the whole request.

## Promoted / declined
