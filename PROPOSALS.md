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
- **amd64 release images, and a smoke test of the published one** (2026-11-13, #3): the release workflow
  builds linux/arm64 only (`PLATFORMS` in `.github/workflows/release.yml`), amd64 off by default like the
  fleets' `images/pins.yaml`; turning it on is the one-line change there. No step starts the published image:
  the image smoke test above, run on the image the workflow pushes, would check it before the GitHub release.
- **Release v0.1.x from `release/0.1` with the workflow** (2026-11-13, #3): `release/0.1` predates
  `release.yml`, so a tag there does not run it; it is published by the `publish` dispatch from main (README
  Releasing). Backporting `release.yml` and `ci/` to `release/0.1` would let its tags release themselves.
- **One SLF4J provider in the jar** (2026-10-27, #14): the server pulls in both logback and `slf4j-simple`
  (root `pom.xml` dependencyManagement, `model/pom.xml`, `server/pom.xml`), so every start prints
  `Class path contains multiple SLF4J providers`. Logback wins today, but the choice is classpath order. If
  `slf4j-simple` is only there for tacplus's generated model, it should be test scope or excluded; check
  what tacplus's dependency list expects first.
- **The operator renders the per-tenant Stratum** (2026-10-28, #2): stratum.proto now declares
  `tenant_owned: true` and holds no durable state. The operator (tacplus-dev/operator) has to run one
  Stratum per tenant with no volume, deliver that tenant's members as its `StratumTenantConfig.seed_members` slice of the TenantConfig kernel holds (#26, operator#2 option B; was `Config.seed_members`, #23; required since #18: a member without seeds refuses to start, and there is no multicast fallback; a one-member tenant names itself), set
  `STRATUM_ZONE` from the node's `topology.kubernetes.io/zone` (zone-aware backups), and size memory for
  the tenant's whole dataset (nothing is evicted). The member count per tenant is the footprint knob; one
  is allowed. Since #17 it also renders `grpcconfig.member_port` (`listen_address` the pod's member port,
  5001, the only Hazelcast port the pod exposes; `target_address` a loopback address outside `127.0.2.0/24`,
  the same port on every member), lists `seed_members` as every member's member port, its own included,
  in the same order on every member (a member's place in it is its loopback alias), and the NetworkPolicy
  on the member port (operator#2) as a second layer. Needs an operator issue.
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
- **E2E tests through the instance's tenant door** (2026-10-30, #20): `TenantQueryScopeE2ETest` and
  `RecordWriteSurfaceE2ETest` stub `Authorizer.authorize` whole to skip the token signature check, so the door that
  refuses another tenant's caller on a tenant-owned instance (`setHostNamespace`, tacplus#207) is never exercised over
  the wire here; only the route check and the handle check behind it are. Signing the test tokens with a test JWK
  source instead would put the shipped `authorize` on the path.
- **`STRATUM_BACKUP_COUNT` and `STRATUM_ZONE` out of env too** (2026-11-01, #23): #23 moved the seed
  members into `Config.seed_members` (and #26 on into the tenant's `StratumTenantConfig` slice); stratum still reads these two from env
  (`StratumMapConfig.configuredBackupCount`, `configuredZone`). Under the uni-config's interim rule
  (tacplus-dev/operator#17: no env but downward-API pod identity) both have to go: `backup_count` fits
  `Config`, or the `StratumTenantConfig` slice if it is to differ per tenant (#26); the zone is per-node, so it is either node identity (allowed with `NODE_NAME`, if stratum
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
  The 2026-11-06 nightly scan found six more realms the same way (#31): compiler, jar, resources, plugin-plugin,
  protobuf-maven-plugin and the os-maven-plugin build extension (plexus-utils, commons-io, aircompressor, guava,
  jsoup). An OSV query of every `Included:` jar in `mvn -X package` (17 realms, 127 jars) found them in one pass,
  so the check could read the gates' own build log rather than keep a per-plugin list.
  #31 fixed them (compiler 3.16.0, jar 3.5.1, plugin-plugin 3.16.0; plexus-utils 3.6.1 overrides on resources
  3.5.0, protobuf-maven-plugin 0.6.1 and os-maven-plugin 1.7.1, now declared as a plugin with
  `<extensions>true</extensions>` so it can take them, plus commons-lang3 3.20.0 on os-maven-plugin): every
  realm's jars (106, 20 realms) are clean on OSV. fleet-dev's docs/java-plugins.md has no entry for compiler, resources,
  protobuf-maven-plugin or os-maven-plugin, and covers jar 3.5.0 and plugin-plugin 3.15.1 with plexus-archiver
  overrides where 3.5.1 and 3.16.0 now need none: a fleet-dev issue, as tacplus-dev/fleet-dev#17/#18 were.
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
- **A "Trace propagation" section in the README** (2026-11-05, nightly scan; tacplus-dev/looper#51, #29): the
  nightly scan reads the README's "Trace propagation" section, and there is none (nor "Security checks", see
  above). `looper-trace check` reads no Java (`pass (0 handlers, 0 findings)`, 42 Java files not checked), so
  every run re-derives the handlers by hand. A short section would save that work: the only downstream calls
  are the redo's `Append` and `Replay` (`RecordStore.commit`, `catchUp`, `GrpcRedoLog`), plus the Hazelcast
  forward to the sequencer (`SequencerCall`). It would also say which carry the write's `trace_id`, which are
  marked `// trace: unrelated:`, which wait on tacplus-dev/tacplus#313, and which tests assert it.
- **tacplus: a member port forward that re-resolves its remote** (2026-11-06, #17): `MemberPort.forward`
  takes an `InetSocketAddress`, so Stratum resolves each other member's name (`stratum-<i>.stratum`) once,
  at start. A peer pod that comes back on a new IP, or whose name does not resolve yet when this member
  starts, stays unreachable through that forward until this member restarts. A forward by host and port,
  resolved on each connection, would follow the StatefulSet. Needs a tacplus issue (a Java chassis change;
  the generated `MemberPort` is not Stratum's to edit).
- **Run the generated ServiceManager's member port wiring in a test** (2026-11-06, #17; tacplus
  KNOWN_TEST_GAPS MP-1): `TenantMemberAdmissionTest` drives the generated `MemberPort` itself with a fake
  kernel (a test CA, signing key and minter), as tacplus's `cross-test/memberport` does. Neither runs
  `ServiceManager.startMemberPort` (the `member:stratum` token from the identity manager, the JWKS watch, the
  auto-TLS certificate) against a kernel: MP-1 names Stratum's adoption as one way to close it, which needs a
  fake kernel that mints `member:<svc>` (kernel#30) and serves ManageCert, or a test against a deployed one.
- **Kernel's members behind the same member port** (2026-11-06, #17): kernel's Hazelcast member traffic is
  plain and unauthenticated (tacplus-dev/kernel#24, kernel PROPOSALS.md). `MemberNetwork` (the per-member
  loopback alias, forwarded to each peer's member port) is the glue kernel would need too; if both use it,
  it belongs in tacplus's generated member port rather than in each service.
- **Register `StratumTenantConfig`'s TenantConfig field number** (2026-11-07, #26): tacplus numbers a new
  tenant slice `max + 1` in each build and records it nowhere (tacplus-dev/tacplus#352), so `stratum = 4` in the
  generated TenantConfig holds only because stratum's build and kernel's (tacplus-dev/kernel#34) see the same
  slices. Once tacplus can register a slice field, register `stratum`'s in tacplus-dev/registry (4, as generated
  today) and sync `proto/registry.yaml`.
- **Watch the seed members, not only read them at start** (2026-11-07, #26): the member reads its tenant's
  `StratumTenantConfig.seed_members` once, when it starts (the generated watch's cache, else one GetAll), and a
  changed list applies at the next start, as `Config.seed_members` did. Subscribing to the generated
  `ConfigState.tenant_configs` watch could log, or report in health, a list that no longer matches the one the
  member joined, so a scale-up the operator delivers is visible before the rolling restart.

## Promoted / declined
