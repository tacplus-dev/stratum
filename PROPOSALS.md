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
  Stratum per tenant with no volume, set `STRATUM_SEED_MEMBERS` to that tenant's members, set
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
  pin it needs, #8) removes a dependency tree with a history of HIGH advisories. Check that nothing in
  the generated model still needs it first.
- **Split brain** (2026-10-28, #2): the redo's strict seq keeps the log single-writer, so a split
  cannot fork the database; but a minority side keeps answering reads from what it holds. Hazelcast
  split-brain protection on reads (or refusing reads without the sequencer) would close that.
- **tacplus#205: a depends cycle merges the project's published proto** (2026-10-28, #2): letsencrypt
  depends on stratum, so stratum cannot generate a proto change before it is on `main`. #2 was verified
  with an explicit `--src` merge and the gates re-run after the push.

## Promoted / declined
