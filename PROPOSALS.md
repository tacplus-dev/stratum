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

## Promoted / declined
