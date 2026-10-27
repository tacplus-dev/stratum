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
- **A `just gates` recipe** (2026-10-27, #11): the looper verifies with `just gates`, but the justfile has
  only `generate` and `clean`; #11 ran `just generate` then `mvn -B clean install`. A `gates` recipe saying
  exactly that would make the check the same for every run.

## Promoted / declined
