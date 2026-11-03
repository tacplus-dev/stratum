# The jackson shaded in Hazelcast 5.7.0: unreachable, accepted until 2026-12-03

#28, 2026-11-03. Direction (hub, on #28): prove reachability first; if no path is reachable, record a
30-day expiring exception per advisory with the evidence; ask Hazelcast for a fixed release in
parallel and bump when it ships; re-shading Hazelcast in stratum's build is out.

## What is in the jar

`hazelcast-5.7.0.jar` (the latest Hazelcast) relocates four jackson artifacts into
`com.hazelcast.shaded.*`, and the shade plugin copies them into `server/target/stratum.jar`:

| Shaded artifact | Package | HIGH advisories (trivy) | Fixed in |
|---|---|---|---|
| jackson-core 2.21.2 | `com.hazelcast.shaded.com.fasterxml.jackson.core` | CVE-2026-68494, -89407, -89425 | 2.21.7 |
| jackson-databind 2.21.2 | `com.hazelcast.shaded.com.fasterxml.jackson.databind` | CVE-2026-54512, -54513, -68497, -91776, -91777 | 2.21.7 |
| jackson-core 3.1.2 | `com.hazelcast.shaded.tools.jackson.core` | CVE-2026-68494, -89407, -89425 | 3.1.7 |
| jackson-databind 3.1.2 | `com.hazelcast.shaded.tools.jackson.databind` | CVE-2026-54512, -54513, -68497, -91776, -91777 | 3.1.7 |

Each databind also has 11 MEDIUM advisories. They fall under the same reasoning as the databind
HIGHs below, and the gate does not fail on MEDIUM.

## Reachability

I read the constant pools of every class in the jar: which classes reference the shaded packages,
and which jackson methods they call (`javap -c`).

- **Both databinds are dead code.** No class outside the two databind packages references either
  of them: not Hazelcast's own classes, not jackson-jr, not the other shaded libraries, not by a
  dotted name for reflection. Their `META-INF/services` entries are keyed by the shaded
  `ObjectMapper`/`JsonFactory` types, which nothing loads. So no databind advisory has a path,
  whatever a member's config is.
- **jackson-core 2.21.2 is dead code too.** Only the 2.x databind references it.
- **jackson-core 3.1.2 is used**, by eleven Hazelcast classes:
  - HazelcastJsonValue metadata on put: `JsonMetadataInitializer`, `JsonMetadataMutationObserver`,
    `JsonSchemaHelper`.
  - Queries over JSON values: `JsonGetter`, `JsonDataGetter`, `AbstractJsonGetter` and the
    navigable JSON adapters.
  - Jet's JSON sources and sinks: `jet.json.JsonUtil` and `JsonSerializerHooks`, through jackson-jr.

  Every one of them creates a blocking parser over `byte[]`, `String`, `Reader` or `InputStream`.
  `DataInputNavigableJsonAdapter` wraps its `DataInput` as an `InputStream`. None of the
  jackson-core advisories is on those paths:
  - CVE-2026-68494 is in the non-blocking (async) parser. Only databind creates one
    (`ObjectMapper`/`ObjectReader`).
  - CVE-2026-89425 is in `UTF8DataInputJsonParser`, built only by `createParser(DataInput)`.
    Nothing outside jackson-core calls that.
  - CVE-2026-89407 is in `NumberInput.looksLikeValidNumber`. Only databind's string-to-number
    coercion calls it.

Stratum adds no path of its own:
- It never references `com.hazelcast.shaded.*` or `HazelcastJsonValue`.
- Its records are proto messages (`ProtoMessageSerializer`), and its queries scan in Java, with no
  Hazelcast predicate.
- Its members build their config in code (`StratumMapConfig.build`), not from JSON or YAML files.
- Every surface that speaks JSON is off: the REST API (classic and new), memcache, the advanced
  network's extra endpoints, Jet (and with it SQL), user code deployment, and Management Center
  scripting.

The one JSON-parsing path left reachable is the jackson-core 3 *blocking* parser on a
HazelcastJsonValue. Something has to put one into a map, and only a client or member that talks to
the member port can do that. That port admits by cluster name and address alone (#17, open). Such
a peer can already read and write the tenant's every record, which #17 is about, and the parser it
would reach carries none of these advisories.

`server/.../hazelcast/ShadedJacksonReachabilityTest` keeps this evidence. It fails if any of these
changes:
- a class outside the shaded jackson references either databind or jackson-core 2.x;
- a user of jackson-core 3 names a non-blocking parser, a `DataInput` parser or
  `looksLikeValidNumber`;
- stratum's classes touch the shaded packages or `HazelcastJsonValue`;
- one of the JSON surfaces is turned on.

## Decision

Each of the eight advisories is accepted for exactly the shaded packages above, until
**2026-12-03**, in `.trivyignore.yaml` (purl-scoped, so the unshaded jackson 2.18.12 is not
covered). `just audit-jar`, now part of `just gates`, scans the built `stratum.jar` with trivy and
fails on HIGH or CRITICAL. On expiry it fails again, so the exception is re-examined, never
silently renewed. A Hazelcast bump that changes the reachability fails the test first.

## Upstream

Hazelcast's answer on hazelcast/hazelcast#26597 and #26623 (a Hazelcast contributor, before this
issue): OSS gets no patch releases, so the fixed jackson ships in the next minor or major release,
with no date given. Our request (a fixed release, its date, and dropping the unused databinds):
https://github.com/hazelcast/hazelcast/issues/26597#issuecomment-6601724196. When a fixed release
ships, bump `hazelcast.version` and delete these entries.

An unaffiliated fork that re-shades jackson 2.21.7 / 3.1.7 is advertised in those threads
("Opencast", from its own Maven repository). It is not adopted here: it swaps the provenance of the
store's core dependency, and that is Jody's call, not an exception's.
