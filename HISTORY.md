# History: stratum

Append-only: add entries at the end, never edit or remove one (CI enforces it). One line each:
`- <YYYY-MM-DD> <who: looper|agent|jody> <kind: issue|decision|release|deploy> <one line> (<links>)`

- 2026-10-27 looper issue Javalin 6.7.0 brings Jetty 11.0.25 (2 HIGH advisories); fixes exist only on Jetty 12 / Javalin 7 (#11, 764e82c)
- 2026-10-27 looper issue logback 1.5.29 has 2 LOW deserialization advisories: bump to 1.5.38 (#12, de30c81)
- 2026-10-27 looper issue No gates: justfile has no `gates` recipe (#13, c4b43c0)
- 2026-10-27 looper issue Bouncy Castle 1.80 has 2 CRITICAL advisories: bump bcprov/bcpkix to 1.86 (#7)
- 2026-10-27 looper issue Jackson 2.18.3 (via google-cloud-storage) has 9 HIGH advisories: pin jackson-bom 2.18.12 (#8)
- 2026-10-27 looper issue Hazelcast 5.5.0: CRITICAL client-to-member memory access, fixed only in 5.7.0 (#10)
- 2026-10-27 looper issue Base image gcr.io/distroless/java21-debian12 is not on the latest Debian line: use java21-debian13 (#14, af1db81)
- 2026-10-28 looper issue Guava 33.5.0-android (via grpc-protobuf) has a HIGH deserialization advisory: pin 33.7.2 (#15, 5b9c2ef)
