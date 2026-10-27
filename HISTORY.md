# History: stratum

Append-only: add entries at the end, never edit or remove one (CI enforces it). One line each:
`- <YYYY-MM-DD> <who: looper|agent|jody> <kind: issue|decision|release|deploy> <one line> (<links>)`

- 2026-10-27 looper issue Javalin 6.7.0 brings Jetty 11.0.25 (2 HIGH advisories); fixes exist only on Jetty 12 / Javalin 7 (#11, 764e82c)
- 2026-10-27 looper issue logback 1.5.29 has 2 LOW deserialization advisories: bump to 1.5.38 (#12, de30c81)
- 2026-10-27 looper issue No gates: justfile has no `gates` recipe (#13, c4b43c0)
