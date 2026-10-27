# Roadmap: stratum

Owned by Jody; changed only by an agent at Jody's request. Loopers propose in PROPOSALS.md.

## Now
Stratum is the platform's record store: it hosts the tacplus-declared records (`(spec.record)`) and serves the record query plane.
Unreleased; tacplus floor 2.4 (#1). The code is still the pre-2026-10-25 design (writer/search roles, volumes, bus ingest).
The platform's highest priority: the per-tenant, in-memory rebuild (#2), on the path tacplus#163 → redo#1 → #2.

## Next
- Add a `just gates` recipe (#13)
- Bump logback to 1.5.38 (#12)
- Javalin 6.7.0 / Jetty 11 advisories: drop Javalin or move to Javalin 7 (#11)
- Hazelcast 5.5.0 CRITICAL advisory: bump to 5.7.0, now or folded into #2 (#10)
- Remove unused calcite-core (#9)

## Later
- Per-tenant, in-memory Stratum, durability through the redo service (#2), after tacplus#163 and redo#1
- Release: tag v0.1.0 and publish the image (#3), after tacplus-dev/looper#12
- Port SeaBeam stratum 864f70f2..6cb0d4fb (#5), after #3
- Bump Bouncy Castle to 1.86 (#7), after #13
- Pin jackson-bom 2.18.12 (#8), after #13
- Base image to distroless java21-debian13 (#14), after #13
- Cross-cluster black box: layer-4 path, member discovery and member certificates across clusters (tacplus-dev/roadmap:decisions/stratum-per-tenant-2026-10-25.md, Open)
- Region failover: active/passive clusters, passive Stratum tails the redo, witness lease (tacplus-dev/roadmap:decisions/stratum-per-tenant-2026-10-25.md, Kept for later)
- Transactions across maps (tacplus-dev/roadmap:items/stratum-transactions.md, held)
- Multi-cluster (tacplus-dev/roadmap:items/multi-cluster.md, held)

## Not doing
- Durable state in Stratum (volumes, redo log, checkpoint, disk index): durability is the redo service (decisions/stratum-per-tenant-2026-10-25.md)
- Ingest from the event bus: records are written through Stratum's own generated API; eventbus is no one's database (decisions/stratum-per-tenant-2026-10-25.md)
- Replacing Hazelcast: Hazelcast stays in Stratum (decisions/platform-direction-2026-10-23.md)
