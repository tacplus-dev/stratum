# State: stratum

Overwritten to stay current (loopers and agents). Last updated: 2026-10-29 by the looper (#18).

- **Version / line:** no release yet (no vX.Y.Z tag); tacplus floor 2.5 (the record write surface and the redo contract, tacplus#163)
- **Works:** the per-tenant, in-memory record store (#2): writes through the generated write surface (Put, InsertIfAbsent, ReplaceIfUnchanged, Delete, Apply) applied only after the redo service acks them, Get with revisions, Watch<T>Changes, replay from the redo at every start; no event-bus ingest, no volumes. Each tenant's members form cluster `stratum-<tid>` (tid from the instance's own SPIFFE identity) by TCP/IP join to `STRATUM_SEED_MEMBERS` only; never multicast, and no seeds means no start (#18). `just gates` passes on main: codegen 34 tests, server 59, 0 failures
- **Released / deployed:** nothing released from this repository yet; the operator does not render the per-tenant Stratum yet (PROPOSALS.md)
- **Known gaps:** Hazelcast member traffic is not authenticated or encrypted, so admission is by cluster name and address alone (#17, waiting on Jody); soft-deleted rows are not purged; REMOTE writes are refused until redo#1 step 2; no test against a deployed redo; a proto change cannot be generated before it is on main (tacplus#205); no dependency audit in the gates (PROPOSALS.md)
- **Blocked on:** nothing
