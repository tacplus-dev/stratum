# Stratum — Design

## Purpose

Stratum is the platform's **record store**: it hosts tacplus-declared records
(`option (spec.record)`) for fast lookup and serves the query planes built on
them. How records are physically stored — in-memory maps, disk, archival tiers —
is stratum's own concern; tacplus declares the record contract, stratum decides
the storage strategy.

Stratum is also the Supervisor LLM's stateful SQL workspace: a dynamic MCP tool
surface over the records it holds (distributed working-set maps, a durable
scratchpad, async job management, a SQL engine across loaded data). The
Supervisor is a persistent single brain — not a stateless request handler — and
stratum provides the infrastructure for that persistence.

## Current shape (post proto-split cutover, 2026-07-29)

- Language: Java. Declares `record_store` in `stratum.proto`; generates from
  `[proto-base, proto-app]` because it hosts records for base and app vservices
  alike (agentsensor, llmsensor, scmsensor, gitlabconnector).
- **RecordMap plane** (generated): typed record maps per record type with keyset
  paging; stratum's service-local codegen (`stratum-codegen`, a registry.json
  consumer — only tacplus reads protos) emits the store wiring.
- **StratumQuery vservice** (hosted here): the query surface consumers use —
  argus delegates its session queries (paged lists + `subscribe_live_sessions`)
  to it. Implementation over the record store is the active P3 work item; the
  current handlers are compile-bridge stubs and MUST become real before P4.
- **Events in**: record-bearing topics arrive via eventbus subscriptions; the
  1.2-era record watch plane is deliberately gone (no consumer; revisited only
  if the e2e demands live record subscriptions).

## Rewrite note

The pre-cutover version of this document described a retired storage-tier
architecture and no longer reflected the system. This stub is intentionally
minimal and truthful; the P3 stratum pass (StratumQuery implementation) should
expand it with the storage-tier design it actually builds.
