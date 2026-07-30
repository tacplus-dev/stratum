# Stratum durability contract

## What stratum is

Stratum is a **hot store**. Its record store is an in-memory Hazelcast
`IMap<event_id, Message>` held by a single member (1 replica; no persistence, no
MapStore, no hot-restart). Nothing stratum holds survives its own process.

**Every record stratum serves is reconstructable from the event log, and that is the
only reason stratum can lose its process without losing data.**

On every startup, stratum replays each subscribed EventBus topic from offset 0 into
the store (`ColdStartRebuilder`). Replay is idempotent — the map is keyed by
`event_id`, so a `put` for a record already present is a no-op — and runs exactly
once per process, on every start, regardless of how many records the store holds.

## Consequences that follow

- **Stratum's retention IS the event log's retention.** If the broker drops a record
  because it aged out, stratum permanently loses that record at its next restart.
  Stratum's effective history is `min(what was ever published, broker retention)`.
  Retention is an eventbus/Kafka configuration decision, so changing it silently
  changes what stratum can serve.
- **Recovery time scales with log size.** Startup replays the whole log. That is fast
  today (50 records in under 4 seconds) and will not stay fast. When it stops being
  fast, the answer is a snapshot/checkpoint — not skipping the replay.
- **A partially populated store is repaired, not trusted.** Replay is unconditional
  precisely so a store that came back non-empty but incomplete converges instead of
  silently serving a subset.

## What stratum does NOT guarantee

- **It is not the system of record.** A record's presence in stratum is not proof it
  happened, and its absence is not proof it did not. The event log is the truth.
- **It does not survive a lost event log.** Wiping the Kafka data directory wipes
  stratum's recoverable history with it. That has been done deliberately several times
  during development; it is data loss, not a reset.
- **It is not highly available.** One replica. While stratum is down, queries fail —
  they do not degrade.

## Verification

`ColdStartRebuilderTest` pins the replay decision. The live proof is P5 drill #6
(`control/runs/2026-07-30-p7/DRILL6-PROOF.md`): stratum was force-killed while a
publisher was mid-run, 20 events reached Kafka while it was dead, and all 53 events
were present after restart — zero loss.

Note what that drill does and does not show. The store came back **empty**, which is
the only case this deployment can produce, so the drill does not exercise partial
survival. That path is unit-tested only. It exists so that adding Hazelcast
persistence or a second member later cannot silently reintroduce loss.
