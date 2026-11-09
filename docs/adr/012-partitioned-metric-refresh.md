# ADR 012: partition-bounded exact metric refreshes

## Measured trigger

The exact retained migration completed for 15,871,442 identities (full min/max strings, not hash-only verification). Sixteen conflicts remain excluded, leaving 15,871,426 canonical events. This did not repair full-history refresh memory: pinned 25.8 produces sixteen aggregation streams despite max_threads=2; a sorted-stream consolidation also exceeded 256 MiB during source reads. Count-only reads sometimes passed but do not establish usable business projections.

A combined traffic/revenue GROUPING SETS query restricted to one existing UUID-hash partition completed over 992,254 physical rows in 2,119ms with 8,576,379 bytes of recorded query memory. This is a read prototype, not a freshness/SLO result.

## Decision and files

Add migration 007 with sixteen native refreshable projections, one per immutable `cityHash64(event_id)%16` partition, and compact aggregation into the existing two served metric tables. Each source query reads canonical min/max values for one partition, uses exact Int128 sums/counts and a 64 MiB/one-thread budget. Four-second schedules with offsets 0–3 stagger work. The target freshness SLO remains p95 <5 seconds; this interval is a CPU/memory tradeoff to be measured, not a relaxed target.

A UUID's original, duplicate and conflicting variants always occupy the same source partition. A conflict still excludes the entire identity and withdraws previous contributions. Each shard atomically replaces its own metric cache. The final endpoint snapshots atomically replace independently; shards can reflect different observation times. No global transactional snapshot or Kafka processed watermark is promised.

`internal/analytics/read.go` checks both wrapper and all sixteen shard metadata rows, fails closed for missing/failed/disabled shards, and uses the earliest successful refresh start across those components. The pinned system.view_refreshes schema defines last_success_time as the start of the successful attempt, not its completion. This is conservative refresh metadata, not an event-time/ack watermark. The final refresh queries also refuse failed/incomplete shard input. Existing query grants on system.view_refreshes remain sufficient.

Files: migration 007, query-health implementation/regression, this ADR and current evidence/runbooks. No additional daemon, per-event PostgreSQL ledger, payload-hash identity policy, raw deletion, TTL change or AWS work. Existing canonical identity views remain authoritative; only derived snapshot work is partitioned.

## Validation gates

- Golden revenue/currency/traffic/late/duplicate/conflict/Int128 results remain exact.
- Stopping any source shard must reject API traffic even if a wrapper has a recent success.
- All sixteen retained projections must succeed; aggregate counts must match the verified canonical source; keep the original failed experiments.
- Run the sustained concurrent benchmark only with a disclosed starting state and retain every query/freshness failure. Full-history work still grows; the sharding is a bounded-memory projection strategy, not incremental work or HA.

Rollback changes only derived refresh definitions and query-health expectations; it does not delete or reverse the verified identity migration. Reverting to a known memory-failing full scan is not a performance recovery claim.


## Retained rollout result

Isolated integration passed in 115.502s. All sixteen retained caches have completed runs and the served traffic total matched 15,871,426 canonical events, but several sources still intermittently fail the 3.60 GiB server-wide memory limit. Wrapper/API health rejects these states. The new concurrent benchmark remains gated on readiness; no SLO pass follows from the exact total. Restarting with allocator-based tracking preserved data and limits but did not resolve failures; that configuration experiment was reverted.
