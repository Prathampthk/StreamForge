# ADR 003: sink deduplication (PROPOSED; unresolved Phase 3 gate)

A deterministic batch ID and durable immutable manifest could preserve exact row membership/checksum/offset range across ambiguous retry and rebalance. Proposed manifest fields: batch_id, topic, partition, start/end offsets, payload checksum, row count, schema version, pending/confirmed state, timestamps. The partition owner would create before insert and recover before committing. No manifest table or runtime is implemented here.

Bounded ClickHouse insert deduplication windows and tokens alone cannot guarantee indefinite logical-event uniqueness. Partial view writes, changed batching, finite dedup windows and same event ID at multiple Kafka offsets remain failure modes. A PostgreSQL manifest is not an atomic transaction with ClickHouse and does not solve logical duplicates.

Alternatives: durable per-event identity ledger (extra write throughput and cross-store consistency, requires explicit flow-change approval); canonical event-ID facts with refreshable/recomputed aggregates (correctness/latency/cost tradeoff); narrowly bounded block dedup (insufficient for full requirement). Proposed direction: evaluate canonical per-ID data plus rebuildable aggregates against the five-second freshness target before approving Phase 3. Same ID/same payload should yield one logical contribution; same ID/different payload must be rejected or quarantined visibly, never silently replaced. Exact collision policy and ownership must be settled before runtime coding.

No strategy is accepted yet. Phase 1 schema demonstrates unique synthetic data only. Benchmark and worker-crash correctness gates remain unfulfilled.
