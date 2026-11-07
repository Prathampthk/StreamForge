# ADR 003: canonical identities and refreshable metrics (accepted Phase 3)

## Decision and invariant

Keep append-only events_raw as forensic delivery evidence. Add payload_json, the normalized logical event serialized deterministically: lowercase UUID, UTC RFC3339Nano instant, sorted attributes, and equivalent absent/null optionals. Sink/Kafka coordinates, receive time, batch IDs and headers are not logical payload. An incremental materialized view stores min/max **full normalized strings** per global event_id in AggregatingMergeTree. Grouping with minMerge/maxMerge works before physical merges. Equality yields exactly one canonical contribution. Inequality quarantines the entire identity in event_identity_conflicts, exposes payload hashes for inspection, and excludes it from events_canonical. Comparisons use exact strings; hashes are diagnostic only.

No payload wins a conflict silently, even if the tenant, event type or minute differs. A previously visible contribution is withdrawn on the next successful metric refresh when a later conflicting payload appears. Both raw variants remain available for investigation; conflict resolution/replay is not implemented. Canonical reads exclude conflicts immediately; minute snapshots converge after refresh. Identical replays never multiply canonical revenue/counts.

Revenue and traffic have separate one-second refreshable replacement snapshots, revenue grouped by currency. Each refresh replaces its target atomically; the two targets are not one cross-table transaction and may briefly reflect different source snapshots. Revenue sums use Int128 to avoid summing valid Int64 money into a narrow accumulator. Existing Phase 1 incremental sum tables are **diagnostic only**, still run for legacy smoke compatibility, and must never be exposed as final analytics. Phase 4 must read *_canonical tables and report/check refresh status.

## Retry, ownership and failure model

Synchronous inserts disable insert deduplication and async inserts, require materialized-view errors to propagate, and wait for query completion. A raw/MV partial write or lost response can produce extra raw and identity-state rows; repeat the complete immutable batch until confirmed, then commit only its partition's next offset. Min/max states are idempotent under repetition, independent of batch boundaries, producer sessions and ClickHouse block dedup windows. A deterministic batch ID (topic, partition, ordered offsets and payload checksum) is diagnostic; no PostgreSQL manifest is needed for correctness because arbitrary replay grouping is safe. No per-event PostgreSQL ledger or new control-plane flow is introduced.

Commit after successful source insert includes successful synchronous identity MV execution, not completion of a refreshable minute snapshot. Refresh failures leave the previous snapshot intact; event data/identity states remain durable and the scheduled retry rebuilds it. Expose/check system.view_refreshes; the application must not call stale metrics current. Local disk loss, deleting tables, hash-collision assumptions in Kafka partitioning, or broker retention exhaustion are not solved. Source JSON is validated independently of current clock age so backlog does not turn into poison merely by waiting.

## Alternatives and tradeoffs

- ReplacingMergeTree alone and bounded block dedup tokens cannot retract duplicate incremental sums or cover indefinite retries/HTTP duplicate IDs.
- A per-event PostgreSQL ledger adds a hot-path write and a cross-store transaction problem, and would require separate flow-change approval. Not used.
- Per-batch manifests preserve batch shape but do not solve logical IDs and changed batching; unnecessary for this idempotent projection.
- Full raw scans would be simpler but repeat duplicate delivery history. Incremental identity states reduce redundant payload history before periodic canonical recomputation.

Refresh scans still grow with retained unique identities. No 10k/s or <5s freshness claim follows from a one-second schedule. This correctness-first baseline can miss those targets as data grows; Phase 5 must measure cost/freshness and propose an evidence-backed redesign if needed. Identity states intentionally have no TTL (prevent forgotten IDs becoming new contributions); raw keeps 30-day TTL. State-retention/storage growth and privacy deletion need an explicit lifecycle decision before external deployment. Legacy pre-Phase-3 synthetic rows have no payload_json and are not backfilled; all actual ingestion records remain replayable from Kafka. No automatic destructive migration/backfill is run.

## Consumer model

One poll owner bounds fetched records/flush interval. BlockRebalanceOnPoll protects assignment while bounded partition tasks run; no partition has concurrent tasks. An epoch has one bounded processing deadline shorter than rebalance timeout, then successful partitions are committed serially and rebalances are allowed. Revoked/lost callbacks never commit unprocessed work. On processing/commit errors exit for supervised recovery, preserving uncommitted backlog. Poison stops forward progress without a source commit; a durable DLQ path is deferred to Phase 5. Kafka log offset gaps are allowed, but observed record order cannot regress; no commit skips an unprocessed delivered record.

## Verification planned

Repeat exact rows and differently grouped batches, same-ID changed tenant/minute/amount conflict, raw/identity/metric reconciliation, response lost after a successful actual insert, worker SIGKILL boundaries, ClickHouse outage/recovery, and real 1→3 assignment/rebalance. Zero missing accepted IDs and canonical exact totals are the gate, not unique physical raw rows. Live tests must prove the pinned ClickHouse 25.8 syntax and refresh behavior.

References: https://clickhouse.com/docs/materialized-view/refreshable-materialized-view ; https://clickhouse.com/docs/materialized-view/incremental-materialized-view ; https://clickhouse.com/docs/engines/table-engines/mergetree-family/aggregatingmergetree .
