# Architecture: Phase 2

Smallest slice: Kafka + relational metadata + analytical fact and minute-state tables, with real synthetic SQL checks. Phase 2 adds the ingestion HTTP service only.

Implemented path: HTTP ingestion → Kafka acknowledgment; future continuation: Kafka → assigned partition consumer → confirmed ClickHouse insert → source offset commit. Query API reads merged minute states. PostgreSQL is off the event hot path. Kafka headers include schema_version, correlation_id, produced_at and optional traceparent.

Local pins: Kafka 4.0.2 (KRaft), ClickHouse 25.8.4.13, PostgreSQL 17.6, Go toolchain 1.26.0. These are reproducibility pins, not a claim of latest security patches. Review patches before external deployment. Kafka has 12 partitions, RF=1, min ISR=1, seven-day retention and 1 MiB broker record cap. Application event cap is 8 KiB. Local plaintext networking binds published ports to loopback. No AWS credentials needed. Budget at least 6 GiB Docker memory and 10 GiB free disk; service memory caps total 4.5 GiB. Databases start independently because neither depends on another; migrations run only after all health checks pass.

Raw MergeTree retention is 30 days of event time; aggregate retention is currently indefinite. TTL deletion is asynchronous. Raw expiry does not retract minute-state contributions. New materialized views do not backfill old rows automatically. Future backfills need a separately controlled procedure.

References checked for Phase 1:
- https://kafka.apache.org/40/getting-started/docker/
- https://kafka.apache.org/40/configuration/producer-configs/
- https://clickhouse.com/docs/materialized-view/incremental-materialized-view
- https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree
- https://www.postgresql.org/docs/17/sql-createtable.html
- https://go.dev/doc/go1.26

See ADRs for unresolved correctness gates. The ingestion API returns 202 only after successful Kafka producer promises. No sink deduplication, broker-failure durability proof or performance target is established. See ingestion.md and ADR 004.
