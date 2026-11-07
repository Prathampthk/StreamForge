# Architecture: Phase 3

Implemented flow: HTTP ingestion validates/authenticates and waits for Kafka all-ISR acknowledgment → members of a Go consumer group process assigned partitions in bounded batches → synchronous ClickHouse raw insert plus incremental identity-state MV → next source offset commit. Periodic canonical revenue/traffic snapshots derive one contribution per unambiguous logical event ID. PostgreSQL remains control-plane metadata off the event path. Query API is Phase 4 work.

Kafka remains local single-node KRaft, 12 event partitions, RF1/min ISR1, seven-day retention, 1 MiB broker record cap, unsafe election disabled. Clients bind tenant_id:user_id keys through Java-compatible murmur2; same key follows one partition, with no global/tenant-wide ordering guarantee. HTTP event cap is 8 KiB. Headers include schema_version, correlation_id, produced_at and optional traceparent. Producer idempotence is not logical HTTP deduplication.

ClickHouse events_raw is forensic append evidence with 30-day event-time TTL. `event_identity_states` incrementally merges exact normalized min/max payload strings by global event_id. `events_canonical` exposes one row only when min=max; `event_identity_conflicts` exposes ambiguous identities and excludes them from final analytics. Revenue and traffic each have a one-second atomic refreshable snapshot. Final sources are revenue_1m_canonical and traffic_1m_canonical. The old Phase 1 append aggregate states remain diagnostic only because replays can inflate them. Identity states intentionally retain history indefinitely; lifecycle/scale is a known cost, not a hidden guarantee.

Snapshots can be stale after a failed/slow refresh; Phase 4 must check system.view_refreshes rather than assume freshness. Full canonical refreshes have growing cost and do not establish the <5s SLO. See ADR 003 for why this correctness-first shape replaces direct append-based final sums, and consumer.md for ownership/retry/commit details. Same-ID changed payloads quarantine the whole identity and can withdraw a previously visible contribution; there is no silent replacement or implemented replay resolution.

Pins: Kafka 4.0.2, ClickHouse 25.8.4.13, PostgreSQL 17.6, Go 1.26.0; review security patches before external exposure. All host ports bind loopback, sample credentials are development-only, no AWS credentials are needed. Budget >=6 GiB Docker memory and >=10 GiB free disk initially; infrastructure caps total 4.5 GiB plus ingest 256 MiB and each consumer 512 MiB. Three consumers plus test/build containers require additional Docker headroom.

Initial migrations are idempotent object creation, not a general migration engine; add versioned migrations for changes. No destructive backfill of prior synthetic rows is automatic. Local named volumes survive make down. The system has no redundant broker or ClickHouse disk and no proof of survival under host/disk loss. Tests prove specified local replay/crash behavior, not unqualified exactly-once semantics or achieved performance targets.

References checked against pinned behavior:
- https://kafka.apache.org/40/configuration/producer-configs/
- https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo
- https://clickhouse.com/docs/materialized-view/incremental-materialized-view
- https://clickhouse.com/docs/materialized-view/refreshable-materialized-view
- https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree
- https://www.postgresql.org/docs/17/sql-createtable.html
