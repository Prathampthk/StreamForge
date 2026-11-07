# Failure model and current limits

Phase 1 validates infrastructure and SQL shape; Phase 2 validates ingestion-to-Kafka behavior. A single broker and single ClickHouse host are non-HA. Persisted volumes survive make down, but host/disk loss is outside local durability. Phase 2 has accepted synthetic events with confirmed local Kafka promises.

Broker-failure claims require a separate three-broker test: RF=3, min ISR=2, acks=all, verified producer idempotence, unsafe leader election disabled. Producer idempotence does not deduplicate logical HTTP retries. Producer compression and bounded retries must be verified against the Phase 2 client, not presumed here.

Phase 3 planned commits: disable auto commit; commit next offset after highest contiguous persisted or durably quarantined record per assigned partition. Never skip a failed earlier offset. SIGKILL after sink insert and before commit produces replay. Ambiguous insert outcomes and partition revocation must preserve retry identity. DLQ outage must block source commit. Kafka retention exhaustion can lose unprocessed backlog; alert well before seven days.

The current append table and incremental views double count duplicate rows. ReplacingMergeTree merges do not undo prior view contributions. Phase 3 cannot pass until duplicate IDs, conflicting payloads, ambiguous writes and rebalance recovery have an approved proven strategy. There is no exactly-once claim.

Migrations are initial idempotent DDL, not a general migration engine: reruns create absent objects and do not upgrade existing definitions. Never edit an applied file to change schema; add a version. PostgreSQL initial DDL is transactional; ClickHouse DDL is not transactional, so rerun after a partial failure. Concurrent migration runs are unsupported. Down SQL is destructive and never invoked by automation.

Kafka acks=all confirms log acceptance by the in-sync replica set, not an fsync-per-request or survival of host/disk loss. The local RF=1 configuration has no redundant copy.
