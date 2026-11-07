# Consumer operations and verified guarantees

## Run locally

After `make up` and `make migrate`, run `make ingest-up`, `make consumer-up`, then `make consumer-smoke`. The API and consumer are separate Go binaries. Docker builds pin Go 1.26.0. Consumer replicas do not publish a host port, so scaling requires no host-port changes:

```sh
docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.ingest.yaml -f deploy/compose.consumer.yaml up -d --scale consumer=3 consumer
make ps
make test-integration
```

For metrics/readiness from a replica, use Docker's internal endpoint `http://consumer:8081` or run `/consumer healthcheck` inside it. `/livez` is process-only. `/readyz` requires a running poll loop, at least one assigned partition and a successful bounded ClickHouse schema query. It does not assert metric snapshot freshness. Stop with `make consumer-down` (SIGTERM); `make down` stops the full local project and retains data volumes. Never scale beyond the 12 useful partition owners expecting additional parallelism.

Compose retries a failed consumer process with `restart: on-failure`. A failed insert gets bounded retries inside its processing epoch; if exhausted the process exits, releases ownership and leaves failed offsets uncommitted. Kafka remains the backlog. A restart or another owner safely retries with potentially different batch boundaries. Persistent outages must be investigated before Kafka's seven-day retention expires; restart alone does not guarantee infinite retention. Structural poison uses the durable audited quarantine path described in [replay.md](replay.md); transient sink outages remain in Kafka.

## Offset and concurrency invariant

One goroutine owns polling. Each epoch collects up to batch-record limit × worker count over at most 250ms, stopping when its configured byte budget is reached. Compose uses 1000 records/chunk and an 8MiB collection budget; Go defaults remain 128/1MiB. A final poll can overshoot the byte target, and client fetch buffers are additional memory. Partition tasks run in order with at most four active insert requests per instance; only one task processes a partition in an epoch. At most 12 partition goroutines exist, all joined before rebalance/commit or error return. Rebalances are blocked after nonempty polls. Processing has a ten-second budget and the one serial commit operation a three-second budget, leaving >5s margin inside the 30-second rebalance timeout. No callbacks perform sink I/O or commit speculative work.

Each successful partition supplies its last persisted record; franz-go CommitRecords commits that record's offset+1. It includes only the highest safely persisted or durably quarantined contiguous prefix from each partition. A later delivered record cannot bypass an earlier failure in its partition. Kafka offset gaps (e.g. control records) are allowed; delivered offsets must increase. Commit failures, lost generations and cancellation return without inventing completion; already persisted rows may replay. Revoked/lost callbacks only report ownership. A sink or quarantine failure exits after joining workers and committing safe prefixes. Healthy records before poison can flush and commit in the same epoch; the failed record and following tail remain uncommitted. A durable Kafka DLQ acknowledgment plus PostgreSQL audit is required before poison can be passed.

Fetch settings bound one concurrent fetch, 2 MiB total response target and 1 MiB per partition target. Kafka may exceed a fetch-byte target for the first oversized record so progress is possible; broker message limit=1 MiB is the additional physical bound. Collection accounts for key/header/value bytes and per-record overhead, stops at its byte target and processes per-partition chunks <=batch records. A final poll and Kafka client buffers are additional bounded-count/fetch memory; the byte setting is not a total process heap guarantee. Keep the 512MiB consumer cap consistent with record/byte/worker tuning. No unbounded queue or per-record goroutine is created.

## Sink and identity model

See ADR 003. The synchronous HTTP insert explicitly sets async_insert=0, insert_deduplicate=0, wait_end_of_query=1 and materialized_views_ignore_errors=0. It requires HTTP 200 with an empty success body. Transport timeouts, a nonempty execution-error body even with HTTP 200, and partial results are unconfirmed. The full immutable JSONEachRow batch is retried; source offsets remain uncommitted until confirmation. Connections are bounded by worker count, response bodies by 64 KiB, requests by attempt timeout, and server execution budget by ten seconds. A timed-out server-side insert may finish later; canonical identity makes that late completion safe. These are replay-safe application semantics, not an atomic Kafka/ClickHouse transaction or a disk-failure durability guarantee.

Batch IDs deterministically hash source topic, partition, ordered offsets and original payload bytes. New owners can regroup differently; final correctness does not depend on stable grouping or ClickHouse dedup windows. No PostgreSQL batch-manifest table is needed, and no per-event PostgreSQL write was introduced.

Logical normalization lowercases UUIDs, expresses occurred_at in UTC with up to nanosecond precision, marshals fields/attribute keys deterministically and treats allowed null/absent optional fields equivalently. Full normalized strings—not hashes—decide identity equality. New different payloads for one global event ID quarantine the whole identity and withdraw it from the next metric refresh. This includes changes to tenant, currency, amount or minute. Inspect `event_identity_conflicts` for IDs/hashes and `events_raw` for the retained forensic variants; there is no automatic conflict-resolution API. Normal identical duplicates never multiply canonical counts or money.

`received_at` in raw rows is the Kafka record CreateTime (producer timestamp), not broker acknowledgment or ClickHouse visibility time; it is retained across retries. `occurred_at` raw precision is milliseconds for Phase 1 compatibility, while payload_json/canonical time preserves nanoseconds. Kafka headers retain original ingress correlation/tracing data. Consumer structural validation does not reapply the current wall-clock lateness policy to aged backlog.

Final metric sources are **revenue_1m_canonical** and **traffic_1m_canonical**. They atomically replace their own snapshots every second after querying merged identity states; their snapshots are not a shared cross-table transaction. Legacy revenue_1m/traffic_1m append states can double count replays and are diagnostic only. No application exposes them as final analytics. The query API checks refresh health and uses only these canonical sources.

## Refresh health and retention

```sql
SELECT view, status, last_success_time, exception
FROM system.view_refreshes WHERE database='streamforge';
SELECT event_id, min_payload_sha256, max_payload_sha256
FROM streamforge.event_identity_conflicts;
```

A failed refresh retains its previous target. Scheduled retries can rebuild from identity state after recovery; source offsets need not rewind because data is preserved. Refresh queries cap execution at 15s and memory at 256MiB each, two threads and a 64MiB external aggregation threshold (migration 003). A one-second schedule is not proof of <5s freshness: full canonical scans grow with unique IDs and can fall behind or fail as history grows. Phase 5 measurements demonstrate the growing-history cost and failed freshness/readiness targets; see the report. Raw facts expire after 30 event-time days, identity states intentionally do not expire, and metric snapshots retain all canonical history. Define a reviewed archival/deletion lifecycle before external deployment. Legacy raw synthetic fixtures without payload_json are not silently guessed/backfilled; actual pre-Phase-3 ingestion messages are consumed from retained Kafka.

## Configuration

Consumer settings (environment, defaults):

| Setting | Default / bound |
|---|---|
| KAFKA_BROKERS / KAFKA_TOPIC | localhost:9092 / streamforge.events.v1 |
| CONSUMER_GROUP | streamforge-sink-v1; nonempty <=128 chars |
| CONSUMER_HTTP_ADDR | 127.0.0.1:8081; Compose 0.0.0.0:8081, no host publication |
| CLICKHOUSE_URL | http://localhost:8123; http(s) origin, no embedded credentials/query/path |
| CLICKHOUSE_USER / CLICKHOUSE_PASSWORD | streamforge / required environment secret |
| CONSUMER_BATCH_RECORDS | Go 128 / Compose 1000; 1..1000 |
| CONSUMER_BATCH_BYTES | Go 1048576 / Compose 8388608; >=records*8192, <=8 MiB |
| CONSUMER_WORKERS | 4; 1..12 |
| CONSUMER_INSERT_ATTEMPTS | 3; 1..10; exponential jitter between attempts |
| CONSUMER_FLUSH_INTERVAL | 250ms; 10ms..1s |
| CONSUMER_INSERT_TIMEOUT | 2s; 100ms..10s |
| CONSUMER_PROCESS_TIMEOUT | 10s; >=insert timeout, <=30s |
| CONSUMER_COMMIT_TIMEOUT | 3s; 1s..10s |
| CONSUMER_REBALANCE_TIMEOUT | 30s; >=process+commit+flush+5s, <=60s |
| CONSUMER_SHUTDOWN_TIMEOUT | 8s; 1s..60s for health-server drain; increase Compose 20s grace if budgets increase |
| TRACE_SAMPLE_RATIO / OTEL_EXPORTER_OTLP_ENDPOINT | 0.1 / optional exporter endpoint |

Failpoints are disabled unless ALLOW_TEST_FAILPOINTS=true and CONSUMER_FAILPOINT is one of before_insert, after_insert, before_commit or during_retry. They log a marker and wait for context cancellation for controlled local kill experiments. Do not enable them in normal development or deployments. Integration subprocess helpers exercise the same consumer engine; the in-rebalance test adds a member and verifies the broker enters PreparingRebalance before killing the uncommitted old owner.

Metrics expose finite partition labels (0..11), current ownership, confirmed next offsets, observed lag, batch sizes, insert duration including retries, attempt failures and rebalance callbacks. Lag is sampled from fetch high-watermarks and is not a continuous cluster-wide lag audit; it can become stale while idle. Logs report assignment and successful commits with group/topic/partition/offset; no payloads or credentials. Consumer spans inherit the first record's trace context in each partition batch, not a separate span for every record.

## Test evidence and scope

`make test` runs contracts/unit/race tests. `make test-integration` uses real Kafka and ClickHouse on the Compose network, creates isolated test topics and deletes only those test-owned topics during cleanup. Small forensic fixtures remain in ClickHouse. It verifies identical/differently grouped retry inserts, exact normalization, visible cross-tenant/minute/payload conflict quarantine, poison commit gaps, sink endpoint outage/recovery, 1→3 ownership and SIGKILL at five boundaries including a lost successful insert response and a broker-observed rebalance. Live consumer-smoke independently sends ten HTTP-acknowledged deliveries for five IDs and checks canonical IDs and minute metrics against a preexisting-data baseline.

Phase 5 adds actual performance/fault measurements and audited DLQ/replay; query/Grafana were added in Phase 4. No multi-broker durability demonstration, AWS deployment or universal exactly-once claim is included. The outage test makes the sink endpoint unavailable through a controlled 503 proxy; it does not stop the ClickHouse daemon or simulate disk corruption. The lost-response proxy forwards to real ClickHouse, waits for acceptance, then closes the connection without replying to the client.
