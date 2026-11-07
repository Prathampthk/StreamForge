# Query API and reconciliation

## Start and use

After `make up`, `make migrate`, `make ingest-up` and `make consumer-up`, run `make query-up`. Query listens at http://localhost:8082, with a separate local query bearer key from .env.example. `make query-smoke` sends a small synthetic golden dataset through the real ingestion API, captures only 202-acknowledged deliveries, checks the query endpoints and reconciles an initially empty owned historical window. It prints the manifest path. If you customize credentials/tenants, export the matching settings before running Python scripts; Make/Compose reads .env, Python helpers do not.

```sh
curl --get http://localhost:8082/v1/metrics/revenue \
  -H 'Authorization: Bearer streamforge_query_demo_key_local_only' \
  --data-urlencode 'tenant_id=merchant_demo' \
  --data-urlencode 'from=2026-11-06T17:22:00Z' \
  --data-urlencode 'to=2026-11-06T17:24:00Z' \
  --data-urlencode 'interval=1m' --data-urlencode 'currency=USD'
```

Traffic uses `/v1/metrics/traffic` with the same parameters except currency. Choose a window containing your data. Windows are [from,to): lower bound inclusive, upper exclusive. Require RFC3339, minute-aligned UTC instants, from<to, max 24h by default and supported calendar years 1970..2105. Offsets are accepted and converted to UTC. `interval=1m` and tenant_id are required. Unknown/repeated parameters, unaligned fractional minutes, invalid currency and malformed URLs fail with 400. A valid key for another tenant fails with 403 before any database access. An invalid/missing key fails with 401.

Responses contain tenant_id, currency (empty on traffic), UTC from/to, interval, correlation_id, snapshot_last_success, snapshot_age_seconds and dense data. Each minute exists even without events. Revenue rows contain minute, purchase_count and **revenue_cents as a decimal integer string**. This preserves Int128 sums exactly, including values larger than Int64/JavaScript integers; it is never floating-point money. Explicit currency isolation is mandatory. Traffic rows contain events_per_minute, rpm (API events), api_errors (status>=500) and error_rate (errors/rpm, or zero). The rate may be floating point; cents are not.

`GET /livez` is process-only. `/readyz` requires both canonical refreshes to be available, successful, running/scheduled and within age bounds. Health/Prometheus endpoints are unauthenticated local operations routes. Admission saturation/draining, missing/failed/disabled/old refresh metadata and database failure/timeout return 503 with a sanitized error envelope and Retry-After=1. No stale-success fallback is offered.

## Snapshot semantics

Before each metric read, query its system.view_refreshes row. Reject no prior success, failed last attempt, stopped/disabled state, excessive age or unexpected future clock skew. Read the atomic canonical target, then check that the *earlier observed* refresh time is still within the age limit. Report that conservative timestamp as a lower bound; never use a later metadata lookup to claim an earlier data query read a newer snapshot. Revenue/traffic replace independently and are not one cross-endpoint snapshot. A failure just after the health observation may not be reported until the next request; the last successful snapshot remains within the age bound at response time.

Refresh age is **not event freshness**: Kafka backlog may remain even while refresh succeeds. The <5s acknowledgment-to-aggregate SLO remains unmeasured until Phase 5. Server clocks must be synchronized; metadata permits up to one second future skew. Consumers deduplicate normalized identical IDs; conflicts withdraw the whole identity on refresh. Full-state refresh cost and indefinite identity retention from ADR 003 still need measured lifecycle/scaling work.

The read repository uses fixed SQL with ClickHouse typed parameters, readonly=1, max_execution_time=1s, max_memory_usage=128MiB, max_threads=2, wait_end_of_query=1 and max 1441 returned rows (1440 accepted). Responses cap at 1MiB, connections at admitted query count. Dense responses cap at 1440 rows. Any malformed/duplicate/out-of-window database bucket fails instead of being silently accepted. HTTP 200 execution-error bodies and transport errors do not become API success. Runtime credentials are local shared development credentials; external deployment needs least-privilege database users/TLS in Phase 6.

EXPLAIN on the actual canonical revenue table showed primary-key predicates on tenant_id/minute/currency. The smoke dataset occupied one granule (1/1), so no partition/pruning speedup was measured or claimed. The snapshots are ORDER BY tenant_id/minute(/currency), rebuilt by atomic replacement, with no monthly partitions; adding partitions without a measured benefit is deferred.

## Configuration

| Setting | Default / constraint |
|---|---|
| QUERY_HTTP_ADDR | 127.0.0.1:8082; Compose 0.0.0.0 behind loopback publication |
| QUERY_API_KEY | required >=24 bytes; Compose uses an explicit development-only default |
| QUERY_TENANT | merchant_demo |
| CLICKHOUSE_URL / USER / PASSWORD | localhost:8123 / streamforge / required password; Compose supplies internal origin |
| QUERY_MAX_CONNECTIONS / QUERY_MAX_REQUESTS | 128 / 16; requests <=connections and <=128 |
| QUERY_DB_TIMEOUT | 1s, 100ms..5s HTTP client bound; fixed server query cap remains 1s |
| QUERY_REQUEST_TIMEOUT | 3s; >=2*DB timeout, <=15s |
| QUERY_SHUTDOWN_TIMEOUT | 8s; >request timeout, <=60s; Compose kill grace=15s must be raised if this increases |
| QUERY_MAX_WINDOW | 24h maximum; positive whole minutes |
| QUERY_MAX_SNAPSHOT_AGE | 10s; 1s..60s |
| TRACE_SAMPLE_RATIO / OTEL_EXPORTER_OTLP_ENDPOINT | 0.1 / optional http(s) Collector endpoint |

Read header timeout=1s, read timeout=request timeout, write timeout=request+1s, keepalive idle=30s and headers=8KiB. Listener connections, request slots, scrape slots, database pool and telemetry queues are bounded. No new application database migration is needed for Phase 4. SIGTERM marks drain and finishes admitted requests within shutdown bounds, then closes the database pool and tracing.

## Reconciliation contract

```sh
make reconcile MANIFEST=/absolute/path/streamforge-accepted-TRACEID.json
python3 scripts/reconcile.py MANIFEST --drain-timeout 0
```

A schema_version=1 JSON manifest has scope {tenants:[...], from:..., to:...} and accepted [{event:canonical-wire-event, acknowledged_at:RFC3339}, ...]. The scope must be owned by the test/dataset, not a window containing unrelated traffic. Only record known successful HTTP/broker acknowledgments; a 503/disconnect cannot be promoted to accepted by the audit. These timestamps are caller-recorded evidence, not cryptographically signed receipts. The versioned golden fixture is synthetic unit-test data; query-smoke records real 202 responses.

Limits: <=4MiB manifest, <=5000 accepted entries, <=20 unique tenants, aligned window <=24h, query responses <=2MiB and drain timeout 0..60s. The script uses only Python's standard library. It normalizes UUIDs/UTC timestamps/optional nulls independently (preserving up to nine fractional digits), groups full logical payloads by ID, expects one canonical event for equal variants and quarantine for differing variants, then computes exact revenue by currency/minute and API/event counts. No float participates in financial sums.

The audit compares canonical IDs/payloads, expected conflict IDs, source raw evidence/count lower bounds, extra IDs and canonical minute-table values. Repeated raw IDs are evidence, not automatically a failed audit; the raw_duplicate_rows statistic includes changed-payload variants of an ID, separately reported as quarantine. Expected conflict IDs must be visibly quarantined and excluded. Unexpected conflicts fail. Both refreshes must be healthy/recent. A discrepancy or dependency/manifest error exits nonzero; success exits zero. Printed mismatch IDs are capped at 20 per category; the comparisons cover the full bounded dataset. The comparison spans separate reads, so continuous writes can cause temporary mismatches; drain/retry before interpreting the result.

The live smoke chooses an empty raw+canonical four-minute window within the seven-day ingress lateness policy, sends the nine-entry golden workload twice (18 acknowledged deliveries), and checks six canonical IDs plus one conflict. It leaves small forensic rows/identity states intentionally. It is a correctness fixture, not an event generator or throughput benchmark. Run it only in the local development dataset. The negative audit test changes expected USD cents and must return nonzero without writing new data.
