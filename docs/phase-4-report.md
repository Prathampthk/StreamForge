# Phase 4 completion report

- Implemented:
  - Separate tenant-scoped Go query API: revenue and traffic from canonical minute snapshots, explicit currency isolation, minute-aligned half-open UTC windows <=24h, dense zero buckets, stable error envelopes and exact Int128 cent totals as decimal strings.
  - Typed ClickHouse parameters with fixed SQL, bounded connections/admission/rows/bytes/time/memory/threads, refresh-health checks and conservative observed successful-refresh timestamps. Missing/failed/disabled/stale snapshots, invalid rows, saturation and read failures return non-success. Recent snapshot refresh is not advertised as event freshness.
  - Query metrics, concrete ClickHouse insert/read spans, optional pinned Prometheus/Grafana/Collector/Jaeger stack, provisioned dashboard/datasources and actual HTTP/export smoke checks. All host ports bind loopback; no frontend or cloud resources.
  - Independent bounded accepted-manifest reconciliation script, deterministic golden expectations, live receipt capture and positive/negative audit tests. Compares canonical payload/IDs, raw evidence/extra IDs, expected conflict quarantine and exact per-minute/currency business metrics. No Phase 5 generator, DLQ/replay runtime or benchmark harness.
  - ADR 005, OpenAPI updates, query/observability operations and updated architecture/failure-model instructions.

- Commands executed and results:
  - `make fmt`; `PATH="$PWD/.venv/bin:$PATH" make test lint`: PASS. Four Python tests (including 13 event-contract fixtures and independent reconciliation golden/mismatch tests), all Go unit/race checks and go vet. Query tests cover UTC offsets, half-open bounds, bad/duplicate parameters, exact large cents, zero/error-rate buckets, auth/tenant isolation, saturation, cancellation, stale/failed/missing refreshes and metric label cardinality. Event-fixture discovery now selects only valid-/invalid- event fixtures, not the new manifest fixture.
  - `make query-up`: PASS, pinned Go 1.26.0 scratch/non-root query image built; canonical refresh readiness succeeded.
  - First full integration run: previous Kafka/consumer/crash tests passed; new query golden test FAILED waiting for a usable snapshot response. Actual ClickHouse error NO_COMMON_TYPE revealed SELECT alias `minute` replaced an unqualified DateTime predicate with a formatted String. Qualified source columns in the read repository and reconciliation metric queries. Targeted `TestQueryGoldenLateEventsAndIsolation` then passed (`ok ... 3.684s`). No validation/acknowledgment setting was weakened.
  - Final `make test-integration`: PASS with race detector, `ok streamforge/tests/integration 70.916s`. This is suite duration, not a latency benchmark. It reran all Phase 3 crash/outage/rebalance/gap and Phase 2 Kafka checks, plus the Phase 4 golden and actual stopped-refresh/unreachable-database checks. The stopped revenue refresh was restarted explicitly and by deferred cleanup.
  - Golden query test against real ClickHouse: PASS for USD/EUR separation, exact nanosecond minute edges and timezone-equivalent events, zero-filled minutes, duplicate/regrouped data and conflict exclusion, tenant isolation, a late event updating its original minute, and two maximum Int64 purchases summed exactly to string `18446744073709551614`. In-memory span assertions include query HTTP, refresh health and revenue/traffic database reads.
  - `make observe-up`: pinned images pulled/built, Prometheus/Grafana provisioning loaded, applications wired to the Collector. Initial Collector startup FAILED configuration validation: max_elapsed_time=10s was below its implicit max_interval. Fixed explicit initial_interval=1s/max_interval=5s, added actual HTTP health probing and bounded Jaeger memory storage to 2000 traces. Final observe-up PASS, including Prometheus/Grafana/Collector/Jaeger HTTP health. Docker running state alone is not used as proof for the last two images.
  - `make query-smoke`: PASS for actual HTTP receipts and full pipeline. Nine golden entries sent twice -> 18 acknowledged deliveries, six canonical IDs and one deliberately quarantined conflict. Final USD=137/EUR=250 cents, minute traffic [events=4,RPM=2,errors=1,rate=.5] and [events=2,RPM=1,errors=0,rate=0], plus two empty zero buckets. Raw rows=18; repeated logical-ID rows=11 (includes conflict variants). No missing/extra/payload/business-metric mismatches.
  - Actual accepted manifest: `/var/folders/rd/jx_ng9x140x012vnjlvwmc8c0000gn/T/streamforge-accepted-7eee025114854102b7bb0a0fd58f4853.json`; trace ID `7eee025114854102b7bb0a0fd58f4853`. This is a small local temporary artifact, not committed credentials or load results. A prior successful receipt manifest ending 65f9b73e5a59487c9ea788898be0e4e0 was also audited independently with `make reconcile MANIFEST=...`: PASS, exit zero and no mismatches.
  - Negative reconciliation: copied the real manifest and intentionally changed expected USD cents 137 ->138 across equal variants. `python3 scripts/reconcile.py NEGATIVE_MANIFEST --drain-timeout 0` returned exit 1 with payload and revenue-bucket discrepancies, as required. No new data was written by that audit.
  - `make observe-smoke MANIFEST=...7eee...json`: PASS. Prometheus ingest/consumer/query targets all up; Grafana dashboard UID streamforge-pipeline had 12 panels and Prometheus/Jaeger datasources; Jaeger returned the same trace containing http.ingest, kafka.publish, consumer.partition_insert, clickhouse.insert, http.query.revenue and revenue/traffic read operations. This verifies real Collector export, not just in-memory spans. Dashboard provisioning was API-checked; no visual screenshot QA claim.
  - Actual `EXPLAIN indexes=1` for a bounded tenant/currency/time revenue read showed primary-key conditions on tenant_id/minute/currency. The dataset was one part/one granule (1/1); this proves the eligible index condition, not measured pruning savings or performance.
  - `make query-down` and docker inspect: exit_code=0, oom_killed=false on SIGTERM (idle deployed query process). `make observe-up` restored it, actual optional HTTP health passed and `make ps` showed all ten project services running. Eight have Docker health checks; Collector/Jaeger are running and externally HTTP-probed. One normal consumer remains running; test group members were cleaned up.
  - Python compile, OpenAPI/dashboard JSON syntax, git whitespace checks and ignored .env/.venv checks: PASS. No Go dependency or schema migration was required in Phase 4. No golangci-lint, throughput/freshness/latency benchmark or external security scan is claimed.
  - Query/reconciliation milestone committed as aba116e (`feat(query): serve bounded canonical metrics and reconcile accepted events`). Observability/docs/report are the following commit; actual GitHub push results are confirmed in final chat.

- Acceptance criteria passed/failed:
  - PASS: canonical query endpoints, minute/timezone/empty/currency/large-cent semantics, late data, tenant isolation and failure rejection; bounded query concurrency/race checks; finite metric-label test; provisioned dashboard and real scraped targets; actual distributed trace export through Kafka/consumer/ClickHouse plus query reads; positive/negative accepted-ID/business-metric reconciliation on seeded duplicate/conflict datasets; existing crash suite remains green.
  - NOT CLAIMED: >=10k accepted events/s, p95 query/ingest/freshness targets, >=2x consumer scaling throughput, broker-failure or disk-loss durability, per-event exactly-once delivery, production auth/TLS, durable traces, client-side latency measurements or complete failure-space coverage.

- Design tradeoffs and known limitations:
  - Query keys map to one tenant locally; optional local tooling uses development-only defaults. TLS, multi-tenant metadata auth and least-privilege database credentials remain Phase 6 hardening. readonly=1 is per-request protection, not a replacement for database grants.
  - Dense windows cap at 1440 minutes; unaligned partial-minute windows and other intervals are rejected. Revenue cents are integer strings to avoid Int128 rounding. The two endpoint snapshots are independently atomic, not one transaction.
  - Refresh metadata is checked immediately before reads and its conservative age checked after. A newly occurring failure can be observed only by the next request. Refresh timestamps are second-granularity metadata and require synchronized clocks (<=1s future skew). A recent refresh is not a Kafka watermark or acknowledgment-to-visibility measure. Receipt acknowledged_at is client-observed success time, not an exact broker timestamp; no freshness SLO is computed from it.
  - Canonical refresh scans and indefinite identity-state retention from Phase 3 remain growing-cost concerns. Legacy append metrics may double count and are never queried as final analytics. No rewrite to those incorrect sums was used to improve timings.
  - Reconciliation requires a manifest-owned scope, bounded size and trustworthy receipt capture; it does not prove delivery for unknown HTTP outcomes. It performs multiple reads, so drain/retry before treating transient concurrent-write differences as loss. Current mismatch output caps ID lists at 20/category but compares all bounded rows. Raw repeated-ID evidence includes quarantine variants and is not automatically a correctness failure.
  - Trace data is sampled/best effort/volatile (Jaeger max 2000 traces, Docker cap 256MiB); Collector has bounded memory/batches/queue/retries. Prometheus/Grafana have retained local volumes. No trace-storage or UI performance guarantee. Increased optional/build/test memory/disk costs are local only.
  - Normal Compose targets may warn about the separately enabled optional services as orphans; they do not delete them. Use observe-up to retain automatic Collector wiring; observe-down stops tools only. make down intentionally stops the full project without deleting persisted volumes.

- Next phase preview (do NOT implement): durable poison-event DLQ publish-before-commit, bounded audited replay, failure-injection/load harness and actual target measurements with environment/resource evidence. Continue correctness reconciliation and explicitly measure full-refresh costs instead of claiming targets from smoke tests. AWS remains Phase 6 and separately approved.
- Approval requested: "Phase 4 is complete. Do you approve proceeding to Phase 5 (DLQ, replay, fault injection, and performance tuning)?"

## Files changed in Phase 4

- `.env.example`
- `Makefile`
- `README.md`
- `api/openapi/streamforge.yaml`
- `cmd/consumer/main.go`
- `cmd/query-api/main.go`
- `deploy/compose.observability.yaml`
- `deploy/compose.query.yaml`
- `deploy/docker/query.Dockerfile`
- `deploy/grafana/dashboards/pipeline.json`
- `deploy/grafana/provisioning/dashboards/default.yaml`
- `deploy/grafana/provisioning/datasources/default.yaml`
- `deploy/otel/config.yaml`
- `deploy/otel/jaeger.yaml`
- `deploy/prometheus/prometheus.yaml`
- `docs/adr/005-query-consistency.md`
- `docs/architecture.md`
- `docs/contracts.md`
- `docs/failure-model.md`
- `docs/observability.md`
- `docs/phase-4-report.md`
- `docs/query.md`
- `internal/analytics/read.go`
- `internal/analytics/read_test.go`
- `internal/analytics/sink.go`
- `internal/config/query.go`
- `internal/config/query_test.go`
- `internal/domain/validate.go`
- `internal/domain/validate_test.go`
- `internal/query/http.go`
- `internal/query/http_test.go`
- `internal/telemetry/query.go`
- `internal/telemetry/telemetry.go`
- `scripts/observe-health.py`
- `scripts/observe-smoke.py`
- `scripts/query-smoke.py`
- `scripts/reconcile.py`
- `tests/fixtures/reconciliation-golden.json`
- `tests/integration/query_test.go`
- `tests/test_contract.py`
- `tests/test_reconciliation.py`
