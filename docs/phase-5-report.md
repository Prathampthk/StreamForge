# Phase 5 completion report

Implemented durable poison quarantine, compact PostgreSQL auditing and a bounded operator replay CLI; deterministic acknowledged-event load generation, k6 profile, local fault/scaling harnesses and resource-bounded ClickHouse/batching tuning. No AWS resources or Phase 6 deployment/security/CI work were performed.

## Correctness and failure evidence

- Full Docker integration suite with the race detector passed in 99.373 seconds before the final worker-scaled collection adjustment. It covers existing SIGKILL/insert-before-commit/rebalance/ambiguous-insert behavior plus real Kafka/ClickHouse, query failure handling and new DLQ/replay behavior. The final collection adjustment is exercised by the completed scaling run and final checks recorded below.
- `TestDurableDLQAndBoundedReplay`: healthy prefix → poison → healthy tail commits only after DLQ acknowledgment and durable audit. Invalid raw replay is refused; explicit repair preserves event identity; dry-run publishes nothing; same-job resume skips audited publication; a new job repeats the logical event without inflating canonical USD revenue (521 cents).
- `TestDLQFailureCannotAdvancePoison`: absent DLQ fails visibly; the valid prefix commits but poison and its following record remain uncommitted. PostgreSQL is not on the valid-event path. Publish-before-audit crashes can duplicate a stable envelope; exactly-once quarantine is not claimed.
- `python3 scripts/local-faults.py`: clean successful run at `2026-11-07T02:54:45.948366Z`. Five events received 202 while ClickHouse was stopped; after recovery all five IDs and exact revenue were verified. Kafka stopped → HTTP 503 `publish_unconfirmed`; delivery is explicitly unknown. Earlier fault setup attempts failed while local ClickHouse/consumer configuration was being repaired; those attempts are not counted as passes.
- Integration fixtures now isolate the Kafka acknowledgment test in its own topic and wait for independently refreshed revenue/traffic snapshots and actual stopped-view state. These fixes remove invalid assumptions exposed by the larger dataset; correctness expectations remain intact.

## Measured consumer scaling

Same 60,000 records, 12 balanced partitions, prefilled canonical identities, one worker per replica, batches <=1000, 8MiB budget, identical settings, two concurrent query callers; timing first insert through confirmed commits, 50ms observation resolution. This isolates consumer processing from HTTP generation. One local host hosts every component; short runs and prefilled dedup state limit generalization.

| Replicas | Persisted + committed events/s | Seconds | Query requests/errors | Successful query p95 |
|---|---:|---:|---:|---:|
| 1 | 9,679.60 | 6.199 | 113 / 0 | 26.30ms |
| 2 | 17,240.00 | 3.480 | 62 / 0 | 29.34ms |
| 3 | 18,483.88 | 3.246 | 59 / 0 | 28.29ms |

**1→3 speedup = 1.90957×; the >=2× gate FAILED.** The numerical target is reported separately from functional correctness checks. Earlier >2× numbers had query failures and an audit OOM and are not accepted evidence of a combined pass. An intermediate twelve-partition collection epoch exceeded single-worker processing deadlines; collection now scales with active workers, retaining byte/concurrency/deadline bounds. See `load/results/scaling.json` and ADR 007.

## Pilot measurements (not the 15-minute result)

| 30-second pilot | Steady acknowledged/s | Unknown events | Query errors/requests | Successful client p95 | Freshness observations |
|---|---:|---:|---:|---:|---|
| 1,000 offered/s | 1,000 | 0 | 68 / 526 | 19.77ms | 29 successes, p95 1.683s, max 2.309s |
| 10,000 offered/s, interim tuning | 9,923.33 | 2,300 | 11 / 496 | 31.14ms | 5 successes, 7 failed/censored, 17 dropped; successful p95/max 22.002s |

An additional high-rate setup failed before measurement and is not a completed test. These pilots used different tuning from the formal run and reused a pilot tenant; they are not identical-environment performance comparisons or a complete manifest-only metric reconciliation.

## Fifteen-minute measurement

**Interrupted evidence, not a qualifying continuous run.** The 900-second wall-clock interval started `2026-11-07T03:11:12.332093757Z`. macOS power logs confirm clamshell sleep at 21:17:13 local, followed by dark/maintenance wake and full wake at 21:23:02. Resource sampling has a 279.56-second gap. These gaps cannot be attributed solely to the backend or removed from the denominator to manufacture a sustained result. A fresh awake 15-minute run remains unverified.

Environment: Apple M4, 10 cores, 16GiB host RAM; Docker 10 CPUs/8,319,438,848 bytes RAM; macOS 26.6.2 arm64. Kafka 4.0.2, ClickHouse 25.8.4.13, PostgreSQL 17.6, Go 1.26.0. One consumer, four workers, 1000-record chunks/8MiB budget, 12 Kafka partitions/RF=1, idempotent lz4 acks=all producer. Two traffic query callers use 15-minute/24-hour windows, plus JPY revenue freshness probes and a sparse 1440-event historical seed. Root tracing is disabled on dedicated benchmark endpoints. Code baseline: `2f09d3e`; no builds/restarts/fault injections were deliberately introduced during measurement.

| Observation | Actual value | Assessment |
|---|---:|---|
| Offered / attempted steady events | 9,450,000 / 6,048,300 | 3,401,700 explicitly dropped by bounded driver queue/scheduling |
| Acknowledged inside 900s | 6,047,400 = 6,719.33/s | Below target; interrupted interval cannot establish continuous SLO |
| Accepted all stages | 6,048,929 | Includes 1440 seed and 89 probes; journaled individually |
| Unknown / rejected events | 900 / 0 | Unknown can have been published; not silently treated as absent |
| Client p50/p95/p99 to response headers | 14.904 / 311.203 / 736.448ms | Original timer omitted body read; full-response latency is at least this large, so <100ms fails; exact full-body values unmeasured |
| Server successful-request p95 | <=500ms histogram bucket; 298.452ms interpolation | Estimate, not exact sample p95; <100ms fails |
| Queries / errors | 9350 / 8958 | Only 392 successes; no combined query SLO pass |
| Successful query p95 | 157.383ms | Excludes errors; two concurrent callers, includes endpoint response read |
| Successful freshness p50/p95/p99/max | 16.383 / 29.652 / 29.652 / 29.652s | Seven successes only; misses <5s |
| Failed/censored / dropped probes | 82 / 510 | Host sleep also prevented scheduled samples; cannot infer all-accepted-event freshness |

Sampled ClickHouse peak memory was 4074.50MiB of its 4GiB cap, peak CPU 759.86% (100%=one core). Kafka sampled peaks were 723.1MiB/210.61%; consumer 87.11MiB/67.14%; benchmark ingest 85.74MiB/246.09%; benchmark query 14.47MiB/30.38%. These are sampled maxima, not continuous peaks. Actual refresh exceptions include query memory-limit failures at 256MiB during full-state aggregation/spill reads. The process experienced consumer restarts under sink pressure. Raw resource/power/receipt evidence remains local in ignored `load/results/raw/steady-15m/`; small summaries and a graph explicitly labeled interrupted are committed.

The corrected future driver times through body read/close and passes a delayed-body regression test. The wrapper now holds a scoped `caffeinate -i` assertion on macOS and records sleep/wake qualification. This prevents idle sleep only: the lid must remain open. None of these changes rewrites the original measured results.

## Awake 15-minute result (final driver)

Executed `python3 scripts/benchmark.py --duration 15m --rate 10500 --name steady-15m-awake --allow-unready-query` at source revision `1ebc0df`. Steady interval: `2026-11-07T03:44:08.271270139Z` through 900 seconds later. macOS power-log qualification recorded **no sleep inside the interval**. Root traces remained disabled on benchmark endpoints; no tests, audit reads, intentional faults or builds ran during this interval.

The initial query readiness was **503**, with failed full-state refreshes already present. Starting state: 6,567,877 physical identity-state rows and 6,911,275 raw rows; these are physical counts, not unique-ID counts. This is an explicitly degraded, growing-history experiment, not a healthy-start combined SLO result. An initial fresh setup correctly refused unhealthy query readiness; the explicit flag then permitted measuring acknowledgment throughput while retaining every analytics failure. The failed ordered-aggregation experiment was reverted to migration 003 before this run.

| Target / measurement | Actual awake result | Assessment |
|---|---:|---|
| >=10,000 acknowledged events/s for 900s | **9,301,900 / 900 = 10,335.44/s** | PASS for Kafka acceptance only; single broker RF=1 |
| Offered / attempted steady events | 9,450,000 / 9,302,000 | 148,000 bounded-driver drops explicitly counted |
| Accepted all stages | 9,303,564 | Includes 1440 seed, 124 probes and 100 steady acknowledgments outside interval |
| Unknown / rejected events | 0 / 0 | Every successful receipt retained |
| Full-response client ingest p50/p95/p99 | 20.835 / **248.577** / 522.373ms | p95 <100ms FAILED |
| Server ingest p95 | <=250ms histogram bucket; **232.010ms estimate** | <100ms FAILED; estimate is not an exact sample quantile |
| Query requests / errors | **13,953 / 13,953** | FAILED; no successful latency samples, p95 is null |
| Freshness samples / failed-or-censored / dropped | **124 / 124 / 775** | FAILED/unavailable; zero successful samples, all success quantiles null |
| Consumer 1→3 scaling (separate comparison) | **1.90957x** | >=2x FAILED; all 60,000 scaling IDs independently verified |

Sampled maxima: ClickHouse 4096MiB/686.58% CPU; Kafka 722.9MiB/167.84%; consumer 88.33MiB/74.23%; benchmark ingest 100.4MiB/225.89%; benchmark query 18.34MiB/27.63%. CPU 100%=one core; these are sampled peaks, not continuously observed maxima. The 130 resource samples and their timestamps are retained. Host sleep absence alone does not establish an SLO; the per-event successful callback receipts supply the acknowledgment count.

See `load/results/steady-15m-awake.json`, `awake-environment.json`, `awake-quality.json`, resource summaries and both graphs. All raw journals remain in ignored `load/results/raw/steady-15m-awake/`.

### Post-load accepted-ID and metric audit

The initial 5000-ID audit failed at the HTTP form-field limit and verifies no IDs; it is preserved as a failed harness attempt. A real 2000-ID request matched every expected ID/SHA256 before the exhaustive audit. Active full-state refreshes caused intermittent read failures even at the smaller size. Final post-load auditing temporarily pauses those already-failing refreshes, retains the same bounded read limits, retries a transient read at most three times and always restarts both views in `finally` before checking business metrics. This isolation is **outside the measured interval**, improves no benchmark number, and cannot establish live freshness. Remaining failed chunks are retained privately and counted unverified.

Final exhaustive audit: **9,303,564 accepted receipts checked; missing IDs=0, payload mismatches=0, unverified reads=0**. Fifty-five transient read failures succeeded on bounded retry. Every accepted ID and normalized payload SHA256 matched canonical storage. This proves no missing acknowledged IDs in this specific post-load cohort, not universal broker/disk-loss durability.

**Revenue exact=false, traffic exact=false; overall audit=false (exit 1).** Both refresh jobs were restarted and observed `Running`; their memory-limit failures and stale snapshots persist. Final business metrics are therefore not correct/usable at this history size even though all canonical facts are present. See `load/results/awake-audit.json`. No double-count-free served-aggregate or live-freshness guarantee is inferred from canonical persistence alone.

## Final validation and reproducibility

- `PATH="$PWD/.venv/bin:$PATH" make test lint`: PASS after the awake measurement. Four Python tests; Go unit/race tests and go vet. Corrected full-response timing is covered by a delayed-response-body HTTP regression test; generator distributions/UUIDv7 and replay identity/tenant/bounds checks pass.
- Built `replayctl --status --job-id 0b67aa82-2bde-4f0d-bda9-4b1b12260641`: PASS, read the persisted completed integration job (one scanned/republished), no publication.
- Final real-service subset with race detector: `TestKafkaAcknowledgedHTTP`, `TestUnavailableBrokerDoesNotAcknowledge`, `TestDLQFailureCannotAdvancePoison`: PASS, 8.044s. This suite duration is not a performance number. The earlier complete integration suite passed before the large-history readiness failures; a current full query-golden suite is not claimed green against the failed growing-history snapshots.
- `make k6-smoke`: PASS, 51/51 requests, 5100 acknowledged events, zero HTTP/check failures; HTTP p95 64.06ms over five seconds on the separate normal demo tenant. This is driver verification, not the 15-minute SLO result.
- `make observe-up`: final runtime images built and optional Prometheus/Grafana/Collector/Jaeger HTTP health passed before measurement. `make tools-build`: PASS for final eventgen/replayctl images. Actual Grafana API confirmed 16 provisioned panels after adding DLQ/failure panels and freshness-measurement guidance. No screenshot QA claim.
- Formatting/static checks, Python compilation, OpenAPI/dashboard JSON parsing and git whitespace checks are recorded in final validation. Raw journals and local credentials remain ignored. Small summaries/graphs/commands are committed.
- Reproduction and failure handling: [benchmarking](benchmarking.md), [runbook](runbook.md), [replay](replay.md), ADRs 006/007 and the [one-page evidence report](benchmark-report.md).

## Design tradeoffs and remaining limits

- Single-broker RF=1, local persisted volumes, all processes share a Mac/Docker host. No broker/disk-loss durability or exactly-once Kafka/ClickHouse/PostgreSQL transaction guarantee.
- Canonical analytics retains full-payload identity state indefinitely and fully recomputes snapshots. Increasing history makes the scans expensive. Legacy append sums are diagnostic only; they were never substituted to improve results.
- ClickHouse now has 4GiB rather than 2GiB, bounded 128/64MiB caches, four background workers and refresh queries limited to two threads/256MiB with 64MiB external aggregation. Docker has approximately 7.75GiB total. These explicit environment changes are not a free performance gain at the original resource cap.
- Replay is a local operator CLI, dry-run by default, <=1000 offsets/one partition/16MiB/60 seconds. Stable job selectors and advisory locking support audited resume; identity-changing repair is refused. Production grants/auth/TLS remain Phase 6.
- The generator records unknown outcomes and bounded-queue drops. Successful latency samples do not erase failed queries, censored probes or missing-at-audit IDs. A finite drain audit cannot establish permanent loss when backlog remains.

## Next phase

Phase 5 implementation and measurement work is complete; this does **not** mean all performance targets passed. The measured bottleneck is full-history canonical recomputation. Proposed next experiments are bounded affected-minute/currency recomputation with explicit conflict withdrawal, history lifecycle controls, and a separate load-generator host. Each must preserve global identity semantics and rerun correctness/fault/steady-state measurements. None is implemented or claimed as a performance gain.

Phase 6 is AWS deployment, security hardening, CI/CD and final evidence. No provisioning or `terraform apply` is authorized by this report. Stop for approval after completing and pushing Phase 5 evidence.

Approval question: **"Phase 5 is complete. Do you approve proceeding to Phase 6 (AWS deployment, security hardening, CI/CD, and final evidence)?"**

## Files changed in Phase 5

- `.dockerignore`
- `Makefile`
- `README.md`
- `api/openapi/streamforge.yaml`
- `cmd/consumer/main.go`
- `cmd/eventgen/main.go`
- `cmd/eventgen/main_test.go`
- `cmd/replayctl/main.go`
- `deploy/clickhouse/resources.xml`
- `deploy/compose.bench.yaml`
- `deploy/compose.consumer.yaml`
- `deploy/compose.yaml`
- `deploy/docker/consumer.Dockerfile`
- `deploy/docker/ingest.Dockerfile`
- `deploy/docker/query.Dockerfile`
- `deploy/docker/tools.Dockerfile`
- `deploy/grafana/dashboards/pipeline.json`
- `docs/adr/006-dlq-replay.md`
- `docs/adr/007-measured-performance.md`
- `docs/architecture.md`
- `docs/benchmark-report.md`
- `docs/benchmarking.md`
- `docs/consumer.md`
- `docs/contracts.md`
- `docs/failure-model.md`
- `docs/ingestion.md`
- `docs/observability.md`
- `docs/phase-5-report.md`
- `docs/replay.md`
- `docs/runbook.md`
- `go.mod`
- `go.sum`
- `internal/broker/consumer.go`
- `internal/broker/producer.go`
- `internal/config/config.go`
- `internal/ingest/http.go`
- `internal/metadata/jobs.go`
- `internal/metadata/store.go`
- `internal/replay/envelope.go`
- `internal/replay/envelope_test.go`
- `internal/replay/quarantine.go`
- `internal/replay/runner.go`
- `internal/telemetry/acks.go`
- `internal/telemetry/consumer.go`
- `internal/telemetry/telemetry.go`
- `load/k6/mixed.js`
- `load/ordered-refresh-experiment.sql`
- `load/results/awake-audit.json`
- `load/results/awake-environment.json`
- `load/results/awake-quality.json`
- `load/results/environment.json`
- `load/results/interrupted-15m-outcomes.png`
- `load/results/interrupted-15m-resources.json`
- `load/results/interrupted-15m.json`
- `load/results/interrupted-15m.png`
- `load/results/interrupted-audit.json`
- `load/results/interrupted-power.txt`
- `load/results/interrupted-quality.json`
- `load/results/k6-smoke.txt`
- `load/results/local-faults.json`
- `load/results/ordered-refresh-check.json`
- `load/results/refresh-failures.json`
- `load/results/scaling-id-audit.json`
- `load/results/scaling.json`
- `load/results/steady-15m-awake-outcomes.png`
- `load/results/steady-15m-awake-resources.json`
- `load/results/steady-15m-awake.json`
- `load/results/steady-15m-awake.png`
- `migrations/clickhouse/003_refresh_resource_budgets.sql`
- `migrations/postgres/002_dlq_audit.down.sql`
- `migrations/postgres/002_dlq_audit.up.sql`
- `requirements-bench.txt`
- `scripts/bench-audit.py`
- `scripts/bench-graphs.py`
- `scripts/bench-monitor.py`
- `scripts/bench-power.py`
- `scripts/benchmark.py`
- `scripts/local-faults.py`
- `scripts/migrate.sh`
- `scripts/observe-smoke.py`
- `tests/integration/consumer_test.go`
- `tests/integration/ingest_test.go`
- `tests/integration/query_test.go`
- `tests/integration/replay_test.go`
- `tests/integration/scaling_test.go`
