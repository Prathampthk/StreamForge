# Phase 3 completion report

- Implemented:
  - Partition-aware consumer binary with one poll owner, bounded flush/count/byte budgets, bounded parallel partition inserts, auto commits disabled, blocked rebalances during bounded processing, and serial next-offset commits only for successful partition batches.
  - Synchronous ClickHouse HTTP sink, deterministic forensic batch identity/checksum, exact immutable body retries, response/connection/deadline bounds and explicit ambiguous-outcome handling. No PostgreSQL per-event or batch-manifest write was introduced.
  - Accepted ADR 003: incremental exact min/max payload identity states, one canonical contribution for normalized-equal logical IDs, visible quarantine/exclusion for conflicting IDs, and separate atomically replaced revenue/traffic minute snapshots. The old append aggregate states remain diagnostic only. This intentional aggregation-shape adjustment prevents replay inflation; a raw ReplacingMergeTree or finite block dedup window would not establish the required correctness.
  - Consumer config validation, assignment/commit/lag/batch/retry metrics, structured logs, consumed-batch tracing, health server, non-root consumer image, Compose service/scaling support, test-only failpoints and operations/retention documentation.
  - Unit/race, actual Kafka+ClickHouse integration and deployed HTTP-to-canonical-metrics smoke checks. No query API, DLQ/replay runtime, load generator, dashboard, AWS resource or later-phase implementation.

- Commands executed and results:
  - Inspected clean main at 816be68, existing schemas/ADR and pinned franz-go commit/rebalance APIs; checked current official ClickHouse refreshable/incremental view documentation before choosing the canonical design.
  - Applied the candidate migration against ClickHouse 25.8.4.13: PASS. `make migrate`: PASS, PostgreSQL and both ClickHouse migrations reran without deleting data; event/DLQ topics remain 12 partitions/RF1.
  - `./scripts/go.sh sh -c 'gofmt -w internal cmd && go vet ./... && go test -race ./...'` and subsequent `make fmt`, `PATH="$PWD/.venv/bin:$PATH" make test lint`: PASS. All Python fixtures still pass. Go unit tests cover normalization (including UTC-equivalent timestamp spelling and aged backlog), offset gaps/regression, deterministic retry identity/body, bounded retry/cancellation, safe insert settings, HTTP 200 execution-error rejection and invalid timing/config combinations. Host Go remains absent; pinned golang:1.26.0 is used.
  - `make test-integration`: final PASS, `ok streamforge/tests/integration 66.746s` with race detector. This duration is test runtime, not a performance measurement. The matrix is below. Actual accepted ID lists are reconciled for crash replacements, rebalance and outage recovery.
  - `make consumer-up`: PASS, scratch/non-root image built, consumer joined the existing local group and drained retained Phase 2 messages. All five local services healthy.
  - `make consumer-smoke`: PASS twice, including after final image rebuild/restart. Ten HTTP-acknowledged deliveries for five known UUIDs -> five canonical IDs; exactly one incremental USD purchase of 137 cents and EUR purchase of 250 cents, five events, two API requests and one API error. Both repeated HTTP batches returned 202. Raw forensic rows=10, confirming duplicate physical evidence without duplicate final contributions. The script checks against a preexisting-data baseline and never sums different currencies.
  - Queried system.view_refreshes: revenue_1m_refresh and traffic_1m_refresh both Scheduled with recent successful refresh timestamps and empty exception fields.
  - `make consumer-down` then `docker inspect streamforge-consumer-1 --format 'exit_code={{.State.ExitCode}} oom_killed={{.State.OOMKilled}}'`: exit_code=0, oom_killed=false. This is an idle deployed-container SIGTERM check. Restarted with `make consumer-up` and reran live smoke successfully. In-flight abrupt termination is exercised separately by the SIGKILL matrix.
  - Python compile, shell syntax, `git diff --check` and staged whitespace checks: PASS. No external Go dependencies were added in this phase. No golangci-lint or performance benchmark run.
  - Foundation milestone committed as 8c2aa7e (`feat(analytics): derive replay-safe metrics from canonical event identities`); runtime/test/report milestone is the following commit. Both are intended for origin/main under the user's standing commit/push instruction; final chat reports actual push outcome.

## Real integration matrix

| Experiment | Actual verified result |
|---|---|
| Identical inserts, then different batch grouping | 15 raw delivery rows -> 5 canonical events; USD=100/EUR=250 cents, one purchase each; events=5/RPM=2/errors=1 |
| Same ID with changed tenant, amount and minute | Conflict ID/hashes visible; canonical identity excluded, earlier USD contribution withdrawn on scheduled refresh |
| SIGKILL before insert | No premature source commit; replacement drained all five accepted IDs; canonical totals exact |
| SIGKILL after insert | Raw evidence replayed; no premature commit, missing canonical IDs or double-counted final metrics |
| SIGKILL immediately before commit | Same successful recovery/reconciliation |
| SIGKILL during ambiguous insert retry | Proxy forwarded to actual ClickHouse, waited for successful insert, then closed the connection without a response; no premature commit; replay kept exact canonical totals |
| SIGKILL during rebalance | New member joined while old owner held an uncommitted successful insert; broker state PreparingRebalance observed before kill; replacement recovered all IDs with exact totals |
| One consumer -> three | Ownership settled at 4/4/4 over 12 partitions; all 60 accepted IDs reconciled; EUR=3000/USD=1200 cents separately; this is assignment correctness, not a speedup claim |
| Poison between valid records | Earlier completed prefix committed; poison and later same-partition valid record remained uncommitted, with a visible error; no skip/DLQ claim |
| Sink endpoint unavailable | Exactly 3 bounded attempts, no source offset or false raw persistence; restored endpoint/replacement drained all five IDs and exact metrics |
| Previous ingestion tests | Real Kafka HTTP acknowledgment/key/header/payload/trace checks still pass; unavailable broker returns 503 with zero accepted requests |

Each crash case uses a separately killable process running the actual consumer engine, real Kafka group offsets and real ClickHouse inserts. Integration topics are unique test-owned resources; cleanup deletes only those topics. Small uniquely named raw/canonical/conflict fixtures remain in ClickHouse for inspection. Consumer outage is injected through a 503 endpoint proxy, not by stopping the database daemon or corrupting disks. The ambiguous-response proxy does perform the real database insert before dropping the reply.

- Acceptance criteria passed/failed:
  - PASS: healthy acknowledged ingestion-to-canonical-aggregate path; stable source ordering and no commit past a failed delivered record; real 1→3 assignment; identical and regrouped replay idempotence for final metrics; explicit logical-ID conflict quarantine; evidence-backed worker-crash recovery at prescribed boundaries; ambiguous insert response; sink endpoint outage/backlog recovery; bounded retries and resource ownership.
  - PASS within tested local failure model: all non-conflicting accepted IDs in the recovery datasets appear exactly once in canonical data, with exact per-currency business totals after drain/refresh. Physical raw rows may duplicate by design. Conflicted IDs are explicitly quarantined and excluded, not silently lost.
  - NOT CLAIMED: universal exactly-once delivery, atomic Kafka/ClickHouse transactions, source-raw-row uniqueness, multi-broker/disk-loss durability, achieved 10k/s or latency/freshness/scaling SLOs, real external OTLP collector delivery, exhaustive fault coverage or online conflict-resolution workflow.

- Design tradeoffs and known limitations:
  - Final metrics intentionally use *_canonical replacement snapshots rather than the original direct append sums. Refresh scans grow with lifetime unique IDs. A one-second schedule is not proof of <5s freshness; failed/slow refreshes retain stale prior targets and must be reported by the Phase 4 query API. Revenue and traffic snapshots are independently atomic, not one transaction. Each refresh has a 15s/512MiB budget; a large dataset may exceed it.
  - Identity states have no TTL to avoid forgotten identities becoming new contributions. Raw has 30-day event-time TTL. A reviewed state-retention/privacy/storage lifecycle is needed before external use. Legacy pre-Phase-3 synthetic raw rows missing payload_json are excluded from canonical facts; no guessed/destructive backfill was run.
  - Same-ID/different-payload events quarantine the entire identity, including changed tenant/currency/time. Previously visible contributions are withdrawn after a successful refresh. HTTP itself does not reject these collisions; no replay/resolution control plane exists yet.
  - Source commits certify raw+incremental identity-state persistence, not completed minute refresh. Replays are safe even with different batches because equality compares full normalized payloads, not finite insert-dedup windows or diagnostic hashes. received_at is retained Kafka CreateTime, not actual broker ack/visibility time.
  - Poison/retry exhaustion exits for supervised recovery without committing failed work. The failed batch remains blocked. A durable DLQ publish-before-commit path is Phase 5 work; no unsafe skip is implemented. Healthy partitions in the same epoch can commit completed work before exit.
  - Kafka/ClickHouse remain single-host, non-HA. No AWS, shared-database migration or cloud spend. Services remain running (one normal consumer); three consumers existed inside the integration test and were closed afterward.
  - First integration runs emitted Compose orphan/cache ownership warnings from separate overlays; the Make targets now use the combined overlays and explicit external cache volumes. Final run was clean. No correctness-test failures occurred in Phase 3; the suite was expanded as boundaries were implemented.

- Next phase preview (do NOT implement): query API reading the canonical minute snapshots with refresh health and tenant/window limits, expanded observability and accepted-ID/business-metric reconciliation. Retention/refresh scaling remains a measured tuning concern, not a reason to expose the duplicate-prone legacy sums.
- Approval requested: "Phase 3 is complete. Do you approve proceeding to Phase 4 (analytics query API, observability, and reconciliation)?"

## Files changed in Phase 3

- `.env.example`
- `Makefile`
- `README.md`
- `cmd/consumer/main.go`
- `deploy/compose.consumer.yaml`
- `deploy/docker/consumer.Dockerfile`
- `docs/adr/003-sink-dedup-proposal.md`
- `docs/architecture.md`
- `docs/consumer.md`
- `docs/contracts.md`
- `docs/failure-model.md`
- `docs/phase-3-report.md`
- `internal/analytics/sink.go`
- `internal/analytics/sink_test.go`
- `internal/broker/consumer.go`
- `internal/config/consumer.go`
- `internal/config/consumer_test.go`
- `internal/domain/validate.go`
- `internal/pipeline/batch.go`
- `internal/pipeline/batch_test.go`
- `internal/telemetry/consumer.go`
- `internal/telemetry/telemetry.go`
- `migrations/clickhouse/002_canonical_identity.sql`
- `scripts/consumer-smoke.py`
- `tests/integration/consumer_test.go`
