# StreamForge

Backend-only near-real-time analytics project in Go. **Phase 4: canonical query API, observability and reconciliation** is implemented. DLQ/replay, load/fault benchmarking and AWS remain approval-gated.

## Quick start

Requires Docker/Compose, Python 3.10+ and make. Allow at least 6 GiB Docker memory and 10 GiB disk. Ports 8080, 8082, 9092, 5432, 8123 and 9000 must be free. Go commands use Go 1.26.0; Make falls back to its pinned Docker image when host Go is absent.

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env  # first checkout only; keep your existing .env
make up
make migrate
make ingest-up
make consumer-up
make query-up
make test
make lint
make test-integration
make ingest-smoke
make consumer-smoke
make query-smoke
make smoke
```

An existing Phase 1 .env must also include the ingestion settings in .env.example. All example credentials are **local-only**. The `.env` is ignored and never copied into images. Database passwords in existing volumes do not rotate when environment values change. No AWS account is needed. Host Go is optional; `scripts/go.sh` uses named dependency/build caches. Integration tests run inside the Compose network using `kafka:19092`; a test container must not use Kafka's host-advertised `localhost:9092`. `make up` starts infrastructure only; `make ingest-up` builds the non-root ingestion image and starts it after Kafka health checks.

## Try ingestion

This example uses the demo key from .env.example. Adjust it if you changed the key. Each logical event needs an immutable UUID; the example generates a new one.

```sh
python3 - <<'PYCODE' > /tmp/streamforge-event.json
import json, uuid
from datetime import datetime, timezone
print(json.dumps({"schema_version":1,"event_id":str(uuid.uuid4()),"tenant_id":"merchant_demo","user_id":"user_1","event_type":"purchase","occurred_at":datetime.now(timezone.utc).isoformat(),"order_id":"order_1","amount_cents":12999,"currency":"USD"}))
PYCODE
curl -i http://localhost:8080/v1/events \
  -H 'Authorization: Bearer streamforge_demo_key_local_only_change_me' \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/streamforge-event.json
curl http://localhost:8080/readyz
curl http://localhost:8080/metrics
```

`202` confirms Kafka acknowledgment for every submitted event; Consumers persist to ClickHouse before committing source offsets. `POST /v1/events/batch` accepts a JSON array (default <=100 events, <=1 MiB total, each <=8 KiB). Full validation precedes publishing, but broker publication is non-atomic. Failed/timeout/disconnected requests may have published some or all events. Retry the original IDs; duplicate HTTP submissions currently produce duplicate Kafka records. The consumer deduplicates identical normalized payloads by logical ID. Conflicting payloads quarantine the whole ID and retract any prior metric contribution on refresh; HTTP itself does not reject cross-request conflicts.

Query canonical revenue/traffic at http://localhost:8082 using the separate local QUERY_API_KEY. Requests require tenant_id/from/to/interval=1m and an explicit currency for revenue. Windows are minute-aligned [from,to), at most 24h, with empty minutes zero-filled. Revenue cents are exact integer strings; query responses reject unhealthy/stale refreshes. See [query API/reconciliation](docs/query.md).

See [OpenAPI](api/openapi/streamforge.yaml), [contracts](docs/contracts.md), [configuration](docs/ingestion.md), [architecture](docs/architecture.md), [consumer operations](docs/consumer.md), [failure model](docs/failure-model.md), and [ADRs](docs/adr).

## Verified behavior and limitations

Phase 1 smoke checks legacy SQL shape. Phase 2 tests check broker-acknowledged ingestion and bounds. Phase 3 tests check canonical identity/conflicts, poison offset gaps, real sink writes with a lost response, SIGKILL recovery before/after insert and commit and during rebalance, sink endpoint outage/recovery, and 1→3 ownership. Live pipeline smoke sends ten acknowledged deliveries for five logical IDs and verifies exact canonical IDs and currency-specific minute metrics. Phase 4 adds real ClickHouse golden/late-event/large-money/tenant tests, stale/failure rejection, bounded concurrency/cardinality tests and an independent accepted-manifest audit. See [Phase 4 report](docs/phase-4-report.md), [Phase 3 report](docs/phase-3-report.md), [Phase 2 report](docs/phase-2-report.md) and [Phase 1 report](docs/phase-1-report.md) for actual commands and limits.

Local Kafka is **RF=1, non-HA**. No broker-failure durability proof or benchmark SLO achievement is claimed. Replay-safe canonical projections have passed the local failure tests; this is not an atomic Kafka/ClickHouse transaction or universal exactly-once guarantee. PostgreSQL is not on the ingestion path. Only a configured single-tenant demo bearer key is supported; raw keys are not stored in PostgreSQL. PII is prohibited in attributes, but syntax validation cannot detect all PII. API transport defaults to loopback plaintext. External TLS and multi-tenant key management are later hardening work.

## Observability and operations

Structured JSON request logs contain correlation/trace IDs, outcome and duration; no payload/key is logged. `/metrics` exports bounded outcome labels, HTTP duration and inflight gauges, and producer promise duration/outcome counts. `streamforge_kafka_events_total{outcome="acknowledged"}` counts resolved Kafka acknowledgments, including late outcomes after a caller timeout; HTTP accepted requests are a separate metric. Consumer metrics report assigned partitions, confirmed next offsets, sampled lag, batch sizes, retry failures and insert durations. Final tables are **revenue_1m_canonical** and **traffic_1m_canonical**. The old append aggregates are diagnostic and may double count. One-second full canonical refreshes replace targets atomically but have growing cost; inspect system.view_refreshes for stale/failed snapshots. Identity-state retention is indefinite, with a lifecycle decision still required.

Run `make observe-up` for optional local Prometheus, provisioned Grafana (http://localhost:3000) and Collector → Jaeger (http://localhost:16686). `make observe-smoke MANIFEST=...` checks scrape targets, the 12-panel dashboard/datasources and actual exported pipeline/query spans. See [observability operations](docs/observability.md). OTLP sampling/export remain configurable. Snapshot age is not event freshness.

`make ps` reports container health. `./scripts/compose.sh logs --tail 100 SERVICE` inspects failures. `make ingest-smoke` uses the default demo key; export INGEST_API_KEY/INGEST_TENANT if customized. `make consumer-smoke` also needs CLICKHOUSE_PASSWORD if customized. `make ingest-down` sends SIGTERM to the API; `make consumer-down` stops consumers; `make query-down` stops queries; `make down` stops the project, including the API, **without deleting volumes**. API readiness goes false during drain; in-flight requests settle within shutdown bounds. Smoke tests append a few uniquely named synthetic rows per run. Initial migrations create absent objects; future changes need new versioned files. Down SQL is destructive and never run automatically. Never remove volumes merely to fix a migration error.

## Costs, evidence and next steps

Only local CPU/memory/disk costs; no cloud resources. Retained volumes and Go build caches consume disk after shutdown. [SLOs](docs/benchmarking.md) remain targets, with load/fault experiments planned for later phases. Next is DLQ/replay and fault/performance evaluation, requiring explicit approval. `make reconcile MANIFEST=...` audits a bounded, actually accepted dataset and exits nonzero on discrepancies. No throughput or SLO achievement is claimed. Repository milestones are committed and pushed after relevant validation; credentials, .env, build outputs and large benchmark artifacts remain ignored.

## Interview defense

Explain why broker acknowledgment precedes HTTP success, why timeout is an unknown outcome, why producer retry idempotence differs from logical-event deduplication, why currency must be a revenue grouping key, and why raw-row replacement cannot undo incremental aggregate double counting. The implemented replay strategy uses exact min/max payload states and rebuildable canonical metrics. Explain its conflict-withdrawal policy, temporary snapshot inconsistency and growing refresh/storage cost; these tradeoffs still need measured tuning.

Phase 5 operations: [quarantine/replay](docs/replay.md), [benchmark methodology](docs/benchmarking.md), [measured results](docs/phase-5-report.md). Build the bounded tools with `make tools-build`; `make fault-test`, `make scaling-test` and `make benchmark` are explicit local operations and must run separately. Raw receipt journals are ignored by Git. No AWS resources are provisioned by these commands.

Measured Phase 5: 10,335 Kafka-acknowledged events/s over an awake 900-second interval; all 9.30M accepted IDs/payloads verified after load. Ingestion latency, query/freshness, final aggregate reconciliation and 2x scaling targets **did not pass**. See the [one-page benchmark evidence](docs/benchmark-report.md) before using performance claims.
