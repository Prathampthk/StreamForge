# StreamForge

Backend-only near-real-time analytics project in Go. **Phase 2: broker-acknowledged ingestion** is implemented. Consumers, query API, replay and AWS are still approval-gated.

## Quick start

Requires Docker/Compose, Python 3.10+ and make. Allow at least 6 GiB Docker memory and 10 GiB disk. Ports 8080, 9092, 5432, 8123 and 9000 must be free. Go commands use Go 1.26.0; Make falls back to its pinned Docker image when host Go is absent.

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env  # first checkout only; keep your existing .env
make up
make migrate
make ingest-up
make test
make lint
make test-integration
make ingest-smoke
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

`202` confirms Kafka acknowledgment for every submitted event; ClickHouse processing is not implemented. `POST /v1/events/batch` accepts a JSON array (default <=100 events, <=1 MiB total, each <=8 KiB). Full validation precedes publishing, but broker publication is non-atomic. Failed/timeout/disconnected requests may have published some or all events. Retry the original IDs; duplicate HTTP submissions currently produce duplicate Kafka records. **Logical-ID conflict rejection and duplicate-free analytics are not implemented.** Do not claim them until the Phase 3 gate is resolved.

See [OpenAPI](api/openapi/streamforge.yaml), [contracts](docs/contracts.md), [configuration](docs/ingestion.md), [architecture](docs/architecture.md), [failure model](docs/failure-model.md), and [ADRs](docs/adr).

## Verified behavior and limitations

Phase 1 smoke verifies six unique synthetic raw rows, currency-separated revenue, UTC minute buckets, API RPM and error rates. Phase 2 tests verify strict validation, tenant isolation, bounds, acknowledgment-before-success, producer buffer saturation, absent-broker 503, W3C propagation, actual Kafka key/headers/payload and SIGTERM during an unacknowledged request. See [Phase 1 report](docs/phase-1-report.md) and [Phase 2 report](docs/phase-2-report.md) for actual commands and limitations.

Local Kafka is **RF=1, non-HA**. No broker-failure durability proof, logical-event deduplication guarantee, consumer processing, or benchmark SLO achievement is claimed. PostgreSQL is not on the ingestion path. Only a configured single-tenant demo bearer key is supported; raw keys are not stored in PostgreSQL. PII is prohibited in attributes, but syntax validation cannot detect all PII. API transport defaults to loopback plaintext. External TLS and multi-tenant key management are later hardening work.

## Observability and operations

Structured JSON request logs contain correlation/trace IDs, outcome and duration; no payload/key is logged. `/metrics` exports bounded outcome labels, HTTP duration and inflight gauges, and producer promise duration/outcome counts. `streamforge_kafka_events_total{outcome="acknowledged"}` counts resolved Kafka acknowledgments, including late outcomes after a caller timeout; HTTP accepted requests are a separate metric. OTLP tracing is optional; set OTEL_EXPORTER_OTLP_ENDPOINT and TRACE_SAMPLE_RATIO. No collector/dashboard is provisioned yet.

`make ps` reports container health. `./scripts/compose.sh logs --tail 100 SERVICE` inspects failures. `make ingest-smoke` uses the default demo key; export INGEST_API_KEY/INGEST_TENANT if customized. `make ingest-down` sends SIGTERM to the API; `make down` stops the project, including the API, **without deleting volumes**. API readiness goes false during drain; in-flight requests settle within shutdown bounds. Smoke tests append a few uniquely named synthetic rows per run. Initial migrations create absent objects; future changes need new versioned files. Down SQL is destructive and never run automatically. Never remove volumes merely to fix a migration error.

## Costs, evidence and next steps

Only local CPU/memory/disk costs; no cloud resources. Retained volumes and Go build caches consume disk after shutdown. [SLOs](docs/benchmarking.md) remain targets, with load/fault experiments planned for later phases. Next is partition-aware consumption and sink correctness, requiring explicit approval. Repository milestones are committed and pushed after relevant validation; credentials, .env, build outputs and large benchmark artifacts remain ignored.

## Interview defense

Explain why broker acknowledgment precedes HTTP success, why timeout is an unknown outcome, why producer retry idempotence differs from logical-event deduplication, why currency must be a revenue grouping key, and why raw-row replacement cannot undo incremental aggregate double counting. The unresolved hard boundary is correctness across Kafka and ClickHouse after ambiguous sink writes.
