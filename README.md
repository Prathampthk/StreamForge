# StreamForge

A backend analytics project in Go: validate events, acknowledge Kafka publication, persist with safe partition offsets, and query canonical minute metrics. Its purpose is to make distributed-system guarantees and failure boundaries explicit.

**Status:** the local pipeline, DLQ/replay and measurements are implemented. Phase 6 AWS deployment/security/CI preparation is validated **offline only**. Nothing has been deployed to AWS. Local [analytics remediation](docs/analytics-remediation-report.md) is in progress: exact-string storage and verified backfill tooling pass disposable checks; retained-data cutover and end-to-end SLO retesting remain pending. This is not a production-ready analytics service.

## Architecture

```mermaid
flowchart LR
  Events[Event clients] --> Ingest[Go ingest]
  Ingest -->|broker ACK before 202| Kafka[Kafka: 12 partitions]
  Kafka --> Consumer[Partition consumers]
  Consumer -->|insert before commit| CH[ClickHouse canonical identity]
  CH --> Refresh[Atomic minute snapshots]
  Refresh --> Query[Go query API]
  Consumer --> DLQ[Durable DLQ + PG audit]
  DLQ --> Replay[Bounded replay CLI]
  Replay --> Kafka
```

See the [full local/AWS architecture](docs/final-architecture.md), [guarantees](docs/failure-model.md), [ADRs](docs/adr), [runbook](docs/runbook.md) and [OpenAPI](api/openapi/streamforge.yaml).

## Quick start

Requires Docker/Compose, Python 3.10+ and make. The Go toolchain is pinned to 1.26.10; host Go is optional because Make uses its digest-pinned Docker image. Allow roughly 8GiB Docker memory; large benchmark journals/state/spills need much more disk than smoke tests. Ingest/query/Kafka/PostgreSQL/ClickHouse host ports must be free. Existing volumes and `.env` are retained.

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env                 # first checkout only
make up migrate
make ingest-up consumer-up query-up
make test lint
make test-integration
make ingest-smoke consumer-smoke query-smoke
```

All sample credentials are local-only; listeners publish on loopback. Do not overwrite an existing `.env`. Changing a database password variable does not rotate credentials in existing volumes. `make down` stops services without deleting persisted volumes. A failed readiness check on an existing large dataset can be the documented refresh failure, not an instruction to delete data.

## API examples

Create a valid event in a private temporary file using an immutable UUID:

```sh
python3 - <<'PY' > /tmp/streamforge-event.json
import json,uuid
from datetime import datetime,timezone
print(json.dumps(dict(schema_version=1,event_id=str(uuid.uuid4()),tenant_id='merchant_demo',user_id='user_1',event_type='purchase',occurred_at=datetime.now(timezone.utc).isoformat(),order_id='order_1',amount_cents=12999,currency='USD')))
PY
curl http://localhost:8080/v1/events \
  -H 'Authorization: Bearer streamforge_demo_key_local_only_change_me' \
  -H 'Content-Type: application/json' --data-binary @/tmp/streamforge-event.json
```

`POST /v1/events/batch` takes a bounded JSON array. A successful response includes event IDs and broker-callback acknowledgment times. Validation precedes publishing; a batch is not an atomic Kafka transaction. A 503/disconnect is delivery-unknown: retry the original IDs.

The separate query API is at `localhost:8082`, using the demo key `streamforge_query_demo_key_local_only`. Revenue/traffic requests require tenant_id, minute-aligned RFC3339 from/to, interval=1m and explicit currency for revenue. Windows are <=24h, half-open UTC; empty minutes are dense zeros and exact cent totals are strings. Queries reject failed/stale snapshots. See [query examples](docs/query.md) and [contracts](docs/contracts.md).

## Guarantees and limits

- 202 follows successful broker callbacks for every submitted event, not local enqueueing. RF=1 is non-HA and proves no broker/disk-loss durability.
- Consumers commit only safely persisted or durably quarantined contiguous partition prefixes. Sink failures remain in Kafka; DLQ/audit failure retains poison and its following tail.
- Same normalized payload/ID replays do not multiply canonical facts. Different payloads for a global ID quarantine the whole identity and withdraw its prior contribution on refresh. Legacy append sums are diagnostic only.
- Kafka, ClickHouse and PostgreSQL are not one exactly-once transaction. DLQ/replay publish-before-audit gaps can repeat stable IDs. Replay is bounded, audited and dry-run by default: [operations](docs/replay.md).
- Full-history refreshes retain old snapshots on failure. The query API rejects them; indefinite identity retention and growing scan cost require structural work before a reliable large-history analytics demo.
- Local auth maps one key to one tenant. External multi-tenant identity/key lifecycle and HA are not accomplished guarantees. Verified TLS/custom CA and Kafka SCRAM are available for the prepared private deployment; local Compose stays loopback/plaintext.

## Actual benchmark evidence

Historical Phase 5 measurement: one Apple M4/16GiB Mac, ~7.75GiB Docker, Go 1.26.0; all components and the driver shared the host. Later dependency/toolchain patches were not re-benchmarked.

| Measurement | Actual result |
|---|---|
| Kafka acknowledgments over an awake 900 seconds | **10,335 events/s**; throughput target passed for acceptance only |
| Post-load canonical facts | **9,303,564 IDs/payload hashes verified**, zero missing/mismatched/unverified |
| Client ingestion p95 | **248.58ms**, failed <100ms |
| Query/freshness | All measured queries and probes failed/censored; no successful quantiles |
| Final revenue/traffic snapshots | Failed exact reconciliation |
| Consumer 1→3 throughput | **1.91x**, failed 2x target |

The run began with failed analytics readiness. ID auditing isolated failed refresh work after load and retried transient reads; it establishes persistence for that cohort, not live freshness or correct served aggregates. An earlier interval interrupted by host sleep is preserved and disqualified. Read the [one-page benchmark report](docs/benchmark-report.md), [full Phase 5 report](docs/phase-5-report.md), [methodology](docs/benchmarking.md) and [small result artifacts](load/results). Huge private journals stay ignored.

## Tests, observability and failures

`make test lint` covers contracts, race tests and static checks. `scripts/integration-isolated.sh` uses separate test-owned infrastructure/ports and preserves the normal dataset. Real-service tests cover SIGKILL before/after inserts/commits, ambiguous responses, rebalances, outages, late/conflicting events, exact money, DLQ failure and replay. Topic-creation tests wait for actual leader readiness.

`make observe-up` starts optional Prometheus/Grafana/Collector/Jaeger; Grafana has 16 provisioned panels. Snapshot age is not event freshness; the load driver measures per-probe contribution visibility. Logs omit event bodies/keys; metric labels are bounded. See [observability](docs/observability.md). `make fault-test`, `make scaling-test` and `make benchmark` are explicit local experiments: run them separately. Keep the Mac lid open for a continuous measurement.

## Offline cloud preparation and security

```sh
make cloud-check        # Terraform schemas/mock plans, cert/config and workflow checks
make security           # reachable Go vulnerability gate
make reproducible       # independent binaries and representative runtime image
```

Prepared infrastructure: private ECS tasks, private EC2 Kafka/PostgreSQL and ClickHouse, encrypted retained disks/state/artifacts, scoped secrets and IAM, SSM access, optional ACM/ALB HTTPS, immutable ECR digests and a protected manual OIDC image-publishing workflow. There is **no automatic apply/deploy/destroy**. Public access and publisher roles are off by default. No AWS credentials are mounted by offline tooling.

CI uses pinned action commits and tools, unit/race/integration tests, govulncheck, workflow lint and HIGH/CRITICAL image/IaC gates. Security checks are evidence at a point in time, not a universal vulnerability guarantee. Vendor images must also pass scans before any future publication. The build patch/dependency updates cleared the observed reachable findings and tested ingest-image HIGH/CRITICAL findings.

Read [deployment/rotation](docs/aws/deployment.md), [cost assumptions](docs/aws/costs.md), [teardown/retention](docs/aws/teardown.md), [ADR 008](docs/adr/008-aws-deployment.md), and the [Phase 6 offline report](docs/phase-6-report.md). Rough planning is ~$230–$300/month if the private demo is left running under illustrative US East rates; this is **not a current regional quote or approved spend**. Endpoint/data retention continues to cost money after stopping compute. A live plan, spend, upload/publication/apply and cleanup require separate review/approval.

## Interview defense

The hard correctness bug is append metrics overcounting retries even when raw storage deduplicates later. Full normalized min/max identity states fix regrouped replay/conflict semantics, but their full-refresh strategy then fails under growing history. That measured tradeoff is visible rather than hidden behind a generated-rate claim. Durable DLQ and PostgreSQL audit expose an honest at-least-once crash gap; bounded concurrency and contiguous offsets prevent skipping poison. Integer minor units and explicit currency avoid money errors. [Resume evidence](docs/resume-evidence.md) lists defensible claims and claims to avoid.

Retained-data validation exposed a remaining failure after the first merge optimization. The exact-string backfill/cutover completed for 15,871,442 retained identities. Partitioned metric refreshes match the canonical event total but still have intermittent server-memory failures; see the [measurements and migration boundary](docs/analytics-remediation-report.md). The next performance gate is a new concurrent benchmark, followed by any explicitly approved cloud validation environment. No cloud performance, live IAM/SG, HA or cleanup result has been invented.

Useful work before AWS and remaining gates are tracked in [offline readiness](docs/offline-readiness.md).
