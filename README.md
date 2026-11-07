# StreamForge

Backend analytics portfolio project. **Phase 1 foundation only**: local Kafka, PostgreSQL, ClickHouse, contracts, SQL smoke checks and design records. There is no ingestion, consumer, query API, replay runtime or AWS deployment.

## Quick start

Prerequisites: Docker with Compose, Python 3.10+, make; Go 1.26.0 for future Go checks. Allow at least 6 GiB Docker memory / 10 GiB disk. Ports 9092, 5432, 8123 and 9000 must be free. All published ports bind loopback.

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
make up
make migrate
make migrate  # intentional safe initial-DDL rerun
make ps
.venv/bin/python -m unittest discover -s tests -v
make smoke
make down     # retains named volumes
```

`make up` creates .env from development-only defaults if absent. To customize, copy .env.example first and edit it. Compose reads it explicitly. Changing passwords after creating database volumes does not rotate database credentials. Never use these credentials outside local development. No AWS account is needed. Smoke appends six small synthetic rows under a unique tenant per run; it never clears existing data.

`make test` runs contract tests using the active Python environment; activate .venv first. `make fmt` and `make lint` require the pinned Go toolchain; only the Go event contract exists; there are no runtime services. There are no external Go dependencies, hence no go.sum. Initial migrations create missing objects; changed definitions require new versioned migrations. Do not execute the PostgreSQL down file unless intentionally deleting metadata. Never delete volumes to fix migrations without first examining data.

## Architecture and guarantees

See [architecture](docs/architecture.md), [contracts](docs/contracts.md), [failure model](docs/failure-model.md), and [ADRs](docs/adr). Raw facts feed separate currency-specific revenue and currency-independent traffic minute states. Local Kafka is **RF=1, non-HA**. The foundation does not guarantee logical-event deduplication or exactly-once analytics. No performance SLO has been measured; [targets](docs/benchmarking.md) are not achievements.

## Operations and next steps

`make ps` reports health; `./scripts/compose.sh logs --tail 100 SERVICE` inspects failures. Resolve occupied ports or Docker memory pressure, then rerun `make up`. Bring services up before migrations. PostgreSQL migrations fail on errors; ClickHouse reruns recover missing DDL objects. Optional observability is deferred until services exist. No provisioned cloud cost; local Docker uses CPU, memory and disk. Retained volumes consume disk even after shutdown.

HTTP API examples, Grafana dashboards, fault experiments, CI/CD and benchmark evidence will be added only in their approved phases. Phase 2 preview: bounded Go ingestion that responds success only after Kafka acknowledgment. Approval is required before beginning.

## Interview defense

Explain why acknowledgment precedes HTTP success; why offsets follow sink confirmation; why currency must be a revenue grouping key; and why raw-row deduplication cannot repair incremental aggregate double counting. The key unresolved engineering risk is crash-safe, logical-event deduplication across Kafka and ClickHouse—not merely retrying a batch. No hard production bug or performance result is claimed yet.
