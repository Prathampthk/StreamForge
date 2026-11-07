# Local pipeline runbook

`make observe-up`, `make migrate`, `make tools-build` start the existing local environment. `make down` preserves volumes. Do not delete volumes or canonical state to make an audit or benchmark pass. Local keys/passwords are development-only; production deployment/security is a separate phase.

## Diagnose readiness and backlog

- Ingest `/livez` reports process health; `/readyz` checks Kafka leaders. A successful 202 means all event promises resolved successfully; a 503/disconnect can still have published data. Retry original IDs rather than generating replacements.
- Query `/livez` can succeed while `/readyz` and metric endpoints return 503 because snapshots are failed/stale. This is deliberate. Inspect `system.view_refreshes` for status, last success and exception. A one-second schedule is not a freshness guarantee.
- Consumer logs contain partition/next-offset metadata, never event bodies. Prometheus shows assignments, observed lag, confirmed commits, retries and epoch failures. Observed lag is sampled and may be stale while idle. An exhausted sink/quarantine retry exits and Compose restarts the process; Kafka retains uncommitted work.
- Monitor disk and Docker RSS/CPU. Kafka retains seven days, raw rows thirty event-time days, identity states indefinitely. Raw/state/receipt/spill data all consume space; Docker caps do not reserve memory. An outage beyond retention is outside the recovery guarantee.

## ClickHouse memory/readiness failures

The Phase 5 growing-history experiment recorded memory-limit errors during full canonical recomputation. Old snapshots remain intact but are rejected as stale by the query API. More ingestion or consumer replicas do not repair this read bottleneck. Keep receipt journals and source offsets, stop optional load generation, let consumers drain, and inspect actual exceptions/resource limits before changing settings. Do not replace canonical queries with duplicate-prone legacy append sums.

Migration 003 is the adopted bounded profile: 256MiB/two threads per refresh, 64MiB external aggregation threshold; server caches 128/64MiB and ClickHouse cap 4GiB on the measured host. `load/ordered-refresh-experiment.sql` is a **failed** experiment, intentionally excluded from migrations. Larger memory budgets and ordered aggregation alone did not restore this dataset's health. Any structural fix must retain global identity conflicts, withdrawal of previous revenue/minute contributions, UTC bucketing and exact integer currency totals, then rerun correctness and sustained measurements.

## Poison and replay

Use [replay.md](replay.md). A DLQ/audit failure retains poison and its following partition tail; valid prefixes may commit. Inspect the compact audit before replaying. Dry-run first, use explicit operator/tenant/bounds and a new execution job UUID, preserve original event IDs, and resume with the identical selector. Repeated or ambiguous publication is at least once; canonical identity prevents normal same-payload count inflation. Never log raw DLQ bodies or put credentials into repair files.

## Measurements and evidence

Use [benchmarking.md](benchmarking.md). Keep the Mac lid open. The wrapper's process-scoped inhibitor prevents idle sleep only. A sleep-interrupted interval cannot establish sustained throughput. Normal benchmark startup requires healthy snapshots; `--allow-unready-query` is an explicit degraded-baseline experiment that records readiness failure and every query/probe error. It cannot become a combined SLO pass.

Run fault injection and scaling separately from the sustained profile. Audit every successful receipt after drain; retain unverified reads, unknown HTTP outcomes, metric mismatches and failed/censored probes in the report. A missing-at-audit ID or failed read is not evidence of permanent disk loss. Commit small summaries/graphs; keep huge private journals in ignored raw directories.
