# Local pipeline runbook

`make observe-up`, `make migrate`, `make tools-build` start the existing local environment. `make down` preserves volumes. Do not delete volumes or canonical state to make an audit or benchmark pass. Local keys/passwords are development-only; production deployment/security is a separate phase.

## Diagnose readiness and backlog

- Ingest `/livez` reports process health; `/readyz` checks Kafka leaders. A successful 202 means all event promises resolved successfully; a 503/disconnect can still have published data. Retry original IDs rather than generating replacements.
- Query `/livez` can succeed while `/readyz` and metric endpoints return 503 because snapshots are failed/stale. This is deliberate. Inspect `system.view_refreshes` for status, last success and exception. A one-second schedule is not a freshness guarantee.
- Consumer logs contain partition/next-offset metadata, never event bodies. Prometheus shows assignments, observed lag, confirmed commits, retries and epoch failures. Observed lag is sampled and may be stale while idle. An exhausted sink/quarantine retry exits and Compose restarts the process; Kafka retains uncommitted work.
- Monitor disk and Docker RSS/CPU. Kafka retains seven days, raw rows thirty event-time days, identity states indefinitely. Raw/state/receipt/spill data all consume space; Docker caps do not reserve memory. An outage beyond retention is outside the recovery guarantee.

## ClickHouse memory/readiness failures

The Phase 5 growing-history experiment recorded memory-limit errors during full canonical recomputation. Old snapshots remain intact but are rejected as stale by the query API. More ingestion or consumer replicas do not repair this read bottleneck. Keep receipt journals and source offsets, stop optional load generation, let consumers drain, and inspect actual exceptions/resource limits before changing settings. Do not replace canonical queries with duplicate-prone legacy append sums.

Use `scripts/migrate-clickhouse.sh` and its ledger. Retained legacy data requires the approved exact backfill before 006; never replay older canonical view definitions over a completed cutover. The retained installation records 1–7. Migration 007 uses sixteen 64MiB/one-thread source refreshes every four seconds, then sums compact caches into the two endpoint snapshots. Query readiness checks every source shard and wrapper. Container cap remains 4GiB and server caches 128/64MiB. These bounds have not established sustained retained-data health: intermittent global memory errors remain. Inspect fixed-size `system.merges`, `system.metrics`, `system.asynchronous_metrics` and `system.view_refreshes` fields; avoid scanning huge historical `system.query_log.query` strings during memory pressure.

`load/ordered-refresh-experiment.sql` and the allocator-accounting restart experiment both failed to restore this dataset's health. Preserve exact global identity conflicts, withdrawal, UTC bucketing and integer currency totals in further fixes. Pause legacy `event_identity_states` merges again after a restart if preserving its frozen diagnostic state; never reattach its write MV as a shortcut rollback after new writes. See [the remediation report](analytics-remediation-report.md) for measured results.

## Backup and recovery

Run the separate [disposable recovery rehearsal](backup-restore.md). It never restores over retained data. Keep configuration/credentials and Kafka retention/replay planning separate from database snapshots. The rehearsal is not a full retained-data backup or a production recovery-time result.

## Poison and replay

Use [replay.md](replay.md). A DLQ/audit failure retains poison and its following partition tail; valid prefixes may commit. Inspect the compact audit before replaying. Dry-run first, use explicit operator/tenant/bounds and a new execution job UUID, preserve original event IDs, and resume with the identical selector. Repeated or ambiguous publication is at least once; canonical identity prevents normal same-payload count inflation. Never log raw DLQ bodies or put credentials into repair files.

## Measurements and evidence

Use [benchmarking.md](benchmarking.md). Keep the Mac lid open. The wrapper's process-scoped inhibitor prevents idle sleep only. A sleep-interrupted interval cannot establish sustained throughput. Normal benchmark startup requires healthy snapshots; `--allow-unready-query` is an explicit degraded-baseline experiment that records readiness failure and every query/probe error. It cannot become a combined SLO pass.

Run fault injection and scaling separately from the sustained profile. Audit every successful receipt after drain; retain unverified reads, unknown HTTP outcomes, metric mismatches and failed/censored probes in the report. A missing-at-audit ID or failed read is not evidence of permanent disk loss. Commit small summaries/graphs; keep huge private journals in ignored raw directories.
