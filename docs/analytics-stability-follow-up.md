# Local analytics stability follow-up — November 9

The original end-to-end performance targets remain unmet. No new fifteen-minute benchmark was run; normal retained readiness did not pass its gate. AWS remains deferred.

## Completed changes

- Migration 008 uses one named-tuple JSON extraction per canonical row. EXPLAIN shows one JSON extraction versus six in the earlier metric plan. Missing/null optional fields, Int64 bounds, nanosecond UTC, conflicts, predicate safety and exact totals pass native integration. Timings are mixed across shared-host experiments; no general speedup is claimed.
- Container-based allocation-counter correction preserves the 4 GiB Docker cap, 90% automatic server cap and 64 MiB source-query budgets. The 1 GiB disposable resource/grants fixture passes. Subsequent sampled windows had no source Code 241 failures, but execution timeouts remained.
- Migration 009 and API health now compute completion from successful start plus duration. The ten-second completed-snapshot limit is unchanged; the separate p95 event-freshness target stays below five seconds. Native fixtures test eleven positive/negative cases, including absent duration/source/wrapper, failure, disabled source, stale completion and legitimate zero duration.
- Consumer metadata retries are bounded and restricted to empty polls before any current-epoch records are collected. Buffered/current records, permanent errors and corruption remain fail-closed. Empty polls do not reset the retry budget; confirmed commits do. The fixture waits for all twelve topic leaders, and concurrent child diagnostics no longer race.
- Eventgen now measures revenue and traffic over both 15-minute and 24-hour windows, with USD/EUR revenue and separate successful quantiles/error counts. Earlier traffic-only runs are not relabeled.
- Migration 010 increases vectorized blocks within existing hard limits. Its retained trial still produced only one ready response in twelve samples. This is not a completed performance repair.

The final local full integration run passed in 161.605s; affected unit/race tests passed. [Hosted CI](https://github.com/Prathampthk/StreamForge/actions/runs/42689464878) passed all six jobs on 0fec5ce, including the final retry-budget/error-cause changes and all current migrations. Earlier failed runs are retained privately. The tests exposed a cross-shard EUR convergence assumption, a failure-diagnostic race, transient Kafka group startup and fresh-topic metadata timing; those failures are disclosed rather than omitted.

## Rejected storage experiment

`scripts/analytics-decoded-experiment.py` copied 992,254 identity rows from one hash partition into a unique owned table. It compared runtime tuple decoding with a stored materialized tuple at concurrency 1, 4 and 16. All 42 queries returned exactly the same 1,095 metric rows. At concurrency sixteen, group wall time was 5.2674s for runtime parsing and 6.1643s for stored fields. Other levels were mixed. Both candidates reused the same copied partition on a warm shared host; this is not sixteen distinct retained shards or an SLO workload.

No reliable concurrent improvement was established, so no stored-field column or materialization was applied to retained identity data. The experiment removed its owned table and restored active schedules with no cleanup errors. Two earlier harness failures are preserved: read-only SETTINGS rejection and an alias substitution bug. See [small evidence](evidence/decoded-storage-experiment.json).

## Remaining blocker and next work

The analytical read path still scans retained canonical history. Correct accounting and correct snapshot-age semantics do not make that work incremental. Keep readiness fail-closed and preserve accepted receipts, facts and earlier benchmark results. The next design must bound refresh work by new/changed identities or buckets while preserving global-ID conflicts and withdrawal; validate exact fixtures and measure an isolated prototype before another retained migration. A per-event PostgreSQL ledger or other topology change is not implemented or approved by this report.

At handoff, Docker was unreachable, so no final live container inspection or further runtime test was possible. The completed experiment's recorded cleanup succeeded before that interruption. Do not interpret an unavailable daemon as evidence of a database crash.
