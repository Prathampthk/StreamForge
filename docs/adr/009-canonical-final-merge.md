# ADR 009: stream canonical identity merges (accepted local remediation)

## Objective and proposed slice

Phase 5's full-history `GROUP BY event_id` retains full min/max payload strings and exceeds the refresh query's 256 MiB budget. Test reading `event_identity_states FINAL` and applying `finalizeAggregation` to the existing min/max states. AggregatingMergeTree merges equal sorting keys during FINAL; this may stream identities instead of maintaining a hash table for every identity. Exact full-string conflict comparisons, global UUID identity, immutable raw evidence and sink-before-offset commits must remain unchanged.

Initial files: this ADR and `scripts/analytics-merge-experiment.py`. If the pinned-version experiment passes, add migration `004_stream_identity_merges.sql`, an unmerged-part integration regression, and a local remediation report/evidence. Do not edit historical measurements or automatically migrate the benchmark volume. No AWS work.

## Decisions and risks

- Use the existing immutable identity states; no per-event PostgreSQL ledger, payload hashes as identity, retention change or data backfill.
- Compare the GROUP BY baseline and FINAL candidate on the same deterministic data, with background merges stopped so correctness cannot accidentally depend on physical consolidation. Exercise replays and payload changes across tenant/minute/currency.
- Finalize both states **after** FINAL. Prevent PREWHERE from filtering non-key payload variants before merging; predicates must not resurrect a globally conflicted identity.
- Keep the 256 MiB/two-thread refresh budget. Measure memory/time at increasing history sizes in a disposable container, not against shared benchmark data. FINAL still reads retained history; reduced memory is not bounded incremental work or an established throughput/freshness SLO.

## Evidence and remaining boundary

The isolated integration suite passed (89.678s), including exact-payload equality against the original query and predicate safety before/after physical merges. At five million synthetic identities, the original query exceeded its 256 MiB budget; FINAL completed with 72,113,912 bytes of query memory on the same data/settings in a 2 GiB container. Timing is a query microbenchmark with the candidate second (potential warm-cache advantage), not an end-to-end latency claim. No historical benchmark data was migrated. See the remediation report for larger results and hosted CI.

## Test checklist

1. Exact canonical payloads and conflict IDs match the original query before/after physical merges, including different insert groupings and changed payloads.
2. Candidate and baseline produce identical revenue/traffic results, including Int128 sums and cross-tenant/time/currency conflicts.
3. Record query-log memory, duration and errors under identical settings. Adopt only if correctness passes and resource behavior improves.
4. Run existing real-service integration and hosted CI after any migration. The full 15-minute concurrent benchmark remains a separate validation; no claimed gain from a microbenchmark alone.

References: [FINAL modifier](https://clickhouse.com/docs/sql-reference/statements/select/from#final-modifier), [AggregatingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/aggregatingmergetree), [PREWHERE](https://clickhouse.com/docs/sql-reference/statements/select/prewhere). Demonstrate behavior on pinned 25.8.4.13 rather than assuming current defaults.
