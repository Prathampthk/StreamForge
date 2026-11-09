# ADR 013: parse canonical JSON once

## Trigger and decision

The current view extracts each business field from a String payload separately. A retained single-partition probe compared existing metrics to a named-tuple JSONExtract candidate with the same FINAL/full-string conflict policy and 64 MiB/one-thread query budget. Five completed reads returned identical 1,095 metric rows: separate extraction took 1.356–1.488s; tuple extraction took 0.942–0.949s. A sixth read failed the existing global memory limit; the probe is not a sustained-health or controlled speedup claim. All refresh schedules were restored afterward.

Migration 008 replaces only the canonical read view with one named-tuple JSON extraction. Payload strings, identity storage, exact conflict exclusion, UTC nanoseconds, Int64 inputs/Int128 sums, query budgets and freshness targets stay unchanged. Missing/null optional fields use the same typed defaults. No new storage backfill, daemon, PostgreSQL hot-path lookup or AWS operation is introduced.

## Gates and limits

The regression checks missing and null optional fields, API status, maximum Int64 input and nanosecond timezone normalization before/after identity merges, alongside exact payload equivalence, global conflict predicates and Int128 sums. Full integration must pass before retained rollout. Readiness and exact metrics must then be measured with all refresh schedules active; the small probe alone does not justify a recovery claim.

This reduces repeated parsing but retains full-history scan work and independent shard snapshots. If global memory or scan cost still prevents healthy sustained operation, do not relax health limits or claim the bottleneck solved. Rollback restores the canonical view definition from migration 006 without rewriting facts or identity values; use an explicit forward migration, not ledger replay.

## Additional validation

The full isolated integration rerun passed in 119.402s. Its first run exposed an existing cross-shard timing assumption: USD visibility was treated as proof that EUR had incorporated the same batch. The test now independently waits for the same exact EUR result within 15 seconds, as traffic already did. It does not claim a cross-shard transaction or change an SLO.

The reproducible retained comparison is `python3 scripts/compare-canonical-parsing.py --pause-refreshes --rounds 2 --out <new-result.json>`. It checks every metric row, alternates order, preserves failures and restores the schedules it found active. Its first normal-operation run still failed global memory after a successful baseline read. A diagnostic repeat with system-table merges paused completed all four reads, with wall times 1.4007–1.6886s for tuple and 1.4750–1.6337s for separate extraction; this repeat does not establish a timing gain. System merges and all active refreshes were restored. The optimization is a correctness-verified candidate with a promising earlier sample, not a proven sustained performance fix.
