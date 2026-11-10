# Adversarial incremental analytics — storage limits and unsafe ownership control

**The prototype is not safe for independent writers.** A deliberately controlled split-brain schedule changes a reader's already-pinned generation. A successful test run here means the harness reproduced that counterexample alongside its single-writer correctness gates; it is not a production concurrency pass. No production consumer, retained data, serving view, PostgreSQL schema or AWS resource is changed.

Run `make incremental-analytics-stress`, or:

```sh
python3 experiments/incremental-analytics/prototype.py stress --out <new-summary.json>
```

This extends the [original isolated experiment](incremental-analytics-prototype.md) in a fresh owned, network-disabled ClickHouse container, with the same pinned image, 2 GiB container cap and 64 MiB query budgets. Initialization, worker orchestration and synthetic source assumptions remain disclosed there. Cases use planned server restarts before hot-ID, many-bucket and each ownership scenario to avoid carrying caches/allocator state between independent cases. CI runs the original compacted-scale fixture and this stress command as separate container lifecycles, and explicitly includes the expected unsafe controls. Test outputs separate `success` (harness gates) from `production_concurrency_safe=false`.

## What is tested

| Scenario | Fixture and gate |
|---|---|
| Unmerged parts | Seed 128,000 IDs into 64 overlapping UUID-range parts while merges are stopped. Process a 384-record duplicate/conflict/new-ID batch while seed parts remain unmerged and check exact counts. Read the same 256 IDs at generation one before and after merging only the owned tables; full min/max strings and current totals must match. |
| Hot identity history | Eight purchase IDs gain a different normalized attribute extreme for each of 32 generations; after generation two they remain conflicted and cannot contribute. One new page view per generation keeps the same minute bucket active. Every generation must update nine states, and generations after two emit only the new page-view delta. |
| Historical pins | After merging and a real ClickHouse restart, independently rebuild and compare every identity string and metric row at generations 1, 2, 8, 16 and 32. Old valid pins must retain old results. |
| Bucket history | Measure the selected minute query as generations accumulate; merge the tables and measure again. A physical merge must not erase generation history needed by pinned snapshots. This is a growth measurement, not a compactor implementation. |
| Many buckets | One 1,024-event batch covers 32 tenants × 32 minutes, producing 2,048 traffic/revenue keys. Verify every row against the independent rebuild. A second batch conflicts 128 existing IDs across their old tenant/minute/currency buckets; verify the 1,792 remaining metric rows. Check a narrow tenant/minute query separately. |
| Split brain — expected unsafe | Two real worker processes use independent local lock roots. Both pass the same head/plan checks and pause before writing. A publishes generation one; a reader pins it. B then resumes its stale plan and writes a different source batch under generation one. Both processes complete successfully; the pinned result changes and two distinct inputs claim the same committed generation. |

A second split-brain case reuses the **same global event ID** with a conflicting payload. Merges are paused to hold both variants and make the verification-failure path deterministic. B exits with an identity-verification error and does not publish a commit, yet its staged deltas still change A's pinned snapshot. Only one commit record exists. This demonstrates why rejecting the stale publication is insufficient without attempt-isolated data.

The split-brain barrier intentionally models hosts with independent filesystems. It bypasses the fixture's same-directory lock by design; it does not target any external system. The point is that the ClickHouse protocol itself does not enforce cross-writer ownership. A pre-write read/check is not a storage-side fence, and adding another read/check alone does not close the race.

## Verification memory repair

The first stress attempt completed the part and hot-ID stages, then failed on the 2,048-key verification expression. A separate empty-table reproduction confirmed ClickHouse Code 241 at the existing 64 MiB query limit. The worker had built one large OR predicate; expression construction alone exhausted the budget.

Verification now reads at most **128 bucket keys per query**, collects the exact logical rows and compares the complete result with the deterministic plan. It neither samples nor skips verification and does not raise the memory budget. Native DateTime columns are also preserved in metric predicates instead of reusing a same-name string alias. Diagnostics retain the beginning of native errors rather than losing the error code behind a long SQL tail.

Ignored raw worker/server logs, failed-run summaries and successful stage checkpoints are retained under `load/results/raw/streamforge-incremental-*`. The original failing run predates partial-stage checkpoint output; its completed worker logs remain, but no missing intermediate measurement is invented. New runs persist `stress-progress.json` after each completed case and record top-level failure details before cleanup.

## Engineering conclusion

A sparse index reduces work on a compacted seed but cannot hide an increasing number of overlapping parts. Repeated changes to an already-conflicted ID still grow its exact state history, and repeatedly updating one bucket grows the deltas it must read. Ordinary merging preserves these distinct generations; it does not provide a bounded historical base.

The ownership counterexample is a correctness blocker before Kafka integration. The next proposed protocol needs attempt-isolated data and an authoritative single winner per generation, followed by compaction that respects pinned readers. The [ownership, offset-recovery and compaction proposal](incremental-analytics-ownership-design.md) specifies the invariants, failure boundaries, estimated batch-coordinator cost and remaining tests. It is a proposal, not implemented or approved production data flow. No per-event PostgreSQL ledger is introduced.

An initial [lookup-only fragmentation run](evidence/incremental-analytics-stress-lookup-only.json) tested the unmerged lookup but compacted the seed before the full update. It is preserved separately; the final worker test keeps the seed unmerged through publication and compares pinned reads on the same data before/after merging.

## Memory scope and measured results

Several later runs hit the **total server** 1.8 GiB limit under the 2 GiB container cap: during million-ID seed loading, after merging fragmented state, and on a nine-ID hot lookup. They failed and cleaned up; [seed failure](evidence/incremental-analytics-stress-resource-seed.json), [fragmented failure](evidence/incremental-analytics-stress-resource-fragmented.json), and [hot failure](evidence/incremental-analytics-stress-resource-hot.json) remain evidence. These are different from the earlier per-query predicate-construction failure and are not relabeled as passes.

The experimental server now overlays [sampling.xml](../experiments/incremental-analytics/sampling.xml), which disables only the wide `metric_log` and `query_metric_log` sampling logs. It keeps query logs, part logs, errors and native per-query statistics, verifies the merged configuration, and retains the same container/query memory limits. The production resource configuration and observability services are unchanged. Runs with this profile are not equivalent to the earlier full-observability measurements; no latency speedup or production memory repair is claimed. Planned case restarts also mean this is not an uninterrupted stability workload. The combined `run --stress` option remains available for reproducing the larger lifecycle; it is not the recommended isolated stress command.

The first sampling-profile stress run passed its **harness** gates in 43.945s with successful cleanup, including both expected unsafe controls; see [pre-commit evidence](evidence/incremental-analytics-stress.json). Source hashes identify this dirty-tree run. The original [combined initial run](evidence/incremental-analytics-stress-combined-initial.json) is retained separately and predates the sampling overlay and second conflict control.

| Measurement | Result |
|---|---:|
| Seed state parts / parts after update | 64 / 65 |
| Same pinned lookup, before / after merge | 128,256 / 84,480 rows |
| All worker SELECT rows while seed unmerged | 256,259 |
| Hot versions, before / after merge | 256 / 256 |
| Hot lookup rows, generation 1 / 32 | 9 / 288 |
| Minute query rows, generation 1 / 32 / after merge | 2 / 34 / 34 |
| First many-bucket batch / remaining rows after conflicts | 2,048 / 1,792 metric keys |
| Narrow tenant/minute query | 512 rows read |
| Both unsafe controls | pinned generation 1 changes from 2 to 4 metric rows |

The fragmented worker reads more physical rows across its verification queries than the seed contains. Hot-state and bucket histories continue growing after normal merges. These are measured limitations, not a bounded-work or freshness guarantee.

The final clean-source runs on `87e9f77` passed in **27.862s** (compacted-scale fixture) and **42.973s** (independent adversarial fixture), with verified sampling configuration and successful cleanup. [Selected clean-run evidence](evidence/incremental-analytics-stress-verified.json) records provenance, raw project locations, all physical-read counts, historical-pin audits and both unsafe controls. The conflicting-ID control explicitly records stale-worker exit code 2, one committed input, and a pinned snapshot changing from two to four metric rows. This is evidence of unsafe publication even when stale verification rejects the write; the full global-ID rebuild would exclude that conflict.

The [Linux experiment job](https://github.com/Prathampthk/StreamForge/actions/runs/42876396564/job/130040733229) reproduced both unsafe controls on clean `87e9f77`. Its compacted fixture passed in **46.816s** and independent stress fixture in **63.587s**, with verified sampling configuration and successful cleanup. [Selected CI evidence](evidence/incremental-analytics-stress-ci.json) preserves source hashes, read counts, snapshot audits and the stale-worker failure/exposure outcome. Green harness status confirms the expected counterexamples; `production_concurrency_safe` remains false.

All eight jobs in [the full CI run](https://github.com/Prathampthk/StreamForge/actions/runs/42876396564) passed on `87e9f77`. This includes the original integration and RF3/shutdown checks. The experiment intentionally reproduces unsafe concurrency, and no production readiness/performance gate is marked complete by this result.
