# Incremental analytics — isolated ClickHouse prototype

This experiment tests a replacement for repeated full-history metric refreshes. It is not a production migration, approved topology change, Kafka consumer, or repair of retained readiness. No PostgreSQL hot-path ledger, serving API change, retained table/view change, AWS authentication or cloud resource is involved.

The subsequent [adversarial stress stage](incremental-analytics-stress.md) demonstrates that independent writers can change a pinned generation. This is a confirmed ownership blocker, not an implemented production safety property.

Run `make incremental-analytics-test`, or:

```sh
python3 experiments/incremental-analytics/prototype.py run --out <new-summary.json>
```

Only Python's standard library and Docker are needed. The runner creates one unique, network-disabled ClickHouse container using the repository's pinned vendor image, a 2 GiB container cap and 64 MiB query budgets. It publishes no ports and never connects to the existing stack. It preserves private raw artifacts under ignored `load/results/raw/streamforge-incremental-*`, then removes only its owned container and anonymous volumes. `--out` refuses to overwrite earlier evidence.

## Algorithm and publication boundary

A single worker reads one immutable, already-normalized synthetic source batch of at most 4,096 events. It looks up only that batch's global IDs at a pinned committed generation. Full payload min/max strings preserve the current conflict invariant: equality contributes once; inequality contributes nothing, across tenant, user, currency and time changes. For every changed identity, the worker emits the difference between its old and new canonical contributions. An identical replay emits no new identity state or metric delta. A conflict subtracts the old tenant/minute/currency contribution and adds no conflicting replacement.

The worker writes an exact immutable generation plan, new identity versions, and coalesced signed minute/currency deltas. It checks the exact logical writes and publishes one contiguous commit marker last. A reader captures a committed generation and filters every subsequent read to that generation or earlier. An incomplete generation stays invisible. Recovery must finish that same plan before advancing; it refuses changed pending/committed input and generation gaps. This is an explicit publication protocol, **not** a ClickHouse multi-table transaction.

Both identity and delta replacement keys include the generation. Otherwise a background merge could discard an older committed row in favor of an unpublished row. `ReplacingMergeTree` removes repeated physical writes of the same immutable generation; metric queries still use `FINAL`, rather than waiting for merges. Latest identity reads use `argMax` restricted to selected IDs and the committed generation. Verification reads also restrict themselves to the changed IDs/buckets. Exact plan comparisons use full strings; hashes are diagnostic provenance only. See [schema](../experiments/incremental-analytics/schema.sql) and [worker/runner](../experiments/incremental-analytics/prototype.py).

The signed traffic deltas count events, API requests and server errors. Revenue deltas retain currency, purchase counts and Int128 cents. A late event or conflict corrects its original minute, even if far outside the current query window.

## Correctness gates

The fixture compares all identity min/max strings and metric rows with an independent full-rebuild oracle after every publication/recovery, plus hand-calculated final totals. It covers:

- Identical IDs across batch boundaries, duplicates within a batch, and conflicts within/across batches.
- Conflict withdrawal across tenant, user, minute, currency, amount and API status changes.
- Late events, zero revenue, separate USD/EUR buckets and an exact sum beyond Int64.
- Abrupt real Python child exits after plan, after identity write, after delta write, before commit and after commit. These are exit-code-86 failpoints, not Go consumer or OS SIGKILL tests.
- Forced merges while a generation is unpublished; a real ClickHouse restart before publication and another after publication; retry of an already committed batch after a lost response.
- Changed-input/generation-gap rejection and competing same-host writer rejection using a shared filesystem lock.

The small fixture checks every identity and full payload string. The larger scaling fixture checks all affected identities through worker verification and checks exact expected aggregate totals; it is not an exhaustive million-ID payload audit.

## Scaling method and limits

Each independent scale seeds 10,000, 100,000 or 1,000,000 synthetic identities in bounded inserts, with one historical aggregate bucket. Initialization and seed compaction are excluded from update timing. Each measured batch contains exactly 384 records: 128 identical existing replays, 128 existing IDs that become conflicts, and 128 new current-minute page views. It should change 256 identities and emit two bucket deltas: withdraw 128 historical contributions and add 128 current contributions.

The summary retains native ClickHouse `rows_read`, `bytes_read` and elapsed time for lookup and verification queries, plus a window-filtered metric read. Whole-worker wall time includes Python process launch and Docker/native-client overhead. A separate full-identity **counts-only** rebuild serves as a resource comparison under the same 64 MiB query limit; it is not the full production revenue/traffic refresh. Resource failures remain failures in the report. No speedup ratio between unlike operations is justified.

These are individual warm/shared-host measurements, not p95 distributions, a concurrent workload, 10k events/s ingestion, <5s event freshness or a fifteen-minute SLO run. UUIDs are deterministically distributed; the seed is compacted before measurement. Sparse index granules, active part count and version history still determine physical reads. A fixed set of IDs need not read a constant number of physical rows as history grows.

## Production decision gates still open

1. **Writer ownership and Kafka integration.** The host file lock is only a fixture exclusion mechanism, not distributed fencing. Current consumers process multiple partitions concurrently; global-ID corrections need serialized ownership or another proved protocol, immutable source ranges, checkpoints tied to Kafka offsets, rebalance handling, durable source retention and fail-closed recovery. Do not wire this worker into production without an explicit design/topology decision.
2. **Durability and reads.** The tests cover process death/restart with retained local disk. They do not establish host/power/disk-loss safety, replication consistency, or hardware durability of a multi-table commit protocol. Concurrent readers must pin a generation; snapshots cannot silently use an uncommitted or mixed head. Clock-based readiness, event freshness and stalled-worker health still need implementation.
3. **Storage and compaction.** Plans retain full batch/plan strings, and states retain changed generations. Hot-ID conflict churn, journal growth, part growth, backpressure and safe retention/compaction for pinned readers are unresolved. Window queries read the window's retained deltas; this is not bounded bucket compaction.
4. **Representative scale.** Repeat with many active buckets, adversarial ID changes, many unmerged parts, concurrent ingestion/queries and retained-size data. Measure source-to-query freshness, tail latency, write amplification and recovery lag. A successful synthetic run does not repair the existing retained database.
5. **Contract and rollout.** Inputs here are deterministic normalized fixtures, not the Go decoder/normalizer or poison/DLQ path. API integration, tenant/auth validation, full contract equivalence, retained backfill, dual-run reconciliation and a reviewed rollback are separate work. No migration is proposed for immediate execution.

The next local step after this experiment is a concurrency/ownership and compaction design backed by adversarial tests; only then consider an isolated Kafka-connected prototype. The current fail-closed serving path and existing performance targets remain in force.

References: [ReplacingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree), [transactional/ACID support](https://clickhouse.com/docs/guides/developer/transactional).

## First measured results

The local pruned run passed all fixture gates in 28.641s with successful cleanup, on a shared Docker VM reporting ten CPUs and 8,319,438,848 bytes of memory. It verified all twelve fixture IDs, five conflicts and four exact final metric rows, including `18446744073709551614` USD cents. This is a pre-commit run (`source_dirty=true`) with exact source hashes in [the summary](evidence/incremental-analytics-prototype.json).

| Seed identities | Lookup rows read | All update SELECT rows read | Whole worker wall time | Window rows read | Counts-only full rebuild |
|---:|---:|---:|---:|---:|---|
| 10,000 | 10,000 | 20,259 | 0.484s | 1 | Passed |
| 100,000 | 70,144 | 116,483 | 0.483s | 1 | Failed: Code 241, 64 MiB |
| 1,000,000 | 120,832 | 184,067 | 0.551s | 1 | Failed: Code 241, 64 MiB |

Only the compacted synthetic history is represented here. The measured row reduction is evidence that this query shape avoids a mandatory full scan in that fixture; it does not prove constant cost or remove active-part/version-history limits. A regression gate requires all measured worker SELECT reads at one million seed IDs to remain below half the seed size. This is a physical-read guard, not a throughput target.

Earlier attempts remain disclosed: the first large seed insert exceeded 64 MiB at 100,000 rows, so setup now uses 10,000-row chunks without raising limits. A correctness-passing but unpruned run read all one million IDs because a same-name string alias obscured the native UUID predicate; [that evidence is preserved](evidence/incremental-analytics-unpruned.json). The native UUID projection removes that conversion. Unexpected SQL errors fail the experiment; only explicit memory/timeout errors are retained as resource failures for the comparison query.

The final clean-source local run on `d192914` passed in **29.442s**, including the physical-read and explicit negative-guard checks, with successful cleanup. [Verified local evidence](evidence/incremental-analytics-verified.json) records unchanged physical reads (184,067 total worker SELECT rows at one million seed IDs), whole-worker times of 0.512/0.503/0.598s at the three scales, exact correctness/restart results and source hashes. These wall times include orchestration overhead and are not production throughput measurements.

The [Linux CI experiment job](https://github.com/Prathampthk/StreamForge/actions/runs/42763251546/job/129653656467) passed on clean `d192914` in **49.877s**, with successful cleanup, on a four-CPU Docker VM reporting 16,760,348,672 bytes of memory. [Hosted evidence](evidence/incremental-analytics-ci.json) is extracted from that job's timestamped summary. It reproduced the 20,259 / 116,483 / 184,067 total worker SELECT rows at 10k / 100k / 1m seed IDs; whole-worker wall times were 0.819 / 0.813 / 0.812s. This checks Linux reproducibility of the fixture, not the application's SLOs.

All eight jobs in [the complete CI run](https://github.com/Prathampthk/StreamForge/actions/runs/42763251546) passed on `d192914`, including the existing integration, RF3/shutdown, infrastructure and four image jobs. The retained-system performance gate remains unmet.

Subsequent experiment invocations use a disclosed [sampling overlay and independent stress lifecycles](incremental-analytics-stress.md#memory-scope-and-measured-results). Wide internal sampling logs are disabled only inside the disposable fixture, while query/part/error logs and query statistics remain. Earlier timing evidence retains its original configuration and must not be silently compared as an equivalent performance trial.
