# Local analytics memory remediation

**Current status, November 9:** user-approved migrations 005/006 completed on retained data: **15,871,442 IDs**, exact full min/max strings across **3,899 verified ranges**, all 16 partitions and equal source/target cardinality. Sixteen conflicts leave **15,871,426 canonical events**. Original raw facts and legacy identity tables remain; the legacy write MV is permanently detached. Migration 007's partitioned metric caches produced that same canonical total, but intermittent server-wide memory failures still prevent sustained healthy readiness. No new concurrent benchmark or AWS operation has run. See [ADR 012](adr/012-partitioned-metric-refresh.md) and [offline readiness](offline-readiness.md).

The first merge optimization changed the canonical identity read to use `AggregatingMergeTree FINAL` followed by `finalizeAggregation` instead of a full-history hash `GROUP BY event_id`. It retains exact full-string min/max conflict detection, duplicate-safe metrics and the existing Kafka acknowledgment/offset boundaries. No PostgreSQL hot-path ledger, hash-based identity substitution, retention change or new service was introduced.

## Implemented files

- `migrations/clickhouse/004_stream_identity_merges.sql`: replace the canonical/conflict read views; keep the 15-second/256 MiB/two-thread refresh limits, with 1024-row/64 KiB read buffers. Disable PREWHERE movement so payload variants are merged before filtering.
- `tests/integration/canonical_final_test.go`: own disposable tables, unmerged retries and five conflict kinds; compare every canonical payload to the original query before/after physical merges. Check predicate pushdown cannot resurrect conflicts and Int128 revenue totals remain exact.
- `scripts/analytics-merge-experiment.py`: disposable network-isolated pinned ClickHouse, declared memory envelope, deterministic large state fixture, identical query budgets, query-log measurements and optional real scheduled-refresh validation.
- [ADR 009](adr/009-canonical-final-merge.md), this report and small evidence summaries. Historical Phase 5/6 measurements remain unchanged.

## Validation

`./scripts/integration-isolated.sh` passed in **89.678 seconds**, including the new exact-state regression, crash/rebalance/replay recovery, golden queries and stale-refresh rejection. The first fixture attempt referenced the source integration table when copying a qualified view; that fixture bug was corrected before the passing rerun. `python3 -m py_compile scripts/analytics-merge-experiment.py` and `git diff --check` passed.

The local synthetic comparison used pinned ClickHouse 25.8.4.13, a **2 GiB container**, **256 MiB per query**, two threads, 64 MiB external aggregation, 1024-row blocks and 64 KiB read buffers. Background identity merges were stopped. Strings are synthetic JSON state payloads, not a Kafka/event-validation workload. The candidate always ran second, so cache warmth and unrelated host activity prevent treating the timing ratio as a controlled speedup.

| Identities | Original query | FINAL candidate | Candidate query memory |
|---|---|---|---|
| 1,000,000 | completed, 3.260s | completed, 1.169s | 59,308,926 bytes |
| 5,000,000 | failed: 256 MiB query limit | completed, 7.069s | 72,113,912 bytes |
| 10,000,000 | failed: 256 MiB query limit | completed, 9.201s | 89,439,609 bytes |

The candidate returned exactly `N-1` canonical identities (one deliberately conflicted ID) and independently expected amount sums. Where both queries completed, all measured totals/checksums matched. Those checksums are an experiment diagnostic; exact-string equivalence is separately exercised by the regression, and production conflict semantics do not use hashes.

Initial **1 GiB** fixture runs exhausted server memory while generating data; a smaller bounded generator and explicitly declared 2 GiB envelope were used for the larger comparison. This is a disclosed environment change, not a gain at the original smaller server limit. Raw logs and intermediate failures stay under ignored `load/results/raw/analytics-merge/`.

A separate **ten-million-identity repeat using all actual migrations** also passed both scheduled refreshes in the 2 GiB container. Independently expected totals matched: **9,999,999 events; 9,000,000 API requests; 1,285,714 API errors; 999,999 purchases; 4,995,000,000 revenue cents**. Both refreshes recorded a successful run and no exception. The harness observed exact snapshots within 11.355s after starting its wait (the refreshes were already started); this is **not** Kafka-ack-to-visible freshness. In that repeat the standalone candidate query took 6.637s and 89,170,456 bytes, while the baseline again exceeded its memory budget. Timing variation reinforces the absence of a controlled speedup claim.

Small results are committed in [machine-readable evidence](evidence/analytics-merge.json). [Hosted CI](https://github.com/Prathampthk/StreamForge/actions/runs/42447375707) passed all six jobs on `72a271b04f9713f8b963004c03b5e7d65b71a61c`: unit/race/static/vulnerability checks, binary reproducibility, real-service integration/database privileges, infrastructure, four application image security gates and ingest-image reproducibility. The final documentation-only commit does not change that tested source.

## Operational boundary

Migration 004 was applied to the retained local volume after user approval, with its previous four view definitions saved privately for rollback. It did not restore healthy refreshes. Original benchmark facts, identity states, historical results and credentials remain intact. Task-owned diagnostic copies were created; their merge work is paused. No AWS operations were performed.

For a new isolated environment, apply every numbered migration in order. Existing environments must use `scripts/migrate-clickhouse.sh` and its version ledger; retained legacy identities require the verified backfill before 006. The retained installation now records versions 1–7. Do not replay old view definitions manually. A rollback after new writes requires reverse backfill because the frozen legacy identity table no longer receives events.

The ordered synthetic fixture avoided the demonstrated hash-aggregation memory failure within its measured scope; retained-data recovery did not pass. It does **not** make refresh work incremental: scan/JSON/merge cost still grows with retained identities and unmerged part count. Concurrent 15-minute ingestion/query/freshness and 1→3 scaling SLOs have not been remeasured. Phase 5's failed SLOs remain the current end-to-end evidence. The next performance gate is a new, separately labeled healthy-start concurrent benchmark; never overwrite the prior run or present this query experiment as 10k/s acceptance.

## Subsequent exact-string migration, completed locally

Migrations 005/006 introduce `event_identity_values` with exact min/max SimpleAggregateFunction strings and 512-row granules, an additive incremental MV, and a guarded cutover. The backfill script pauses the local consumer/refreshes and legacy raw/identity merges, copies bounded UUID ranges, compares every full min/max string and ID, checkpoints only verified ranges, checks total cardinality, then changes read views. It keeps original raw facts and legacy tables. The retired legacy write MV is permanently detached, preserving its definition; it can be attached again for a reviewed rollback. New writes after cutover are absent from the frozen legacy table, so rollback after new traffic requires reverse backfill before restoring the old read path.

The schema ledger prevents replaying old read-view definitions over a completed cutover. `scripts/migrate-clickhouse.sh` skips recorded versions; `scripts/migrate.sh` uses it. No marker is written for a partially verified backfill. The cutover refuses retained legacy data without the verification marker.

Actual checks:

- Full isolated integration: **PASS, 82.886s**, including exact payload/conflict/Int128/predicate behavior before/after physical merges on the new table type.
- Disposable backfill/cutover: **PASS**, 64 exact-verified identities across 16 partitions; unverified cutover refusal, dual writes, unchanged raw/legacy evidence, conflict withdrawal, stopped legacy writes, completed-invocation repeat and migration-ledger repeat.
- Python compilation, shell syntax and `git diff --check`: **PASS**.
- Retained-data 005/006 backfill/cutover: **PASS after explicit user approval**. All 15,871,442 IDs and their full min/max strings matched over 3,899 ranges. The last resume took 162.698s with one transient memory retry; this is not the total backfill duration. Source/target UUIDs are bound in the checkpoint.
- New concurrent benchmark: **NOT STARTED**; readiness must recover first.

The earlier prototype count of 827,392 identities demonstrates a read in a task-owned partial copy with competing work paused, not an exhaustive retained-data migration or full-load acceptance. Initial diagnostic copies and large INSERT SELECT attempts hit memory limits; those failures remain in the private logs. The migration-script fixture first had a container-reference typo, corrected before its passing run.

[GitHub CI for the prepared migration](https://github.com/Prathampthk/StreamForge/actions/runs/42450557963) passed all six jobs on source `076e26b381ea619f4b6763207f7aa6468a3d2b7f`, including the new disposable migration/ledger check, database grants and real-service integration. [Current preparation evidence](evidence/identity-values-preparation.json) records the completed retained-data execution separately from historical fixtures. The corrected random-UUID legacy fixture also passed at 10,000 identities; retained identity correctness is now verified, while sustained refresh recovery remains unverified.


## Partitioned refresh and restart follow-up

Migration 007 and its source-shard fail-closed health check passed isolated integration in **115.502s**, including stopping a source shard while wrappers were recent. [CI](https://github.com/Prathampthk/StreamForge/actions/runs/42664828534) passed on `caba1e8d799627c4fc798798b193e940023a6371`. The live derived traffic total matched 15,871,426 canonical events, but multiple refreshes still intermittently hit the server's 3.60 GiB memory limit. That count match is not sustained freshness acceptance.

A disposable 1 GiB probe confirmed allocator-based accounting preserved the automatic 90% server budget. A retained-volume restart preserved ledger versions 1–7 and the 4 GiB container/3.60 GiB server limits, but allocator accounting did not eliminate failures. A further cgroup-based correction trial also returned zero ready responses in twelve samples. Both experiments were reverted to the committed memory profile; legacy table merges were paused again after restart. No data, logs or private benchmark receipts were removed. The original full-load SLO failures remain current.


A subsequent isolated one-partition read-buffer comparison completed with 1,095 result rows at each size: 128 rows/8 KiB in 1.387s, 512 rows/32 KiB in 1.104s, and 1,024 rows/64 KiB in 1.093s. All refreshes were paused only for this diagnostic and restored afterward. Candidates ran in that order on a warm shared host; these are not controlled speedups or concurrency results. No buffer change was adopted from this comparison. Retained ingestion/consumer/database services remain healthy; query APIs still reject failed/stale projections.
