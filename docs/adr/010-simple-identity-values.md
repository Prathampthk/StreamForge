# ADR 010: exact identity values with bounded retained-data migration

## Retained-data finding

Applying migration 004 to the retained local data exposed overlapping random-UUID parts and memory failures not represented by the ordered-UUID synthetic fixture. Both query/global memory failures occurred. The earlier ten-million results remain true only for that recorded layout; they do not prove recovery of this retained environment. No new healthy benchmark has started.

## Decision and implementation slice

Use `SimpleAggregateFunction(min, String)` and `SimpleAggregateFunction(max, String)` in a new `event_identity_values` AggregatingMergeTree. These are exact full payload strings, not payload hashes or a latest-wins identity policy. Same UUIDs retain min/max string conflict exclusion before and after merges. Use immutable UUID-hash partitions as before and 512-row index granules. Bound FINAL threads and read blocks explicitly. Avoid opaque AggregateFunction string-state read buffers in the served path.

Files: migrations 005 (new table/dual-write MV) and 006 (verified cutover), a local resumable backfill script, updates to sink readiness, database grants, exact-state regression and the experiment harness, plus a schema ledger to prevent replaying retired read paths. Update measured reports only with actual outcomes.

For retained data, create the new incremental MV first. Pause the local consumer, refreshes and legacy raw/identity background merges during bounded backfill; Kafka retains accepted backlog. For every UUID range compare exact event IDs and full min/max strings against the legacy states before checkpointing. Never skip errors or mark unverified ranges complete. Repeated copying is idempotent. Keep old tables and raw facts. Only after all partitions and total identity count match, apply 006 and restore the consumer/refreshes. Freeze legacy-state merges after cutover; its MV is detached, not dropped, for rollback. No benchmark interval includes migration/copy/paused work.

Fresh installations apply all migrations before accepting traffic and have empty source tables. Migration 006 refuses a nonempty legacy source without a verified migration marker, so ordinary migration commands cannot accidentally expose an incomplete backfill. Existing AWS disks still require a separately reviewed migration; no AWS operations are authorized here.

Risks: dual-write/copy briefly adds IO and disk use; smaller granules add index overhead. Simple exact-string min/max still scans retained identities for full snapshots and cannot establish <5s freshness without concurrent measurement. Background compaction/global host memory remain finite. This is a local correction, not HA, disk-loss protection or an exactly-once claim.

## Gates

- Exact canonical payload/conflict/Int128 and predicate safety before/after physical merges on SimpleAggregateFunction columns.
- Every retained identity's full min/max values verified in bounded ranges, plus total ID cardinality equality; preserve legacy facts and rollback definitions.
- New table's merge/read behavior measured on retained random-UUID layout and fresh integration tests.
- Fresh benchmark only after readiness succeeds, retaining all errors and original benchmark results.

Local checks passed: full integration 82.886s and disposable exact backfill/cutover/repeat/ledger checks. After explicit user approval, retained-data execution completed on November 9: 15,871,442 IDs and full min/max strings verified over 3,899 ranges, with equal source/target cardinality. Original raw and legacy tables remain. This proves the identity migration, not refresh performance. Rollback after subsequent writes requires reverse backfill from the new values, not merely reattaching the frozen legacy MV.

Hosted CI passed all six jobs on `076e26b`; prepared code is reviewable at that commit. No hosted result constitutes a retained-database cutover or concurrent benchmark.
