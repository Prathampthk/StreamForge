# ADR 011: bound internal metric-log merges on the local dev host

During the approved retained-data migration, process/merge sampling identified `system.query_metric_log` background merges using 2,332,943,428 bytes, competing with the 4 GiB ClickHouse container and aborting otherwise bounded migration work. The related `metric_log` is also a wide internal log table. This is separate from canonical identity grouping and must not be hidden as a successful migration.

Keep logging and all existing log rows. On existing tables, use reversible settings: `merge_max_block_size=128`, `vertical_merge_algorithm_min_rows_to_activate=1`, `vertical_merge_algorithm_min_bytes_to_activate=1`, and zero wide-part row/byte thresholds. The pinned version supports compact-to-wide vertical merges. Fresh tables use the same settings and 256-row index granules through `deploy/clickhouse/resources.xml`. Existing table granularity is not silently changed. No system/project table is truncated or dropped and no query-error counter is suppressed.

Tradeoff: more column/file IO and smaller blocks can cost CPU/metadata work. This is an explicit resource-envelope change, not a controlled same-configuration speedup claim. Maintain the same 4 GiB container cap and 256 MiB snapshot query budgets. Record the actual schema/configuration hashes, merge settings, retained history and failed attempts before measuring new throughput/freshness.

The revised fresh configuration passed the disposable database grants/schema check. Retained-data backfill progress and later runtime measurements are recorded separately; this ADR alone establishes no SLO result.

Reference: [ClickHouse MergeTree settings](https://clickhouse.com/docs/operations/settings/merge-tree-settings). Observed settings/part formats were checked on pinned 25.8.4.13 rather than inferred from newer defaults.
