# Work before AWS

As of November 9, 2026. AWS setup is not required for any local task below. Existing performance targets remain unchanged. Historical failed benchmarks remain available.

| Task | Current evidence / next gate |
|---|---|
| Exact retained identity migration | Complete: 15,871,442 IDs and full min/max strings verified; 3,899 ranges; all 16 partitions; 16 conflicts excluded. Raw and legacy data preserved. |
| Bound business refresh work | Migration 007 applied; sixteen partition projections match the 15,871,426 canonical-event total. Sustained readiness still intermittently fails under global memory pressure. |
| Query health and correctness | Isolated integration passed in 115.502s, including stopped-source-shard rejection, conflict withdrawal, currency/Int128 totals, replay and crash recovery. Hosted CI passed on caba1e8; all six jobs also passed on 3298ede, including the new recovery rehearsal. |
| Retained restart recovery | Ledger versions 1–7 and data survived restart. Allocator-accounting experiment did not recover sustained readiness and was reverted. Legacy table merges paused again. |
| Native backup/restore | Passed in 57.286s: source volumes removed; exact ClickHouse raw/identity/conflict/canonical/metric/ledger recovery, PostgreSQL metadata/owner/reader ACL recovery. Small fixture only; see the [recovery guide](backup-restore.md). |
| Fresh 15-minute benchmark | Pending healthy analytics. Run with ingestion, concurrent queries and freshness probes; retain failures, accepted receipt audit and sleep checks. Do not bypass readiness to claim success. |
| Consumer 1→3 scaling | Repeat after benchmark/audit, separately from steady load. Historical 1.91x fails the 2x target. |
| Replicated Kafka failure drill | Still required: isolated three-broker RF3/minISR2 acknowledgment and broker-loss checks. Current RF1 base does not establish HA or replica-loss durability. |
| Security and deployment artifacts | Existing offline infrastructure/mock-plan, grants, pinned-image and CI checks are available. Revalidate final source and artifacts before publication. |

The next engineering blocker is refresh stability on retained history, not AWS credentials. Partitioning bounds each aggregation, but repeated full-history scans and shared server memory remain a scaling limit. Query readiness must stay fail-closed while it is investigated.

AWS-dependent work comes later: account/profile and region, live permissions/network validation, current regional cost review, artifact publication and a separately authorized deployment. No account provisioning, authenticated AWS action, image publication or spend has occurred. Follow [account setup](aws/account-setup.md) in parallel; never commit access keys or database secrets.
