# Interview and resume evidence

Use only the claims supported by the checked-in reports. The historical Phase 5 benchmark used Go 1.26.0; Phase 6 patched the toolchain/dependencies and did not repeat throughput measurements.

- Built a modular Go/Kafka/ClickHouse event pipeline; measured **10,335 Kafka-acknowledged events/s for 900 seconds on one Mac/Docker host**, then verified all 9,303,564 accepted IDs and normalized payload hashes after load. This was a degraded analytics baseline: query/freshness/aggregate targets failed; it is not 10k end-to-end usable analytics/s.
- Implemented contiguous partition commits, retry-safe canonical identity/conflict quarantine, durable audited poison handling and bounded dry-run replay; real local SIGKILL/rebalance/ambiguous-insert and replay tests pass. Kafka/ClickHouse/PostgreSQL are not one exactly-once transaction.
- Prepared private ECS/EC2 Terraform with scoped secret/image roles, retained encrypted data, verified TLS clients and offline safety plans; added unit/race/integration/security gates and reproducible runtime-image checks. **AWS deployment, cloud performance and live IAM/SG verification are not accomplished claims.**

Defend the tradeoffs: append-only sums overcount repeated deliveries; full min/max identity state preserves correctness across regrouped retries/conflicts but full refresh cost grows with history; durable DLQ acknowledgment and PostgreSQL audit have a duplicate-publication crash gap; successful latency percentiles exclude failed/censored observations; RF=1 preserves no broker/disk-loss guarantee. The measured 1→3 speedup was 1.91x, not 2x. The strongest portfolio evidence is explicit failure diagnosis and reproducible boundaries, not a fabricated all-green benchmark.
