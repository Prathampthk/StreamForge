# SLO targets; no measured performance yet

| Target | Measurement |
|---|---|
| >=10,000 accepted events/s | Kafka-acknowledged events over continuous 15 minutes |
| ingest p95 <100ms | client and server arrival-to-broker-ack/response |
| query p95 <250ms | concurrent writes; record concurrency, windows and dataset size |
| freshness p95 <5s | sampled acknowledged IDs to aggregate visibility; p50/p95/p99/max |
| 3 consumers >=2x one | same consumer-bound workload, host and settings |
| zero lost acknowledged IDs | drain and reconcile unique IDs and per-minute/currency metrics after faults |

Phase 5 will record seeds (60% page views, 20% carts, 10% purchases, 10% API requests), error distribution, versions, host resources, accepted/produced/persisted/aggregated/queried counts, raw evidence, graph data and failure logs. No load generator or load test exists in Phase 1. Faults include broker/sink outage, poison input, DLQ unavailable, rebalance, SIGTERM/SIGKILL and ambiguous insert. Local smoke duration is not a throughput benchmark.
