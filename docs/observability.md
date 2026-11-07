# Optional local observability

Run `make observe-up` after infrastructure/migrations. This combined overlay builds/starts the three Go services with OTLP export enabled and starts Prometheus, Grafana, OTel Collector and Jaeger. It also probes their actual HTTP health; Docker's running state alone does not establish Collector/Jaeger readiness.

- Grafana: http://localhost:3000 — admin / development password in .env.example (or your GRAFANA_ADMIN_PASSWORD).
- Prometheus: http://localhost:9090 — targets/queries.
- Jaeger: http://localhost:16686 — traces in temporary in-memory storage.
- Collector health: http://localhost:13133/ . OTLP is internal-only on port 4318.

All published ports bind loopback. No cloud account or external telemetry endpoint is needed. Pins: Prometheus v3.5.0, Grafana 12.1.0, Collector contrib 0.133.0, Jaeger 2.9.0. These are explicit reproducibility pins, not claims of newest security releases; review before external deployment. Additional memory caps total 960MiB plus the query service's 256MiB, on top of previous infrastructure/consumers. Build/test processes require additional headroom. No remote exposure, TLS, multi-user Grafana auth or production trace durability is claimed.

Prometheus scrapes ingestion/query at fixed internal addresses and resolves consumer DNS A records to discover replicas every five seconds. App metrics have finite outcomes and fixed operations/kinds; consumer partition labels are bounded by 12. Instance labels distinguish replicas. No event ID, arbitrary tenant/user/trace ID, SQL string, error text or request URL becomes a metric label. The Go race/cardinality test exercises rejected/saturated/concurrent queries and checks label names. Prometheus target checks verify all three Go components are scraped. Consumer lag is an observed sample, not a constantly audited group lag; snapshot age is a refresh-age signal, not event freshness.

Provisioned Grafana dashboard UID streamforge-pipeline has 12 panels: acknowledged Kafka event rate, ingestion outcomes and latency, consumer lag/insert time/retries/ownership, query outcomes/latency, snapshot age/health, and producer unsettled records. Prometheus and Jaeger datasource UIDs are stable. Dashboard files overwrite UI edits on provisioning; change source-controlled JSON for persistent edits. Empty/NaN rate panels before sufficient traffic are not benchmark results.

OTel spans cover HTTP ingress, Kafka publication, consumer partition batches, concrete ClickHouse inserts, query HTTP and ClickHouse reads/refresh-health requests. Producer headers propagate W3C traceparent. Consumer batches inherit the first record's context; no per-record span fanout or claim that every record has a distinct trace. Sampling is parent-based with configurable root ratio; synthetic smoke sets a sampled traceparent for deterministic verification. No raw payloads, API keys or SQL parameter values are span attributes.

Collector uses a 128MiB memory limiter, batches capped at 256 spans, a 128-request export queue and 1s..5s retry interval with a 10s elapsed budget. The first attempted config had max_elapsed_time smaller than its implicit max_interval; validation rejected it. Explicit intervals fix that, and observe-health now verifies the running HTTP endpoint. Jaeger stores at most 2000 recent traces in memory, capped by a 256MiB container. Traces are best effort, evicted and lost on restart. Prometheus/Grafana named volumes persist metrics/config; Prometheus retains seven days. None of this changes Kafka acknowledgment or sink correctness. Monitor memory during later load tests and tune sampling/retention honestly.

```sh
make query-smoke   # prints an actual accepted manifest path and trace ID
make observe-smoke MANIFEST=/absolute/path/streamforge-accepted-TRACEID.json
```

The check queries Prometheus target health, Grafana's dashboard/datasource APIs and Jaeger's trace API; it requires HTTP/Kafka/consumer/ClickHouse insert/query operations in the real exported trace. This verifies provisioning and export, not a load SLO or visual screenshot QA. The standalone Collector/Jaeger health probes are separate from Docker health status because those images lack a general HTTP probe client.

`make observe-down` stops the four optional tools; it does not remove volumes or rewrite the already running apps' exporter settings. Use ordinary app `*-up` targets to restore environment-configured (normally disabled) export, or `make observe-up` to keep the full overlay wiring. Running an individual non-observability Compose target while tools are up may print harmless orphan-service warnings; never add --remove-orphans to an up/test command to suppress them. `make down` intentionally stops the whole project and retains named volumes.

Phase 5 adds finite-class producer failure counters (`buffer_full`, `timeout_or_cancel`, `broker_error`) and consumer DLQ completion/failure counters. DLQ completion means the Kafka acknowledgment and PostgreSQL audit both succeeded. The benchmark stores per-ID callback timestamps and polls uniquely isolated aggregate probes; its freshness results are separate from snapshot age. Resource samples, dropped work, unknown outcomes, query failures and probe censoring are retained even when successful latency percentiles look healthy. Benchmark endpoints disable root trace sampling explicitly to bound overhead; they share the normal broker/consumer/database pipeline.
