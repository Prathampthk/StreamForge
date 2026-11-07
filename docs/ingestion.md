# Ingestion operations and configuration

The API uses net/http with request/connection limits and franz-go v1.22.2. Producer options are pinned and checked in tests: idempotence enabled, acks=all, keyed murmur2, LZ4, 2ms linger, 1 MiB batches, 10 record retries with jittered exponential backoff (12.5–800ms range), safe record delivery budget, and no auto topic creation. `MaxBufferedRecords` and `MaxBufferedBytes` bound Kafka record buffers (not total process heap). Heap also includes at most the configured admitted bodies, response buffers, connections and telemetry queues. Keep container limits consistent with any raised application limits.

| Environment | Default / constraint |
|---|---|
| HTTP_ADDR | 127.0.0.1:8080; Compose uses 0.0.0.0:8080 behind loopback publication |
| KAFKA_BROKERS | localhost:9092; Compose kafka:19092 |
| KAFKA_TOPIC | streamforge.events.v1 only |
| INGEST_API_KEY | required, >=24 bytes; local demo maps to one tenant |
| INGEST_TENANT | merchant_demo |
| MAX_CONNECTIONS | 256; 1..10000 |
| MAX_INFLIGHT_REQUESTS | 64; 1..MAX_CONNECTIONS |
| MAX_BATCH_EVENTS | 100; 1..1000 |
| MAX_BODY_BYTES | 1048576; 8192..8388608 (batch only) |
| PRODUCER_MAX_RECORDS | 10000; at least MAX_BATCH_EVENTS, <=100000 |
| PRODUCER_MAX_BYTES | 8388608; 8192..268435456 |
| REQUESTS_PER_SECOND / REQUEST_BURST | 2000 / 2000; per-instance authenticated request token bucket, not events/tenant distributed quota |
| PUBLISH_TIMEOUT | 5s; 1s..60s HTTP wait/safe producer retry budget |
| REQUEST_TIMEOUT | 8s; greater than publish, <=120s |
| SHUTDOWN_TIMEOUT | 15s; greater than request, <=300s; Compose SIGKILL grace=20s must also be raised if this changes |
| MAX_LATENESS | 168h maximum; must be >0 and <=Kafka retention (7 days) |
| FUTURE_SKEW | 5m; 0..1h |
| TRACE_SAMPLE_RATIO | 0.1; 0..1, parent-based sampling |
| OTEL_EXPORTER_OTLP_ENDPOINT | empty disables export; optional http(s) collector base URL |

Health and metrics routes are unauthenticated on the local listener; do not expose them publicly. Liveness never calls Kafka. Readiness queries metadata with a 1s deadline and checks 12 partition leaders; it is an advisory check, not a guarantee of the next publish. Only one readiness call runs at once. HTTP read header timeout=2s, read timeout=request timeout, write timeout=request+1s, keepalive idle timeout=30s, maximum header bytes=8KiB (net/http may reserve extra parser space). netutil.LimitListener bounds accepted connections; excess connections wait in the OS backlog. Request-limit exhaustion returns 503 immediately after admission checking, rather than creating a local backlog. Rate-limit exhaustion returns 429. Both include Retry-After=1.

Bodies must be UTF-8 application/json without compression. Duplicate JSON object keys, unknown/case-mismatched fields, extra top-level values and nesting >16 are rejected. Single events cap total body at 8192 bytes. A batch is validated completely before publication; the encoded size of each array element also caps at 8192 bytes. Identical IDs inside or across batches are not deduplicated. Original RFC3339 occurred_at text is retained in Kafka; business bucketing must later parse its instant into UTC. Timestamps permit up to nine fractional digits; UUIDs are normalized to lowercase. See the pinned currency snapshot and event schema.

SIGTERM marks drain, stops new requests and waits for active HTTP handlers. After their bounded waits, producer close settles remaining promises before tracing shutdown. Client disconnects and errors are unknown delivery outcomes. Do not generate new IDs on retries. API responses cannot resolve logical duplicate conflicts; Phase 3 must establish canonical identity before final aggregate guarantees.

Metrics use finite outcomes only. Kafka acknowledged/failed counters are per-record promise outcomes; failed does not imply absent from broker. HTTP accepted counter is per-request and does not prove a disconnected client received the response. Request histograms include HTTP validation and broker wait. Producer histogram starts at enqueue. See the Phase 5 measurement report for actual client/server observations and limitations. No event/tenant/user/trace IDs are metric labels. HTTP and Kafka spans propagate traceparent; optional exporter queues are bounded. Valid ingestion still performs no PostgreSQL lookup. Consumer/query/DLQ/replay are separate binaries/modules; see their operations documents.

Phase 5 returns one broker-callback acknowledgment timestamp per successful event. The producer record limit increased to 10,000 to accommodate up to 64 concurrent 100-event requests; the independent byte cap remains 8MiB. Producer failures expose bounded class counters. No response based solely on enqueueing is accepted.
