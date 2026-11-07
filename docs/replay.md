# Quarantine and replay operations

Apply local migrations with `make migrate`, then build `make tools-build`. The consumer requires `POSTGRES_DSN` for poison auditing; valid records do not touch PostgreSQL. A database or DLQ outage retains poison and the following partition tail. Valid prefixes can commit. ClickHouse outages are retried as sink failures, never reclassified as poison.

The version-1 JSON envelope contains original raw/key bytes (base64), source topic/partition/offset, stable envelope UUID, original event UUID when parseable, first-failure/DLQ timestamps and sanitized failure code. Only schema_version, correlation_id, traceparent and produced_at headers survive. Body bytes can contain sensitive forensic data: restrict Kafka/CLI access and do not paste payloads into logs. Kafka DLQ retention is seven days; the compact PostgreSQL audit intentionally has no payload. A published row records the DLQ acknowledgment coordinates. See ADR 006 for the publish-before-audit crash gap and source-topic immutability assumption.

Run the CLI on the local Docker network (replace the example range and identifiers):

```sh
docker compose --env-file .env -f deploy/compose.yaml -f deploy/compose.bench.yaml run --rm --no-deps replayctl \
  --partition 0 --from 10 --to 11 --tenant merchant_demo --requested-by local_operator \
  --job-id 74fe4123-f837-4ab7-a9cf-348be1bb8120
```

This is an audited dry-run. It validates the whole selection before any publication. For invalid events, supply `--repair-file /work/repairs.json`: a JSON object mapping envelope UUIDs to complete repaired event objects. The file is <=1MiB. Tenant and original parseable event UUID cannot change; the key must match tenant:user. Unused repair keys are rejected. Repairs cannot silently overwrite a different payload already recorded for that logical ID: canonical conflict quarantine still applies.

Use a **new job UUID** with `--execute` after reviewing the dry-run. Job selectors (including execute mode, repair digest, tenant and actor) are immutable. Resume an interrupted execution with the same UUID and identical flags/repair bytes. Query `replayctl --status --job-id UUID` for durable status. PostgreSQL advisory locks serialize a job; a different job may deliberately repeat the same logical event. Acknowledged-but-unaudited publication can repeat after a crash, so replay relies on canonical identity semantics, not exactly-once Kafka delivery.

Each invocation is bounded to one of 12 partitions, <=1000 offset positions, <=16MiB selected envelopes (default 8MiB) and 60 seconds. Invalid/expired ranges, malformed envelopes, unsafe repairs and unavailable audit/broker services fail visibly. There is no source consumer-group offset reset and no unrestricted replay HTTP endpoint. Production identity, grants and TLS are Phase 6.
