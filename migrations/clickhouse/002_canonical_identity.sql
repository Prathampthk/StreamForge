-- New canonical projections. Legacy Phase 1 append metrics are diagnostic only.
ALTER TABLE events_raw ADD COLUMN IF NOT EXISTS payload_json String DEFAULT '';

CREATE TABLE IF NOT EXISTS event_identity_states (
 event_id UUID,
 payload_min AggregateFunction(min, String),
 payload_max AggregateFunction(max, String)
) ENGINE=AggregatingMergeTree
PARTITION BY cityHash64(event_id) % 16
ORDER BY event_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS event_identity_mv TO event_identity_states AS
SELECT event_id, minState(payload_json) AS payload_min, maxState(payload_json) AS payload_max
FROM events_raw WHERE payload_json != '' GROUP BY event_id;

CREATE VIEW IF NOT EXISTS events_canonical AS
SELECT event_id, payload,
 JSONExtractString(payload, 'tenant_id') AS tenant_id,
 JSONExtractString(payload, 'user_id') AS user_id,
 JSONExtractString(payload, 'event_type') AS event_type,
 parseDateTime64BestEffort(JSONExtractString(payload, 'occurred_at'), 9, 'UTC') AS occurred_at,
 JSONExtractInt(payload, 'amount_cents') AS amount_cents,
 JSONExtractString(payload, 'currency') AS currency,
 JSONExtractUInt(payload, 'status_code') AS status_code
FROM (SELECT event_id, minMerge(payload_min) AS payload, maxMerge(payload_max) AS last_payload
 FROM event_identity_states GROUP BY event_id HAVING payload = last_payload);

CREATE VIEW IF NOT EXISTS event_identity_conflicts AS
SELECT event_id, hex(SHA256(minMerge(payload_min))) AS min_payload_sha256,
 hex(SHA256(maxMerge(payload_max))) AS max_payload_sha256
FROM event_identity_states GROUP BY event_id
HAVING minMerge(payload_min) != maxMerge(payload_max);

CREATE TABLE IF NOT EXISTS revenue_1m_canonical (
 tenant_id String, minute DateTime('UTC'), currency String,
 purchase_count UInt64, revenue_cents Int128
) ENGINE=MergeTree ORDER BY (tenant_id, minute, currency);

CREATE MATERIALIZED VIEW IF NOT EXISTS revenue_1m_refresh
REFRESH EVERY 1 SECOND TO revenue_1m_canonical AS
SELECT tenant_id, toStartOfMinute(occurred_at) AS minute, currency,
 count() AS purchase_count, sum(toInt128(amount_cents)) AS revenue_cents
FROM events_canonical WHERE event_type='purchase'
GROUP BY tenant_id, minute, currency
SETTINGS max_execution_time=15, max_memory_usage=536870912;

CREATE TABLE IF NOT EXISTS traffic_1m_canonical (
 tenant_id String, minute DateTime('UTC'), events_per_minute UInt64,
 rpm UInt64, api_errors UInt64, error_rate Float64
) ENGINE=MergeTree ORDER BY (tenant_id, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS traffic_1m_refresh
REFRESH EVERY 1 SECOND TO traffic_1m_canonical AS
SELECT tenant_id, toStartOfMinute(occurred_at) AS minute,
 count() AS events_per_minute, countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code >= 500) AS api_errors,
 if(rpm=0, toFloat64(0), api_errors/rpm) AS error_rate
FROM events_canonical GROUP BY tenant_id, minute
SETTINGS max_execution_time=15, max_memory_usage=536870912;
