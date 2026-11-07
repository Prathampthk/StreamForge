CREATE TABLE IF NOT EXISTS events_raw (
  event_id UUID,
  tenant_id LowCardinality(String),
  user_id String,
  session_id String,
  event_type LowCardinality(String),
  occurred_at DateTime64(3, 'UTC'),
  received_at DateTime64(3, 'UTC'),
  order_id String,
  amount_cents Int64,
  currency LowCardinality(String),
  status_code Nullable(UInt16),
  kafka_topic LowCardinality(String),
  kafka_partition UInt16,
  kafka_offset UInt64,
  sink_batch_id String,
  schema_version UInt16
) ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (tenant_id, occurred_at, event_type, event_id)
TTL occurred_at + INTERVAL 30 DAY DELETE;

CREATE TABLE IF NOT EXISTS revenue_1m (
  tenant_id LowCardinality(String),
  minute DateTime('UTC'),
  currency LowCardinality(String),
  purchase_count_state AggregateFunction(sum, UInt64),
  revenue_cents_state AggregateFunction(sum, Int64)
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (tenant_id, minute, currency);

CREATE MATERIALIZED VIEW IF NOT EXISTS revenue_1m_mv TO revenue_1m AS
SELECT
  tenant_id,
  toStartOfMinute(occurred_at) AS minute,
  currency,
  sumState(toUInt64(1)) AS purchase_count_state,
  sumState(amount_cents) AS revenue_cents_state
FROM events_raw
WHERE event_type = 'purchase'
GROUP BY tenant_id, minute, currency;

CREATE TABLE IF NOT EXISTS traffic_1m (
  tenant_id LowCardinality(String),
  minute DateTime('UTC'),
  event_count_state AggregateFunction(count),
  api_request_count_state AggregateFunction(sum, UInt64),
  api_error_count_state AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (tenant_id, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS traffic_1m_mv TO traffic_1m AS
SELECT
  tenant_id,
  toStartOfMinute(occurred_at) AS minute,
  countState() AS event_count_state,
  sumState(toUInt64(event_type = 'api_request')) AS api_request_count_state,
  sumState(toUInt64(event_type = 'api_request'
      AND ifNull(status_code, toUInt16(0)) >= 500))
    AS api_error_count_state
FROM events_raw
GROUP BY tenant_id, minute;
