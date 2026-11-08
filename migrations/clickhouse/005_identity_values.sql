-- Additive: start dual writes before copying retained legacy states.
CREATE TABLE IF NOT EXISTS event_identity_values (
 event_id UUID,
 payload_min SimpleAggregateFunction(min,String),
 payload_max SimpleAggregateFunction(max,String)
) ENGINE=AggregatingMergeTree
PARTITION BY cityHash64(event_id)%16 ORDER BY event_id
SETTINGS index_granularity=512;
CREATE MATERIALIZED VIEW IF NOT EXISTS event_identity_values_mv TO event_identity_values AS
SELECT event_id,minSimpleState(payload_json) AS payload_min,maxSimpleState(payload_json) AS payload_max
FROM events_raw WHERE payload_json!='' GROUP BY event_id;
CREATE TABLE IF NOT EXISTS identity_values_migration (
 revision UInt8, verified_ids UInt64, verified_at DateTime64(3,'UTC')
) ENGINE=MergeTree ORDER BY revision;
