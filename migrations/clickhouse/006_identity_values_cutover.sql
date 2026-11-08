-- Refuse an unverified retained-data cutover; empty fresh installs need no copy.
SELECT throwIf(
 (SELECT count() FROM event_identity_states LIMIT 1)>0
 AND (SELECT count() FROM identity_values_migration WHERE revision=1)=0,
 'Retained identities require scripts/backfill-identity-values.py before cutover');
-- FINAL merges equal event IDs before finalization or non-key filtering.
CREATE OR REPLACE VIEW events_canonical AS
SELECT event_id, payload,
 JSONExtractString(payload, 'tenant_id') AS tenant_id,
 JSONExtractString(payload, 'user_id') AS user_id,
 JSONExtractString(payload, 'event_type') AS event_type,
 parseDateTime64BestEffort(JSONExtractString(payload, 'occurred_at'), 9, 'UTC') AS occurred_at,
 JSONExtractInt(payload, 'amount_cents') AS amount_cents,
 JSONExtractString(payload, 'currency') AS currency,
 JSONExtractUInt(payload, 'status_code') AS status_code
FROM (SELECT event_id, payload_min AS payload,
 payload_max AS last_payload
 FROM event_identity_values FINAL)
WHERE payload = last_payload
SETTINGS max_final_threads=1,optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE OR REPLACE VIEW event_identity_conflicts AS
SELECT event_id,hex(SHA256(payload_min)) AS min_payload_sha256,
 hex(SHA256(payload_max)) AS max_payload_sha256
FROM event_identity_values FINAL
WHERE payload_min != payload_max
SETTINGS max_final_threads=1,optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

-- Preserve refresh limits; smaller read blocks bound the sorted merge buffers.
ALTER TABLE revenue_1m_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,currency,
 count() AS purchase_count,sum(toInt128(amount_cents)) AS revenue_cents
FROM events_canonical WHERE event_type='purchase' GROUP BY tenant_id,minute,currency
SETTINGS max_execution_time=15,max_memory_usage=268435456,max_threads=2,max_bytes_before_external_group_by=67108864,
 max_block_size=1024,max_read_buffer_size=65536,max_final_threads=1,optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE traffic_1m_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,count() AS events_per_minute,
 countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 if(rpm=0,toFloat64(0),api_errors/rpm) AS error_rate
FROM events_canonical GROUP BY tenant_id,minute
SETTINGS max_execution_time=15,max_memory_usage=268435456,max_threads=2,max_bytes_before_external_group_by=67108864,
 max_block_size=1024,max_read_buffer_size=65536,max_final_threads=1,optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

-- Keep the legacy table and its MV definition for rollback; stop duplicate writes.
DETACH TABLE IF EXISTS event_identity_mv PERMANENTLY;

-- Record the baseline only after successful cutover. The migration runner must
-- never replay the legacy read-view definitions over a completed migration.
CREATE TABLE IF NOT EXISTS schema_migrations (
 version UInt16,applied_at DateTime64(3,'UTC')
) ENGINE=MergeTree ORDER BY version;
INSERT INTO schema_migrations SELECT toUInt16(number+1),now64(3) FROM numbers(6)
WHERE toUInt16(number+1) NOT IN (SELECT version FROM schema_migrations);
