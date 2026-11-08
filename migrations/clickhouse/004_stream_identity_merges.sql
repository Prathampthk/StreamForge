-- Read-side merge of the existing exact min/max states. No data rewrite/backfill.
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
FROM (SELECT event_id, finalizeAggregation(payload_min) AS payload,
 finalizeAggregation(payload_max) AS last_payload
 FROM event_identity_states FINAL)
WHERE payload = last_payload
SETTINGS optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE OR REPLACE VIEW event_identity_conflicts AS
SELECT event_id,hex(SHA256(finalizeAggregation(payload_min))) AS min_payload_sha256,
 hex(SHA256(finalizeAggregation(payload_max))) AS max_payload_sha256
FROM event_identity_states FINAL
WHERE finalizeAggregation(payload_min) != finalizeAggregation(payload_max)
SETTINGS optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

-- Preserve refresh limits; smaller read blocks bound the sorted merge buffers.
ALTER TABLE revenue_1m_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,currency,
 count() AS purchase_count,sum(toInt128(amount_cents)) AS revenue_cents
FROM events_canonical WHERE event_type='purchase' GROUP BY tenant_id,minute,currency
SETTINGS max_execution_time=15,max_memory_usage=268435456,max_threads=2,max_bytes_before_external_group_by=67108864,
 max_block_size=1024,max_read_buffer_size=65536,optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE traffic_1m_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,count() AS events_per_minute,
 countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 if(rpm=0,toFloat64(0),api_errors/rpm) AS error_rate
FROM events_canonical GROUP BY tenant_id,minute
SETTINGS max_execution_time=15,max_memory_usage=268435456,max_threads=2,max_bytes_before_external_group_by=67108864,
 max_block_size=1024,max_read_buffer_size=65536,optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
