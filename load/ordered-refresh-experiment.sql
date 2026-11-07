-- FAILED EXPERIMENT, NOT AN AUTOMATIC MIGRATION.
-- Both 256MiB and 512MiB ordered refreshes still hit read/global memory limits.
-- Follow-up after actual full-state refresh memory failures at larger history.
-- Stream identity groups in primary-key order; preserve full min/max semantics.
ALTER TABLE revenue_1m_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,currency,
 count() AS purchase_count,sum(toInt128(amount_cents)) AS revenue_cents
FROM events_canonical WHERE event_type='purchase' GROUP BY tenant_id,minute,currency
SETTINGS max_execution_time=15,max_memory_usage=536870912,max_threads=2,max_bytes_before_external_group_by=16777216,optimize_aggregation_in_order=1,max_block_size=1024,max_read_buffer_size=65536;
ALTER TABLE traffic_1m_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,count() AS events_per_minute,
 countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 if(rpm=0,toFloat64(0),api_errors/rpm) AS error_rate
FROM events_canonical GROUP BY tenant_id,minute
SETTINGS max_execution_time=15,max_memory_usage=536870912,max_threads=2,max_bytes_before_external_group_by=16777216,optimize_aggregation_in_order=1,max_block_size=1024,max_read_buffer_size=65536;
