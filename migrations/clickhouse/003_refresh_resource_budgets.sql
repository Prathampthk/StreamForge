-- Measured pilot: parallel aggregate refreshes exceeded the 2GiB server budget.
-- Bound refresh threads and spill aggregation to disk before memory pressure.
ALTER TABLE revenue_1m_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,currency,
 count() AS purchase_count,sum(toInt128(amount_cents)) AS revenue_cents
FROM events_canonical WHERE event_type='purchase' GROUP BY tenant_id,minute,currency
SETTINGS max_execution_time=15,max_memory_usage=268435456,max_threads=2,max_bytes_before_external_group_by=67108864;
ALTER TABLE traffic_1m_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,count() AS events_per_minute,
 countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 if(rpm=0,toFloat64(0),api_errors/rpm) AS error_rate
FROM events_canonical GROUP BY tenant_id,minute
SETTINGS max_execution_time=15,max_memory_usage=268435456,max_threads=2,max_bytes_before_external_group_by=67108864;
