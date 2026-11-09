-- Larger vectorized blocks reduce per-block overhead. Keep 64 MiB, one
-- thread, fifteen-second execution limit and four-second schedules unchanged.
SYSTEM STOP VIEW revenue_1m_refresh;
SYSTEM STOP VIEW traffic_1m_refresh;
SYSTEM STOP VIEW identity_metrics_00_refresh;
SYSTEM STOP VIEW identity_metrics_01_refresh;
SYSTEM STOP VIEW identity_metrics_02_refresh;
SYSTEM STOP VIEW identity_metrics_03_refresh;
SYSTEM STOP VIEW identity_metrics_04_refresh;
SYSTEM STOP VIEW identity_metrics_05_refresh;
SYSTEM STOP VIEW identity_metrics_06_refresh;
SYSTEM STOP VIEW identity_metrics_07_refresh;
SYSTEM STOP VIEW identity_metrics_08_refresh;
SYSTEM STOP VIEW identity_metrics_09_refresh;
SYSTEM STOP VIEW identity_metrics_10_refresh;
SYSTEM STOP VIEW identity_metrics_11_refresh;
SYSTEM STOP VIEW identity_metrics_12_refresh;
SYSTEM STOP VIEW identity_metrics_13_refresh;
SYSTEM STOP VIEW identity_metrics_14_refresh;
SYSTEM STOP VIEW identity_metrics_15_refresh;
ALTER TABLE identity_metrics_00_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=0)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_01_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=1)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_02_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=2)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_03_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=3)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_04_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=4)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_05_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=5)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_06_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=6)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_07_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=7)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_08_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=8)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_09_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=9)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_10_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=10)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_11_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=11)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_12_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=12)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_13_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=13)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_14_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=14)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
ALTER TABLE identity_metrics_15_refresh MODIFY QUERY
SELECT tenant_id,toStartOfMinute(occurred_at) AS minute,
 if(grouping(event_currency)=1,toUInt8(1),toUInt8(2)) AS metric,
 if(grouping(event_currency)=1,'',event_currency) AS currency,
 count() AS events_per_minute,countIf(event_type='api_request') AS rpm,
 countIf(event_type='api_request' AND status_code>=500) AS api_errors,
 countIf(event_type='purchase') AS purchase_count,
 sumIf(toInt128(amount_cents),event_type='purchase') AS revenue_cents
FROM (SELECT tenant_id,occurred_at,currency AS event_currency,event_type,status_code,amount_cents
 FROM events_canonical WHERE cityHash64(event_id)%16=15)
GROUP BY GROUPING SETS ((tenant_id,minute),(tenant_id,minute,event_currency))
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1,max_final_threads=1,
 max_block_size=4096,max_read_buffer_size=65536,preferred_block_size_bytes=131072,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
SYSTEM START VIEW identity_metrics_00_refresh;
SYSTEM START VIEW identity_metrics_01_refresh;
SYSTEM START VIEW identity_metrics_02_refresh;
SYSTEM START VIEW identity_metrics_03_refresh;
SYSTEM START VIEW identity_metrics_04_refresh;
SYSTEM START VIEW identity_metrics_05_refresh;
SYSTEM START VIEW identity_metrics_06_refresh;
SYSTEM START VIEW identity_metrics_07_refresh;
SYSTEM START VIEW identity_metrics_08_refresh;
SYSTEM START VIEW identity_metrics_09_refresh;
SYSTEM START VIEW identity_metrics_10_refresh;
SYSTEM START VIEW identity_metrics_11_refresh;
SYSTEM START VIEW identity_metrics_12_refresh;
SYSTEM START VIEW identity_metrics_13_refresh;
SYSTEM START VIEW identity_metrics_14_refresh;
SYSTEM START VIEW identity_metrics_15_refresh;
SYSTEM START VIEW revenue_1m_refresh;
SYSTEM START VIEW traffic_1m_refresh;
INSERT INTO schema_migrations SELECT 10,now64(3) WHERE (SELECT count() FROM schema_migrations WHERE version=10)=0;
