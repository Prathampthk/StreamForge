-- Exact hash-partition projections. All variants of an ID share a shard.
-- Pause wrappers until all shard metadata can be validated; no facts are altered.
SYSTEM STOP VIEW revenue_1m_refresh;
SYSTEM STOP VIEW traffic_1m_refresh;
CREATE TABLE IF NOT EXISTS identity_metrics_00 (
 tenant_id String,minute DateTime('UTC'),metric UInt8,currency String,
 events_per_minute UInt64,rpm UInt64,api_errors UInt64,purchase_count UInt64,revenue_cents Int128
) ENGINE=MergeTree ORDER BY (metric,tenant_id,minute,currency);
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_00_refresh
REFRESH EVERY 4 SECOND TO identity_metrics_00 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_01 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_01_refresh
REFRESH EVERY 4 SECOND OFFSET 1 SECOND TO identity_metrics_01 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_02 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_02_refresh
REFRESH EVERY 4 SECOND OFFSET 2 SECOND TO identity_metrics_02 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_03 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_03_refresh
REFRESH EVERY 4 SECOND OFFSET 3 SECOND TO identity_metrics_03 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_04 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_04_refresh
REFRESH EVERY 4 SECOND TO identity_metrics_04 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_05 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_05_refresh
REFRESH EVERY 4 SECOND OFFSET 1 SECOND TO identity_metrics_05 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_06 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_06_refresh
REFRESH EVERY 4 SECOND OFFSET 2 SECOND TO identity_metrics_06 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_07 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_07_refresh
REFRESH EVERY 4 SECOND OFFSET 3 SECOND TO identity_metrics_07 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_08 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_08_refresh
REFRESH EVERY 4 SECOND TO identity_metrics_08 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_09 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_09_refresh
REFRESH EVERY 4 SECOND OFFSET 1 SECOND TO identity_metrics_09 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_10 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_10_refresh
REFRESH EVERY 4 SECOND OFFSET 2 SECOND TO identity_metrics_10 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_11 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_11_refresh
REFRESH EVERY 4 SECOND OFFSET 3 SECOND TO identity_metrics_11 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_12 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_12_refresh
REFRESH EVERY 4 SECOND TO identity_metrics_12 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_13 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_13_refresh
REFRESH EVERY 4 SECOND OFFSET 1 SECOND TO identity_metrics_13 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_14 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_14_refresh
REFRESH EVERY 4 SECOND OFFSET 2 SECOND TO identity_metrics_14 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE TABLE IF NOT EXISTS identity_metrics_15 AS identity_metrics_00;
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_metrics_15_refresh
REFRESH EVERY 4 SECOND OFFSET 3 SECOND TO identity_metrics_15 AS
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
 max_block_size=128,max_read_buffer_size=8192,preferred_block_size_bytes=16384,
 optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;

CREATE OR REPLACE VIEW identity_metrics_all AS
SELECT * FROM identity_metrics_00
UNION ALL
SELECT * FROM identity_metrics_01
UNION ALL
SELECT * FROM identity_metrics_02
UNION ALL
SELECT * FROM identity_metrics_03
UNION ALL
SELECT * FROM identity_metrics_04
UNION ALL
SELECT * FROM identity_metrics_05
UNION ALL
SELECT * FROM identity_metrics_06
UNION ALL
SELECT * FROM identity_metrics_07
UNION ALL
SELECT * FROM identity_metrics_08
UNION ALL
SELECT * FROM identity_metrics_09
UNION ALL
SELECT * FROM identity_metrics_10
UNION ALL
SELECT * FROM identity_metrics_11
UNION ALL
SELECT * FROM identity_metrics_12
UNION ALL
SELECT * FROM identity_metrics_13
UNION ALL
SELECT * FROM identity_metrics_14
UNION ALL
SELECT * FROM identity_metrics_15;
ALTER TABLE revenue_1m_refresh MODIFY QUERY
SELECT m.tenant_id AS tenant_id,m.minute AS minute,m.currency AS currency,
 sum(m.purchase_count) AS purchase_count,sum(m.revenue_cents) AS revenue_cents
FROM identity_metrics_all AS m WHERE m.metric=2 AND m.purchase_count>0 AND (SELECT throwIf(count()!=16 OR countIf(exception!='' OR status NOT IN ('Running','Scheduled') OR ifNull(toUnixTimestamp(last_success_time),0)=0 OR last_success_time<now()-INTERVAL 10 SECOND)>0,'Identity metric shards incomplete, failed or stale') FROM system.view_refreshes WHERE database='streamforge' AND match(view,'^identity_metrics_[0-9]{2}_refresh$'))=0
GROUP BY m.tenant_id,m.minute,m.currency
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1;
ALTER TABLE traffic_1m_refresh MODIFY QUERY
SELECT m.tenant_id AS tenant_id,m.minute AS minute,sum(m.events_per_minute) AS events_per_minute,
 sum(m.rpm) AS rpm,sum(m.api_errors) AS api_errors,
 if(sum(m.rpm)=0,toFloat64(0),sum(m.api_errors)/sum(m.rpm)) AS error_rate
FROM identity_metrics_all AS m WHERE m.metric=1 AND (SELECT throwIf(count()!=16 OR countIf(exception!='' OR status NOT IN ('Running','Scheduled') OR ifNull(toUnixTimestamp(last_success_time),0)=0 OR last_success_time<now()-INTERVAL 10 SECOND)>0,'Identity metric shards incomplete, failed or stale') FROM system.view_refreshes WHERE database='streamforge' AND match(view,'^identity_metrics_[0-9]{2}_refresh$'))=0
GROUP BY m.tenant_id,m.minute
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1;
SYSTEM START VIEW revenue_1m_refresh;
SYSTEM START VIEW traffic_1m_refresh;
INSERT INTO schema_migrations SELECT 7,now64(3) WHERE (SELECT count() FROM schema_migrations WHERE version=7)=0;
