-- Native last_success_time is the successful attempt start, not completion.
-- Keep the same 10-second snapshot age limit and reject missing durations.
ALTER TABLE revenue_1m_refresh MODIFY QUERY
SELECT m.tenant_id AS tenant_id,m.minute AS minute,m.currency AS currency,
 sum(m.purchase_count) AS purchase_count,sum(m.revenue_cents) AS revenue_cents
FROM identity_metrics_all AS m WHERE m.metric=2 AND m.purchase_count>0 AND (SELECT throwIf(count()!=16 OR countIf(exception!='' OR status NOT IN ('Running','Scheduled') OR ifNull(toUnixTimestamp(addMilliseconds(last_success_time,last_success_duration_ms)),0)=0 OR addMilliseconds(last_success_time,last_success_duration_ms)<now()-INTERVAL 10 SECOND)>0,'Identity metric shards incomplete, failed or stale') FROM system.view_refreshes WHERE database='streamforge' AND match(view,'^identity_metrics_[0-9]{2}_refresh$'))=0
GROUP BY m.tenant_id,m.minute,m.currency
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1;
ALTER TABLE traffic_1m_refresh MODIFY QUERY
SELECT m.tenant_id AS tenant_id,m.minute AS minute,sum(m.events_per_minute) AS events_per_minute,
 sum(m.rpm) AS rpm,sum(m.api_errors) AS api_errors,
 if(sum(m.rpm)=0,toFloat64(0),sum(m.api_errors)/sum(m.rpm)) AS error_rate
FROM identity_metrics_all AS m WHERE m.metric=1 AND (SELECT throwIf(count()!=16 OR countIf(exception!='' OR status NOT IN ('Running','Scheduled') OR ifNull(toUnixTimestamp(addMilliseconds(last_success_time,last_success_duration_ms)),0)=0 OR addMilliseconds(last_success_time,last_success_duration_ms)<now()-INTERVAL 10 SECOND)>0,'Identity metric shards incomplete, failed or stale') FROM system.view_refreshes WHERE database='streamforge' AND match(view,'^identity_metrics_[0-9]{2}_refresh$'))=0
GROUP BY m.tenant_id,m.minute
SETTINGS max_execution_time=15,max_memory_usage=67108864,max_threads=1;
INSERT INTO schema_migrations SELECT 9,now64(3) WHERE (SELECT count() FROM schema_migrations WHERE version=9)=0;
