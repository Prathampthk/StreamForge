-- Decode the normalized JSON once per canonical identity. Keep full-string
-- equality and FINAL before non-key filtering; storage and conflict policy do
-- not change. Optional missing/null fields retain their typed defaults.
CREATE OR REPLACE VIEW events_canonical AS
SELECT event_id,payload,
 decoded.tenant_id AS tenant_id,decoded.user_id AS user_id,
 decoded.event_type AS event_type,
 parseDateTime64BestEffort(decoded.occurred_at,9,'UTC') AS occurred_at,
 decoded.amount_cents AS amount_cents,decoded.currency AS currency,
 decoded.status_code AS status_code
FROM (
 SELECT event_id,payload_min AS payload,payload_max AS last_payload,
 JSONExtract(payload_min,'Tuple(tenant_id String,user_id String,event_type String,occurred_at String,amount_cents Int64,currency String,status_code UInt16)') AS decoded
 FROM event_identity_values FINAL
)
WHERE payload=last_payload
SETTINGS max_final_threads=1,optimize_move_to_prewhere=0,optimize_move_to_prewhere_if_final=0;
INSERT INTO schema_migrations SELECT 8,now64(3) WHERE (SELECT count() FROM schema_migrations WHERE version=8)=0;
