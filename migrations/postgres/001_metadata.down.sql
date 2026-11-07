-- DESTRUCTIVE: manual use only; removes metadata and audit history.
BEGIN;
DROP TABLE replay_audit, replay_jobs, api_keys, streams, tenants;
COMMIT;
