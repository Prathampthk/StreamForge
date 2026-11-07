BEGIN;
CREATE TABLE IF NOT EXISTS tenants (
  tenant_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS streams (
  stream_id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id),
  name TEXT NOT NULL,
  kafka_topic TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version > 0),
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS api_keys (
  key_id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id),
  key_hash TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS replay_jobs (
  job_id UUID PRIMARY KEY,
  tenant_id TEXT REFERENCES tenants(tenant_id),
  source_topic TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN
    ('pending','running','paused','completed','failed','cancelled')),
  selector JSONB NOT NULL,
  requested_by TEXT NOT NULL,
  records_scanned BIGINT NOT NULL DEFAULT 0,
  records_republished BIGINT NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS replay_audit (
  audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES replay_jobs(job_id),
  source_topic TEXT NOT NULL,
  source_partition INTEGER NOT NULL,
  source_offset BIGINT NOT NULL,
  event_id UUID,
  outcome TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (job_id, source_topic, source_partition, source_offset)
);

CREATE INDEX IF NOT EXISTS streams_tenant_idx ON streams(tenant_id);
CREATE INDEX IF NOT EXISTS api_keys_tenant_idx ON api_keys(tenant_id);
CREATE INDEX IF NOT EXISTS replay_jobs_status_idx ON replay_jobs(status, created_at);

COMMIT;
