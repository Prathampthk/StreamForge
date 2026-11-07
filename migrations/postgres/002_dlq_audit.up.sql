BEGIN;
CREATE TABLE IF NOT EXISTS dlq_audit (
 source_topic TEXT NOT NULL,
 source_partition INTEGER NOT NULL CHECK(source_partition>=0),
 source_offset BIGINT NOT NULL CHECK(source_offset>=0),
 envelope_id UUID NOT NULL UNIQUE,
 event_id UUID,
 failure_code TEXT NOT NULL CHECK(failure_code='invalid_event'),
 first_failure_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 published_at TIMESTAMPTZ,
 dlq_partition INTEGER,
 dlq_offset BIGINT,
 PRIMARY KEY(source_topic,source_partition,source_offset),
 CHECK((published_at IS NULL AND dlq_partition IS NULL AND dlq_offset IS NULL) OR (published_at IS NOT NULL AND dlq_partition>=0 AND dlq_offset>=0))
);
CREATE INDEX IF NOT EXISTS dlq_audit_pending_idx ON dlq_audit(first_failure_at) WHERE published_at IS NULL;
COMMIT;
