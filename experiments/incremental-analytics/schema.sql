-- Disposable prototype only. Not a production migration.
-- Keep generation in the identity key: uncommitted newer state must never
-- merge away the last committed state. Retries of a generation are identical.
CREATE TABLE states (
 event_id UUID,generation UInt64,payload_min String,payload_max String
) ENGINE=ReplacingMergeTree ORDER BY (event_id,generation)
SETTINGS index_granularity=512;
CREATE TABLE deltas (
 tenant String,minute DateTime('UTC'),kind UInt8,currency String,generation UInt64,
 events Int128,rpm Int128,errors Int128,purchases Int128,cents Int128
) ENGINE=ReplacingMergeTree
PARTITION BY toYYYYMM(minute) ORDER BY (tenant,minute,kind,currency,generation)
SETTINGS index_granularity=512;
-- Exact immutable plans make retry regrouping/drift fail closed. Hashes in
-- commits are diagnostics; plan comparisons use full strings, not hashes.
CREATE TABLE plans (
 generation UInt64,input_json String,plan_json String
) ENGINE=MergeTree ORDER BY generation;
CREATE TABLE commits (
 generation UInt64,input_json String,input_sha256 String,plan_sha256 String
) ENGINE=MergeTree ORDER BY generation;
