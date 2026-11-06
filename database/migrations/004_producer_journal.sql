BEGIN;
SELECT pg_advisory_xact_lock(7301004);
CREATE TABLE IF NOT EXISTS media_storage_attempts (
    attempt_id TEXT PRIMARY KEY CHECK(attempt_id ~ '^[0-9a-f]{32}$'),
    processing_job_id TEXT NOT NULL REFERENCES media_processing_jobs(processing_job_id),
    lease_generation BIGINT NOT NULL CHECK(lease_generation>0),
    input_revision BIGINT NOT NULL CHECK(input_revision>0),
    input JSONB NOT NULL CHECK(jsonb_typeof(input)='object'),
    phase TEXT NOT NULL DEFAULT 'allocated' CHECK(phase IN ('allocated','planned')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(processing_job_id,lease_generation,input_revision)
);
CREATE TABLE IF NOT EXISTS media_storage_operations (
    attempt_id TEXT NOT NULL REFERENCES media_storage_attempts(attempt_id),
    position INTEGER NOT NULL CHECK(position BETWEEN 0 AND 6),
    filename TEXT NOT NULL,
    target JSONB NOT NULL CHECK(jsonb_typeof(target)='object'),
    sha256 TEXT NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
    size BIGINT NOT NULL CHECK(size>0 AND size<=33554432),
    content_type TEXT NOT NULL,
    metadata JSONB NOT NULL CHECK(jsonb_typeof(metadata)='object'),
    state TEXT NOT NULL DEFAULT 'prepared' CHECK(state='prepared'),
    PRIMARY KEY(attempt_id,filename),
    UNIQUE(attempt_id,position)
);
COMMIT;
