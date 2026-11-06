BEGIN;
SELECT pg_advisory_xact_lock(7301003);
CREATE TABLE IF NOT EXISTS media_processing_jobs (
    processing_job_id TEXT PRIMARY KEY,
    input JSONB NOT NULL,
    input_revision BIGINT NOT NULL DEFAULT 0 CHECK(input_revision>=0),
    lease_generation BIGINT NOT NULL DEFAULT 0 CHECK(lease_generation>=0),
    attempts_in_revision INTEGER NOT NULL DEFAULT 0 CHECK(attempts_in_revision>=0),
    lease_until TIMESTAMPTZ,
    attempt_deadline TIMESTAMPTZ,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','running','completed','blocked')),
    failure_reason TEXT,
    manifest JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS media_processing_jobs_pending ON media_processing_jobs(created_at)
    WHERE state IN ('pending','running');
CREATE INDEX IF NOT EXISTS media_processing_jobs_attempts ON media_processing_jobs(attempts_in_revision,lease_until)
    WHERE state IN ('pending','running');
CREATE TABLE IF NOT EXISTS media_finalization_inbox (
    job_id TEXT PRIMARY KEY,
    processing_job_id TEXT NOT NULL REFERENCES media_processing_jobs(processing_job_id),
    payload JSONB NOT NULL,
    registration_state TEXT NOT NULL DEFAULT 'pending' CHECK(registration_state IN ('pending','completed','blocked')),
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS media_processing_destinations (
    processing_job_id TEXT NOT NULL REFERENCES media_processing_jobs(processing_job_id),
    identity_key TEXT NOT NULL,
    object_ref JSONB NOT NULL,
    PRIMARY KEY(processing_job_id,identity_key)
);
COMMIT;
