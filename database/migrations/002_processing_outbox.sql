BEGIN;
SELECT pg_advisory_xact_lock(7301001);
CREATE TABLE IF NOT EXISTS upload_processing_intents (
    sha256 TEXT NOT NULL,
    owner TEXT NOT NULL,
    size BIGINT NOT NULL CHECK(size>=0),
    content_type TEXT NOT NULL,
    write_mode TEXT NOT NULL,
    gcs_bucket TEXT NOT NULL,
    b2_bucket TEXT,
    completed BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(sha256,owner)
);
CREATE INDEX IF NOT EXISTS upload_processing_intents_pending
    ON upload_processing_intents(updated_at) WHERE NOT completed;
CREATE TABLE IF NOT EXISTS upload_processing_outbox (
    job_id TEXT PRIMARY KEY,
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_token TEXT,
    delivered_at TIMESTAMPTZ,
    finalized_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS upload_processing_outbox_pending
    ON upload_processing_outbox(next_attempt_at) WHERE delivered_at IS NULL;
CREATE TABLE IF NOT EXISTS upload_processing_session_receipts (
    upload_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES upload_processing_outbox(job_id)
);

ALTER TABLE upload_processing_outbox ADD COLUMN IF NOT EXISTS finalized_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();
CREATE INDEX IF NOT EXISTS upload_processing_outbox_original_finalized
    ON upload_processing_outbox ((payload->>'sha256'), finalized_at DESC, job_id DESC);
COMMIT;
