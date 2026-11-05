BEGIN;
SELECT pg_advisory_xact_lock(7301001);
CREATE TABLE IF NOT EXISTS upload_sessions (
    upload_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL,
    state JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS upload_chunks (
    temp_key TEXT NOT NULL,
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    sha256 TEXT NOT NULL,
    objects JSONB NOT NULL,
    PRIMARY KEY (temp_key, start_offset)
);
CREATE TABLE IF NOT EXISTS upload_final_objects (
    temp_key TEXT PRIMARY KEY,
    objects JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS upload_session_tombstones (upload_id TEXT PRIMARY KEY);
ALTER TABLE upload_sessions ADD COLUMN IF NOT EXISTS cleanup_checked_at TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0);
COMMIT;
