BEGIN;
SELECT pg_advisory_xact_lock(7301006);
CREATE TABLE IF NOT EXISTS upload_write_intents (
    intent_id TEXT PRIMARY KEY CHECK(intent_id ~ '^[0-9a-f]{32}$'),
    request_key TEXT NOT NULL UNIQUE CHECK(request_key ~ '^[0-9a-f]{64}$'),
    input JSONB NOT NULL CHECK(jsonb_typeof(input)='object' AND octet_length(input::text)<=8192),
    state TEXT NOT NULL DEFAULT 'prepared' CHECK(state='prepared'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE media_storage_attempts ADD COLUMN IF NOT EXISTS subject_kind TEXT NOT NULL DEFAULT 'producer';
ALTER TABLE media_storage_attempts ADD COLUMN IF NOT EXISTS upload_write_intent_id TEXT REFERENCES upload_write_intents(intent_id);
ALTER TABLE media_storage_attempts ALTER COLUMN processing_job_id DROP NOT NULL;
ALTER TABLE media_storage_attempts ALTER COLUMN lease_generation DROP NOT NULL;
ALTER TABLE media_storage_attempts ALTER COLUMN input_revision DROP NOT NULL;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='media_storage_subject' AND conrelid='media_storage_attempts'::regclass) THEN
  ALTER TABLE media_storage_attempts ADD CONSTRAINT media_storage_subject CHECK (
   (subject_kind='producer' AND processing_job_id IS NOT NULL AND lease_generation IS NOT NULL AND input_revision IS NOT NULL AND upload_write_intent_id IS NULL)
   OR (subject_kind='original' AND processing_job_id IS NULL AND lease_generation IS NULL AND input_revision IS NULL AND upload_write_intent_id IS NOT NULL)
  );
 END IF;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS original_write_attempt_identity ON media_storage_attempts(upload_write_intent_id) WHERE subject_kind='original';
CREATE UNIQUE INDEX IF NOT EXISTS media_storage_subject_identity ON media_storage_attempts(attempt_id,subject_kind);
ALTER TABLE media_storage_operations ADD COLUMN IF NOT EXISTS subject_kind TEXT NOT NULL DEFAULT 'producer' CHECK(subject_kind='producer');
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='producer_operation_subject' AND conrelid='media_storage_operations'::regclass) THEN
  ALTER TABLE media_storage_operations ADD CONSTRAINT producer_operation_subject FOREIGN KEY(attempt_id,subject_kind) REFERENCES media_storage_attempts(attempt_id,subject_kind);
 END IF;
END $$;
CREATE TABLE IF NOT EXISTS upload_write_operations (
    attempt_id TEXT NOT NULL REFERENCES media_storage_attempts(attempt_id),
    position INTEGER NOT NULL CHECK(position BETWEEN 0 AND 3),
    target JSONB NOT NULL CHECK(jsonb_typeof(target)='object' AND octet_length(target::text)<=4096),
    optional BOOLEAN NOT NULL,
    subject_kind TEXT NOT NULL DEFAULT 'original' CHECK(subject_kind='original'),
    FOREIGN KEY(attempt_id,subject_kind) REFERENCES media_storage_attempts(attempt_id,subject_kind),
    state TEXT NOT NULL DEFAULT 'prepared' CHECK(state='prepared'),
    PRIMARY KEY(attempt_id,position)
);
COMMIT;
