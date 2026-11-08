BEGIN;
SELECT pg_advisory_xact_lock(7301001);
ALTER TABLE upload_processing_intents
    ADD COLUMN IF NOT EXISTS sync_thumbnail_generation TEXT,
    ADD COLUMN IF NOT EXISTS sync_thumbnail_required BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS sync_thumbnail_receipt JSONB;
COMMIT;
