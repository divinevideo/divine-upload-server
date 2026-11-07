BEGIN;
SELECT pg_advisory_xact_lock(7301005);
ALTER TABLE media_storage_attempts ADD COLUMN IF NOT EXISTS gate_claim_request JSONB;
ALTER TABLE media_storage_attempts ADD COLUMN IF NOT EXISTS gate_claim_outcome JSONB;
ALTER TABLE media_storage_attempts ADD COLUMN IF NOT EXISTS gate_observation JSONB;
DO $$ BEGIN
    IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='media_storage_gate_claim_shape' AND conrelid='media_storage_attempts'::regclass) THEN
        ALTER TABLE media_storage_attempts ADD CONSTRAINT media_storage_gate_claim_shape CHECK (
            (gate_claim_request IS NULL AND gate_claim_outcome IS NULL AND gate_observation IS NULL)
            OR (gate_claim_request IS NOT NULL AND jsonb_typeof(gate_claim_request)='object' AND octet_length(gate_claim_request::text)<=8192
                AND (gate_claim_outcome IS NULL OR (jsonb_typeof(gate_claim_outcome)='object' AND octet_length(gate_claim_outcome::text)<=8192))
                AND (gate_observation IS NULL OR (jsonb_typeof(gate_observation)='object' AND octet_length(gate_observation::text)<=8192)))
        );
    END IF;
END $$;
COMMIT;
