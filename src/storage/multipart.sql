CREATE TABLE IF NOT EXISTS b2_multipart_intents (
 bucket TEXT NOT NULL,
 object_key TEXT NOT NULL,
 upload_id TEXT NOT NULL,
 lease_expires TIMESTAMPTZ NOT NULL,
 PRIMARY KEY (bucket, object_key, upload_id)
);
CREATE INDEX IF NOT EXISTS b2_multipart_expiry ON b2_multipart_intents(lease_expires);
