# divine-upload-server

Rust upload data plane for Divine Blossom media uploads.

## Scope

`divine-upload-server` owns:

- resumable upload session lifecycle
- direct chunk uploads to GCS
- upload finalization and canonical blob writes
- audit-log ingress from the Fastly Blossom control plane
- thumbnail and media follow-up hooks

It does not own the Blossom control plane. `divine-blossom` remains the Fastly-facing service that answers client control-plane requests such as `HEAD /upload`, validates Blossom auth, and proxies short `init` and `complete` calls to this service.

## Runtime Configuration

The service reads configuration from environment variables:

- `GCS_BUCKET`
- `CDN_BASE_URL`
- `UPLOAD_BASE_URL`
- `PORT`
- `MIGRATION_NSEC` when migration auth is required
- `TRANSCODER_URL`
- `TRANSCRIBER_URL`
- `RESUMABLE_SESSION_TTL_SECS`
- `RESUMABLE_CHUNK_SIZE`

## Development

Run tests:

```bash
cargo test
```

Run locally:

```bash
cargo run
```

The default local port is `8080`.

## Deployment

Phase 1 targets production GKE behind `https://upload.divine.video`.

The current media follow-up path still points at the existing transcoder endpoint through `TRANSCODER_URL`. GPU-backed transcoder migration is a separate phase.

`export-video-upload-hashes.py` still assumes the legacy Cloud Run log query shape and needs a follow-up update once production audit logs come from the GKE deployment.
