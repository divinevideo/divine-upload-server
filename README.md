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

In the approved Divine resumable flow:

- `https://media.divine.video` is the client-facing control plane
- `https://upload.divine.video` is the opaque resumable session data plane
- `uploadUrl` values returned to clients are server-issued session URLs and must be treated as opaque

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
- `RESUMABLE_MAX_REQUEST_BODY_SIZE`

`RESUMABLE_CHUNK_SIZE` is capped to `RESUMABLE_MAX_REQUEST_BODY_SIZE` before it is advertised in `/upload/init`, so the published chunk contract cannot exceed the actual request-body limit on `upload.divine.video`. The default route limit is `1048576` bytes, matching the current production nginx ingress behavior. `UPLOAD_ROUTE_MAX_BODY_SIZE` is also accepted as a compatibility alias.

## Development

Run tests:

```bash
cargo test
python3 -m unittest discover -s tests -p 'test_*.py'
```

Run locally:

```bash
cargo run
```

The default local port is `8080`.

## Deployment

Phase 1 targets production GKE behind `https://upload.divine.video`.

The current media follow-up path still points at the existing transcoder endpoint through `TRANSCODER_URL`. GPU-backed transcoder migration is a separate phase.

The repository CI workflow builds and tests on pull requests, then publishes `divine-upload-server` to `us-central1-docker.pkg.dev/dv-platform-prod/containers-production` on pushes to `main`.

`export-video-upload-hashes.py` queries both the legacy Cloud Run audit logs and the GKE `k8s_container` audit logs so historic exports continue to work across the migration.

---

Part of [Divine](https://divine.video) — your playground for human creativity · [Brand guidelines](https://github.com/divinevideo/brand-guidelines)
