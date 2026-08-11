# Divine Upload Server

`divine-upload-server` is the Rust upload data plane for Divine Blossom media. It receives blob bytes, stores them content-addressed in Google Cloud Storage, and fires the media follow-up hooks (thumbnails, dimension probes, HLS transcoding, transcription) that turn a raw upload into a playable asset. It handles both single-request uploads and resumable, chunked sessions.

It is the data plane only. The Fastly-facing Blossom control plane (`divine-blossom`) answers client control-plane requests such as `HEAD /upload`, validates Blossom auth, and proxies short `init` and `complete` calls to this service. In the Divine resumable flow:

- `https://media.divine.video` is the client-facing control plane and CDN origin.
- `https://upload.divine.video` is the opaque resumable session data plane served by this service.
- `uploadUrl` values returned to clients are server-issued session URLs and must be treated as opaque.

## Features

- **Direct Blossom uploads** — `PUT /upload` streams a blob to GCS, hashes it, and returns the canonical Blossom descriptor.
- **Resumable, chunked uploads** — an `init` / chunk / `complete` session protocol backed by GCS resumable uploads, with offset queries and resume support.
- **Blossom Nostr auth** — verifies `Authorization: Nostr <base64 event>` (kind `24242`) including event id, expiration, action tag, and Schnorr signature.
- **Content-addressed storage** — finalized blobs are stored under their `sha256`, so identical content is deduplicated.
- **Media follow-up hooks** — video thumbnail extraction, dimension probing, HLS transcoding, and transcription are triggered as non-blocking work; failures never fail the upload.
- **BUD-04 mirror / migration** — `POST /migrate` fetches a blob from an allowlisted Blossom or CDN host and re-stores it.
- **Audit-log ingress** — `POST /audit` accepts entries from the Fastly Blossom control plane and re-emits them as structured Cloud Logging JSON.

## Endpoints

The service runs a single axum router over HTTP/1 and HTTP/2. Endpoints that mutate state require Blossom auth or a session token as noted; most routes also answer `OPTIONS` for CORS preflight.

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| `PUT` | `/upload`, `/` | Nostr (`upload`) | Direct single-request blob upload; streams to GCS and runs follow-up hooks. |
| `POST` | `/upload/init` | Nostr (`upload`) | Open a resumable session; returns `uploadId`, opaque `uploadUrl`, `chunkSize`, and a Bearer session token. |
| `PUT` | `/sessions/:upload_id` | Bearer session token | Upload one chunk, addressed by `Content-Range`. |
| `HEAD` | `/sessions/:upload_id` | Bearer session token | Query the next expected offset to resume an interrupted upload. |
| `POST` | `/upload/:upload_id/complete` | Nostr (`upload`) | Verify the assembled bytes against the declared `sha256` and promote them to the canonical blob. |
| `DELETE` | `/upload/:upload_id` | Bearer session token | Abort a session and discard its temporary object. |
| `POST` | `/migrate` | none (server-side Blossom auth) | Mirror a blob from an allowlisted Blossom/CDN host. |
| `POST` | `/audit` | none | Ingest a control-plane audit-log entry. |
| `GET` | `/thumbnail/:hash` | none | Return (or generate on demand) the JPEG thumbnail for a stored video. |
| `GET` | `/` | none | Human-readable landing page describing `upload.divine.video`. |

## Architecture

### Upload flow

**Direct upload.** `PUT /upload` validates the Blossom auth event, streams the body to GCS while computing its SHA-256, and stores it under `<sha256>` with an `owner` metadata tag. For video content types it also extracts a thumbnail, probes dimensions, and (fire-and-forget) triggers HLS transcoding and transcription. The response is a Blossom blob descriptor: `sha256`, `size`, `content_type`, `uploaded`, a `url` on the CDN base, and optional `thumbnail_url` / `dim`.

**Resumable upload.** The client opens a session with `POST /upload/init`, sending the target `sha256`, `size`, and `contentType`. If the auth event carries an `x` hash tag it must match the declared `sha256`. The service creates a GCS resumable upload for a temporary object, persists a session manifest, and returns:

- `uploadId` and an opaque `uploadUrl` pointing at `/sessions/:upload_id`,
- the advertised `chunkSize` and `nextOffset`,
- `requiredHeaders` including `Authorization: Bearer <session token>`,
- `capabilities` (`resume`, `queryOffset`).

The client then `PUT`s chunks to `/sessions/:upload_id` with a `Content-Range` header. Chunks must arrive in order at the expected offset and — except for the final chunk — be a multiple of 256 KiB. `HEAD /sessions/:upload_id` returns the next offset so an interrupted transfer can resume. `POST /upload/:upload_id/complete` requires the whole declared size to be committed, re-hashes the stored bytes, rejects a mismatch, and copies the temporary object to the canonical `<sha256>` blob (skipping the copy if that blob already exists) before deleting the temp object. `DELETE /upload/:upload_id` aborts and cleans up.

### Storage

Google Cloud Storage is the only backend (bucket default `divine-blossom-media`):

- Canonical blobs live at the object key `<sha256>` — content-addressed and deduplicated.
- Resumable temp objects live under `__resumable/uploads/<upload_id>/blob`.
- Session manifests are JSON at `__resumable/sessions/<upload_id>.json`, holding the owner, declared hash/size, offset, session URL, and token. Sessions expire after `RESUMABLE_SESSION_TTL_SECS`.
- Thumbnails are stored alongside blobs as `<sha256>.jpg`.

### How it fits Divine's media stack

`divine-blossom` (Fastly) is the client-facing Blossom control plane; this service is the data plane behind `upload.divine.video`. Finalized blobs and thumbnails are served from the CDN at `media.divine.video`. Video uploads hand off to the transcoder (`TRANSCODER_URL`) for HLS and to the transcription service (`TRANSCRIBER_URL`); these calls are best-effort and asynchronous, so upload latency is unaffected and a downstream outage does not lose the blob.

## Getting started

Requires a Rust toolchain and `ffmpeg` on `PATH` for thumbnail extraction and video probing.

Run the test suites:

```bash
cargo test
python3 -m unittest discover -s tests -p 'test_*.py'
```

Run the service locally (defaults to port `8080`):

```bash
cargo run
```

Without GCS credentials the storage-backed routes will fail, but the server starts and serves the landing page and CORS preflight. The Python helpers at the repository root — `export-video-upload-hashes.py` and `backfill-thumbnails.py` — are standalone operational scripts.

## Configuration

All configuration comes from environment variables. The table below lists the code defaults, which are not always what production deploys — see the note under the table.

| Variable | Default | Purpose |
| --- | --- | --- |
| `GCS_BUCKET` | `divine-blossom-media` | Bucket for blobs, thumbnails, and session state. |
| `CDN_BASE_URL` | `https://media.divine.video` | Base URL for returned blob and thumbnail URLs. |
| `UPLOAD_BASE_URL` | `https://upload.divine.video` | Base URL used to build opaque session `uploadUrl`s. |
| `PORT` | `8080` | Listen port. |
| `MIGRATION_NSEC` | — | Nostr secret key used to sign Blossom auth when mirroring via `/migrate`. |
| `TRANSCODER_URL` | — | Transcoder endpoint for HLS generation; unset disables transcoding. |
| `TRANSCRIBER_URL` | falls back to `TRANSCODER_URL` | Transcription endpoint for audio/video. |
| `RESUMABLE_SESSION_TTL_SECS` | `86400` | Resumable session lifetime (24h). |
| `RESUMABLE_CHUNK_SIZE` | `8388608` | Advertised chunk size (8 MiB), capped to `RESUMABLE_MAX_REQUEST_BODY_SIZE`. |
| `RESUMABLE_MAX_REQUEST_BODY_SIZE` | `1048576` | Upper bound on the *advertised* chunk size (1 MiB); `UPLOAD_ROUTE_MAX_BODY_SIZE` is accepted as an alias. |

`RESUMABLE_CHUNK_SIZE` is capped to `RESUMABLE_MAX_REQUEST_BODY_SIZE` before it is advertised in `/upload/init`.

`RESUMABLE_MAX_REQUEST_BODY_SIZE` is **not** an enforced request-body limit, despite the name — it is consumed only by that clamp. Inbound chunk size is bounded by the ingress: the `divine-upload-server` HTTPRoute attaches the `upload-body-size` SnippetsFilter (`client_max_body_size 16m`), a location-context directive that overrides the 100 MiB gateway-wide `ClientSettingsPolicy`.

The server itself never rejects a chunk for being too large. On a chunk `PUT` it checks that the `Content-Range` start equals the session's next offset, that the `Content-Range` length matches the body length, and that a non-final chunk is a multiple of 256 KiB. Lowering the advertised chunk size therefore cannot reject clients that keep sending larger chunks, as long as they start at the expected offset and stay 256 KiB-aligned.

These are code defaults. Deployed values differ per environment and live in the `divine-upload-server` manifests in `divine-iac-coreconfig`; read those rather than assuming the defaults are what production runs.

## Deployment

The service is packaged as a multi-stage Docker image (Rust build stage, Debian slim runtime with `ca-certificates` and `ffmpeg`) and runs on production GKE behind `https://upload.divine.video`.

CI (`.github/workflows/ci.yml`) checks formatting, runs `clippy` with warnings denied, and runs the Rust and Python test suites on every pull request. On pushes to `main` it authenticates to Google Cloud via Workload Identity Federation and publishes `divine-upload-server` to `us-central1-docker.pkg.dev/dv-platform-prod/containers-production`, tagged `latest` and the short commit SHA.

Video follow-up currently points at the existing transcoder endpoint through `TRANSCODER_URL`. `export-video-upload-hashes.py` reads both the legacy Cloud Run audit logs and the GKE `k8s_container` audit logs, so historic exports keep working across the platform migration.

---

Part of [Divine](https://divine.video) — your playground for human creativity · [Brand guidelines](https://github.com/divinevideo/brand-guidelines)
