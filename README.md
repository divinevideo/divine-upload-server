# Divine Upload Server

`divine-upload-server` is the Rust upload data plane for Divine Blossom media. It receives blob bytes, stores them content-addressed in Google Cloud Storage, Backblaze B2, or both, and fires the media follow-up hooks (thumbnails, dimension probes, HLS transcoding, transcription) that turn a raw upload into a playable asset. It handles both single-request uploads and resumable, chunked sessions.

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

GCS is the default backend (bucket default `divine-blossom-media`); B2 and dual-write modes use the same public object keys:

- Canonical blobs live at the object key `<sha256>` — content-addressed and deduplicated.
- Resumable temp objects live under `__resumable/uploads/<upload_id>/blob`. In B2 and dual modes each chunk is instead stored as its own object under `__resumable/staged/<mode>/<hex GCS bucket>/<hex B2 bucket>/<upload_id>/chunks/`, and the chunks are assembled at completion.
- Without `COORDINATION_DATABASE_URL`, session manifests are JSON at `__resumable/sessions/<upload_id>.json`, holding the owner, declared hash/size, offset, session URL, and token. With it, sessions are stored in PostgreSQL instead. Sessions expire after `RESUMABLE_SESSION_TTL_SECS`.
- Thumbnails are stored alongside blobs as `<sha256>.jpg`.

### How it fits Divine's media stack

`divine-blossom` (Fastly) is the client-facing Blossom control plane; this service is the data plane behind `upload.divine.video`. Finalized blobs and thumbnails are served from the CDN at `media.divine.video`. Video uploads hand off to the transcoder (`TRANSCODER_URL`) for HLS, the transcription service (`TRANSCRIBER_URL`), and the self-hosted detector (`AI_DETECTOR_BASE_URL`) for evidence-only NSFW scanning. These calls are best-effort and asynchronous, so upload latency is unaffected and a downstream outage does not lose the blob. Repeated uploads of an already stored hash do not resubmit detector evidence.

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
| `AI_DETECTOR_BASE_URL` | — | Self-hosted detector base URL. Newly stored videos request the `nsfw` signal asynchronously; unset disables the hook. |
| `RESUMABLE_SESSION_TTL_SECS` | `86400` | Resumable session lifetime (24h). |
| `RESUMABLE_CHUNK_SIZE` | `8388608` | Advertised chunk size (8 MiB), capped to `RESUMABLE_MAX_REQUEST_BODY_SIZE`. |
| `RESUMABLE_MAX_REQUEST_BODY_SIZE` | `1048576` | Upper bound on the *advertised* chunk size (1 MiB); `UPLOAD_ROUTE_MAX_BODY_SIZE` is accepted as an alias. |

`RESUMABLE_CHUNK_SIZE` is capped to `RESUMABLE_MAX_REQUEST_BODY_SIZE` before it is advertised in `/upload/init`.

`RESUMABLE_MAX_REQUEST_BODY_SIZE` is **not** an enforced request-body limit, despite the name — it is consumed only by that clamp. Inbound chunk size is bounded by the ingress: the `divine-upload-server` HTTPRoute attaches the `upload-body-size` SnippetsFilter (`client_max_body_size 16m`), a location-context directive that overrides the 100 MiB gateway-wide `ClientSettingsPolicy`.

On the chunk `PUT` path the server independently caps the collected body at 16 MiB and shares the `MAX_CONCURRENT_UPLOADS` admission slots. A chunk must also satisfy that the `Content-Range` total equals the session's declared size, that the range lies inside that size, that the range start equals the session's next offset, that the range length matches the body length, and that a non-final chunk is a multiple of 256 KiB. Lowering the advertised chunk size does not reject larger aligned chunks within the 16 MiB body cap and the declared size.

These are code defaults. Deployed values differ per environment and live in the `divine-upload-server` manifests in `divine-iac-coreconfig`; read those rather than assuming the defaults are what production runs.

## B2 primary with synchronous GCS backup

`MEDIA_WRITE_PROVIDER=b2_primary` selects a separate staged migration policy:
B2 is authoritative and is written and fully verified first; the canonical GCS
backup must then be stored and independently verified before upload success.
Both immutable references are retained in B2, GCS order. Public hash URLs remain
unchanged. `dual` retains its historical GCS-first policy.

This mode uses the existing GCS bucket for small private admission records and
resumable session state; it requires **no coordination database or new worker**.
Keep `MEDIA_FINALIZED_URL` and `MEDIA_FINALIZED_TOKEN` unset: startup rejects
those settings in this mode so an intake-only outbox cannot suppress the existing
HLS, transcription and NSFW hooks. Those processors can continue reading the
verified canonical GCS backup. Their existing best-effort/retry limitations are
unchanged. Resumable completion retains its existing NSFW/Core publication and
on-demand HLS behavior; it does not promise eager HLS or a thumbnail.

Required configuration is the existing GCS bucket/credentials and
`B2_MEDIA_BUCKET`, `B2_REGION`, `B2_ACCESS_KEY_ID`, `B2_SECRET_ACCESS_KEY`.
GCS compatibility must remain enabled. `MEDIA_ORIGINAL_WRITE_ENABLED` remains off;
its preparation-only API is not used as an upload prerequisite.

New resumable sessions stage bytes in GCS under
`__resumable/b2_primary/<hex GCS bucket>/<hex B2 bucket>/<upload_id>/blob`.
Completion checks the declared digest and promotes through the pinned B2-first
policy, including after new admissions are switched back to GCS. New-mode JSON
session manifests use generation preconditions; deleting a session retains a
small tombstone so stale writers cannot resurrect it. Old GCS/B2/dual sessions
retain their original policy and provider requirements.

Per-key admission records at `__resumable/b2-primary-writes/v1/` bind the bytes,
first metadata and provider buckets using GCS create-only/generation CAS.
Provider issuance is recorded before effects; the actual immutable B2 receipt
is recorded before verification, and verified primary identity before backup
I/O. Multipart upload IDs are retained in the same records. Completed replays
read exact recorded versions and verify bytes. An explicitly retried verified
primary with a backup that was never issued can continue without rewriting B2.
An admission whose provider slots are both still not-issued can be reacquired
with generation CAS after a lost admission response or restart; the old actor
cannot then issue an effect using its stale generation.
An issued operation with missing/unverified receipt stays pending for explicit
reconciliation; no TTL, latest-HEAD adoption, automatic repair or blind re-PUT
clears it. These private records are retained, not automatically garbage-collected.

If a database already holds historical pending upload intents, retaining its
existing configuration preserves that state: direct/mirror retries resolve the
old reservation, thumbnail barrier and ordered event receipt per request. New
hashes do not create PostgreSQL intents or suppress legacy processing. Historical
events still require their already configured consumer/deployment for delivery;
this mode does not create a replacement dispatcher. A deployment with no such
history needs no database. Do not discard an existing history database or its
consumer while accepted work remains.

To roll back new admissions, keep this image, GCS compatibility and the B2
bucket/credential bindings, and set `MEDIA_WRITE_PROVIDER=gcs`. This GCS-plus-B2
drain configuration does not require PostgreSQL. New hashes use GCS; recorded
B2-primary direct/mirror retries and on-demand derivative policy still use their
accepted record. Existing new-mode resumable sessions finish against their
pinned buckets. Keep the same GCS bucket as the state anchor; moving that bucket
is not a supported admission rollback. Within that retained state namespace,
missing or changed B2 bindings fail instead of silently retargeting. Historical
`b2`/`dual` PostgreSQL sessions still require their old database and processing settings; this path does not convert or migrate them.

These controls serialize this uploader's new-mode writes; they do not add shared
copy/erasure exclusion or publication approval. Existing Core pre/post-upload
policy checks and delivery guards remain in force, with their existing race and
lookup-error limitations. Ordinary upload paths do not currently join the shared
B2 copy/erasure gate. Source checks do not prove provider-backed erasure, full
artifact parity or an activated cutover. Operators must route normal Core uploads
through this data plane and validate both copies and normal publication before
claiming the upload switch is working; GCS retirement remains separate.

## B2 and dual-write transition

`MEDIA_WRITE_PROVIDER` selects `gcs` (the default), `dual`, or `b2`. In `dual`
mode every finalized original and locally generated thumbnail must be verified
in both configured destinations before its storage operation succeeds. A failed
second write is returned as an error; retrying repairs the missing copy. Public
Blossom URLs stay on `CDN_BASE_URL`.

B2 requires `B2_MEDIA_BUCKET`, `B2_REGION`, `B2_ACCESS_KEY_ID`, and
`B2_SECRET_ACCESS_KEY`. Use the private authoritative media bucket agreed with
Blossom, never the approved-only playback canary bucket. Credentials belong in
secret bindings. B2-only startup does not require Google credentials. Set `GCS_COMPAT_ENABLED=true`
when B2 mode still needs to finish old GCS sessions. When switching new writes
back to GCS, retain B2 settings so existing B2 sessions can finish.

B2 and dual modes also require `COORDINATION_DATABASE_URL`, `MEDIA_FINALIZED_URL`,
and `MEDIA_FINALIZED_TOKEN`. The PostgreSQL database holds durable upload state
and processing work. The HTTPS processing endpoint must implement the versioned
[finalization contract](docs/contracts/media-finalized-v1.md); an ordinary
transcoder endpoint is not a compatible substitute. Queue acceptance is distinct
from completed thumbnail/transcode/transcript work. Deploy the companion consumer
and verify B2 processing and erasure coordination before enabling these modes.

Existing GCS sessions retain their backend. Keep their credentials available
until they finish or expire; changing the new-upload mode does not migrate an
active session. To stop creating B2 uploads, change admission back to GCS while
retaining the B2-capable binary, workers, database, and credentials to drain work.
Do not roll back to an older GCS-only image after accepting B2 uploads.

The legacy `backfill-thumbnails.py` deletion helper supports GCS only and rejects
B2/dual selection; it must not delete one copy while another remains cached.

With the outbox enabled, direct and mirror retries use an unfinished intent's
recorded provider policy, even after the new-upload mode changes. Keep every
required provider and bucket available: an unavailable or changed destination
returns 409 rather than retargeting the intent. A dual retry still requires both
copies after GCS admission is restored. Resumable sessions retain their own
policy, so completing one for bytes whose unfinished intent was recorded under
another mode still returns 409; drain or reconcile those intents before a mode
switch. See the deployment runbook for reconciliation before changing bucket
mappings. With the outbox enabled, unauthenticated `/migrate`
events always have a null owner instead of trusting the request's declaration.
Thumbnail generation and mirroring have independent configurable admission pools
(`MAX_CONCURRENT_THUMBNAILS`, default 2; `MAX_CONCURRENT_MIRRORS`, default 1), separate
from uploads. Thumbnail cache hits bypass generation admission. Mirror fetches use
HTTPS allowlisted hosts without redirects and are bounded by
`MIRROR_FETCH_TIMEOUT_SECS` (default 120). Busy session locks return 503 with
`Retry-After`.

## Deployment

The service is packaged as a multi-stage Docker image (Rust build stage, Debian slim runtime with `ca-certificates` and `ffmpeg`) and runs on production GKE behind `https://upload.divine.video`.

CI (`.github/workflows/ci.yml`) checks formatting, runs `clippy` with warnings denied, and runs the Rust and Python test suites on every pull request. CI and local checks use Rust 1.98.1 (`rust-toolchain.toml`) so a moving stable toolchain cannot change the release gate unexpectedly. On pushes to `main` it authenticates to Google Cloud via Workload Identity Federation and publishes `divine-upload-server` to `us-central1-docker.pkg.dev/dv-platform-prod/containers-production`, tagged `latest`, the existing **seven-character** source SHA, and the **full** source SHA. Short-SHA tags remain available for existing manual promotion consumers. The build summary records the full SHA and pushed image digest; the new GitOps sender requests a digest-pinned release, never `latest`.

The optional GitOps promotion job requests a digest-pinned production image-update PR in `divinevideo/divine-iac-coreconfig` after both verification and image publication succeed. Existing production approvals remain required. The job stays disabled until the companion receiver and protected-environment prerequisites are ready. Dispatch acceptance is **not** deployment completion. See [the sender runbook](docs/deployment.md) for prerequisites, the payload contract, reruns, and the link to private operational instructions.

Video follow-up currently points at the existing transcoder endpoint through `TRANSCODER_URL`. `export-video-upload-hashes.py` reads both the legacy Cloud Run audit logs and the GKE `k8s_container` audit logs, so historic exports keep working across the platform migration.

The optional [durable finalization inbox and processing authority](docs/contracts/media-processing-assignment-v1.md)
reuses PostgreSQL coordination and the existing finalization event contract. It
is default-off, requires separate intake/worker credentials, and starts no
automatic processing workers. Persisted intake is distinct from complete
registration, moderation and artifact production; these remain outbox activation
requirements.

The default-off [producer preparation journal](docs/contracts/producer-journal-v1.md)
persists attempt nonces and immutable seven-artifact plans. Journal scope cannot
dispatch provider writes or commit new processing results without the remaining
authenticated parent-exclusion integration.

The default-off [original-upload preparation boundary](docs/contracts/original-write-preparation-v1.md)
records signed upload authorization and accepted storage destinations before
original finalization. `MEDIA_ORIGINAL_WRITE_ENABLED=true` requires PostgreSQL
and deliberately returns unavailable before new original provider I/O; it does
not enable upload execution. Existing committed resumable receipts remain
readable. Keep this flag off until the shared writer/policy and settlement
integration is complete.

---

Part of [Divine](https://divine.video) — your playground for human creativity · [Brand guidelines](https://github.com/divinevideo/brand-guidelines)
