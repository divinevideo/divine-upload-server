# B2 upload and media-processing support implementation plan

> **For agentic workers:** Use superpowers:executing-plans or superpowers:subagent-driven-development to implement this plan task by task after review. Steps use checkboxes for tracking.

**Status:** Implemented for code review with GCS-only defaults; production activation remains separate.

**Goal:** Make divine-upload-server store new uploads and generated media in B2, resume uploads reliably, and dispatch processing without depending on GCS media storage, while preserving its existing Blossom API.

**Architecture:** Add provider-aware storage behind the existing upload handlers and resumable manager. Pin storage identity to each upload and processing job; preserve canonical public URLs. Durable finalization records connect verified storage to retryable processing and Blossom registration.

**Tech stack:** Rust 1.98.1, axum, tokio, existing GCS client, a maintained S3-compatible Rust client for B2, and a proposed PostgreSQL coordination store. Choose and lock the S3 client version after exercising it against B2; AWS S3 compatibility alone is insufficient evidence.

**Spec:** The design decisions below specialize Task 4 of [Blossom's cutover plan](https://github.com/divinevideo/divine-blossom/blob/b526f5407841a19cd2caa7288cf6cf8948c52a7d/docs/superpowers/plans/2026-10-21-gcs-b2-cutover.md). This document incorporates the user's narrower scope: upload and processing support only.

## Scope and ownership

This repository owns direct uploads, resumable session handling, the existing mirror endpoint's storage operations, local thumbnail generation, and outgoing processing requests. Implement B2 support in those paths.

Other agents own legacy inventory/backfill, archive recovery, publisher changes, CDN selection, B2 delivery replication, production bucket provisioning, and global GCS retirement. Do not add a bulk archive importer, change the archive pipeline, or operate migration workers here.

The target is a server that can run with B2 media storage and without GCS credentials. Temporary GCS compatibility supports uploads already in progress and rollback. Removing historical media or changing published URLs is outside this plan.

## Evidence and existing interfaces

Investigated against upload-server `bf70414` and Blossom `0edaa57` on 2026-11-05. Recheck these contracts before implementation because companion work is active.

- `src/main.rs`: `Config` and `AppState` hardwire GCS; startup authenticates GCS unconditionally. Direct uploads, the existing `/migrate` handler, thumbnail retrieval, and thumbnail writes all use it directly.
- `stream_to_gcs_with_hash` currently buffers original bytes and derivative copies. Direct and mirrored uploads patch content type/owner after writing and ignore patch errors. Metadata must be part of successful B2 creation.
- `src/resumable.rs`: `SessionStore` and `ResumableBackend` provide useful seams. `UploadSession` stores a GCS session URL but no provider/bucket/version/part ledger. Manifests are GCS JSON objects with unconditional saves; concurrent replicas need stronger coordination.
- Direct uploads spawn transient transcoder/transcriber calls. Resumable completion locally invokes the detector; Blossom's `publish_upload_service_upload` handles control-plane registration and additional follow-up. Preserve that division and prevent duplicate jobs.
- Blossom already defines `StorageProvider` and `ObjectRef { provider, bucket, key, version }` in [storage_origin.rs](https://github.com/divinevideo/divine-blossom/blob/0edaa57/blossom-core/src/storage_origin.rs). Its comment explicitly distinguishes opaque object versions from ETags. Reuse that wire shape; this server does not need the Fastly runtime dependency.
- The existing GCS OBJECT_FINALIZE mirror is a GCS-source adapter, not a B2-upload finalization consumer. [Blossom's mirror design](https://github.com/divinevideo/divine-blossom/blob/d1ab38d/docs/superpowers/specs/2026-10-28-b2-upload-mirror-design.md) explicitly leaves B2-authoritative uploads for later work. Brain independently located the companion event work: document `5C6E3AADD6B222D444FA4E7593`, [infrastructure PR #2322](https://github.com/divinevideo/divine-iac-coreconfig/pull/2322).

## Proposed design decisions

### Storage and public compatibility

Introduce `ObjectRef` with Blossom's field names and require a nonempty version for persisted B2 objects. Add `StoredBlob { object, sha256, size, content_type, owner }`. Provider references are internal; public descriptors continue to use the configured CDN host and existing hash paths.

Proposed settings: `MEDIA_WRITE_PROVIDER=gcs|b2`, `B2_MEDIA_BUCKET`, `B2_REGION`, `B2_ACCESS_KEY_ID`, `B2_SECRET_ACCESS_KEY`, `GCS_COMPAT_ENABLED`, and `COORDINATION_DATABASE_URL`. Credential values come from secret bindings. Derive the HTTPS B2 endpoint from the validated region; do not accept arbitrary client-supplied storage endpoints.

GCS remains the initial default while the new path is disabled. B2 writes fail/retry on B2 failure; they never silently switch providers. B2 startup validates required settings and can run without initializing a GCS client when compatibility is disabled. Existing GCS sessions use their recorded backend regardless of the current write selection.

Keep public routes, authorization, SHA-256 identity, response field casing, status/error behavior, opaque session URLs, offset headers, and thumbnail URLs compatible. Preserve original bytes; sanitization applies only to derivatives. Do not turn storage success into moderation approval or claim that a private authority object is already a verified delivery replica.

### Coordination and durable finalization

**Proposal requiring review:** use PostgreSQL for session revisions, operation intents, object references, and a processing outbox. It provides atomic updates across replicas and removes GCS manifests as a permanent dependency. Provision a scoped database/schema through the platform workflow; this is control data, not media storage. No new database service is presumed available.

The alternative is transitional GCS manifests with generation-checked updates and a durable outbox, matching Blossom's allowance for temporary low-volume GCS coordination. That permits earlier B2 media writes but leaves a second coordination migration before this server can run without GCS. PostgreSQL is the recommended target; the first storage-adapter task can proceed independently of provisioning.

Persist an operation intent before provider I/O. After a verified canonical object is present, atomically record its provider/version and pending processing event. A bounded reconciler resolves intents after crashes, including an uncertain upload response. Never acknowledge successful completion with only a transient `tokio::spawn` responsible for processing. A retry must recover an already completed write rather than create an uncontrolled new version.

Proposed internal `MediaFinalizedV1` fields: `schema_version=1`, `job_id`, `source:ObjectRef`, `sha256`, `size`, `content_type`, `owner`, and `pipeline_version`. Persist a unique event for `(provider,bucket,key,version,pipeline_version)`; consumers must deduplicate by `job_id`. Reprocessing is explicit through a new pipeline version. Queue records contain no signed URLs or storage credentials.

An authenticated internal consumer must durably accept this envelope before the outbox marks it delivered. The consumer owns fan-out to processing and Blossom registration under current policy. Use an allowlisted service identity and endpoint; do not let callers choose callback URLs. Exact endpoint/auth provisioning is a companion interface decision, not an API claimed to exist today.

### Resumable uploads

Version the session schema. Persist provider, bucket, temporary object identity, negotiated chunk size, multipart ID, part numbers/ETags, immutable part references where needed, offset, expiry, and finalization state. A missing schema/provider on legacy JSON means GCS. Preserve the existing 24-hour default TTL; pin its actual expiry and chunk size at creation.

Negotiate 8 MiB for new B2 sessions, as specified by Blossom's plan, and verify the ingress admits that size. Current clients can send smaller 256 KiB-aligned nonfinal chunks; retaining that behavior requires durable coalescing. Stage sub-part chunks in private B2 session objects, record exact versions before acknowledging offsets, and assemble bounded 8 MiB multipart parts. An aligned full part can go directly to multipart storage. No acknowledged bytes may exist only in pod memory or temporary disk.

Keep part ETags for multipart completion, but compute the whole-object SHA-256 from original bytes and verify the completed temporary object's exact version before canonical promotion. [B2 multipart completion](https://www.backblaze.com/apidocs/s3-complete-multipart-upload) requires part numbers/ETags and returns an object version. Do not substitute that ETag for SHA-256.

Use revision-checked session updates and per-session operation ownership. Duplicate identical chunks recover the committed offset; conflicting bytes at a committed offset are rejected. Lost acknowledgements, a successful part PUT followed by failed state persistence, and uncertain multipart completion each have explicit reconciliation states.

Cancel/expire sessions by aborting multipart uploads and removing their exact staging versions. Use a scheduled bounded sweeper, not only cleanup when a client next accesses an expired session. Finalization, cleanup, and erasure must coordinate with Blossom's authority contract; a local database lock alone cannot exclude an external erasure worker. Preserve uncertain writes for reconciliation and never report cleanup complete while provider I/O may still commit.

### Processing

Route thumbnail source reads and JPEG writes through the same storage abstraction. Carry source version and destination provider through processing; never rediscover a B2 source by assuming its hash belongs in the GCS bucket. Preserve existing original/HLS source selection and thumbnail path conventions.

Move expensive follow-up work off the upload request through the durable dispatch path. Keep existing optional response fields compatible when a thumbnail/dimension result is not ready. The synchronous `/transcribe` audio proxy remains a byte proxy; it needs no object-store rewrite, but its auth and admission tests must keep passing.

Companion transcoder/transcriber/process-blob code in divine-blossom must accept provider references, read B2 originals, write derivatives to the agreed B2 destination, and return status for the same job/source version. The current hash-and-owner request does not establish that support. Deploy compatibility at consumers before enabling B2 writes here. Late status from an older source/job must not overwrite newer results.

## Global constraints

- Public upload behavior remains compatible; storage provider selection is server-controlled.
- Raw originals and generated assets use private authoritative storage. Approved delivery replication stays with Blossom.
- B2 support must cover images and audio as well as video; the existing video-only mirror canary is not the acceptance criterion.
- Preserve owner/content type/checksum/provenance metadata at object creation and propagate provider errors accurately: 403/429/5xx are not absence.
- Use bounded buffers, spool capacity, queue depth, and concurrency; shed admission with a retryable response before accepting work the service cannot durably retain.
- Retry and deletion logic must address actual immutable versions; do not assume B2 offers GCS generation-precondition semantics.
- Do not expose credentials, signed URLs, raw provider errors, or per-user records in logs or fixtures.
- No historical backfill, CDN switch, account-wide cleanup, or archive import campaign is part of this implementation.

## Review focus

1. A request is retried after bytes committed but before its response/state update: recover once (Tasks 2–4).
2. Two pods handle the same session or hash while deletion starts: preserve offsets and the shared erasure boundary (Tasks 2–4).
3. A client continues sending 1 MiB chunks or resumes after the provider flag changes: preserve acknowledged bytes and the session backend (Task 3).
4. B2 is healthy but a processing consumer still assumes GCS: reject activation; never silently strand media in processing (Tasks 5–6).
5. B2-only mode has no Google credentials, including on cold startup: all enabled upload/thumbnail paths work (Task 6).

## Implementation sequence

### Task 1 — Storage interface and B2 adapter

**Files:** create `src/storage/mod.rs`, `src/storage/gcs.rs`, `src/storage/b2.rs`, `src/storage/tests.rs`; modify `Cargo.toml`, `Cargo.lock`, `src/main.rs`.

**Interfaces:** define `StorageProvider`, `ObjectRef`, `StoredBlob`, `ObjectMetadata`, `PutResult`, and `StorageError`. `MediaStore` exposes `stat(&ObjectRef) -> Result<Option<ObjectMetadata>, StorageError>`, `read(&ObjectRef) -> Result<BoxStream<'static, Result<Bytes, StorageError>>, StorageError>`, `put_verified(&WriteIntent, &Path) -> Result<PutResult, StorageError>`, and `delete_version(&ObjectRef) -> Result<(), StorageError>`. `WriteIntent` fixes provider/bucket/key, expected SHA-256/size, MIME, owner, and operation ID. Concrete adapters implement verification/reconciliation; callers never infer content identity from an ETag.

- [ ] Add failing contract tests for metadata preservation, full-byte integrity, version-pinned reads, 404 versus 403/429/5xx, and redacted errors. Run `cargo test storage::tests` and observe the expected missing implementation failures.
- [ ] Implement the GCS adapter around current behavior and B2 adapter using the pinned client. Restrict SDK retry behavior so uncertain writes reconcile rather than blindly repeat.
- [ ] Run shared tests against a local HTTP provider fixture. Perform a bounded synthetic B2 integration check in the configured test bucket before enabling the adapter; mocks alone cannot establish B2 compatibility.
- [ ] Commit the independently tested adapter change with a Conventional Commit title.

### Task 2 — Durable state, finalization, and recovery

**Files:** create `src/coordination.rs`, `src/finalization.rs`, `src/outbox.rs`, `database/migrations/001_upload_coordination.sql`; modify `src/main.rs`, `src/resumable.rs`, `Cargo.toml`, `Cargo.lock`.

**Interfaces:** `CoordinationStore::save_session(session, expected_revision)`, `begin_write(intent)`, and `finish_write(operation_id, stored_blob, event)` use revision/unique constraints; `finish_write` atomically commits the object record and outbox entry. `OutboxDispatcher::run_once(limit)` leases pending jobs and acknowledges only durable consumer acceptance. An intent reconciler checks the recorded provider/key/version/operation identity before retrying.

- [ ] Add failing tests for crash points before/after storage commit, before/after outbox commit, lost consumer acknowledgements, and duplicate completion. Assert one logical finalization job and no lost committed object.
- [ ] Implement transactional state and bounded reconciliation. Preserve legacy GCS session loading for outstanding sessions; new sessions use the selected coordination store.
- [ ] Run `cargo test coordination`, `cargo test finalization`, and `cargo test outbox`, including two clients against a disposable PostgreSQL database. Assert stale revisions cannot advance offsets or release another operation's ownership.
- [ ] Commit state and recovery support. Document database provisioning as a deployment prerequisite.

### Task 3 — B2 resumable sessions

**Files:** modify `src/resumable.rs`, `src/main.rs`; create `src/storage/b2_resumable.rs` and focused tests alongside it.

**Interfaces:** extend `BackendCreateSession` and `UploadSession` with the versioned state above. Add `B2ResumableBackend` implementing the existing `ResumableBackend` operations with explicit version references. Resolve a backend from the loaded session, not a global current-provider flag. Replace unconditional session saves with Task 2 revision checks.

- [ ] Write failing tests: `legacy_session_defaults_to_gcs`, `b2_session_keeps_provider_after_switch`, `small_chunks_survive_restart`, `lost_part_ack_recovers_offset`, `duplicate_chunk_does_not_duplicate_bytes`, `conflicting_chunk_is_rejected`, `bad_whole_hash_never_promotes`, and `abort_reconciles_uncertain_completion`.
- [ ] Implement 8 MiB part negotiation, durable small-chunk coalescing, part ledger, version-pinned verification and promotion, and expiry sweeping. Preserve existing headers, auth, and completion descriptor snapshots.
- [ ] Run `cargo test resumable` and the B2 HTTP fixture suite. Exercise a pod restart between a provider write and state update; verify HEAD reports only durable contiguous bytes.
- [ ] Commit resumable support.

### Task 4 — Direct upload and existing mirror endpoint

**Files:** modify `src/main.rs`; create `src/upload.rs`; use Tasks 1–2.

**Interfaces:** replace `stream_to_gcs_with_hash` with `store_original` returning `StoredBlob` plus derivative-processing input. Both `process_upload` and existing `process_migrate` enter the same verified write/finalization flow. Preserve current source authorization/allowlisting; add no bulk import endpoint.

- [ ] Add failing tests for direct image/audio/video uploads and existing mirror requests using B2, preserved original SHA-256, atomic MIME/owner metadata, deduplication, and retries after uncertain writes.
- [ ] Stream into a bounded temporary spool while hashing, then upload with known length/hash. Avoid whole-file clones and bound concurrent spool use. Route successful writes into durable finalization before responding; processing itself remains asynchronous.
- [ ] Run `cargo test upload` plus existing auth and response-compatibility tests. Assert a B2 error never produces a successful GCS fallback write and a duplicate upload does not overwrite existing ownership/policy.
- [ ] Commit direct-upload support. This is the first new-media B2 milestone once Task 5's consumer contract is available; B2 resumable work need not block a separately selected direct-upload deployment.

### Task 5 — Thumbnail storage and processing contract

**Files:** modify `src/main.rs`, `src/thumbnail.rs`; create `src/processing.rs`, `docs/contracts/media-finalized-v1.md`; update `README.md`. Changes to actual remote processors are companion work in divine-blossom.

**Interfaces:** use `StoredBlob` as processing input and `MediaFinalizedV1` as the authenticated durable handoff. Preserve public URLs and the existing `/transcribe` proxy contract. Resolve thumbnail source and derivative destination from recorded storage identity; expose no private object references in public descriptors.

- [ ] Add failing tests for B2 thumbnail cache hits/misses, generated JPEG metadata, pinned source reads, delayed jobs, duplicate jobs, stale status, and processor rejection of unsupported provider versions.
- [ ] Implement storage-neutral thumbnail helpers and outbox dispatch. Agree with Blossom on one owner of each follow-up so local hooks and edge completion do not launch duplicate work.
- [ ] Verify a synthetic B2-only upload through the actual updated consumer: stored original, thumbnail, processing completion, and Blossom descriptor. Test image/audio/video and an unavailable consumer; accepted uploads remain durably queued.
- [ ] Run `cargo test processing`, `cargo test thumbnail`, and existing transcription tests. Commit the server changes and versioned contract documentation.

### Task 6 — Configuration, release checks, and GCS independence

**Files:** modify `src/main.rs`, `README.md`, `docs/deployment.md`, `backfill-thumbnails.py`; create `tests/test_backfill_thumbnail_storage.py` if helper behavior changes. Platform manifests and credential bindings are a companion PR owned by the infrastructure agent.

- [ ] Add failing startup tests for absent B2 settings, B2-only operation without Google credentials, and disabled legacy compatibility with outstanding GCS sessions. Add a helper test ensuring B2 mode never silently deletes a GCS thumbnail.
- [ ] Make GCS initialization optional; document separate media and coordination settings. Make the thumbnail maintenance helper provider-explicit or refuse unsupported B2 maintenance rather than operating on the wrong store.
- [ ] Verify direct/resumable uploads, thumbnails, cancellation, expiry, and processing dispatch in B2-only configuration with GCS network access denied. Inspect counters for zero GCS SDK calls in that mode.
- [ ] Run repository gates: `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --all`, and `python3 -m unittest discover -s tests -p 'test_*.py'`.
- [ ] Commit docs/configuration changes; deliver reviewed PRs with required checks inspected before activation.

## Companion integration requirements

| Owner | Contract this server needs | Why it gates enabling B2 writes |
| --- | --- | --- |
| divine-blossom control plane | Accept authoritative B2 object identity through a trusted internal finalization path; preserve policy, ownership and public descriptor behavior | Current GCS-only finalization/mirror assumptions cannot register B2-only objects automatically |
| Blossom processing services | Versioned authenticated jobs, B2 reads/writes, idempotent completion/status | Current requests contain only hash/owner and consumers still resolve GCS |
| Blossom erasure owner | Shared coordination for in-flight authority writes, version cleanup and derivative jobs | A service-local lock does not prevent cross-service resurrection |
| Platform | Private authority bucket, scoped keys, database/schema and credentials, ingress accepting 8 MiB, job runner/alerts | Storage and durable processing must operate across replicas/restarts |
| Migration/delivery agents | Confirm canonical URLs can serve new B2-backed media and that their rollback retains B2 reads | This server cannot make B2-only blobs playable by changing upload storage alone |

No existing canary readiness record should be repurposed as authority registration. Agree the authority/delivery bucket mapping with Blossom; never point new raw uploads at its approved-only canary bucket.

## Service rollout and rollback

1. Ship disabled adapters and backward-compatible readers. Deploy the durable consumer and coordination store before selecting B2 writes.
2. Exercise synthetic B2 uploads and processing through existing public upload contracts. Verify ownership/policy checks with the companion service and exact-version cleanup.
3. Select B2 for a bounded cohort of new direct uploads, then resumable sessions after their tests pass. Watch upload error/latency, committed offset recovery, finalization backlog age, derivative completion, and provider bytes read/written. Set numerical thresholds against the current measured baseline before the release; do not invent throughput claims from unit tests.
4. Expand new B2 writes through the platform's approved rollout. Existing sessions finish on their original provider. Stop creating GCS sessions, then drain or expire the last ones and verify no unresolved GCS intents/jobs remain.
5. Once external readers and processing support the objects this server needs, disable GCS compatibility and run the denied-GCS-network acceptance suite. Revoke this server's obsolete GCS access through the platform owner. Legacy media movement/deletion remains elsewhere.

Rollback stops admitting new B2 writes while keeping the B2-capable binary, readers, session handlers, workers, and credentials running. Existing B2 sessions finish or are explicitly cancelled and reconciled. Do not roll back to a GCS-only image after accepting B2 bytes. Queue retries retain their original provider reference.

## Review decisions and completion criteria

The substantive review choice is the coordination store: PostgreSQL now (recommended for eventual GCS independence), or temporary GCS control records with a separately scheduled replacement. The other cross-service decision is the authenticated finalization/erasure contract with Blossom; its field shape is proposed above and must be implemented by both producers and consumers before activation.

This server's work is complete when its public upload contracts pass on B2, processing receives durable version-pinned inputs, cancellation/expiry safely clean temporary objects, active sessions survive restart and provider switches, and B2-only deployment passes without GCS credentials or requests. It does not require this agent to migrate the historical catalogue or operate CDN cutover.


## Implementation decisions and verification

The implementation instruction added `dual` mode: both destinations are verified
before success, while retries repair partial writes. PostgreSQL was selected for
new session and processing state. Existing GCS sessions remain supported.

The B2 adapter reuses the project's reqwest/HMAC approach rather than introducing
a large S3 SDK dependency. Its production signing helpers are checked against
AWS's published S3 signature vector, plus local HTTP multipart/provider fixtures.
The adapter supports streamed 8 MiB multipart writes and durable cleanup tracking.

Background recovery verifies all required existing copies; it never repairs a
missing copy without the companion erasure boundary. Derivative storage follows
the recorded finalization policy across mode changes. Unknown write outcomes
remain durable for operator reconciliation. See the deployment runbook for
resource bounds, supervised database reconnect behavior, and activation drills.

Local verification: 94 Rust tests with disposable PostgreSQL, 16 Python tests,
formatting, Clippy with warnings denied, actionlint, and whitespace checks passed.
A synthetic B2-only cold start served HTTP without Google credentials; terminating
its dedicated database connections made the process exit for supervised restart.
Live B2, the actual companion consumer, and CDN/erasure integration remain
explicit deployment acceptance checks, not completed local-test claims.
