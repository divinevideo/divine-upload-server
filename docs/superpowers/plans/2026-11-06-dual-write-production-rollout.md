# Production GCS and B2 Dual Writes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking. This is a proposed plan for user review, not authorization to implement or activate it.

**Goal:** All active new-media upload paths and generated artifacts have verified GCS and private B2 copies, with B2-source processing and bounded playback acceptance proven so full cutover can follow promptly.

**Architecture:** Use the existing strict `MEDIA_WRITE_PROVIDER=dual` implementation. Keep GCS first in finalization events and keep current public playback routing. Add shared write/erasure coordination, a durable finalization consumer, and version-aware derivative processing; provision and activate them through coreconfig. Deploy dependencies with dual admission off, then test a separate synthetic endpoint before switching production admission.

**Tech Stack:** Existing Rust uploader and Blossom processing code; PostgreSQL coordination/outbox; private Backblaze B2; GCS; Kubernetes/ArgoCD/Kustomize and OpenTofu/Terragrunt in divine-iac-coreconfig.

**Spec:** User request in this session: plan first, reach dual writing before full cutover. Protocol baseline: `docs/contracts/media-finalized-v1.md`. Prior uploader implementation: `docs/superpowers/plans/2026-11-05-upload-server-b2-support.md`. Cross-repo constraints: divine-blossom `docs/superpowers/plans/2026-10-21-gcs-b2-cutover.md` and `docs/runbooks/b2-original-sync.md`. Strict dual success is approved; the implementation sequence and other proposed design decisions below await review.

## Proposed design and scope

- Confirmed by the user: strict dual writes. Upload success requires both verified originals; locally generated thumbnails follow that policy. If B2 fails after GCS succeeds, return a retryable failure and retain durable intent. Processing completion remains asynchronous.
- Alternative considered and rejected for this milestone: accept GCS and mirror asynchronously using the existing Blossom mirror approach. This preserves upload availability but permits successful uploads without a B2 copy and has different completion/backlog guarantees. The user selected strict dual success.
- Extend the existing versioned finalization contract, not the old hash-only transcoder request. A 2xx acknowledgement means persisted work; it never means transcoding, moderation or delivery approval has completed. Upload acknowledgement requires both originals and durable processing ownership; asynchronous artifact status remains pending until its required copies are verified.
- Proposed placement: a small Rust `media-finalizer/` service in divine-blossom, deployed on GKE through coreconfig, owns durable intake and retry workers. Extend existing processors rather than move the Cloud Run transcoder. This gives workers a supervised lifecycle and avoids depending on background execution after an HTTP acknowledgement on Cloud Run. Confirm repository layout during Task 1 before creating files.
- All active new-media upload paths, originals and newly generated thumbnails, HLS/renditions, transcripts/subtitles, and extracted audio follow the recorded dual policy. Audit additional artifacts and alternative writers before activation. Build and test B2-only source processing in this milestone; historical migration and the default playback switch remain separate activation steps.
- Retain GCS originals, public URLs, current playback routing and existing approved-only B2 delivery behavior. Raw B2 writes do not create approved-replica readiness records.

## Verified starting point and unknowns

- Coreconfig PR 2455 deployed source `2efac310e3b55dd5e667a0863bb7348231930c73`; both production pods matched published digest `sha256:fa0bd2d1b0e3e3838548b7f52bf183b95272f171fc7ed301607819e612f6e241`. Storage remained GCS because no dual settings were bound.
- Live signed direct/resumable uploads, HEAD offsets, completion byte readback and abort passed in GCS mode. These were synthetic protocol smoke tests, not actual-client saturation or B2 tests. Local suite: 112 Rust tests with disposable PostgreSQL, 16 Python tests.
- The uploader already has B2 signing/multipart, verified dual originals/thumbnails, PostgreSQL intents/outbox, explicit retry repair and provider-pinned sessions.
- Blossom has a live approved-media B2 replica and copy/erasure coordination for that delivery destination. This is not proof of an authoritative raw bucket or coverage of uploader writes.
- No compatible deployed finalization receiver or raw-authority binding was verified in the prior investigation. Existing resources must be inventoried before creating replacements; do not treat incomplete discovery as proof they do not exist.
- Existing processors use hash-oriented requests and GCS assumptions. Their acknowledgement is not the durable consumer acknowledgement required by the uploader. Binding the outbox endpoint/token even in GCS mode disables existing uploader processing/NSFW follow-ups; provisioning secrets must not silently switch production processing. The current local-thumbnail path also catches persistence failures after original finalization: full dual needs explicit artifact ownership/recovery, not reliance on this best-effort behavior.
- No numerical production latency/error/load thresholds have been measured for this rollout. Task 1 must establish them before activation; do not invent capacity claims.

## Global constraints

- Separate private raw-authority and approved-only delivery buckets. Verify exact account, name, ID, region, privacy, version/retention/lifecycle behavior and erasure capability privately; preserve existing buckets.
- No credential values in code, plans, commands printed to users or public artifacts. Use separate scoped workload secrets and existing approved provisioning processes.
- Full hashes and exact immutable provider versions; ETags are not content identity. Public descriptors retain canonical CDN/hash URLs.
- Every writer and eraser for a destination must share an enforced coordination boundary. Finalization after a write is too late to prevent resurrection.
- Raw uploads may await moderation; storing bytes grants no public visibility. Preserve authenticated ownership, moderation, quarantine, tombstones and permanent deletion intent.
- Preserve GCS sessions and pending work across deployment/provider changes. No rollback to a GCS-only binary after admitting B2 work.
- Implementation follows per-repo isolated worktrees, independent reviews, mapped reviewers and GitHub checks/approval. This planning document authorizes no production switches or credential changes.

## Review focus

1. Lost provider responses and partial dual success: retain intent and resolve exact versions without blind overwrite or automatic copying during erasure.
2. Deletion races and duplicate owners: no late write/derivative can resurrect media or bypass ownership policy.
3. Mixed deployments and old sessions: requests preserve provider/bucket mapping, tokens and contiguous offsets across replicas/restarts.
4. Consumer crashes and duplicate delivery: durable acknowledgement, bounded retries and one expensive processing run per source/pipeline.
5. Client saturation and large/malformed media: bounded resources, explicit 413/503 behavior, usable client recovery and observable failures.

## Task 1 — Establish the deployment and contract baseline

**Repos/files:** uploader `docs/deployment.md`, `docs/contracts/media-finalized-v1.md`; Blossom `src/storage.rs`, `src/canary.rs`, `src/delete_policy.rs`, `cloud-run-transcoder/src/main.rs`, `docs/runbooks/b2-original-sync.md`; coreconfig `docs/for-platform/media-b2-storage.md`, `infrastructure/modules/media-b2-storage/`, upload overlay and PostgreSQL definitions.

**Produces:** A reviewed integration map, private resource inventory, rollout thresholds and exact contract/schema proposal. Identify every original/derivative writer and eraser, including legacy upload paths. Agree authority registration and derivative job ownership once. Route alternate live upload paths through the dual-aware data plane or update them to the same contract; close any bypass before claiming full dual.

- [ ] Inventory existing B2 authority resources, secret *names*, databases, services and pending PRs through repo history, Brain and live metadata. Reuse suitable existing resources. If absent, specify precisely what Task 2 provisions.
- [ ] Measure representative peak concurrent direct/chunk/finalization work, upload sizes, scratch use and existing error/latency baseline. Record numerical rollout stop thresholds and observation interval in the private runbook before launch.
- [ ] Define authenticated internal grant/complete/reconcile operations for authority writes and erasure. Their contract must cover destination/hash, attempt ownership, permanent tombstones, uncertain outcomes and recovery; proposed endpoint names are not existing APIs.
- [ ] Define durable consumer receipt/storage, worker fan-out and the exact processor envelope carrying source, replicas, owner, job and pipeline version. Select one owner per follow-up so GCS notifications and the new consumer do not run duplicate pipelines.
- [ ] Add consumer/producer contract fixtures that reject wrong versions, missing replicas, invalid acknowledgement and unauthenticated requests. Confirm the new receiver tests fail before implementation. Run existing producer `cargo test processing` unchanged as baseline.
- [ ] Commit reviewed contract/runbook changes. Exit: resource reuse/provisioning decisions and shared coordination semantics are concrete; no guessed bucket or endpoint.

## Task 2 — Provision the private authority and durable runtime, disabled

**Coreconfig files:** existing `infrastructure/modules/media-b2-storage/` plus a production Terragrunt consumer if required; `k8s/postgres-clusters/base/` database/role resources using current CNPG conventions; upload `base/external-secret.yaml` and production overlay; new `k8s/applications/media-finalizer/{base,overlays/production}/` and ArgoCD application definition if Task 1 confirms no suitable receiver.

**Produces:** Verified private authority destination, scoped keys, dedicated database/schema and TLS connection, durable receiver/worker deployment definitions. Provision secrets separately from binding outbox settings to the production uploader; deploy the new receiver only after Task 4 provides its tested image. Required eventual uploader references include `B2_MEDIA_BUCKET`, `B2_REGION`, `B2_ACCESS_KEY_ID`, `B2_SECRET_ACCESS_KEY`, `COORDINATION_DATABASE_URL`, `MEDIA_FINALIZED_URL`, `MEDIA_FINALIZED_TOKEN`.

- [ ] Add plan/manifest tests for private authority vs delivery separation, separate reader/writer/eraser scopes, required secret references and `MEDIA_WRITE_PROVIDER=gcs` during preparation.
- [ ] Provision/adopt through normal reviewed infrastructure plans. Do not repurpose the canary, widen a delivery key or change retention on an existing bucket to make tests pass.
- [ ] Provide supervised workers, resource/scratch bounds, database backups, connectivity/network access and startup/readiness checks; ensure uploader and receiver roles cannot administer unrelated databases.
- [ ] Run module validation/mock plans for changed infrastructure, render affected Kustomize overlays, run kubeconform and relevant coreconfig contract tests. Inspect real plan and deployed secret-sync/readiness status without exposing values.
- [ ] Commit and deliver reviewed PRs. Exit: authority/database/resources are independently verified and deployment definitions are ready; production processing bindings remain unchanged. Receiver readiness is verified after Task 4 deployment.

## Task 3 — Enforce shared authority write/erasure exclusion

**Files:** uploader `src/storage/mod.rs`, `src/resumable.rs`, `src/resumable_storage.rs`, `src/outbox.rs` and a focused new authority-coordination module; Blossom policy/erasure modules and Task 1 internal contract implementation. Include processing writers from Task 5.

**Consumes:** Task 1 coordination contract and Task 2 runtime. **Produces:** A guard held for every affected provider mutation, shared with all-version erasure; recoverable unresolved outcomes.

- [ ] Write failing cross-service tests for erase-before-write, erase-during-upload, late provider commit after timeout, duplicate upload/retry, derivative completion after tombstone and competing attempt release.
- [ ] Implement grants scoped to authority destination/hash with fenced ownership. Audit all B2 writes, multipart completion, canonical promotion and explicit missing-copy repair. Temporary bytes and multipart sessions require their own cleanup/erasure coverage. Register each parent-version→derivative identity and deletion fence before provider I/O, including HLS path keys and separately hashed extracted audio; erasing a parent must cover artifacts whose keys differ from its hash.
- [ ] Reuse the *semantics* of delivery coordination where appropriate, not its approved-only bucket identity or readiness records. A service-local Postgres advisory lock alone does not exclude Blossom erasure.
- [ ] Retain uncertain writes until writers are conclusively drained and exact provider versions reconciled. No TTL/unconditional unlock or automatic resurrection repair. Define how deleting one owner's association differs from global erasure under existing ownership policy.
- [ ] Build and exercise operator reconciliation for incomplete direct/mirror intents: distinguish never-started writes from uncertain accepted writes, drain writers, inspect exact provider versions and conditionally resolve state. Existing mode/bucket changes otherwise return 409; retaining B2 credentials alone does not preserve retry repair for those intents. Never silently retarget an intent.
- [ ] Run producer and Blossom race suites with disposable state plus a bounded provider-backed race drill. Commit reviewed changes. Exit: all affected writers and erasers enforce the same boundary; no partial deployment can enable dual admission.

## Task 4 — Add the durable finalization receiver and single processing handoff

**Proposed Blossom files:** `media-finalizer/Cargo.toml`, `media-finalizer/src/{main,contract,queue,worker}.rs`, `media-finalizer/database/migrations/001_finalization.sql`, integration tests. Update uploader `src/processing.rs`, `src/outbox.rs` only where the agreed contract requires it.

**Consumes:** `MediaFinalizedV1` from `docs/contracts/media-finalized-v1.md`. **Produces:** authenticated HTTPS intake with durable `(schema_version=1, job_id, durable=true)` acknowledgement after commit, persistent job state and supervised retry workers.

- [ ] Write failing tests for acknowledgement loss, receiver restart before/after commit, duplicate events, changed versions/pipeline, wrong token, unverified owner/mirror-null owner and permanent deletion intent.
- [ ] Persist the complete versioned event before acknowledgement; reject malformed references and validate source/replica bucket mapping. Deduplicate receipt by job ID and expensive processing separately by source/pipeline; retain per-owner registration semantics.
- [ ] Worker rechecks current policy/ownership/deletion before registration and each follow-up, preserving raw quarantine. Coordinate GCS-event processing and consumer processing through one durable dedupe authority. A GCS event can precede the B2 write/finalization; merge or wait for the recorded destination requirements so an earlier GCS-only job cannot permanently win and leave B2 artifacts absent. Late dual finalization must durably repair required replicas without duplicating processing. Include NSFW/moderation, transcription, ownership registration and legacy follow-ups in the replacement inventory.
- [ ] Verify at-least-once delivery and retry isolation against disposable PostgreSQL and actual producer wire fixtures. Run `cargo test --manifest-path media-finalizer/Cargo.toml` and uploader `cargo test processing` / `cargo test outbox` with disposable database.
- [ ] Commit/deploy receiver with production outbox bindings unchanged. Use a synthetic-only GCS+outbox cohort to test the actual processing/moderation handoff before any production binding switch. Exit: actual uploader-to-receiver delivery survives restarts and unavailable consumers; replacement hooks work and no processing/NSFW follow-up is lost or duplicated.

## Task 5 — Make derivative processing follow both recorded destinations

**Blossom files:** `cloud-run-transcoder/src/main.rs`, a focused storage/job module, transcription/audio/subtitle writers identified in Task 1; associated tests. Uploader `src/main.rs`/`src/thumbnail.rs` only for remaining local-thumbnail handoff gaps. Receiver worker from Task 4 schedules the jobs.

**Consumes:** version-pinned source/replicas and recorded write policy. **Produces:** one processing run with verified dual outputs and version-scoped completion status; existing public descriptor URLs.

- [ ] Write failing tests for stale source versions, delayed callbacks after erasure, repeated jobs, B2 outage on output, retry after one output succeeds and provider-mode changes while a job waits.
- [ ] Keep GCS as the dual job's first source initially, but read the recorded generation and retain the B2 reference. Replace hash-only source resolution for this new path. Implement B2 version-pinned source reads now and run the same processing suite with GCS source access denied in isolation, producing verified dual outputs where the selected policy requires them. Source selection and output policy are distinct; do not claim B2-source readiness from successful GCS-source processing.
- [ ] Generate each artifact once, write/verify both destinations under Task 3 coordination and persist per-artifact state. Resume missing writes without redoing successful expensive work or losing retries. Include generated JPEGs, renditions/HLS, extracted audio and transcripts/subtitles. Assign exactly one thumbnail generation owner across uploader, receiver and on-demand paths; persist missing-copy retries and track dual artifact completion. Do not swallow a failed B2 thumbnail write or return a descriptor claiming a stored thumbnail when that claim is unverified. Document justified artifact exclusions before launch.
- [ ] Preserve legacy processor endpoints/jobs during transition. Dispatch immutable source/job identities in callbacks so older results cannot overwrite newer state.
- [ ] Run image/audio/video processing integration tests plus existing thumbnail/transcription tests. Exit: actual finalization produces verified dual artifacts and policy-correct descriptors, with no duplicate GCS-notification pipeline.

## Task 6 — Exercise the complete system before production admission

**Files:** uploader new `scripts/dual-write-smoke.py` and test-fixture helpers; `docs/deployment.md`; Blossom integration harness/runbook; a synthetic-only coreconfig overlay/route if needed. No customer media or keys in fixtures.

**Consumes:** Tasks 2–5. **Produces:** dated acceptance evidence for the exact tested images/configuration and a tested rollback runbook.

- [ ] Implement a bounded synthetic harness that authenticates using an ephemeral identity, exercises existing public direct/resumable contracts and independently reads exact provider versions/checksums. Never output its credentials/session tokens.
- [ ] Test direct image/audio/video; multi-chunk resume after connection interruption and pod restart; duplicate completion; abort/expiry; stale offsets; and full SHA-256/size/metadata equivalence in both stores. Include B2 multipart upload, completion and abort with real provider credentials scoped to test data.
- [ ] Test B2 write/read outage, partial dual success and explicit retry repair; database/consumer outage and recovery; missing-copy reconciliation; delayed ACK; GCS notification arriving before dual finalization; local-thumbnail persistence failure; and one logical finalization/processing run. No successful strict-dual descriptor if a required copy is missing or unverified.
- [ ] Run actual current mobile/web clients under admission exhaustion in an isolated environment: occupy all slots, observe 503/Retry-After, release pressure and verify same resumable session/offset completes. Test size limits and scratch exhaustion; adjust measured limits/client retry behavior if these tests fail. Do not overload the public production endpoint to prove this.
- [ ] Exercise deletion during writes, queued jobs and derivative output; verify all-version deletion across both stores and existing cache/descriptor handling, with no late resurrection. Clean up all synthetic media/state through the supported policy-aware path.
- [ ] Verify normal playback continues through existing GCS-backed URLs and controls. On a bounded synthetic cohort, prove B2-primary original and derivative delivery, ranges/HEAD, policy enforcement and fallback using the intended cutover routing. The approved-only replica does not prove raw-authority read routing. Default playback remains unchanged. Test switching new admission back to GCS while B2 sessions/jobs drain, including a restart during rollback.
- [ ] Run `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --all` with disposable PostgreSQL and Python unit suites for affected repos; run coreconfig render/schema/contract checks and inspect CI. Exit: real-provider, real-consumer, current-client and erasure/rollback evidence all recorded, with measured stop thresholds.

## Task 7 — Activate strict dual admission through coreconfig

**Files:** uploader production overlay, bound secrets and deployment runbook; monitoring/alerts for uploader, receiver and processors. Optional synthetic route from Task 6 is separate from general client routing.

- [ ] Deploy compatible writers, erasers, receiver and processors first. Pass the synthetic GCS+outbox hook-replacement gate before binding production outbox settings, even if storage mode is still GCS. Verify that every active alternate upload endpoint is routed to the same dual policy or has a compatible implementation before declaring full dual. Verify all required pods/runtime images, credentials by metadata, database health and jobs. Keep upload mode GCS until this preflight passes.
- [ ] Drain/reconcile unfinished intents before introducing the outbox or changing provider/bucket mappings. Existing legacy GCS sessions may finish on GCS with compatibility retained. Do not retarget stored intents or delete rows to clear 409s.
- [ ] Run the exact dual configuration on the synthetic-only endpoint; verify complete end-to-end evidence. Current uploader selection is global per deployment, not a built-in per-user rollout flag. Do not mix differently configured pods behind one session route without verified sticky/provider-aware routing.
- [ ] Merge the reviewed production overlay setting `MEDIA_WRITE_PROVIDER=dual` with both provider bindings, coordination DB and durable consumer settings. Retain Google credentials/compatibility for old sessions. Verify serving pods and runtime mode explicitly, then run bounded synthetic live dual smoke/processing/readback/cleanup.
- [ ] Observe the Task 1 interval: upload success/latency and 413/503 rates; provider failures and bytes; partial-write intents; session recovery; outbox oldest age/retries; processing/derivative completion; erasure backlog; scratch/CPU/memory/database health. Stop or roll back at the agreed numerical thresholds.
- [ ] Rollback changes **new admission** to GCS, preserving the B2-capable binary, credentials, DB, receiver/workers, readers and pinned session/job mappings. Drain/reconcile pending work before any mapping change; retain a controlled endpoint with the original dual/bucket configuration for incomplete direct/mirror intent retries until they finish or undergo the tested operator reconciliation. Keep provider-pinned resumable work independently routable during the drain. Never revert to the old GCS-only image after B2 acceptance.
- [ ] Record serving images, mode, test evidence, observation results and unresolved limitations on rollout PRs. Declare this milestone complete only when production is actually dual writing verified originals and generated artifacts while existing playback stays healthy.

## Critical path and immediate cutover readiness

Work order: Task 1 resolves resource/contract decisions; Task 2 provisions dependencies while Tasks 3–5 implement coordination, receiver and processors against the agreed contracts; Task 6 integrates them; Task 7 activates full dual. Changes can be prepared concurrently after their interfaces are fixed, but deployment follows this dependency order. Do not estimate calendar completion until resource/IAM access and the erasure contract are verified. No original-only bridge counts as completion.

Prepare the cutover evidence alongside dual work, not after it:

- [ ] Inventory historical originals and every derivative class with missing/incorrect B2 copies; run verified backfill through the existing policy/erasure-aware machinery. Track historical coverage separately from coverage of new dual uploads.
- [ ] Prove B2-source processing with GCS reads denied in isolation and B2-primary delivery on the bounded cohort, including private-origin signing, browser/range behavior, descriptor references and policy/erasure handling.
- [ ] Exercise a limited B2-write-only admission endpoint, with both existing-provider session types draining correctly; verify that restoring dual/GCS admission preserves accepted B2 work.
- [ ] Record the exact remaining cutover changes: routing/origin preference, new admission to `b2`, provider/session/job drain and retention of necessary GCS reads. Establish cutover thresholds and rollback from measured results.

After full dual observation passes, hold a separate, explicit go/no-go for B2-primary reads and B2-only new admission. Existing media must have verified B2 coverage or an explicit GCS fallback; GCS dependency is not retired while that fallback is needed. Keep Google URLs and unrelated state/backups intact. Remove compatibility/access only after proven drain and the relevant retention decision. Default read routing, B2-only production admission and deletion of historical GCS media are not authorized by this planning request.

## Planning decisions awaiting user review

1. Resolved: the user selected strict dual success; both copies must be verified before upload success.
2. One supervised finalization service in Blossom/coreconfig rather than disguising the old transcoder endpoint as a durable receiver. Reuse an existing compatible deployment if inventory finds one.
3. User clarified full dual and an urgent later cutover: include all active upload paths/artifacts, build B2-source processing now, and prepare/test read cutover and historical coverage concurrently. Approve actual routing/admission cutover separately.

No production settings were changed while preparing this plan.
