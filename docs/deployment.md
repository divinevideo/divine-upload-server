# GitOps image-update sender

This code prepares a production image-update request. It does not provision
credentials, configure GitHub Environments, enable promotion, merge a PR, or
perform a controlled rollout. Those actions require separate authorization.

## Prerequisites before enabling

A Platform administrator must complete and verify these prerequisites through
the approved operational process, **before** making credentials available or
setting this repository's `GITOPS_DEPLOY_ENABLED` variable to `true`:

1. The companion receiver for
   [coreconfig issue 2338](https://github.com/divinevideo/divine-iac-coreconfig/issues/2338)
   is merged, reviewed and ready for the payload below. Its
   `UPLOAD_SERVER_GITOPS_ENABLED` opt-in is separately controlled. Sender code
   alone does not establish receiver readiness or authorize either opt-in.
2. Create/configure the GitHub Environment **`production-gitops`** with a custom
   deployment-branch policy allowing **only the branch `main`**, not tags or
   arbitrary protected branches. The workflow references that environment;
   the reference alone does not create or prove the required restrictions.
   Leave the sender disabled if that environment or its policy is absent.
3. Supply the existing dispatch App credentials as **environment secrets** in
   `production-gitops`: `CORECONFIG_DISPATCH_APP_ID` and
   `CORECONFIG_DISPATCH_APP_PRIVATE_KEY`. Do not expose them as repository-wide
   secrets or make organization-level copies accessible to every branch in this
   repository. Reuse the established App through the approved process rather
   than creating a new credential. Before enabling, confirm neither name is
   already available to this repository from repository or organization secrets.
   Never print or commit values.
4. Verify the App installation can dispatch to `divine-iac-coreconfig` with
   Contents write (and implicit Metadata read). The minted token explicitly
   requests only that repository and permission. The ordinary `GITHUB_TOKEN`
   remains Contents read and checks the source main tip only. The environment's
   branch restriction, rather than workflow self-checks alone, controls access
   to the dispatch credential.
5. Verify the serving destination and obtain rollout authorization using the
   [private receiver runbook](https://github.com/divinevideo/divine-iac-coreconfig/blob/main/docs/how-to/companion-image-deploy.md).
   That runbook owns cluster access, production approval, serving-image checks,
   and operational rollback. No such validation is performed by sender PR CI.

Missing credentials fail the enabled promotion job; there is no fallback to a
manual deployment or a weaker approval path.

## Sender contract

Only a successful **push to main** can publish and request promotion. The
promotion job depends on Verify and Publish Production Image for the same SHA.
Pull requests, other branches and `workflow_dispatch` cannot publish or deploy.
`dispatch-image.py --check-source` checks the current main tip before image
publication; the default helper mode repeats the tip check before dispatch.
A superseded main rerun fails instead of deliberately moving old registry tags
or requesting an older release. The checks cannot recall requests already sent.

The receiver event is `image-deploy`, with this `client_payload`:

```json
{
  "application": "divine-upload-server",
  "environments": ["production"],
  "images": [{"name": "divine-upload-server", "tag": "<full-source-sha>", "digest": "sha256:<64-hex>"}],
  "internal": true,
  "hotfix": false,
  "auto_promote": false,
  "source_repo": "divinevideo/divine-upload-server",
  "source_sha": "<same-full-source-sha>",
  "source_ref": "main",
  "image_digest": "sha256:<same-64-hex>"
}
```

Both digest fields are required by the companion contract and must match.
There are ten payload properties, the API limit; do not add another without
coordinating a receiver contract change. SHA tags provide source identity;
the receiver must **deploy by digest**, not by mutable tag. A same-source
rebuild can produce different content and requires its own digest-specific
production update and approval. Short-SHA tags remain published for existing
manual consumers but are not used by this sender's payload.

Build and promotion summaries record the full source SHA, image path and
digest. Retain both run URLs with the receiver run and generated PR. The image
path in the promotion summary comes from the publishing job's output, not a
second hard-coded registry path. A successful dispatch means only that GitHub
accepted a request; it is not proof of a receiver PR, approval, healthy rollout,
or serving image. Complete those checks using the private runbook.

## Failure recovery and reruns

- Failed verification/publication prevents promotion. Inspect the failed step
  before rerunning the failed job/dependent jobs on the **current main push**.
- HTTP errors report their status and a diagnostic category. Do not log raw
  response bodies or credentials to diagnose access or payload problems.
- A lost response can still have accepted a dispatch. Check receiver runs and
  open image-update PRs for the same SHA/digest before retrying. A successful
  dispatch can also fail receiver validation; inspect its run, not just the
  sender summary. Never weaken receiver checks or production approval to retry.
- `workflow_dispatch` on `main` repeats **Verify only**, in a separate concurrency
  group from push runs, so it cannot cancel publication/promotion. Rerunning the
  original current-main push is the promotion recovery path; arbitrary refs and
  stale main pushes are rejected.
- Rebuilding current main may move its tags to a different digest. Do not approve
  an older pending update whose registry-tag/digest comparison no longer holds;
  reconcile it with the newly built request. An already approved digest remains
  pinned even if tags move.
- Roll back through an approved receiver PR selecting a previously verified
  digest, then verify the serving image and health as the private runbook says.
  Do not rerun an old source build, deploy `latest`, patch a live workload, or
  assume a pending newer promotion will not overwrite the rollback.

## B2 support activation prerequisites

The B2 implementation is disabled by the default `MEDIA_WRITE_PROVIDER=gcs`.
Select `dual` to require verified GCS and B2 copies, or `b2` once the companion
system can read/process new B2 objects. Set the B2 credentials, scoped
`COORDINATION_DATABASE_URL`, and compatible authenticated `MEDIA_FINALIZED_URL`
and `MEDIA_FINALIZED_TOKEN` through the platform configuration. This endpoint is
a new durable consumer contract, not the current hash-only transcoder endpoint.
See [the contract](contracts/media-finalized-v1.md).

Use the private authoritative bucket agreed with Blossom; do not upload raw
media into the approved-only canary/delivery bucket. Activation requires the
companion policy/erasure exclusion and version-aware derivative pipeline, a
bounded real-B2 upload/readback/multipart/abort drill, and playback through the
existing CDN descriptor URLs. Local provider fixtures do not establish those
production properties. This change does not provision or enable that system.

Direct/mirrored upload spools and resumable declarations default to at most
1 GiB per object (`MAX_UPLOAD_BYTES`). Upload and finalization operations share
four slots per pod (`MAX_CONCURRENT_UPLOADS`, maximum 1024). Thumbnail generation
uses its own `MAX_CONCURRENT_THUMBNAILS` slots (default 2); `/migrate` uses separate
`MAX_CONCURRENT_MIRRORS` slots (default 1). Each setting accepts 1–1024. Cache hits
bypass thumbnail generation admission. Mirrors cannot occupy thumbnail or upload
admission; generation cannot occupy upload admission.
Exhausted admission returns 503; oversized uploads return 413. Provision scratch
disk for at least the configured maximum times the upload and mirror slot counts,
plus thumbnail scratch files and operational
reserve; disk-full errors fail the request and remove local temporary files.
Resumable requests are capped at 16 MiB per chunk and share admission slots.
Local thumbnail decoding retains the legacy 100 MiB input limit; larger assets
require the durable asynchronous processing path for thumbnails. GCS mode without
a durable consumer still probes dimensions from its disk spool for larger direct
uploads; it does not promise synchronous thumbnails above that limit.
Set measured limits before expanding concurrency; these defaults are bounds,
not a capacity estimate.

Admission and size limits apply in GCS mode too, including resumable chunk PUTs.
Keeping GCS as the storage default does not preserve the old unbounded admission
behavior. Before rolling out this binary, size the limits for the existing GCS
workload and verify saturation/recovery with the current clients. A client that
exhausts its short chunk-retry budget on repeated 503 responses can start a new
session rather than retain progress; `Retry-After` alone does not establish that
the client waits. The four-slot default is not a production-capacity measurement.

With the outbox enabled, `/migrate` does not propagate caller-supplied owners to
stored-object metadata or ownership-registration events. Its events use a null
owner. Do not infer ownership from these mirror events before a separately
verified caller/upload authorization contract is deployed.

Changing new-upload mode does not change unfinished intents. With the outbox
enabled, direct and mirror byte retries resolve the recorded policy before
provider I/O at the normal endpoint. Retain both provider adapters, credentials
and required bucket mappings when returning new admission to GCS: existing dual
work still requires B2 and may remain pending during its outage. Resumable
finalization must match the session's recorded policy; it cannot adopt a different
pending intent's destinations. A mode-only change therefore still strands a
resumable session for an owner and hash that already has an unfinished intent
recorded under the other mode: completion returns 409 after the full transfer
until that intent finishes through a direct or mirror retry or is reconciled.
Drain or reconcile such intents before switching modes when clients may retry the
same bytes through resumable upload. Finalization refuses missing, extra,
reordered or wrong-bucket references rather than marking such an intent complete.
An already committed resumable completion remains replayable after a later
upload changes policy: its durable session receipt is acknowledged without
changing the newer intent or artifact source ordering, even if the process stopped
before saving the final session manifest. The event, intent completion and receipt
commit before that manifest save; staging is retained until they commit. A receipt
also prevents aborting an already committed upload whose manifest save failed.
An unreceipted completion must match the current intent's destination policy; an
identical event from a different session does not establish completion. A session
saved by an older binary between storage finalization and receipt commit returns
409 if the policy has since changed. Reconcile that upload through its recorded
policy; do not delete or retarget the newer intent. A session completed through
the outbox before the receipts schema existed has no receipt either: it returns
the same 409 after a policy change, and with a matching policy its replay counts
as a new completion that makes its event the newest for those bytes again. Where
the outbox ran before that schema, wait one `RESUMABLE_SESSION_TTL_SECS` (24 hours
by default) after the schema went live before upgrading to this behavior or
enabling dual admission. Startup adds the
`upload_processing_session_receipts` table under the existing schema lock. Its
receipt/event/intent transaction distinguishes a new session reusing an event
identity from replay of a previously completed session. Expired-session cleanup,
both the background sweep and a request that finds an expired session, deletes
the session's receipt just before the session itself. Once a session expires,
`complete` returns 410 or 404 before any receipt is read, so a receipt lasts no
longer than its session. A failed receipt delete keeps the session for the next
pass. The outbox events that receipts referenced are retained; this
repository has no outbox retention job.

Thumbnail and derivative writes use every copy recorded for the same bytes,
across owners, not only the newest finalization. The newest event still leads
and supplies its versions; a provider it lacks comes from the newest event that
recorded one. A later GCS-only completion therefore cannot narrow derivatives of
media that already has a B2 copy. That completion might be a GCS intent left
unfinished from before dual admission, a background recovery of one, or an
upload accepted after admission returns to GCS. Keep the B2 binding while such
media can still need derivatives: without it, their derivative writes fail with
a mapping error instead of silently skipping B2. The same holds for GCS. Once
any event for the bytes records a GCS copy, their derivatives require GCS, so a
B2-only deployment without GCS compatibility cannot generate them. Retiring GCS
therefore needs an explicit change to how derivatives treat recorded GCS copies;
removing the GCS binding alone does not do it.

Before changing a required destination bucket or removing a provider, drain or
reconcile affected unfinished intents and accepted sessions. A mismatched mapping
returns HTTP 409 rather than retargeting unknown provider outcomes. Do not delete
an intent or silently change its mapping to clear that conflict.
This repository does not implement a general abandonment/retargeting tool.
Defining and exercising the companion operator reconciliation procedure is an
activation prerequisite before enabling the outbox or changing a mapping with
unfinished intents, including abandoned uploads with no completed source. It must
drain writers, distinguish never-started writes from uncertain accepted writes,
and inspect exact provider versions before any authorized state removal. A HEAD
404 alone does not establish that a delayed write cannot commit. Until that
procedure exists, retain the original required destinations rather than changing
their mapping or deleting rows to clear 409s. A mode-only change with all recorded
destinations retained does not require retargeting an intent for direct and mirror
retries; resumable completions follow the drain rule above. This retry behavior
does not provide the shared admission-policy store or atomic fleet handover in
the production rollout plan.

Mirror fetches accept HTTPS allowlisted hosts (including dot-separated subdomains),
reject URL credentials and redirects, and have a total timeout per fetch attempt:
`MIRROR_FETCH_TIMEOUT_SECS` (default 120, range 1–3600). The deadline includes the
body, so slow sources cannot retain mirror admission indefinitely. Size this
deadline for the intended mirror sizes and source throughput before rollout;
timeouts discard the temporary spool without beginning a provider write.

B2 control requests have a 120-second total deadline. Object GETs have a
120-second response-header deadline and a 30-second body stall deadline that
resets on progress, not a 120-second whole-object download budget. Verification
of a large object can therefore continue while bytes arrive. Busy resumable
session locks return 503 with `Retry-After: 5` rather than a server-error verdict.
GCS on-demand thumbnails preserve best-effort regeneration and return generated
JPEG bytes when persistence fails; that response does not claim a stored copy.
B2 and dual thumbnail failures remain explicit rather than claiming verified
persistence across destinations.

Database transport failures terminate the process so its supervisor restarts
connections and workers. Run the service under a restart supervisor (as in the
GKE Deployment); a standalone process requires equivalent restart behavior.
Pending state stays in PostgreSQL. Keep all credentials and provider-aware
readers needed by already accepted sessions and queued events during rollback.

An `upload_chunks` reservation with an empty object-reference list has an
unresolved provider outcome. Abort and expiry retain that session/reservation
rather than claiming terminal cleanup. HEAD 404 and an HTTP timeout do not prove
that an accepted PUT cannot commit later. Drain every writer, inspect exact
provider versions, and reconcile through the approved operator process before
clearing uncertain state. Do not delete rows merely to unblock a sweeper.
Multipart IDs are persisted before uploading parts; an uncertain initiation can
leave an empty provider-side unfinished upload and needs provider-side lifecycle
or operator reconciliation. No cleanup completion or erasure claim should be
made without the companion live drills.

Background finalization recovery only verifies existing intended copies and
queues them; it never creates a missing replica while erasure may be active.
An explicitly retried upload can repair a partial dual write. Queue processing
is idempotent by versioned job identity; owner registration is distinct from
expensive media-processing deduplication.
