# Producer preparation journal v1

This first bounded stage supplies durable attempt identity and immutable
artifact planning in the upload service's existing PostgreSQL database.
It performs no provider writes, starts no worker and grants no parent exclusion
or storage dispatch. A planned checksum is an expectation, not proof that bytes
exist or were verified.

## Configuration and compatibility

**MEDIA_PROCESSING_JOURNAL_ENABLED** defaults to absent/false. Only exact true
enables journal routes, and requires MEDIA_FINALIZED_INBOX_ENABLED=true.
Other values fail startup. Existing separate intake and worker credentials
are reused; no new credential or service is introduced.

Migration 004_producer_journal.sql creates media_storage_attempts and
media_storage_operations when the optional inbox initializes. It does not
require the producer outbox to be configured or dispatched. Initialization
is serialized by a transaction advisory lock.

**Enabling the journal also prevents new completions through the existing
/internal/media-processing/complete endpoint on that instance.** An exact
already-committed completion can still be replayed after lease expiry.
A caller cannot skip the missing producer gate/journal stages, even if it has
not begun an attempt. With the new flag off, existing completion behavior
remains unchanged. Keep runtime enablement off until integration and rollout
are authorized.

## Private API

All routes use POST and the existing worker token. Authentication precedes body
consumption; intake credentials cannot access worker routes. Requests retain
the 32 KiB body bound and one ten-second body/database deadline. SQL
statement/lock timeouts remain five/three seconds. Responses are private,
no-store; errors omit raw database/provider details. No automatic HTTP retries.

The common lease object is the existing server-issued
{processing_job_id,lease_generation,input_revision}.

| Route under /internal/media-processing/attempts | Request | Result |
| --- | --- | --- |
| /begin | {lease} | {attempt}, including a durable nonce |
| /prepare | {lease,attempt_id,artifacts} | {attempt} with immutable expected inventory |
| /inspect | {attempt_id} | {attempt} or {attempt:null}, including historical attempts |
| /dispatch | {lease,attempt_id,filename} | 503 producer parent gate unavailable; never dispatches |

Malformed input returns 400; stale fences or conflicting replay return 409.
Disabled routes return 503. Unavailable persistence cannot grant work.
Dispatch validates the live fence and planned attempt, but never changes an
operation out of prepared or returns a write permit.

## Attempt identity and fencing

Begin commits a cryptographically random 128-bit lowercase hexadecimal
attempt_id before returning it. The unique job/generation/revision binding
makes concurrent/exact replay return the same persisted nonce. A lost response
cannot create a different revision namespace. Mutation checks use the existing
live lease, hard deadline, generation, input revision and running state, with
the job row locked before journal changes.

The attempt includes exact source, source SHA/size, pipeline, content_type_hint,
bound generation/revision, phase and ordered artifacts. Source MIME remains
a producer/owner hint, not a verified probe result. Preparing a plan approves
neither the hinted type nor current-source selection.

An allocated attempt has no artifact rows. A planned attempt has all seven rows
committed atomically. Exact normalized prepare replay returns the same inventory;
different replay is rejected. New destinations or lease expiry prevent old
begin/prepare/dispatch, but never delete historical nonce or plan records.
Inspect reads identity, phase and complete ordered inventory in one SQL
snapshot; it does not assert that the attempt is current.

Because this stage cannot dispatch any external effect, another assignment
can allocate a planning nonce after the prior reservation expires. Future
dispatch/uncertainty support must prevent takeover while an old actor or its
provider effects remain unsettled. Lease expiry is not provider cancellation.

## Bounded artifact expectations

Every request artifact has exactly filename, sha256, size and content_type.
Unknown fields, foreign targets, duplicate or missing names are rejected.
The exact seven filenames, in returned dependency order, are:

1. stream_720p.ts
2. stream_720p.mp4
3. stream_480p.ts
4. stream_480p.mp4
5. stream_720p.m3u8
6. stream_480p.m3u8
7. master.m3u8

Checksums are full lowercase SHA-256 of intended artifact bytes, distinct
from parent identity by meaning. Sizes must be positive and at most 32 MiB
each, 64 MiB for the family, and 64 KiB for each playlist. MIME is exactly
video/mp2t, video/mp4 or application/vnd.apple.mpegurl, respectively.

The service constructs each target using the assigned source provider/bucket
and {parent}/revisions/v1/{attempt_id}/{filename}. It is a proposed target,
not an ObjectRef: there is no invented provider version. A future executing
adapter must separately validate configured provider/bucket and exact source.
Expected metadata contains only canonical source-sha256, derivative=hls and
hls-filename. Caller owner/provenance fields are not copied. Optional verified
provenance and legacy alias normalization remain integration work.

No completion marker is written or accepted as new completion in journal scope.
The agreed future marker key is exactly
**_private/artifact-families/v1/{parent}/complete.json**, with the actual returned
version pinned. It must follow verified immutable outputs and HLS byte closure;
its all-version erasure inventory is separate from parent-prefix cleanup.

## Remaining production stages

The real authenticated ParentGate integration, uncached policy/current-source
checks, one-shot provider dispatch intents, pinned output readback and metadata
verification, marker construction/publication, atomic journal-backed completion,
uncertain-write recovery and original-writer hooks remain necessary.

Original writes/dedupe/promotion must join the same exclusion as processing and
erasure. The uploader's existing canonical PostgreSQL session lock does not
supply that cross-service authority. Attempts and uncertain outcomes must use
shared cloud durability; Cloud Run temporary files, local JSON and SQLite are
not producer recovery authority. Preserve accepted provider/session mappings
and permanent/uncertain gates.

Tests use disposable PostgreSQL and actual Axum requests. Missing
TEST_COORDINATION_DATABASE_URL causes database tests to announce skips and is
not database proof. Source checks do not establish provider-backed B2-only
processing, erasure or matched GCS/B2 native performance acceptance.
