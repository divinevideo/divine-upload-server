# Media finalization consumer contract, version 1

The upload server posts this internal contract to the operator-configured
`MEDIA_FINALIZED_URL`. It authenticates with `Authorization: Bearer` using the
`MEDIA_FINALIZED_TOKEN` secret. The consumer endpoint must use HTTPS; the client
rejects redirects and never forwards its token to another endpoint.

This is a new companion integration contract. The existing Blossom transcoder,
transcriber, and GCS notification consumers must not be assumed to implement it.
Deploy a compatible consumer before enabling a B2 upload mode.

## Request

The JSON body has `schema_version: 1`, `job_id`, `source`, `replicas`, `sha256`,
`size`, `content_type`, `owner`, and `pipeline_version: 1`. `owner` is always null
for the unauthenticated `/migrate` route when the outbox is enabled, regardless of
the request's owner declaration. It must not grant ownership. A verified
caller/upload authorization contract is required before mirror events can claim
an owner; legacy metadata is not registration authority.
`replicas` is omitted when there are no additional verified copies.

`source` and every replica use Blossom's internal object-reference shape:
`provider` is `gcs` or `b2`; `bucket` and `key` name private storage; `version` is
the exact verified provider generation/version. A version is never an ETag or a
signed URL. The key matches the complete lowercase original SHA-256. No storage
credentials are included in the request. Consumers use their own scoped access.

In dual-write mode the event describes both verified destinations but represents
one finalization event. The first destination is the currently authoritative source;
replicas do not request duplicate transcoding or transcription. The consumer
must retain all storage identities when generating or scheduling derivatives so
outputs follow the selected write policy rather than silently defaulting to GCS.

`job_id` is a deterministic SHA-256 over the pipeline version, owner, and versioned
source/replica identities. The same ID is sent in the `Idempotency-Key` header.
The consumer must deduplicate retries durably. Ownership registration is per
owner, so different owners receive distinct finalization events. Expensive media
processing should separately deduplicate by source version and pipeline version. A new pipeline version is the
mechanism for deliberately requesting another processing run.

The consumer owns idempotent fan-out and must check current Blossom moderation,
quarantine, ownership, tombstones, and erasure intent before registration or
processing. Storage finalization grants no publication or delivery approval.
Status callbacks must carry job/source version identity so delayed work cannot
overwrite newer state. The consumer must not publish the private object reference
in public descriptors; existing CDN/hash URLs remain canonical.

## Durable acknowledgement

A successful response must have a 2xx status and a JSON body containing exactly
the corresponding values `schema_version: 1`, `job_id: <request job ID>`, and
`durable: true`. Extra response fields are ignored. This acknowledgement means
the consumer has persisted the work and owns subsequent retries; it does not
mean every derivative has already finished.

A plain 200/202, empty body, mismatched job ID, or `durable: false` leaves the job
pending. Responses are capped at 8 KiB, including chunked bodies. Calls have a
10-second connect timeout and 30-second total timeout. Error bodies and signed
provider URLs are never logged.

## Producer durability and recovery

The producer first persists an intent containing the expected hash, length,
owner, content type, write mode and exact destination bucket mapping. It only
marks that intent complete in the transaction that records a processing event
for all verified destinations. A retry always attempts this transaction even if
the object already existed, repairing a lost finalization acknowledgement.
The first intent's content type is retained when the same owner retries the same
bytes with another MIME declaration; this does not permanently reject that hash.
Direct and mirror retries resolve the unfinished intent's recorded provider mode
and required destinations before storage I/O, independently of the current
new-upload mode. Required adapters/buckets must still be configured. Resumable
sessions instead require the intent to match their already recorded policy.
The finalization transaction checks the exact destination set and ordering
against the locked intent: GCS only, B2 only, or GCS then B2 for dual. Missing,
extra, duplicate, reordered or wrong-bucket references cannot complete the intent.
Replaying a completed resumable session may acknowledge an exact event already
persisted in the outbox without changing the current intent or refreshing that
event's finalization order. This preserves successful session replay after a
newer upload changes policy, without completing newer pending work.

Outbox workers claim jobs through PostgreSQL `FOR UPDATE SKIP LOCKED`, with a
60-second lease. Delivery is at least once: a crash after consumer acceptance but
before producer acknowledgement causes a retry with the same job ID. Consumer
idempotence is required. Transient failures use exponential backoff capped at
24 hours; failures do not delete queued work. Logs contain aggregate outcomes,
not media identifiers or credentials.

A bounded reconciliation pass can queue an interrupted finalization only after
independently reading and checking the full SHA-256 and length of every intended
destination. It uses the intent's pinned mode and destination mapping. Changed
configuration never retargets an unresolved intent to another bucket. Background
reconciliation first checks metadata for all destinations; a missing replica
leaves the intent pending without downloading the surviving copy on every pass.
Background
reconciliation never copies or repairs media from a surviving replica: that could
race erasure. A partial dual write remains pending for an explicit client retry
or operator investigation. Automatic copy repair requires the shared Blossom
copy/erasure exclusion contract before activation.

Run the database-backed tests with `TEST_COORDINATION_DATABASE_URL` pointing to
an explicitly disposable PostgreSQL database. Production database credentials
must never be used for tests.
