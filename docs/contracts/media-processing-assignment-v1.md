# Media processing inbox and assignment contract v1

This is a default-off durable intake and reservation boundary. It does not start
processing workers or complete the B2 processing migration. Ownership
registration, moderation, dispatch, verified artifact production and erasure
exclusion remain activation requirements. Keep the uploader outbox disabled until
these side effects and crash recovery are proven.

## Placement and configuration

The existing upload service owns this API and uses its existing PostgreSQL
coordination transport and `COORDINATION_DATABASE_URL`. It adds the tables in
`database/migrations/003_finalization_inbox.sql`; no local runtime database or
new provider SDK is introduced. Schema initialization is protected by a
transaction advisory lock.

`MEDIA_FINALIZED_INBOX_ENABLED` is absent/`false` by default; only exact `true`
enables intake. Other values fail startup. Enablement requires separate,
16–1024 byte printable `MEDIA_FINALIZED_INBOX_TOKEN` and
`MEDIA_PROCESSING_WORKER_TOKEN` values
and the database binding. The disabled API returns 503 and never connects to the
database. This does not configure `MEDIA_FINALIZED_URL` or enable the producer's
outbox automatically.

Every operation uses POST, an authenticated bearer token and a private no-store
response. Intake and worker tokens cannot be interchanged. Authentication
precedes body consumption. Bodies are capped at 32 KiB and one ten-second
deadline covers body reading and database work together. SQL statement/lock
timeouts are five/three seconds. There are no automatic HTTP retries.

## Durable intake

`POST /internal/media-finalized` accepts the existing
[media-finalized-v1](media-finalized-v1.md) event and requires its `job_id` in
`Idempotency-Key`. It verifies the producer's existing deterministic event ID,
supported schema, pinned source/replica references, hash, size and bounded MIME
and owner fields before database writes.

One transaction persists:

- The complete owner-specific inbox event, with registration still pending.
- A separate source-version/pipeline processing job.
- Every source/replica destination named by that event.

Only a committed transaction returns the matching
`{"schema_version":1,"job_id":"...","durable":true}` acknowledgement. A lost
response can be retried with the same event. Conflicting replay data returns
409 and preserves the accepted work. Different owners retain distinct inbox
events while sharing expensive processing for the same source and pipeline.
MIME declarations remain producer hints; actual source MIME/media must be
verified by the version-pinned adapter/probe before routing side effects.

The destination set is bounded to sixteen pinned references. An event that
exceeds this bound is rejected atomically, leaving the producer's durable outbox
responsible for the event. Nothing is silently dropped or retargeted.

## Processing identity

`processing_job_id` is the lowercase SHA-256 of the UTF-8 bytes emitted by Rust
`serde_json::to_vec` for this tuple:

```text
("divine-media-processing-v1", pipeline_version, source_object_ref)
```

`ObjectRef` serializes fields in the existing struct order: `provider`, `bucket`,
`key`, `version`. Versions are opaque provider generations/version IDs, never
ETags or URLs. Owner and replicas are excluded from this processing identity;
their original event records and complete destination set are retained.

Synthetic golden encoding, with no whitespace or trailing newline:

```json
["divine-media-processing-v1",1,{"provider":"b2","bucket":"synthetic-private","key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","version":"version-1"}]
```

Expected SHA-256:

```text
31dd6874aa402c20b9c8ce3f9f5f2d01d5d9f95b545035239e90140130e6457b
```

Consumers should retain the server-issued ID and use the golden vector for any
independent implementation. Do not invent a different JSON canonicalization.

## Assignment and fencing

`POST /internal/media-processing/claim`, with the worker token and `{}`, claims
at most one job using PostgreSQL row locking and `SKIP LOCKED`. It returns
`{"assignment":null}` or an assignment with exactly these fields:

```text
schema_version: 1
processing_job_id: lowercase SHA-256
lease_generation: monotonically increasing PostgreSQL integer
input_revision: destination-set revision integer
source: exact ObjectRef
destinations: all required pinned ObjectRefs, in stable identity order
sha256, size, content_type, pipeline_version
lease_expires_at, attempt_deadline: Unix seconds
```

Each attempt has a 60-second lease and a hard 15-minute deadline. Renew using
`POST /internal/media-processing/renew` with
`{processing_job_id,lease_generation,input_revision}`. Renewal requires the
current, unexpired generation/revision and never extends the hard deadline.
Workers must renew well before expiry and stop effects when authority is lost.

Adding a genuinely new destination increments `input_revision`. It prevents an
old snapshot from renewing or completing, while retaining its unexpired lease
to avoid immediate overlapping attempts. Repeated owner events that introduce
no new destination do not change the revision or reset the attempt budget.

Three abandoned attempts for one input revision exhaust its budget. A bounded
64-row sweep skips locked rows and records `blocked` with the fixed reason
`attempt_budget_exhausted`. Work and inbox events remain durable. A new
destination revision has a fresh bounded budget; the lease generation never
resets. Operator recovery of unchanged blocked work requires an explicit
head-matched database action, diagnosis and renewed policy authority, not an
automatic retry loop. Preserve all external permanent and uncertain gates.

The lease is a reservation, **not** approval to publish, register ownership or
copy media. A database fence alone cannot stop a stale process's storage writes.
Before activation, workers must enforce current policy and shared copy/erasure
exclusion around every side effect, use immutable attempt/revision outputs, and
retain uncertain writes rather than clearing another actor's gate by TTL.

## Completion and replica reads

`POST /internal/media-processing/complete` accepts the worker token and:

```json
{"lease":{"processing_job_id":"...","lease_generation":1,"input_revision":1},"manifest":{"provider":"b2","bucket":"...","key":"_private/artifact-families/v1/<parent>/complete.json","version":"..."}}
```

The manifest must be a version-pinned ObjectRef under the private control prefix
for the job's actual parent hash. Never put it under the public hash/HLS route.
Completion requires the still-live generation/revision and records the pinned
reference durably. Exact receipt replay succeeds after a lost response; a stale
generation, superseded input or different manifest cannot complete that receipt.

The API validates reference structure and fences; it does **not** fetch or
independently verify the marker's contents. Artifact production, metadata
readback, checksum/closure verification and policy exclusion are mandatory
producer/replica responsibilities. Marker publication remains unimplemented
here. Private marker versions also need explicit erasure inventory coverage.

`POST /internal/media-processing/lookup`, with the worker token and
`{"processing_job_id":"..."}`, returns `{"receipt":null}` or a consistent
single-statement snapshot with `schema_version`, processing ID, input revision,
lease generation, `state`, source, destinations, SHA/size/type/pipeline and
`manifest`. A completed receipt remains completed after lease expiry; a live
processing lease is not required to consume an already committed result.
New destination revisions remove that result and change the revision.

Replica acceptance must require `state=completed`, a pinned nonnull manifest,
and the expected assignment/source/pipeline identity. Read the exact marker
version, never discover a latest mutable key. Recheck the receipt/fences before
publishing readiness, alongside the replica's own policy and exclusion checks.
Missing, pending, blocked, superseded or unavailable evidence stays pending.
This job ID and arrival order do not establish the globally current original
source; that must come from the selected verified original authority.

The receipt's completed state concerns the recorded artifact result. It does
not assert that owner registration, moderation, subtitles or every other
finalization side effect is complete. No private source/bucket/job identity may
be copied into public descriptors or playback metadata.

## Verification and rollout

Use an explicitly disposable PostgreSQL database, never production credentials:

```sh
TEST_COORDINATION_DATABASE_URL=<disposable-url> cargo test --locked inbox::
cargo test --locked --all
cargo clippy --locked --all-targets -- -D warnings
python3 -m unittest discover -s tests -p 'test_*.py'
```

Database cases announce a skip when that binding is absent. A passing run with
no database binding is not PostgreSQL durability proof. CI supplies its existing
disposable PostgreSQL service. No deployment, consumer URL, token, storage flag,
worker schedule or production database was changed by this source slice.
