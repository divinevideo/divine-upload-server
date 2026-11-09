# Original-upload preparation v1

This boundary records upload authorization before a processing job exists.
It is the preparation stage of the original-writer integration, not permission
to write, deduplicate, promote, publish a completion marker, or release a gate.

## Configuration and HTTP behavior

`MEDIA_ORIGINAL_WRITE_ENABLED` is absent/`false` by default. Only literal `true`
enables preparation; other values fail startup. It requires
`COORDINATION_DATABASE_URL` and reuses the existing coordination transport and
schema. It needs no intake/worker/edge credential or processing endpoint.
Missing finalization delivery configuration is allowed while this preparation
boundary prevents new original finalization. No worker is started by it.

With the flag enabled:

- Direct upload authenticates the signed upload event, spools and hashes the
  request locally, records its authorization, and returns private, no-store
  HTTP 503 before canonical original, thumbnail, or outbox provider effects.
- Both staged and legacy resumable completion inspect PostgreSQL session state
  before any provider lookup, remote hash, promotion, or cleanup. A valid
  unfinished session is prepared and returns private, no-store 503. A legacy
  session additionally requires an existing durable pending upload intent that
  pins its GCS bucket; an old provider-only manifest or unknown mapping remains
  unavailable. No current-config bucket is substituted for missing history.
- A matching already committed resumable outbox receipt can replay HTTP 200
  using PostgreSQL alone. It creates no new write intent or provider request.
- Migration requests with no authenticated owner remain unavailable before
  fetching their source. No fabricated owner is accepted.

Authentication or immutable-input conflicts return 401 or 409 respectively;
unavailable responses do not expose raw database/provider errors. With the flag
off, existing upload behavior remains in use. Chunk admission/private staging
is not new original finalization authority, and this change does not fence every
writer or generic derivative route.

## Durable subject and identity

Migration 006 adds `upload_write_intents` and `upload_write_operations` to the
existing PostgreSQL authority. Each intent has a server-generated 128-bit nonce,
a unique request key and immutable accepted input. A direct request key binds
the signed event ID and server-computed SHA-256; a resumable key binds the
stored upload ID and revision. The stored owner comes from verified signed
authorization and must match the session owner. Authorization is rechecked
after transactional waits before committing preparation.

Accepted input contains ingress, owner, first authorization event ID, hash,
size, MIME hint, provider/bucket destinations, byte-evidence kind, and optional
session/revision. Retry preserves the first accepted identity and MIME hint;
changed owner, size, session, or destinations conflicts. MIME is an untrusted
hint, not a media probe or policy grant. Direct evidence is
`server_spooled_sha256`; resumable evidence is
`session_declared_not_verified`, pending later pinned-byte verification.

One server-generated attempt nonce is retained per intent. The existing
`media_storage_attempts` table distinguishes `producer` and `original` subjects:
producer subjects require a processing job and lease generation/revision;
original subjects require an upload intent and have no producer job/lease
fields. Database constraints reject cross-subject artifact operations. Existing
producer inspection and gate receipt APIs filter original subjects out before
deserializing producer authority.

There are at most four prepared target slots: original and optional JPEG for
each of at most two accepted provider/bucket destinations. These targets have
no invented provider version and no issued state. Optional thumbnail slots are
inventory only, not blanket derivative authorization. The plan and attempt
remain durable across retries; OW1 has no claim, transport issuance, dispatch,
completion, cancellation, or release transition.

## Remaining execution prerequisites

Execution needs a trusted original-write policy subject and shared parent
exclusion across every original writer, dedupe, and promotion path. It must
check tombstone/quarantine/vanish/reference policy from trusted authority,
verify pinned source/output bytes and metadata, journal one-shot effects
before transport, and seal the actor before positively settling them. A job
lease, caller `gate_held` boolean, MIME hint, current HEAD observation, or
missing blob cannot grant that authority.

Timeouts and uncertain provider outcomes must remain durable. Recovery may
settle an outcome only with positive evidence for the exact own effect/version
and operation identity, at-most-one transport issuance, and irreversible actor
sealing that proves no outstanding effects. Lease expiry or object absence is
not settlement or permission to release exclusion. Those transitions are not
implemented here. Keep OriginalWrite claims, producer dispatch and runtime
activation unavailable until these prerequisites exist.

## Verification

The source tests use isolated schemas on an explicitly disposable PostgreSQL
database and loopback HTTP providers. They cover concurrent identity reuse,
signed ownership, a two-connection authorization-expiry/row-lock race, accepted
mapping conflicts, producer/original subject separation, direct and both
resumable completion hooks with zero provider calls, receipt replay, migration
refusal, and GCS/B2/dual preparation without a processing endpoint. They prove
preparation behavior, not provider-backed completion or B2-only acceptance.
