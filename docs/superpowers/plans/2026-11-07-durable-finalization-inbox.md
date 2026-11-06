# Durable media finalization inbox and assignments

> For agentic workers: use superpowers:executing-plans with TDD for each task.

**Goal:** Persist the existing media-finalized-v1 contract before acknowledging
delivery, and provide version-bound processing assignments that stale workers
cannot complete.

**Architecture:** Extend the upload server's existing PostgreSQL coordination,
ObjectRef and event identity implementation. Keep each owner-specific event,
deduplicate expensive processing by exact source version and pipeline, and retain
all requested destinations. A lease is reservation authority only: it grants no
publication, ownership, moderation approval or permission to bypass erasure.

**Tech stack:** Existing Rust/Axum, tokio-postgres, PostgreSQL and HMAC/SHA-256.

## Scope and safety boundaries

- Default off; explicit enablement requires database and separate intake/worker
  tokens. No storage adapter, provider operation or automatic worker activation.
- Authenticate and bound input before parsing. Verify the existing deterministic
  event ID and Idempotency-Key. A matching durable ACK follows one committed
  transaction containing the inbox event, processing work and destination set.
- Reject conflicting replay data rather than retarget an accepted job.
- Claims increment a PostgreSQL lease generation. Destination changes increment
  an input revision and invalidate completion of an older snapshot. Renewals and
  completion require matching generation/revision and an unexpired lease.
- Bound an attempt with a hard deadline and an explicit retry budget; exhausted
  work remains visibly blocked, never deleted or retried forever.
- Completion records only a private version-pinned marker ObjectRef. Replica
  acceptance additionally requires the selected original, pipeline and current
  policy/gate checks. Arrival order and UUIDs never define the latest source.
- Ownership registration, moderation, dispatch adapters and processing side
  effects remain unimplemented activation prerequisites. Keep the uploader
  outbox disabled until those paths and recovery are proven.

## Implementation sequence

- [x] TDD strict event validation and source/pipeline processing identity.
- [x] TDD PostgreSQL atomic acceptance, exact replay and conflicting payloads.
- [x] TDD destination-set revision, competing claims, lease renewal/expiry,
      stale completion, duplicate completion and bounded exhausted recovery.
- [x] TDD opt-in authenticated bounded HTTP intake and assignment operations.
- [ ] Run the real disposable PostgreSQL tests, full Rust/Python suite and lint;
      document the exact assignment schema and remaining activation gates.
- [ ] Publish the isolated source PR and send the frozen contract to the
      transcoder/replication owners. No provider deployment or configuration.
