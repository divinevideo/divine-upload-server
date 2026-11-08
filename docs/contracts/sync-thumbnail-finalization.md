# Direct dual upload thumbnail finalization

For new direct-upload intents with an accepted dual storage policy, a recognized
video MIME type, and nonempty input no larger than 100 MiB, original finalization
also requires a durable synchronous-thumbnail receipt. GCS-only uploads retain their existing
best-effort thumbnail behavior. Large videos, resumable uploads, mirrors,
migration and on-demand generation are outside this synchronous requirement.

Before original storage I/O, the uploader reserves this requirement in the
existing PostgreSQL intent. The reservation retains the accepted destination
mapping on retries. Extraction or thumbnail persistence failure returns a
retryable HTTP 503 with `Retry-After: 5`; the original intent stays pending.
Recovery never generates or repairs thumbnails in the background.

The thumbnail receipt records the actual JPEG SHA-256, length, and verified GCS
generation and B2 version, together with both exact original references. An
existing JPEG is reused and verified rather than compared to a newly encoded
image. Finalization checks this receipt under the intent row lock before
inserting the original processing event. Recovery uses the same check, including
after a crash between receipt persistence and enqueue. A changed original
version cannot use the earlier receipt. A new direct admission after completion,
or completed-intent readmission under a different destination policy, clears the receipt and rotates its reservation;
a delayed writer cannot attach its old receipt to that admission.

Pending-thumbnail intents remain visible to the pending-intent census. They do
not consume background original-read verification until a receipt exists. The
`MediaFinalizedV1` wire contract remains original finalization; this receipt is
not an assertion that an asynchronous artifact family has completed.

## Release and manual acceptance

Apply the additive `007_sync_thumbnail_barrier.sql` migration after `002`, or
allow normal outbox initialization to apply both. Migrations 005 and 006 belong
to independent broker/original-write work and are not dependencies of this
change. Verify every writer and recovery process runs the barrier-capable image
before admitting dual uploads: an older binary does not enforce this barrier.
Keep that image and the accepted provider mappings available during rollback.

On an isolated deployment, authenticate an owned direct dual upload and verify
both original versions and both JPEG versions by full-byte readback. Interrupt
B2 JPEG storage and repeat: expect 503, a visible pending intent and no new
original finalization event. Retry the same request with its accepted mappings;
expect successful finalization only after both JPEG copies verify. Restart after
receipt persistence but before enqueue and verify recovery finalizes only the
recorded original versions. Repeat GCS-only with failed thumbnail generation;
the original must retain its existing successful best-effort response.

This change does not provision B2 or database bindings, enable production dual
writes, replace the legacy processing hooks, supply an assignment worker, or
establish shared writer/erasure coordination. Those rollout requirements and
all-path/artifact acceptance remain in the
[production dual-write plan](../superpowers/plans/2026-11-06-dual-write-production-rollout.md).
