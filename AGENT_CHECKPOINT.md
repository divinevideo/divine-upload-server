# B2 upload support checkpoint

## Objective

Support GCS, B2, and verified dual writes in the upload/media-processing server.
Legacy catalogue migration and CDN cutover belong to companion work.

## Current status

[VERIFIED] Implementation is locally validated and ready for code review.
Default mode is GCS. No production credentials, configuration, or cloud media
were changed. Independent delegated review found no remaining blocking findings
in the implementation tree. PR publication and CI inspection follow this commit.

## Branch / commit

[VERIFIED] Harness-owned branch `elastic-shrimp`, based on `bf70414`.
This checkpoint accompanies the implementation commit; use git for its final SHA.

## Validation

[VERIFIED] `cargo test --all` with `TEST_COORDINATION_DATABASE_URL` pointing to a
disposable PostgreSQL database: 94 passed, zero failed or ignored.
[VERIFIED] Python unittest discovery: 16 passed.
[VERIFIED] Formatting, Clippy with warnings denied, actionlint and whitespace checks passed.
[VERIFIED] `cargo build` passed. Synthetic B2-only startup without Google
credentials served HTTP 200; disconnecting only its database connections caused
exit 1 for supervisor restart. No cloud object request was made by that smoke test.
[UNVERIFIED] Container build locally: Docker daemon unavailable. Dockerfile was
updated for the repository toolchain and embedded SQL/runtime dependencies.

## Current mental model

- Dual success means both exact object versions have been verified. Retrying an
  explicit upload can repair one missing copy; automatic recovery only verifies
  existing copies and queues processing. It does not recreate missing media.
- PostgreSQL coordinates canonical writes, resumable sessions, processing intents
  and the outbox. Losing a connection terminates production processes so the
  supervisor restores workers and releases session locks.
- Sessions and derivative jobs keep their provider/bucket policy through mode
  switches. Queue retry time is not finalization order.
- Processing events carry source/replica versions and per-owner registration
  identity; the consumer must separately deduplicate expensive processing.
- An unknown provider write outcome is retained rather than treated as absence.
  Operator reconciliation is intentional; do not clear rows to unblock cleanup.
- Local disk and admission are bounded by MAX_UPLOAD_BYTES and
  MAX_CONCURRENT_UPLOADS. Large derivatives use the durable companion pipeline.

## Deployment dependencies

[UNVERIFIED] Live B2 account behavior and actual consumer integration still need
bounded cloud drills. The configured HTTPS finalization endpoint must implement
`docs/contracts/media-finalized-v1.md`; the old hash-only transcoder does not.
Companion policy/erasure exclusion and derivative handling must be ready before
B2/dual activation. Keep B2-capable readers/workers and accepted-session
credentials during rollback. Follow `docs/deployment.md`.

## Next steps

1. Inspect the PR's required CI checks, including database-backed tests.
2. Hand the reviewed code to the mapped repository reviewers; merging and
   production activation require their governing workflow and authorization.
3. Coordinate the versioned consumer contract and activation drills with the
   Blossom and infrastructure owners. Do not perform legacy migration here.
