# Optional GCS backup candidate validation

This plan validates the behavior changed in [PR48](https://github.com/divinevideo/divine-upload-server/pull/48).
It describes candidate checks, not evidence of a deployed configuration. Use
synthetic media and an isolated candidate; deployment and configuration changes
require the owning operator's authorization.

1. With B2-primary admissions and backups enabled, upload a short video directly
   and through a resumable session. Verify identical original bytes, pinned B2
   then GCS references, working playback/transcription, and thumbnail retrieval.
2. Disable media backups on the candidate. Upload fresh synthetic content by
   both routes. Verify no canonical GCS media backup is created, while the
   small GCS admission/session records and staging retain their existing role.
   Confirm playback and processors can fetch B2-only originals. Repeat each
   upload: no original rewrite or repeated new-upload moderation submission is
   expected after an ordinary successful completion.
3. Inject a request failure after B2 verification but before final completion.
   Retry identical bytes: the request must take a fresh admission fence,
   complete without rewriting B2, and preserve a best-effort hook opportunity.
   Separately lose the completion-record response: exact payload readback must
   resolve a landed write. A delayed old CAS must fail after a newer fence.
   A crash after completion but before hook submission is still a known
   best-effort limitation, not an exactly-once delivery guarantee.
4. Re-enable backups on the same release. Fetch a thumbnail generated while
   backups were off: it must return the exact recorded B2 JPEG without media
   writes. Retry its original upload: repair only the missing backup, preserving
   B2 bytes/owner/MIME, and do not label the original newly stored or migrated.
   Check the same thumbnail after switching new admissions back to GCS.
5. Finish an older GCS or dual resumable session whose hash already has a
   completed B2-only record. Completion must verify its staged bytes and repair
   the required missing GCS copy through the primary admission fence. A plain
   B2 session must complete using only B2. Lost provider acknowledgements with
   an `issued` slot must remain pending for explicit reconciliation.
6. Retry a recorded B2-primary outbox intent with the current backup switch off.
   Originals and any required thumbnail must still satisfy its recorded two-copy
   contract. The bounded reconciler must wait when either required copy is
   missing and recover once both settled versions exist, without issuing media
   writes itself. A recorded plain-B2 intent needs only its B2 copy.
7. Corrupt a candidate derivative's bytes, recorded version, owner or MIME.
   Cache reads must fail closed, including same-size byte corruption. A different
   latest B2 version must not change which recorded version is read.

The completion marker uses an existing opaque string field, so the old strict
v1 JSON schema remains readable. That compatibility does not make old images
capable of serving all B2-only media. Keep the compatible reader release while
rolling back the flag, or restore required backups before an image downgrade.
Do not infer that a flag change has backfilled content that was never retried.

Automated coverage uses loopback GCS/B2 fixtures and PostgreSQL. Run:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
python3 -m unittest discover -s tests -p 'test_*.py'
```

Set `TEST_COORDINATION_DATABASE_URL` to a disposable local PostgreSQL instance
to execute outbox and historical-session tests; those tests return early when
it is absent. No real media or production credentials are needed.
