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
   than creating a new credential. Never print or commit values.
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
