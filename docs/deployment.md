# Image publication and production promotion

## Enable only after the receiver is ready

The sender targets only the existing upload-server **production** overlay in
`divinevideo/divine-iac-coreconfig`. It adds no environment and does not change
the production approval policy. Before enabling it, a Platform maintainer must:

1. Merge the upload-server companion receiver changes: its application/image
   allowlist entry and upload-server-specific post-merge sync/verification path.
   The existing production overlay must continue to use
   `us-central1-docker.pkg.dev/dv-platform-prod/containers-production/divine-upload-server`.
2. Verify the public `https://upload.divine.video/` route actually reaches that
   workload and check the running image, not just the desired-state overlay.
3. Confirm the receiver's production approval rule remains enforced. The sender
   uses `internal: true`, **never** `hotfix` or `auto_promote`. The receiver may
   enable auto-merge on its PR; that is distinct from bypassing required approval.
4. Make `APP_ID` and `APP_PRIVATE_KEY` available to this repository's Actions
   runner through the approved credential-management process. These are the
   GitHub App credentials used to mint a short-lived token, not GCP credentials.
   The App installation must cover **only the required receiver repository**
   (`divine-iac-coreconfig`) with Contents write permission for
   `repository_dispatch` (and implicit Metadata read). The token request scopes
   itself to that repository and permission. The sender's ordinary `GITHUB_TOKEN`
   remains Contents read and is used only to verify the source main tip. The
   receiver keeps its own existing credentials for creating promotion PRs.
5. Set the repository Actions variable `GITOPS_DEPLOY_ENABLED` to `true` only
   after those prerequisites and the rollout authorization are in place.

Neither merging sender code nor a successful PR check provisions credentials,
enables that variable, authorizes a rollout, or proves the receiver is ready.
Missing credentials fail the enabled promotion job; there is no silent fallback
to a laptop deployment or production bypass.

## What runs automatically

Only a **push to main** can publish and request promotion. The promotion job
depends on successful Verify and Publish Production Image jobs. Pull requests,
other branches, and `workflow_dispatch` do not publish or deploy. The helper also
checks the event, source repository, ref, full SHA and digest, then reads the
current source `main` through GitHub before dispatching. A rerun of a superseded
main commit fails instead of requesting an older version.

The image tag is the full source SHA; the receiver's existing ordinary-image
contract writes that tag into the overlay. The exact build digest is included
as `image_digest` provenance in the dispatch and in the build and promotion job
summaries. It is **not** an instruction to pin a digest in the manifest: the
current receiver updates `newTag`. Retain the build summary and verify the
running digest against it after promotion. Do not treat a mutable tag alone as
proof of the image being served.

## Controlled rollout and evidence

Perform this only with separate rollout approval and existing read access.
Record the source workflow run URL, source SHA, image digest, receiver
`image-deploy` run, generated production PR, its approval/merge, and the
`post-merge-sync` run. Before approving the generated PR, confirm the SHA is the
intended release, the registry tag resolves to the recorded digest, and the PR
changes only the intended production image. A newer main push after dispatch
does not recall an already-created PR; compare pending promotion PRs and do not
approve an older request over a newer release.

After the approved promotion merges, check ArgoCD sync and health and inspect
the Deployment and every serving pod's image ID. For existing Connect Gateway
access to the production cluster, read-only examples are:

```sh
kubectl --context=connectgateway_dv-platform-prod_us-central1_gke-production-membership \
  -n media get deployment divine-upload-server \
  -o jsonpath='{.spec.template.spec.containers[*].image}{"\n"}{.status.availableReplicas}{"\n"}'
kubectl --context=connectgateway_dv-platform-prod_us-central1_gke-production-membership \
  -n media get pods -l app=divine-upload-server \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\t"}{.status.containerStatuses[*].imageID}{"\n"}{end}'
curl --fail --show-error https://upload.divine.video/
```

Check that the image tag equals the intended full SHA, all desired replicas are
ready, and **all** serving pod digests equal the recorded build digest. Public
landing-page success alone establishes neither readiness nor image identity.
Use an authorized synthetic upload smoke test for application-level verification;
do not use private media or real creator identifiers in evidence. Do not call the
rollout complete until these checks succeed. No controlled rollout is performed
by the sender PR's validation.

## Recovery, reruns and rollback

- If Verify or publication fails, no promotion job runs. Fix the failing step
  and rerun the failed job/dependent jobs on the **current main push** in Actions.
- If token creation or dispatch fails, inspect the receiver's run list and open
  image-update PRs before retrying: a lost response can still have created a
  request. Retry only after establishing that no matching SHA request exists.
  A successful dispatch can still fail receiver validation, so inspect its run.
- For a receiver rejection, repair the receiver prerequisite through its normal
  review path, then rerun only the current-main promotion job. Do not weaken
  allowlists, registry checks or production approval to recover.
- To repeat verification manually, use `workflow_dispatch` on `main`; it runs
  Verify only. It intentionally cannot force publication or promotion. Re-running
  the original current-main **push** is the supported promotion recovery path;
  arbitrary refs and stale main pushes remain rejected.
- For rollback, choose a previously verified SHA and digest in the production
  registry, open a receiver PR reverting the upload-server overlay to that tag,
  obtain normal production approval, and follow post-merge sync and the same
  serving-image/health checks. Do not rerun an old source workflow to roll back,
  patch the live Deployment, or deploy `latest`. Coordinate pending newer
  promotion PRs so they do not immediately undo the approved rollback.

Credential changes, enabling/disabling the dispatch variable, merging promotion
PRs and live rollback are operational actions with their own authorization;
these instructions do not grant it.
