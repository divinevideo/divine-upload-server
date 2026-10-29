"""Request a GitOps image update for a checked push at the current main tip."""

import json
import os
import re
import urllib.request


SOURCE_REPO = "divinevideo/divine-upload-server"
TARGET_REPO = "divinevideo/divine-iac-coreconfig"
IMAGE = "us-central1-docker.pkg.dev/dv-platform-prod/containers-production/divine-upload-server"


def make_payload(event, ref, repository, sha, digest):
    if (event, ref, repository) != ("push", "refs/heads/main", SOURCE_REPO):
        raise ValueError("Promotion requires a push to this repository's main branch")
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("Promotion requires a full source commit SHA")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("Promotion requires the published image digest")
    return {
        "event_type": "image-deploy",
        "client_payload": {
            "application": "divine-upload-server",
            "environments": ["production"],
            "images": [{"name": "divine-upload-server", "tag": sha, "digest": digest}],
            "internal": True,
            "hotfix": False,
            "auto_promote": False,
            "source_repo": SOURCE_REPO,
            "source_sha": sha,
            "source_ref": "main",
            "image_digest": digest,
        },
    }


def api(path, token, payload=None):
    request = urllib.request.Request(
        "https://api.github.com/repos/" + path,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if payload is None:
            return json.load(response)
        if response.status != 204:
            raise RuntimeError("Dispatch was not accepted")


def main():
    sha = os.environ["GITHUB_SHA"]
    digest = os.environ["IMAGE_DIGEST"]
    payload = make_payload(
        os.environ["GITHUB_EVENT_NAME"], os.environ["GITHUB_REF"],
        os.environ["GITHUB_REPOSITORY"], sha, digest,
    )
    # An old workflow rerun must not request an older image over a newer main.
    tip = api(SOURCE_REPO + "/commits/main", os.environ["GITHUB_TOKEN"])
    if tip["sha"] != sha:
        raise RuntimeError("Source is no longer the main tip; rerun the current main push")
    api(TARGET_REPO + "/dispatches", os.environ["DEPLOY_TOKEN"], payload)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
        summary.write(
            f"### GitOps update requested\n\nSource: `{sha}`\n\n"
            f"Image: `{IMAGE}@{digest}`\n\n"
            "Dispatch accepted; this is not rollout completion. Follow the receiver's "
            "image-deploy run, production approval and post-merge sync verification.\n"
        )


if __name__ == "__main__":
    main()
