"""Request a GitOps image update for a checked push at the current main tip."""

import argparse
import json
import os
import re
import urllib.error
import urllib.request


SOURCE_REPO = "divinevideo/divine-upload-server"
TARGET_REPO = "divinevideo/divine-iac-coreconfig"


def validate_source(event, ref, repository, sha):
    if (event, ref, repository) != ("push", "refs/heads/main", SOURCE_REPO):
        raise ValueError("Promotion requires a push to this repository's main branch")
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("Promotion requires a full source commit SHA")


def make_payload(event, ref, repository, sha, digest):
    validate_source(event, ref, repository, sha)
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
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if payload is None:
                return json.load(response)
            if response.status != 204:
                raise RuntimeError(f"Dispatch returned HTTP {response.status}; expected 204")
    except urllib.error.HTTPError as error:
        # Response bodies can echo input. Report actionable status without logging them.
        reason = {
            401: "authentication rejected; check the configured App credentials",
            403: "access denied; check App permissions and installation scope",
            404: "repository or ref not found or inaccessible to this token",
            422: "request validation failed; check the payload and receiver contract",
            429: "rate limited; inspect the run before retrying",
        }.get(error.code, "GitHub request failed; inspect the run before retrying")
        raise RuntimeError(f"GitHub HTTP {error.code}: {reason}") from None


def main(check_only=False):
    sha = os.environ["GITHUB_SHA"]
    source = (os.environ["GITHUB_EVENT_NAME"], os.environ["GITHUB_REF"],
              os.environ["GITHUB_REPOSITORY"], sha)
    validate_source(*source)
    # Also used in the publishing job, before it can move registry tags.
    tip = api(SOURCE_REPO + "/git/ref/heads/main", os.environ["GITHUB_TOKEN"])
    if tip["object"]["sha"] != sha:
        raise RuntimeError("Source is no longer the main tip; rerun the current main push")
    if check_only:
        return
    digest = os.environ["IMAGE_DIGEST"]
    image = os.environ["PUBLISHED_IMAGE"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9./_-]*", image):
        raise ValueError("Published image must be a registry image path without a tag or digest")
    payload = make_payload(*source, digest)
    api(TARGET_REPO + "/dispatches", os.environ["DEPLOY_TOKEN"], payload)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
        summary.write(
            f"### GitOps update requested\n\nSource: `{sha}`\n\n"
            f"Image: `{image}@{digest}`\n\n"
            "Dispatch accepted; this is not rollout completion. Follow the receiver's "
            "image-deploy run, production approval and post-merge sync verification.\n"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-source", action="store_true", help="verify main tip without dispatching")
    main(check_only=parser.parse_args().check_source)
