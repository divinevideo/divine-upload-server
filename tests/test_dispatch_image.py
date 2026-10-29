import importlib.util
import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "dispatch-image.py"
SPEC = importlib.util.spec_from_file_location("dispatch_image", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
SHA = "a" * 40
DIGEST = "sha256:" + "b" * 64


class DispatchTests(unittest.TestCase):
    def payload(self, **changes):
        values = dict(event="push", ref="refs/heads/main", repository=MODULE.SOURCE_REPO,
                      sha=SHA, digest=DIGEST)
        values.update(changes)
        return MODULE.make_payload(**values)

    def test_payload_preserves_approval_and_source_identity(self):
        payload = self.payload()
        self.assertEqual(payload["event_type"], "image-deploy")
        client = payload["client_payload"]
        self.assertEqual(client["environments"], ["production"])
        self.assertEqual(client["images"], [{"name": "divine-upload-server", "tag": SHA,
                                            "digest": DIGEST}])
        self.assertEqual(client["source_sha"], SHA)
        self.assertEqual(client["image_digest"], DIGEST)
        self.assertIs(client["internal"], True)
        self.assertIs(client["hotfix"], False)
        self.assertIs(client["auto_promote"], False)

    def test_unapproved_events_refs_repositories_and_identifiers_fail(self):
        for changes in ({"event": "pull_request"}, {"event": "workflow_dispatch"},
                        {"ref": "refs/heads/feature"}, {"ref": "refs/tags/release"},
                        {"repository": "example/fork"}, {"sha": "a" * 7},
                        {"sha": "latest"}, {"digest": ""}, {"digest": "sha256:bad"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.payload(**changes)

    def test_same_source_with_new_build_retains_distinct_deployment_digest(self):
        rebuilt = self.payload(digest="sha256:" + "c" * 64)["client_payload"]
        original = self.payload()["client_payload"]
        self.assertEqual(original["images"][0]["tag"], rebuilt["images"][0]["tag"])
        self.assertNotEqual(original["images"][0]["digest"], rebuilt["images"][0]["digest"])
        self.assertEqual(rebuilt["images"][0]["digest"], rebuilt["image_digest"])

    def run_main(self, tip):
        with tempfile.TemporaryDirectory() as folder:
            summary = pathlib.Path(folder) / "summary.md"
            environment = dict(GITHUB_SHA=SHA, IMAGE_DIGEST=DIGEST, GITHUB_EVENT_NAME="push",
                               GITHUB_REF="refs/heads/main", GITHUB_REPOSITORY=MODULE.SOURCE_REPO,
                               GITHUB_TOKEN="synthetic-source", DEPLOY_TOKEN="synthetic-dispatch",
                               GITHUB_STEP_SUMMARY=str(summary))
            with mock.patch.dict(os.environ, environment, clear=True), \
                    mock.patch.object(MODULE, "api", side_effect=[{"sha": tip}, None]) as api:
                if tip != SHA:
                    with self.assertRaisesRegex(RuntimeError, "no longer the main tip"):
                        MODULE.main()
                    self.assertEqual(api.call_count, 1)
                    self.assertFalse(summary.exists())
                else:
                    MODULE.main()
                    self.assertEqual(api.call_args_list[0].args,
                                     (MODULE.SOURCE_REPO + "/commits/main", "synthetic-source"))
                    self.assertEqual(api.call_args_list[1].args,
                                     (MODULE.TARGET_REPO + "/dispatches", "synthetic-dispatch", self.payload()))
                    self.assertIn("not rollout completion", summary.read_text())
                    self.assertIn(DIGEST, summary.read_text())

    def test_current_main_dispatches_once(self):
        self.run_main(SHA)

    def test_stale_rerun_cannot_dispatch(self):
        self.run_main("c" * 40)

    def test_api_uses_separate_request_and_requires_acceptance(self):
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.status = 204
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response) as urlopen:
            payload = self.payload()
            MODULE.api(MODULE.TARGET_REPO + "/dispatches", "synthetic", payload)
            request = urlopen.call_args.args[0]
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(json.loads(request.data), payload)
            self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)
            response.status = 200
            with self.assertRaises(RuntimeError):
                MODULE.api(MODULE.TARGET_REPO + "/dispatches", "synthetic", payload)
