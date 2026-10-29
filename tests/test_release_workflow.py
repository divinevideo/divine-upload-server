import pathlib
import tomllib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = (ROOT / ".github/workflows/ci.yml").read_text()


class ReleaseWorkflowTests(unittest.TestCase):
    def test_ci_toolchain_matches_local_pin(self):
        version = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
        self.assertIn(f"uses: dtolnay/rust-toolchain@{version}\n", WORKFLOW)

    def test_publish_keeps_legacy_tag_and_checks_source_before_push(self):
        publish = WORKFLOW.split("  docker:\n", 1)[1].split("  promote:\n", 1)[0]
        self.assertIn('SHORT_SHA="${GITHUB_SHA::7}"', publish)
        self.assertIn('echo "${IMAGE}:${SHORT_SHA}"', publish)
        self.assertIn('echo "${IMAGE}:${GITHUB_SHA}"', publish)
        self.assertLess(publish.index("python3 dispatch-image.py --check-source"),
                        publish.index("uses: docker/build-push-action@v6"))

    def test_dispatch_job_references_protected_environment_and_scoped_outputs(self):
        promote = WORKFLOW.split("  promote:\n", 1)[1]
        self.assertIn("    environment: production-gitops\n", promote)
        self.assertIn("secrets.CORECONFIG_DISPATCH_APP_ID", promote)
        self.assertIn("secrets.CORECONFIG_DISPATCH_APP_PRIVATE_KEY", promote)
        self.assertNotIn("secrets.APP_", promote)
        self.assertIn("PUBLISHED_IMAGE: ${{ needs.docker.outputs.image }}", promote)
        self.assertIn("needs: [verify, docker]", promote)
        self.assertIn("github.event_name == 'push' && github.ref == 'refs/heads/main'", promote)
