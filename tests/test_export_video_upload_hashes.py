import importlib.util
import pathlib
import unittest


SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "export-video-upload-hashes.py"
SPEC = importlib.util.spec_from_file_location("export_video_upload_hashes", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class BuildFilterTests(unittest.TestCase):
    def test_build_filter_covers_cloud_run_and_gke_shapes(self) -> None:
        log_filter = MODULE.build_filter(
            "divine-upload-server",
            "2026-03-01T03:33:49Z",
            "2026-03-02T03:33:49Z",
        )

        self.assertIn('resource.type="cloud_run_revision"', log_filter)
        self.assertIn('resource.labels.service_name="divine-upload-server"', log_filter)
        self.assertIn('resource.type="k8s_container"', log_filter)
        self.assertIn('resource.labels.container_name="divine-upload-server"', log_filter)
        self.assertIn('labels.service="divine-blossom"', log_filter)
        self.assertIn('labels.component="audit"', log_filter)
        self.assertIn('jsonPayload.metadata_snapshot.type="video/mp4"', log_filter)


class ExtractRecordTests(unittest.TestCase):
    def test_extract_record_falls_back_to_entry_timestamp(self) -> None:
        entry = {
            "timestamp": "2026-03-02T04:05:06Z",
            "jsonPayload": {
                "sha256": "abc123",
                "metadata_snapshot": {
                    "owner": "npub1owner",
                    "size": 42,
                    "dim": "1920x1080",
                },
            },
        }

        record = MODULE.extract_record(entry)

        self.assertEqual(
            record,
            {
                "sha256": "abc123",
                "uploaded": "2026-03-02T04:05:06Z",
                "owner": "npub1owner",
                "size": 42,
                "dim": "1920x1080",
                "thumbnail": "https://media.divine.video/abc123.jpg",
            },
        )


if __name__ == "__main__":
    unittest.main()
