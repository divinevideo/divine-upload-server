import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('thumbnail_helper', Path(__file__).parents[1] / 'backfill-thumbnails.py')
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)

class StorageModeTests(unittest.TestCase):
    def test_rejects_dual_and_b2_before_provider_operations(self):
        for mode in ('dual', 'b2'):
            with self.subTest(mode=mode), patch.dict('os.environ', {'MEDIA_WRITE_PROVIDER': mode}), patch('sys.argv', ['backfill-thumbnails.py']):
                with self.assertRaises(SystemExit):
                    helper.parse_args()

    def test_gcs_mode_remains_available(self):
        with patch.dict('os.environ', {'MEDIA_WRITE_PROVIDER': 'gcs'}), patch('sys.argv', ['backfill-thumbnails.py']):
            self.assertEqual(helper.parse_args().storage_provider, 'gcs')


class ThumbnailServiceTokenTests(unittest.TestCase):
    """The upload server can require Blossom's service token on /thumbnail (issue #52)."""

    def regenerate_headers(self, env):
        calls = []

        def fake_request(url, **kwargs):
            calls.append((url, kwargs))
            return 200

        with patch.dict('os.environ', env, clear=True), patch.object(helper, 'request', side_effect=fake_request):
            helper.process_hash('a' * 64, 'bucket', 'https://upload.test', 'https://media.test')
        regenerate = [kwargs for url, kwargs in calls if '/thumbnail/' in url]
        self.assertEqual(len(regenerate), 1)
        return regenerate[0].get('headers') or {}

    def test_sends_the_service_token_when_configured(self):
        headers = self.regenerate_headers({'THUMBNAIL_SERVICE_TOKEN': ' svc-token '})
        self.assertEqual(headers.get('X-Divine-Thumbnail-Token'), 'svc-token')

    def test_sends_no_token_header_when_unset(self):
        headers = self.regenerate_headers({})
        self.assertNotIn('X-Divine-Thumbnail-Token', headers)


class ThumbnailServiceTokenWarningTests(unittest.TestCase):
    def test_warns_when_no_token_is_set(self):
        with patch.dict('os.environ', {}, clear=True):
            warnings = helper.thumbnail_token_warnings(helper.DEFAULT_UPLOAD_URL)
        self.assertEqual(len(warnings), 1)
        self.assertIn('THUMBNAIL_SERVICE_TOKEN', warnings[0])
        self.assertIn('404', warnings[0])

    def test_quiet_with_a_token_for_the_default_host(self):
        with patch.dict('os.environ', {'THUMBNAIL_SERVICE_TOKEN': 'svc'}, clear=True):
            self.assertEqual(helper.thumbnail_token_warnings(helper.DEFAULT_UPLOAD_URL), [])

    def test_warns_before_sending_the_token_to_another_host(self):
        with patch.dict('os.environ', {'THUMBNAIL_SERVICE_TOKEN': 'svc'}, clear=True):
            warnings = helper.thumbnail_token_warnings('https://elsewhere.example')
        self.assertEqual(len(warnings), 1)
        self.assertIn('elsewhere.example', warnings[0])
        self.assertNotIn('svc', warnings[0])
