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
