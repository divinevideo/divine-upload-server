import importlib.util
from io import BytesIO, StringIO
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.request

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


class UploadUrlValidationTests(unittest.TestCase):
    def test_rejects_non_https_and_malformed_upload_urls(self):
        for bad in (
            'http://upload.divine.video',
            'https://user:pass@upload.divine.video',
            'https://upload.divine.video/subpath',
            'https://upload.divine.video?a=1',
            'https://upload.divine.video#frag',
        ):
            with self.subTest(url=bad):
                with self.assertRaises(ValueError):
                    helper.validate_upload_url(bad, allow_non_default=True)

    def test_non_default_https_origin_requires_explicit_opt_in(self):
        self.assertEqual(
            helper.validate_upload_url('https://upload.divine.video/'),
            helper.DEFAULT_UPLOAD_URL,
        )
        with self.assertRaises(ValueError):
            helper.validate_upload_url('https://staging-upload.example')
        self.assertEqual(
            helper.validate_upload_url(
                'https://staging-upload.example/', allow_non_default=True
            ),
            'https://staging-upload.example',
        )

    def test_parse_args_rejects_non_default_without_opt_in(self):
        with patch('sys.argv', ['backfill-thumbnails.py', '--upload-url', 'https://other.example']):
            with self.assertRaises(SystemExit):
                helper.parse_args()
        with patch(
            'sys.argv',
            [
                'backfill-thumbnails.py',
                '--upload-url',
                'https://other.example/',
                '--allow-non-default-upload-url',
            ],
        ):
            args = helper.parse_args()
        self.assertEqual(args.upload_url, 'https://other.example')


class TokenResolutionTests(unittest.TestCase):
    def test_requires_token_unless_explicitly_allowed(self):
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaises(SystemExit):
                helper.resolve_thumbnail_token()
            self.assertEqual(
                helper.resolve_thumbnail_token(allow_unauthenticated=True), ''
            )

    def test_rejects_non_visible_ascii_token_without_echoing_it(self):
        with patch.dict('os.environ', {'THUMBNAIL_SERVICE_TOKEN': 'bad token'}, clear=True):
            with self.assertRaises(SystemExit) as ctx:
                helper.resolve_thumbnail_token()
        self.assertNotIn('bad token', str(ctx.exception))


class ThumbnailHttpHelperTests(unittest.TestCase):
    def test_request_refuses_to_send_token_over_http(self):
        with self.assertRaises(ValueError):
            helper.request(
                'http://upload.divine.video/thumbnail/' + ('a' * 64),
                headers={'X-Divine-Thumbnail-Token': 'svc-token'},
            )

    def test_request_does_not_follow_redirects_with_token(self):
        seen_urls = []

        class StubHTTPSHandler(urllib.request.HTTPSHandler):
            def https_open(self, req):
                seen_urls.append(req.full_url)
                resp = urllib.request.addinfourl(
                    BytesIO(b''),
                    {'Location': 'https://evil.example/steal'},
                    req.full_url,
                    302,
                )
                resp.msg = 'Found'
                return resp

        orig_build_opener = urllib.request.build_opener
        helper.THREAD_LOCAL.openers = {}
        try:
            with patch.object(
                urllib.request,
                'build_opener',
                side_effect=lambda *handlers: orig_build_opener(StubHTTPSHandler(), *handlers),
            ):
                status = helper.request(
                    'https://upload.divine.video/thumbnail/' + ('a' * 64),
                    headers={'X-Divine-Thumbnail-Token': 'svc-token'},
                    follow_redirects=True,
                )
        finally:
            helper.THREAD_LOCAL.openers = {}
        self.assertEqual(status, 302)
        self.assertEqual(
            seen_urls,
            ['https://upload.divine.video/thumbnail/' + ('a' * 64)],
        )

    def test_request_with_headers_returns_lowercased_response_headers(self):
        class StubHTTPSHandler(urllib.request.HTTPSHandler):
            def https_open(self, req):
                resp = urllib.request.addinfourl(
                    BytesIO(b''),
                    {'X-Divine-Thumbnail-Auth': ' verified '},
                    req.full_url,
                    204,
                )
                resp.msg = 'No Content'
                return resp

        orig_build_opener = urllib.request.build_opener
        helper.THREAD_LOCAL.openers = {}
        try:
            with patch.object(
                urllib.request,
                'build_opener',
                side_effect=lambda *handlers: orig_build_opener(StubHTTPSHandler(), *handlers),
            ):
                status, resp_headers = helper.request_with_headers(
                    'https://upload.divine.video/thumbnail/auth-check',
                    headers={'X-Divine-Thumbnail-Token': 'svc-token'},
                )
        finally:
            helper.THREAD_LOCAL.openers = {}
        self.assertEqual(status, 204)
        self.assertEqual(resp_headers.get('x-divine-thumbnail-auth'), 'verified')


class ThumbnailPreflightTests(unittest.TestCase):
    def test_preflight_with_token_requires_204_and_verified_header(self):
        seen = []

        def fake_request(url, **kwargs):
            seen.append((url, kwargs))
            return 204, {'x-divine-thumbnail-auth': 'verified'}

        with patch.object(helper, 'request_with_headers', side_effect=fake_request):
            helper.verify_thumbnail_access(
                helper.DEFAULT_UPLOAD_URL,
                headers={'X-Divine-Thumbnail-Token': 'svc-token'},
            )
        self.assertEqual(
            seen,
            [
                (
                    f'{helper.DEFAULT_UPLOAD_URL}/thumbnail/auth-check',
                    {'headers': {'X-Divine-Thumbnail-Token': 'svc-token'}, 'follow_redirects': False},
                )
            ],
        )

        for bad_response in (
            (400, {}),
            (204, {}),
            (204, {'x-divine-thumbnail-auth': 'open'}),
            (404, {}),
            (302, {}),
            (503, {}),
        ):
            with self.subTest(resp=bad_response), patch.object(
                helper, 'request_with_headers', return_value=bad_response
            ):
                with self.assertRaises(SystemExit) as ctx:
                    helper.verify_thumbnail_access(
                        helper.DEFAULT_UPLOAD_URL,
                        headers={'X-Divine-Thumbnail-Token': 'svc-token'},
                    )
                self.assertNotIn('svc-token', str(ctx.exception))

    def test_preflight_unauthenticated_requires_204_and_open_header(self):
        with patch.object(
            helper,
            'request_with_headers',
            return_value=(204, {'x-divine-thumbnail-auth': 'open'}),
        ):
            helper.verify_thumbnail_access(helper.DEFAULT_UPLOAD_URL, headers={})

        for bad_response in (
            (400, {}),
            (204, {}),
            (204, {'x-divine-thumbnail-auth': 'verified'}),
            (404, {}),
            (302, {}),
        ):
            with self.subTest(resp=bad_response), patch.object(
                helper, 'request_with_headers', return_value=bad_response
            ):
                with self.assertRaises(SystemExit):
                    helper.verify_thumbnail_access(
                        helper.DEFAULT_UPLOAD_URL, headers={}
                    )

    def test_main_aborts_on_generic_400_or_404_before_deleting_anything(self):
        for bad_response in ((400, {}), (404, {})):
            with (
                self.subTest(resp=bad_response),
                patch.dict('os.environ', {'THUMBNAIL_SERVICE_TOKEN': 'svc-token'}, clear=True),
                patch('sys.argv', ['backfill-thumbnails.py', 'a' * 64]),
                patch.object(helper, 'request_with_headers', return_value=bad_response),
                patch.object(helper, 'process_hash') as process_mock,
            ):
                with self.assertRaises(SystemExit):
                    helper.main()
            process_mock.assert_not_called()

    def test_dry_run_works_without_credentials_or_network_calls(self):
        stdout = StringIO()
        with (
            patch.dict('os.environ', {}, clear=True),
            patch('sys.argv', ['backfill-thumbnails.py', '--dry-run', 'a' * 64]),
            patch.object(helper, 'request_with_headers') as req_headers_mock,
            patch.object(helper, 'request') as req_mock,
            patch.object(helper, 'process_hash') as process_mock,
            patch('sys.stdout', stdout),
        ):
            helper.main()
        self.assertEqual(stdout.getvalue().strip(), 'would_process=1')
        req_headers_mock.assert_not_called()
        req_mock.assert_not_called()
        process_mock.assert_not_called()


class ThumbnailServiceTokenTests(unittest.TestCase):
    """The upload server can require Blossom's service token on /thumbnail (issue #52)."""

    def regenerate_kwargs(self, env):
        calls = []

        def fake_request(url, **kwargs):
            calls.append((url, kwargs))
            return 200

        with patch.dict('os.environ', env, clear=True), patch.object(helper, 'request', side_effect=fake_request):
            helper.process_hash(
                'a' * 64,
                'bucket',
                'https://upload.test',
                'https://media.test',
                allow_non_default=True,
            )
        regenerate = [kwargs for url, kwargs in calls if '/thumbnail/' in url]
        self.assertEqual(len(regenerate), 1)
        return regenerate[0]

    def test_sends_the_service_token_when_configured_without_redirects(self):
        kwargs = self.regenerate_kwargs({'THUMBNAIL_SERVICE_TOKEN': ' svc-token '})
        self.assertEqual((kwargs.get('headers') or {}).get('X-Divine-Thumbnail-Token'), 'svc-token')
        self.assertIs(kwargs.get('follow_redirects'), False)

    def test_sends_no_token_header_when_unset(self):
        headers = self.regenerate_kwargs({}).get('headers') or {}
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
