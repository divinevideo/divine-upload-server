#!/usr/bin/env python3
"""Delete, regenerate, and revalidate thumbnails for a set of video hashes."""

import argparse
import os
import concurrent.futures
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_BUCKET = "divine-blossom-media"
DEFAULT_UPLOAD_URL = "https://upload.divine.video"
DEFAULT_MEDIA_URL = "https://media.divine.video"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill thumbnails by deleting the stored JPEG, regenerating it, and revalidating the CDN URL."
    )
    parser.add_argument(
        "hashes",
        nargs="*",
        help="Optional SHA-256 hashes to repair.",
    )
    parser.add_argument(
        "--hash-file",
        help="Read one SHA-256 hash per line from this file.",
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help=f"GCS bucket holding thumbnail objects. Default: {DEFAULT_BUCKET}",
    )
    parser.add_argument(
        "--upload-url",
        default=DEFAULT_UPLOAD_URL,
        help=f"Upload service base URL (HTTPS origin only). Default: {DEFAULT_UPLOAD_URL}",
    )
    parser.add_argument(
        "--allow-non-default-upload-url",
        action="store_true",
        help="Explicitly allow sending THUMBNAIL_SERVICE_TOKEN to an HTTPS --upload-url other than the default.",
    )
    parser.add_argument(
        "--allow-unauthenticated",
        action="store_true",
        help="Allow running without THUMBNAIL_SERVICE_TOKEN (only for open/observe environments).",
    )
    parser.add_argument(
        "--media-url",
        default=DEFAULT_MEDIA_URL,
        help=f"Public media base URL. Default: {DEFAULT_MEDIA_URL}",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Concurrent worker count. Default: 32",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Process only the first N hashes after loading input.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Emit progress every N completed hashes. Default: 100",
    )
    parser.add_argument(
        "--failure-file",
        help="Write per-hash failures here.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate input and print planned count without changing anything.",
    )
    parser.add_argument("--storage-provider", choices=("gcs", "b2", "dual"), default=os.environ.get("MEDIA_WRITE_PROVIDER", "gcs"), help="This maintenance tool supports GCS only.")
    args = parser.parse_args()
    if args.storage_provider != "gcs":
        parser.error("Thumbnail deletion supports GCS only; use the provider-aware service workflow for B2/dual storage")
    try:
        args.upload_url = validate_upload_url(
            args.upload_url, allow_non_default=args.allow_non_default_upload_url
        )
    except ValueError as exc:
        parser.error(str(exc))
    return args


def load_hashes(args: argparse.Namespace) -> list[str]:
    hashes: list[str] = []

    if args.hash_file:
        with open(args.hash_file, encoding="utf-8") as handle:
            for line in handle:
                line = line.split("#", 1)[0].strip().lower()
                if line:
                    hashes.append(line)

    hashes.extend(hash_.lower() for hash_ in args.hashes)

    if not hashes:
        raise SystemExit("No hashes provided. Use positional hashes or --hash-file.")

    deduped: list[str] = []
    seen: set[str] = set()
    for hash_ in hashes:
        if len(hash_) != 64 or any(ch not in "0123456789abcdef" for ch in hash_):
            raise SystemExit(f"Invalid SHA-256 hash: {hash_}")
        if hash_ in seen:
            continue
        seen.add(hash_)
        deduped.append(hash_)

    if args.limit is not None:
        deduped = deduped[: args.limit]

    return deduped


class TokenManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._token = ""
        self._expires_at = 0.0

    def get(self, force_refresh: bool = False) -> str:
        now = time.time()
        with self._lock:
            if force_refresh or not self._token or now >= self._expires_at:
                self._token = subprocess.check_output(
                    ["gcloud", "auth", "print-access-token"], text=True
                ).strip()
                self._expires_at = now + 45 * 60
            return self._token


TOKEN_MANAGER = TokenManager()
THREAD_LOCAL = threading.local()


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def build_opener(*, follow_redirects: bool = True):
    if follow_redirects:
        return urllib.request.build_opener()
    return urllib.request.build_opener(_NoRedirectHandler())


def get_opener(*, follow_redirects: bool = True):
    openers = getattr(THREAD_LOCAL, "openers", None)
    if openers is None:
        openers = {}
        THREAD_LOCAL.openers = openers
    key = bool(follow_redirects)
    opener = openers.get(key)
    if opener is None:
        opener = build_opener(follow_redirects=key)
        openers[key] = opener
    return opener


def request(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    timeout: int = 60,
    auth: bool = False,
    follow_redirects: bool = True,
) -> int:
    final_headers = dict(headers or {})
    has_thumbnail_token = any(
        key.lower() == "x-divine-thumbnail-token" for key in final_headers
    )
    if has_thumbnail_token:
        if urllib.parse.urlsplit(url).scheme.lower() != "https":
            raise ValueError("Refusing to send thumbnail service token over non-HTTPS URL")
        follow_redirects = False
    opener = get_opener(follow_redirects=follow_redirects)

    for attempt in range(2):
        if auth:
            token = TOKEN_MANAGER.get(force_refresh=attempt > 0)
            final_headers["Authorization"] = f"Bearer {token}"

        req = urllib.request.Request(url, method=method, headers=final_headers)
        try:
            with opener.open(req, timeout=timeout) as resp:
                return resp.status
        except urllib.error.HTTPError as exc:
            if auth and exc.code in (401, 403) and attempt == 0:
                continue
            return exc.code

    return 599


def validate_upload_url(upload_url: str, *, allow_non_default: bool = False) -> str:
    """Validate and normalize --upload-url as a bare HTTPS origin."""
    parsed = urllib.parse.urlsplit(upload_url.strip())
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        raise ValueError("--upload-url must be an https:// origin")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("--upload-url must not embed credentials")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ValueError("--upload-url must not include a path, query, or fragment")
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    netloc = f"{host}:{parsed.port}" if parsed.port not in (None, 443) else host
    origin = f"https://{netloc}"
    if origin != DEFAULT_UPLOAD_URL and not allow_non_default:
        raise ValueError(
            f"Non-default --upload-url ({origin}) requires --allow-non-default-upload-url"
        )
    return origin


def resolve_thumbnail_token(*, allow_unauthenticated: bool = False) -> str:
    """Read and validate THUMBNAIL_SERVICE_TOKEN without leaking its value."""
    token = os.environ.get("THUMBNAIL_SERVICE_TOKEN", "").strip()
    if not token:
        if allow_unauthenticated:
            return ""
        raise SystemExit(
            "THUMBNAIL_SERVICE_TOKEN is required (pass --allow-unauthenticated only for open/observe environments)"
        )
    if not all(33 <= ord(ch) <= 126 for ch in token):
        raise SystemExit("THUMBNAIL_SERVICE_TOKEN must be visible ASCII with no spaces")
    return token


def thumbnail_service_headers() -> dict[str, str]:
    """Blossom's service token, which the upload server can require on /thumbnail."""
    token = os.environ.get("THUMBNAIL_SERVICE_TOKEN", "").strip()
    return {"X-Divine-Thumbnail-Token": token} if token else {}


def thumbnail_token_warnings(upload_url: str) -> list[str]:
    """Operator warnings about the /thumbnail service token for this run."""
    if not os.environ.get("THUMBNAIL_SERVICE_TOKEN", "").strip():
        return [
            "warning: THUMBNAIL_SERVICE_TOKEN is not set; once the upload server "
            "requires it, every regeneration fails as regenerate:404"
        ]
    if upload_url.rstrip("/") != DEFAULT_UPLOAD_URL:
        host = urllib.parse.urlparse(upload_url).netloc or upload_url
        return [f"warning: sending THUMBNAIL_SERVICE_TOKEN to a non-default upload host: {host}"]
    return []


def verify_thumbnail_access(
    upload_url: str,
    *,
    headers: dict[str, str],
    allow_non_default: bool = False,
) -> None:
    """Confirm /thumbnail accepts our credential before deleting any GCS JPEG."""
    origin = validate_upload_url(upload_url, allow_non_default=allow_non_default)
    status = request(
        f"{origin}/thumbnail/preflight-check",
        headers=headers,
        follow_redirects=False,
    )
    if status == 400:
        return
    if status == 404:
        raise SystemExit(
            "Thumbnail service preflight returned 404 before deleting objects; "
            "verify THUMBNAIL_SERVICE_TOKEN (pass --allow-unauthenticated only when unrequired)"
        )
    raise SystemExit(
        f"Thumbnail service preflight returned unexpected HTTP {status} before deleting objects"
    )


def process_hash(
    hash_: str,
    bucket: str,
    upload_url: str,
    media_url: str,
    *,
    allow_non_default: bool = False,
    headers: dict[str, str] | None = None,
) -> tuple[str, str | None]:
    origin = validate_upload_url(upload_url, allow_non_default=allow_non_default)
    object_name = urllib.parse.quote(f"{hash_}.jpg", safe="")
    delete_url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{object_name}"
    delete_status = request(delete_url, method="DELETE", auth=True)
    delete_error = None if delete_status in (200, 204, 404) else f"delete:{delete_status}"

    req_headers = thumbnail_service_headers() if headers is None else headers
    regenerate_status = request(
        f"{origin}/thumbnail/{hash_}",
        headers=req_headers,
        follow_redirects=False,
    )
    if regenerate_status != 200:
        errors = [err for err in (delete_error, f"regenerate:{regenerate_status}") if err]
        return hash_, ",".join(errors)

    revalidate_status = request(
        f"{media_url}/{hash_}.jpg", headers={"Cache-Control": "no-cache"}
    )
    revalidate_error = (
        None if revalidate_status == 200 else f"revalidate:{revalidate_status}"
    )

    errors = [err for err in (delete_error, revalidate_error) if err]
    return hash_, ",".join(errors) if errors else None


def main() -> None:
    args = parse_args()
    token = resolve_thumbnail_token(allow_unauthenticated=args.allow_unauthenticated)
    headers = {"X-Divine-Thumbnail-Token": token} if token else {}
    for warning in thumbnail_token_warnings(args.upload_url):
        print(warning, file=sys.stderr)
    hashes = load_hashes(args)
    total = len(hashes)

    if args.dry_run:
        print(f"would_process={total}")
        return

    verify_thumbnail_access(
        args.upload_url,
        headers=headers,
        allow_non_default=args.allow_non_default_upload_url,
    )

    failure_handle = (
        open(args.failure_file, "a", encoding="utf-8") if args.failure_file else None
    )
    completed = 0
    failures = 0
    started_at = time.time()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_hash = {
                executor.submit(
                    process_hash,
                    hash_,
                    args.bucket,
                    args.upload_url,
                    args.media_url,
                    allow_non_default=args.allow_non_default_upload_url,
                    headers=headers,
                ): hash_
                for hash_ in hashes
            }

            for future in concurrent.futures.as_completed(future_to_hash):
                hash_ = future_to_hash[future]
                try:
                    _, error = future.result()
                except Exception as exc:  # pragma: no cover - operational logging
                    error = f"exception:{type(exc).__name__}"

                completed += 1
                if error:
                    failures += 1
                    line = f"{hash_} {error}\n"
                    if failure_handle:
                        failure_handle.write(line)
                    else:
                        sys.stderr.write(line)

                if completed % args.progress_every == 0 or completed == total:
                    elapsed = max(time.time() - started_at, 1e-6)
                    rate = completed / elapsed
                    print(
                        f"completed={completed}/{total} failures={failures} rate={rate:.2f}/s",
                        file=sys.stderr,
                    )
    finally:
        if failure_handle:
            failure_handle.close()


if __name__ == "__main__":
    main()
