#!/usr/bin/env python3
"""Build a bounded, authenticated GitHub receipt for a merged exact PR head."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json
import math
import os
import re
import selectors
import signal
import subprocess
import time
from typing import Any, Callable
import urllib.error
import urllib.parse
import urllib.request

from workstream_http import default_ssl_context


_API_AUTHORITY = "api.github.com"
_OID = re.compile(r"[0-9a-f]{40}")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_CHECK_FIELDS = {"name", "status", "conclusion", "details_url"}
_MAX_TOKEN_BYTES = 8192
_MAX_TIMEOUT_SECONDS = 60.0
_SAFE_COMMAND_ENV = {
    "PATH", "HOME", "USERPROFILE", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT",
    "TMP", "TEMP", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE",
}
_RECEIPT_FIELDS = {
    "schema_version",
    "provider",
    "repository",
    "provider_repository_id",
    "pull_request_number",
    "pr_head",
    "merged",
    "merged_at",
    "merge_sha",
    "checks",
    "checks_sha256",
    "provider_receipt_sha256",
}


class GitHubBackfillReceiptError(RuntimeError):
    """A typed fail-closed GitHub receipt validation or transport error."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


_VERIFIED_RECEIPT_SENTINEL = object()


class VerifiedGitHubBackfillReceipt:
    """Opaque immutable result minted only by an authenticated reader."""

    __slots__ = ("__payload",)

    def __init__(self, receipt: dict[str, Any], *, _sentinel: object | None = None):
        if _sentinel is not _VERIFIED_RECEIPT_SENTINEL:
            raise GitHubBackfillReceiptError(
                "github_verified_receipt_constructor_private"
            )
        validated = validate_github_backfill_receipt(receipt)
        object.__setattr__(
            self,
            "_VerifiedGitHubBackfillReceipt__payload",
            _canonical_bytes(validated),
        )

    def __init_subclass__(cls, **_kwargs: Any) -> None:
        raise TypeError("VerifiedGitHubBackfillReceipt cannot be subclassed")

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise AttributeError("VerifiedGitHubBackfillReceipt is immutable")

    def __delattr__(self, _name: str) -> None:
        raise AttributeError("VerifiedGitHubBackfillReceipt is immutable")

    def as_dict(self) -> dict[str, Any]:
        """Return a detached structural receipt suitable for persistence."""
        payload = object.__getattribute__(
            self, "_VerifiedGitHubBackfillReceipt__payload"
        )
        value = json.loads(payload)
        assert isinstance(value, dict)
        return value


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        raise GitHubBackfillReceiptError("github_redirect_refused")


def _default_opener(request: urllib.request.Request, *, timeout: float) -> Any:
    opener = urllib.request.build_opener(
        _NoRedirects(),
        urllib.request.HTTPSHandler(context=default_ssl_context()),
    )
    return opener.open(request, timeout=timeout)


def _terminate_and_reap(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=0.5)
    except subprocess.TimeoutExpired:
        process.kill()
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired as error:
            raise GitHubBackfillReceiptError("github_auth_process_unreaped") from error


def github_token_from_command(argv: list[str], *, timeout: float = 10.0) -> str:
    """Read one token from a bounded noninteractive fixed-argv command."""
    if (
        not isinstance(argv, list)
        or not argv
        or not all(isinstance(item, str) and item for item in argv)
    ):
        raise GitHubBackfillReceiptError("github_auth_fixed_argv_required")
    if (
        not isinstance(timeout, (int, float))
        or isinstance(timeout, bool)
        or timeout <= 0
        or timeout > _MAX_TIMEOUT_SECONDS
        or not math.isfinite(timeout)
    ):
        raise GitHubBackfillReceiptError("github_auth_timeout_invalid")
    environment = {
        key: value for key, value in os.environ.items() if key in _SAFE_COMMAND_ENV
    }
    environment["GIT_TERMINAL_PROMPT"] = "0"
    try:
        process = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            start_new_session=True,
        )
    except OSError as error:
        raise GitHubBackfillReceiptError("github_auth_command_unavailable") from error
    assert process.stdout is not None and process.stderr is not None
    selector = selectors.DefaultSelector()
    stdout = bytearray()
    stderr = bytearray()
    for stream, target in ((process.stdout, stdout), (process.stderr, stderr)):
        os.set_blocking(stream.fileno(), False)
        selector.register(stream, selectors.EVENT_READ, target)
    deadline = time.monotonic() + float(timeout)
    failure: str | None = None
    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                failure = "github_auth_timeout"
                break
            events = selector.select(min(0.05, remaining))
            for key, _mask in events:
                try:
                    chunk = os.read(key.fileobj.fileno(), 65536)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                key.data.extend(chunk)
                if len(stdout) + len(stderr) > _MAX_TOKEN_BYTES:
                    failure = "github_auth_too_large"
                    break
            if failure:
                break
            if process.poll() is not None and not events and selector.get_map():
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                deadline = min(deadline, time.monotonic() + 0.5)
    finally:
        for key in list(selector.get_map().values()):
            selector.unregister(key.fileobj)
            key.fileobj.close()
        selector.close()
    if failure:
        _terminate_and_reap(process)
        raise GitHubBackfillReceiptError(failure)
    try:
        process.wait(timeout=max(0.01, deadline - time.monotonic()))
    except subprocess.TimeoutExpired as error:
        _terminate_and_reap(process)
        raise GitHubBackfillReceiptError("github_auth_timeout") from error
    if process.returncode != 0:
        raise GitHubBackfillReceiptError("github_auth_command_failed")
    try:
        token = bytes(stdout).decode("utf-8").strip()
    except UnicodeDecodeError as error:
        raise GitHubBackfillReceiptError("github_auth_token_malformed") from error
    if not token or any(character.isspace() for character in token):
        raise GitHubBackfillReceiptError("github_auth_token_malformed")
    return token


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _valid_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _check_sort_key(check: dict[str, Any]) -> tuple[str, str, str, str]:
    details_url = check["details_url"]
    return (
        check["name"], check["status"], check["conclusion"],
        "" if details_url is None else details_url,
    )


def _canonical_checks(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted((deepcopy(check) for check in checks), key=_check_sort_key)


def validate_github_backfill_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    """Validate the exact schema and digests, returning an isolated copy."""
    if not isinstance(receipt, dict) or set(receipt) != _RECEIPT_FIELDS:
        raise GitHubBackfillReceiptError("github_receipt_schema_invalid")
    if receipt.get("schema_version") != 1 or receipt.get("provider") != "github":
        raise GitHubBackfillReceiptError("github_receipt_schema_invalid")
    repository = receipt.get("repository")
    if (
        not isinstance(repository, str)
        or not _REPOSITORY.fullmatch(repository)
        or repository != repository.lower()
    ):
        raise GitHubBackfillReceiptError("github_receipt_repository_invalid")
    provider_id = receipt.get("provider_repository_id")
    if not isinstance(provider_id, str) or not provider_id:
        raise GitHubBackfillReceiptError("github_receipt_repository_id_invalid")
    pr_number = receipt.get("pull_request_number")
    if not isinstance(pr_number, int) or isinstance(pr_number, bool) or pr_number <= 0:
        raise GitHubBackfillReceiptError("github_receipt_pr_invalid")
    if not isinstance(receipt.get("pr_head"), str) or not _OID.fullmatch(receipt["pr_head"]):
        raise GitHubBackfillReceiptError("github_receipt_head_invalid")
    if receipt.get("merged") is not True or not _valid_timestamp(receipt.get("merged_at")):
        raise GitHubBackfillReceiptError("github_receipt_merge_invalid")
    if not isinstance(receipt.get("merge_sha"), str) or not _OID.fullmatch(receipt["merge_sha"]):
        raise GitHubBackfillReceiptError("github_receipt_merge_invalid")

    checks = receipt.get("checks")
    if not isinstance(checks, list) or not checks:
        raise GitHubBackfillReceiptError("github_receipt_checks_invalid")
    for check in checks:
        if not isinstance(check, dict) or set(check) != _CHECK_FIELDS:
            raise GitHubBackfillReceiptError("github_receipt_checks_invalid")
        if not isinstance(check.get("name"), str) or not check["name"]:
            raise GitHubBackfillReceiptError("github_receipt_checks_invalid")
        if check.get("status") != "completed" or check.get("conclusion") != "success":
            raise GitHubBackfillReceiptError("github_receipt_checks_unsuccessful")
        details_url = check.get("details_url")
        if details_url is not None and (
            not isinstance(details_url, str)
            or urllib.parse.urlparse(details_url).scheme != "https"
        ):
            raise GitHubBackfillReceiptError("github_receipt_checks_invalid")
    if checks != _canonical_checks(checks):
        raise GitHubBackfillReceiptError("github_receipt_checks_not_canonical")
    checks_digest = receipt.get("checks_sha256")
    if (
        not isinstance(checks_digest, str)
        or not _DIGEST.fullmatch(checks_digest)
        or checks_digest != _digest(checks)
    ):
        raise GitHubBackfillReceiptError("github_receipt_checks_digest_mismatch")
    receipt_digest = receipt.get("provider_receipt_sha256")
    unsigned = {key: value for key, value in receipt.items() if key != "provider_receipt_sha256"}
    if (
        not isinstance(receipt_digest, str)
        or not _DIGEST.fullmatch(receipt_digest)
        or receipt_digest != _digest(unsigned)
    ):
        raise GitHubBackfillReceiptError("github_receipt_digest_mismatch")
    return deepcopy(receipt)


class GitHubBackfillReceiptReader:
    """Read PR and check-run truth only from authenticated ``api.github.com``."""

    def __init__(
        self,
        token: str,
        *,
        timeout: float = 20.0,
        max_pages: int = 10,
        max_response_bytes: int = 1024 * 1024,
        max_total_bytes: int = 4 * 1024 * 1024,
        opener: Callable[..., Any] | None = None,
    ):
        if not isinstance(token, str) or not token:
            raise GitHubBackfillReceiptError("github_auth_unavailable")
        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or timeout <= 0
            or timeout > _MAX_TIMEOUT_SECONDS
            or not math.isfinite(timeout)
        ):
            raise GitHubBackfillReceiptError("github_timeout_invalid")
        for value, code in (
            (max_pages, "github_page_bound_invalid"),
            (max_response_bytes, "github_response_bound_invalid"),
            (max_total_bytes, "github_total_bound_invalid"),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise GitHubBackfillReceiptError(code)
        if max_total_bytes < max_response_bytes:
            raise GitHubBackfillReceiptError("github_total_bound_invalid")
        self._token = token
        self.timeout = float(timeout)
        self.max_pages = max_pages
        self.max_response_bytes = max_response_bytes
        self.max_total_bytes = max_total_bytes
        self.opener = opener or _default_opener
        self._bytes_read = 0

    @staticmethod
    def _validate_api_url(url: str) -> None:
        if not isinstance(url, str):
            raise GitHubBackfillReceiptError("github_api_authority_mismatch")
        parsed = urllib.parse.urlparse(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != _API_AUTHORITY
            or parsed.port is not None
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise GitHubBackfillReceiptError("github_api_authority_mismatch")

    def _read_json(self, url: str) -> dict[str, Any]:
        self._validate_api_url(url)
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "agent-workstream",
            },
        )
        try:
            with self.opener(request, timeout=self.timeout) as response:
                raw = response.read(self.max_response_bytes + 1)
                final_url = response.geturl() if hasattr(response, "geturl") else url
        except GitHubBackfillReceiptError:
            raise
        except (OSError, urllib.error.URLError, ValueError) as error:
            raise GitHubBackfillReceiptError("github_read_unavailable") from error
        self._validate_api_url(final_url)
        if not isinstance(raw, bytes):
            raise GitHubBackfillReceiptError("github_response_malformed")
        if len(raw) > self.max_response_bytes:
            raise GitHubBackfillReceiptError("github_response_too_large")
        self._bytes_read += len(raw)
        if self._bytes_read > self.max_total_bytes:
            raise GitHubBackfillReceiptError("github_total_too_large")
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise GitHubBackfillReceiptError("github_response_malformed") from error
        if not isinstance(payload, dict):
            raise GitHubBackfillReceiptError("github_response_malformed")
        return payload

    def _read_checks(self, repository: str, expected_head: str) -> list[dict[str, Any]]:
        observed: list[dict[str, Any]] = []
        observed_ids: set[int] = set()
        expected_total: int | None = None
        for page in range(1, self.max_pages + 1):
            query = urllib.parse.urlencode({"filter": "all", "per_page": 100, "page": page})
            payload = self._read_json(
                f"https://{_API_AUTHORITY}/repos/{repository}/commits/"
                f"{expected_head}/check-runs?{query}"
            )
            total = payload.get("total_count")
            batch = payload.get("check_runs")
            if (
                not isinstance(total, int)
                or isinstance(total, bool)
                or total < 0
                or not isinstance(batch, list)
                or len(batch) > 100
            ):
                raise GitHubBackfillReceiptError("github_checks_response_malformed")
            if expected_total is None:
                expected_total = total
                if total > self.max_pages * 100:
                    raise GitHubBackfillReceiptError("github_checks_pagination_exceeded")
            elif total != expected_total:
                raise GitHubBackfillReceiptError("github_checks_changed_during_read")
            if not batch and len(observed) < total:
                raise GitHubBackfillReceiptError("github_checks_pagination_incomplete")
            for item in batch:
                if not isinstance(item, dict):
                    raise GitHubBackfillReceiptError("github_checks_response_malformed")
                check_id = item.get("id")
                if (
                    not isinstance(check_id, int)
                    or isinstance(check_id, bool)
                    or check_id <= 0
                    or check_id in observed_ids
                ):
                    raise GitHubBackfillReceiptError(
                        "github_checks_changed_during_read"
                    )
                observed_ids.add(check_id)
                if item.get("head_sha") != expected_head:
                    raise GitHubBackfillReceiptError("github_check_head_mismatch")
                if item.get("status") != "completed" or item.get("conclusion") != "success":
                    raise GitHubBackfillReceiptError("github_checks_unsuccessful")
                name = item.get("name")
                details_url = item.get("details_url")
                if not isinstance(name, str) or not name or (
                    details_url is not None
                    and (not isinstance(details_url, str)
                         or urllib.parse.urlparse(details_url).scheme != "https")
                ):
                    raise GitHubBackfillReceiptError("github_checks_response_malformed")
                observed.append({
                    "name": name,
                    "status": "completed",
                    "conclusion": "success",
                    "details_url": details_url,
                })
            if len(observed) > total:
                raise GitHubBackfillReceiptError("github_checks_changed_during_read")
            if len(observed) == total:
                break
        else:
            raise GitHubBackfillReceiptError("github_checks_pagination_exceeded")
        if expected_total is None or expected_total == 0 or len(observed) != expected_total:
            raise GitHubBackfillReceiptError("github_checks_empty_or_incomplete")
        return _canonical_checks(observed)

    def read(
        self,
        *,
        repository: str,
        provider_repository_id: str,
        pull_request_number: int,
        expected_head: str,
        expected_merge_sha: str,
    ) -> VerifiedGitHubBackfillReceipt:
        """Return a canonical receipt bound to caller-reviewed immutable inputs."""
        if not isinstance(repository, str) or not _REPOSITORY.fullmatch(repository):
            raise GitHubBackfillReceiptError("github_repository_invalid")
        if not isinstance(provider_repository_id, str) or not provider_repository_id:
            raise GitHubBackfillReceiptError("github_repository_id_invalid")
        if (
            not isinstance(pull_request_number, int)
            or isinstance(pull_request_number, bool)
            or pull_request_number <= 0
        ):
            raise GitHubBackfillReceiptError("github_pr_invalid")
        if not isinstance(expected_head, str) or not _OID.fullmatch(expected_head):
            raise GitHubBackfillReceiptError("github_expected_head_invalid")
        if not isinstance(expected_merge_sha, str) or not _OID.fullmatch(expected_merge_sha):
            raise GitHubBackfillReceiptError("github_expected_merge_sha_invalid")

        self._bytes_read = 0
        payload = self._read_json(
            f"https://{_API_AUTHORITY}/repos/{repository}/pulls/{pull_request_number}"
        )
        base_repo = (payload.get("base") or {}).get("repo")
        if not isinstance(base_repo, dict):
            raise GitHubBackfillReceiptError("github_pr_response_malformed")
        observed_ids = {
            str(value) for value in (base_repo.get("id"), base_repo.get("node_id"))
            if value is not None and str(value)
        }
        if provider_repository_id not in observed_ids:
            raise GitHubBackfillReceiptError("github_repository_identity_mismatch")
        if str(base_repo.get("full_name", "")).lower() != repository.lower():
            raise GitHubBackfillReceiptError("github_repository_coordinate_mismatch")
        if payload.get("number") != pull_request_number:
            raise GitHubBackfillReceiptError("github_pr_number_mismatch")
        head = (payload.get("head") or {}).get("sha")
        if head != expected_head:
            raise GitHubBackfillReceiptError("github_pr_head_mismatch")
        if payload.get("merged") is not True or not _valid_timestamp(payload.get("merged_at")):
            raise GitHubBackfillReceiptError("github_pr_not_merged")
        if payload.get("merge_commit_sha") != expected_merge_sha:
            raise GitHubBackfillReceiptError("github_merge_sha_mismatch")

        checks = self._read_checks(repository, expected_head)
        receipt: dict[str, Any] = {
            "schema_version": 1,
            "provider": "github",
            "repository": repository.lower(),
            "provider_repository_id": provider_repository_id,
            "pull_request_number": pull_request_number,
            "pr_head": expected_head,
            "merged": True,
            "merged_at": payload["merged_at"],
            "merge_sha": expected_merge_sha,
            "checks": checks,
            "checks_sha256": _digest(checks),
        }
        receipt["provider_receipt_sha256"] = _digest(receipt)
        return VerifiedGitHubBackfillReceipt(
            receipt, _sentinel=_VERIFIED_RECEIPT_SENTINEL,
        )


__all__ = [
    "GitHubBackfillReceiptError",
    "GitHubBackfillReceiptReader",
    "VerifiedGitHubBackfillReceipt",
    "github_token_from_command",
    "validate_github_backfill_receipt",
]
