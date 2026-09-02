#!/usr/bin/env python3

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import unittest
import urllib.parse

sys.path.insert(0, str(Path(__file__).parent))

from workstream_github_backfill import (  # noqa: E402
    GitHubBackfillReceiptError,
    GitHubBackfillReceiptReader,
    VerifiedGitHubBackfillReceipt,
    github_token_from_command,
    validate_github_backfill_receipt,
)


HEAD = "a" * 40
MERGE = "b" * 40


class Response:
    def __init__(self, payload, *, url=None, raw=None):
        self.raw = raw if raw is not None else json.dumps(payload).encode()
        self.url = url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, size=-1):
        return self.raw if size < 0 else self.raw[:size]

    def geturl(self):
        return self.url


def pr_payload(**updates):
    payload = {
        "number": 106,
        "merged": True,
        "merged_at": "2026-09-02T12:34:56Z",
        "merge_commit_sha": MERGE,
        "head": {"sha": HEAD},
        "base": {"repo": {
            "id": 123456,
            "node_id": "R_kgDOExample",
            "full_name": "Generous-Corp/agent-workstream",
        }},
    }
    payload.update(updates)
    return payload


def check(
    name, *, status="completed", conclusion="success", head=HEAD,
    check_id=None,
):
    return {
        "id": (
            int.from_bytes(hashlib.sha256(name.encode()).digest()[:8], "big")
            if check_id is None else check_id
        ),
        "name": name,
        "status": status,
        "conclusion": conclusion,
        "head_sha": head,
        "details_url": f"https://github.com/example/checks/{name}",
    }


class FakeOpener:
    def __init__(self, pr=None, pages=None):
        self.pr = pr if pr is not None else pr_payload()
        self.pages = pages if pages is not None else [[check("unit")]]
        self.requests = []

    def __call__(self, request, *, timeout):
        self.requests.append((request, timeout))
        if "/pulls/" in request.full_url:
            return Response(self.pr, url=request.full_url)
        page = int(urllib.parse.parse_qs(
            urllib.parse.urlparse(request.full_url).query
        )["page"][0])
        batch = self.pages[page - 1] if page <= len(self.pages) else []
        total = sum(len(items) for items in self.pages)
        return Response(
            {"total_count": total, "check_runs": batch}, url=request.full_url,
        )


class GitHubBackfillReceiptTests(unittest.TestCase):
    def reader(self, opener, **kwargs):
        return GitHubBackfillReceiptReader("secret-token", opener=opener, **kwargs)

    def read(self, opener, **kwargs):
        return self.reader(opener, **kwargs).read(
            repository="Generous-Corp/agent-workstream",
            provider_repository_id="R_kgDOExample",
            pull_request_number=106,
            expected_head=HEAD,
            expected_merge_sha=MERGE,
        )

    def test_success_is_canonical_paginated_and_authenticated(self):
        first = [check(f"z-{number:03}") for number in range(100)]
        second = [check("a-final")]
        opener = FakeOpener(pages=[first, second])
        verified = self.read(opener)
        self.assertIsInstance(verified, VerifiedGitHubBackfillReceipt)
        receipt = verified.as_dict()
        self.assertEqual(receipt["repository"], "generous-corp/agent-workstream")
        self.assertEqual(receipt["provider_repository_id"], "R_kgDOExample")
        self.assertEqual(receipt["pull_request_number"], 106)
        self.assertEqual(receipt["pr_head"], HEAD)
        self.assertEqual(receipt["merge_sha"], MERGE)
        self.assertEqual(receipt["checks"][0]["name"], "a-final")
        canonical = lambda value: json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ).encode()
        self.assertEqual(
            receipt["checks_sha256"], hashlib.sha256(canonical(receipt["checks"])).hexdigest(),
        )
        unsigned = {key: value for key, value in receipt.items()
                    if key != "provider_receipt_sha256"}
        self.assertEqual(
            receipt["provider_receipt_sha256"], hashlib.sha256(canonical(unsigned)).hexdigest(),
        )
        self.assertEqual(validate_github_backfill_receipt(receipt), receipt)
        self.assertEqual(len(opener.requests), 3)
        for request, _timeout in opener.requests:
            self.assertEqual(urllib.parse.urlparse(request.full_url).hostname, "api.github.com")
            self.assertEqual(request.get_header("Authorization"), "Bearer secret-token")

    def test_duplicate_check_identity_across_pages_refuses(self):
        duplicate = check("same", check_id=123)
        with self.assertRaisesRegex(
            GitHubBackfillReceiptError, "github_checks_changed_during_read",
        ):
            self.read(FakeOpener(pages=[[duplicate], [deepcopy(duplicate)]]))

    def test_repository_identity_coordinate_and_pr_number_fail_closed(self):
        cases = [
            ("provider_repository_id", "wrong", "github_repository_identity_mismatch"),
        ]
        for field, value, code in cases:
            with self.subTest(code=code):
                args = {
                    "repository": "Generous-Corp/agent-workstream",
                    "provider_repository_id": "R_kgDOExample",
                    "pull_request_number": 106,
                    "expected_head": HEAD,
                    "expected_merge_sha": MERGE,
                }
                args[field] = value
                with self.assertRaisesRegex(GitHubBackfillReceiptError, code):
                    self.reader(FakeOpener()).read(**args)
        with self.assertRaisesRegex(GitHubBackfillReceiptError, "coordinate_mismatch"):
            self.read(FakeOpener(pr=pr_payload(base={"repo": {
                "id": 123456, "node_id": "R_kgDOExample", "full_name": "other/repo",
            }})))
        with self.assertRaisesRegex(GitHubBackfillReceiptError, "pr_number_mismatch"):
            self.read(FakeOpener(pr=pr_payload(number=107)))
        numeric = self.reader(FakeOpener()).read(
            repository="Generous-Corp/agent-workstream",
            provider_repository_id="123456",
            pull_request_number=106,
            expected_head=HEAD,
            expected_merge_sha=MERGE,
        )
        self.assertEqual(numeric.as_dict()["provider_repository_id"], "123456")

    def test_head_and_merge_failures(self):
        failures = [
            (pr_payload(head={"sha": "c" * 40}), "github_pr_head_mismatch"),
            (pr_payload(merged=False, merged_at=None), "github_pr_not_merged"),
            (pr_payload(merge_commit_sha="c" * 40), "github_merge_sha_mismatch"),
        ]
        for payload, code in failures:
            with self.subTest(code=code):
                with self.assertRaisesRegex(GitHubBackfillReceiptError, code):
                    self.read(FakeOpener(pr=payload))

    def test_checks_must_be_nonempty_completed_successful_and_exact_head(self):
        failures = [
            ([], "github_checks_empty_or_incomplete"),
            ([check("unit", status="in_progress", conclusion=None)],
             "github_checks_unsuccessful"),
            ([check("unit", conclusion="failure")], "github_checks_unsuccessful"),
            ([check("unit", head="c" * 40)], "github_check_head_mismatch"),
            ([check("unit", check_id=0)], "github_checks_changed_during_read"),
            ([check("unit", check_id="1")], "github_checks_changed_during_read"),
        ]
        for checks, code in failures:
            with self.subTest(code=code):
                with self.assertRaisesRegex(GitHubBackfillReceiptError, code):
                    self.read(FakeOpener(pages=[checks]))

    def test_pagination_and_byte_bounds(self):
        with self.assertRaisesRegex(GitHubBackfillReceiptError, "pagination_exceeded"):
            self.read(FakeOpener(pages=[
                [check(f"check-{page}-{item}") for item in range(100)]
                for page in range(2)
            ]), max_pages=1)
        oversized = Response({}, url=(
            "https://api.github.com/repos/Generous-Corp/agent-workstream/pulls/106"
        ), raw=b"x" * 65)

        def opener(_request, *, timeout):
            return oversized

        with self.assertRaisesRegex(GitHubBackfillReceiptError, "response_too_large"):
            self.read(opener, max_response_bytes=64, max_total_bytes=128)

    def test_validator_rejects_digest_and_noncanonical_checks(self):
        receipt = self.read(FakeOpener(pages=[[check("a"), check("b")]])).as_dict()
        bad_digest = deepcopy(receipt)
        bad_digest["checks_sha256"] = "0" * 64
        with self.assertRaisesRegex(GitHubBackfillReceiptError, "checks_digest_mismatch"):
            validate_github_backfill_receipt(bad_digest)
        unsorted = deepcopy(receipt)
        unsorted["checks"].reverse()
        with self.assertRaisesRegex(GitHubBackfillReceiptError, "not_canonical"):
            validate_github_backfill_receipt(unsorted)

    def test_verified_receipt_cannot_be_minted_from_a_plain_dict(self):
        verified = self.read(FakeOpener())
        receipt = verified.as_dict()
        self.assertNotIsInstance(receipt, VerifiedGitHubBackfillReceipt)
        with self.assertRaisesRegex(
            GitHubBackfillReceiptError, "verified_receipt_constructor_private",
        ):
            VerifiedGitHubBackfillReceipt(receipt)
        with self.assertRaisesRegex(
            GitHubBackfillReceiptError, "verified_receipt_constructor_private",
        ):
            VerifiedGitHubBackfillReceipt(receipt, _sentinel=object())
        with self.assertRaisesRegex(AttributeError, "immutable"):
            verified.receipt = receipt
        with self.assertRaisesRegex(TypeError, "cannot be subclassed"):
            class ForgedReceipt(VerifiedGitHubBackfillReceipt):
                pass
        detached = verified.as_dict()
        detached["repository"] = "attacker/repo"
        self.assertEqual(
            verified.as_dict()["repository"], "generous-corp/agent-workstream",
        )

    def test_token_command_is_bounded_and_noninteractive(self):
        self.assertEqual(github_token_from_command([
            sys.executable, "-c", "print('token-value')",
        ]), "token-value")
        failures = [
            ([sys.executable, "-c", "raise SystemExit(2)"], {}, "command_failed"),
            ([sys.executable, "-c", "print('two tokens')"], {}, "token_malformed"),
            ([sys.executable, "-c", "print('x' * 9000)"], {}, "too_large"),
            ([sys.executable, "-c", "import time; time.sleep(2)"],
             {"timeout": 0.05}, "timeout"),
        ]
        for argv, kwargs, code in failures:
            with self.subTest(code=code):
                with self.assertRaisesRegex(GitHubBackfillReceiptError, code):
                    github_token_from_command(argv, **kwargs)
        for invalid_timeout in (float("nan"), float("inf"), 61.0, 10**400):
            with self.subTest(token_timeout=invalid_timeout), self.assertRaisesRegex(
                GitHubBackfillReceiptError, "github_auth_timeout_invalid",
            ):
                github_token_from_command(
                    [sys.executable, "-c", "print('token')"],
                    timeout=invalid_timeout,
                )
        for invalid_timeout in (float("nan"), float("inf"), 61.0, 10**400):
            with self.subTest(reader_timeout=invalid_timeout), self.assertRaisesRegex(
                GitHubBackfillReceiptError, "github_timeout_invalid",
            ):
                GitHubBackfillReceiptReader(
                    "secret-token", opener=FakeOpener(), timeout=invalid_timeout,
                )


if __name__ == "__main__":
    unittest.main()
