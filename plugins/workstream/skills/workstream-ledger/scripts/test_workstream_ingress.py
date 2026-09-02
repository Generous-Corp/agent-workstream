#!/usr/bin/env python3

import argparse
import gc
import hashlib
import importlib.util
import io
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
import warnings
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("workstream_ingress.py")
SPEC = importlib.util.spec_from_file_location("workstream_ingress", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(MODULE)


def capture_payload(
    event_id="e1", *, workstream_id="ABC-12", prompt="prompt",
    prompt_sha256=None, captured_at="2026-08-14T01:00:00Z",
    context_url=None, redactions=0, truncated=False,
):
    if prompt_sha256 is None:
        prompt_sha256 = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    return {
        "schema_version": 1,
        "event_id": event_id,
        "captured_at": captured_at,
        "provider": "codex",
        "session_id": "session-a",
        "turn_id": f"turn-{event_id}",
        "surface_id": "surface-1",
        "workspace_id": "workspace-1",
        "cwd": "/repo",
        "workstream_id": workstream_id,
        "context_url": context_url,
        "prompt": prompt,
        "prompt_sha256": prompt_sha256,
        "redactions": redactions,
        "truncated": truncated,
    }


class WorkstreamIngressTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.env = mock.patch.dict(os.environ, {
            "WORKSTREAM_INGRESS_STATE_DIR": str(self.root / "state"),
            "WORKSTREAM_INGRESS_CONFIG": str(self.root / "config/config.json"),
            "WORKSTREAM_ID": "ABC-12",
            "WORKSTREAM_CONTEXT_URL": "https://linear.app/x/ABC-12",
            "WORKSTREAM_SURFACE_ID": "surface:92",
            "WORKSTREAM_WORKSPACE_ID": "workspace:9",
        }, clear=False)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def test_event_id_is_idempotent_for_same_provider_turn(self):
        payload = {"session_id": "s1", "turn_id": "t1", "cwd": "/tmp", "prompt": "hello"}
        first = MODULE.event_record(payload, "codex")
        second = MODULE.event_record(payload, "codex")
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual(first["workstream_id"], "ABC-12")

    def test_discarded_connection_closes_without_resource_warning(self):
        with warnings.catch_warnings(record=True) as observed:
            warnings.simplefilter("always", ResourceWarning)
            connection = MODULE.connect()
            del connection
            gc.collect()
        self.assertFalse([
            item for item in observed if item.category is ResourceWarning
        ])

    def test_redacts_credentials_and_oauth_query_values(self):
        prompt, count, truncated = MODULE.redact_prompt(
            "Authorization: Bearer abcdef token=topsecret "
            "https://user:password@example.test/callback?code=abc&safe=yes&state=xyz "
            "postgres://dbuser:dbpassword@db.example/app"
        )
        self.assertNotIn("abcdef", prompt)
        self.assertNotIn("topsecret", prompt)
        self.assertNotIn("user:password", prompt)
        self.assertNotIn("dbuser:dbpassword", prompt)
        self.assertIn("https://[REDACTED]@example.test", prompt)
        self.assertIn("postgres://[REDACTED]@db.example", prompt)
        self.assertIn("code=[REDACTED]", prompt)
        self.assertIn("safe=yes", prompt)
        self.assertIn("state=[REDACTED]", prompt)
        self.assertGreaterEqual(count, 2)
        self.assertFalse(truncated)

    def test_local_capture_precedes_remote_ack_and_survives_failure(self):
        payload = {"session_id": "s1", "turn_id": "t1", "cwd": "/tmp", "prompt": "new requirement"}
        with mock.patch("sys.stdin", io.StringIO(json.dumps(payload))), \
             mock.patch.object(MODULE, "upload_event", side_effect=RuntimeError("offline")), \
             mock.patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(MODULE.command_capture(mock.Mock(provider="codex")), 0)
        self.assertEqual(output.getvalue(), "{}\n")
        conn = MODULE.connect()
        row = conn.execute("SELECT prompt,remote_acked_at FROM events").fetchone()
        self.assertEqual(row, ("new requirement", None))

    def test_duplicate_capture_is_one_local_event(self):
        payload = {"session_id": "s1", "turn_id": "t1", "cwd": "/tmp", "prompt": "same"}
        with mock.patch.object(MODULE, "upload_event", return_value=False):
            for _ in range(2):
                with mock.patch("sys.stdin", io.StringIO(json.dumps(payload))), \
                     mock.patch("sys.stdout", new_callable=io.StringIO):
                    MODULE.command_capture(mock.Mock(provider="claude"))
        count = MODULE.connect().execute("SELECT COUNT(*) FROM events").fetchone()[0]
        self.assertEqual(count, 1)

    def test_malformed_capture_records_metadata_only_failure(self):
        with mock.patch("sys.stdin", io.StringIO("not-json")), \
             mock.patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(MODULE.command_capture(mock.Mock(provider="codex")), 0)
        failure = json.loads((self.root / "state/failures.jsonl").read_text())
        self.assertEqual(failure["stage"], "local-capture")
        self.assertNotIn("not-json", json.dumps(failure))

    def test_upload_writes_remote_ack_only_after_success(self):
        conn = MODULE.connect()
        event = MODULE.event_record(
            {"session_id": "s1", "turn_id": "t1", "cwd": "/tmp", "prompt": "ship it"}, "codex"
        )
        MODULE.insert_event(conn, event)
        response = {"id": 44, "html_url": "https://github.com/private/issues/1#issuecomment-44"}
        with mock.patch.object(MODULE, "gh", return_value=response) as call:
            self.assertTrue(MODULE.upload_event(conn, event["event_id"], {"repo": "o/r", "issue": 1}))
        sent = json.loads(call.call_args.kwargs["stdin"])
        self.assertIn(event["event_id"], sent["body"])
        row = conn.execute("SELECT remote_comment_id,remote_acked_at FROM events").fetchone()
        self.assertEqual(row[0], 44)
        self.assertIsNotNone(row[1])

    def test_remote_recovery_deduplicates_but_keeps_mutable_classification_open(self):
        capture = capture_payload()
        processed = {
            "schema_version": 2, "event_id": "e1", "processed_at": "2026-08-14T02:00:00Z",
            "disposition": "no-material-delta", "promoted_issue": None,
        }
        comments = [
            {"id": 1, "body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture), "html_url": "u1"},
            {"id": 2, "body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture), "html_url": "u2"},
            {"id": 3, "body": MODULE.comment_body(MODULE.PROCESSED_MARKER, processed),
             "html_url": "u3", "user": {"login": "trusted-bot"}},
        ]
        with mock.patch.object(MODULE, "gh", side_effect=[
            [{"number": 7, "url": "i", "title": "ingress"}], [comments]
        ]):
            events = MODULE.remote_events("o/r", "ABC-12")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["remote_url"], "u1")
        self.assertFalse(events[0]["classification_hint"]["authoritative"])

    def test_remote_recovery_refuses_conflicting_duplicate_capture(self):
        first = capture_payload(prompt="first")
        conflicting = capture_payload(prompt="second")
        comments = [
            {"id": 1, "body": MODULE.comment_body(MODULE.CAPTURE_MARKER, first)},
            {"id": 2, "body": MODULE.comment_body(
                MODULE.CAPTURE_MARKER, conflicting)},
        ]
        with mock.patch.object(MODULE, "gh", side_effect=[
            [{"number": 7, "url": "i", "title": "ingress"}], [comments],
        ]), self.assertRaisesRegex(ValueError, "conflicting_capture:e1"):
            MODULE.remote_events("o/r", "ABC-12")

    def test_remote_recovery_quarantines_malformed_captures_before_keys_or_sort(self):
        early = capture_payload("e-early", captured_at="2026-08-14T00:00:00Z")
        late = capture_payload("e-late", captured_at="2026-08-14T02:00:00Z")
        malformed = [
            {**late, "captured_at": 1},
            {**late, "event_id": []},
            {**late, "prompt": {}},
            {**late, "prompt_sha256": []},
            {**late, "redactions": {}},
            {**late, "truncated": []},
            {**late, "session_id": {}},
            {**late, "unexpected": "field"},
            {key: value for key, value in late.items() if key != "provider"},
            ["not", "an", "object"],
        ]
        comments = [
            {"body": body} for body in (None, 1, [], {})
        ] + [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, item)}
            for item in [*malformed, late, early]
        ] + [
            {"body": MODULE.comment_body(MODULE.BIND_MARKER, item)}
            for item in ([], ["not", "a", "binding"], "not-a-binding", {})
        ] + [
            {"body": MODULE.comment_body(MODULE.PROCESSED_MARKER, item)}
            for item in ([], ["not", "processed"], "not-processed", {})
        ]
        comments.insert(0, ["not", "a", "comment"])
        with mock.patch.object(MODULE, "gh", side_effect=[
            [{"number": 7, "url": "i", "title": "ingress"}], [comments],
        ]):
            events = MODULE.remote_events("o/r", "ABC-12")
        self.assertEqual([event["event_id"] for event in events], [
            "e-early", "e-late",
        ])

    def test_capture_integrity_matches_genuine_emitter_shapes(self):
        self.assertTrue(MODULE._is_exact_capture_payload(capture_payload()))

        raw_redacted = "Authorization: Bearer topsecret"
        prompt, redactions, truncated = MODULE.redact_prompt(raw_redacted)
        self.assertTrue(MODULE._is_exact_capture_payload(capture_payload(
            prompt=prompt,
            prompt_sha256=hashlib.sha256(raw_redacted.encode("utf-8")).hexdigest(),
            redactions=redactions,
            truncated=truncated,
        )))

        raw_truncated = "x" * (MODULE.MAX_PROMPT_BYTES + 1)
        prompt, redactions, truncated = MODULE.redact_prompt(raw_truncated)
        self.assertTrue(truncated)
        self.assertTrue(MODULE._is_exact_capture_payload(capture_payload(
            prompt=prompt,
            prompt_sha256=hashlib.sha256(raw_truncated.encode("utf-8")).hexdigest(),
            redactions=redactions,
            truncated=truncated,
        )))

    def test_capture_integrity_quarantines_oversize_stale_and_inconsistent_fields(self):
        invalid = [
            capture_payload(prompt="x" * (MODULE.MAX_PROMPT_BYTES + 1)),
            capture_payload(prompt_sha256="0" * 64),
            capture_payload(redactions=True),
            capture_payload(redactions=-1),
            capture_payload(redactions=MODULE.MAX_REDACTIONS + 1),
            capture_payload(redactions=1),
            capture_payload(truncated=1),
            capture_payload(truncated=True),
            capture_payload(
                prompt="x" * (MODULE.MAX_PROMPT_BYTES + 1) + MODULE.TRUNCATION_SUFFIX,
                truncated=True,
            ),
        ]
        self.assertTrue(all(
            not MODULE._is_exact_capture_payload(payload) for payload in invalid
        ))

    def test_remote_recovery_refuses_malformed_transport_envelopes(self):
        for issues in (None, {}, "not-issues", [None], [1]):
            with self.subTest(kind="issues", issues=issues), mock.patch.object(
                MODULE, "gh", return_value=issues,
            ), self.assertRaisesRegex(ValueError, "ingress_issues_envelope_invalid"):
                MODULE.remote_events("o/r", "ABC-12")

        with mock.patch.object(MODULE, "gh", return_value=[{}]), \
             self.assertRaisesRegex(ValueError, "ingress_issue_route_invalid"):
            MODULE.remote_events("o/r", "ABC-12")

        issue_list = [{"number": 7, "url": "i", "title": "ingress"}]
        for pages in (None, {}, "not-comments"):
            with self.subTest(kind="comments", pages=pages), mock.patch.object(
                MODULE, "gh", side_effect=[issue_list, pages],
            ), self.assertRaisesRegex(
                ValueError, "ingress_comments_envelope_invalid:7",
            ):
                MODULE.remote_events("o/r", "ABC-12")

        with mock.patch.object(MODULE, "gh", side_effect=[
            issue_list, [[], {}],
        ]), self.assertRaisesRegex(ValueError, "ingress_comment_pages_invalid:7"):
            MODULE.remote_events("o/r", "ABC-12")

        for pages in ([None], [{}], [1], [[None]], [[{}]]):
            with self.subTest(kind="comment-items", pages=pages), mock.patch.object(
                MODULE, "gh", side_effect=[issue_list, pages],
            ), self.assertRaisesRegex(ValueError, "ingress_comment_items_invalid:7"):
                MODULE.remote_events("o/r", "ABC-12")

    def test_remote_event_state_uses_the_same_transport_envelope_validation(self):
        for pages in (None, {}, [None], [{}], [1], [[None]], [[{}]]):
            expected = (
                "ingress_comments_envelope_invalid:7"
                if not isinstance(pages, list)
                else "ingress_comment_items_invalid:7"
            )
            with self.subTest(pages=pages), mock.patch.object(
                MODULE, "gh", return_value=pages,
            ), self.assertRaisesRegex(ValueError, expected):
                MODULE.remote_event_state("o/r", 7, "e1")

        malformed_json = (
            MODULE.CAPTURE_MARKER + "\n```json\n{not-json\n```"
        )
        with mock.patch.object(MODULE, "gh", return_value=[[
            {"body": malformed_json},
        ]]):
            state = MODULE.remote_event_state("o/r", 7, "e1")
        self.assertEqual(state, {
            "capture": None, "promotion": None, "processed": None,
        })

        valid = capture_payload()
        malformed = {**valid, "captured_at": 1}
        with mock.patch.object(MODULE, "gh", return_value=[[
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, malformed)},
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, valid)},
        ]]):
            state = MODULE.remote_event_state("o/r", 7, "e1")
        self.assertEqual(state["capture"], valid)

    def test_remote_recovery_refuses_non_object_promotion_as_schema(self):
        for promotion in ([], ["not", "a", "promotion"], "not-a-promotion", {}):
            comments = [{"body": MODULE.comment_body(
                MODULE.PROMOTION_MARKER, promotion)}]
            with self.subTest(promotion=promotion), mock.patch.object(
                MODULE, "gh", side_effect=[
                    [{"number": 7, "url": "i", "title": "ingress"}], [comments],
                ],
            ), self.assertRaisesRegex(ValueError, "promotion_marker_schema_invalid"):
                MODULE.remote_events("o/r", "ABC-12")

    def test_remote_recovery_quarantines_unhashable_binding_event_id(self):
        capture = capture_payload()
        comments = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture)},
            {"body": MODULE.comment_body(MODULE.BIND_MARKER, {
                "event_id": [], "workstream_id": "ABC-12", "context_url": None,
            })},
        ]
        with mock.patch.object(MODULE, "gh", side_effect=[
            [{"number": 7, "url": "i", "title": "ingress"}], [comments],
        ]):
            events = MODULE.remote_events("o/r", "ABC-12")
        self.assertEqual([event["event_id"] for event in events], ["e1"])

    def test_remote_recovery_refuses_unhashable_promotion_event_id_as_schema(self):
        promotion = {
            "schema_version": 2, "event_id": [], "prompt_sha256": "a" * 64,
            "workstream_id": "ABC-12", "plan_revision": "b" * 64,
            "authority": {}, "ingress_route": {}, "expected_material_revision": 0,
            "changes": [], "source_captured_at": "2026-08-14T01:00:00Z",
            "promotion_id": "wsp_bad",
        }
        comments = [{"body": MODULE.comment_body(
            MODULE.PROMOTION_MARKER, promotion)}]
        with mock.patch.object(MODULE, "gh", side_effect=[
            [{"number": 7, "url": "i", "title": "ingress"}], [comments],
        ]), self.assertRaisesRegex(ValueError, "promotion_marker_event_id_invalid"):
            MODULE.remote_events("o/r", "ABC-12")

    def test_remote_binding_promotes_an_initially_unbound_event(self):
        capture = capture_payload(workstream_id=None)
        binding = {"event_id": "e1", "workstream_id": "ABC-12", "context_url": "https://linear/ABC-12"}
        comments = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture), "html_url": "u1"},
            {"body": MODULE.comment_body(MODULE.BIND_MARKER, binding), "html_url": "u2"},
        ]
        with mock.patch.object(MODULE, "gh", side_effect=[
            [{"number": 7, "url": "i", "title": "ingress"}], [comments]
        ]):
            events = MODULE.remote_events("o/r", "ABC-12")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["context_url"], "https://linear/ABC-12")

    def test_remote_unbind_supersedes_a_wrong_binding(self):
        capture = capture_payload(workstream_id=None)
        binding = {"event_id": "e1", "workstream_id": "ABC-12", "context_url": "https://linear/ABC-12"}
        unbinding = {"event_id": "e1", "workstream_id": None, "context_url": None}
        comments = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture), "html_url": "u1"},
            {"body": MODULE.comment_body(MODULE.BIND_MARKER, binding), "html_url": "u2"},
            {"body": MODULE.comment_body(MODULE.BIND_MARKER, unbinding), "html_url": "u3"},
        ]
        side_effect = [[{"number": 7, "url": "i", "title": "ingress"}], [comments]]
        with mock.patch.object(MODULE, "gh", side_effect=side_effect):
            self.assertEqual(MODULE.remote_events("o/r", "ABC-12"), [])

    def test_bind_fails_closed_without_exact_identity(self):
        args = mock.Mock(
            workstream="ABC-12",
            context_url="https://linear/ABC-12",
            event=None,
            session=None,
            surface=None,
            limit=100,
        )
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "cwd-only binding is unsafe"):
                MODULE.command_bind(args)

    def test_bind_by_session_does_not_capture_a_sibling_tab(self):
        conn = MODULE.connect()
        for event_id, session in (("mine", "session-a"), ("sibling", "session-b")):
            conn.execute(
                "INSERT INTO events (event_id,captured_at,provider,session_id,cwd,prompt,prompt_sha256,redactions,truncated) VALUES (?,?,?,?,?,?,?,?,?)",
                (event_id, "2026-08-14T00:00:00Z", "codex", session, "/same/repo", "p", "h", 0, 0),
            )
        conn.commit()
        args = mock.Mock(
            workstream="ABC-12",
            context_url="https://linear/ABC-12",
            event=None,
            session="session-a",
            surface=None,
            limit=100,
        )
        with mock.patch("sys.stdout", new_callable=io.StringIO):
            MODULE.command_bind(args)
        rows = MODULE.connect().execute(
            "SELECT event_id,workstream_id FROM events ORDER BY event_id"
        ).fetchall()
        self.assertEqual(rows, [("mine", "ABC-12"), ("sibling", None)])

    def test_unbind_requires_expected_workstream_and_exact_identity(self):
        conn = MODULE.connect()
        for event_id, session in (("mine", "session-a"), ("sibling", "session-b")):
            conn.execute(
                "INSERT INTO events (event_id,captured_at,provider,session_id,workstream_id,prompt,prompt_sha256,redactions,truncated) VALUES (?,?,?,?,?,?,?,?,?)",
                (event_id, "2026-08-14T00:00:00Z", "codex", session, "ABC-12", "p", "h", 0, 0),
            )
        conn.commit()
        args = mock.Mock(
            workstream="ABC-12",
            event=None,
            session="session-a",
            surface=None,
            limit=100,
        )
        with mock.patch("sys.stdout", new_callable=io.StringIO):
            MODULE.command_unbind(args)
        rows = MODULE.connect().execute(
            "SELECT event_id,workstream_id FROM events ORDER BY event_id"
        ).fetchall()
        self.assertEqual(rows, [("mine", None), ("sibling", "ABC-12")])

    def test_new_remote_issue_uses_create_response_without_search_index(self):
        created = {
            "number": 4, "title": "[Workstream ingress] test 2026-08",
            "html_url": "https://github/private/issues/4", "state": "open",
        }
        with mock.patch.object(MODULE.socket, "gethostname", return_value="test"), \
             mock.patch.object(MODULE.subprocess, "run"), \
             mock.patch.object(MODULE, "gh", side_effect=[[], created]) as gh:
            issue = MODULE.ensure_remote_issue("o/r")
        self.assertEqual(issue["number"], 4)
        self.assertEqual(issue["url"], created["html_url"])
        self.assertEqual(gh.call_count, 2)

    def test_explicit_machine_names_the_rotating_issue(self):
        month = MODULE.datetime.now(MODULE.timezone.utc).strftime("%Y-%m")
        existing = [{
            "number": 4, "title": f"[Workstream ingress] m5 {month}",
            "url": "https://github/private/issues/4", "state": "OPEN",
        }]
        with mock.patch.object(MODULE.subprocess, "run"), \
             mock.patch.object(MODULE, "gh", return_value=existing) as gh:
            issue = MODULE.ensure_remote_issue("o/r", "m5")
        self.assertEqual(issue["number"], 4)
        self.assertNotIn("--search", gh.call_args.args[0])

    def test_prune_keeps_unuploaded_events(self):
        conn = MODULE.connect()
        old = "2020-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO events (event_id,captured_at,provider,prompt,prompt_sha256,redactions,truncated,remote_acked_at) VALUES (?,?,?,?,?,?,?,?)",
            ("remote", old, "codex", "x", "h", 0, 0, old),
        )
        conn.execute(
            "INSERT INTO events (event_id,captured_at,provider,prompt,prompt_sha256,redactions,truncated) VALUES (?,?,?,?,?,?,?)",
            ("local", old, "codex", "y", "h", 0, 0),
        )
        conn.commit()
        self.assertEqual(MODULE.prune(conn, {"local_retention_days": 30}), 1)
        remaining = conn.execute("SELECT event_id FROM events").fetchall()
        self.assertEqual(remaining, [("local",)])

    def test_bound_evicts_only_remote_acked_rows(self):
        conn = MODULE.connect()
        for event_id, ack in (("remote", "2026-08-14T00:00:00Z"), ("local", None)):
            conn.execute(
                "INSERT INTO events (event_id,captured_at,provider,prompt,prompt_sha256,redactions,truncated,remote_acked_at) VALUES (?,?,?,?,?,?,?,?)",
                (event_id, "2026-08-14T00:00:00Z", "codex", "12345", "h", 0, 0, ack),
            )
        conn.commit()
        self.assertTrue(MODULE.enforce_bound(conn, {"max_local_bytes": 5}))
        self.assertEqual(conn.execute("SELECT event_id FROM events").fetchall(), [("local",)])

    def test_bound_refuses_to_delete_local_only_rows(self):
        conn = MODULE.connect()
        conn.execute(
            "INSERT INTO events (event_id,captured_at,provider,prompt,prompt_sha256,redactions,truncated) VALUES (?,?,?,?,?,?,?)",
            ("local", "2026-08-14T00:00:00Z", "codex", "123456", "h", 0, 0),
        )
        conn.commit()
        self.assertFalse(MODULE.enforce_bound(conn, {"max_local_bytes": 5}))
        self.assertEqual(conn.execute("SELECT event_id FROM events").fetchall(), [("local",)])


class CredentialPathTests(unittest.TestCase):
    """A non-interactive capture launcher must diagnose credential failures."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.token = self.root / "token"
        self.env = mock.patch.dict(os.environ, {
            "WORKSTREAM_INGRESS_STATE_DIR": str(self.root / "state"),
            "WORKSTREAM_INGRESS_CONFIG": str(self.root / "config/config.json"),
            "WORKSTREAM_INGRESS_TOKEN_FILE": str(self.token),
        }, clear=False)
        self.env.start()
        for leaked in ("GH_TOKEN", "GITHUB_TOKEN", "WORKSTREAM_INGRESS_GH_BIN"):
            os.environ.pop(leaked, None)

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def _write_token(self, value: str, mode: int = 0o600) -> None:
        self.token.write_text(value)
        os.chmod(self.token, mode)

    # --- failure classification ------------------------------------------
    def test_classifies_the_three_observed_remote_failures(self):
        # Representative remote failure shapes.
        self.assertEqual(
            MODULE.classify_remote_failure("[Errno 2] No such file or directory: 'gh'"),
            "gh-missing",
        )
        # GitHub names an IP only for UNAUTHENTICATED requests, so this is a
        # missing-credential report even though it reads as a rate limit.
        self.assertEqual(
            MODULE.classify_remote_failure(
                "gh: API rate limit exceeded for 73.189.56.7. (But here's the good news...)"
            ),
            "unauthenticated",
        )
        self.assertEqual(
            MODULE.classify_remote_failure("gh: Requires authentication (HTTP 401)"),
            "unauthenticated",
        )
        self.assertEqual(
            MODULE.classify_remote_failure(
                "gh: No server is currently available to service your request. (HTTP 503)"
            ),
            "github-unavailable",
        )

    def test_authenticated_rate_limit_is_not_reported_as_missing_credentials(self):
        # The same text WITHOUT an IP is a genuine quota problem and must not
        # send anyone hunting for a credential that is present and working.
        self.assertEqual(
            MODULE.classify_remote_failure("gh: API rate limit exceeded for user ID 25807."),
            "rate-limited",
        )

    # --- gh resolution ----------------------------------------------------
    def test_resolves_gh_outside_path_via_explicit_override(self):
        fake = self.root / "gh"
        fake.write_text("#!/bin/sh\nexit 0\n")
        os.chmod(fake, 0o755)
        with mock.patch.dict(os.environ, {"WORKSTREAM_INGRESS_GH_BIN": str(fake)}):
            self.assertEqual(MODULE.gh_binary(), str(fake))

    def test_resolves_gh_from_standard_install_locations_when_path_lacks_it(self):
        location = self.root / "brewbin"
        location.mkdir()
        fake = location / "gh"
        fake.write_text("#!/bin/sh\nexit 0\n")
        os.chmod(fake, 0o755)
        with mock.patch.dict(os.environ, {"PATH": "/nonexistent"}), \
                mock.patch.object(MODULE, "GH_SEARCH_PATHS", (str(location),)):
            self.assertEqual(MODULE.gh_binary(), str(fake))

    def test_missing_gh_names_the_searched_locations(self):
        with mock.patch.dict(os.environ, {"PATH": "/nonexistent"}), \
                mock.patch.object(MODULE, "GH_SEARCH_PATHS", ("/also-nonexistent",)):
            with self.assertRaises(RuntimeError) as caught:
                MODULE.gh_binary()
        self.assertIn("/also-nonexistent", str(caught.exception))
        self.assertEqual(MODULE.classify_remote_failure(str(caught.exception)), "gh-missing")

    # --- token handling ---------------------------------------------------
    def test_file_backed_token_authenticates_the_call(self):
        self._write_token("ghp_exampletokenvalue")
        self.assertEqual(MODULE.gh_env(), {"GH_TOKEN": "ghp_exampletokenvalue"})

    def test_existing_environment_token_is_left_alone(self):
        self._write_token("ghp_filetoken")
        with mock.patch.dict(os.environ, {"GH_TOKEN": "ghp_envtoken"}):
            self.assertEqual(MODULE.gh_env(), {})

    def test_absent_token_file_is_not_an_error(self):
        self.assertEqual(MODULE.gh_env(), {})

    def test_group_readable_token_is_refused(self):
        self._write_token("ghp_looseperms", mode=0o644)
        with self.assertRaises(RuntimeError) as caught:
            MODULE.gh_env()
        self.assertIn("0600", str(caught.exception))
        self.assertNotIn("ghp_looseperms", str(caught.exception))

    def test_token_value_never_reaches_the_failure_log(self):
        self._write_token("ghp_supersecretvalue", mode=0o644)
        try:
            MODULE.gh_env()
        except RuntimeError as error:
            MODULE.record_failure("remote-upload", error)
        log = MODULE.state_root() / "failures.jsonl"
        self.assertTrue(log.exists())
        self.assertNotIn("ghp_supersecretvalue", log.read_text())

    def test_exception_secrets_never_reach_the_failure_log(self):
        secret = "ghp_" + "abcdefghijklmnopqrstuvwxyz" + "123456"
        url = "postgres://dbuser:dbpassword@db.example/app"
        MODULE.record_failure("remote-upload", RuntimeError(f"failed {secret} at {url}"))
        text = (MODULE.state_root() / "failures.jsonl").read_text()
        self.assertNotIn(secret, text)
        self.assertNotIn("dbuser:dbpassword", text)
        self.assertIn("[REDACTED]", text)

    # --- backlog drain ----------------------------------------------------
    def _seed(self, conn, *event_ids):
        for index, event_id in enumerate(event_ids):
            conn.execute(
                "INSERT INTO events (event_id,captured_at,provider,prompt,prompt_sha256,"
                "redactions,truncated) VALUES (?,?,?,?,?,0,0)",
                (event_id, f"2026-08-1{index}T00:00:00Z", "codex", "p", "h"),
            )
        conn.commit()

    def test_a_successful_capture_drains_older_pending_rows(self):
        conn = MODULE.connect()
        self._seed(conn, "old1", "old2", "current")
        with mock.patch.object(MODULE, "upload_event", return_value=True) as upload:
            drained = MODULE.drain_pending(conn, {"repo": "o/r", "issue": 4}, "current", 5)
        self.assertEqual(drained, 2)
        self.assertNotIn("current", [call.args[1] for call in upload.call_args_list])

    def test_drain_stops_at_the_first_failure_instead_of_hammering(self):
        conn = MODULE.connect()
        self._seed(conn, "old1", "old2", "old3", "current")
        with mock.patch.object(MODULE, "upload_event", side_effect=RuntimeError("gh: HTTP 503")) as upload:
            drained = MODULE.drain_pending(conn, {"repo": "o/r", "issue": 4}, "current", 5)
        self.assertEqual(drained, 0)
        self.assertEqual(upload.call_count, 1)
        self.assertIn("github-unavailable", (MODULE.state_root() / "failures.jsonl").read_text())

    def test_drain_is_bounded_so_the_hook_stays_fast(self):
        conn = MODULE.connect()
        self._seed(conn, *[f"old{index}" for index in range(10)], "current")
        with mock.patch.object(MODULE, "upload_event", return_value=True) as upload:
            MODULE.drain_pending(conn, {"repo": "o/r", "issue": 4}, "current", 3)
        self.assertEqual(upload.call_count, 3)

    # --- the hook must never block the prompt -----------------------------
    def test_capture_survives_a_dead_remote_and_keeps_the_row(self):
        payload = json.dumps({"session_id": "s", "turn_id": "t", "cwd": "/tmp", "prompt": "hi"})
        args = argparse.Namespace(provider="codex")
        with mock.patch.object(MODULE, "upload_event", side_effect=RuntimeError("gh: HTTP 503")), \
                mock.patch.object(MODULE.sys, "stdin", io.StringIO(payload)), \
                mock.patch.object(MODULE.sys, "stdout", io.StringIO()):
            self.assertEqual(MODULE.command_capture(args), 0)
        conn = MODULE.connect()
        pending = conn.execute(
            "SELECT COUNT(*) FROM events WHERE remote_acked_at IS NULL"
        ).fetchone()[0]
        self.assertEqual(pending, 1, "a failed upload must leave the row for a later flush")


class FlushReportingTests(unittest.TestCase):
    """A flush that stops must say why, in its own output.

    Stopping at the first refusal is correct. Stopping silently is the same
    defect as a capture that fails without saying so: an operator should not
    need a separate failures.jsonl read to learn that GitHub returned 503,
    while the flush itself printed only
    `{"pending_before": 58, "uploaded": 0}`.
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.env = mock.patch.dict(os.environ, {
            "WORKSTREAM_INGRESS_STATE_DIR": str(self.root / "state"),
            "WORKSTREAM_INGRESS_CONFIG": str(self.root / "config/config.json"),
        }, clear=False)
        self.env.start()
        conn = MODULE.connect()
        for index in range(3):
            conn.execute(
                "INSERT INTO events (event_id,captured_at,provider,prompt,prompt_sha256,"
                "redactions,truncated) VALUES (?,?,?,?,?,0,0)",
                (f"e{index}", f"2026-08-1{index}T00:00:00Z", "codex", "p", "h"),
            )
        conn.commit()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def _flush(self):
        out = io.StringIO()
        with mock.patch.object(MODULE.sys, "stdout", out):
            code = MODULE.command_flush(argparse.Namespace())
        return code, json.loads(out.getvalue())

    def _failure_log(self):
        log = MODULE.state_root() / "failures.jsonl"
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines() if line.strip()]

    def test_a_stopped_flush_names_the_cause_in_its_output(self):
        with mock.patch.object(MODULE, "upload_event",
                               side_effect=RuntimeError("gh: HTTP 503 no server available")):
            code, summary = self._flush()
        self.assertEqual(code, 1)
        self.assertEqual(summary["stopped_because"], "github-unavailable")
        self.assertEqual(summary["remaining"], 3)
        self.assertIn("503", summary["stopped_detail"])

    def test_a_stopped_flush_never_returns_or_persists_secrets(self):
        secret = "ghp_" + "abcdefghijklmnopqrstuvwxyz" + "123456"
        url = "https://user:password@example.test/private"
        with mock.patch.object(MODULE, "upload_event",
                               side_effect=RuntimeError(f"failed {secret} at {url}")):
            code, summary = self._flush()
        rendered = json.dumps(summary)
        persisted = (MODULE.state_root() / "failures.jsonl").read_text()
        for value in (secret, "user:password"):
            self.assertNotIn(value, rendered)
            self.assertNotIn(value, persisted)
        self.assertEqual(code, 1)
        self.assertIn("[REDACTED]", rendered)

    def test_a_stopped_flush_records_the_failure(self):
        with mock.patch.object(MODULE, "upload_event",
                               side_effect=RuntimeError("gh: Requires authentication (HTTP 401)")):
            self._flush()
        entries = self._failure_log()
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["stage"], "flush")
        self.assertEqual(entries[0]["cause"], "unauthenticated")

    def test_a_flush_still_stops_at_the_first_refusal(self):
        # Reporting the reason must not turn into retrying past it: if the
        # remote just refused, the rest of the backlog will refuse identically.
        with mock.patch.object(MODULE, "upload_event",
                               side_effect=RuntimeError("gh: HTTP 503")) as upload:
            self._flush()
        self.assertEqual(upload.call_count, 1)

    def test_a_clean_flush_stays_quiet(self):
        # The negative control: a flush that drains everything must not invent
        # a stop reason or write to the failure log, or the signal is noise.
        with mock.patch.object(MODULE, "upload_event", return_value=True):
            code, summary = self._flush()
        self.assertEqual(code, 0)
        self.assertEqual(summary["uploaded"], 3)
        self.assertNotIn("stopped_because", summary)
        self.assertEqual(self._failure_log(), [])

    def test_a_partial_flush_reports_what_is_left(self):
        outcomes = [True, True, RuntimeError("gh: HTTP 503")]

        def upload(conn, event_id, config):
            result = outcomes.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        with mock.patch.object(MODULE, "upload_event", side_effect=upload):
            code, summary = self._flush()
        self.assertEqual(code, 1)
        self.assertEqual(summary["uploaded"], 2)
        self.assertEqual(summary["remaining"], 1)


class PersistedBindingTests(unittest.TestCase):
    """A binding must outlive the rows it was applied to.

    Before this, `bind` only backfilled the events that already existed, so
    every LATER turn of the same session was captured unbound again and nothing
    revisited it.
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.env = mock.patch.dict(os.environ, {
            "WORKSTREAM_INGRESS_STATE_DIR": str(self.root / "state"),
            "WORKSTREAM_INGRESS_CONFIG": str(self.root / "config/config.json"),
        }, clear=False)
        self.env.start()
        for leaked in ("WORKSTREAM_ID", "WORKSTREAM_CONTEXT_URL",
                       "WORKSTREAM_SURFACE_ID", "WORKSTREAM_WORKSPACE_ID",
                       "WHENCE_WORKSTREAM_ID", "CMUX_SURFACE_ID", "CMUX_WORKSPACE_ID"):
            os.environ.pop(leaked, None)

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def _capture(self, session_id, cwd="/Users/x/Code/project", turn="t1", surface=None):
        payload = {"session_id": session_id, "turn_id": turn, "cwd": cwd, "prompt": "p"}
        overrides = {"WORKSTREAM_SURFACE_ID": surface} if surface else {}
        with mock.patch.dict(os.environ, overrides), \
                mock.patch.object(MODULE, "upload_event", return_value=True), \
                mock.patch.object(MODULE.sys, "stdin", io.StringIO(json.dumps(payload))), \
                mock.patch.object(MODULE.sys, "stdout", io.StringIO()):
            code = MODULE.command_capture(argparse.Namespace(provider="codex"))
        self.assertEqual(code, 0)

    def _bind(self, workstream, **identity):
        args = argparse.Namespace(
            workstream=workstream, context_url=f"https://linear.app/x/{workstream}",
            event=identity.get("event"), session=identity.get("session"),
            surface=identity.get("surface"), limit=100)
        with mock.patch.object(MODULE, "gh", return_value={"id": 1, "html_url": "u"}), \
                mock.patch.object(MODULE.sys, "stdout", io.StringIO()):
            MODULE.command_bind(args)

    def _unbind(self, workstream, **identity):
        args = argparse.Namespace(
            workstream=workstream, event=identity.get("event"),
            session=identity.get("session"), surface=identity.get("surface"), limit=100)
        with mock.patch.object(MODULE, "gh", return_value={"id": 1, "html_url": "u"}), \
                mock.patch.object(MODULE.sys, "stdout", io.StringIO()):
            MODULE.command_unbind(args)

    def _workstreams(self):
        conn = MODULE.connect()
        return dict(conn.execute("SELECT event_id, workstream_id FROM events").fetchall())

    # --- the recurrence this exists to stop --------------------------------
    def test_a_bound_session_binds_its_later_turns_automatically(self):
        self._capture("s1", turn="t1")
        self._bind("ABC-35", session="s1")
        self._capture("s1", turn="t2")
        values = list(self._workstreams().values())
        self.assertEqual(len(values), 2)
        self.assertTrue(all(v == "ABC-35" for v in values),
                        f"a later turn of a bound session was left unbound: {values}")

    def test_binding_by_exact_event_persists_that_events_session(self):
        self._capture("s1", turn="t1")
        conn = MODULE.connect()
        event_id = conn.execute("SELECT event_id FROM events").fetchone()[0]
        self._bind("ABC-35", event=event_id)
        self._capture("s1", turn="t2")
        self.assertTrue(all(v == "ABC-35" for v in self._workstreams().values()))

    # --- cwd is not a trusted identity -------------------------------------
    def test_cwd_is_never_used_to_bind(self):
        # Two sessions in one checkout must remain independently bindable.
        shared = "/Users/x/Code/project"
        self._capture("bound-session", cwd=shared)
        self._capture("other-session", cwd=shared)
        self._bind("ABC-35", session="bound-session")
        self._capture("other-session", cwd=shared, turn="t2")
        conn = MODULE.connect()
        rows = conn.execute(
            "SELECT session_id, workstream_id FROM events ORDER BY captured_at"
        ).fetchall()
        for session_id, workstream in rows:
            if session_id == "other-session":
                self.assertIsNone(
                    workstream,
                    "a session was bound by cwd; several tabs share one checkout")

    def test_an_earlier_row_with_a_different_workstream_is_not_rewritten(self):
        self._capture("s1", turn="t1")
        conn = MODULE.connect()
        conn.execute("UPDATE events SET workstream_id='ABC-12' WHERE session_id='s1'")
        conn.commit()
        self._bind("ABC-35", session="s1")
        self.assertEqual(list(self._workstreams().values()), ["ABC-12"],
                         "a deliberate earlier binding was overwritten")

    def test_unbinding_stops_later_turns_from_re_binding(self):
        # Correcting a mistake must actually correct it: without forgetting the
        # persisted identity, the very next turn would silently re-apply the
        # same wrong workstream.
        self._capture("s1", turn="t1")
        self._bind("ABC-17", session="s1")
        self._unbind("ABC-17", session="s1")
        self._capture("s1", turn="t2")
        self.assertTrue(all(v is None for v in self._workstreams().values()),
                        "a corrected binding came back on the next turn")

    def test_a_surface_binds_when_there_is_no_session(self):
        self._capture(None, surface="surface:92")
        self._bind("ABC-35", surface="surface:92")
        self._capture(None, surface="surface:92", turn="t2")
        self.assertTrue(all(v == "ABC-35" for v in self._workstreams().values()))

    def test_an_explicit_workstream_wins_over_a_persisted_binding(self):
        # A caller naming a workstream for THIS turn is making a more specific
        # statement than a binding recorded earlier.
        self._bind("ABC-35", session="s1")
        with mock.patch.dict(os.environ, {"WORKSTREAM_ID": "ABC-99"}):
            self._capture("s1")
        self.assertEqual(list(self._workstreams().values()), ["ABC-99"])

    # --- the negative: no binding must still capture, and still be visible --
    def test_an_unbound_session_still_captures_and_is_counted(self):
        # This is the case that regressed silently for 55 events: capture must
        # succeed, the row must survive, and the gap must be COUNTED rather
        # than guessed at.
        self._capture("no-binding-session")
        conn = MODULE.connect()
        rows = conn.execute("SELECT workstream_id FROM events").fetchall()
        self.assertEqual(rows, [(None,)])
        out = io.StringIO()
        with mock.patch.object(MODULE.sys, "stdout", out):
            MODULE.command_status(argparse.Namespace())
        status = json.loads(out.getvalue())
        self.assertEqual(status["unbound_events"], 1)
        self.assertEqual(status["unbound_sessions"], 1)
        self.assertEqual(status["persisted_bindings"], 0)
        self.assertIsNotNone(status["oldest_unbound_age_hours"])

    def test_status_counts_unbound_sessions_separately_from_events(self):
        # "many sessions, days old" and "one session, minutes old" call for
        # opposite responses, so one number cannot serve both.
        for session in ("a", "b", "c"):
            self._capture(session)
            self._capture(session, turn="t2")
        self._bind("ABC-35", session="a")
        out = io.StringIO()
        with mock.patch.object(MODULE.sys, "stdout", out):
            MODULE.command_status(argparse.Namespace())
        status = json.loads(out.getvalue())
        self.assertEqual(status["unbound_events"], 4)
        self.assertEqual(status["unbound_sessions"], 2)
        self.assertEqual(status["persisted_bindings"], 1)

    def test_a_binding_never_records_a_cwd_identity(self):
        # Belt and braces: the schema itself refuses any identity kind other
        # than the two trustworthy ones, so no future caller can add cwd
        # inference without changing this constraint deliberately.
        conn = MODULE.connect()
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO bindings (kind, identity, workstream_id, bound_at) "
                "VALUES ('cwd', '/Users/x/Code/project', 'ABC-35', '2026-08-17T00:00:00Z')")


class RatchetTests(unittest.TestCase):
    """Alert on growth or an invariant violation, not a historical level."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.env = mock.patch.dict(os.environ, {
            "WORKSTREAM_INGRESS_STATE_DIR": str(self.root / "state"),
            "WORKSTREAM_INGRESS_CONFIG": str(self.root / "config/config.json"),
        }, clear=False)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def _add_unbound(self, event_id, session_id="s1"):
        conn = MODULE.connect()
        conn.execute(
            "INSERT INTO events (event_id,captured_at,provider,session_id,prompt,"
            "prompt_sha256,redactions,truncated) VALUES (?,?,?,?,?,?,0,0)",
            (event_id, "2026-08-17T00:00:00Z", "codex", session_id, "p", "h"))
        conn.commit()

    def _ratchet(self):
        out = io.StringIO()
        with mock.patch.object(MODULE.sys, "stdout", out):
            code = MODULE.command_ratchet(argparse.Namespace())
        return code, json.loads(out.getvalue())

    def test_the_first_observation_records_a_baseline_without_alerting(self):
        self._add_unbound("e1")
        code, report = self._ratchet()
        self.assertEqual(code, 0)
        self.assertTrue(report["first_observation"])
        self.assertEqual(report["alerts"], [])

    def test_a_level_that_does_not_move_stays_green(self):
        # The whole point: a large known backlog must not hold the check red.
        for index in range(50):
            self._add_unbound(f"e{index}")
        self._ratchet()
        code, report = self._ratchet()
        self.assertEqual(code, 0)
        self.assertEqual(report["unbound_events"], 50)
        self.assertEqual(report["grew_by"], 0)

    def test_growth_since_the_last_check_alerts(self):
        self._add_unbound("e1")
        self._ratchet()
        self._add_unbound("e2")
        self._add_unbound("e3")
        code, report = self._ratchet()
        self.assertEqual(code, 1)
        self.assertEqual(report["grew_by"], 2)
        self.assertTrue(any("grew by 2" in alert for alert in report["alerts"]))

    def test_the_baseline_advances_so_the_check_cannot_stay_red(self):
        # A ratchet that kept the OLD baseline after an increase would stay red
        # until the entire backlog was triaged, which is the muting failure.
        self._add_unbound("e1")
        self._ratchet()
        self._add_unbound("e2")
        self.assertEqual(self._ratchet()[0], 1)
        code, report = self._ratchet()
        self.assertEqual(code, 0, "the ratchet stayed red after the growth interval passed")
        self.assertEqual(report["grew_by"], 0)

    def test_a_shrinking_backlog_never_alerts(self):
        for index in range(5):
            self._add_unbound(f"e{index}")
        self._ratchet()
        conn = MODULE.connect()
        conn.execute("UPDATE events SET workstream_id='ABC-35' WHERE event_id IN ('e0','e1')")
        conn.commit()
        code, report = self._ratchet()
        self.assertEqual(code, 0)
        self.assertEqual(report["grew_by"], -2)

    def test_an_unbound_event_whose_session_is_bound_is_a_hard_regression(self):
        # The sharper signal. Capture resolves from the bindings table, so this
        # combination cannot occur unless resolution regressed.
        self._add_unbound("e1", session_id="bound-session")
        conn = MODULE.connect()
        MODULE.record_binding(conn, "session", "bound-session", "ABC-35", None)
        conn.commit()
        code, report = self._ratchet()
        self.assertEqual(code, 1)
        self.assertEqual(report["unbound_with_binding"], 1)
        self.assertTrue(any("resolution regressed" in a for a in report["alerts"]))

    def test_a_corrupt_baseline_is_treated_as_a_first_observation(self):
        # The state file must never be able to wedge the check.
        self._add_unbound("e1")
        (MODULE.state_root() / "ratchet.json").write_text("{not json")
        code, report = self._ratchet()
        self.assertEqual(code, 0)
        self.assertTrue(report["first_observation"])

    def test_the_baseline_file_is_not_group_readable(self):
        self._add_unbound("e1")
        self._ratchet()
        mode = (MODULE.state_root() / "ratchet.json").stat().st_mode & 0o777
        self.assertEqual(mode, 0o600)


class ClassificationAuthorityTests(unittest.TestCase):
    """Mutable GitHub classification comments never close a raw capture."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.env = mock.patch.dict(os.environ, {
            "WORKSTREAM_INGRESS_STATE_DIR": str(Path(self.temp.name) / "state"),
            "WORKSTREAM_INGRESS_CONFIG": str(Path(self.temp.name) / "config.json"),
        }, clear=False)
        self.env.start()
        self.repo = "private/ingress"
        self.issue = 7
        self.capture = capture_payload(
            "wsi_classify", workstream_id="GEN-37", prompt="No longer material",
            captured_at="2026-08-29T01:00:00Z",
        )

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def payload(self, disposition, *, schema_version=2):
        return {
            "schema_version": schema_version, "event_id": "wsi_classify",
            "processed_at": "2026-08-29T01:01:00Z", "disposition": disposition,
            "promoted_issue": None,
            "ingress_route": {"provider": "github", "repository": self.repo, "issue": self.issue},
            "capture_sha256": "b" * 64,
            "classification_actor": {"provider": "github", "login": "trusted-bot", "id": 100},
            "classification_source": "reviewed_agent_classification",
            "classification_id": "wsc_" + "c" * 32,
        }

    def recover(self, payload, *, comment_metadata=None):
        comments = [
            {"id": 1, "body": MODULE.comment_body(MODULE.CAPTURE_MARKER, self.capture)},
            {
                "id": 2, "body": MODULE.comment_body(MODULE.PROCESSED_MARKER, payload),
                **(comment_metadata or {
                    "created_at": "2026-08-29T01:01:00Z",
                    "updated_at": "2026-08-29T01:01:00Z",
                    "user": {"login": "trusted-bot", "id": 100},
                }),
            },
        ]
        with mock.patch.object(MODULE, "gh", side_effect=[
            [{"number": self.issue, "url": "i", "title": "ingress"}], [comments],
        ]):
            return MODULE.remote_events(self.repo, "GEN-37")

    def assert_visible_hint(self, payload, *, metadata=None):
        events = self.recover(payload, comment_metadata=metadata)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_id"], "wsi_classify")
        self.assertEqual(events[0]["classification_hint"], {
            "authoritative": False,
            "reason": "mutable_github_comment",
            "observed_count": 1,
            "additional_hints_omitted": 0,
            "dispositions": [payload["disposition"]],
            "ambiguous": False,
        })

    def test_every_mutable_classification_variant_remains_open_for_both_dispositions(self):
        variants = {
            "valid-looking-trusted-author": lambda payload: ({}, {}),
            "edited-comment": lambda payload: ({}, {
                "created_at": "2026-08-29T01:01:00Z",
                "updated_at": "2026-08-29T01:02:00Z",
                "user": {"login": "trusted-bot", "id": 100},
            }),
            "schema-float": lambda payload: ({"schema_version": 2.0}, {}),
            "login-recycle": lambda payload: ({}, {
                "created_at": "2026-08-29T01:01:00Z",
                "updated_at": "2026-08-29T01:01:00Z",
                "user": {"login": "trusted-bot", "id": 200},
            }),
            "wrong-user-id": lambda payload: ({
                "classification_actor": {
                    "provider": "github", "login": "trusted-bot", "id": 999,
                },
            }, {}),
        }
        for disposition in ("no-material-delta", "superseded"):
            for name, mutate in variants.items():
                with self.subTest(disposition=disposition, variant=name):
                    payload = self.payload(disposition)
                    changes, metadata = mutate(payload)
                    payload.update(changes)
                    self.assert_visible_hint(payload, metadata=metadata or None)

    def test_conflicting_orphan_and_cross_route_hints_never_block_inventory(self):
        other_capture = {
            **self.capture, "event_id": "wsi_other",
            "captured_at": "2026-08-29T01:02:00Z", "prompt": "Still open",
            "prompt_sha256": hashlib.sha256(b"Still open").hexdigest(),
        }
        first = self.payload("no-material-delta")
        conflicting_actor = self.payload("no-material-delta")
        conflicting_actor["classification_actor"] = {
            "provider": "github", "login": "other", "id": 200,
        }
        conflicting_disposition = self.payload("superseded")
        cross_route = self.payload("no-material-delta")
        orphan = self.payload("superseded")
        orphan["event_id"] = "wsi_orphan"
        issue_seven = [
            {"id": 1, "body": MODULE.comment_body(MODULE.CAPTURE_MARKER, self.capture)},
            {"id": 2, "body": MODULE.comment_body(MODULE.PROCESSED_MARKER, first),
             "user": {"login": "trusted-bot", "id": 100}},
            {"id": 3, "body": MODULE.comment_body(
                MODULE.PROCESSED_MARKER, conflicting_actor),
             "user": {"login": "other", "id": 200}},
            {"id": 4, "body": MODULE.comment_body(
                MODULE.PROCESSED_MARKER, conflicting_disposition)},
            {"id": 5, "body": MODULE.comment_body(
                MODULE.PROCESSED_MARKER, ["malformed", "hint"])},
        ]
        issue_eight = [
            {"id": 6, "body": MODULE.comment_body(
                MODULE.CAPTURE_MARKER, other_capture)},
            {"id": 7, "body": MODULE.comment_body(MODULE.PROCESSED_MARKER, cross_route)},
            {"id": 8, "body": MODULE.comment_body(MODULE.PROCESSED_MARKER, orphan)},
        ]
        with mock.patch.object(MODULE, "gh", side_effect=[
            [
                {"number": 7, "url": "i7", "title": "ingress"},
                {"number": 8, "url": "i8", "title": "ingress"},
            ],
            [issue_seven], [issue_eight],
        ]):
            events = MODULE.remote_events(self.repo, "GEN-37")
        self.assertEqual([event["event_id"] for event in events], [
            "wsi_classify", "wsi_other",
        ])
        self.assertEqual(events[0]["classification_hint"], {
            "authoritative": False,
            "reason": "mutable_github_comment",
            "observed_count": 3,
            "additional_hints_omitted": 0,
            "dispositions": ["no-material-delta", "superseded"],
            "ambiguous": True,
        })
        self.assertNotIn("classification_hint", events[1])

    def test_unhashable_processed_hint_dispositions_are_quarantined(self):
        for disposition in ([], {}):
            with self.subTest(disposition=disposition):
                payload = self.payload(disposition)
                events = self.recover(payload)
                self.assertEqual([event["event_id"] for event in events], [
                    "wsi_classify",
                ])
                self.assertNotIn("classification_hint", events[0])
                self.assertFalse(MODULE._is_exact_legacy_processed_hint(payload))

    def test_process_refuses_to_publish_nonauthoritative_classifications(self):
        for disposition in ("no-material-delta", "superseded"):
            args = argparse.Namespace(
                disposition=disposition, event="wsi_classify", issue=None,
                repo=self.repo, remote_issue=self.issue,
            )
            with self.subTest(disposition=disposition), \
                 mock.patch.object(MODULE, "gh") as gh:
                with self.assertRaisesRegex(ValueError, "classification_not_durable"):
                    MODULE.command_process(args)
                gh.assert_not_called()


class LegacyProcessedCompatibilityTests(unittest.TestCase):
    """Original five-field processed markers are hints, never receipts."""

    repo = "private/ingress"
    issue = 7

    @staticmethod
    def capture(event_id, *, session_id="session-a", workstream_id="GEN-37"):
        prompt = f"Prompt for {event_id}"
        return {
            "schema_version": 1,
            "event_id": event_id,
            "captured_at": "2026-08-29T01:00:00Z",
            "provider": "codex",
            "session_id": session_id,
            "turn_id": f"turn-{event_id}",
            "surface_id": f"surface-{session_id}",
            "workspace_id": "workspace-1",
            "cwd": "/repo",
            "workstream_id": workstream_id,
            "context_url": "https://linear.app/generous/issue/GEN-37/x",
            "prompt": prompt,
            "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "redactions": 0,
            "truncated": False,
        }

    @staticmethod
    def legacy(event_id, disposition="promoted", *, promoted_issue="GEN-37"):
        return {
            "schema_version": 1,
            "event_id": event_id,
            "processed_at": "2026-08-29T01:01:00Z",
            "disposition": disposition,
            "promoted_issue": promoted_issue,
        }

    def recover(self, issues, comments, *, workstream="GEN-37"):
        effects = [issues] + [[comments[number]] for number in comments]
        with mock.patch.object(MODULE, "gh", side_effect=effects):
            return MODULE.remote_events(self.repo, workstream)

    def promotion(self, capture):
        request = {
            "schema_version": 1,
            "ingress": {
                "repo": self.repo, "remote_issue": self.issue,
                "event_id": capture["event_id"],
                "prompt_sha256": capture["prompt_sha256"],
            },
            "authority": {
                "workspace_id": "11111111-1111-4111-8111-111111111111",
                "team_id": "22222222-2222-4222-8222-222222222222",
                "project_id": "33333333-3333-4333-8333-333333333333",
                "root_issue_id": "44444444-4444-4444-8444-444444444444",
            },
            "workstream_id": "GEN-37",
            "plan_revision": "b" * 64,
            "expected_material_revision": 0,
            "changes": [{"kind": "requirement", "payload": {"text": "Keep open"}}],
        }
        return MODULE.promotion_payload(request, capture)

    def test_live_shape_legacy_history_keeps_all_thirty_events_across_two_sessions(self):
        comments = []
        dispositions = ("promoted", "no-material-delta", "superseded")
        for index in range(30):
            event_id = f"wsi_live_{index:02d}"
            session_id = "session-a" if index < 15 else "session-b"
            capture = self.capture(event_id, session_id=session_id)
            legacy = self.legacy(event_id, dispositions[index % len(dispositions)])
            comments.extend([
                {"id": index * 2, "body": MODULE.comment_body(
                    MODULE.CAPTURE_MARKER, capture)},
                {"id": index * 2 + 1, "body": MODULE.comment_body(
                    MODULE.PROCESSED_MARKER, legacy)},
            ])
        events = self.recover(
            [{"number": self.issue, "url": "i", "title": "ingress"}],
            {self.issue: comments},
        )
        self.assertEqual(len(events), 30)
        self.assertEqual({event["session_id"] for event in events}, {
            "session-a", "session-b",
        })
        self.assertEqual(
            {event["classification_hint"]["dispositions"][0] for event in events},
            set(dispositions),
        )
        self.assertTrue(all(
            event["classification_hint"]["authoritative"] is False for event in events
        ))

    def test_legacy_promoted_marker_with_matching_binding_does_not_suppress(self):
        capture = self.capture("wsi_bound", workstream_id=None)
        binding = {
            "event_id": "wsi_bound", "workstream_id": "GEN-37",
            "context_url": "https://linear.app/generous/issue/GEN-37/x",
        }
        comments = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture)},
            {"body": MODULE.comment_body(MODULE.BIND_MARKER, binding)},
            {"body": MODULE.comment_body(
                MODULE.PROCESSED_MARKER, self.legacy("wsi_bound"))},
        ]
        events = self.recover(
            [{"number": self.issue, "url": "i", "title": "ingress"}],
            {self.issue: comments},
        )
        self.assertEqual([event["event_id"] for event in events], ["wsi_bound"])
        self.assertEqual(events[0]["workstream_id"], "GEN-37")
        self.assertEqual(events[0]["classification_hint"]["dispositions"], ["promoted"])

    def test_legacy_marker_leaves_a_staged_modern_promotion_visible(self):
        capture = self.capture("wsi_staged")
        promotion = self.promotion(capture)
        comments = [
            ["not", "a", "comment"],
            {"body": None},
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture)},
            {"body": MODULE.comment_body(MODULE.PROMOTION_MARKER, promotion)},
            {"body": MODULE.comment_body(
                MODULE.PROCESSED_MARKER, self.legacy("wsi_staged"))},
        ]
        events = self.recover(
            [{"number": self.issue, "url": "i", "title": "ingress"}],
            {self.issue: comments},
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["promotion_state"], "staged")
        self.assertEqual(events[0]["promotion"], promotion)

    def test_schema_two_classification_hint_leaves_staged_promotion_visible(self):
        capture = self.capture("wsi_schema_two")
        promotion = self.promotion(capture)
        classification = {
            **self.legacy(
                "wsi_schema_two", "no-material-delta", promoted_issue=None,
            ),
            "schema_version": 2,
            "classification_source": "reviewed_agent_classification",
        }
        comments = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture)},
            {"body": MODULE.comment_body(MODULE.PROMOTION_MARKER, promotion)},
            {"body": MODULE.comment_body(MODULE.PROCESSED_MARKER, classification)},
        ]
        events = self.recover(
            [{"number": self.issue, "url": "i", "title": "ingress"}],
            {self.issue: comments},
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["promotion_state"], "staged")
        self.assertEqual(
            events[0]["classification_hint"]["dispositions"],
            ["no-material-delta"],
        )

    def test_conflicting_legacy_hints_leave_staged_promotion_visible(self):
        capture = self.capture("wsi_conflicting_hints")
        promotion = self.promotion(capture)
        comments = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture)},
            {"body": MODULE.comment_body(MODULE.PROMOTION_MARKER, promotion)},
            {"body": MODULE.comment_body(MODULE.PROCESSED_MARKER, self.legacy(
                "wsi_conflicting_hints", "promoted"))},
            {"body": MODULE.comment_body(MODULE.PROCESSED_MARKER, self.legacy(
                "wsi_conflicting_hints", "superseded", promoted_issue=None))},
            {"body": MODULE.comment_body(
                MODULE.PROCESSED_MARKER, ["malformed", "hint"])},
        ]
        events = self.recover(
            [{"number": self.issue, "url": "i", "title": "ingress"}],
            {self.issue: comments},
        )
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["promotion_state"], "staged")
        self.assertEqual(events[0]["classification_hint"]["dispositions"], [
            "promoted", "superseded",
        ])
        self.assertTrue(events[0]["classification_hint"]["ambiguous"])

    def test_legacy_orphan_and_cross_route_markers_establish_no_route(self):
        capture = self.capture("wsi_cross")
        issue_seven = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture)},
        ]
        issue_eight = [
            {"body": MODULE.comment_body(
                MODULE.PROCESSED_MARKER, self.legacy("wsi_cross"))},
            {"body": MODULE.comment_body(
                MODULE.PROCESSED_MARKER, self.legacy("wsi_orphan"))},
        ]
        events = self.recover(
            [
                {"number": 7, "url": "i7", "title": "ingress"},
                {"number": 8, "url": "i8", "title": "ingress"},
            ],
            {7: issue_seven, 8: issue_eight},
        )
        self.assertEqual([event["event_id"] for event in events], ["wsi_cross"])
        self.assertNotIn("classification_hint", events[0])

    def test_modern_orphan_and_cross_route_receipts_still_refuse(self):
        def modern(event_id):
            return {
                "schema_version": 1, "event_id": event_id,
                "processed_at": "2026-08-29T01:02:00Z", "disposition": "promoted",
                "promoted_issue": "GEN-37", "promotion_id": "wsp_" + "b" * 32,
                "material_event_id": "wsd_" + "c" * 32, "material_revision": 1,
                "material_remote_id": "linear-1",
            }

        orphan = [{"body": MODULE.comment_body(
            MODULE.PROCESSED_MARKER, modern("wsi_orphan"))}]
        with self.subTest(case="orphan"), self.assertRaisesRegex(
            ValueError, "processed_without_capture:wsi_orphan"
        ):
            self.recover(
                [{"number": 8, "url": "i8", "title": "ingress"}], {8: orphan},
            )

        capture = self.capture("wsi_cross")
        issue_seven = [{"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture)}]
        issue_eight = [{"body": MODULE.comment_body(
            MODULE.PROCESSED_MARKER, modern("wsi_cross"))}]
        with self.subTest(case="cross-route"), self.assertRaisesRegex(
            ValueError, "ingress_event_route_collision:wsi_cross"
        ):
            self.recover(
                [
                    {"number": 7, "url": "i7", "title": "ingress"},
                    {"number": 8, "url": "i8", "title": "ingress"},
                ],
                {7: issue_seven, 8: issue_eight},
            )

    def test_receipt_claim_predicate_separates_authority_from_classification_hints(self):
        classification_only = [
            self.legacy("wsi_hint"),
            {**self.legacy("wsi_hint"), "schema_version": 1.0},
            {**self.legacy("wsi_hint", "superseded"), "unexpected": "metadata"},
            {**self.legacy("wsi_hint", "no-material-delta"),
             "material_revision": 1},
        ]
        receipt_claims = [
            {**self.legacy("wsi_receipt"), "promotion_id": "wsp_bad"},
            {**self.legacy("wsi_receipt"), "material_event_id": "wsd_bad"},
            {**self.legacy("wsi_receipt"), "material_revision": 1},
            {**self.legacy("wsi_receipt"), "material_remote_id": "linear-1"},
        ]
        self.assertTrue(all(
            not MODULE._claims_processed_promotion_receipt(item)
            for item in classification_only
        ))
        self.assertTrue(all(
            MODULE._claims_processed_promotion_receipt(item) for item in receipt_claims
        ))

    def test_partial_float_and_invalid_receipt_claims_fail_closed(self):
        variants = {
            "partial": {**self.legacy("wsi_bad"), "promotion_id": "wsp_bad"},
            "float": {
                **self.legacy("wsi_bad"), "schema_version": 1.0,
                "material_revision": 1,
            },
            "invalid": {
                **self.legacy("wsi_bad"), "promoted_issue": 37,
                "material_remote_id": "linear-1",
            },
        }
        capture = self.capture("wsi_bad")
        for name, marker in variants.items():
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError, "processed_promotion_schema_invalid"
            ):
                MODULE.reduce_ingress_comments([
                    {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture)},
                    {"body": MODULE.comment_body(MODULE.PROCESSED_MARKER, marker)},
                ], event_id="wsi_bad", repo=self.repo, issue=self.issue)

    def test_legacy_hint_does_not_displace_modern_receipt_claim(self):
        capture = self.capture("wsi_conflict")
        promotion = self.promotion(capture)
        delta = MODULE.promotion_delta(promotion)
        modern = {
            "schema_version": 1, "event_id": "wsi_conflict",
            "processed_at": "2026-08-29T01:02:00Z", "disposition": "promoted",
            "promoted_issue": "GEN-37", "promotion_id": promotion["promotion_id"],
            "material_event_id": delta.event_id, "material_revision": 1,
            "material_remote_id": "linear-1",
        }
        comments = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture)},
            {"body": MODULE.comment_body(MODULE.PROMOTION_MARKER, promotion)},
            {"body": MODULE.comment_body(
                MODULE.PROCESSED_MARKER, self.legacy("wsi_conflict"))},
            {"body": MODULE.comment_body(MODULE.PROCESSED_MARKER, modern)},
        ]
        state = MODULE.reduce_ingress_comments(
            comments, event_id="wsi_conflict", repo=self.repo, issue=self.issue,
        )
        self.assertEqual(state["processed"], modern)

    def test_conflicting_modern_receipt_claims_still_fail_closed(self):
        capture = self.capture("wsi_receipt_conflict")
        promotion = self.promotion(capture)
        delta = MODULE.promotion_delta(promotion)
        first = {
            "schema_version": 1, "event_id": "wsi_receipt_conflict",
            "processed_at": "2026-08-29T01:02:00Z", "disposition": "promoted",
            "promoted_issue": "GEN-37", "promotion_id": promotion["promotion_id"],
            "material_event_id": delta.event_id, "material_revision": 1,
            "material_remote_id": "linear-1",
        }
        second = {**first, "material_remote_id": "linear-forged"}
        comments = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture)},
            {"body": MODULE.comment_body(MODULE.PROMOTION_MARKER, promotion)},
            {"body": MODULE.comment_body(MODULE.PROCESSED_MARKER, first)},
            {"body": MODULE.comment_body(MODULE.PROCESSED_MARKER, second)},
        ]
        with self.assertRaisesRegex(
            ValueError, "conflicting_processed:wsi_receipt_conflict"
        ):
            MODULE.reduce_ingress_comments(
                comments, event_id="wsi_receipt_conflict",
                repo=self.repo, issue=self.issue,
            )

    def test_malformed_capture_invalid_escape_remains_quarantined(self):
        malformed = (
            MODULE.CAPTURE_MARKER
            + '\n```json\n{"event_id":"wsi_bad_escape","prompt":"bad\\q"}\n```'
        )
        comments = [
            {"body": malformed},
            {"body": MODULE.comment_body(
                MODULE.PROCESSED_MARKER, self.legacy("wsi_bad_escape"))},
        ]
        self.assertEqual(self.recover(
            [{"number": self.issue, "url": "i", "title": "ingress"}],
            {self.issue: comments},
        ), [])


class ManagedPromotionTests(unittest.TestCase):
    """Raw capture must survive every boundary through processed successor proof."""

    class Remote:
        def __init__(self, capture):
            self.comments = [{"id": "capture", "body": MODULE.comment_body(
                MODULE.CAPTURE_MARKER, capture)}]
            self.writes = []

        def gh(self, args, *, stdin=None, timeout=4):
            if "--paginate" in args:
                return [list(self.comments)]
            if args[-2:] == ["--input", "-"]:
                body = json.loads(stdin)["body"]
                item = {"id": f"comment-{len(self.comments)}", "body": body,
                        "html_url": f"https://example/{len(self.comments)}"}
                self.comments.append(item)
                self.writes.append(body)
                return item
            raise AssertionError(args)

        def logical(self, marker):
            return [MODULE.parse_comment(item["body"], marker) for item in self.comments
                    if MODULE.parse_comment(item["body"], marker)]

    class Linear:
        def __init__(self):
            self.events = {}
            self.mutations = 0

        def current_revision(self, workstream_id):
            return len(self.events)

        def comments(self):
            return [
                {"id": receipt[2], "body": MODULE.encode_event_comment(receipt[0])}
                for receipt in self.events.values()
            ]

        def apply(self, delta):
            existing = self.events.get(delta.event_id)
            if existing:
                self.assert_replay(existing, delta)
                return MODULE.MutationReceipt(delta.event_id, existing[1], existing[2])
            if delta.expected_revision != len(self.events):
                raise MODULE.RevisionConflict("stale")
            self.mutations += 1
            receipt = (delta, len(self.events) + 1, f"linear-{len(self.events) + 1}")
            self.events[delta.event_id] = receipt
            return MODULE.MutationReceipt(delta.event_id, receipt[1], receipt[2])

        @staticmethod
        def assert_replay(existing, requested):
            recorded = existing[0]
            if not (
                recorded.event_id == requested.event_id
                and recorded.workstream_id == requested.workstream_id
                and recorded.kind == requested.kind
                and recorded.source == requested.source
                and recorded.payload == requested.payload
                and recorded.created_at == requested.created_at
                and recorded.expected_revision >= requested.expected_revision
            ):
                raise AssertionError("conflicting replay")

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.env = mock.patch.dict(os.environ, {
            "WORKSTREAM_INGRESS_STATE_DIR": str(self.root / "state"),
            "WORKSTREAM_INGRESS_CONFIG": str(self.root / "config.json"),
        }, clear=False)
        self.env.start()
        self.capture = capture_payload(
            "wsi_raw", workstream_id="GEN-37",
            context_url="https://linear.app/generous/issue/GEN-37/x",
            prompt="Add the missing recovery gate",
            captured_at="2026-08-29T01:00:00Z",
        )
        self.remote = self.Remote(self.capture)
        self.linear = self.Linear()
        self.request = self.root / "promotion.json"
        self.request.write_text(json.dumps({
            "schema_version": 1,
            "ingress": {"repo": "private/ingress", "remote_issue": 7,
                        "event_id": "wsi_raw",
                        "prompt_sha256": self.capture["prompt_sha256"]},
            "authority": {
                "workspace_id": "11111111-1111-4111-8111-111111111111",
                "team_id": "22222222-2222-4222-8222-222222222222",
                "project_id": "33333333-3333-4333-8333-333333333333",
                "root_issue_id": "44444444-4444-4444-8444-444444444444",
            },
            "workstream_id": "GEN-37", "plan_revision": "b" * 64,
            "expected_material_revision": 0,
            "changes": [{"kind": "requirement", "payload": {
                "text": "Add the missing recovery gate", "acceptance": "planted crash passes"}}],
        }))
        self.patches = [
            mock.patch.object(MODULE, "gh", side_effect=self.remote.gh),
            mock.patch.object(MODULE, "linear_adapter_for_promotion", return_value=self.linear),
        ]
        for patcher in self.patches:
            patcher.start()

    def tearDown(self):
        for patcher in reversed(self.patches):
            patcher.stop()
        self.env.stop()
        self.temp.cleanup()

    def args(self, *, request=True):
        return argparse.Namespace(
            request=str(self.request) if request else None,
            repo=None if request else "private/ingress", remote_issue=None if request else 7,
            event=None if request else "wsi_raw", config=None, max_conflicts=8, apply=True,
        )

    def _promote(self, *, request=True):
        output = io.StringIO()
        with mock.patch.object(MODULE.sys, "stdout", output):
            code = MODULE.command_promote(self.args(request=request))
        return code, json.loads(output.getvalue())

    def test_crash_after_durable_stage_recovers_without_source_files(self):
        with mock.patch.object(MODULE, "promotion_failpoint", side_effect=RuntimeError("crash")):
            with self.assertRaisesRegex(RuntimeError, "crash"):
                self._promote()
        self.assertEqual(len(self.remote.logical(MODULE.PROMOTION_MARKER)), 1)
        self.assertEqual(self.linear.mutations, 0)
        # The successor has neither the reviewed request nor the source outbox.
        self.request.unlink()
        shutil.rmtree(MODULE.state_root(), ignore_errors=True)
        code, result = self._promote(request=False)
        self.assertEqual(code, 0)
        self.assertEqual(result["disposition"], "promoted")
        self.assertEqual(self.linear.mutations, 1)
        self.assertEqual(len(self.remote.logical(MODULE.PROCESSED_MARKER)), 1)

    def test_crash_after_linear_accept_replays_without_duplicate_material(self):
        def fail(stage):
            if stage == "after_linear":
                raise RuntimeError("crash after linear")
        with mock.patch.object(MODULE, "promotion_failpoint", side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, "crash after linear"):
                self._promote()
        self.assertEqual(self.linear.mutations, 1)
        self.assertEqual(self.remote.logical(MODULE.PROCESSED_MARKER), [])
        self.request.unlink()
        shutil.rmtree(MODULE.state_root(), ignore_errors=True)
        self._promote(request=False)
        self.assertEqual(self.linear.mutations, 1, "replay duplicated the Linear material event")
        self.assertEqual(len(self.remote.logical(MODULE.PROCESSED_MARKER)), 1)

    def test_crash_after_processed_marker_is_a_zero_write_successor_replay(self):
        def fail(stage):
            if stage == "after_processed":
                raise RuntimeError("crash after processed")
        with mock.patch.object(MODULE, "promotion_failpoint", side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, "crash after processed"):
                self._promote()
        writes = len(self.remote.writes)
        self.request.unlink()
        shutil.rmtree(MODULE.state_root(), ignore_errors=True)
        _, result = self._promote(request=False)
        self.assertTrue(result["replay"])
        self.assertEqual(self.linear.mutations, 1)
        self.assertEqual(len(self.remote.writes), writes, "successor appended a second marker")

    def test_preview_performs_no_remote_or_linear_mutation(self):
        args = self.args()
        args.apply = False
        with mock.patch.object(MODULE.sys, "stdout", io.StringIO()):
            self.assertEqual(MODULE.command_promote(args), 0)
        self.assertEqual(self.remote.writes, [])
        self.assertEqual(self.linear.mutations, 0)

    def test_conflicting_durable_intent_refuses_before_linear_mutation(self):
        other = json.loads(self.request.read_text())
        other["changes"][0]["payload"]["text"] = "different"
        promotion = MODULE.promotion_payload(other, self.capture)
        self.remote.comments.append({"id": "bad", "body": MODULE.comment_body(
            MODULE.PROMOTION_MARKER, promotion)})
        with self.assertRaisesRegex(ValueError, "conflicting_promotion"):
            self._promote()
        self.assertEqual(self.linear.mutations, 0)

    def test_forged_processed_marker_cannot_replace_linear_readback(self):
        request = json.loads(self.request.read_text())
        promotion = MODULE.promotion_payload(request, self.capture)
        delta = MODULE.promotion_delta(promotion)
        processed = {
            "schema_version": 1, "event_id": "wsi_raw",
            "processed_at": "2026-08-29T01:01:00Z", "disposition": "promoted",
            "promoted_issue": "GEN-37", "promotion_id": promotion["promotion_id"],
            "material_event_id": delta.event_id, "material_revision": 1,
            "material_remote_id": "claimed-linear-receipt",
        }
        self.remote.comments.extend([
            {"id": "promotion", "body": MODULE.comment_body(
                MODULE.PROMOTION_MARKER, promotion)},
            {"id": "processed", "body": MODULE.comment_body(
                MODULE.PROCESSED_MARKER, processed)},
        ])
        with self.assertRaisesRegex(ValueError, "processed_material_event_missing"):
            self._promote(request=False)
        self.assertEqual(self.linear.mutations, 0)

    def test_boolean_revision_is_rejected_before_remote_reads(self):
        request = json.loads(self.request.read_text())
        request["expected_material_revision"] = False
        self.request.write_text(json.dumps(request))
        with self.assertRaisesRegex(ValueError, "expected_revision_invalid"):
            self._promote()
        self.assertEqual(self.remote.writes, [])

    def test_plan_revision_is_required_and_validated_before_remote_reads(self):
        request = json.loads(self.request.read_text())
        request["plan_revision"] = "not-a-plan-digest"
        self.request.write_text(json.dumps(request))
        with self.assertRaisesRegex(ValueError, "promotion_request_plan_revision_invalid"):
            self._promote()
        self.assertEqual(self.remote.writes, [])

    def test_plan_revision_is_bound_into_promotion_identity(self):
        request = MODULE.load_promotion_request(str(self.request))
        first = MODULE.promotion_payload(request, self.capture)
        request["plan_revision"] = "c" * 64
        second = MODULE.promotion_payload(request, self.capture)
        self.assertNotEqual(first["promotion_id"], second["promotion_id"])
        self.assertEqual(first["plan_revision"], "b" * 64)
        self.assertEqual(second["plan_revision"], "c" * 64)

    def test_promotion_request_schema_float_is_rejected_before_remote_reads(self):
        request = json.loads(self.request.read_text())
        request["schema_version"] = 1.0
        self.request.write_text(json.dumps(request))
        with self.assertRaisesRegex(ValueError, "promotion_request_schema_unsupported"):
            self._promote()
        self.assertEqual(self.remote.writes, [])

    def test_legacy_process_cannot_bypass_material_receipt_verification(self):
        args = argparse.Namespace(
            disposition="promoted", event="wsi_raw", issue="GEN-37",
            repo="private/ingress", remote_issue=7,
        )
        with self.assertRaisesRegex(ValueError, "requires.*promote"):
            MODULE.command_process(args)
        self.assertEqual(self.remote.writes, [])

    def test_final_encoded_marker_budget_is_checked_before_write(self):
        request = json.loads(self.request.read_text())
        request["changes"][0]["payload"]["padding"] = ""
        compact = json.dumps(request, separators=(",", ":"))
        target = 16_341
        request["changes"][0]["payload"]["padding"] = "x" * (target - len(compact))
        compact = json.dumps(request, separators=(",", ":"))
        self.assertEqual(len(compact.encode()), target)
        self.request.write_text(compact)
        with self.assertRaisesRegex(ValueError, "output_envelope_over_budget"):
            self._promote()
        self.assertEqual(self.remote.writes, [])
        self.assertEqual(self.linear.mutations, 0)

    def test_ingress_repo_issue_route_is_part_of_promotion_identity(self):
        request = json.loads(self.request.read_text())
        first = MODULE.promotion_payload(request, self.capture)
        other = json.loads(self.request.read_text())
        other["ingress"]["repo"] = "public/other"
        other["ingress"]["remote_issue"] = 999
        second = MODULE.promotion_payload(other, self.capture)
        self.assertNotEqual(first["promotion_id"], second["promotion_id"])
        self.assertNotEqual(first["ingress_route"], second["ingress_route"])

    def test_route_bearing_promotion_requires_exact_schema_two(self):
        request = json.loads(self.request.read_text())
        current = MODULE.promotion_payload(request, self.capture)
        self.assertEqual(current["schema_version"], 2)
        self.assertEqual(MODULE.validate_promotion_payload(current), current)
        legacy = dict(current, schema_version=1)
        legacy["promotion_id"] = MODULE.promotion_id_for({
            key: value for key, value in legacy.items() if key != "promotion_id"
        })
        with self.assertRaisesRegex(ValueError, "promotion_marker_schema_unsupported"):
            MODULE.validate_promotion_payload(legacy)
        schema_float = dict(current, schema_version=2.0)
        schema_float["promotion_id"] = MODULE.promotion_id_for({
            key: value for key, value in schema_float.items() if key != "promotion_id"
        })
        with self.assertRaisesRegex(ValueError, "promotion_marker_schema_unsupported"):
            MODULE.validate_promotion_payload(schema_float)

    def test_only_linear_receipted_promotion_suppresses_remote_capture(self):
        request = json.loads(self.request.read_text())
        promotion = MODULE.promotion_payload(request, self.capture)
        delta = MODULE.promotion_delta(promotion)
        receipt = self.linear.apply(delta)
        processed = {
            "schema_version": 1, "event_id": "wsi_raw",
            "processed_at": "2026-08-29T01:01:00Z", "disposition": "promoted",
            "promoted_issue": "GEN-37", "promotion_id": promotion["promotion_id"],
            "material_event_id": delta.event_id, "material_revision": receipt.revision,
            "material_remote_id": receipt.remote_id,
        }
        comments = [
            {"id": 1, "body": MODULE.comment_body(MODULE.CAPTURE_MARKER, self.capture)},
            {"id": 2, "body": MODULE.comment_body(MODULE.PROMOTION_MARKER, promotion)},
            {"id": 3, "body": MODULE.comment_body(MODULE.PROCESSED_MARKER, processed)},
        ]
        with mock.patch.object(MODULE, "gh", side_effect=[
            [{"number": 7, "url": "i", "title": "ingress"}], [comments],
        ]):
            self.assertEqual(MODULE.remote_events("private/ingress", "GEN-37"), [])

    def test_processed_promotion_schema_float_is_rejected(self):
        request = json.loads(self.request.read_text())
        promotion = MODULE.promotion_payload(request, self.capture)
        delta = MODULE.promotion_delta(promotion)
        processed = {
            "schema_version": 1.0, "event_id": "wsi_raw",
            "processed_at": "2026-08-29T01:01:00Z", "disposition": "promoted",
            "promoted_issue": "GEN-37", "promotion_id": promotion["promotion_id"],
            "material_event_id": delta.event_id, "material_revision": 1,
            "material_remote_id": "linear-1",
        }
        comments = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, self.capture)},
            {"body": MODULE.comment_body(MODULE.PROMOTION_MARKER, promotion)},
            {"body": MODULE.comment_body(MODULE.PROCESSED_MARKER, processed)},
        ]
        with self.assertRaisesRegex(ValueError, "processed_promotion_value_invalid"):
            MODULE.reduce_ingress_comments(
                comments, event_id="wsi_raw", repo="private/ingress", issue=7,
            )

    def test_staged_intent_cannot_be_replayed_from_a_different_repo_issue(self):
        request = json.loads(self.request.read_text())
        promotion = MODULE.promotion_payload(request, self.capture)
        self.remote.comments.append({"id": "promotion", "body": MODULE.comment_body(
            MODULE.PROMOTION_MARKER, promotion)})
        args = self.args(request=False)
        args.repo = "public/other"
        args.remote_issue = 999
        with self.assertRaisesRegex(ValueError, "promotion_ingress_route_mismatch"):
            with mock.patch.object(MODULE.sys, "stdout", io.StringIO()):
                MODULE.command_promote(args)
        self.assertEqual(self.linear.mutations, 0)

    def test_remote_recover_refuses_forged_processed_receipt(self):
        request = json.loads(self.request.read_text())
        promotion = MODULE.promotion_payload(request, self.capture)
        delta = MODULE.promotion_delta(promotion)
        processed = {
            "schema_version": 1, "event_id": "wsi_raw",
            "processed_at": "2026-08-29T01:01:00Z", "disposition": "promoted",
            "promoted_issue": "GEN-37", "promotion_id": promotion["promotion_id"],
            "material_event_id": delta.event_id, "material_revision": 1,
            "material_remote_id": "forged",
        }
        comments = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, self.capture)},
            {"body": MODULE.comment_body(MODULE.PROMOTION_MARKER, promotion)},
            {"body": MODULE.comment_body(MODULE.PROCESSED_MARKER, processed)},
        ]
        with mock.patch.object(MODULE, "gh", side_effect=[
            [{"number": 7, "url": "i", "title": "ingress"}], [comments],
        ]):
            with self.assertRaisesRegex(ValueError, "processed_material_event_missing"):
                MODULE.remote_events("private/ingress", "GEN-37")


if __name__ == "__main__":
    unittest.main()
