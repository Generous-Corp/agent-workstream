#!/usr/bin/env python3

import argparse
import importlib.util
import io
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("workstream_ingress.py")
SPEC = importlib.util.spec_from_file_location("workstream_ingress", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(MODULE)


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

    def test_remote_recovery_deduplicates_and_hides_processed(self):
        capture = {"event_id": "e1", "captured_at": "2026-08-14T01:00:00Z", "workstream_id": "ABC-12"}
        processed = {"event_id": "e1", "processed_at": "2026-08-14T02:00:00Z"}
        comments = [
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture), "html_url": "u1"},
            {"body": MODULE.comment_body(MODULE.CAPTURE_MARKER, capture), "html_url": "u2"},
            {"body": MODULE.comment_body(MODULE.PROCESSED_MARKER, processed), "html_url": "u3"},
        ]
        with mock.patch.object(MODULE, "gh", side_effect=[
            [{"number": 7, "url": "i", "title": "ingress"}], [comments]
        ]):
            self.assertEqual(MODULE.remote_events("o/r", "ABC-12"), [])

    def test_remote_binding_promotes_an_initially_unbound_event(self):
        capture = {"event_id": "e1", "captured_at": "2026-08-14T01:00:00Z", "workstream_id": None}
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
        capture = {"event_id": "e1", "captured_at": "2026-08-14T01:00:00Z", "workstream_id": None}
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
        existing = [{
            "number": 4, "title": "[Workstream ingress] m5 2026-08",
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
        secret = "ghp_abcdefghijklmnopqrstuvwxyz123456"
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
        secret = "ghp_abcdefghijklmnopqrstuvwxyz123456"
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


if __name__ == "__main__":
    unittest.main()
