import importlib.util
import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("workstream_plan.py")
SPEC = importlib.util.spec_from_file_location("workstream_plan", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class PlanIntakeTests(unittest.TestCase):
    def http_error(self, source, code, message):
        error = MODULE.HTTPError(source, code, message, None, io.BytesIO())
        self.addCleanup(error.close)
        return error

    def test_remote_plan_fetch_uses_verified_tls_context(self):
        context = object()
        response = mock.MagicMock()
        response.read.return_value = b"# Remote plan\n"
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        with mock.patch.object(MODULE, "default_ssl_context", return_value=context), \
             mock.patch.object(MODULE, "urlopen", return_value=response) as urlopen:
            raw, identity = MODULE.source_bytes("https://example.test/plan.md")

        self.assertEqual(raw, b"# Remote plan\n")
        self.assertEqual(identity, "https://example.test/plan.md")
        self.assertIs(urlopen.call_args.kwargs["context"], context)

    def test_github_blob_fetches_exact_raw_bytes_with_optional_auth(self):
        response = mock.MagicMock()
        response.read.return_value = b"# Private plan\n"
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        source = "https://github.com/acme/plans/blob/deadbeef/PLAN.md"
        with mock.patch.dict(MODULE.os.environ, {"GITHUB_TOKEN": "secret"}, clear=True), \
             mock.patch.object(MODULE, "urlopen", return_value=response) as urlopen:
            raw, identity = MODULE.source_bytes(source)
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url,
                         "https://raw.githubusercontent.com/acme/plans/deadbeef/PLAN.md")
        self.assertEqual(request.get_header("Authorization"), "Bearer secret")
        self.assertEqual(raw, b"# Private plan\n")
        self.assertEqual(identity, source)

    def test_exact_github_blob_404_falls_back_to_bounded_isolated_ssh(self):
        commit = "a" * 40
        source = (
            "https://github.com/Generous-Corp/pulp-planning/blob/"
            f"{commit}/plans/continuity.md"
        )
        missing = self.http_error(source, 404, "Not Found")
        calls = []

        def run(arguments, *, environment, timeout):
            calls.append((arguments, dict(environment), timeout))
            wrapper = Path(environment["GIT_SSH"])
            self.assertTrue(wrapper.is_file())
            self.assertEqual(wrapper.read_text(encoding="utf-8"), MODULE.SSH_WRAPPER)
            self.assertEqual(wrapper.stat().st_mode & 0o777, 0o700)
            return b"# Private exact plan\n" if len(calls) == 3 else b""

        with mock.patch.dict(
            MODULE.os.environ,
            {
                "PATH": "/usr/bin:/bin", "HOME": "/Users/test",
                "SSH_AUTH_SOCK": "/tmp/agent.sock", "GITHUB_TOKEN": "secret",
                "GH_TOKEN": "also-secret", "GIT_SSH_COMMAND": "unsafe",
            },
            clear=True,
        ), mock.patch.object(MODULE, "urlopen", side_effect=missing), \
             mock.patch.object(MODULE, "_run_bounded", side_effect=run):
            raw, identity = MODULE.source_bytes(source)

        self.assertEqual(raw, b"# Private exact plan\n")
        self.assertEqual(identity, source)
        self.assertEqual(len(calls), 3)
        init, fetch, show = calls
        isolated = init[0][-1]
        self.assertEqual(init[0][0:4], ["git", "init", "--bare", "--quiet"])
        self.assertEqual(Path(isolated).name, "repository.git")
        self.assertEqual(fetch[0], [
            "git", "-C", isolated, "fetch", "--quiet", "--no-tags",
            "--depth=1", "git@github.com:Generous-Corp/pulp-planning.git", commit,
        ])
        self.assertEqual(show[0], [
            "git", "-C", isolated, "show", "--no-ext-diff", "--no-textconv",
            f"{commit}:plans/continuity.md",
        ])
        self.assertFalse(Path(isolated).parent.exists())
        for arguments, environment, _timeout in calls:
            self.assertIsInstance(arguments, list)
            self.assertEqual(environment["GIT_TERMINAL_PROMPT"], "0")
            self.assertEqual(environment["GIT_CONFIG_NOSYSTEM"], "1")
            self.assertEqual(environment["GIT_CONFIG_GLOBAL"], os.devnull)
            self.assertEqual(environment["GIT_SSH_VARIANT"], "ssh")
            self.assertNotIn("GITHUB_TOKEN", environment)
            self.assertNotIn("GH_TOKEN", environment)
            self.assertNotIn("GIT_SSH_COMMAND", environment)
        self.assertEqual(init[2], MODULE.GIT_SHOW_TIMEOUT_SECONDS)
        self.assertEqual(fetch[2], MODULE.GIT_FETCH_TIMEOUT_SECONDS)
        self.assertEqual(show[2], MODULE.GIT_SHOW_TIMEOUT_SECONDS)
        self.assertIn('"-oBatchMode=yes"', MODULE.SSH_WRAPPER)

    def test_github_ssh_fallback_refuses_mutable_or_malformed_blob_urls(self):
        invalid = [
            "https://github.com/acme/plans/blob/main/PLAN.md",
            f"https://github.com/acme/plans/blob/{'a' * 39}/PLAN.md",
            f"https://github.com/bad_owner/plans/blob/{'a' * 40}/PLAN.md",
            f"https://github.com/acme/plans/blob/{'a' * 40}/../PLAN.md",
            f"https://github.com/acme/plans/blob/{'a' * 40}/PLAN%2Emd",
            f"https://github.com/acme/plans/blob/{'a' * 40}/PLAN.md?raw=1",
            f"http://github.com/acme/plans/blob/{'a' * 40}/PLAN.md",
        ]
        for source in invalid:
            with self.subTest(source=source), \
                 mock.patch.object(
                     MODULE, "urlopen",
                     side_effect=self.http_error(source, 404, "Not Found"),
                 ), mock.patch.object(MODULE, "_github_ssh_blob_bytes") as fallback:
                with self.assertRaises(MODULE.HTTPError):
                    MODULE.source_bytes(source)
                fallback.assert_not_called()

    def test_github_ssh_fallback_only_handles_https_404(self):
        source = (
            "https://github.com/acme/plans/blob/"
            f"{'a' * 40}/PLAN.md"
        )
        with mock.patch.object(
            MODULE, "urlopen",
            side_effect=self.http_error(source, 403, "Forbidden"),
        ), mock.patch.object(MODULE, "_github_ssh_blob_bytes") as fallback:
            with self.assertRaises(MODULE.HTTPError):
                MODULE.source_bytes(source)
        fallback.assert_not_called()

    def test_github_ssh_fallback_timeout_fails_closed(self):
        commit = "a" * 40
        source = f"https://github.com/acme/plans/blob/{commit}/PLAN.md"
        missing = self.http_error(source, 404, "Not Found")
        with mock.patch.object(MODULE, "urlopen", side_effect=missing), \
             mock.patch.object(MODULE, "_run_bounded", side_effect=[
                 b"", TimeoutError("immutable GitHub SSH plan fetch timed out"),
             ]):
            with self.assertRaisesRegex(TimeoutError, "timed out"):
                MODULE.source_bytes(source)

    def test_github_ssh_fallback_command_failure_fails_closed(self):
        commit = "a" * 40
        source = f"https://github.com/acme/plans/blob/{commit}/PLAN.md"
        missing = self.http_error(source, 404, "Not Found")
        with mock.patch.object(MODULE, "urlopen", side_effect=missing), \
             mock.patch.object(
                 MODULE, "_run_bounded",
                 side_effect=OSError("immutable GitHub SSH plan fetch failed"),
             ):
            with self.assertRaisesRegex(OSError, "fetch failed"):
                MODULE.source_bytes(source)

    def test_bounded_runner_kills_and_reaps_descendant_process_group(self):
        child_code = """
import pathlib
import subprocess
import sys
import time

grandchild = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
pathlib.Path(sys.argv[1]).write_text(str(grandchild.pid), encoding="utf-8")
time.sleep(60)
"""
        parent_code = """
import pathlib
import subprocess
import sys
import time

child = subprocess.Popen([sys.executable, "-c", sys.argv[3], sys.argv[2]])
pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding="utf-8")
time.sleep(60)
"""
        observed = []
        with tempfile.TemporaryDirectory() as directory:
            child_pid = Path(directory) / "child.pid"
            grandchild_pid = Path(directory) / "grandchild.pid"
            try:
                with self.assertRaisesRegex(TimeoutError, "timed out"):
                    MODULE._run_bounded(
                        [
                            sys.executable, "-c", parent_code, str(child_pid),
                            str(grandchild_pid), child_code,
                        ],
                        environment=dict(os.environ), timeout=1.0,
                    )
                self.assertTrue(child_pid.is_file())
                self.assertTrue(grandchild_pid.is_file())
                observed = [int(child_pid.read_text()), int(grandchild_pid.read_text())]
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline and any(
                    self.process_exists(pid) for pid in observed
                ):
                    time.sleep(0.02)
                self.assertEqual(
                    [pid for pid in observed if self.process_exists(pid)], [],
                    "timed-out descendant process survived group cleanup",
                )
            finally:
                for pid in observed:
                    if self.process_exists(pid):
                        os.kill(pid, signal.SIGKILL)

    @staticmethod
    def process_exists(pid):
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False

    def test_exact_revision_and_stable_graph(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.md"
            path.write_text("# Plan: Restore audio\n\n## Build\n\n1. Test\n")
            first = MODULE.plan_payload(str(path))
            second = MODULE.plan_payload(str(path))
        self.assertEqual(first, second)
        self.assertEqual(first["root"]["title"], "Restore audio")
        self.assertEqual([item["title"] for item in first["children"]], ["Build", "Test"])
        self.assertEqual(first["root"]["plan_revision"], first["source"]["sha256"])

    def test_revision_changes_when_source_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.md"
            path.write_text("# One\n")
            first = MODULE.plan_payload(str(path))
            path.write_text("# Two\n")
            second = MODULE.plan_payload(str(path))
        self.assertNotEqual(first["root"]["plan_revision"], second["root"]["plan_revision"])
        self.assertEqual(first["root"]["stable_key"], second["root"]["stable_key"])

    def test_canonical_identity_deduplicates_local_and_pasted_sources(self):
        identity = "https://example.test/plans/work.md"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.md"
            path.write_bytes(b"# Work\n\n## Build\n")
            local = MODULE.plan_payload(str(path), identity)
        stdin = mock.Mock()
        stdin.buffer = io.BytesIO(b"# Work\n\n## Build\n")
        with mock.patch.object(MODULE.sys, "stdin", stdin):
            pasted = MODULE.plan_payload("-", identity)
        self.assertEqual(local, pasted)

    def test_inline_content_has_content_addressed_identity(self):
        stdin = mock.Mock()
        stdin.buffer = io.BytesIO(b"# Work\n")
        with mock.patch.object(MODULE.sys, "stdin", stdin):
            payload = MODULE.plan_payload("-")
        self.assertEqual(
            payload["source"]["identity"],
            f"inline-sha256:{payload['source']['sha256']}",
        )

    def test_child_keys_are_unique_across_sections_and_ignore_fenced_examples(self):
        markdown = """# Work

## Build
1. Verify

```md
## Not a task
1. Not a task
```

## Release
1. Verify
"""
        children = MODULE.extract_children(markdown)
        self.assertEqual(
            [item["title"] for item in children],
            ["Build", "Verify", "Release", "Verify"],
        )
        self.assertEqual(len({item["key"] for item in children}), len(children))

    def test_child_keys_survive_unrelated_prose_edit(self):
        first = MODULE.extract_children("# Work\n\n## Build\n\n1. Verify\n")
        second = MODULE.extract_children("# Work\n\nNew context.\n\n## Build\n\n1. Verify\n")
        self.assertEqual(
            [item["key"] for item in first],
            [item["key"] for item in second],
        )

    def test_title_ignores_headings_in_fenced_examples(self):
        markdown = "```md\n# Example\n```\n\n# Actual plan\n"
        self.assertEqual(MODULE.first_heading(markdown).group(2), "Actual plan")

    def test_payload_is_json_serializable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "plan.md"
            path.write_text("# Plan\n")
            json.dumps(MODULE.plan_payload(str(path)))


if __name__ == "__main__":
    unittest.main()
