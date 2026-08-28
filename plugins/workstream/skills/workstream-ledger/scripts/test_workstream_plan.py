import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("workstream_plan.py")
SPEC = importlib.util.spec_from_file_location("workstream_plan", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class PlanIntakeTests(unittest.TestCase):
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
