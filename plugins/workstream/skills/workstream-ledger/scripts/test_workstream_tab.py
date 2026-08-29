import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("workstream_tab.py")
SPEC = importlib.util.spec_from_file_location("workstream_tab", SCRIPT)
tab = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
sys.modules["workstream_tab"] = tab
SPEC.loader.exec_module(tab)


class FakeCmux:
    def __init__(self, title="Linear", accept_rename=True):
        self.title = title
        self.accept_rename = accept_rename
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        if argv[1] == "ping":
            output = "pong"
        elif argv[1] == "identify":
            output = json.dumps({"caller": {
                "surface_ref": "surface:7", "pane_ref": "pane:2",
                "workspace_ref": "workspace:3", "window_ref": "window:1",
            }})
        elif argv[1] == "list-pane-surfaces":
            output = json.dumps({"surfaces": [{"ref": "surface:7", "title": self.title}]})
        elif argv[1] == "rename-tab":
            if self.accept_rename:
                self.title = argv[-1]
            output = "{}"
        else:
            raise AssertionError(argv)
        return subprocess.CompletedProcess(argv, 0, output, "")


class WorkstreamTabTests(unittest.TestCase):
    def apply(self, title="Linear", token="GEN-37"):
        fake = FakeCmux(title)
        result = tab.apply_title(
            token, target="surface:7", runner=fake, which=lambda _: "/opt/cmux",
        )
        return fake, result

    def test_appends_one_canonical_token_and_verifies_readback(self):
        fake, result = self.apply("Linear")
        self.assertEqual(result["status"], "updated")
        self.assertEqual(result["title"], "Linear · GEN-37")
        self.assertEqual([call[1] for call in fake.calls], [
            "ping", "identify", "list-pane-surfaces", "rename-tab", "list-pane-surfaces",
        ])
        self.assertEqual(fake.calls[3][-1], "Linear · GEN-37")

    def test_empty_title_becomes_token_without_separator(self):
        _, result = self.apply("   ")
        self.assertEqual(result["title"], "GEN-37")

    def test_same_token_is_a_zero_mutation_noop(self):
        fake, result = self.apply("Linear · gen-37")
        self.assertEqual(result["status"], "unchanged")
        self.assertEqual(result["title"], "Linear · gen-37")
        self.assertNotIn("rename-tab", [call[1] for call in fake.calls])

    def test_conflicting_or_duplicate_token_fails_before_mutation(self):
        for title in ("Linear · GEN-38", "GEN-37 / GEN-37", "GEN-37 / GEN-38"):
            with self.subTest(title=title):
                fake = FakeCmux(title)
                with self.assertRaises(tab.TabTitleError):
                    tab.apply_title(
                        "GEN-37", target="surface:7", runner=fake,
                        which=lambda _: "/opt/cmux",
                    )
                self.assertNotIn("rename-tab", [call[1] for call in fake.calls])

    def test_non_cmux_fallback_is_successful_and_does_not_probe(self):
        called = False

        def runner(*args, **kwargs):
            nonlocal called
            called = True
            raise AssertionError("must not run")

        result = tab.apply_title("gen-37", environ={}, runner=runner)
        self.assertEqual(result, {
            "status": "unavailable", "reason": "not_in_cmux_surface", "token": "GEN-37",
        })
        self.assertFalse(called)

    def test_installed_but_unreachable_cmux_falls_back_without_mutation(self):
        def runner(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 1, "", "no socket")

        result = tab.apply_title(
            "GEN-37", target="surface:7", runner=runner,
            which=lambda _: "/opt/cmux",
        )
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["reason"], "cmux_unavailable")

    def test_reachable_cmux_with_unresolved_target_fails_closed(self):
        fake = FakeCmux()

        def runner(argv, **kwargs):
            if argv[1] == "identify":
                return subprocess.CompletedProcess(argv, 1, "", "unknown surface")
            return fake(argv, **kwargs)

        with self.assertRaisesRegex(tab.TabTitleError, "cmux_command_failed"):
            tab.apply_title(
                "GEN-37", target="surface:404", runner=runner,
                which=lambda _: "/opt/cmux",
            )
        self.assertNotIn("rename-tab", [call[1] for call in fake.calls])

    def test_reachable_cmux_without_caller_binding_is_optional_noop(self):
        fake = FakeCmux()

        def runner(argv, **kwargs):
            if argv[1] == "identify":
                return subprocess.CompletedProcess(argv, 0, "{}", "")
            return fake(argv, **kwargs)

        result = tab.apply_title(
            "GEN-37", target="surface:404", runner=runner,
            which=lambda _: "/opt/cmux",
        )

        self.assertEqual(result, {
            "status": "unavailable", "reason": "cmux_target_unresolved",
            "token": "GEN-37",
        })
        self.assertNotIn("rename-tab", [call[1] for call in fake.calls])

    def test_cli_distinguishes_optional_unavailable_from_binding_error(self):
        unavailable = {
            "status": "unavailable", "reason": "cmux_target_unresolved",
            "token": "GEN-37",
        }
        with mock.patch.object(tab, "apply_title", return_value=unavailable), \
             mock.patch.object(sys, "stdout", new_callable=io.StringIO) as stdout:
            self.assertEqual(tab.main(["GEN-37"]), 0)
        self.assertEqual(json.loads(stdout.getvalue()), unavailable)

        with mock.patch.object(
            tab, "apply_title", side_effect=tab.TabTitleError("cmux_command_failed"),
        ), mock.patch.object(
            sys, "stderr", new_callable=io.StringIO,
        ) as stderr:
            self.assertEqual(tab.main(["GEN-37"]), 2)
        self.assertEqual(stderr.getvalue().strip(), "workstream-tab: cmux_command_failed")

    def test_malformed_title_read_fails_closed(self):
        fake = FakeCmux()

        def runner(argv, **kwargs):
            result = fake(argv, **kwargs)
            if argv[1] == "list-pane-surfaces":
                result.stdout = '{"surfaces":[]}'
            return result

        with self.assertRaisesRegex(tab.TabTitleError, "cmux_target_title_unresolved"):
            tab.apply_title(
                "GEN-37", target="surface:7", runner=runner,
                which=lambda _: "/opt/cmux",
            )
        self.assertNotIn("rename-tab", [call[1] for call in fake.calls])

    def test_negative_control_rejects_overwrite_transition(self):
        with self.assertRaisesRegex(tab.TabTitleError, "existing_title_not_preserved"):
            tab.validate_transition("Human title", "GEN-37", "GEN-37")

    def test_rename_requires_exact_readback(self):
        fake = FakeCmux("Linear", accept_rename=False)
        with self.assertRaisesRegex(tab.TabTitleError, "cmux_title_readback_mismatch"):
            tab.apply_title(
                "GEN-37", target="surface:7", runner=fake,
                which=lambda _: "/opt/cmux",
            )


if __name__ == "__main__":
    unittest.main()
