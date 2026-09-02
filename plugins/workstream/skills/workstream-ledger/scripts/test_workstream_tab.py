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
    def __init__(self, title="Linear", accept_rename=True,
                 *, resolve_target=None):
        self.title = title
        self.accept_rename = accept_rename
        self.calls = []
        self.options = []
        self.resolve_target = resolve_target

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        self.options.append(kwargs)
        if argv[1] == "rpc":
            output = json.dumps({
                "workspace_id": "workspace:3",
                "surface_id": self.resolve_target,
            }) if self.resolve_target else "{}"
        elif argv[1] == "ping":
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


class FakeHerdr:
    def __init__(self, label="Linear", *, workspace="w1", accept_rename=True):
        self.label = label
        self.workspace = workspace
        self.accept_rename = accept_rename
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        if argv[1:3] == ["tab", "get"]:
            output = json.dumps({
                "id": "request-1", "result": {
                    "type": "tab_info", "tab": {
                        "tab_id": argv[3], "workspace_id": self.workspace,
                        "number": 1, "label": self.label, "focused": True,
                        "pane_count": 1, "agent_status": "idle",
                    },
                },
            })
        elif argv[1:3] == ["tab", "rename"]:
            if self.accept_rename:
                self.label = argv[4]
            output = json.dumps({"id": "request-2", "result": {
                "type": "tab_info", "tab": {
                    "tab_id": argv[3], "workspace_id": self.workspace,
                    "number": 1, "label": self.label, "focused": True,
                    "pane_count": 1, "agent_status": "idle",
                },
            }})
        else:
            raise AssertionError(argv)
        return subprocess.CompletedProcess(argv, 0, output, "")


class WorkstreamTabTests(unittest.TestCase):
    def apply(
        self, title="Linear", token="GEN-37", *, project_name=None,
        automatic_title=None,
    ):
        fake = FakeCmux(title)
        result = tab.apply_title(
            token, target="surface:7", project_name=project_name,
            automatic_title=automatic_title, runner=fake,
            which=lambda _: "/opt/cmux",
        )
        return fake, result

    def test_appends_one_canonical_token_and_verifies_readback(self):
        fake, result = self.apply("Linear")
        self.assertEqual(result["status"], "updated")
        self.assertEqual(result["title"], "Linear · GEN-37")
        self.assertEqual([call[1] for call in fake.calls], [
            "ping", "identify", "list-pane-surfaces", "list-pane-surfaces",
            "rename-tab", "list-pane-surfaces",
        ])
        self.assertEqual(fake.calls[4][-1], "Linear · GEN-37")

    def test_surface_id_precedes_workspace_valued_legacy_tab_id(self):
        fake = FakeCmux("Linear", resolve_target="surface:7")
        result = tab.apply_title(
            "GEN-37", environ={
                "CMUX_SURFACE_ID": "surface:7",
                "CMUX_TAB_ID": "workspace:3",
            }, runner=fake, which=lambda _: "/opt/cmux",
        )
        self.assertEqual(result["status"], "updated")
        self.assertEqual(fake.calls[1][fake.calls[1].index("--surface") + 1], "surface:7")

    def test_absent_surface_uses_one_bounded_controlling_tty_target(self):
        fake = FakeCmux("Linear", resolve_target="surface:7")
        with mock.patch.object(tab, "_bounded_ancestor_pids", return_value=[42]):
            result = tab.apply_title(
                "GEN-37", environ={"CMUX_SOCKET_PATH": "/tmp/cmux.sock"}, runner=fake,
                which=lambda _: "/opt/cmux",
            )
        self.assertEqual(result["status"], "updated")
        self.assertTrue(any("agent.resolve_delivery_target" in call for call in fake.calls))

    def test_explicit_target_failure_never_retargets_through_tty(self):
        fake = FakeCmux("Linear", resolve_target="surface:other")
        def runner(argv, **kwargs):
            if argv[1] == "identify" and "surface:missing" in argv:
                return subprocess.CompletedProcess(argv, 1, "", "unknown surface")
            return fake(argv, **kwargs)
        with mock.patch.object(tab, "_bounded_ancestor_pids", return_value=[42]):
            result = tab.apply_title(
                "GEN-37", target="surface:missing", runner=runner,
                which=lambda _: "/opt/cmux",
            )
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["reason"], "cmux_target_unresolved")
        self.assertNotIn("rename-tab", [call[1] for call in fake.calls])

    def test_tty_target_resolver_preserves_unresolved_vs_ambiguous(self):
        with mock.patch.object(tab, "_bounded_ancestor_pids", return_value=[1, 2]):
            def ambiguous(argv, **kwargs):
                payload = json.loads(argv[-1])
                surface = "surface:a" if payload["pid"] == 1 else "surface:b"
                return subprocess.CompletedProcess(argv, 0, json.dumps({
                    "workspace_id": "workspace:3", "surface_id": surface,
                }), "")
            with self.assertRaisesRegex(tab.TabTitleError, "cmux_target_ambiguous"):
                tab._resolve_cmux_tty_target("cmux", ambiguous, {})

        with mock.patch.object(tab, "_bounded_ancestor_pids", return_value=[1]):
            with self.assertRaisesRegex(tab.TabTitleError, "cmux_target_unresolved"):
                tab._resolve_cmux_tty_target(
                    "cmux", lambda argv, **kwargs: subprocess.CompletedProcess(
                        argv, 0, "{}", "",
                    ), {},
                )

    def test_cmux_commands_inherit_the_exact_socket_namespace(self):
        fake = FakeCmux("Linear · GEN-37")
        environment = {
            "CMUX_SURFACE_ID": "surface:7",
            "CMUX_SOCKET_PATH": "/tmp/cmux-exact.sock",
        }
        result = tab.apply_title(
            "GEN-37", target="surface:7", environ=environment, runner=fake,
            which=lambda _: "/opt/cmux",
        )
        self.assertEqual(result["status"], "unchanged")
        self.assertTrue(fake.options)
        self.assertTrue(all(
            options["env"]["CMUX_SOCKET_PATH"] == "/tmp/cmux-exact.sock"
            for options in fake.options
        ))

    def test_unnamed_title_becomes_project_label_and_token(self):
        _, result = self.apply("   ", project_name="Linear Integration")
        self.assertEqual(result["title"], "Linear Integration · GEN-37")

    def test_exact_automatic_title_is_replaced_but_custom_title_is_preserved(self):
        fake, generated = self.apply(
            "~/Code/pulp", project_name="Linear Integration",
            automatic_title="~/Code/pulp",
        )
        self.assertEqual(generated["title"], "Linear Integration · GEN-37")
        self.assertEqual(fake.calls[4][-1], "Linear Integration · GEN-37")

        _, custom = self.apply(
            "My project", project_name="Linear Integration",
        )
        self.assertEqual(custom["title"], "My project · GEN-37")

    def test_missing_or_stale_generated_title_provenance_is_optional_noop(self):
        for title, options, error in (
            ("", {}, "project_name_required_for_generated_title"),
            ("pulp", {"project_name": "Linear", "automatic_title": "zsh"},
             "automatic_title_changed"),
            ("", {"project_name": "GEN-37 project"}, "invalid_project_name"),
        ):
            with self.subTest(title=title, error=error):
                fake = FakeCmux(title)
                result = tab.apply_title(
                    "GEN-37", target="surface:7", runner=fake,
                    which=lambda _: "/opt/cmux", **options,
                )
                self.assertEqual(result["status"], "unavailable")
                self.assertEqual(result["reason"], error)
                self.assertNotIn("rename-tab", [call[1] for call in fake.calls])

    def test_same_canonical_token_is_a_zero_mutation_noop(self):
        fake, result = self.apply("Linear · GEN-37")
        self.assertEqual(result["status"], "unchanged")
        self.assertEqual(result["title"], "Linear · GEN-37")
        self.assertNotIn("rename-tab", [call[1] for call in fake.calls])

    def test_lowercase_or_embedded_token_refuses_as_noncanonical(self):
        for title in ("Linear · gen-37", "Investigate GEN-37 now"):
            with self.subTest(title=title):
                fake = FakeCmux(title)
                with self.assertRaisesRegex(
                    tab.TabTitleError,
                    "title_contains_noncanonical_workstream_token",
                ):
                    tab.apply_title(
                        "GEN-37", target="surface:7", runner=fake,
                        which=lambda _: "/opt/cmux",
                    )
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

        fake = FakeCmux("Linear · GEN-38")
        with self.assertRaisesRegex(tab.TabTitleError, "workstream_tab_conflict"):
            tab.apply_title(
                "GEN-37", target="surface:7", runner=fake,
                which=lambda _: "/opt/cmux",
            )

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

    def test_reachable_cmux_with_unresolved_target_is_optional_noop(self):
        fake = FakeCmux()

        def runner(argv, **kwargs):
            if argv[1] == "identify":
                return subprocess.CompletedProcess(argv, 1, "", "unknown surface")
            return fake(argv, **kwargs)

        result = tab.apply_title(
            "GEN-37", target="surface:404", runner=runner,
            which=lambda _: "/opt/cmux",
        )
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["reason"], "cmux_target_unresolved")
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

    def test_cli_forwards_explicit_project_and_automatic_title_contract(self):
        result = {"status": "updated", "token": "GEN-37"}
        with mock.patch.object(tab, "apply_title", return_value=result) as apply, \
             mock.patch.object(sys, "stdout", new_callable=io.StringIO):
            self.assertEqual(tab.main([
                "GEN-37", "--surface", "surface:7",
                "--project-name", "Linear Integration",
                "--automatic-title", "~/Code/pulp",
            ]), 0)
        apply.assert_called_once_with(
            "GEN-37", target="surface:7", project_name="Linear Integration",
            automatic_title="~/Code/pulp",
        )

    def test_pre_mutation_title_unavailable_or_missing_target_is_optional_noop(self):
        for mode in ("command_unavailable", "target_missing"):
            with self.subTest(mode=mode):
                fake = FakeCmux()

                def runner(argv, **kwargs):
                    result = fake(argv, **kwargs)
                    if argv[1] == "list-pane-surfaces":
                        if mode == "command_unavailable":
                            return subprocess.CompletedProcess(
                                argv, 1, "", "socket stopped",
                            )
                        result.stdout = '{"surfaces":[]}'
                    return result

                result = tab.apply_title(
                    "GEN-37", target="surface:7", runner=runner,
                    which=lambda _: "/opt/cmux",
                )
                self.assertEqual(result, {
                    "status": "unavailable", "reason": "cmux_target_unresolved",
                    "token": "GEN-37",
                })
                self.assertNotIn("rename-tab", [call[1] for call in fake.calls])

    def test_malformed_title_read_fails_closed(self):
        fake = FakeCmux()

        def runner(argv, **kwargs):
            result = fake(argv, **kwargs)
            if argv[1] == "list-pane-surfaces":
                result.stdout = '{"surfaces":"not-a-list"}'
            return result

        with self.assertRaisesRegex(tab.TabTitleError, "invalid_cmux_surface_response"):
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

    def test_concurrent_cmux_rename_is_fenced_before_overwrite(self):
        fake = FakeCmux("Linear")
        reads = 0

        def runner(argv, **kwargs):
            nonlocal reads
            if argv[1] == "list-pane-surfaces":
                reads += 1
                if reads == 2:
                    fake.title = "Human renamed this tab"
            return fake(argv, **kwargs)

        with self.assertRaisesRegex(tab.TabTitleError, "cmux_title_changed"):
            tab.apply_title(
                "GEN-37", target="surface:7", expected_title="Linear",
                runner=runner, which=lambda _: "/opt/cmux",
            )
        self.assertEqual(fake.title, "Human renamed this tab")
        self.assertNotIn("rename-tab", [call[1] for call in fake.calls])

    def test_terminal_manager_detection_is_shared_and_ambiguous_fails(self):
        herdr_without_flag = self.herdr_env()
        herdr_without_flag.pop("HERDR_ENV")
        result = tab.apply_title(
            "GEN-37", environ=herdr_without_flag, runner=FakeHerdr(),
            which=lambda _: None,
        )
        self.assertEqual(result["manager"], "herdr")

        ambiguous = dict(herdr_without_flag, CMUX_SURFACE_ID="surface:7")
        with self.assertRaisesRegex(
            tab.TabTitleError, "terminal_context_ambiguous",
        ):
            tab.apply_title(
                "GEN-37", environ=ambiguous, runner=FakeHerdr(),
                which=lambda _: None,
            )

    def test_post_rename_readback_unavailable_remains_fatal(self):
        fake = FakeCmux("Linear")
        reads = 0

        def runner(argv, **kwargs):
            nonlocal reads
            if argv[1] == "list-pane-surfaces":
                reads += 1
                if reads == 3:
                    return subprocess.CompletedProcess(
                        argv, 1, "", "socket stopped",
                    )
            return fake(argv, **kwargs)

        with self.assertRaisesRegex(tab.TabTitleError, "cmux_command_failed"):
            tab.apply_title(
                "GEN-37", target="surface:7", runner=runner,
                which=lambda _: "/opt/cmux",
            )
        self.assertIn("rename-tab", [call[1] for call in fake.calls])

    @staticmethod
    def herdr_env(socket="/tmp/herdr-a.sock"):
        return {
            "HERDR_ENV": "1", "HERDR_BIN_PATH": "/opt/herdr",
            "HERDR_SOCKET_PATH": socket, "HERDR_WORKSPACE_ID": "w1",
            "HERDR_TAB_ID": "w1:t1", "HERDR_PANE_ID": "w1:p1",
        }

    def test_herdr_appends_token_and_verifies_exact_readback(self):
        fake = FakeHerdr("Linear")
        result = tab.apply_title(
            "GEN-37", environ=self.herdr_env(), runner=fake,
            which=lambda _: None,
        )
        self.assertEqual(result["status"], "updated")
        self.assertEqual(result["manager"], "herdr")
        self.assertEqual(result["title"], "Linear · GEN-37")
        self.assertEqual([call[1:3] for call in fake.calls], [
            ["tab", "get"], ["tab", "get"], ["tab", "rename"],
            ["tab", "get"],
        ])

    def test_herdr_same_token_is_noop_and_conflict_refuses(self):
        fake = FakeHerdr("Linear · GEN-37")
        result = tab.apply_title(
            "GEN-37", environ=self.herdr_env(), runner=fake,
            which=lambda _: None,
        )
        self.assertEqual(result["status"], "unchanged")
        self.assertNotIn(["tab", "rename"], [call[1:3] for call in fake.calls])

        conflicting = FakeHerdr("Linear · GEN-38")
        with self.assertRaisesRegex(tab.TabTitleError, "workstream_tab_conflict"):
            tab.apply_title(
                "GEN-37", environ=self.herdr_env(), runner=conflicting,
                which=lambda _: None,
            )
        self.assertNotIn(
            ["tab", "rename"], [call[1:3] for call in conflicting.calls],
        )

    def test_herdr_unnamed_and_exact_automatic_titles_use_project_label(self):
        for before, automatic_title in (("", None), ("~/Code/pulp", "~/Code/pulp")):
            with self.subTest(before=before):
                fake = FakeHerdr(before)
                result = tab.apply_title(
                    "GEN-37", environ=self.herdr_env(), runner=fake,
                    which=lambda _: None, project_name="Linear Integration",
                    automatic_title=automatic_title,
                )
                self.assertEqual(result["title"], "Linear Integration · GEN-37")
                self.assertEqual([call[1:3] for call in fake.calls], [
                    ["tab", "get"], ["tab", "get"], ["tab", "rename"],
                    ["tab", "get"],
                ])

    def test_herdr_missing_or_stale_generated_provenance_is_optional_noop(self):
        for before, options, reason in (
            ("", {}, "project_name_required_for_generated_title"),
            ("~/Code/pulp", {
                "project_name": "Linear Integration",
                "automatic_title": "zsh",
            }, "automatic_title_changed"),
        ):
            with self.subTest(before=before, reason=reason):
                fake = FakeHerdr(before)
                result = tab.apply_title(
                    "GEN-37", environ=self.herdr_env(), runner=fake,
                    which=lambda _: None, **options,
                )
                self.assertEqual(result["status"], "unavailable")
                self.assertEqual(result["reason"], reason)
                self.assertNotIn(
                    ["tab", "rename"], [call[1:3] for call in fake.calls],
                )

    def test_herdr_missing_identity_binary_or_target_is_optional_noop(self):
        called = False

        def runner(*args, **kwargs):
            nonlocal called
            called = True
            raise AssertionError("must not run")

        missing = self.herdr_env()
        missing.pop("HERDR_TAB_ID")
        result = tab.apply_title(
            "GEN-37", environ=missing, runner=runner, which=lambda _: None,
        )
        self.assertEqual(result["reason"], "herdr_identity_unavailable")
        self.assertFalse(called)

        no_binary = self.herdr_env()
        no_binary.pop("HERDR_BIN_PATH")
        result = tab.apply_title(
            "GEN-37", environ=no_binary, runner=runner, which=lambda _: None,
        )
        self.assertEqual(result["reason"], "herdr_cli_unavailable")
        self.assertFalse(called)

        def unavailable(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 1, "", "stopped socket")

        result = tab.apply_title(
            "GEN-37", environ=self.herdr_env(), runner=unavailable,
            which=lambda _: None,
        )
        self.assertEqual(result["reason"], "herdr_target_unresolved")

    def test_herdr_named_session_namespace_prevents_public_id_collision(self):
        first = tab.apply_title(
            "GEN-37", environ=self.herdr_env("/tmp/herdr-a.sock"),
            runner=FakeHerdr(), which=lambda _: None,
        )
        second = tab.apply_title(
            "GEN-37", environ=self.herdr_env("/tmp/herdr-b.sock"),
            runner=FakeHerdr(), which=lambda _: None,
        )
        self.assertEqual(first["tab"], second["tab"])
        self.assertNotEqual(
            first["session_namespace_sha256"],
            second["session_namespace_sha256"],
        )

    def test_herdr_workspace_mismatch_is_unavailable_and_readback_mismatch_refuses(self):
        unresolved = tab.apply_title(
            "GEN-37", environ=self.herdr_env(),
            runner=FakeHerdr(workspace="w2"), which=lambda _: None,
        )
        self.assertEqual(unresolved["reason"], "herdr_target_unresolved")

        with self.assertRaisesRegex(tab.TabTitleError, "herdr_title_readback_mismatch"):
            tab.apply_title(
                "GEN-37", environ=self.herdr_env(),
                runner=FakeHerdr(accept_rename=False), which=lambda _: None,
            )


if __name__ == "__main__":
    unittest.main()
