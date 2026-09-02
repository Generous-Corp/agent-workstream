import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("workstream_this_session.py")
SPEC = importlib.util.spec_from_file_location("workstream_this_session", SCRIPT)
session = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
sys.path.insert(0, str(SCRIPT.parent))
SPEC.loader.exec_module(session)


class FakeCmux:
    def __init__(self, title="Linear", *, surface="surface-old", workspace="workspace-1",
                 surface_ref=None, workspace_ref=None, fail_rename=False,
                 pane="pane-id", pane_ref="pane:1", caller_surface=None,
                 caller_workspace=None, caller_pane=None):
        self.title = title
        self.surface = surface
        self.workspace = workspace
        self.surface_ref = surface_ref or surface
        self.workspace_ref = workspace_ref or workspace
        self.pane = pane
        self.pane_ref = pane_ref
        self.fail_rename = fail_rename
        self.caller_surface = caller_surface or surface
        self.caller_workspace = caller_workspace or workspace
        self.caller_pane = caller_pane or pane
        self.calls = []
        self.options = []

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        self.options.append(kwargs)
        action = next((value for value in (
            "ping", "identify", "list-pane-surfaces", "rename-tab",
        ) if value in argv), None)
        if action == "ping":
            return subprocess.CompletedProcess(argv, 0, "pong", "")
        if action == "identify":
            socket = argv[argv.index("--socket") + 1] if "--socket" in argv else "/tmp/cmux-a.sock"
            value = {
                "socket_path": socket, "bundle_identifier": "com.cmuxterm.app",
                "app_bundle_path": "/Applications/cmux.app", "caller": {
                "surface_id": self.caller_surface,
                "surface_ref": self.caller_surface,
                "workspace_id": self.caller_workspace,
                "workspace_ref": self.caller_workspace,
                "pane_id": self.caller_pane, "pane_ref": self.caller_pane,
                "window_id": "window-1", "window_ref": "window-1",
            }}
            return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")
        if "rpc" in argv:
            method = argv[argv.index("rpc") + 1]
            if method == "workspace.list":
                value = {"workspaces": [{
                    "id": self.workspace, "ref": self.workspace_ref,
                }]}
            elif method == "surface.list":
                value = {"surfaces": [{
                    "id": self.surface, "ref": self.surface_ref,
                    "pane_id": self.pane, "pane_ref": self.pane_ref,
                    "title": self.title,
                }]}
            else:
                raise AssertionError(argv)
            return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")
        if action == "list-pane-surfaces":
            value = {"surfaces": [{
                "id": self.surface, "ref": self.surface_ref,
                "title": self.title,
            }]}
            return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")
        if action == "rename-tab":
            if self.fail_rename:
                return subprocess.CompletedProcess(argv, 1, "", "refused")
            self.title = argv[-1]
            return subprocess.CompletedProcess(argv, 0, "{}", "")
        raise AssertionError(argv)


class FakeHerdr:
    def __init__(self, label="Linear", *, tab="tab-1", workspace="workspace-1"):
        self.label = label
        self.tab = tab
        self.workspace = workspace
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        if argv[1:3] == ["tab", "get"]:
            value = {"result": {"type": "tab_info", "tab": {
                "tab_id": self.tab, "workspace_id": self.workspace,
                "label": self.label,
            }}}
        elif argv[1:3] == ["tab", "rename"]:
            self.label = argv[4]
            value = {"result": {"type": "tab_info", "tab": {
                "tab_id": self.tab, "workspace_id": self.workspace,
                "label": self.label,
            }}}
        else:
            raise AssertionError(argv)
        return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")


def full_resume_runner(calls, *, project="Linear Integration", authority="full",
                       options=None):
    def run(argv, **kwargs):
        calls.append(argv)
        if options is not None:
            options.append(kwargs)
        return subprocess.CompletedProcess(argv, 0, json.dumps({
            "resume_authority": authority, "workstream_id": argv[-1],
            "project_name": project, "children": [],
        }), "")
    return run


class ThisSessionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "bindings.sqlite3"

    @staticmethod
    def cmux_env(surface="surface-old", socket="/tmp/cmux-a.sock", session_id="new"):
        value = {
            "CMUX_SURFACE_ID": surface, "CMUX_WORKSPACE_ID": "workspace-1",
            "CMUX_SOCKET_PATH": socket, "CODEX_SESSION_ID": session_id,
        }
        return value

    @staticmethod
    def herdr_env(socket="/tmp/herdr-a.sock"):
        return {
            "HERDR_ENV": "1", "HERDR_TAB_ID": "tab-1",
            "HERDR_WORKSPACE_ID": "workspace-1", "HERDR_SOCKET_PATH": socket,
            "HERDR_BIN_PATH": "/opt/herdr", "CODEX_SESSION_ID": "new",
        }

    def seed(self, fake, token="GEN-37", *, env=None, provider_session="old"):
        env = dict(env or self.cmux_env())
        resolution = session.resolve_this_session(
            environ=env, runner=fake, which=lambda _: "/opt/cmux",
            binding_path=self.db,
        ) if session.workstream_tab.tokens_in_title(fake.title) else {
            **session._terminal_identity(env), "workstream_id": token,
        }
        resolution["namespace_sha256"] = session._namespace("cmux", {
            "socket_path": env["CMUX_SOCKET_PATH"],
            "bundle_identifier": "com.cmuxterm.app",
            "app_bundle_path": "/Applications/cmux.app",
        })
        env["CODEX_SESSION_ID"] = provider_session
        return session.record_successor_binding(
            self.db, resolution, environ=env, created_at="2026-09-01T00:00:00Z",
        )

    @staticmethod
    def resolution(*, title="Linear · GEN-37", binding=None):
        return {
            "manager": "cmux", "namespace_sha256": "a" * 64,
            "workspace_id": "workspace-1", "target_id": "surface-old",
            "cmux_socket_path": "/tmp/cmux-a.sock",
            "workstream_id": "GEN-37", "candidate_source": (
                "binding_and_title" if binding else "title"
            ), "observed_title": title, "prior_binding": binding,
        }

    def test_old_spectr_surface_binding_repairs_linear_title_and_records_successor(self):
        env = self.cmux_env("08C477D7-0094-4DCB-B5C2-39140C17426A")
        fake = FakeCmux("Linear", surface=env["CMUX_SURFACE_ID"])
        self.seed(fake, env=env)
        calls = []
        result = session.resume_this_session(
            environ=env, runner=full_resume_runner(calls), terminal_runner=fake,
            which=lambda _: "/opt/cmux", binding_path=self.db,
            resume_script=Path("resume.py"), created_at="2026-09-01T01:00:00Z",
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(result["this_session_resolution"]["candidate_source"], "binding")
        self.assertEqual(result["tab_binding"]["title"], "Linear · GEN-37")
        self.assertEqual(result["resume_binding"]["predecessor_provider_session_id"], "old")
        self.assertEqual(len(calls), 1)

    def test_replacement_surface_title_only_resumes_binds_and_repeats_without_writes(self):
        env = self.cmux_env("211BECC6-4E03-4C8A-A0FE-B04E89590B77")
        fake = FakeCmux("Spectr · GEN-37", surface=env["CMUX_SURFACE_ID"])
        calls = []
        first = session.resume_this_session(
            environ=env, runner=full_resume_runner(calls), terminal_runner=fake,
            which=lambda _: "/opt/cmux", binding_path=self.db,
            resume_script=Path("resume.py"), created_at="2026-09-01T01:00:00Z",
        )
        second = session.resume_this_session(
            environ=env, runner=full_resume_runner(calls), terminal_runner=fake,
            which=lambda _: "/opt/cmux", binding_path=self.db,
            resume_script=Path("resume.py"), created_at="2026-09-01T02:00:00Z",
        )
        self.assertEqual(first["resume_binding"]["writes_performed"], 1)
        self.assertEqual(second["resume_binding"]["writes_performed"], 0)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], [sys.executable, "resume.py", "GEN-37"])
        connection = sqlite3.connect(self.db)
        self.assertEqual(connection.execute(
            "SELECT count(*) FROM terminal_binding_events_v1"
        ).fetchone()[0], 1)
        connection.close()

    def test_authenticated_resume_timeout_is_separate_from_terminal_budget(self):
        """A slow Linear recovery must not make terminal probes unbounded."""
        env = self.cmux_env("211BECC6-0094-4DCB-B5C2-39140C17426A")
        fake = FakeCmux("Spectr · GEN-37", surface=env["CMUX_SURFACE_ID"])
        resume_options = []
        result = session.resume_this_session(
            environ=env,
            runner=full_resume_runner([], options=resume_options),
            terminal_runner=fake,
            which=lambda _: "/opt/cmux",
            binding_path=self.db,
            resume_script=Path("resume.py"),
            created_at="2026-09-01T01:00:00Z",
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(len(resume_options), 1)
        self.assertEqual(
            resume_options[0]["timeout"], session.RESUME_TIMEOUT_SECONDS,
        )
        self.assertGreaterEqual(session.RESUME_TIMEOUT_SECONDS, 60)
        self.assertTrue(fake.options)
        self.assertIn(
            session.TERMINAL_TIMEOUT_SECONDS,
            [option["timeout"] for option in fake.options],
        )
        self.assertTrue(all(
            option["timeout"] <= session.TERMINAL_TIMEOUT_SECONDS
            for option in fake.options
        ))

    def test_two_tokens_and_binding_title_mismatch_refuse_without_resume_or_write(self):
        for title, seed in (("GEN-37 / GEN-38", False), ("Linear · GEN-38", True)):
            with self.subTest(title=title):
                db = Path(self.temp.name) / ("binding.sqlite3" if seed else "multi.sqlite3")
                fake = FakeCmux(title)
                if seed:
                    self.db = db
                    self.seed(FakeCmux("Linear"), token="GEN-37")
                calls = []
                with self.assertRaises(session.ThisSessionError):
                    session.resume_this_session(
                        environ=self.cmux_env(), runner=full_resume_runner(calls),
                        terminal_runner=fake, which=lambda _: "/opt/cmux",
                        binding_path=db, resume_script=Path("resume.py"),
                    )
                self.assertEqual(calls, [])

    def test_same_cwd_different_exact_surfaces_do_not_collide(self):
        first_env = self.cmux_env("surface-a", "/tmp/cmux-a.sock")
        second_env = self.cmux_env("surface-b", "/tmp/cmux-a.sock")
        first_env["PWD"] = second_env["PWD"] = "/same/repo"
        first = session.resolve_this_session(
            environ=first_env, runner=FakeCmux("A · GEN-37", surface="surface-a"),
            which=lambda _: "/opt/cmux", binding_path=self.db,
        )
        second = session.resolve_this_session(
            environ=second_env, runner=FakeCmux("B · GEN-38", surface="surface-b"),
            which=lambda _: "/opt/cmux", binding_path=self.db,
        )
        self.assertEqual((first["workstream_id"], second["workstream_id"]),
                         ("GEN-37", "GEN-38"))

    def test_missing_injected_identity_never_uses_focus_and_explicit_resume_is_unchanged(self):
        called = False
        def forbidden(*args, **kwargs):
            nonlocal called
            called = True
            raise AssertionError("no terminal probe")
        with self.assertRaisesRegex(session.ThisSessionError, "session_context_unavailable"):
            session.resolve_this_session(environ={}, runner=forbidden,
                                        binding_path=self.db, which=lambda _: None)
        self.assertFalse(called)
        self.assertTrue(Path(__file__).with_name("workstream_resume.py").is_file())

    def test_cmux_bounded_ancestor_fallback_requires_one_consistent_target(self):
        fake = FakeCmux("Linear · GEN-37", surface="surface-resolved",
                        workspace="workspace-resolved")
        rpc_requests = []

        def runner(argv, **kwargs):
            if "agent.resolve_delivery_target" in argv:
                rpc_requests.append(json.loads(argv[-1]))
                return subprocess.CompletedProcess(argv, 0, json.dumps({
                    "pid_resolution": "controlling_tty", "source": "pid",
                    "surface_id": "surface-resolved",
                    "workspace_id": "workspace-resolved",
                }), "")
            return fake(argv, **kwargs)

        result = session.resolve_this_session(
            environ={}, runner=runner, which=lambda _: "/opt/cmux",
            binding_path=self.db, pid_chain=[10, 9], socket_candidates=[],
        )
        self.assertEqual(result["workstream_id"], "GEN-37")
        self.assertEqual(result["target_id"], "surface-resolved")
        self.assertEqual(result["workspace_id"], "workspace-resolved")
        self.assertEqual(rpc_requests, [
            {"pid": 10, "pid_resolution": "controlling_tty"},
            {"pid": 9, "pid_resolution": "controlling_tty"},
        ])
        all_argv = [item for call in fake.calls for item in call]
        self.assertNotIn("focused", all_argv)
        identify = [call for call in fake.calls if "identify" in call]
        self.assertTrue(any("surface-resolved" in call for call in identify))
        self.assertTrue(any("workspace-resolved" in call for call in identify))

    def test_cmux_ancestor_disagreement_refuses_before_title_or_resume(self):
        fake = FakeCmux("Linear · GEN-37")
        counter = 0

        def runner(argv, **kwargs):
            nonlocal counter
            if "agent.resolve_delivery_target" in argv:
                counter += 1
                return subprocess.CompletedProcess(argv, 0, json.dumps({
                    "surface_id": f"surface-{counter}",
                    "workspace_id": "workspace-1",
                }), "")
            return fake(argv, **kwargs)

        with self.assertRaisesRegex(session.ThisSessionError,
                                    "session_context_ambiguous"):
            session.resolve_this_session(
                environ={}, runner=runner, which=lambda _: "/opt/cmux",
                binding_path=self.db, pid_chain=[10, 9], socket_candidates=[],
            )

    def test_tagged_cmux_without_explicit_socket_refuses(self):
        with self.assertRaisesRegex(session.ThisSessionError,
                                    "cmux_tag_requires_socket"):
            session.resolve_this_session(
                environ={"CMUX_TAG": "nightly"}, runner=FakeCmux(),
                which=lambda _: "/opt/cmux", binding_path=self.db,
                pid_chain=[10], socket_candidates=[],
            )

    def test_resume_binding_without_provider_session_is_optional(self):
        env = self.cmux_env()
        del env["CODEX_SESSION_ID"]
        fake = FakeCmux("Linear · GEN-37")
        result = session.resume_this_session(
            environ=env, runner=full_resume_runner([]), terminal_runner=fake,
            which=lambda _: "/opt/cmux", binding_path=self.db,
            resume_script=Path("resume.py"),
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertIsNone(result["resume_binding"]["provider_session_id"])

    def test_tab_adapter_failure_cannot_downgrade_full_resume(self):
        env = self.cmux_env()
        fake = FakeCmux("Linear · GEN-37", fail_rename=True)
        with mock.patch.object(
            session.workstream_tab, "apply_title",
            side_effect=session.workstream_tab.TabTitleError("cmux_command_failed"),
        ):
            result = session.resume_this_session(
                environ=env, runner=full_resume_runner([]), terminal_runner=fake,
                which=lambda _: "/opt/cmux", binding_path=self.db,
                resume_script=Path("resume.py"),
            )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(result["tab_binding"], {
            "status": "unavailable", "reason": "cmux_command_failed",
        })

    def test_herdr_socket_namespace_separates_same_public_ids(self):
        fake = FakeHerdr("Linear · GEN-37")
        first = session.resolve_this_session(
            environ=self.herdr_env("/tmp/herdr-a.sock"), runner=fake,
            which=lambda _: None, binding_path=self.db,
        )
        second = session.resolve_this_session(
            environ=self.herdr_env("/tmp/herdr-b.sock"), runner=fake,
            which=lambda _: None, binding_path=self.db,
        )
        self.assertNotEqual(first["namespace_sha256"], second["namespace_sha256"])

    def test_cmux_socket_namespace_separates_same_public_ids(self):
        first = session.resolve_this_session(
            environ=self.cmux_env(socket="/tmp/cmux-a.sock"),
            runner=FakeCmux("Linear · GEN-37"), which=lambda _: "/opt/cmux",
            binding_path=self.db,
        )
        second = session.resolve_this_session(
            environ=self.cmux_env(socket="/tmp/cmux-b.sock"),
            runner=FakeCmux("Linear · GEN-37"), which=lambda _: "/opt/cmux",
            binding_path=self.db,
        )
        self.assertNotEqual(first["namespace_sha256"], second["namespace_sha256"])

    def test_legacy_bare_surface_rows_are_quarantined_not_reused(self):
        connection = sqlite3.connect(self.db)
        connection.execute(
            "CREATE TABLE bindings(kind TEXT, identity TEXT, workstream_id TEXT)"
        )
        connection.execute(
            "INSERT INTO bindings VALUES ('surface','surface-old','GEN-38')"
        )
        connection.commit()
        connection.close()
        result = session.resolve_this_session(
            environ=self.cmux_env(), runner=FakeCmux("Linear · GEN-37"),
            which=lambda _: "/opt/cmux", binding_path=self.db,
        )
        self.assertEqual(result["workstream_id"], "GEN-37")
        self.assertIsNone(result["prior_binding"])

    def test_workspace_title_is_never_read_or_changed(self):
        fake = FakeCmux("Linear · GEN-37")
        session.resolve_this_session(
            environ=self.cmux_env(), runner=fake, which=lambda _: "/opt/cmux",
            binding_path=self.db,
        )
        flat = [item for call in fake.calls for item in call]
        self.assertNotIn("rename-workspace", flat)
        self.assertNotIn("list-workspaces", flat)

    def test_full_authority_required_before_binding_mutation(self):
        calls = []
        fake = FakeCmux("Linear · GEN-37")
        with self.assertRaisesRegex(session.ThisSessionError,
                                    "workstream_resume_authority_not_full"):
            session.resume_this_session(
                environ=self.cmux_env(),
                runner=full_resume_runner(calls, authority="inspection_only"),
                terminal_runner=fake, which=lambda _: "/opt/cmux",
                binding_path=self.db, resume_script=Path("resume.py"),
            )
        self.assertFalse(self.db.exists())
        self.assertEqual(len(calls), 1)

    def test_title_change_between_resolution_and_resume_refuses_without_mutation(self):
        initial = self.resolution()
        changed = self.resolution(title="Renamed · GEN-37")
        calls = []
        with mock.patch.object(
            session, "resolve_this_session", side_effect=[initial, changed],
        ):
            with self.assertRaisesRegex(session.ThisSessionError,
                                        "session_context_changed"):
                session.resume_this_session(
                    environ=self.cmux_env(), runner=full_resume_runner(calls),
                    binding_path=self.db, resume_script=Path("resume.py"),
                )
        self.assertEqual(calls, [])
        self.assertFalse(self.db.exists())

    def test_binding_revision_or_content_change_before_resume_refuses(self):
        binding = {
            "workstream_id": "GEN-37", "provider": "codex",
            "provider_session_id": "old", "event_id": "wsb_old",
            "updated_at": "2026-09-01T00:00:00Z",
        }
        for field, value in (
            ("event_id", "wsb_new"),
            ("provider_session_id", "other-session"),
        ):
            with self.subTest(field=field):
                initial = self.resolution(binding=binding)
                changed_binding = dict(binding)
                changed_binding[field] = value
                changed = self.resolution(binding=changed_binding)
                calls = []
                with mock.patch.object(
                    session, "resolve_this_session",
                    side_effect=[initial, changed],
                ):
                    with self.assertRaisesRegex(
                        session.ThisSessionError, "session_context_changed",
                    ):
                        session.resume_this_session(
                            environ=self.cmux_env(),
                            runner=full_resume_runner(calls),
                            binding_path=self.db, resume_script=Path("resume.py"),
                        )
                self.assertEqual(calls, [])
                self.assertFalse(self.db.exists())

    def test_cmux_identify_caller_must_match_exact_requested_target(self):
        fake = FakeCmux(
            "Linear · GEN-37", caller_surface="surface-other",
        )
        with self.assertRaisesRegex(session.ThisSessionError,
                                    "session_target_identity_mismatch"):
            session.resolve_this_session(
                environ=self.cmux_env(), runner=fake,
                which=lambda _: "/opt/cmux", binding_path=self.db,
            )
        self.assertFalse(self.db.exists())

    def test_cmux_caller_accepts_exact_pane_uuid_or_ref_and_rejects_other(self):
        for caller_pane in ("pane-uuid", "pane:7"):
            with self.subTest(caller_pane=caller_pane):
                fake = FakeCmux(
                    "Linear · GEN-37", pane="pane-uuid", pane_ref="pane:7",
                    caller_pane=caller_pane,
                )
                result = session.resolve_this_session(
                    environ=self.cmux_env(), runner=fake,
                    which=lambda _: "/opt/cmux", binding_path=self.db,
                )
                self.assertEqual(result["workstream_id"], "GEN-37")

        wrong = FakeCmux(
            "Linear · GEN-37", pane="pane-uuid", pane_ref="pane:7",
            caller_pane="pane:wrong",
        )
        with self.assertRaisesRegex(session.ThisSessionError,
                                    "session_target_identity_mismatch"):
            session.resolve_this_session(
                environ=self.cmux_env(), runner=wrong,
                which=lambda _: "/opt/cmux", binding_path=self.db,
            )

    def test_cmux_ref_form_identity_normalizes_to_canonical_ids(self):
        env = self.cmux_env(surface="surface:132")
        env["CMUX_WORKSPACE_ID"] = "workspace:1"
        fake = FakeCmux(
            "Linear · GEN-37",
            surface="C79BBE38-546F-41A3-B1F9-7C5D66C526F4",
            workspace="5763BFC4-F0AC-4EE6-BDA9-76D3DA25F0AC",
            surface_ref="surface:132", workspace_ref="workspace:1",
            caller_surface="surface:132", caller_workspace="workspace:1",
        )
        result = session.resolve_this_session(
            environ=env, runner=fake, which=lambda _: "/opt/cmux",
            binding_path=self.db,
        )
        self.assertEqual(
            result["target_id"], "C79BBE38-546F-41A3-B1F9-7C5D66C526F4",
        )
        self.assertEqual(
            result["workspace_id"],
            "5763BFC4-F0AC-4EE6-BDA9-76D3DA25F0AC",
        )

    def test_wrong_or_mixed_target_refs_refuse(self):
        fake = FakeCmux(
            "Linear · GEN-37", surface="surface-id", workspace="workspace-id",
            surface_ref="surface:132", workspace_ref="workspace:1",
            caller_surface="surface:132", caller_workspace="workspace:1",
        )
        for workspace, surface in (
            ("workspace:1", "surface:999"),
            ("workspace:999", "surface-id"),
        ):
            with self.subTest(workspace=workspace, surface=surface):
                env = self.cmux_env(surface=surface)
                env["CMUX_WORKSPACE_ID"] = workspace
                with self.assertRaisesRegex(
                    session.ThisSessionError,
                    "session_target_identity_mismatch",
                ):
                    session.resolve_this_session(
                        environ=env, runner=fake, which=lambda _: "/opt/cmux",
                        binding_path=self.db,
                    )

    def test_title_change_during_authenticated_resume_refuses_before_local_mutation(self):
        state = {"value": self.resolution()}
        process_calls = []

        def resolved(**kwargs):
            return json.loads(json.dumps(state["value"]))

        def resume_runner(argv, **kwargs):
            process_calls.append(argv)
            state["value"] = self.resolution(title="Changed · GEN-37")
            return subprocess.CompletedProcess(argv, 0, json.dumps({
                "resume_authority": "full", "project_name": "Linear",
            }), "")

        with mock.patch.object(session, "resolve_this_session", side_effect=resolved):
            with self.assertRaisesRegex(session.ThisSessionError,
                                        "session_context_changed"):
                session.resume_this_session(
                    environ=self.cmux_env(), runner=resume_runner,
                    binding_path=self.db, resume_script=Path("resume.py"),
                )
        self.assertEqual(len(process_calls), 1)
        self.assertFalse(self.db.exists())

    def test_binding_change_during_authenticated_resume_refuses_before_local_mutation(self):
        binding = {
            "workstream_id": "GEN-37", "provider": "codex",
            "provider_session_id": "old", "event_id": "wsb_old",
            "updated_at": "2026-09-01T00:00:00Z",
        }
        state = {"value": self.resolution(binding=binding)}
        process_calls = []

        def resolved(**kwargs):
            return json.loads(json.dumps(state["value"]))

        def resume_runner(argv, **kwargs):
            process_calls.append(argv)
            changed = dict(binding)
            changed["event_id"] = "wsb_changed_during_resume"
            state["value"] = self.resolution(binding=changed)
            return subprocess.CompletedProcess(argv, 0, json.dumps({
                "resume_authority": "full", "project_name": "Linear",
            }), "")

        with mock.patch.object(session, "resolve_this_session", side_effect=resolved):
            with self.assertRaisesRegex(session.ThisSessionError,
                                        "session_context_changed"):
                session.resume_this_session(
                    environ=self.cmux_env(), runner=resume_runner,
                    binding_path=self.db, resume_script=Path("resume.py"),
                )
        self.assertEqual(len(process_calls), 1)
        self.assertFalse(self.db.exists())


if __name__ == "__main__":
    unittest.main()
