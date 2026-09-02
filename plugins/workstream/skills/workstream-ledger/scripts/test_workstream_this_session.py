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
            socket = (
                argv[argv.index("--socket") + 1]
                if "--socket" in argv else kwargs.get("env", {}).get(
                    "CMUX_SOCKET_PATH", "/tmp/cmux-a.sock",
                )
            )
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


def full_resume_runner(
    calls, *, project="Linear Integration", authority="full", options=None,
    root_token=None, requested_focus=None, authenticated_route=None,
):
    def run(argv, **kwargs):
        calls.append(argv)
        if options is not None:
            options.append(kwargs)
        payload = {
            "resume_authority": authority,
            "workstream_id": root_token or argv[-1],
            "project_name": project, "children": [],
        }
        if requested_focus is not None:
            payload["requested_focus"] = requested_focus
        if authenticated_route is not None:
            payload["authenticated_route"] = authenticated_route
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")
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

    @staticmethod
    def owned_child(token="GEN-94", root="GEN-37"):
        return {
            "kind": "owned_child", "identifier": token,
            "issue_id": "child-issue-uuid-94",
            "parent_issue_id": "root-issue-uuid-37",
            "root_identifier": root, "repository_key": "agent-workstream",
            "status": "In Progress",
        }

    def seed(self, fake, token="GEN-37", *, env=None, provider_session="old"):
        env = dict(env or self.cmux_env())
        resolution = session.resolve_this_session(
            environ=env, runner=fake, which=lambda _: "/opt/cmux",
            binding_path=self.db,
        ) if session.workstream_tab.tokens_in_title(fake.title) else {
            **session._terminal_identity(env), "workstream_id": token,
        }
        provenance = {
            "socket_path": env["CMUX_SOCKET_PATH"],
            "bundle_identifier": "com.cmuxterm.app",
            "app_bundle_path": "/Applications/cmux.app",
        }
        resolution["terminal_provenance"] = provenance
        resolution["namespace_sha256"] = session._namespace("cmux", provenance)
        env["CODEX_SESSION_ID"] = provider_session
        return session.record_successor_binding(
            self.db, resolution, environ=env, created_at="2026-09-01T00:00:00Z",
        )

    def two_event_chain(self, name):
        self.db = Path(self.temp.name) / f"chain-{name}.sqlite3"
        surface = f"surface-chain-{name}"
        env = self.cmux_env(surface)
        fake = FakeCmux("Linear", surface=surface)
        first = self.seed(fake, env=env, provider_session="old-session")
        resolution = session.resolve_this_session(
            environ=env, runner=fake, which=lambda _: "/opt/cmux",
            binding_path=self.db,
        )
        second = session.record_successor_binding(
            self.db, resolution,
            environ=dict(env, CODEX_SESSION_ID="new-session"),
            created_at="2026-09-01T01:00:00Z",
        )
        return env, fake, resolution, first, second

    @staticmethod
    def replace_event(connection, event_id, mutate):
        event = list(connection.execute(
            "SELECT manager,namespace_sha256,workspace_id,target_id,"
            "workstream_id,provider,provider_session_id,predecessor_event_id,"
            "predecessor_provider_session_id,created_at "
            "FROM terminal_binding_events_v1 WHERE event_id=?", (event_id,),
        ).fetchone())
        mutate(event)
        replacement_id = session._event_digest(tuple(event))
        connection.execute(
            "INSERT INTO terminal_binding_events_v1 VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (replacement_id, *event),
        )
        connection.execute(
            "UPDATE terminal_bindings_v1 SET current_event_id=?,updated_at=? "
            "WHERE current_event_id=?",
            (replacement_id, event[9], event_id),
        )
        connection.execute(
            "DELETE FROM terminal_binding_events_v1 WHERE event_id=?",
            (event_id,),
        )
        return replacement_id

    def assert_chain_refusal(self, env, fake, expected):
        fake.title = "Conflicting title · GEN-38"
        resume_calls = []
        before_renames = len([
            call for call in fake.calls if "rename-tab" in call
        ])
        connection = sqlite3.connect(self.db)
        before = tuple(connection.iterdump())
        connection.close()
        with self.assertRaisesRegex(session.ThisSessionError, expected):
            session.resume_this_session(
                environ=env, runner=full_resume_runner(resume_calls),
                terminal_runner=fake, which=lambda _: "/opt/cmux",
                binding_path=self.db, resume_script=Path("resume.py"),
                created_at="2026-09-02T00:00:00Z",
            )
        self.assertEqual(resume_calls, [])
        self.assertEqual(fake.title, "Conflicting title · GEN-38")
        self.assertEqual(len([
            call for call in fake.calls if "rename-tab" in call
        ]), before_renames)
        connection = sqlite3.connect(self.db)
        after = tuple(connection.iterdump())
        connection.close()
        self.assertEqual(after, before)

    @staticmethod
    def resolution(*, title="Linear · GEN-37", binding=None):
        provenance = {
            "socket_path": "/tmp/cmux-a.sock",
            "bundle_identifier": "com.cmuxterm.app",
            "app_bundle_path": "/Applications/cmux.app",
        }
        return {
            "manager": "cmux",
            "namespace_sha256": session._namespace("cmux", provenance),
            "workspace_id": "workspace-1", "target_id": "surface-old",
            "cmux_socket_path": "/tmp/cmux-a.sock",
            "terminal_provenance": provenance,
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

    def test_bound_generic_human_title_is_preserved_and_suffixed(self):
        env = self.cmux_env("surface-pulp")
        fake = FakeCmux("pulp", surface="surface-pulp")
        self.seed(fake, env=env)
        result = session.resume_this_session(
            environ=env, runner=full_resume_runner([]), terminal_runner=fake,
            which=lambda _: "/opt/cmux", binding_path=self.db,
            resume_script=Path("resume.py"),
            created_at="2026-09-01T01:00:00Z",
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(result["tab_binding"]["title"], "pulp · GEN-37")
        self.assertEqual(result["resume_binding"]["writes_performed"], 1)

    def test_unknown_title_provenance_is_preserved_not_guessed_automatic(self):
        env = self.cmux_env("surface-unknown-title-source")
        fake = FakeCmux("~/Code/pulp", surface="surface-unknown-title-source")
        self.seed(fake, env=env)
        result = session.resume_this_session(
            environ=env, runner=full_resume_runner([]), terminal_runner=fake,
            which=lambda _: "/opt/cmux", binding_path=self.db,
            resume_script=Path("resume.py"),
            created_at="2026-09-01T01:00:00Z",
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(fake.title, "~/Code/pulp · GEN-37")
        self.assertEqual(result["tab_binding"]["status"], "updated")

    def test_generic_title_without_exact_binding_refuses_before_resume(self):
        for title in ("Linear", "pulp"):
            with self.subTest(title=title):
                calls = []
                with self.assertRaisesRegex(
                    session.ThisSessionError, "session_workstream_unresolved",
                ):
                    session.resume_this_session(
                        environ=self.cmux_env(),
                        runner=full_resume_runner(calls),
                        terminal_runner=FakeCmux(title),
                        which=lambda _: "/opt/cmux", binding_path=self.db,
                        resume_script=Path("resume.py"),
                    )
                self.assertEqual(calls, [])
                self.assertFalse(self.db.exists())

    def test_arbitrary_issue_like_prose_is_not_a_session_handle(self):
        for title in ("Investigate GEN-37 before launch", "GEN-37"):
            with self.subTest(title=title):
                with self.assertRaisesRegex(
                    session.ThisSessionError,
                    "session_title_workstream_noncanonical",
                ):
                    session.resolve_this_session(
                        environ=self.cmux_env(), runner=FakeCmux(title),
                        which=lambda _: "/opt/cmux", binding_path=self.db,
                    )

    def test_strict_suffix_can_bootstrap_on_another_machine_namespace(self):
        env = self.cmux_env(
            "surface-m5", socket="/tmp/m5-cmux.sock", session_id="m5-new",
        )
        result = session.resume_this_session(
            environ=env, runner=full_resume_runner([]),
            terminal_runner=FakeCmux("Spectr · GEN-37", surface="surface-m5"),
            which=lambda _: "/opt/cmux", binding_path=self.db,
            resume_script=Path("resume.py"),
            created_at="2026-09-01T01:00:00Z",
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(
            result["this_session_resolution"]["candidate_source"], "title",
        )
        self.assertEqual(result["resume_binding"]["writes_performed"], 1)

    def test_full_resume_root_must_match_candidate_before_title_or_binding(self):
        env = self.cmux_env("surface-root-mismatch")
        fake = FakeCmux(
            "Linear · GEN-37", surface="surface-root-mismatch",
        )
        calls = []
        with self.assertRaisesRegex(
            session.ThisSessionError, "workstream_resume_identity_mismatch",
        ):
            session.resume_this_session(
                environ=env,
                runner=full_resume_runner(calls, root_token="GEN-38"),
                terminal_runner=fake, which=lambda _: "/opt/cmux",
                binding_path=self.db, resume_script=Path("resume.py"),
            )
        self.assertEqual(len(calls), 1)
        self.assertEqual(fake.title, "Linear · GEN-37")
        self.assertNotIn("rename-tab", [
            call[call.index("rename-tab")] for call in fake.calls
            if "rename-tab" in call
        ])
        self.assertFalse(self.db.exists())

    def test_direct_root_and_owned_child_full_contexts_bind_exact_candidate(self):
        cases = (
            ("GEN-37", "GEN-37", None, None),
            (
                "GEN-94", "GEN-37", self.owned_child(),
                # Fixed-frontier output may truncate display-only route fields;
                # the exact, schema-validated requested focus remains intact.
                {"root_issue_id": "root-issue-uuid-37…[truncated]"},
            ),
        )
        for candidate, root, focus, route in cases:
            with self.subTest(candidate=candidate):
                db = Path(self.temp.name) / f"{candidate}.sqlite3"
                surface = f"surface-{candidate}"
                fake = FakeCmux(
                    f"Linear · {candidate}", surface=surface,
                )
                result = session.resume_this_session(
                    environ=self.cmux_env(surface),
                    runner=full_resume_runner(
                        [], root_token=root, requested_focus=focus,
                        authenticated_route=route,
                    ),
                    terminal_runner=fake, which=lambda _: "/opt/cmux",
                    binding_path=db, resume_script=Path("resume.py"),
                    created_at="2026-09-01T01:00:00Z",
                )
                self.assertEqual(result["resume_authority"], "full")
                self.assertEqual(
                    result["this_session_resolution"]["workstream_id"],
                    candidate,
                )
                self.assertEqual(result["resume_binding"]["writes_performed"], 1)

    def test_forged_or_malformed_owned_child_focus_refuses_without_mutation(self):
        candidate = "GEN-94"
        invalid = []
        wrong_root = self.owned_child(root="GEN-38")
        invalid.append(("GEN-37", wrong_root))
        extra = dict(self.owned_child(), injected="forged")
        invalid.append(("GEN-37", extra))
        blank = dict(self.owned_child(), repository_key=" ")
        invalid.append(("GEN-37", blank))
        invalid.append(("GEN-38", None))
        for index, (root, focus) in enumerate(invalid):
            with self.subTest(index=index):
                db = Path(self.temp.name) / f"invalid-{index}.sqlite3"
                surface = f"surface-invalid-{index}"
                fake = FakeCmux(
                    "Linear · GEN-94", surface=surface,
                )
                with self.assertRaisesRegex(
                    session.ThisSessionError,
                    "workstream_resume_identity_mismatch",
                ):
                    session.resume_this_session(
                        environ=self.cmux_env(surface),
                        runner=full_resume_runner(
                            [], root_token=root, requested_focus=focus,
                        ),
                        terminal_runner=fake, which=lambda _: "/opt/cmux",
                        binding_path=db, resume_script=Path("resume.py"),
                    )
                self.assertFalse(db.exists())
                self.assertNotIn("rename-tab", [
                    part for call in fake.calls for part in call
                ])

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

    def test_cmux_ancestor_uuids_authenticate_against_ref_only_caller(self):
        workspace_id = "5763BFC4-F0AC-4EE6-BDA9-76D3DA25F0AC"
        surface_id = "C79BBE38-546F-41A3-B1F9-7C5D66C526F4"
        fake = FakeCmux(
            "Linear · GEN-37", surface=surface_id, surface_ref="surface:2",
            workspace=workspace_id, workspace_ref="workspace:1",
            pane="9058DC8D-9EA7-426D-94C8-7D649EC97476",
            pane_ref="pane:1",
        )

        def runner(argv, **kwargs):
            if "agent.resolve_delivery_target" in argv:
                return subprocess.CompletedProcess(argv, 0, json.dumps({
                    "surface_id": surface_id, "workspace_id": workspace_id,
                }), "")
            result = fake(argv, **kwargs)
            if "identify" in argv and "--workspace" in argv:
                value = json.loads(result.stdout)
                # Current cmux 0.501 production output authenticates the
                # caller with refs even when the exact selector was a UUID.
                value["caller"] = {
                    "surface_ref": "surface:2", "workspace_ref": "workspace:1",
                    "pane_ref": "pane:1", "window_ref": "window:1",
                }
                result.stdout = json.dumps(value)
            return result

        result = session.resolve_this_session(
            environ={}, runner=runner, which=lambda _: "/opt/cmux",
            binding_path=self.db, pid_chain=[10], socket_candidates=[],
        )
        self.assertEqual(result["workstream_id"], "GEN-37")
        self.assertEqual(result["workspace_id"], workspace_id)
        self.assertEqual(result["target_id"], surface_id)

    def test_no_env_full_wrapper_normalizes_uuid_ref_title_and_binding(self):
        workspace_id = "5763BFC4-F0AC-4EE6-BDA9-76D3DA25F0AC"
        surface_id = "C79BBE38-546F-41A3-B1F9-7C5D66C526F4"
        provenance = {
            "socket_path": "/tmp/cmux-a.sock",
            "bundle_identifier": "com.cmuxterm.app",
            "app_bundle_path": "/Applications/cmux.app",
        }
        for name, title, seeded, expected_tab in (
            ("suffix", "Linear · GEN-37", False, "unchanged"),
            ("binding", "Linear", True, "updated"),
        ):
            with self.subTest(name=name):
                db = Path(self.temp.name) / f"uuid-ref-{name}.sqlite3"
                fake = FakeCmux(
                    title, surface=surface_id, surface_ref="surface:2",
                    workspace=workspace_id, workspace_ref="workspace:1",
                    pane="9058DC8D-9EA7-426D-94C8-7D649EC97476",
                    pane_ref="pane:1",
                )
                if seeded:
                    resolution = {
                        "manager": "cmux", "workspace_id": workspace_id,
                        "target_id": surface_id,
                        "cmux_socket_path": provenance["socket_path"],
                        "terminal_provenance": dict(provenance),
                        "namespace_sha256": session._namespace(
                            "cmux", provenance,
                        ),
                        "workstream_id": "GEN-37",
                    }
                    session.record_successor_binding(
                        db, resolution,
                        environ={"CODEX_SESSION_ID": "old-session"},
                        created_at="2026-09-01T00:00:00Z",
                    )

                def terminal_runner(argv, **kwargs):
                    if "agent.resolve_delivery_target" in argv:
                        return subprocess.CompletedProcess(
                            argv, 0, json.dumps({
                                "surface_id": surface_id,
                                "workspace_id": workspace_id,
                            }), "",
                        )
                    result = fake(argv, **kwargs)
                    if "identify" in argv and "--no-caller" not in argv:
                        value = json.loads(result.stdout)
                        # cmux 0.501 authenticates this exact UUID selector but
                        # reports only its caller refs in production.
                        value["caller"] = {
                            "surface_ref": "surface:2",
                            "workspace_ref": "workspace:1",
                            "pane_ref": "pane:1", "window_ref": "window:1",
                        }
                        result.stdout = json.dumps(value)
                    return result

                with (
                    mock.patch.object(
                        session, "_bounded_ancestor_pids", return_value=[101],
                    ),
                    mock.patch.object(
                        session, "_default_socket_candidates", return_value=[],
                    ),
                ):
                    result = session.resume_this_session(
                        environ={"CODEX_SESSION_ID": "new-session"},
                        runner=full_resume_runner([]),
                        terminal_runner=terminal_runner,
                        which=lambda _: "/opt/cmux", binding_path=db,
                        resume_script=Path("resume.py"),
                        created_at="2026-09-01T01:00:00Z",
                    )
                self.assertEqual(result["resume_authority"], "full")
                self.assertEqual(result["tab_binding"]["status"], expected_tab)
                self.assertEqual(fake.title, "Linear · GEN-37")
                self.assertEqual(result["resume_binding"]["writes_performed"], 1)
                self.assertEqual(
                    result["this_session_resolution"]["workspace_id"],
                    workspace_id,
                )
                self.assertEqual(
                    result["this_session_resolution"]["target_id"],
                    surface_id,
                )
                connection = sqlite3.connect(db)
                self.assertEqual(connection.execute(
                    "SELECT count(*) FROM terminal_binding_events_v1"
                ).fetchone()[0], 2 if seeded else 1)
                self.assertEqual(connection.execute(
                    "SELECT provider_session_id FROM terminal_bindings_v1"
                ).fetchone()[0], "new-session")
                connection.close()

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

    def test_initial_binding_without_provider_session_is_anonymous_but_usable(self):
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
        self.assertEqual(result["resume_binding"]["writes_performed"], 1)

    def test_missing_current_session_never_erases_known_provider_binding(self):
        env = self.cmux_env("surface-no-provider-session")
        del env["CODEX_SESSION_ID"]
        fake = FakeCmux("Linear", surface="surface-no-provider-session")
        seeded = self.seed(fake, env=env, provider_session="01a01d46-known")
        result = session.resume_this_session(
            environ=env, runner=full_resume_runner([]), terminal_runner=fake,
            which=lambda _: "/opt/cmux", binding_path=self.db,
            resume_script=Path("resume.py"),
            created_at="2026-09-02T02:00:00Z",
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(result["tab_binding"]["status"], "updated")
        self.assertEqual(fake.title, "Linear · GEN-37")
        self.assertEqual(result["resume_binding"], {
            "status": "unavailable",
            "reason": "provider_session_identity_unavailable",
            "event_id": seeded["event_id"], "provider": None,
            "provider_session_id": None, "preserved_provider": "codex",
            "preserved_provider_session_id": "01a01d46-known",
            "predecessor_provider_session_id": "01a01d46-known",
            "writes_performed": 0,
        })
        connection = sqlite3.connect(self.db)
        self.assertEqual(connection.execute(
            "SELECT count(*) FROM terminal_binding_events_v1"
        ).fetchone()[0], 1)
        self.assertEqual(connection.execute(
            "SELECT provider,provider_session_id,current_event_id "
            "FROM terminal_bindings_v1"
        ).fetchone(), ("codex", "01a01d46-known", seeded["event_id"]))
        connection.close()

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
        self.assertEqual(result["resume_binding"], {
            "status": "unavailable", "reason": "terminal_title_unverified",
        })
        self.assertFalse(self.db.exists())

    def test_exact_binding_resumes_when_cmux_title_adapter_is_unavailable(self):
        env = self.cmux_env("surface-bound-unavailable")
        fake = FakeCmux("Linear", surface="surface-bound-unavailable")
        self.seed(fake, env=env)
        workspace_reads = 0

        def terminal_runner(argv, **kwargs):
            nonlocal workspace_reads
            if "rpc" in argv and argv[argv.index("rpc") + 1] == "workspace.list":
                workspace_reads += 1
                # Identity normalization succeeds; only the later optional
                # title probe is unavailable on each resolution pass.
                if workspace_reads % 2 == 0:
                    return subprocess.CompletedProcess(
                        argv, 1, "", "unavailable",
                    )
            if "list-pane-surfaces" in argv:
                return subprocess.CompletedProcess(argv, 1, "", "unavailable")
            return fake(argv, **kwargs)

        calls = []
        result = session.resume_this_session(
            environ=env, runner=full_resume_runner(calls),
            terminal_runner=terminal_runner, which=lambda _: "/opt/cmux",
            binding_path=self.db, resume_script=Path("resume.py"),
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            result["this_session_resolution"]["candidate_source"], "binding",
        )
        self.assertEqual(result["tab_binding"]["status"], "unavailable")
        self.assertEqual(result["resume_binding"]["status"], "unavailable")
        self.assertEqual(fake.title, "Linear")
        connection = sqlite3.connect(self.db)
        self.assertEqual(connection.execute(
            "SELECT count(*) FROM terminal_binding_events_v1"
        ).fetchone()[0], 1)
        self.assertEqual(connection.execute(
            "SELECT provider_session_id FROM terminal_bindings_v1"
        ).fetchone()[0], "old")
        connection.close()

    def test_fixed_envelope_project_name_labels_unnamed_bound_tab_and_binds(self):
        env = self.cmux_env("surface-fixed-project")
        fake = FakeCmux("", surface="surface-fixed-project")
        self.seed(fake, env=env)

        def fixed_resume(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 0, json.dumps({
                "context_schema": {
                    "envelope": "fixed_frontier_authority_v1",
                },
                "resume_authority": "full", "workstream_id": "GEN-37",
                "project_name": "Linear Integration",
            }), "")

        result = session.resume_this_session(
            environ=env, runner=fixed_resume, terminal_runner=fake,
            which=lambda _: "/opt/cmux", binding_path=self.db,
            resume_script=Path("resume.py"),
            created_at="2026-09-01T01:00:00Z",
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(fake.title, "Linear Integration · GEN-37")
        self.assertEqual(result["tab_binding"]["status"], "updated")
        self.assertEqual(result["resume_binding"]["writes_performed"], 1)

    def test_malformed_project_name_is_optional_after_full_authority(self):
        env = self.cmux_env("surface-malformed-project")
        fake = FakeCmux("", surface="surface-malformed-project")
        self.seed(fake, env=env)

        def malformed_resume(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 0, json.dumps({
                "context_schema": {
                    "envelope": "fixed_frontier_authority_v1",
                },
                "resume_authority": "full", "workstream_id": "GEN-37",
                "project_name": {"forged": "object"},
            }), "")

        result = session.resume_this_session(
            environ=env, runner=malformed_resume, terminal_runner=fake,
            which=lambda _: "/opt/cmux", binding_path=self.db,
            resume_script=Path("resume.py"),
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(result["tab_binding"]["reason"], "invalid_project_name")
        self.assertEqual(result["resume_binding"]["status"], "unavailable")
        self.assertEqual(fake.title, "")
        connection = sqlite3.connect(self.db)
        self.assertEqual(connection.execute(
            "SELECT count(*) FROM terminal_binding_events_v1"
        ).fetchone()[0], 1)
        connection.close()

    def test_unavailable_cmux_title_without_binding_refuses_before_resume(self):
        env = self.cmux_env("surface-unbound-unavailable")
        fake = FakeCmux("Linear", surface="surface-unbound-unavailable")
        workspace_reads = 0

        def terminal_runner(argv, **kwargs):
            nonlocal workspace_reads
            if "rpc" in argv and argv[argv.index("rpc") + 1] == "workspace.list":
                workspace_reads += 1
                if workspace_reads % 2 == 0:
                    return subprocess.CompletedProcess(
                        argv, 1, "", "unavailable",
                    )
            return fake(argv, **kwargs)

        calls = []
        with self.assertRaisesRegex(
            session.ThisSessionError,
            "session_workstream_unresolved:terminal_adapter_unavailable:cmux",
        ):
            session.resume_this_session(
                environ=env, runner=full_resume_runner(calls),
                terminal_runner=terminal_runner, which=lambda _: "/opt/cmux",
                binding_path=self.db, resume_script=Path("resume.py"),
            )
        self.assertEqual(calls, [])
        self.assertFalse(self.db.exists())

    def test_exact_herdr_binding_resumes_when_cli_is_unavailable(self):
        env = self.herdr_env()
        resolution = {
            **session._terminal_identity(env), "workstream_id": "GEN-37",
        }
        old_env = dict(env, CODEX_SESSION_ID="old")
        session.record_successor_binding(
            self.db, resolution, environ=old_env,
            created_at="2026-09-01T00:00:00Z",
        )

        def unavailable(argv, **kwargs):
            return subprocess.CompletedProcess(argv, 1, "", "unavailable")

        result = session.resume_this_session(
            environ=env, runner=full_resume_runner([]),
            terminal_runner=unavailable, which=lambda _: None,
            binding_path=self.db, resume_script=Path("resume.py"),
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(
            result["this_session_resolution"]["candidate_source"], "binding",
        )
        self.assertEqual(result["tab_binding"]["status"], "unavailable")
        self.assertEqual(result["resume_binding"]["status"], "unavailable")

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

    def test_herdr_provenance_requires_explicit_environment_flag(self):
        for flag in (None, "0"):
            with self.subTest(flag=flag):
                env = self.herdr_env("/tmp/herdr-m5.sock")
                if flag is None:
                    del env["HERDR_ENV"]
                else:
                    env["HERDR_ENV"] = flag
                fake = FakeHerdr("Linear · GEN-37")
                resume_calls = []
                with self.assertRaisesRegex(
                    session.ThisSessionError,
                    "session_context_invalid:HERDR_ENV",
                ):
                    session.resume_this_session(
                        environ=env, runner=full_resume_runner(resume_calls),
                        terminal_runner=fake, which=lambda _: None,
                        binding_path=self.db, resume_script=Path("resume.py"),
                    )
                self.assertEqual(resume_calls, [])
                self.assertEqual(fake.calls, [])
                self.assertFalse(self.db.exists())

    def test_unflagged_herdr_tuple_never_falls_through_to_cmux_ancestor(self):
        env = self.herdr_env("/tmp/herdr-stale.sock")
        del env["HERDR_ENV"]
        fake = FakeCmux(
            "Linear · GEN-37", surface="surface-live-cmux",
            workspace="workspace-live-cmux",
        )
        with self.assertRaisesRegex(
            session.ThisSessionError, "session_context_invalid:HERDR_ENV",
        ):
            session.resolve_this_session(
                environ=env, runner=fake, which=lambda _: "/opt/cmux",
                binding_path=self.db, pid_chain=[101, 100],
                socket_candidates=[Path("/tmp/cmux-live.sock")],
            )
        self.assertEqual(fake.calls, [])
        self.assertFalse(self.db.exists())

    def test_flagged_partial_herdr_context_refuses_before_any_adapter_call(self):
        env = self.herdr_env()
        del env["HERDR_SOCKET_PATH"]
        fake = FakeHerdr()
        with self.assertRaisesRegex(
            session.ThisSessionError,
            "session_context_invalid:HERDR_SOCKET_PATH",
        ):
            session.resolve_this_session(
                environ=env, runner=fake, which=lambda _: "/opt/cmux",
                binding_path=self.db, pid_chain=[101],
            )
        self.assertEqual(fake.calls, [])
        self.assertFalse(self.db.exists())

    def test_mixed_cmux_and_herdr_provenance_refuses(self):
        env = dict(self.herdr_env(), CMUX_SURFACE_ID="surface-old")
        with self.assertRaisesRegex(
            session.ThisSessionError, "session_context_ambiguous",
        ):
            session.resolve_this_session(
                environ=env, runner=FakeHerdr(), which=lambda _: None,
                binding_path=self.db,
            )

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

    def test_cmux_socket_must_be_canonical_absolute_before_any_probe(self):
        for socket in ("relative/cmux.sock", "/tmp/dir/../cmux.sock"):
            with self.subTest(socket=socket):
                calls = []

                def forbidden(*args, **kwargs):
                    calls.append(args)
                    raise AssertionError("relative socket must refuse first")

                with self.assertRaisesRegex(
                    session.ThisSessionError,
                    "session_context_invalid:CMUX_SOCKET_PATH",
                ):
                    session.resolve_this_session(
                        environ=self.cmux_env(socket=socket), runner=forbidden,
                        which=lambda _: "/opt/cmux", binding_path=self.db,
                    )
                self.assertEqual(calls, [])

    def test_cmux_live_socket_readback_must_exactly_match_selector(self):
        env = self.cmux_env(socket="/tmp/cmux-expected.sock")
        fake = FakeCmux("Linear · GEN-37")

        def mismatched(argv, **kwargs):
            result = fake(argv, **kwargs)
            if "identify" in argv:
                value = json.loads(result.stdout)
                value["socket_path"] = "/tmp/cmux-other.sock"
                result.stdout = json.dumps(value)
            return result

        with self.assertRaisesRegex(
            session.ThisSessionError, "session_context_mismatch:cmux_socket",
        ):
            session.resolve_this_session(
                environ=env, runner=mismatched, which=lambda _: "/opt/cmux",
                binding_path=self.db,
            )
        self.assertFalse(self.db.exists())

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

    def test_missing_or_corrupt_current_binding_event_refuses_before_title_fallback(self):
        mutations = {
            "missing": (
                "DELETE FROM terminal_binding_events_v1", (),
                "session_binding_history_incomplete",
            ),
            "forged_digest": (
                "UPDATE terminal_binding_events_v1 SET provider_session_id=?",
                ("forged-session",), "session_binding_event_digest_mismatch",
            ),
            "route_drift": (
                "UPDATE terminal_binding_events_v1 SET target_id=?",
                ("surface-other",), "session_binding_history_mismatch",
            ),
        }
        for name, (statement, arguments, expected) in mutations.items():
            with self.subTest(name=name):
                self.db = Path(self.temp.name) / f"corrupt-{name}.sqlite3"
                env = self.cmux_env("surface-corrupt")
                fake = FakeCmux("Linear", surface="surface-corrupt")
                self.seed(fake, env=env)
                connection = sqlite3.connect(self.db)
                connection.execute(statement, arguments)
                if name == "forged_digest":
                    connection.execute(
                        "UPDATE terminal_bindings_v1 "
                        "SET provider_session_id=?", arguments,
                    )
                connection.commit()
                before = connection.iterdump()
                before = tuple(before)
                connection.close()
                fake.title = "Conflicting title · GEN-38"
                resume_calls = []
                with self.assertRaisesRegex(session.ThisSessionError, expected):
                    session.resume_this_session(
                        environ=env,
                        runner=full_resume_runner(resume_calls),
                        terminal_runner=fake, which=lambda _: "/opt/cmux",
                        binding_path=self.db, resume_script=Path("resume.py"),
                    )
                self.assertEqual(resume_calls, [])
                self.assertNotIn("rename-tab", [
                    part for call in fake.calls for part in call
                ])
                connection = sqlite3.connect(self.db)
                after = tuple(connection.iterdump())
                connection.close()
                self.assertEqual(after, before)

    def test_duplicate_exact_binding_rows_refuse_instead_of_fetchone_selection(self):
        env = self.cmux_env("surface-duplicate-binding")
        fake = FakeCmux(
            "Linear · GEN-37", surface="surface-duplicate-binding",
        )
        resolution = session.resolve_this_session(
            environ=env, runner=fake, which=lambda _: "/opt/cmux",
            binding_path=self.db,
        )
        connection = sqlite3.connect(self.db)
        connection.execute(
            "CREATE TABLE terminal_bindings_v1 ("
            "manager TEXT, namespace_sha256 TEXT, workspace_id TEXT, "
            "target_id TEXT, workstream_id TEXT, provider TEXT, "
            "provider_session_id TEXT, current_event_id TEXT, updated_at TEXT)"
        )
        row = (
            resolution["manager"], resolution["namespace_sha256"],
            resolution["workspace_id"], resolution["target_id"], "GEN-37",
            "codex", "old", "wsb_duplicate", "2026-09-01T00:00:00Z",
        )
        connection.executemany(
            "INSERT INTO terminal_bindings_v1 VALUES (?,?,?,?,?,?,?,?,?)",
            (row, row),
        )
        connection.commit()
        before = tuple(connection.iterdump())
        connection.close()
        calls = []
        with self.assertRaisesRegex(
            session.ThisSessionError, "session_binding_ambiguous",
        ):
            session.resume_this_session(
                environ=env, runner=full_resume_runner(calls),
                terminal_runner=fake, which=lambda _: "/opt/cmux",
                binding_path=self.db, resume_script=Path("resume.py"),
            )
        self.assertEqual(calls, [])
        connection = sqlite3.connect(self.db)
        after = tuple(connection.iterdump())
        connection.close()
        self.assertEqual(after, before)

    def test_record_successor_refuses_corrupt_prior_history_without_writes(self):
        env = self.cmux_env("surface-corrupt-record")
        fake = FakeCmux("Linear", surface="surface-corrupt-record")
        self.seed(fake, env=env)
        resolution = session.resolve_this_session(
            environ=env, runner=fake, which=lambda _: "/opt/cmux",
            binding_path=self.db,
        )
        connection = sqlite3.connect(self.db)
        connection.execute("DELETE FROM terminal_binding_events_v1")
        connection.commit()
        before = tuple(connection.iterdump())
        connection.close()
        with self.assertRaisesRegex(
            session.ThisSessionError, "session_binding_history_incomplete",
        ):
            session.record_successor_binding(
                self.db, resolution,
                environ=dict(env, CODEX_SESSION_ID="successor"),
                created_at="2026-09-01T03:00:00Z",
            )
        connection = sqlite3.connect(self.db)
        after = tuple(connection.iterdump())
        connection.close()
        self.assertEqual(after, before)

    def test_binding_chain_refuses_missing_predecessor_without_fallback_or_writes(self):
        env, fake, _, _, second = self.two_event_chain("missing-predecessor")
        connection = sqlite3.connect(self.db)
        self.replace_event(
            connection, second["event_id"],
            lambda event: event.__setitem__(7, "wsb_00000000000000000000000000000000"),
        )
        connection.commit()
        connection.close()
        self.assert_chain_refusal(
            env, fake, "session_binding_history_incomplete",
        )

    def test_binding_chain_refuses_cycle_without_fallback_or_writes(self):
        env, fake, _, _, second = self.two_event_chain("cycle")
        connection = sqlite3.connect(self.db)
        connection.execute(
            "UPDATE terminal_binding_events_v1 SET predecessor_event_id=?,"
            "predecessor_provider_session_id=? WHERE event_id=?",
            (second["event_id"], "new-session", second["event_id"]),
        )
        connection.commit()
        connection.close()
        self.assert_chain_refusal(env, fake, "session_binding_history_cycle")

    def test_binding_chain_refuses_ambiguous_predecessor_without_selection(self):
        env, fake, _, first, _ = self.two_event_chain("ambiguous")
        connection = sqlite3.connect(self.db)
        connection.executescript("""
            ALTER TABLE terminal_binding_events_v1 RENAME TO original_events;
            CREATE TABLE terminal_binding_events_v1 AS
                SELECT * FROM original_events;
        """)
        connection.execute(
            "INSERT INTO terminal_binding_events_v1 "
            "SELECT * FROM original_events WHERE event_id=?",
            (first["event_id"],),
        )
        connection.execute("DROP TABLE original_events")
        connection.commit()
        connection.close()
        self.assert_chain_refusal(
            env, fake, "session_binding_history_ambiguous",
        )

    def test_binding_chain_refuses_cross_key_predecessor_with_valid_digests(self):
        env, fake, _, first, second = self.two_event_chain("cross-key")
        connection = sqlite3.connect(self.db)
        foreign_first_id = self.replace_event(
            connection, first["event_id"],
            lambda event: event.__setitem__(3, "surface-foreign"),
        )
        self.replace_event(
            connection, second["event_id"],
            lambda event: event.__setitem__(7, foreign_first_id),
        )
        connection.commit()
        connection.close()
        self.assert_chain_refusal(
            env, fake, "session_binding_history_mismatch",
        )

    def test_binding_chain_refuses_hash_invalid_predecessor_without_writes(self):
        env, fake, _, first, _ = self.two_event_chain("hash-invalid")
        connection = sqlite3.connect(self.db)
        connection.execute(
            "UPDATE terminal_binding_events_v1 SET created_at=? WHERE event_id=?",
            ("2026-09-01T00:00:01Z", first["event_id"]),
        )
        connection.commit()
        connection.close()
        self.assert_chain_refusal(
            env, fake, "session_binding_event_digest_mismatch",
        )

    def test_binding_chain_refuses_predecessor_session_drift_with_valid_digest(self):
        env, fake, _, _, second = self.two_event_chain("session-drift")
        connection = sqlite3.connect(self.db)
        self.replace_event(
            connection, second["event_id"],
            lambda event: event.__setitem__(8, "different-old-session"),
        )
        connection.commit()
        connection.close()
        self.assert_chain_refusal(
            env, fake, "session_binding_predecessor_session_mismatch",
        )

    def test_binding_chain_refuses_persisted_provider_session_downgrade(self):
        env, fake, _, _, second = self.two_event_chain("provider-downgrade")
        connection = sqlite3.connect(self.db)

        def erase_provider(event):
            event[5] = None
            event[6] = None

        self.replace_event(connection, second["event_id"], erase_provider)
        connection.execute(
            "UPDATE terminal_bindings_v1 SET provider=NULL,"
            "provider_session_id=NULL"
        )
        connection.commit()
        connection.close()
        self.assert_chain_refusal(
            env, fake, "session_binding_history_invalid",
        )

    def test_binding_chain_refuses_reverse_chronology_with_valid_digests(self):
        env, fake, _, first, second = self.two_event_chain("chronology")
        connection = sqlite3.connect(self.db)
        later_first_id = self.replace_event(
            connection, first["event_id"],
            lambda event: event.__setitem__(9, "2026-09-01T02:00:00Z"),
        )
        self.replace_event(
            connection, second["event_id"],
            lambda event: event.__setitem__(7, later_first_id),
        )
        connection.commit()
        connection.close()
        self.assert_chain_refusal(
            env, fake, "session_binding_history_chronology_mismatch",
        )

    def test_record_successor_refuses_time_before_current_tip_without_writes(self):
        env, _, resolution, _, _ = self.two_event_chain("record-chronology")
        connection = sqlite3.connect(self.db)
        before = tuple(connection.iterdump())
        connection.close()
        with self.assertRaisesRegex(
            session.ThisSessionError,
            "session_binding_history_chronology_mismatch",
        ):
            session.record_successor_binding(
                self.db, resolution,
                environ=dict(env, CODEX_SESSION_ID="third-session"),
                created_at="2026-09-01T00:59:59Z",
            )
        connection = sqlite3.connect(self.db)
        after = tuple(connection.iterdump())
        connection.close()
        self.assertEqual(after, before)

    def test_binding_chain_validation_is_bounded_and_refuses_over_budget(self):
        env, fake, _, _, _ = self.two_event_chain("over-budget")
        with mock.patch.object(session, "MAX_BINDING_CHAIN_EVENTS", 1):
            self.assert_chain_refusal(
                env, fake, "session_binding_history_over_budget",
            )

    def test_binding_namespace_must_match_full_terminal_provenance(self):
        env = self.cmux_env("surface-provenance")
        resolution = session.resolve_this_session(
            environ=env,
            runner=FakeCmux(
                "Linear · GEN-37", surface="surface-provenance",
            ),
            which=lambda _: "/opt/cmux", binding_path=self.db,
        )
        forged = json.loads(json.dumps(resolution))
        forged["terminal_provenance"]["bundle_identifier"] = "com.other.cmux"
        with self.assertRaisesRegex(
            session.ThisSessionError, "session_binding_provenance_mismatch",
        ):
            session.record_successor_binding(
                self.db, forged, environ=env,
                created_at="2026-09-01T00:00:00Z",
            )
        self.assertFalse(self.db.exists())

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

    def test_title_change_during_authenticated_resume_preserves_full_authority_without_binding(self):
        state = {"value": self.resolution()}
        process_calls = []

        def resolved(**kwargs):
            return json.loads(json.dumps(state["value"]))

        def resume_runner(argv, **kwargs):
            process_calls.append(argv)
            state["value"] = self.resolution(title="Changed · GEN-37")
            return subprocess.CompletedProcess(argv, 0, json.dumps({
                "resume_authority": "full", "workstream_id": "GEN-37",
                "project_name": "Linear",
            }), "")

        with mock.patch.object(session, "resolve_this_session", side_effect=resolved):
            result = session.resume_this_session(
                environ=self.cmux_env(), runner=resume_runner,
                binding_path=self.db, resume_script=Path("resume.py"),
            )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(
            result["resume_binding"]["reason"], "session_context_changed",
        )
        self.assertEqual(result["tab_binding"]["status"], "unavailable")
        self.assertEqual(len(process_calls), 1)
        self.assertFalse(self.db.exists())

    def test_binding_change_during_authenticated_resume_preserves_full_authority_without_mutation(self):
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
                "resume_authority": "full", "workstream_id": "GEN-37",
                "project_name": "Linear",
            }), "")

        with mock.patch.object(session, "resolve_this_session", side_effect=resolved):
            result = session.resume_this_session(
                environ=self.cmux_env(), runner=resume_runner,
                binding_path=self.db, resume_script=Path("resume.py"),
            )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(
            result["resume_binding"]["reason"], "session_context_changed",
        )
        self.assertEqual(len(process_calls), 1)
        self.assertFalse(self.db.exists())

    def test_concurrent_title_change_rolls_back_atomic_new_binding(self):
        env = self.cmux_env("surface-race")
        fake = FakeCmux("Spectr · GEN-37", surface="surface-race")
        surface_reads = 0

        def terminal_runner(argv, **kwargs):
            nonlocal surface_reads
            if "rpc" in argv and argv[argv.index("rpc") + 1] == "surface.list":
                surface_reads += 1
                # Each resolution now has one identity-normalization read and
                # one title read. The tenth is the title validator inside the
                # immediate SQLite transaction after the row is staged.
                if surface_reads == 10:
                    fake.title = "Other · GEN-38"
            return fake(argv, **kwargs)

        process_calls = []
        result = session.resume_this_session(
            environ=env, runner=full_resume_runner(process_calls),
            terminal_runner=terminal_runner, which=lambda _: "/opt/cmux",
            binding_path=self.db, resume_script=Path("resume.py"),
            created_at="2026-09-01T01:00:00Z",
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(
            result["resume_binding"]["reason"], "session_context_changed",
        )
        self.assertEqual(len(process_calls), 1)
        connection = sqlite3.connect(self.db)
        self.assertEqual(connection.execute(
            "SELECT count(*) FROM terminal_binding_events_v1"
        ).fetchone()[0], 0)
        self.assertEqual(connection.execute(
            "SELECT count(*) FROM terminal_bindings_v1"
        ).fetchone()[0], 0)
        connection.close()

    def test_concurrent_human_rename_never_overwrites_or_advances_successor(self):
        env = self.cmux_env("surface-human-race")
        fake = FakeCmux("Linear", surface="surface-human-race")
        self.seed(fake, env=env)
        title_reads = 0

        def terminal_runner(argv, **kwargs):
            nonlocal title_reads
            if "list-pane-surfaces" in argv:
                title_reads += 1
                if title_reads == 2:
                    fake.title = "Human renamed this tab"
            return fake(argv, **kwargs)

        result = session.resume_this_session(
            environ=env, runner=full_resume_runner([]),
            terminal_runner=terminal_runner, which=lambda _: "/opt/cmux",
            binding_path=self.db, resume_script=Path("resume.py"),
            created_at="2026-09-01T01:00:00Z",
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(result["tab_binding"]["reason"], "cmux_title_changed")
        self.assertEqual(fake.title, "Human renamed this tab")
        self.assertNotIn("rename-tab", [
            call[1] for call in fake.calls if len(call) > 1
        ])
        connection = sqlite3.connect(self.db)
        self.assertEqual(connection.execute(
            "SELECT count(*) FROM terminal_binding_events_v1"
        ).fetchone()[0], 1)
        self.assertEqual(connection.execute(
            "SELECT provider_session_id FROM terminal_bindings_v1"
        ).fetchone()[0], "old")
        connection.close()

    def test_workspace_move_before_title_apply_cannot_rename_or_bind_new_target(self):
        env = self.cmux_env("surface-workspace-race")
        fake = FakeCmux(
            "Linear · GEN-37", surface="surface-workspace-race",
            workspace="workspace-1",
        )
        apply_started = False

        def terminal_runner(argv, **kwargs):
            nonlocal apply_started
            # Resolver calls use the explicit socket prefix; workstream_tab's
            # first ping marks the later optional title-application boundary.
            if "ping" in argv:
                apply_started = True
            if apply_started and "identify" in argv and "--no-caller" not in argv:
                fake.caller_workspace = "workspace-2"
            return fake(argv, **kwargs)

        result = session.resume_this_session(
            environ=env, runner=full_resume_runner([]),
            terminal_runner=terminal_runner, which=lambda _: "/opt/cmux",
            binding_path=self.db, resume_script=Path("resume.py"),
        )
        self.assertEqual(result["resume_authority"], "full")
        self.assertEqual(result["tab_binding"]["reason"], "cmux_workspace_changed")
        self.assertEqual(
            result["resume_binding"]["reason"], "terminal_title_unverified",
        )
        self.assertNotIn("rename-tab", [
            part for call in fake.calls for part in call
        ])
        self.assertFalse(self.db.exists())


if __name__ == "__main__":
    unittest.main()
