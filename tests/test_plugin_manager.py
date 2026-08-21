import contextlib
import importlib.util
import io
import json
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).parents[1] / "scripts/workstream_plugin_manager.py"
SPEC = importlib.util.spec_from_file_location("workstream_plugin_manager", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(MODULE)


class PluginManagerTests(unittest.TestCase):
    def plugin(self, root: Path, version: str = "1.2.3") -> Path:
        for manifest in MODULE.MANIFESTS.values():
            path = root / manifest
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"name": "workstream", "version": version}))
        (root / "skills/demo").mkdir(parents=True)
        (root / "skills/demo/SKILL.md").write_text("demo")
        return root

    def source(self, root: Path, version: str = "1.2.3") -> Path:
        self.plugin(root / "plugins/workstream", version)
        return root

    def receipt(self, client: str, target: Path) -> dict:
        return {"client": client, "host_id": "M5", "target": str(target),
                "commit": "a" * 40, "version": "1.2.3",
                "tree_sha256": "d", "enabled": True, "status": "verified"}

    def test_tree_digest_detects_changed_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.plugin(root / "first")
            second = self.plugin(root / "second")
            self.assertEqual(MODULE.tree_digest(first), MODULE.tree_digest(second))
            (second / "skills/demo/SKILL.md").write_text("changed")
            self.assertNotEqual(MODULE.tree_digest(first), MODULE.tree_digest(second))

    def test_verify_source_binds_clean_exact_commit_and_both_manifests(self):
        with tempfile.TemporaryDirectory(dir="/Users/danielraffel/Code") as directory:
            source = self.source(Path(directory))
            with mock.patch.object(MODULE, "git_head", return_value="a" * 40), \
                 mock.patch.object(MODULE, "require_clean_source"):
                proof = MODULE.verify_source(
                    source, expected_commit="a" * 40, expected_version="1.2.3"
                )
            self.assertEqual(proof["commit"], "a" * 40)
            self.assertEqual(proof["tree_sha256"], MODULE.tree_digest(source / "plugins/workstream"))
            with mock.patch.object(MODULE, "git_head", return_value="b" * 40), \
                 self.assertRaisesRegex(MODULE.InstallError, "source_commit_mismatch"):
                MODULE.verify_source(
                    source, expected_commit="a" * 40, expected_version="1.2.3"
                )

    def test_verify_source_refuses_dirty_and_ephemeral_checkout(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(MODULE.InstallError, "not_durable"):
                MODULE.verify_source(
                    Path(directory), expected_commit="a" * 40,
                    expected_version="1.2.3",
                )
        with tempfile.TemporaryDirectory(dir="/Users/danielraffel/Code") as directory:
            source = self.source(Path(directory))
            with mock.patch.object(MODULE, "git_head", return_value="a" * 40), \
                 mock.patch.object(MODULE, "require_clean_source",
                                   side_effect=MODULE.InstallError("source_checkout_dirty:x")), \
                 self.assertRaisesRegex(MODULE.InstallError, "source_checkout_dirty"):
                MODULE.verify_source(
                    source, expected_commit="a" * 40, expected_version="1.2.3"
                )

    def test_clean_source_refuses_ignored_plugin_content(self):
        with mock.patch.object(MODULE, "run",
                               side_effect=["", "!! plugins/workstream/injected"]), \
             self.assertRaisesRegex(MODULE.InstallError, "contains_ignored_files"):
            MODULE.require_clean_source(Path("/durable/source"))

    def test_clean_source_allows_only_digest_excluded_ignored_content(self):
        ignored = "\n".join([
            "!! plugins/workstream/bin/__pycache__/tool.pyc",
            "!! plugins/workstream/.DS_Store",
            "!! plugins/workstream/.in_use",
        ])
        with mock.patch.object(MODULE, "run", side_effect=["", ignored]):
            MODULE.require_clean_source(Path("/durable/source"))

    def test_inventory_parsers_fail_closed_and_reject_duplicates(self):
        cases = [
            (MODULE.codex_inventory, [[]], "invalid_codex_marketplaces_top_level"),
            (MODULE.codex_inventory, [{"marketplaces": "bad"}, {"installed": []}],
             "invalid_codex_marketplace_container"),
            (MODULE.codex_inventory, [{"marketplaces": ["bad"]}, {"installed": []}],
             "invalid_codex_marketplace_item"),
            (MODULE.claude_inventory, [{}, []], "invalid_claude_marketplaces_top_level"),
            (MODULE.claude_inventory, [["bad"], []], "invalid_claude_marketplace_item"),
        ]
        for function, outputs, error in cases:
            with self.subTest(error=error), mock.patch.object(MODULE, "run", side_effect=outputs), \
                 self.assertRaisesRegex(MODULE.InstallError, error):
                function()
        duplicate = [{"name": MODULE.MARKETPLACE}, {"name": MODULE.MARKETPLACE}]
        with mock.patch.object(MODULE, "run", side_effect=[duplicate, []]), \
             self.assertRaisesRegex(MODULE.InstallError, "duplicate_claude_marketplace"):
            MODULE.claude_inventory()

    def test_inventory_parsers_accept_current_live_shapes(self):
        cm = {"name": MODULE.MARKETPLACE, "root": "/codex"}
        cp = {"pluginId": MODULE.PLUGIN_ID, "enabled": True, "installed": True}
        with mock.patch.object(MODULE, "run", side_effect=[{"marketplaces": [cm]}, {"installed": [cp]}]):
            self.assertEqual(MODULE.codex_inventory(), (cm, cp))
        lm = {"name": MODULE.MARKETPLACE, "installLocation": "/claude"}
        lp = {"id": MODULE.PLUGIN_ID, "enabled": True}
        with mock.patch.object(MODULE, "run", side_effect=[[lm], [lp]]):
            self.assertEqual(MODULE.claude_inventory(), (lm, lp))

    def test_verify_client_requires_enabled_installed_and_exact_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.source(root / "source")
            target = root / "codex"
            installed = self.plugin(MODULE.expected_install_path(target, "1.2.3"))
            marketplace = {"root": str(source)}
            plugin = {"version": "1.2.3", "enabled": True, "installed": True}
            digest = MODULE.tree_digest(source / "plugins/workstream")
            with mock.patch.object(MODULE, "git_head", return_value="a" * 40):
                receipt = MODULE.verify_client(
                    "codex", marketplace, plugin, expected_commit="a" * 40,
                    expected_version="1.2.3", expected_source=source,
                    expected_digest=digest, host_id="M5", target=target,
                )
                self.assertEqual(receipt["target"], str(target))
                plugin["enabled"] = False
                with self.assertRaisesRegex(MODULE.InstallError, "disabled"):
                    MODULE.verify_client(
                        "codex", marketplace, plugin, expected_commit="a" * 40,
                        expected_version="1.2.3", expected_source=source,
                        expected_digest=digest, host_id="M5", target=target,
                    )
                plugin["enabled"] = True
                plugin.pop("installed")
                with self.assertRaisesRegex(MODULE.InstallError, "not_installed"):
                    MODULE.verify_client(
                        "codex", marketplace, plugin, expected_commit="a" * 40,
                        expected_version="1.2.3", expected_source=source,
                        expected_digest=digest, host_id="M5", target=target,
                    )
            self.assertTrue(installed.exists())

    def test_explicit_target_environment_and_claude_override_neutralization(self):
        with mock.patch.dict(os.environ, {
            "CLAUDE_CODE_PLUGIN_CACHE_DIR": "/wrong",
            "CLAUDE_CODE_PLUGIN_SEED_DIR": "/wrong",
        }):
            env, target = MODULE.target_env(
                "claude", codex_home=None, claude_config_dir=Path("/tmp/claude")
            )
        self.assertEqual(env["CLAUDE_CONFIG_DIR"], str(target))
        self.assertNotIn("CLAUDE_CODE_PLUGIN_CACHE_DIR", env)
        self.assertNotIn("CLAUDE_CODE_PLUGIN_SEED_DIR", env)
        with self.assertRaisesRegex(MODULE.InstallError, "codex_home_required"):
            MODULE.target_env("codex", codex_home=None, claude_config_dir=None)

    def test_target_creation_is_update_only(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "new-home"
            with self.assertRaisesRegex(MODULE.InstallError, "target_missing"):
                MODULE.prepare_target(target, create=False)
            MODULE.prepare_target(target, create=True)
            self.assertTrue(target.is_dir())
            self.assertEqual(target.stat().st_mode & 0o777, 0o700)

    def test_run_converts_timeout_to_typed_error(self):
        process = mock.Mock(pid=123, returncode=None)
        process.communicate.side_effect = [
            subprocess.TimeoutExpired(["codex"], 1), ("", "")
        ]
        with mock.patch.object(MODULE.subprocess, "Popen", return_value=process), \
             mock.patch.object(MODULE.os, "killpg") as killpg, \
             self.assertRaisesRegex(MODULE.InstallError, "command_timeout:codex"):
            MODULE.run(["codex", "plugin", "list"])
        killpg.assert_called_once_with(123, MODULE.signal.SIGKILL)

    def test_run_timeout_kills_real_descendant_process(self):
        with tempfile.TemporaryDirectory() as directory:
            pid_file = Path(directory) / "child.pid"
            command = ["sh", "-c", f"sleep 30 & echo $! > {pid_file}; wait"]
            with mock.patch.object(MODULE, "COMMAND_TIMEOUT_SECONDS", 0.2), \
                 mock.patch.object(MODULE, "TERMINATION_TIMEOUT_SECONDS", 2), \
                 self.assertRaisesRegex(MODULE.InstallError, "command_timeout:sh"):
                MODULE.run(command)
            child_pid = int(pid_file.read_text())
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                result = subprocess.run(
                    ["ps", "-o", "stat=", "-p", str(child_pid)],
                    text=True, capture_output=True,
                )
                if result.returncode != 0 or "Z" in result.stdout:
                    break
                time.sleep(0.02)
            else:
                self.fail(f"descendant process {child_pid} survived timeout")

    def test_process_lock_refuses_concurrent_writer_and_releases(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lock"
            with MODULE.ProcessLock(path, exclusive=True):
                with self.assertRaisesRegex(MODULE.InstallError, "update_lock_busy"):
                    with MODULE.ProcessLock(path, exclusive=True):
                        pass
            with MODULE.ProcessLock(path, exclusive=True):
                pass

    def test_process_lock_preserves_existing_parent_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "shared"
            parent.mkdir(mode=0o755)
            before = parent.stat().st_mode & 0o777
            with MODULE.ProcessLock(parent / "lock", exclusive=True):
                pass
            self.assertEqual(parent.stat().st_mode & 0o777, before)

    def test_transaction_journal_persists_phase_and_validates_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = MODULE.TransactionJournal(
                Path(directory), host_id="M5", client="claude",
                target=Path("/Users/example/.claude"), expected_commit="a" * 40,
                expected_version="1.2.3",
            )
            journal.set_phase("marketplace_removed")
            self.assertEqual(journal.phase, "marketplace_removed")
            resumed = MODULE.TransactionJournal(
                Path(directory), host_id="M5", client="claude",
                target=Path("/Users/example/.claude"), expected_commit="a" * 40,
                expected_version="1.2.3",
            )
            self.assertTrue(resumed.exists)
            resumed.clear()
            self.assertFalse(resumed.exists)

    def test_update_client_uses_scoped_local_transition_order(self):
        journal = mock.Mock()
        marketplace = {"name": MODULE.MARKETPLACE, "installLocation": "/old"}
        exact_marketplace = {
            "name": MODULE.MARKETPLACE, "installLocation": "/durable/source"
        }
        with mock.patch.object(MODULE.shutil, "which", return_value="/bin/claude"), \
             mock.patch.object(MODULE, "inventory",
                               side_effect=[(marketplace, {}), (exact_marketplace, None)]), \
             mock.patch.object(MODULE, "run") as run:
            MODULE.update_client(
                "claude", source_root=Path("/durable/source"), env={"PATH": "/bin"},
                journal=journal, expected_version="1.2.3",
            )
        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(commands[0], ["claude", "plugin", "marketplace", "remove",
                                       MODULE.MARKETPLACE, "--scope", "user"])
        self.assertEqual(commands[1], ["claude", "plugin", "marketplace", "add",
                                       "/durable/source", "--scope", "user"])
        self.assertEqual(commands[2][:3], ["claude", "plugin", "install"])

    def test_update_client_recovery_journal_survives_each_mutation_failure(self):
        expected_phases = [
            "removing_marketplace", "adding_marketplace", "installing_plugin"
        ]
        for failure_index, expected_phase in enumerate(expected_phases):
            with self.subTest(phase=expected_phase), tempfile.TemporaryDirectory() as directory:
                journal = MODULE.TransactionJournal(
                    Path(directory), host_id="M5", client="claude",
                    target=Path("/Users/example/.claude"),
                    expected_commit="a" * 40, expected_version="1.2.3",
                )
                journal.set_phase("preparing")
                effects = [None, None, None]
                effects[failure_index] = MODULE.InstallError("injected_failure")
                old_marketplace = {
                    "name": MODULE.MARKETPLACE, "installLocation": "/old"
                }
                exact_marketplace = {
                    "name": MODULE.MARKETPLACE,
                    "installLocation": "/durable/source",
                }
                with mock.patch.object(MODULE.shutil, "which", return_value="/bin/claude"), \
                     mock.patch.object(MODULE, "inventory",
                                       side_effect=[(old_marketplace, {}),
                                                    (exact_marketplace, None)]), \
                     mock.patch.object(MODULE, "run", side_effect=effects), \
                     self.assertRaisesRegex(MODULE.InstallError, "injected_failure"):
                    MODULE.update_client(
                        "claude", source_root=Path("/durable/source"),
                        env={"PATH": "/bin"}, journal=journal,
                        expected_version="1.2.3",
                    )
                self.assertEqual(journal.phase, expected_phase)
                with mock.patch.object(MODULE.shutil, "which", return_value="/bin/claude"), \
                     mock.patch.object(MODULE, "inventory",
                                       return_value=(exact_marketplace, None)), \
                     mock.patch.object(MODULE, "run"):
                    MODULE.update_client(
                        "claude", source_root=Path("/durable/source"),
                        env={"PATH": "/bin"}, journal=journal,
                        expected_version="1.2.3",
                    )
                self.assertEqual(journal.phase, "plugin_installed")
                journal.clear()

    def test_version_collision_refuses_same_version_different_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            installed = self.plugin(MODULE.expected_install_path(target, "1.2.3"))
            (installed / "skills/demo/SKILL.md").write_text("old")
            with self.assertRaisesRegex(MODULE.InstallError, "version_commit_conflict"):
                MODULE.refuse_version_collision(
                    "codex", None, target, "1.2.3", "not-the-digest"
                )

    def test_claude_version_collision_checks_reported_install_path(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "claude"
            alternate = self.plugin(Path(directory) / "alternate")
            plugin = {"installPath": str(alternate), "version": "1.2.3"}
            with self.assertRaisesRegex(MODULE.InstallError, "version_commit_conflict"):
                MODULE.refuse_version_collision(
                    "claude", plugin, target, "1.2.3", "not-the-digest"
                )

    def test_claude_previous_version_does_not_collide_with_upgrade(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "claude"
            alternate = self.plugin(Path(directory) / "alternate", "0.2.0")
            plugin = {"installPath": str(alternate), "version": "0.2.0"}
            MODULE.refuse_version_collision(
                "claude", plugin, target, "0.3.0", "new-digest"
            )

    def test_main_noop_and_partial_receipts_preserve_client_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_proof = {"path": "/durable/source", "commit": "a" * 40,
                            "tree_sha256": "d"}
            targets = {"codex": root / "codex", "claude": root / "claude"}
            for target in targets.values():
                target.mkdir()

            def env_for(client, **_kwargs):
                return ({}, targets[client])

            def verify(client, *_args, **_kwargs):
                if client == "claude":
                    raise MODULE.InstallError("plugin_missing_or_disabled:claude")
                return self.receipt(client, targets[client])

            output = io.StringIO()
            with mock.patch.object(MODULE, "verify_source", return_value=source_proof), \
                 mock.patch.object(MODULE, "target_env", side_effect=env_for), \
                 mock.patch.object(MODULE, "inventory", return_value=({}, {})), \
                 mock.patch.object(MODULE, "verify_client", side_effect=verify), \
                 mock.patch.object(MODULE, "refuse_version_collision"), \
                 mock.patch.object(MODULE, "update_client",
                                   side_effect=MODULE.InstallError("command_timeout:claude")) as update, \
                 mock.patch.object(MODULE, "STATE_ROOT", root / "state"), \
                 contextlib.redirect_stdout(output):
                code = MODULE.main([
                    "update", "--expected-commit", "a" * 40,
                    "--expected-version", "1.2.3", "--host-id", "M5",
                    "--source-root", "/durable/source", "--codex-home", str(targets["codex"]),
                    "--claude-config-dir", str(targets["claude"]),
                ])
            payload = json.loads(output.getvalue())
            self.assertEqual(code, 2)
            self.assertEqual(payload["status"], "partial")
            self.assertEqual(payload["clients"][0]["status"], "verified")
            self.assertEqual(payload["clients"][1]["status"], "refused")
            self.assertEqual(payload["clients"][1]["recovery"], "required")
            self.assertEqual(update.call_count, 1)

    def test_main_source_refusal_makes_zero_client_calls(self):
        output = io.StringIO()
        with mock.patch.object(MODULE, "verify_source",
                               side_effect=MODULE.InstallError("source_checkout_dirty:x")), \
             mock.patch.object(MODULE, "target_env") as target_env, \
             mock.patch.object(MODULE, "update_client") as update, \
             contextlib.redirect_stdout(output):
            code = MODULE.main([
                "update", "--expected-commit", "a" * 40,
                "--expected-version", "1.2.3", "--host-id", "M5",
                "--source-root", "/durable/source", "--codex-home", "/codex",
                "--claude-config-dir", "/claude",
            ])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output.getvalue())["status"], "refused")
        target_env.assert_not_called()
        update.assert_not_called()

    def test_doctor_never_mutates_clients(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proof = {"path": "/durable/source", "commit": "a" * 40,
                     "tree_sha256": "d"}
            targets = {"codex": root / "codex", "claude": root / "claude"}
            for target in targets.values():
                target.mkdir()

            def env_for(client, **_kwargs):
                return ({}, targets[client])

            def verify(client, *_args, **_kwargs):
                return self.receipt(client, targets[client])

            with mock.patch.object(MODULE, "verify_source", return_value=proof), \
                 mock.patch.object(MODULE, "target_env", side_effect=env_for), \
                 mock.patch.object(MODULE, "inventory", return_value=({}, {})), \
                 mock.patch.object(MODULE, "verify_client", side_effect=verify), \
                 mock.patch.object(MODULE, "update_client") as update, \
                 mock.patch.object(MODULE, "STATE_ROOT", root / "state"), \
                 contextlib.redirect_stdout(io.StringIO()):
                code = MODULE.main([
                    "doctor", "--expected-commit", "a" * 40,
                    "--expected-version", "1.2.3", "--host-id", "M5",
                    "--source-root", "/durable/source",
                    "--codex-home", str(targets["codex"]),
                    "--claude-config-dir", str(targets["claude"]),
                ])
            self.assertEqual(code, 0)
            update.assert_not_called()

    def test_ambiguous_inventory_failure_never_authorizes_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proof = {"path": "/durable/source", "commit": "a" * 40,
                     "tree_sha256": "d"}
            targets = {"codex": root / "codex", "claude": root / "claude"}

            def env_for(client, **_kwargs):
                return ({}, targets[client])

            inventory_results = [
                MODULE.InstallError("invalid_json_output:codex"), ({}, {}),
            ]
            output = io.StringIO()
            with mock.patch.object(MODULE, "verify_source", return_value=proof), \
                 mock.patch.object(MODULE, "target_env", side_effect=env_for), \
                 mock.patch.object(MODULE, "inventory", side_effect=inventory_results), \
                 mock.patch.object(MODULE, "verify_client",
                                   return_value=self.receipt("claude", targets["claude"])), \
                 mock.patch.object(MODULE, "update_client") as update, \
                 mock.patch.object(MODULE, "STATE_ROOT", root / "state"), \
                 contextlib.redirect_stdout(output):
                code = MODULE.main([
                    "update", "--expected-commit", "a" * 40,
                    "--expected-version", "1.2.3", "--host-id", "M5",
                    "--source-root", "/durable/source",
                    "--codex-home", str(targets["codex"]),
                    "--claude-config-dir", str(targets["claude"]),
                ])
            payload = json.loads(output.getvalue())
            self.assertEqual(code, 2)
            self.assertEqual(payload["status"], "partial")
            update.assert_not_called()

    def test_recovery_after_installed_phase_verifies_without_reinstall(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "state"
            target = root / "claude"
            proof = {"path": "/durable/source", "commit": "a" * 40,
                     "tree_sha256": "d"}
            journal = MODULE.TransactionJournal(
                state, host_id="M5", client="claude", target=target,
                expected_commit="a" * 40, expected_version="1.2.3",
            )
            journal.set_phase("plugin_installed")
            output = io.StringIO()
            with mock.patch.object(MODULE, "verify_source", return_value=proof), \
                 mock.patch.object(MODULE, "target_env", return_value=({}, target)), \
                 mock.patch.object(MODULE, "inventory", return_value=({}, {})), \
                 mock.patch.object(MODULE, "verify_client",
                                   return_value=self.receipt("claude", target)), \
                 mock.patch.object(MODULE, "update_client") as update, \
                 mock.patch.object(MODULE, "STATE_ROOT", state), \
                 contextlib.redirect_stdout(output):
                code = MODULE.main([
                    "update", "--client", "claude",
                    "--expected-commit", "a" * 40,
                    "--expected-version", "1.2.3", "--host-id", "M5",
                    "--source-root", "/durable/source",
                    "--claude-config-dir", str(target),
                ])
            payload = json.loads(output.getvalue())
            self.assertEqual(code, 0)
            self.assertEqual(payload["clients"][0]["recovery"], "repaired")
            self.assertFalse(journal.exists)
            update.assert_not_called()

    def test_identity_requires_full_commit_semver_and_host(self):
        with self.assertRaisesRegex(MODULE.InstallError, "full_sha"):
            MODULE.validate_identity("abc", "1.2.3", "M5")
        with self.assertRaisesRegex(MODULE.InstallError, "semver"):
            MODULE.validate_identity("a" * 40, "latest", "M5")
        with self.assertRaisesRegex(MODULE.InstallError, "host_id"):
            MODULE.validate_identity("a" * 40, "1.2.3", "bad host")


if __name__ == "__main__":
    unittest.main()
