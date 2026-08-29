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

    def skill_source(self, root: Path) -> Path:
        plugin = root / "plugin"
        for name, body in (("workstream-ledger", "current"),
                           ("workstream-resume", "resume"),
                           ("decision-audit", "audit")):
            skill = plugin / "skills" / name
            skill.mkdir(parents=True)
            (skill / "SKILL.md").write_text(body)
        return plugin

    def test_tree_digest_detects_changed_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.plugin(root / "first")
            second = self.plugin(root / "second")
            self.assertEqual(MODULE.tree_digest(first), MODULE.tree_digest(second))
            (second / "skills/demo/SKILL.md").write_text("changed")
            self.assertNotEqual(MODULE.tree_digest(first), MODULE.tree_digest(second))

    def test_skill_mirror_updates_stale_owned_skills_and_preserves_unrelated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plugin = self.skill_source(root)
            mirror = root.resolve() / "global"
            (mirror / "workstream-ledger").mkdir(parents=True)
            (mirror / "workstream-ledger/SKILL.md").write_text("stale")
            (mirror / "unrelated").mkdir()
            (mirror / "unrelated/SKILL.md").write_text("keep")
            receipt = MODULE.sync_skill_mirror(plugin, mirror, update=True)
            self.assertTrue(receipt["changed"])
            self.assertEqual((mirror / "workstream-ledger/SKILL.md").read_text(), "current")
            self.assertEqual((mirror / "workstream-resume/SKILL.md").read_text(), "resume")
            self.assertEqual((mirror / "decision-audit/SKILL.md").read_text(), "audit")
            self.assertEqual((mirror / "unrelated/SKILL.md").read_text(), "keep")
            self.assertFalse((mirror / MODULE.MIRROR_MARKER).exists())

    def test_skill_mirror_exact_update_is_noop_and_doctor_verifies(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plugin = self.skill_source(root)
            mirror = root.resolve() / "global"
            first = MODULE.sync_skill_mirror(plugin, mirror, update=True)
            before = (mirror / "workstream-ledger/SKILL.md").stat().st_mtime_ns
            second = MODULE.sync_skill_mirror(plugin, mirror, update=True)
            doctor = MODULE.sync_skill_mirror(plugin, mirror, update=False)
            self.assertTrue(first["changed"])
            self.assertFalse(second["changed"])
            self.assertFalse(doctor["changed"])
            self.assertEqual((mirror / "workstream-ledger/SKILL.md").stat().st_mtime_ns, before)

    def test_skill_mirror_doctor_refuses_mismatch_and_unsafe_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plugin = self.skill_source(root)
            mirror = root.resolve() / "global"
            MODULE.sync_skill_mirror(plugin, mirror, update=True)
            (mirror / "workstream-ledger/SKILL.md").write_text("forged")
            with self.assertRaisesRegex(MODULE.InstallError, "digest_mismatch"):
                MODULE.sync_skill_mirror(plugin, mirror, update=False)
            MODULE._remove_tree(mirror / "workstream-ledger")
            (mirror / "workstream-ledger").symlink_to(plugin / "skills/workstream-ledger")
            with self.assertRaisesRegex(MODULE.InstallError, "target_symlink"):
                MODULE.sync_skill_mirror(plugin, mirror, update=True)

    def test_skill_mirror_failure_rolls_back_every_owned_skill(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plugin = self.skill_source(root)
            mirror = root.resolve() / "global"
            for name in ("workstream-ledger", "decision-audit"):
                (mirror / name).mkdir(parents=True)
                (mirror / name / "SKILL.md").write_text(f"old-{name}")
            real_replace = MODULE.os.replace
            injected = False

            def replace(source, target):
                nonlocal injected
                if not injected and Path(source).parent.name == "stage":
                    injected = True
                    raise OSError("planted replace failure")
                return real_replace(source, target)

            with mock.patch.object(MODULE.os, "replace", side_effect=replace), \
                 self.assertRaisesRegex(OSError, "planted replace failure"):
                MODULE.sync_skill_mirror(plugin, mirror, update=True)
            self.assertEqual((mirror / "workstream-ledger/SKILL.md").read_text(),
                             "old-workstream-ledger")
            self.assertEqual((mirror / "decision-audit/SKILL.md").read_text(),
                             "old-decision-audit")
            self.assertFalse((mirror / "workstream-resume").exists())
            self.assertEqual(MODULE._mirror_partial_paths(mirror), [])
            self.assertFalse((mirror / MODULE.MIRROR_MARKER).exists())

    def test_skill_mirror_update_recovers_recorded_partial_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plugin = self.skill_source(root)
            mirror = root.resolve() / "global"
            mirror.mkdir()
            transaction = mirror / f"{MODULE.MIRROR_TRANSACTION_PREFIX}planted"
            backup = transaction / "backup/workstream-ledger"
            backup.mkdir(parents=True)
            (backup / "SKILL.md").write_text("old")
            (transaction / "stage").mkdir()
            (mirror / "workstream-ledger").mkdir()
            (mirror / "workstream-ledger/SKILL.md").write_text("partial")
            old_digest = MODULE.safe_skill_digest(backup)
            expected = MODULE.plugin_skill_digests(plugin)
            MODULE._write_ownership(mirror, {"workstream-ledger": old_digest})
            MODULE._write_json_atomic(mirror / MODULE.MIRROR_MARKER, {
                "schema": MODULE.MIRROR_SCHEMA, "root": str(mirror),
                "transaction": transaction.name, "phase": "publishing",
                "before": {"workstream-ledger": old_digest},
                "after": {"workstream-ledger": expected["workstream-ledger"]},
                "affected": ["workstream-ledger"],
                "ownership_before": {"workstream-ledger": old_digest},
                "ownership_after": expected,
            })
            receipt = MODULE.sync_skill_mirror(plugin, mirror, update=True)
            self.assertEqual(receipt["recovery"], "rolled_back")
            self.assertEqual((mirror / "workstream-ledger/SKILL.md").read_text(), "current")
            self.assertEqual((mirror / "decision-audit/SKILL.md").read_text(), "audit")

    def test_skill_mirror_refuses_orphan_partial_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plugin = self.skill_source(root)
            mirror = root.resolve() / "global"
            (mirror / f"{MODULE.MIRROR_TRANSACTION_PREFIX}orphan").mkdir(parents=True)
            with self.assertRaisesRegex(MODULE.InstallError, "partial_state"):
                MODULE.sync_skill_mirror(plugin, mirror, update=True)

    def test_skill_mirror_binds_to_verified_plugin_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plugin = self.skill_source(root)
            with self.assertRaisesRegex(MODULE.InstallError, "source_digest_mismatch"):
                MODULE.sync_skill_mirror(
                    plugin, root / "global", update=True,
                    expected_plugin_digest="0" * 64,
                )
            self.assertFalse((root / "global").exists())

    def test_skill_mirror_retires_only_exact_prior_owned_skill(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            plugin = self.skill_source(root)
            mirror = root / "global"
            MODULE.sync_skill_mirror(plugin, mirror, update=True)
            MODULE._remove_tree(plugin / "skills/decision-audit")
            with self.assertRaisesRegex(MODULE.InstallError, "obsolete_owned"):
                MODULE.sync_skill_mirror(plugin, mirror, update=False)
            receipt = MODULE.sync_skill_mirror(plugin, mirror, update=True)
            self.assertEqual(receipt["retired"], ["decision-audit"])
            self.assertFalse((mirror / "decision-audit").exists())
            self.assertEqual(MODULE._read_ownership(mirror, required=True),
                             MODULE.plugin_skill_digests(plugin))

    def test_skill_mirror_refuses_modified_obsolete_owned_skill(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            plugin = self.skill_source(root)
            mirror = root / "global"
            MODULE.sync_skill_mirror(plugin, mirror, update=True)
            MODULE._remove_tree(plugin / "skills/decision-audit")
            (mirror / "decision-audit/SKILL.md").write_text("locally modified")
            with self.assertRaisesRegex(MODULE.InstallError, "obsolete_owned_modified"):
                MODULE.sync_skill_mirror(plugin, mirror, update=True)
            self.assertEqual((mirror / "decision-audit/SKILL.md").read_text(),
                             "locally modified")

    def test_skill_mirror_refuses_missing_obsolete_owned_skill(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            plugin = self.skill_source(root)
            mirror = root / "global"
            MODULE.sync_skill_mirror(plugin, mirror, update=True)
            MODULE._remove_tree(plugin / "skills/decision-audit")
            MODULE._remove_tree(mirror / "decision-audit")
            with self.assertRaisesRegex(MODULE.InstallError, "obsolete_owned_missing"):
                MODULE.sync_skill_mirror(plugin, mirror, update=True)

    def test_skill_mirror_recognizes_exact_rollback_with_missing_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            plugin = self.skill_source(root)
            mirror = root / "global"
            MODULE.sync_skill_mirror(plugin, mirror, update=True)
            before = MODULE.plugin_skill_digests(plugin)
            (plugin / "skills/workstream-ledger/SKILL.md").write_text("next")
            after = MODULE.plugin_skill_digests(plugin)
            MODULE._write_json_atomic(mirror / MODULE.MIRROR_MARKER, {
                "schema": MODULE.MIRROR_SCHEMA, "root": str(mirror),
                "transaction": f"{MODULE.MIRROR_TRANSACTION_PREFIX}missing",
                "phase": "publishing", "before": before, "after": after,
                "affected": ["workstream-ledger"],
                "ownership_before": before, "ownership_after": after,
            })
            receipt = MODULE.sync_skill_mirror(plugin, mirror, update=True)
            self.assertEqual(receipt["recovery"], "rolled_back")
            self.assertEqual((mirror / "workstream-ledger/SKILL.md").read_text(), "next")
            self.assertFalse((mirror / MODULE.MIRROR_MARKER).exists())

    def test_skill_mirror_failure_after_marker_recovers_before_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            plugin = self.skill_source(root)
            mirror = root / "global"
            MODULE.sync_skill_mirror(plugin, mirror, update=True)
            ownership_before = MODULE._read_ownership(mirror, required=True)
            (plugin / "skills/workstream-ledger/SKILL.md").write_text("next")

            with mock.patch.object(
                    MODULE, "_journal_phase",
                    side_effect=OSError("planted post-marker failure")), \
                 self.assertRaisesRegex(OSError, "post-marker"):
                MODULE.sync_skill_mirror(plugin, mirror, update=True)
            self.assertEqual((mirror / "workstream-ledger/SKILL.md").read_text(), "current")
            self.assertEqual(MODULE._read_ownership(mirror, required=True), ownership_before)
            self.assertFalse((mirror / MODULE.MIRROR_MARKER).exists())
            self.assertEqual(MODULE._mirror_partial_paths(mirror), [])

    def test_skill_mirror_finalizes_exact_committed_state_without_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            plugin = self.skill_source(root)
            mirror = root / "global"
            MODULE.sync_skill_mirror(plugin, mirror, update=True)
            expected = MODULE.plugin_skill_digests(plugin)
            MODULE._write_json_atomic(mirror / MODULE.MIRROR_MARKER, {
                "schema": MODULE.MIRROR_SCHEMA, "root": str(mirror),
                "transaction": f"{MODULE.MIRROR_TRANSACTION_PREFIX}missing",
                "phase": "committed", "before": expected, "after": expected,
                "affected": [],
                "ownership_before": expected, "ownership_after": expected,
            })
            receipt = MODULE.sync_skill_mirror(plugin, mirror, update=True)
            self.assertEqual(receipt["recovery"], "finalized")
            self.assertFalse((mirror / MODULE.MIRROR_MARKER).exists())

    def test_skill_mirror_recovery_refuses_corrupt_backup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            plugin = self.skill_source(root)
            mirror = root / "global"
            MODULE.sync_skill_mirror(plugin, mirror, update=True)
            expected = MODULE.plugin_skill_digests(plugin)
            transaction = mirror / f"{MODULE.MIRROR_TRANSACTION_PREFIX}corrupt"
            backup = transaction / "backup/workstream-ledger"
            backup.mkdir(parents=True)
            (backup / "SKILL.md").write_text("corrupt")
            (transaction / "stage").mkdir()
            MODULE._write_json_atomic(mirror / MODULE.MIRROR_MARKER, {
                "schema": MODULE.MIRROR_SCHEMA, "root": str(mirror),
                "transaction": transaction.name, "phase": "publishing",
                "before": {"workstream-ledger": expected["workstream-ledger"]},
                "after": {"workstream-ledger": "0" * 64},
                "affected": ["workstream-ledger"],
                "ownership_before": expected, "ownership_after": expected,
            })
            with self.assertRaisesRegex(MODULE.InstallError, "backup_mismatch"):
                MODULE.sync_skill_mirror(plugin, mirror, update=True)

    def test_skill_mirror_rejects_symlink_in_parent_ancestry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            plugin = self.skill_source(root)
            actual = root / "actual"
            actual.mkdir()
            (root / "redirect").symlink_to(actual, target_is_directory=True)
            with self.assertRaisesRegex(MODULE.InstallError, "unsafe_skill_mirror_ancestry"):
                MODULE.sync_skill_mirror(plugin, root / "redirect/skills", update=True)
            self.assertFalse((actual / "skills").exists())

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

    def test_update_client_replaces_stale_codex_plugin_before_add(self):
        journal = mock.Mock()
        marketplace = {"name": MODULE.MARKETPLACE, "root": "/durable/source"}
        stale_plugin = {
            "pluginId": MODULE.PLUGIN_ID,
            "version": "1.2.2",
            "installed": True,
            "enabled": True,
        }
        with mock.patch.object(MODULE.shutil, "which", return_value="/bin/codex"), \
             mock.patch.object(MODULE, "inventory",
                               return_value=(marketplace, stale_plugin)), \
             mock.patch.object(MODULE, "run") as run:
            MODULE.update_client(
                "codex", source_root=Path("/durable/source"), env={"PATH": "/bin"},
                journal=journal, expected_version="1.2.3",
            )

        commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(commands, [
            ["codex", "plugin", "remove", MODULE.PLUGIN_ID, "--json"],
            ["codex", "plugin", "add", MODULE.PLUGIN_ID, "--json"],
        ])
        self.assertEqual(
            [call.args[0] for call in journal.set_phase.call_args_list[-4:]],
            ["removing_plugin", "plugin_removed", "installing_plugin",
             "plugin_installed"],
        )

    def test_update_client_codex_remove_failure_preserves_recovery_phase(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = MODULE.TransactionJournal(
                Path(directory), host_id="M5", client="codex",
                target=Path("/Users/example/.codex"), expected_commit="a" * 40,
                expected_version="1.2.3",
            )
            journal.set_phase("preparing")
            marketplace = {"name": MODULE.MARKETPLACE, "root": "/durable/source"}
            stale_plugin = {
                "pluginId": MODULE.PLUGIN_ID,
                "version": "1.2.2",
                "installed": True,
                "enabled": True,
            }
            with mock.patch.object(MODULE.shutil, "which", return_value="/bin/codex"), \
                 mock.patch.object(MODULE, "inventory",
                                   return_value=(marketplace, stale_plugin)), \
                 mock.patch.object(MODULE, "run",
                                   side_effect=MODULE.InstallError("injected_failure")), \
                 self.assertRaisesRegex(MODULE.InstallError, "injected_failure"):
                MODULE.update_client(
                    "codex", source_root=Path("/durable/source"),
                    env={"PATH": "/bin"}, journal=journal,
                    expected_version="1.2.3",
                )
            self.assertEqual(journal.phase, "removing_plugin")

    def test_update_client_codex_add_failure_retries_without_second_remove(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = MODULE.TransactionJournal(
                Path(directory), host_id="M5", client="codex",
                target=Path("/Users/example/.codex"), expected_commit="a" * 40,
                expected_version="1.2.3",
            )
            journal.set_phase("preparing")
            marketplace = {"name": MODULE.MARKETPLACE, "root": "/durable/source"}
            stale_plugin = {
                "pluginId": MODULE.PLUGIN_ID,
                "version": "1.2.2",
                "installed": True,
                "enabled": True,
            }
            commands = []

            def fail_add(command, **_kwargs):
                commands.append(command)
                if command[2] == "add":
                    raise MODULE.InstallError("injected_add_failure")

            with mock.patch.object(MODULE.shutil, "which", return_value="/bin/codex"), \
                 mock.patch.object(MODULE, "inventory",
                                   return_value=(marketplace, stale_plugin)), \
                 mock.patch.object(MODULE, "run", side_effect=fail_add), \
                 self.assertRaisesRegex(MODULE.InstallError, "injected_add_failure"):
                MODULE.update_client(
                    "codex", source_root=Path("/durable/source"),
                    env={"PATH": "/bin"}, journal=journal,
                    expected_version="1.2.3",
                )
            self.assertEqual(journal.phase, "installing_plugin")
            self.assertEqual([command[2] for command in commands], ["remove", "add"])

            commands.clear()
            with mock.patch.object(MODULE.shutil, "which", return_value="/bin/codex"), \
                 mock.patch.object(MODULE, "inventory",
                                   return_value=(marketplace, None)), \
                 mock.patch.object(MODULE, "run",
                                   side_effect=lambda command, **_kwargs: commands.append(command)):
                MODULE.update_client(
                    "codex", source_root=Path("/durable/source"),
                    env={"PATH": "/bin"}, journal=journal,
                    expected_version="1.2.3",
                )
            self.assertEqual([command[2] for command in commands], ["add"])
            self.assertEqual(journal.phase, "plugin_installed")

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
                 mock.patch.object(MODULE, "sync_skill_mirror") as mirror_sync, \
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
            mirror_sync.assert_not_called()

    def test_main_opt_in_mirror_is_verified_and_emitted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            targets = {"codex": root / "codex", "claude": root / "claude"}
            for target in targets.values():
                target.mkdir()
            proof = {"path": "/durable/source", "commit": "a" * 40,
                     "tree_sha256": "d"}
            mirror_receipt = {"status": "verified", "root": str(root / "skills"),
                              "changed": True, "recovery": "none",
                              "skills": {"workstream-ledger": "abc"}}
            output = io.StringIO()
            def env_for(client, **_kwargs):
                return ({}, targets[client])

            def verify(client, *_args, **_kwargs):
                return self.receipt(client, targets[client])

            with mock.patch.object(MODULE, "verify_source", return_value=proof), \
                 mock.patch.object(MODULE, "target_env", side_effect=env_for), \
                 mock.patch.object(MODULE, "inventory", return_value=({}, {})), \
                 mock.patch.object(MODULE, "verify_client", side_effect=verify), \
                 mock.patch.object(MODULE, "sync_skill_mirror",
                                   return_value=mirror_receipt) as mirror_sync, \
                 mock.patch.object(MODULE, "STATE_ROOT", root / "state"), \
                 contextlib.redirect_stdout(output):
                code = MODULE.main([
                    "doctor",
                    "--expected-commit", "a" * 40,
                    "--expected-version", "1.2.3", "--host-id", "M5",
                    "--source-root", "/durable/source",
                    "--codex-home", str(targets["codex"]),
                    "--claude-config-dir", str(targets["claude"]),
                    "--skill-mirror-root", str(root / "skills"),
                ])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output.getvalue())["skill_mirror"], mirror_receipt)
            mirror_sync.assert_called_once_with(
                Path("/durable/source/plugins/workstream"), root / "skills", update=False,
                expected_plugin_digest="d",
            )

    def test_main_mirror_requires_both_clients_before_any_client_call(self):
        output = io.StringIO()
        with mock.patch.object(MODULE, "verify_source") as verify_source, \
             mock.patch.object(MODULE, "target_env") as target_env, \
             mock.patch.object(MODULE, "sync_skill_mirror") as mirror_sync, \
             contextlib.redirect_stdout(output):
            code = MODULE.main([
                "update", "--client", "codex",
                "--expected-commit", "a" * 40,
                "--expected-version", "1.2.3", "--host-id", "M5",
                "--source-root", "/durable/source", "--codex-home", "/codex",
                "--skill-mirror-root", "/global/skills",
            ])
        self.assertEqual(code, 2)
        self.assertIn("requires_both_clients", output.getvalue())
        verify_source.assert_not_called()
        target_env.assert_not_called()
        mirror_sync.assert_not_called()

    def test_main_client_failure_preserves_last_good_mirror(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            targets = {"codex": root / "codex", "claude": root / "claude"}
            for target in targets.values():
                target.mkdir()
            proof = {"path": "/durable/source", "commit": "a" * 40,
                     "tree_sha256": "d"}

            def env_for(client, **_kwargs):
                return ({}, targets[client])

            def verify(client, *_args, **_kwargs):
                if client == "claude":
                    raise MODULE.InstallError("installed_tree_mismatch:claude")
                return self.receipt(client, targets[client])

            output = io.StringIO()
            with mock.patch.object(MODULE, "verify_source", return_value=proof), \
                 mock.patch.object(MODULE, "target_env", side_effect=env_for), \
                 mock.patch.object(MODULE, "inventory", return_value=({}, {})), \
                 mock.patch.object(MODULE, "verify_client", side_effect=verify), \
                 mock.patch.object(MODULE, "sync_skill_mirror") as mirror_sync, \
                 mock.patch.object(MODULE, "STATE_ROOT", root / "state"), \
                contextlib.redirect_stdout(output):
                code = MODULE.main([
                    "update", "--expected-commit", "a" * 40,
                    "--expected-version", "1.2.3", "--host-id", "M5",
                    "--source-root", "/durable/source",
                    "--codex-home", str(targets["codex"]),
                    "--claude-config-dir", str(targets["claude"]),
                    "--skill-mirror-root", str(root / "skills"),
                ])
            payload = json.loads(output.getvalue())
            self.assertEqual(code, 2)
            self.assertEqual(payload["skill_mirror"]["status"], "preserved")
            mirror_sync.assert_not_called()

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
