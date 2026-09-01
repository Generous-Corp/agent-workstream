import importlib.machinery
import importlib.util
import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "plugins/workstream/bin/workstreamctl"
LOADER = importlib.machinery.SourceFileLoader("workstreamctl", str(SCRIPT))
SPEC = importlib.util.spec_from_loader("workstreamctl", LOADER)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(MODULE)


class WorkstreamCtlTests(unittest.TestCase):
    def config(self):
        return {
            "schema_version": 1,
            "namespace": "example",
            "planning_url": "https://github.com/example/plans/blob/main/plan.md",
            "ledger": {
                "provider": "linear",
                "workspace_id": "workspace-id",
                "team_id": "team-id",
                "project_id": "project-id",
            },
            "repositories": {"github:R_example": {
                "coordinate": "github.com/example/project",
                "acceptance_commands": ["make test"],
            }},
        }

    def write(self, value):
        temp = tempfile.TemporaryDirectory()
        path = Path(temp.name) / "config.json"
        path.write_text(json.dumps(value))
        return temp, path

    def test_valid_provider_neutral_config(self):
        temp, path = self.write(self.config())
        self.addCleanup(temp.cleanup)
        self.assertEqual(MODULE.validate_config(path)["namespace"], "example")

    def test_duplicate_immutable_repository_identity_fails(self):
        raw = json.dumps(self.config()).replace(
            '"repositories": {',
            '"repositories": {"github:R_example":{"coordinate":"git.example/renamed"},',
        )
        temp = tempfile.TemporaryDirectory()
        path = Path(temp.name) / "config.json"
        path.write_text(raw)
        self.addCleanup(temp.cleanup)
        with self.assertRaisesRegex(ValueError, "duplicate JSON key: github:R_example"):
            MODULE.validate_config(path)

    def test_whitespace_and_non_hierarchical_url_fail(self):
        value = self.config()
        value["namespace"] = "   "
        temp, path = self.write(value)
        self.addCleanup(temp.cleanup)
        with self.assertRaisesRegex(ValueError, "namespace"):
            MODULE.validate_config(path)
        value = self.config()
        value["planning_url"] = "mailto:plans@example.com"
        temp, path = self.write(value)
        self.addCleanup(temp.cleanup)
        with self.assertRaisesRegex(ValueError, "absolute URL"):
            MODULE.validate_config(path)

    def test_planning_url_requires_authority_in_runtime_and_schema(self):
        schema = json.loads(
            (Path(__file__).parents[1] / "plugins/workstream/workstream.config.schema.json").read_text()
        )
        schema_pattern = schema["properties"]["planning_url"]["pattern"]
        for planning_url in ("file:///tmp/plan.md", "https:///path"):
            with self.subTest(planning_url=planning_url):
                value = self.config()
                value["planning_url"] = planning_url
                temp, path = self.write(value)
                self.addCleanup(temp.cleanup)
                with self.assertRaisesRegex(ValueError, "absolute URL"):
                    MODULE.validate_config(path)
                self.assertIsNone(re.fullmatch(schema_pattern, planning_url))

    def test_unknown_fields_fail_closed(self):
        value = self.config()
        value["pulp"] = True
        temp, path = self.write(value)
        self.addCleanup(temp.cleanup)
        with self.assertRaisesRegex(ValueError, "unknown config fields"):
            MODULE.validate_config(path)

    def test_config_validate_without_path_uses_repository_discovery(self):
        loaded = (self.config(), Path("/repo/.workstream.json"))
        with mock.patch.object(MODULE, "load_config", return_value=loaded) as load, \
             mock.patch.object(MODULE.sys, "stdout"):
            self.assertEqual(MODULE.main(["config", "validate"]), 0)
        load.assert_called_once_with(None, required=True)

    def test_projection_dispatches_product_reconcile_command(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main(["projection", "GEN-37", "manifest.json"])
        script = MODULE.SCRIPTS / "workstream_projection.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "GEN-37", "manifest.json"],
        )

    def test_generation_dispatches_supported_transition_command(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main(["generation", "activate", "GEN-37", "--help"])
        script = MODULE.SCRIPTS / "workstream_generation.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "activate", "GEN-37", "--help"],
        )

    def test_material_repair_dispatches_reviewed_dry_run_command(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main([
                "material-repair", "GEN-37", "--manifest", "repair.json",
                "--plan-source", "PLAN.md",
            ])
        script = MODULE.SCRIPTS / "workstream_material_repair.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "GEN-37", "--manifest",
             "repair.json", "--plan-source", "PLAN.md"],
        )

    def test_repository_identity_dispatches_fenced_redirect_command(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main(["repository-identity", "--request", "request.json", "--apply"])
        script = MODULE.SCRIPTS / "workstream_repository_identity.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "--request", "request.json", "--apply"],
        )

    def test_repository_identity_seal_dispatches_bounded_migration_command(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main([
                "repository-identity-seal", "--request", "request.json", "--apply",
            ])
        script = MODULE.SCRIPTS / "workstream_repository_identity_seal.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "--request", "request.json", "--apply"],
        )

    def test_reconcile_dispatches_live_closure_command(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main(["reconcile", "GEN-37", "--help"])
        script = MODULE.SCRIPTS / "workstream_reconcile.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "GEN-37", "--help"],
        )

    def test_shipyard_profile_dispatches_private_profile_generator(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            result = MODULE.main([
                "shipyard-profile", "GEN-37", "--model", "gpt-5.6-sol",
                "--reasoning-effort", "medium", "--output", "/private/profile.json",
            ])
        self.assertEqual(result, 0)
        script = MODULE.SCRIPTS / "workstream_shipyard_profile.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [
                MODULE.sys.executable, str(script), "GEN-37", "--model",
                "gpt-5.6-sol", "--reasoning-effort", "medium", "--output",
                "/private/profile.json",
            ],
        )

    def test_intake_dispatches_reviewed_linear_intake_command(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main(["intake", "plan.md", "--identity", "plan:demo", "--plan-revision", "abc", "--root-stable-key", "source-abc", "--accept-none"])
        script = MODULE.SCRIPTS / "workstream_intake.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "plan.md", "--identity", "plan:demo", "--plan-revision", "abc", "--root-stable-key", "source-abc", "--accept-none"],
        )

    def test_tab_title_dispatches_safe_cmux_adapter(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main(["tab-title", "GEN-37"])
        script = MODULE.SCRIPTS / "workstream_tab.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "GEN-37"],
        )

    def test_child_event_dispatches_exact_child_material_writer(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main(["child-event", "GEN-37", "--help"])
        script = MODULE.SCRIPTS / "workstream_child_event.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "GEN-37", "--help"],
        )

    def test_child_origin_repair_dispatches_reviewed_legacy_seal(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main(["child-origin-repair", "GEN-37", "--help"])
        script = MODULE.SCRIPTS / "workstream_child_origin_repair.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "GEN-37", "--help"],
        )

    def test_child_checkpoint_dispatches_exact_child_checkpoint_writer(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main(["child-checkpoint", "GEN-37", "--help"])
        script = MODULE.SCRIPTS / "workstream_child_checkpoint.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "GEN-37", "--help"],
        )

    def test_child_proposal_activate_dispatches_recovery_writer(self):
        with mock.patch.object(MODULE.os, "execv") as execute:
            MODULE.main(["child-proposal-activate", "GEN-37", "--help"])
        script = MODULE.SCRIPTS / "workstream_child_proposal_activate.py"
        execute.assert_called_once_with(
            MODULE.sys.executable,
            [MODULE.sys.executable, str(script), "GEN-37", "--help"],
        )


if __name__ == "__main__":
    unittest.main()
