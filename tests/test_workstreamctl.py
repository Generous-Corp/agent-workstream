import importlib.machinery
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


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
            "repositories": [{
                "provider": "github",
                "repository_id": "R_example",
                "coordinate": "github.com/example/project",
                "acceptance_commands": ["make test"],
            }],
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
        value = self.config()
        value["repositories"].append(dict(value["repositories"][0], coordinate="git.example/renamed"))
        temp, path = self.write(value)
        self.addCleanup(temp.cleanup)
        with self.assertRaisesRegex(ValueError, "duplicate repository identity"):
            MODULE.validate_config(path)

    def test_unknown_fields_fail_closed(self):
        value = self.config()
        value["pulp"] = True
        temp, path = self.write(value)
        self.addCleanup(temp.cleanup)
        with self.assertRaisesRegex(ValueError, "unknown config fields"):
            MODULE.validate_config(path)


if __name__ == "__main__":
    unittest.main()
