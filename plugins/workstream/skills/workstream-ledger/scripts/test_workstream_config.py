import json
import tempfile
import unittest
from pathlib import Path

from workstream_config import load_config, resolve_config_path, resolve_linear_route


def config() -> dict:
    return {
        "schema_version": 1,
        "namespace": "example",
        "ledger": {
            "provider": "linear",
            "workspace_id": "workspace",
            "team_id": "team",
            "project_id": "project",
        },
        "repositories": {
            "github:R_example": {"coordinate": "github.com/example/project"}
        },
    }


class WorkstreamConfigTests(unittest.TestCase):
    def repository(self):
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name)
        (root / ".git").mkdir()
        (root / ".workstream.json").write_text(json.dumps(config()))
        nested = root / "nested" / "deeper"
        nested.mkdir(parents=True)
        self.addCleanup(temp.cleanup)
        return root, nested

    def test_discovers_only_the_enclosing_repository_root_config(self):
        root, nested = self.repository()
        self.assertEqual(resolve_config_path(cwd=nested, env={}), (root / ".workstream.json").resolve())
        loaded = load_config(cwd=nested, env={})
        self.assertEqual(loaded[0]["namespace"], "example")

    def test_explicit_or_environment_path_works_outside_git(self):
        root, _nested = self.repository()
        elsewhere = root.parent
        path = root / ".workstream.json"
        self.assertEqual(resolve_config_path(path, cwd=elsewhere, env={}), path.resolve())
        self.assertEqual(
            resolve_config_path(cwd=elsewhere, env={"WORKSTREAM_CONFIG": str(path)}), path.resolve()
        )

    def test_config_route_is_authoritative_and_conflicts_fail(self):
        _root, nested = self.repository()
        route, path = resolve_linear_route(cwd=nested, env={})
        self.assertEqual(route, {
            "workspace_id": "workspace", "team_id": "team", "project_id": "project"
        })
        self.assertEqual(path.name, ".workstream.json")
        with self.assertRaisesRegex(ValueError, "conflicts with workstream config"):
            resolve_linear_route(team_id="wrong", cwd=nested, env={})

    def test_partial_explicit_route_fails_but_legacy_team_only_remains(self):
        self.assertEqual(
            resolve_linear_route(team_id="team", cwd=Path("/nonexistent"), env={})[0],
            {"team_id": "team"},
        )
        with self.assertRaisesRegex(ValueError, "must be supplied together"):
            resolve_linear_route(
                team_id="team", project_id="project", cwd=Path("/nonexistent"), env={}
            )

    def test_non_linear_provider_is_refused_for_linear_operation(self):
        root, nested = self.repository()
        value = config()
        value["ledger"]["provider"] = "other"
        (root / ".workstream.json").write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, "unsupported ledger provider"):
            resolve_linear_route(cwd=nested, env={})


if __name__ == "__main__":
    unittest.main()
