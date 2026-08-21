import json
import tempfile
import unittest
from pathlib import Path

from workstream_config import (
    load_config, load_linear_api_key, resolve_config_path, resolve_linear_route,
)


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

    def test_linear_auth_prefers_environment_then_standard_file(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            token_dir = home / ".config" / "agent-workstream"
            token_dir.mkdir(parents=True, mode=0o700)
            token_dir.chmod(0o700)
            token_file = token_dir / "linear.token"
            token_file.write_text("file-token\n")
            token_file.chmod(0o600)
            self.assertEqual(
                load_linear_api_key(env={"HOME": str(home)}), "file-token"
            )
            self.assertEqual(
                load_linear_api_key(env={
                    "HOME": str(home), "LINEAR_API_KEY": "environment-token",
                }),
                "environment-token",
            )

    def test_linear_auth_supports_explicit_file_and_rejects_weak_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            token_dir = Path(directory) / "secrets"
            token_dir.mkdir(mode=0o700)
            token_file = token_dir / "linear.token"
            token_file.write_text("secret")
            token_file.chmod(0o600)
            env = {"LINEAR_API_KEY_FILE": str(token_file)}
            self.assertEqual(load_linear_api_key(env=env), "secret")
            token_file.chmod(0o400)
            self.assertEqual(load_linear_api_key(env=env), "secret")
            token_file.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "inaccessible to group/world"):
                load_linear_api_key(env=env)

    def test_linear_auth_expands_explicit_tilde_from_supplied_home(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            token_dir = home / "private"
            token_dir.mkdir(mode=0o700)
            token_file = token_dir / "linear.token"
            token_file.write_text("mapped-home")
            token_file.chmod(0o600)
            self.assertEqual(
                load_linear_api_key(env={
                    "HOME": str(home),
                    "LINEAR_API_KEY_FILE": "~/private/linear.token",
                }),
                "mapped-home",
            )

    def test_linear_auth_does_not_consult_host_home_for_explicit_test_env(self):
        self.assertIsNone(load_linear_api_key(env={}))


if __name__ == "__main__":
    unittest.main()
