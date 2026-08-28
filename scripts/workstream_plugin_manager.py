#!/usr/bin/env python3
"""Install, update, or verify Agent Workstream from one exact local checkout."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import sys
import tempfile
from typing import Any

MARKETPLACE = "generous-workstream"
PLUGIN_ID = "workstream@generous-workstream"
CLIENTS = ("codex", "claude")
COMMAND_TIMEOUT_SECONDS = 180
TERMINATION_TIMEOUT_SECONDS = 5
STATE_ROOT = Path.home() / ".local/state/agent-workstream"
MANIFESTS = {"codex": ".codex-plugin/plugin.json", "claude": ".claude-plugin/plugin.json"}


class InstallError(RuntimeError):
    pass


def run(command: list[str], *, parse_json: bool = False,
        env: dict[str, str] | None = None) -> Any:
    try:
        process = subprocess.Popen(
            command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=env, start_new_session=True,
        )
    except FileNotFoundError as error:
        raise InstallError(f"client_unavailable:{command[0]}") from error
    try:
        stdout, stderr = process.communicate(timeout=COMMAND_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired as error:
        os.killpg(process.pid, signal.SIGKILL)
        try:
            process.communicate(timeout=TERMINATION_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=TERMINATION_TIMEOUT_SECONDS)
        raise InstallError(f"command_timeout:{command[0]}:{COMMAND_TIMEOUT_SECONDS}") from error
    if process.returncode:
        detail = (stderr or stdout or "").strip().replace("\n", " ")
        raise InstallError(f"command_failed:{command[0]}:{detail[:500]}")
    if not parse_json:
        return stdout
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as error:
        raise InstallError(f"invalid_json_output:{command[0]}") from error


def git_head(path: Path) -> str:
    return run(["git", "-C", str(path), "rev-parse", "HEAD"]).strip()


def require_clean_source(path: Path) -> None:
    status = run(["git", "-C", str(path), "status", "--porcelain=v1", "--untracked-files=all"])
    if status.strip():
        raise InstallError(f"source_checkout_dirty:{path}")
    ignored = run([
        "git", "-C", str(path), "status", "--porcelain=v1", "--ignored",
        "--untracked-files=all", "--", "plugins/workstream",
    ])
    for line in ignored.splitlines():
        ignored_path = Path(line[3:]) if len(line) > 3 else Path(line)
        if ("__pycache__" in ignored_path.parts or
                ignored_path.name in {".DS_Store", ".in_use"}):
            continue
        raise InstallError(f"source_plugin_contains_ignored_files:{path}:{ignored_path}")


def tree_digest(path: Path) -> str:
    digest = hashlib.sha256()
    for item in sorted(path.rglob("*"), key=lambda value: value.relative_to(path).as_posix()):
        relative = item.relative_to(path).as_posix()
        if "__pycache__" in item.parts or item.name in {".DS_Store", ".in_use"}:
            continue
        digest.update(relative.encode())
        digest.update(b"\0")
        if item.is_symlink():
            digest.update(b"link\0")
            digest.update(os.readlink(item).encode())
        elif item.is_file():
            digest.update(f"file:{item.stat().st_mode & 0o777:o}\0".encode())
            digest.update(item.read_bytes())
        elif item.is_dir():
            digest.update(b"dir\0")
    return digest.hexdigest()


def manifest_version(plugin_root: Path, client: str) -> str:
    try:
        value = json.loads((plugin_root / MANIFESTS[client]).read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise InstallError(f"invalid_{client}_manifest:{plugin_root}") from error
    if not isinstance(value, dict):
        raise InstallError(f"invalid_{client}_manifest:{plugin_root}")
    version = value.get("version")
    if not isinstance(version, str) or not version:
        raise InstallError(f"missing_{client}_version:{plugin_root}")
    return version


def verify_source(source_root: Path, *, expected_commit: str,
                  expected_version: str) -> dict[str, str]:
    source_root = source_root.expanduser()
    if not source_root.is_absolute():
        raise InstallError("source_checkout_must_be_absolute")
    source_root = source_root.resolve()
    if not source_root.is_dir():
        raise InstallError(f"source_checkout_missing:{source_root}")
    temporary_root = Path(tempfile.gettempdir()).resolve()
    if (source_root.is_relative_to(Path("/tmp")) or
            source_root.is_relative_to(Path("/private/tmp")) or
            source_root.is_relative_to(temporary_root)):
        raise InstallError(f"source_checkout_not_durable:{source_root}")
    observed_commit = git_head(source_root)
    if observed_commit != expected_commit:
        raise InstallError(f"source_commit_mismatch:{observed_commit}:{expected_commit}")
    require_clean_source(source_root)
    plugin_root = source_root / "plugins/workstream"
    for client in CLIENTS:
        observed_version = manifest_version(plugin_root, client)
        if observed_version != expected_version:
            raise InstallError(
                f"source_version_mismatch:{client}:{observed_version}:{expected_version}"
            )
    return {"path": str(source_root), "commit": observed_commit,
            "tree_sha256": tree_digest(plugin_root)}


def exactly_one(items: Any, *, identity_key: str, identity: str,
                error_prefix: str) -> dict[str, Any] | None:
    if not isinstance(items, list):
        raise InstallError(f"invalid_{error_prefix}_container")
    if any(not isinstance(item, dict) for item in items):
        raise InstallError(f"invalid_{error_prefix}_item")
    matches = [item for item in items if item.get(identity_key) == identity]
    if len(matches) > 1:
        raise InstallError(f"duplicate_{error_prefix}_records")
    return matches[0] if matches else None


def codex_inventory(env: dict[str, str] | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    marketplaces = run(["codex", "plugin", "marketplace", "list", "--json"], parse_json=True, env=env)
    if not isinstance(marketplaces, dict):
        raise InstallError("invalid_codex_marketplaces_top_level")
    marketplace = exactly_one(marketplaces.get("marketplaces"), identity_key="name",
                              identity=MARKETPLACE, error_prefix="codex_marketplace")
    plugins = run(["codex", "plugin", "list", "--json"], parse_json=True, env=env)
    if not isinstance(plugins, dict):
        raise InstallError("invalid_codex_plugins_top_level")
    plugin = exactly_one(plugins.get("installed"), identity_key="pluginId",
                         identity=PLUGIN_ID, error_prefix="codex_plugin")
    return marketplace, plugin


def claude_inventory(env: dict[str, str] | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    marketplaces = run(["claude", "plugin", "marketplace", "list", "--json"], parse_json=True, env=env)
    if not isinstance(marketplaces, list):
        raise InstallError("invalid_claude_marketplaces_top_level")
    marketplace = exactly_one(marketplaces, identity_key="name", identity=MARKETPLACE,
                              error_prefix="claude_marketplace")
    plugins = run(["claude", "plugin", "list", "--json"], parse_json=True, env=env)
    if not isinstance(plugins, list):
        raise InstallError("invalid_claude_plugins_top_level")
    plugin = exactly_one(plugins, identity_key="id", identity=PLUGIN_ID,
                         error_prefix="claude_plugin")
    return marketplace, plugin


def target_env(client: str, *, codex_home: Path | None,
               claude_config_dir: Path | None) -> tuple[dict[str, str], Path]:
    env = dict(os.environ)
    if client == "codex":
        if codex_home is None:
            raise InstallError("codex_home_required")
        target = codex_home.expanduser()
        if not target.is_absolute():
            raise InstallError("codex_home_must_be_absolute")
        target = target.resolve()
        env["CODEX_HOME"] = str(target)
    else:
        if claude_config_dir is None:
            raise InstallError("claude_config_dir_required")
        target = claude_config_dir.expanduser()
        if not target.is_absolute():
            raise InstallError("claude_config_dir_must_be_absolute")
        target = target.resolve()
        env["CLAUDE_CONFIG_DIR"] = str(target)
        env.pop("CLAUDE_CODE_PLUGIN_CACHE_DIR", None)
        env.pop("CLAUDE_CODE_PLUGIN_SEED_DIR", None)
    return env, target


def prepare_target(target: Path, *, create: bool) -> None:
    if target.exists():
        if not target.is_dir():
            raise InstallError(f"target_not_directory:{target}")
        return
    if not create:
        raise InstallError(f"target_missing:{target}")
    target.mkdir(parents=True, mode=0o700)
    os.chmod(target, 0o700)


def install_path(client: str, plugin: dict[str, Any], version: str,
                 *, target: Path) -> Path:
    if client == "claude":
        value = plugin.get("installPath")
        if not isinstance(value, str) or not value:
            raise InstallError("claude_install_path_unavailable")
        path = Path(value).resolve()
        try:
            path.relative_to(target)
        except ValueError as error:
            raise InstallError("claude_install_path_outside_target") from error
        return path
    return target / "plugins/cache" / MARKETPLACE / "workstream" / version


def expected_install_path(target: Path, version: str) -> Path:
    return target / "plugins/cache" / MARKETPLACE / "workstream" / version


def refuse_version_collision(client: str, plugin: dict[str, Any] | None,
                             target: Path, version: str,
                             expected_digest: str) -> None:
    candidates = {expected_install_path(target, version)}
    if (client == "claude" and plugin is not None and
            plugin.get("version") == version):
        value = plugin.get("installPath")
        if isinstance(value, str) and value:
            candidates.add(Path(value).resolve())
    for installed in candidates:
        if installed.exists() and tree_digest(installed) != expected_digest:
            raise InstallError(f"version_commit_conflict:{version}:{installed}")


def repairable_verification_error(error: InstallError) -> bool:
    return str(error).startswith((
        "marketplace_missing:", "plugin_missing_or_disabled:",
        "plugin_not_installed:", "marketplace_source_mismatch:",
        "marketplace_commit_mismatch:", "plugin_version_mismatch:",
        "installed_manifest_version_mismatch:",
    ))


def verify_client(client: str, marketplace: dict[str, Any] | None,
                  plugin: dict[str, Any] | None, *, expected_commit: str,
                  expected_version: str, expected_source: Path,
                  expected_digest: str, host_id: str,
                  target: Path) -> dict[str, Any]:
    if marketplace is None:
        raise InstallError(f"marketplace_missing:{client}")
    if plugin is None or plugin.get("enabled") is not True:
        raise InstallError(f"plugin_missing_or_disabled:{client}")
    if client == "codex" and plugin.get("installed") is not True:
        raise InstallError("plugin_not_installed:codex")
    root_value = marketplace.get("root") if client == "codex" else marketplace.get("installLocation")
    if not isinstance(root_value, str) or not root_value:
        raise InstallError(f"marketplace_root_unavailable:{client}")
    marketplace_root = Path(root_value).resolve()
    if marketplace_root != expected_source.resolve():
        raise InstallError(f"marketplace_source_mismatch:{client}:{marketplace_root}:{expected_source.resolve()}")
    observed_commit = git_head(marketplace_root)
    if observed_commit != expected_commit:
        raise InstallError(f"marketplace_commit_mismatch:{client}:{observed_commit}:{expected_commit}")
    source_root = marketplace_root / "plugins/workstream"
    source_version = manifest_version(source_root, client)
    observed_version = plugin.get("version")
    if source_version != expected_version or observed_version != expected_version:
        raise InstallError(f"plugin_version_mismatch:{client}:{source_version}:{observed_version}:{expected_version}")
    installed_root = install_path(client, plugin, expected_version, target=target)
    if manifest_version(installed_root, client) != expected_version:
        raise InstallError(f"installed_manifest_version_mismatch:{client}")
    installed_digest = tree_digest(installed_root)
    if installed_digest != expected_digest:
        raise InstallError(f"installed_tree_mismatch:{client}")
    return {"client": client, "host_id": host_id, "target": str(target),
            "commit": observed_commit, "version": expected_version,
            "tree_sha256": installed_digest, "enabled": True,
            "status": "verified"}


def inventory(client: str, env: dict[str, str]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    return codex_inventory(env) if client == "codex" else claude_inventory(env)


def update_client(client: str, *, source_root: Path, expected_version: str,
                  env: dict[str, str], journal: "TransactionJournal") -> None:
    if shutil.which(client, path=env.get("PATH")) is None:
        raise InstallError(f"client_unavailable:{client}")
    marketplace, plugin = inventory(client, env)
    marketplace_ready = False
    if marketplace is not None:
        root_key = "root" if client == "codex" else "installLocation"
        root_value = marketplace.get(root_key)
        if not isinstance(root_value, str) or not root_value:
            raise InstallError(f"marketplace_root_unavailable:{client}")
        marketplace_ready = Path(root_value).resolve() == source_root.resolve()
    if marketplace is not None and not marketplace_ready:
        journal.set_phase("removing_marketplace")
        if client == "codex":
            run(["codex", "plugin", "marketplace", "remove", MARKETPLACE, "--json"], env=env)
        else:
            run(["claude", "plugin", "marketplace", "remove", MARKETPLACE, "--scope", "user"], env=env)
        journal.set_phase("marketplace_removed")
        marketplace = None
        plugin = None if client == "claude" else plugin
    elif marketplace is None:
        journal.set_phase("marketplace_absent")
    else:
        journal.set_phase("marketplace_ready")
    if not marketplace_ready:
        journal.set_phase("adding_marketplace")
        if client == "codex":
            run(["codex", "plugin", "marketplace", "add", str(source_root), "--json"], env=env)
        else:
            run(["claude", "plugin", "marketplace", "add", str(source_root), "--scope", "user"], env=env)
        journal.set_phase("marketplace_added")
        marketplace, plugin = inventory(client, env)
    plugin_ready = (
        plugin is not None and plugin.get("enabled") is True and
        plugin.get("version") == expected_version and
        (client != "codex" or plugin.get("installed") is True)
    )
    if plugin_ready:
        journal.set_phase("plugin_present")
        return
    journal.set_phase("installing_plugin")
    if client == "claude" and plugin is not None and plugin.get("version") == expected_version:
        run(["claude", "plugin", "enable", PLUGIN_ID, "--scope", "user"], env=env)
    elif client == "claude" and plugin is not None:
        run(["claude", "plugin", "update", PLUGIN_ID, "--scope", "user", "--yes"], env=env)
    elif client == "claude":
        run(["claude", "plugin", "install", PLUGIN_ID, "--scope", "user", "--yes"], env=env)
    else:
        run(["codex", "plugin", "add", PLUGIN_ID, "--json"], env=env)
    journal.set_phase("plugin_installed")


class TransactionJournal:
    def __init__(self, state_dir: Path, *, host_id: str, client: str,
                 target: Path, expected_commit: str, expected_version: str) -> None:
        key = hashlib.sha256(f"{host_id}\0{client}\0{target}".encode()).hexdigest()[:24]
        self.directory = state_dir.expanduser().resolve() / "transactions"
        self.path = self.directory / f"{key}.json"
        self.identity = {
            "host_id": host_id, "client": client, "target": str(target),
            "expected_commit": expected_commit, "expected_version": expected_version,
        }

    @property
    def exists(self) -> bool:
        return self.path.exists()

    @property
    def phase(self) -> str:
        if not self.exists:
            return "none"
        try:
            value = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise InstallError(f"transaction_journal_invalid:{self.path}") from error
        if not isinstance(value, dict) or any(value.get(key) != expected for key, expected in self.identity.items()):
            raise InstallError(f"transaction_journal_identity_mismatch:{self.path}")
        phase = value.get("phase")
        if not isinstance(phase, str) or not phase:
            raise InstallError(f"transaction_journal_invalid:{self.path}")
        return phase

    def set_phase(self, phase: str) -> None:
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.directory, 0o700)
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        with temporary.open("w") as handle:
            handle.write(json.dumps({**self.identity, "phase": phase}, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, self.path)
        directory_fd = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def clear(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        else:
            directory_fd = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)


class ProcessLock:
    def __init__(self, path: Path, *, exclusive: bool) -> None:
        self.path = path.expanduser().resolve()
        self.exclusive = exclusive
        self.handle: Any = None

    def __enter__(self) -> "ProcessLock":
        parent_existed = self.path.parent.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if not parent_existed:
            os.chmod(self.path.parent, 0o700)
        self.handle = self.path.open("a+")
        os.chmod(self.path, 0o600)
        operation = fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH
        try:
            fcntl.flock(self.handle.fileno(), operation | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self.handle.close()
            self.handle = None
            raise InstallError(f"update_lock_busy:{self.path}") from error
        return self

    def __exit__(self, *_args: Any) -> None:
        if self.handle is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()


def validate_identity(commit: str, version: str, host_id: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise InstallError("expected_commit_must_be_full_sha")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?", version):
        raise InstallError("expected_version_must_be_semver")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", host_id):
        raise InstallError("host_id_invalid")


def refusal(client: str, host_id: str, target: Path,
            error: Exception) -> dict[str, Any]:
    return {"client": client, "host_id": host_id, "target": str(target),
            "status": "refused", "error": str(error)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("update", "doctor"))
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--host-id", required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--codex-home", type=Path)
    parser.add_argument("--claude-config-dir", type=Path)
    parser.add_argument("--client", action="append", choices=CLIENTS, dest="clients")
    args = parser.parse_args(argv)
    clients = tuple(dict.fromkeys(args.clients or CLIENTS))
    try:
        validate_identity(args.expected_commit, args.expected_version, args.host_id)
        source = verify_source(args.source_root, expected_commit=args.expected_commit,
                               expected_version=args.expected_version)
        source_root = Path(source["path"])
        with ProcessLock(STATE_ROOT / "plugin-update.lock",
                         exclusive=args.mode == "update"):
            receipts: list[dict[str, Any]] = []
            for client in clients:
                journal: TransactionJournal | None = None
                try:
                    env, target = target_env(client, codex_home=args.codex_home,
                                             claude_config_dir=args.claude_config_dir)
                    prepare_target(target, create=args.mode == "update")
                    journal = TransactionJournal(
                        STATE_ROOT, host_id=args.host_id, client=client,
                        target=target, expected_commit=args.expected_commit,
                        expected_version=args.expected_version,
                    )
                    recovering = journal.exists
                    changed = False
                    if args.mode == "update":
                        marketplace, plugin = inventory(client, env)
                        try:
                            receipt = verify_client(
                                client, marketplace, plugin,
                                expected_commit=args.expected_commit,
                                expected_version=args.expected_version,
                                expected_source=source_root,
                                expected_digest=source["tree_sha256"],
                                host_id=args.host_id, target=target,
                            )
                        except InstallError as verification_error:
                            if not repairable_verification_error(verification_error):
                                raise
                            refuse_version_collision(
                                client, plugin, target, args.expected_version,
                                source["tree_sha256"],
                            )
                            if not recovering:
                                journal.set_phase("preparing")
                            update_client(
                                client, source_root=source_root, env=env,
                                journal=journal,
                                expected_version=args.expected_version,
                            )
                            changed = True
                            marketplace, plugin = inventory(client, env)
                            receipt = verify_client(
                                client, marketplace, plugin,
                                expected_commit=args.expected_commit,
                                expected_version=args.expected_version,
                                expected_source=source_root,
                                expected_digest=source["tree_sha256"],
                                host_id=args.host_id, target=target,
                            )
                        journal.clear()
                    else:
                        if recovering:
                            raise InstallError(
                                f"recovery_required:{client}:{journal.phase}"
                            )
                        marketplace, plugin = inventory(client, env)
                        receipt = verify_client(
                            client, marketplace, plugin,
                            expected_commit=args.expected_commit,
                            expected_version=args.expected_version,
                            expected_source=source_root,
                            expected_digest=source["tree_sha256"],
                            host_id=args.host_id, target=target,
                        )
                    receipt["changed"] = changed
                    receipt["phase"] = "verified"
                    receipt["recovery"] = "repaired" if recovering else "none"
                    receipts.append(receipt)
                except (InstallError, OSError, ValueError) as error:
                    try:
                        _env, target = target_env(client, codex_home=args.codex_home,
                                                 claude_config_dir=args.claude_config_dir)
                    except InstallError:
                        target = Path("<unspecified>")
                    item = refusal(client, args.host_id, target, error)
                    if journal is not None:
                        try:
                            item["phase"] = journal.phase
                            item["recovery"] = "required" if journal.exists else "none"
                        except InstallError as journal_error:
                            item["phase"] = "journal_invalid"
                            item["recovery"] = "required"
                            item["journal_error"] = str(journal_error)
                    receipts.append(item)
    except (InstallError, OSError, ValueError) as error:
        print(json.dumps({"status": "refused", "host_id": args.host_id,
                          "error": str(error)}, sort_keys=True))
        return 2
    verified_count = sum(item["status"] == "verified" for item in receipts)
    status = "verified" if verified_count == len(receipts) else ("partial" if verified_count else "refused")
    print(json.dumps({"status": status, "host_id": args.host_id,
                      "expected_commit": args.expected_commit,
                      "expected_version": args.expected_version,
                      "source": source, "clients": receipts},
                     indent=2, sort_keys=True))
    return 0 if status == "verified" else 2


if __name__ == "__main__":
    sys.exit(main())
