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
import stat
import subprocess
import sys
import tempfile
import tomllib
from typing import Any

MARKETPLACE = "generous-workstream"
PLUGIN_ID = "workstream@generous-workstream"
CODEX_RUNTIME_PLUGIN = "agent-workstream-runtime"
CODEX_RUNTIME_MARKETPLACE_PREFIX = "generous-workstream-runtime-"
CLIENTS = ("codex", "claude")
COMMAND_TIMEOUT_SECONDS = 180
TERMINATION_TIMEOUT_SECONDS = 5
STATE_ROOT = Path.home() / ".local/state/agent-workstream"
MANIFESTS = {"codex": ".codex-plugin/plugin.json", "claude": ".claude-plugin/plugin.json"}
MIRROR_MARKER = ".agent-workstream-skill-sync.json"
MIRROR_TRANSACTION_PREFIX = ".agent-workstream-skill-txn-"
MIRROR_OWNERSHIP = ".agent-workstream-skill-ownership.json"
MIRROR_SCHEMA = 1
CODEX_PROJECTION_SCHEMA = 1


class InstallError(RuntimeError):
    pass


def codex_runtime_generation(commit: str, source_digest: str) -> str:
    material = f"{CODEX_PROJECTION_SCHEMA}\0{commit}\0{source_digest}".encode()
    return hashlib.sha256(material).hexdigest()


def codex_runtime_marketplace(commit: str, source_digest: str) -> str:
    return CODEX_RUNTIME_MARKETPLACE_PREFIX + codex_runtime_generation(commit, source_digest)


def codex_runtime_plugin_id(commit: str, source_digest: str) -> str:
    return f"{CODEX_RUNTIME_PLUGIN}@{codex_runtime_marketplace(commit, source_digest)}"


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


def safe_skill_digest(path: Path) -> str:
    """Digest one skill tree while refusing links and non-regular content."""
    if path.is_symlink() or not path.is_dir():
        raise InstallError(f"unsafe_skill_directory:{path}")
    for walk_root, directories, files in os.walk(path, followlinks=False):
        root_path = Path(walk_root)
        for name in directories + files:
            item = root_path / name
            if item.is_symlink():
                raise InstallError(f"unsafe_skill_symlink:{item}")
            if not item.is_dir() and not item.is_file():
                raise InstallError(f"unsafe_skill_entry:{item}")
    return tree_digest(path)


def plugin_skill_digests(plugin_root: Path) -> dict[str, str]:
    skills_root = plugin_root / "skills"
    if skills_root.is_symlink() or not skills_root.is_dir():
        raise InstallError(f"source_skills_missing_or_unsafe:{skills_root}")
    result: dict[str, str] = {}
    for item in sorted(skills_root.iterdir(), key=lambda value: value.name):
        if not _safe_skill_name(item.name) or item.is_symlink() or not item.is_dir():
            raise InstallError(f"unsafe_source_skill_entry:{item}")
        result[item.name] = safe_skill_digest(item)
    if not result:
        raise InstallError("source_skills_empty")
    return result


def _safe_skill_name(name: Any) -> bool:
    return isinstance(name, str) and bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name))


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x") as handle:
            handle.write(json.dumps(value, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _remove_tree(path: Path) -> None:
    if path.is_symlink():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def _unlink_and_fsync(path: Path) -> None:
    path.unlink()
    _fsync_directory(path.parent)


def _open_safe_mirror_root(mirror_root: Path, *, create: bool) -> tuple[Path, int]:
    raw = mirror_root.expanduser()
    if not raw.is_absolute():
        raise InstallError("skill_mirror_root_must_be_absolute")
    if ".." in raw.parts or raw == Path("/"):
        raise InstallError(f"unsafe_skill_mirror_root:{raw}")
    root = Path(os.path.abspath(raw))
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptor = os.open("/", flags)
    try:
        for component in root.parts[1:]:
            try:
                child = os.open(component, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise InstallError(f"skill_mirror_root_missing:{root}")
                os.mkdir(component, mode=0o700, dir_fd=descriptor)
                os.fsync(descriptor)
                child = os.open(component, flags, dir_fd=descriptor)
            except OSError as error:
                raise InstallError(f"unsafe_skill_mirror_ancestry:{root}:{component}") from error
            os.close(descriptor)
            descriptor = child
        return root, descriptor
    except Exception:
        os.close(descriptor)
        raise


def _assert_mirror_root(root: Path, descriptor: int) -> None:
    observed = os.stat(root, follow_symlinks=False)
    opened = os.fstat(descriptor)
    if (not stat.S_ISDIR(observed.st_mode) or
            (observed.st_dev, observed.st_ino) != (opened.st_dev, opened.st_ino)):
        raise InstallError(f"skill_mirror_root_redirected:{root}")


def _validated_digest_map(value: Any, *, label: str) -> dict[str, str | None]:
    if not isinstance(value, dict):
        raise InstallError(f"skill_mirror_{label}_invalid")
    result: dict[str, str | None] = {}
    for name, digest in value.items():
        if not _safe_skill_name(name) or (digest is not None and not re.fullmatch(r"[0-9a-f]{64}", digest)):
            raise InstallError(f"skill_mirror_{label}_invalid")
        result[name] = digest
    return result


def _read_ownership(root: Path, *, required: bool) -> dict[str, str] | None:
    path = root / MIRROR_OWNERSHIP
    if path.is_symlink():
        raise InstallError(f"unsafe_skill_mirror_ownership:{path}")
    if not path.exists():
        if required:
            raise InstallError("skill_mirror_ownership_missing")
        return None
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise InstallError(f"skill_mirror_ownership_invalid:{path}") from error
    if not isinstance(value, dict) or value.get("schema") != MIRROR_SCHEMA or value.get("plugin") != "workstream":
        raise InstallError(f"skill_mirror_ownership_invalid:{path}")
    skills = _validated_digest_map(value.get("skills"), label="ownership")
    if any(digest is None for digest in skills.values()):
        raise InstallError(f"skill_mirror_ownership_invalid:{path}")
    return {name: digest for name, digest in skills.items() if digest is not None}


def _write_ownership(root: Path, skills: dict[str, str] | None) -> None:
    path = root / MIRROR_OWNERSHIP
    if skills is None:
        if path.exists():
            _unlink_and_fsync(path)
        return
    _write_json_atomic(path, {"schema": MIRROR_SCHEMA, "plugin": "workstream",
                              "skills": skills})


def _observed_targets(root: Path, names: set[str]) -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    for name in sorted(names):
        target = root / name
        if target.is_symlink():
            raise InstallError(f"unsafe_skill_mirror_target_symlink:{target}")
        if not target.exists():
            result[name] = None
        elif not target.is_dir():
            raise InstallError(f"unsafe_skill_mirror_target:{target}")
        else:
            result[name] = safe_skill_digest(target)
    return result


def _mirror_partial_paths(root: Path) -> list[Path]:
    return sorted([*root.glob(f"{MIRROR_TRANSACTION_PREFIX}*"),
                   *root.glob(f".{MIRROR_MARKER}.*.tmp"),
                   *root.glob(f".{MIRROR_OWNERSHIP}.*.tmp")])


def _read_journal(root: Path) -> dict[str, Any]:
    marker = root / MIRROR_MARKER
    if marker.is_symlink():
        raise InstallError(f"unsafe_skill_mirror_marker:{marker}")
    try:
        value = json.loads(marker.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise InstallError(f"skill_mirror_journal_invalid:{marker}") from error
    if (not isinstance(value, dict) or value.get("schema") != MIRROR_SCHEMA or
            value.get("root") != str(root) or
            value.get("phase") not in {"prepared", "publishing", "committed"}):
        raise InstallError(f"skill_mirror_journal_invalid:{marker}")
    transaction = value.get("transaction")
    if not isinstance(transaction, str) or not transaction.startswith(MIRROR_TRANSACTION_PREFIX):
        raise InstallError(f"skill_mirror_journal_invalid:{marker}")
    value["before"] = _validated_digest_map(value.get("before"), label="journal_before")
    value["after"] = _validated_digest_map(value.get("after"), label="journal_after")
    affected = value.get("affected")
    if (not isinstance(affected, list) or not all(_safe_skill_name(name) for name in affected) or
            len(set(affected)) != len(affected)):
        raise InstallError(f"skill_mirror_journal_invalid:{marker}")
    before_ownership = value.get("ownership_before")
    if before_ownership is not None:
        before_ownership = _validated_digest_map(before_ownership, label="journal_ownership_before")
        if any(item is None for item in before_ownership.values()):
            raise InstallError(f"skill_mirror_journal_invalid:{marker}")
    after_ownership = _validated_digest_map(value.get("ownership_after"), label="journal_ownership_after")
    if any(item is None for item in after_ownership.values()):
        raise InstallError(f"skill_mirror_journal_invalid:{marker}")
    value["ownership_before"] = before_ownership
    value["ownership_after"] = after_ownership
    if set(value["before"]) != set(value["after"]):
        raise InstallError(f"skill_mirror_journal_invalid:{marker}")
    if set(affected) != {name for name in value["before"]
                         if value["before"][name] != value["after"][name]}:
        raise InstallError(f"skill_mirror_journal_invalid:{marker}")
    value["affected"] = affected
    return value


def _journal_phase(root: Path, record: dict[str, Any], phase: str) -> None:
    updated = {**record, "phase": phase}
    _write_json_atomic(root / MIRROR_MARKER, updated)
    record["phase"] = phase


def _recover_skill_mirror(root: Path, descriptor: int) -> str:
    record = _read_journal(root)
    transaction = root / record["transaction"]
    current = _observed_targets(root, set(record["after"]))
    ownership = _read_ownership(root, required=False)
    after_exact = current == record["after"] and ownership == record["ownership_after"]
    before_exact = current == record["before"] and ownership == record["ownership_before"]
    transaction_present = transaction.exists() or transaction.is_symlink()
    if after_exact:
        if transaction_present:
            if transaction.is_symlink() or not transaction.is_dir():
                raise InstallError(f"skill_mirror_transaction_unsafe:{transaction}")
            _remove_tree(transaction)
        _unlink_and_fsync(root / MIRROR_MARKER)
        return "finalized"
    if not transaction_present and before_exact:
        _unlink_and_fsync(root / MIRROR_MARKER)
        return "rolled_back"
    if record["phase"] == "committed":
        raise InstallError("skill_mirror_committed_state_mismatch")
    if transaction.is_symlink() or not transaction.is_dir():
        raise InstallError(f"skill_mirror_recovery_missing:{transaction}")
    backup = transaction / "backup"
    for name in record["affected"]:
        digest = record["before"][name]
        saved = backup / name
        if digest is None:
            if saved.exists() or saved.is_symlink():
                raise InstallError(f"skill_mirror_unexpected_backup:{saved}")
        elif safe_skill_digest(saved) != digest:
            raise InstallError(f"skill_mirror_backup_mismatch:{name}")
    rollback = transaction / "rollback"
    rollback.mkdir(exist_ok=True)
    for name in record["affected"]:
        digest = record["before"][name]
        _assert_mirror_root(root, descriptor)
        target = root / name
        if target.exists():
            _remove_tree(rollback / name)
            os.replace(target, rollback / name)
        if digest is not None:
            shutil.copytree(backup / name, target, symlinks=False)
        _fsync_directory(root)
    restored = _observed_targets(root, set(record["before"]))
    if any(restored[name] != record["before"][name] for name in record["affected"]):
        raise InstallError("skill_mirror_rollback_verification_failed")
    _write_ownership(root, record["ownership_before"])
    _remove_tree(transaction)
    _unlink_and_fsync(root / MIRROR_MARKER)
    return "rolled_back"


def sync_skill_mirror(plugin_root: Path, mirror_root: Path, *, update: bool,
                      expected_plugin_digest: str | None = None) -> dict[str, Any]:
    """Verify or transactionally synchronize plugin-owned global skills."""
    plugin_digest = tree_digest(plugin_root)
    if expected_plugin_digest is not None and plugin_digest != expected_plugin_digest:
        raise InstallError("skill_mirror_source_digest_mismatch")
    expected = plugin_skill_digests(plugin_root)
    root, descriptor = _open_safe_mirror_root(mirror_root, create=update)
    try:
        recovery = "none"
        marker = root / MIRROR_MARKER
        if marker.exists() or marker.is_symlink():
            if not update:
                raise InstallError(f"skill_mirror_recovery_required:{marker}")
            recovery = _recover_skill_mirror(root, descriptor)
        partial = _mirror_partial_paths(root)
        if partial:
            raise InstallError(f"skill_mirror_partial_state:{partial[0]}")
        ownership = _read_ownership(root, required=not update)
        prior = ownership or {}
        obsolete = set(prior) - set(expected)
        observed = _observed_targets(root, set(expected) | obsolete)
        if obsolete and not update:
            raise InstallError(f"skill_mirror_obsolete_owned:{','.join(sorted(obsolete))}")
        for name in obsolete:
            if observed[name] is None:
                raise InstallError(f"skill_mirror_obsolete_owned_missing:{name}")
            if observed[name] != prior[name]:
                raise InstallError(f"skill_mirror_obsolete_owned_modified:{name}")
        exact = (ownership == expected and
                 all(observed.get(name) == digest for name, digest in expected.items()) and
                 not obsolete)
        if exact:
            if tree_digest(plugin_root) != plugin_digest:
                raise InstallError("skill_mirror_source_changed_during_verification")
            return {"status": "verified", "root": str(root), "changed": False,
                    "recovery": recovery, "plugin_tree_sha256": plugin_digest,
                    "skills": expected, "retired": []}
        if not update:
            mismatches = sorted(name for name, digest in expected.items()
                                if observed.get(name) != digest)
            raise InstallError(f"skill_mirror_digest_mismatch:{','.join(mismatches)}")

        affected = {name for name, digest in expected.items() if observed.get(name) != digest} | obsolete
        transaction = Path(tempfile.mkdtemp(prefix=MIRROR_TRANSACTION_PREFIX, dir=root))
        stage = transaction / "stage"
        backup = transaction / "backup"
        stage.mkdir()
        backup.mkdir()
        before = {name: observed.get(name) for name in sorted(set(expected) | obsolete)}
        after = {name: expected.get(name) for name in sorted(set(expected) | obsolete)}
        record: dict[str, Any] = {
            "schema": MIRROR_SCHEMA, "root": str(root), "phase": "prepared",
            "transaction": transaction.name, "before": before, "after": after,
            "affected": sorted(affected),
            "ownership_before": ownership, "ownership_after": expected,
        }
        try:
            for name in affected:
                if name in expected:
                    shutil.copytree(plugin_root / "skills" / name, stage / name, symlinks=False)
                    if safe_skill_digest(stage / name) != expected[name]:
                        raise InstallError(f"skill_mirror_stage_mismatch:{name}")
                if before[name] is not None:
                    shutil.copytree(root / name, backup / name, symlinks=False)
                    if safe_skill_digest(backup / name) != before[name]:
                        raise InstallError(f"skill_mirror_backup_mismatch:{name}")
            _write_json_atomic(marker, record)
            _journal_phase(root, record, "publishing")
            retired = transaction / "retired"
            retired.mkdir()
            for name in sorted(affected):
                _assert_mirror_root(root, descriptor)
                target = root / name
                if target.exists():
                    os.replace(target, retired / name)
                if after[name] is not None:
                    os.replace(stage / name, target)
                _fsync_directory(root)
            if _observed_targets(root, set(after)) != after:
                raise InstallError("skill_mirror_post_update_mismatch")
            if tree_digest(plugin_root) != plugin_digest:
                raise InstallError("skill_mirror_source_changed_during_update")
            _write_ownership(root, expected)
            _journal_phase(root, record, "committed")
        except Exception:
            if marker.exists():
                try:
                    _recover_skill_mirror(root, descriptor)
                except Exception as rollback_error:
                    raise InstallError(f"skill_mirror_rollback_failed:{rollback_error}") from rollback_error
            else:
                _remove_tree(transaction)
            raise
        _remove_tree(transaction)
        _unlink_and_fsync(marker)
        return {"status": "verified", "root": str(root), "changed": True,
                "recovery": recovery, "plugin_tree_sha256": plugin_digest,
                "skills": expected, "retired": sorted(obsolete)}
    finally:
        os.close(descriptor)


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


def codex_projection_root(expected_commit: str, expected_digest: str,
                          state_root: Path | None = None) -> Path:
    selected = STATE_ROOT if state_root is None else state_root
    generation = codex_runtime_generation(expected_commit, expected_digest)
    return selected.expanduser().resolve() / "codex-marketplaces" / generation


def active_codex_process_generations() -> list[dict[str, Any]]:
    output = run(["ps", "-axo", "pid=,lstart=,comm="])
    result: list[dict[str, Any]] = []
    for line in output.splitlines():
        fields = line.strip().split()
        if len(fields) >= 3 and Path(fields[-1]).name == "codex":
            try:
                pid = int(fields[0])
            except ValueError:
                continue
            result.append({"pid": pid, "started": " ".join(fields[1:-1])})
    return sorted(result, key=lambda item: (item["pid"], item["started"]))


def verify_codex_projection(root: Path, *, expected_commit: str,
                            expected_version: str,
                            expected_digest: str) -> dict[str, Any]:
    metadata_path = root / ".agent-workstream-source.json"
    try:
        metadata = json.loads(metadata_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise InstallError(f"codex_projection_metadata_invalid:{metadata_path}") from error
    expected = {
        "schema": CODEX_PROJECTION_SCHEMA,
        "source_commit": expected_commit,
        "version": expected_version,
        "source_tree_sha256": expected_digest,
        "generation": codex_runtime_generation(expected_commit, expected_digest),
    }
    pre_migration = metadata.get("pre_migration_processes") if isinstance(metadata, dict) else None
    if (not isinstance(metadata, dict) or
            {key: metadata.get(key) for key in expected} != expected or
            not isinstance(metadata.get("packaged_tree_sha256"), str) or
            not isinstance(pre_migration, list) or
            any(not isinstance(item, dict) or not isinstance(item.get("pid"), int) or
                item["pid"] <= 0 or not isinstance(item.get("started"), str) or
                not item["started"] for item in pre_migration)):
        raise InstallError("codex_projection_metadata_mismatch")
    marketplace_path = root / ".agents/plugins/marketplace.json"
    try:
        marketplace = json.loads(marketplace_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise InstallError(f"codex_projection_marketplace_invalid:{marketplace_path}") from error
    expected_marketplace = {
        "name": codex_runtime_marketplace(expected_commit, expected_digest),
        "interface": {"displayName": "Generous Workstream"},
        "plugins": [{
            "name": CODEX_RUNTIME_PLUGIN,
            "source": {"source": "local", "path": "./plugins/workstream"},
            "policy": {"installation": "AVAILABLE", "authentication": "ON_USE"},
            "category": "Productivity",
        }],
    }
    if marketplace != expected_marketplace:
        raise InstallError("codex_projection_marketplace_mismatch")
    plugin_root = root / "plugins/workstream"
    if manifest_version(plugin_root, "codex") != expected_version:
        raise InstallError("codex_projection_version_mismatch")
    observed_digest = tree_digest(plugin_root)
    if observed_digest != metadata["packaged_tree_sha256"]:
        raise InstallError("codex_projection_tree_mismatch")
    live = active_codex_process_generations()
    return {
        "path": str(root), **expected,
        "packaged_tree_sha256": observed_digest,
        "pre_migration_processes": pre_migration,
        "running_pre_migration_processes": [item for item in pre_migration if item in live],
    }


def verify_codex_config(target: Path, *, expected_source: Path,
                        expected_commit: str,
                        expected_digest: str) -> dict[str, Any]:
    config_path = target / "config.toml"
    try:
        with config_path.open("rb") as handle:
            config = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise InstallError(f"codex_config_invalid:{config_path}") from error
    marketplaces = config.get("marketplaces")
    plugins = config.get("plugins")
    if not isinstance(marketplaces, dict) or not isinstance(plugins, dict):
        raise InstallError("codex_config_plugin_tables_missing")
    expected_marketplace = codex_runtime_marketplace(expected_commit, expected_digest)
    expected_plugin = codex_runtime_plugin_id(expected_commit, expected_digest)
    runtime_marketplaces = sorted(
        key for key in marketplaces
        if isinstance(key, str) and key.startswith(CODEX_RUNTIME_MARKETPLACE_PREFIX)
    )
    runtime_plugins = sorted(
        key for key in plugins
        if isinstance(key, str) and
        key.startswith(f"{CODEX_RUNTIME_PLUGIN}@{CODEX_RUNTIME_MARKETPLACE_PREFIX}")
    )
    if runtime_marketplaces != [expected_marketplace]:
        raise InstallError(
            f"codex_config_runtime_marketplace_mismatch:{runtime_marketplaces}"
        )
    if runtime_plugins != [expected_plugin]:
        raise InstallError(f"codex_config_runtime_plugin_mismatch:{runtime_plugins}")
    marketplace = marketplaces[expected_marketplace]
    plugin = plugins[expected_plugin]
    if (not isinstance(marketplace, dict) or
            marketplace.get("source_type") != "local" or
            not isinstance(marketplace.get("source"), str) or
            Path(marketplace["source"]).expanduser().resolve() != expected_source.resolve()):
        raise InstallError("codex_config_runtime_marketplace_source_mismatch")
    if not isinstance(plugin, dict) or plugin.get("enabled") is not True:
        raise InstallError("codex_config_runtime_plugin_not_enabled")
    if MARKETPLACE in marketplaces or PLUGIN_ID in plugins:
        raise InstallError("legacy_codex_config_registration_remains")
    return {
        "path": str(config_path), "marketplace": expected_marketplace,
        "plugin": expected_plugin, "source": str(expected_source.resolve()),
    }


def sync_codex_projection(source_root: Path, *, expected_commit: str,
                          expected_version: str, expected_digest: str,
                          journal: "TransactionJournal",
                          capture_pre_migration: bool = False) -> tuple[Path, bool]:
    """Atomically project an exact plugin under a collision-proof identity."""
    root = codex_projection_root(expected_commit, expected_digest)
    root.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root.parent, 0o700)
    previous = root.with_name(f".{root.name}.previous")
    root_exact = False
    try:
        verify_codex_projection(
            root, expected_commit=expected_commit,
            expected_version=expected_version, expected_digest=expected_digest,
        )
        root_exact = True
    except InstallError:
        pass
    if previous.exists() or previous.is_symlink():
        if root_exact:
            _remove_tree(previous)
            _fsync_directory(root.parent)
            journal.set_phase("projection_recovery_finalized")
            return root, True
        if not root.exists():
            os.replace(previous, root)
            _fsync_directory(root.parent)
        else:
            raise InstallError("codex_projection_recovery_ambiguous")
    if root_exact:
        if capture_pre_migration:
            metadata_path = root / ".agent-workstream-source.json"
            metadata = json.loads(metadata_path.read_text())
            observed = active_codex_process_generations()
            combined = {
                (item["pid"], item["started"]): item
                for item in [*metadata["pre_migration_processes"], *observed]
            }
            updated = sorted(
                combined.values(), key=lambda item: (item["pid"], item["started"])
            )
            if updated != metadata["pre_migration_processes"]:
                metadata["pre_migration_processes"] = updated
                _write_json_atomic(metadata_path, metadata)
                verify_codex_projection(
                    root, expected_commit=expected_commit,
                    expected_version=expected_version,
                    expected_digest=expected_digest,
                )
                journal.set_phase("projection_pre_migration_extended")
                return root, True
        return root, False
    stage = Path(tempfile.mkdtemp(prefix=f".{root.name}.next-", dir=root.parent))
    activated = False
    saved_previous = False
    try:
        plugin_target = stage / "plugins/workstream"
        plugin_target.parent.mkdir(parents=True)
        shutil.copytree(source_root / "plugins/workstream", plugin_target, symlinks=True)
        runtime_manifest = plugin_target / MANIFESTS["codex"]
        manifest_payload = json.loads(runtime_manifest.read_text())
        manifest_payload["name"] = CODEX_RUNTIME_PLUGIN
        _write_json_atomic(runtime_manifest, manifest_payload)
        packaged_digest = tree_digest(plugin_target)
        marketplace_path = stage / ".agents/plugins/marketplace.json"
        marketplace_path.parent.mkdir(parents=True)
        _write_json_atomic(marketplace_path, {
            "name": codex_runtime_marketplace(expected_commit, expected_digest),
            "interface": {"displayName": "Generous Workstream"},
            "plugins": [{
                "name": CODEX_RUNTIME_PLUGIN,
                "source": {"source": "local", "path": "./plugins/workstream"},
                "policy": {"installation": "AVAILABLE", "authentication": "ON_USE"},
                "category": "Productivity",
            }],
        })
        _write_json_atomic(stage / ".agent-workstream-source.json", {
            "schema": CODEX_PROJECTION_SCHEMA,
            "source_commit": expected_commit,
            "version": expected_version,
            "source_tree_sha256": expected_digest,
            "packaged_tree_sha256": packaged_digest,
            "generation": codex_runtime_generation(expected_commit, expected_digest),
            "pre_migration_processes": active_codex_process_generations(),
        })
        verify_codex_projection(
            stage, expected_commit=expected_commit,
            expected_version=expected_version, expected_digest=expected_digest,
        )
        journal.set_phase("projection_staged")
        if root.exists():
            os.replace(root, previous)
            saved_previous = True
            _fsync_directory(root.parent)
            journal.set_phase("projection_previous_saved")
        os.replace(stage, root)
        activated = True
        _fsync_directory(root.parent)
        journal.set_phase("projection_activated")
        verify_codex_projection(
            root, expected_commit=expected_commit,
            expected_version=expected_version, expected_digest=expected_digest,
        )
        if saved_previous:
            _remove_tree(previous)
            _fsync_directory(root.parent)
        journal.set_phase("projection_ready")
        return root, True
    except Exception:
        if activated and root.exists():
            _remove_tree(root)
        if saved_previous and previous.exists():
            os.replace(previous, root)
            _fsync_directory(root.parent)
        raise
    finally:
        if stage.exists() or stage.is_symlink():
            _remove_tree(stage)


def codex_inventory(env: dict[str, str] | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    marketplaces = run(["codex", "plugin", "marketplace", "list", "--json"], parse_json=True, env=env)
    if not isinstance(marketplaces, dict):
        raise InstallError("invalid_codex_marketplaces_top_level")
    marketplace_items = marketplaces.get("marketplaces")
    if not isinstance(marketplace_items, list):
        raise InstallError("invalid_codex_marketplace_container")
    if any(not isinstance(item, dict) for item in marketplace_items):
        raise InstallError("invalid_codex_marketplace_item")
    runtime_marketplaces = [item for item in marketplace_items if
                            isinstance(item.get("name"), str) and
                            item["name"].startswith(CODEX_RUNTIME_MARKETPLACE_PREFIX)]
    if len(runtime_marketplaces) > 1:
        raise InstallError("duplicate_codex_runtime_marketplace_records")
    marketplace = runtime_marketplaces[0] if runtime_marketplaces else None
    plugins = run(["codex", "plugin", "list", "--json"], parse_json=True, env=env)
    if not isinstance(plugins, dict):
        raise InstallError("invalid_codex_plugins_top_level")
    plugin_items = plugins.get("installed")
    if not isinstance(plugin_items, list):
        raise InstallError("invalid_codex_plugin_container")
    if any(not isinstance(item, dict) for item in plugin_items):
        raise InstallError("invalid_codex_plugin_item")
    runtime_plugins = [item for item in plugin_items if
                       isinstance(item.get("pluginId"), str) and
                       item["pluginId"].startswith(
                           f"{CODEX_RUNTIME_PLUGIN}@{CODEX_RUNTIME_MARKETPLACE_PREFIX}")]
    if len(runtime_plugins) > 1:
        raise InstallError("duplicate_codex_runtime_plugin_records")
    plugin = runtime_plugins[0] if runtime_plugins else None
    return marketplace, plugin


def _validated_runtime_marketplace_name(value: Any) -> str | None:
    if not isinstance(value, str) or not value.startswith(
            CODEX_RUNTIME_MARKETPLACE_PREFIX):
        return None
    generation = value.removeprefix(CODEX_RUNTIME_MARKETPLACE_PREFIX)
    if not re.fullmatch(r"[0-9a-f]{64}", generation):
        raise InstallError(f"malformed_codex_runtime_marketplace:{value}")
    return value


def _validated_runtime_plugin_id(value: Any) -> str | None:
    if not isinstance(value, str) or "@" not in value:
        return None
    plugin_name, marketplace_name = value.split("@", 1)
    marketplace = _validated_runtime_marketplace_name(marketplace_name)
    if marketplace is not None:
        if plugin_name != CODEX_RUNTIME_PLUGIN:
            raise InstallError(f"foreign_codex_runtime_plugin:{value}")
        return value
    if plugin_name == CODEX_RUNTIME_PLUGIN:
        raise InstallError(f"foreign_codex_runtime_plugin:{value}")
    return None


def codex_runtime_registration_inventory(
    target: Path, env: dict[str, str] | None = None,
) -> dict[str, list[str]]:
    """Read the bounded runtime identities from both CLI and raw config."""
    marketplaces = run(
        ["codex", "plugin", "marketplace", "list", "--json"],
        parse_json=True, env=env,
    )
    plugins = run(
        ["codex", "plugin", "list", "--json"], parse_json=True, env=env,
    )
    if not isinstance(marketplaces, dict):
        raise InstallError("invalid_codex_marketplaces_top_level")
    marketplace_items = marketplaces.get("marketplaces")
    if not isinstance(marketplace_items, list):
        raise InstallError("invalid_codex_marketplace_container")
    if any(not isinstance(item, dict) for item in marketplace_items):
        raise InstallError("invalid_codex_marketplace_item")
    if not isinstance(plugins, dict):
        raise InstallError("invalid_codex_plugins_top_level")
    plugin_items = plugins.get("installed")
    if not isinstance(plugin_items, list):
        raise InstallError("invalid_codex_plugin_container")
    if any(not isinstance(item, dict) for item in plugin_items):
        raise InstallError("invalid_codex_plugin_item")

    cli_marketplaces = [
        identity for item in marketplace_items
        if (identity := _validated_runtime_marketplace_name(item.get("name")))
        is not None
    ]
    cli_plugins = [
        identity for item in plugin_items
        if (identity := _validated_runtime_plugin_id(item.get("pluginId")))
        is not None
    ]
    if len(cli_marketplaces) != len(set(cli_marketplaces)):
        raise InstallError("duplicate_codex_runtime_marketplace_records")
    if len(cli_plugins) != len(set(cli_plugins)):
        raise InstallError("duplicate_codex_runtime_plugin_records")

    config_path = target / "config.toml"
    if config_path.exists():
        try:
            with config_path.open("rb") as handle:
                config = tomllib.load(handle)
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise InstallError(f"codex_config_invalid:{config_path}") from error
        if not isinstance(config, dict):
            raise InstallError(f"codex_config_invalid:{config_path}")
    else:
        config = {}
    config_marketplace_table = config.get("marketplaces", {})
    config_plugin_table = config.get("plugins", {})
    if not isinstance(config_marketplace_table, dict):
        raise InstallError("codex_config_marketplace_table_invalid")
    if not isinstance(config_plugin_table, dict):
        raise InstallError("codex_config_plugin_table_invalid")
    config_marketplaces: list[str] = []
    for key, value in config_marketplace_table.items():
        identity = _validated_runtime_marketplace_name(key)
        if identity is not None:
            if not isinstance(value, dict):
                raise InstallError(
                    f"malformed_codex_runtime_marketplace_record:{key}"
                )
            config_marketplaces.append(identity)
    config_plugins: list[str] = []
    for key, value in config_plugin_table.items():
        identity = _validated_runtime_plugin_id(key)
        if identity is not None:
            if not isinstance(value, dict):
                raise InstallError(
                    f"malformed_codex_runtime_plugin_record:{key}"
                )
            config_plugins.append(identity)
    return {
        "cli_marketplaces": sorted(cli_marketplaces),
        "cli_plugins": sorted(cli_plugins),
        "config_marketplaces": sorted(config_marketplaces),
        "config_plugins": sorted(config_plugins),
    }


def cleanup_codex_runtime_generations(
    target: Path, env: dict[str, str], *, expected_plugin_id: str,
    journal: "TransactionJournal",
) -> bool:
    """Remove only stale collision-proof generations through Codex's CLI."""
    expected_plugin = _validated_runtime_plugin_id(expected_plugin_id)
    if expected_plugin is None:
        raise InstallError("expected_codex_runtime_plugin_invalid")
    expected_marketplace = expected_plugin.split("@", 1)[1]
    observed = codex_runtime_registration_inventory(target, env)
    stale_plugins = sorted(
        (
            set(observed["cli_plugins"])
            | set(observed["config_plugins"])
        ) - {expected_plugin}
    )
    stale_marketplaces = sorted(
        (
            set(observed["cli_marketplaces"])
            | set(observed["config_marketplaces"])
        ) - {expected_marketplace}
    )
    changed = False
    for plugin_id in stale_plugins:
        journal.set_phase(f"removing_stale_runtime_plugin:{plugin_id}")
        run(["codex", "plugin", "remove", plugin_id, "--json"], env=env)
        journal.set_phase(f"stale_runtime_plugin_removed:{plugin_id}")
        changed = True
    for marketplace_name in stale_marketplaces:
        journal.set_phase(
            f"removing_stale_runtime_marketplace:{marketplace_name}"
        )
        run([
            "codex", "plugin", "marketplace", "remove", marketplace_name,
            "--json",
        ], env=env)
        journal.set_phase(
            f"stale_runtime_marketplace_removed:{marketplace_name}"
        )
        changed = True
    remaining = codex_runtime_registration_inventory(target, env)
    for key, surface in remaining.items():
        expected = expected_plugin if key.endswith("plugins") else expected_marketplace
        stale = set(surface) - {expected}
        if stale:
            raise InstallError(
                "codex_runtime_generation_cleanup_incomplete:"
                + ",".join(sorted(stale))
            )
    journal.set_phase("runtime_generations_ready")
    return changed


def codex_legacy_inventory(env: dict[str, str] | None = None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    marketplaces = run(["codex", "plugin", "marketplace", "list", "--json"], parse_json=True, env=env)
    plugins = run(["codex", "plugin", "list", "--json"], parse_json=True, env=env)
    if not isinstance(marketplaces, dict) or not isinstance(plugins, dict):
        raise InstallError("invalid_codex_legacy_inventory")
    marketplace = exactly_one(
        marketplaces.get("marketplaces"), identity_key="name",
        identity=MARKETPLACE, error_prefix="codex_legacy_marketplace",
    )
    plugin = exactly_one(
        plugins.get("installed"), identity_key="pluginId",
        identity=PLUGIN_ID, error_prefix="codex_legacy_plugin",
    )
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
    plugin_id = plugin.get("pluginId")
    if not isinstance(plugin_id, str) or "@" not in plugin_id:
        raise InstallError("codex_plugin_identity_unavailable")
    plugin_name, marketplace_name = plugin_id.split("@", 1)
    return target / "plugins/cache" / marketplace_name / plugin_name / version


def expected_install_path(target: Path, version: str, *, expected_commit: str = "a" * 40,
                          expected_digest: str = "d") -> Path:
    return (target / "plugins/cache" /
            codex_runtime_marketplace(expected_commit, expected_digest) /
            CODEX_RUNTIME_PLUGIN / version)


def refuse_version_collision(client: str, plugin: dict[str, Any] | None,
                             target: Path, version: str,
                             expected_digest: str) -> None:
    candidates: set[Path] = set()
    if client == "codex" and plugin is not None and plugin.get("version") == version:
        candidates.add(install_path(client, plugin, version, target=target))
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
        "codex_runtime_marketplace_generation_mismatch",
        "codex_runtime_plugin_generation_mismatch",
        "codex_config_runtime_marketplace_mismatch",
        "codex_config_runtime_plugin_mismatch",
        "duplicate_codex_runtime_marketplace_records",
        "duplicate_codex_runtime_plugin_records",
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
    projection = None
    codex_config = None
    if client == "codex":
        projection = verify_codex_projection(
            marketplace_root, expected_commit=expected_commit,
            expected_version=expected_version, expected_digest=expected_digest,
        )
        observed_commit = projection["source_commit"]
        expected_marketplace_name = codex_runtime_marketplace(
            expected_commit, expected_digest
        )
        if marketplace.get("name") != expected_marketplace_name:
            raise InstallError("codex_runtime_marketplace_generation_mismatch")
        if plugin.get("pluginId") != codex_runtime_plugin_id(
                expected_commit, expected_digest):
            raise InstallError("codex_runtime_plugin_generation_mismatch")
        codex_config = verify_codex_config(
            target, expected_source=expected_source,
            expected_commit=expected_commit, expected_digest=expected_digest,
        )
    else:
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
    expected_installed_digest = (
        projection["packaged_tree_sha256"] if projection is not None else expected_digest
    )
    if installed_digest != expected_installed_digest:
        raise InstallError(f"installed_tree_mismatch:{client}")
    receipt = {"client": client, "host_id": host_id, "target": str(target),
            "commit": observed_commit, "version": expected_version,
            "tree_sha256": installed_digest, "enabled": True,
            "status": "verified"}
    if projection is not None:
        receipt["codex_projection"] = projection
        receipt["codex_config"] = codex_config
        receipt["source_tree_sha256"] = expected_digest
    return receipt


def verify_post_update_stability(client: str, *, expected_commit: str,
                                 expected_version: str, expected_source: Path,
                                 expected_digest: str, host_id: str,
                                 target: Path, env: dict[str, str]) -> dict[str, Any]:
    """Verify the collision-proof production identity after all mutations."""
    marketplace, plugin = inventory(client, env)
    try:
        return verify_client(
            client, marketplace, plugin,
            expected_commit=expected_commit,
            expected_version=expected_version,
            expected_source=expected_source,
            expected_digest=expected_digest,
            host_id=host_id, target=target,
        )
    except InstallError as error:
        raise InstallError(
            f"post_update_stability_failed:{client}:{error}"
        ) from error


def inventory(client: str, env: dict[str, str]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    return codex_inventory(env) if client == "codex" else claude_inventory(env)


def codex_projection_plugin_id(source_root: Path) -> str:
    projection_manifest = json.loads(
        (source_root / ".agents/plugins/marketplace.json").read_text()
    )
    return f"{projection_manifest['plugins'][0]['name']}@{projection_manifest['name']}"


def update_client(client: str, *, source_root: Path, expected_version: str,
                  env: dict[str, str], journal: "TransactionJournal") -> None:
    if shutil.which(client, path=env.get("PATH")) is None:
        raise InstallError(f"client_unavailable:{client}")
    marketplace, plugin = inventory(client, env)
    codex_plugin_id = None
    codex_marketplace_name = None
    if client == "codex":
        codex_plugin_id = codex_projection_plugin_id(source_root)
        codex_marketplace_name = codex_plugin_id.split("@", 1)[1]
    marketplace_ready = False
    if marketplace is not None:
        root_key = "root" if client == "codex" else "installLocation"
        root_value = marketplace.get(root_key)
        if not isinstance(root_value, str) or not root_value:
            raise InstallError(f"marketplace_root_unavailable:{client}")
        marketplace_ready = Path(root_value).resolve() == source_root.resolve()
        if client == "codex":
            marketplace_ready = (
                marketplace_ready and marketplace.get("name") == codex_marketplace_name
            )
    if marketplace is not None and not marketplace_ready:
        journal.set_phase("removing_marketplace")
        if client == "codex":
            run(["codex", "plugin", "marketplace", "remove", marketplace["name"], "--json"], env=env)
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
    if client == "codex" and plugin is not None:
        # Codex intentionally has no plugin-update command. `plugin add` is an
        # install operation and may leave an already-installed older version
        # selected, so replace that registration through the supported CLI
        # before installing the exact marketplace version.
        journal.set_phase("removing_plugin")
        run(["codex", "plugin", "remove", plugin["pluginId"], "--json"], env=env)
        journal.set_phase("plugin_removed")
    journal.set_phase("installing_plugin")
    if client == "claude" and plugin is not None and plugin.get("version") == expected_version:
        run(["claude", "plugin", "enable", PLUGIN_ID, "--scope", "user"], env=env)
    elif client == "claude" and plugin is not None:
        run(["claude", "plugin", "update", PLUGIN_ID, "--scope", "user", "--yes"], env=env)
    elif client == "claude":
        run(["claude", "plugin", "install", PLUGIN_ID, "--scope", "user", "--yes"], env=env)
    else:
        assert codex_plugin_id is not None
        run(["codex", "plugin", "add", codex_plugin_id, "--json"], env=env)
    journal.set_phase("plugin_installed")


def remove_legacy_codex_registration(env: dict[str, str],
                                     journal: "TransactionJournal") -> bool:
    marketplace, plugin = codex_legacy_inventory(env)
    changed = False
    if plugin is not None:
        journal.set_phase("removing_legacy_plugin")
        run(["codex", "plugin", "remove", PLUGIN_ID, "--json"], env=env)
        journal.set_phase("legacy_plugin_removed")
        changed = True
    if marketplace is not None:
        journal.set_phase("removing_legacy_marketplace")
        run(["codex", "plugin", "marketplace", "remove", MARKETPLACE, "--json"], env=env)
        journal.set_phase("legacy_marketplace_removed")
        changed = True
    remaining_marketplace, remaining_plugin = codex_legacy_inventory(env)
    if remaining_marketplace is not None or remaining_plugin is not None:
        raise InstallError("legacy_codex_registration_remains")
    return changed


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
    parser.add_argument(
        "--skill-mirror-root", type=Path,
        help="explicit shared/global skill root to verify or synchronize",
    )
    parser.add_argument("--client", action="append", choices=CLIENTS, dest="clients")
    args = parser.parse_args(argv)
    clients = tuple(dict.fromkeys(args.clients or CLIENTS))
    try:
        validate_identity(args.expected_commit, args.expected_version, args.host_id)
        if args.skill_mirror_root is not None and set(clients) != set(CLIENTS):
            raise InstallError("skill_mirror_requires_both_clients")
        source = verify_source(args.source_root, expected_commit=args.expected_commit,
                               expected_version=args.expected_version)
        source_root = Path(source["path"])
        with ProcessLock(STATE_ROOT / "plugin-update.lock",
                         exclusive=args.mode == "update"):
            mirror_receipt = None
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
                    client_source = source_root
                    if client == "codex":
                        if args.mode == "update":
                            if not recovering:
                                journal.set_phase("preparing_projection")
                            client_source, projection_changed = sync_codex_projection(
                                source_root,
                                expected_commit=args.expected_commit,
                                expected_version=args.expected_version,
                                expected_digest=source["tree_sha256"],
                                journal=journal,
                                capture_pre_migration=recovering,
                            )
                            changed |= projection_changed
                            changed |= remove_legacy_codex_registration(env, journal)
                            changed |= cleanup_codex_runtime_generations(
                                target, env,
                                expected_plugin_id=(
                                    codex_runtime_plugin_id(
                                        args.expected_commit,
                                        source["tree_sha256"],
                                    )
                                ),
                                journal=journal,
                            )
                        else:
                            client_source = codex_projection_root(
                                args.expected_commit, source["tree_sha256"]
                            )
                            verify_codex_projection(
                                client_source,
                                expected_commit=args.expected_commit,
                                expected_version=args.expected_version,
                                expected_digest=source["tree_sha256"],
                            )
                            previous = client_source.with_name(
                                f".{client_source.name}.previous"
                            )
                            if previous.exists() or previous.is_symlink():
                                raise InstallError("codex_projection_recovery_required")
                    if args.mode == "update":
                        marketplace = None
                        plugin = None
                        try:
                            marketplace, plugin = inventory(client, env)
                            receipt = verify_client(
                                client, marketplace, plugin,
                                expected_commit=args.expected_commit,
                                expected_version=args.expected_version,
                                expected_source=client_source,
                                expected_digest=source["tree_sha256"],
                                host_id=args.host_id, target=target,
                            )
                        except InstallError as verification_error:
                            if not repairable_verification_error(verification_error):
                                raise
                            if client == "codex" and recovering:
                                client_source, capture_changed = sync_codex_projection(
                                    source_root,
                                    expected_commit=args.expected_commit,
                                    expected_version=args.expected_version,
                                    expected_digest=source["tree_sha256"],
                                    journal=journal, capture_pre_migration=True,
                                )
                                changed |= capture_changed
                            refuse_version_collision(
                                client, plugin, target, args.expected_version,
                                source["tree_sha256"],
                            )
                            if not recovering and not journal.exists:
                                journal.set_phase("preparing")
                            update_client(
                                client, source_root=client_source, env=env,
                                journal=journal,
                                expected_version=args.expected_version,
                            )
                            changed = True
                            marketplace, plugin = inventory(client, env)
                            receipt = verify_client(
                                client, marketplace, plugin,
                                expected_commit=args.expected_commit,
                                expected_version=args.expected_version,
                                expected_source=client_source,
                                expected_digest=source["tree_sha256"],
                                host_id=args.host_id, target=target,
                            )
                        if client == "codex":
                            journal.set_phase("verifying_stability")
                            receipt = verify_post_update_stability(
                                client,
                                expected_commit=args.expected_commit,
                                expected_version=args.expected_version,
                                expected_source=client_source,
                                expected_digest=source["tree_sha256"],
                                host_id=args.host_id, target=target, env=env,
                            )
                        journal.clear()
                    else:
                        if recovering:
                            raise InstallError(
                                f"recovery_required:{client}:{journal.phase}"
                            )
                        marketplace, plugin = inventory(client, env)
                        if client == "codex":
                            legacy_marketplace, legacy_plugin = codex_legacy_inventory(env)
                            if legacy_marketplace is not None or legacy_plugin is not None:
                                raise InstallError("legacy_codex_registration_remains")
                        receipt = verify_client(
                            client, marketplace, plugin,
                            expected_commit=args.expected_commit,
                            expected_version=args.expected_version,
                            expected_source=client_source,
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
            if args.skill_mirror_root is not None:
                if all(item["status"] == "verified" for item in receipts):
                    try:
                        mirror_receipt = sync_skill_mirror(
                            source_root / "plugins/workstream", args.skill_mirror_root,
                            update=args.mode == "update",
                            expected_plugin_digest=source["tree_sha256"],
                        )
                    except (InstallError, OSError, ValueError) as error:
                        mirror_receipt = {
                            "status": "refused", "root": str(args.skill_mirror_root),
                            "changed": False, "error": str(error),
                        }
                else:
                    mirror_receipt = {
                        "status": "preserved", "root": str(args.skill_mirror_root),
                        "changed": False, "error": "clients_not_verified",
                    }
    except (InstallError, OSError, ValueError) as error:
        print(json.dumps({"status": "refused", "host_id": args.host_id,
                          "error": str(error)}, sort_keys=True))
        return 2
    verified_count = sum(item["status"] == "verified" for item in receipts)
    all_surfaces_verified = (verified_count == len(receipts) and
                             (mirror_receipt is None or mirror_receipt["status"] == "verified"))
    status = "verified" if all_surfaces_verified else ("partial" if verified_count else "refused")
    print(json.dumps({"status": status, "host_id": args.host_id,
                      "expected_commit": args.expected_commit,
                      "expected_version": args.expected_version,
                      "source": source, "skill_mirror": mirror_receipt,
                      "clients": receipts},
                     indent=2, sort_keys=True))
    return 0 if status == "verified" else 2


if __name__ == "__main__":
    sys.exit(main())
