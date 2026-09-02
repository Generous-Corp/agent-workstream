#!/usr/bin/env python3
"""Resolve and resume the one workstream bound to this exact terminal surface."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
from typing import Any, Callable, Mapping, Sequence

import workstream_tab


SCHEMA_VERSION = 1
# Terminal discovery and title-adapter calls must remain responsive.  Linear
# recovery is an authenticated, cross-process operation and can legitimately
# take longer, so it has its own bounded budget rather than sharing this one.
TERMINAL_TIMEOUT_SECONDS = 10
RESUME_TIMEOUT_SECONDS = 60
# Compatibility for callers that imported the old constant; all terminal
# paths use the explicitly named budget above.
TIMEOUT_SECONDS = TERMINAL_TIMEOUT_SECONDS
MAX_IDENTITY_BYTES = 4096


class ThisSessionError(RuntimeError):
    pass


Runner = Callable[..., subprocess.CompletedProcess[str]]


def _identity(value: object, name: str) -> str:
    if (
        not isinstance(value, str) or not value or value != value.strip()
        or "\x00" in value or len(value.encode("utf-8")) > MAX_IDENTITY_BYTES
    ):
        raise ThisSessionError(f"session_context_invalid:{name}")
    return value


def _namespace(manager: str, endpoint: object) -> str:
    encoded = json.dumps(endpoint, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(
        f"agent-workstream-terminal-binding-v1\0{manager}\0{encoded}".encode()
    ).hexdigest()


def state_path(environ: Mapping[str, str]) -> Path:
    override = environ.get("WORKSTREAM_SESSION_BINDING_DB")
    return (
        Path(override).expanduser()
        if override else Path.home() / ".local/state/agent-workstream/session-bindings.sqlite3"
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _terminal_identity(environ: Mapping[str, str]) -> dict[str, str] | None:
    herdr_present = environ.get("HERDR_ENV") == "1" or any(
        environ.get(key) for key in (
            "HERDR_TAB_ID", "HERDR_WORKSPACE_ID", "HERDR_SOCKET_PATH",
        )
    )
    cmux_present = any(environ.get(key) for key in (
        "CMUX_SURFACE_ID", "CMUX_WORKSPACE_ID", "CMUX_SOCKET_PATH",
    ))
    if herdr_present and cmux_present:
        raise ThisSessionError("session_context_ambiguous")
    if herdr_present:
        target = _identity(environ.get("HERDR_TAB_ID"), "HERDR_TAB_ID")
        workspace = _identity(
            environ.get("HERDR_WORKSPACE_ID"), "HERDR_WORKSPACE_ID",
        )
        endpoint = _identity(
            environ.get("HERDR_SOCKET_PATH"), "HERDR_SOCKET_PATH",
        )
        return {
            "manager": "herdr", "target_id": target,
            "workspace_id": workspace,
            "namespace_sha256": _namespace("herdr", {"socket_path": endpoint}),
        }
    if cmux_present:
        target = _identity(environ.get("CMUX_SURFACE_ID"), "CMUX_SURFACE_ID")
        workspace = _identity(
            environ.get("CMUX_WORKSPACE_ID"), "CMUX_WORKSPACE_ID",
        )
        endpoint = _identity(
            environ.get("CMUX_SOCKET_PATH"), "CMUX_SOCKET_PATH",
        )
        return {
            "manager": "cmux", "target_id": target,
            "workspace_id": workspace, "cmux_socket_path": endpoint,
        }
    return None


def _run_json(
    runner: Runner, argv: Sequence[str], *, environment: Mapping[str, str],
    reason: str,
) -> dict[str, Any]:
    try:
        result = runner(
            list(argv), stdin=subprocess.DEVNULL, capture_output=True, text=True,
            timeout=TERMINAL_TIMEOUT_SECONDS, check=False, env=dict(environment),
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ThisSessionError(reason) from error
    if result.returncode != 0:
        raise ThisSessionError(reason)
    try:
        value = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError) as error:
        raise ThisSessionError(reason) from error
    if not isinstance(value, dict):
        raise ThisSessionError(reason)
    return value


def _cmux_binary(
    environ: Mapping[str, str], which: Callable[[str], str | None],
) -> str:
    injected = environ.get("CMUX_BUNDLED_CLI_PATH")
    cmux = injected if injected and os.path.isabs(injected) else which("cmux")
    if not cmux:
        raise ThisSessionError("session_context_unavailable:cmux_cli")
    return cmux


def _cmux_server_provenance(value: dict[str, Any]) -> dict[str, str]:
    result = {}
    for field in ("socket_path", "bundle_identifier", "app_bundle_path"):
        result[field] = _identity(value.get(field), f"cmux_{field}")
    return result


def _default_socket_candidates() -> list[Path]:
    candidates = list((Path.home() / ".local/state/cmux").glob("*.sock"))
    candidates.extend(Path("/tmp").glob("cmux*.sock"))
    return sorted(set(candidates), key=str)[:32]


def _discover_cmux_instance(
    cmux: str, environ: Mapping[str, str], runner: Runner,
    socket_candidates: Sequence[Path] | None,
) -> tuple[list[str], dict[str, str]]:
    explicit = environ.get("CMUX_SOCKET_PATH")
    if explicit:
        prefix = [cmux, "--socket", _identity(explicit, "CMUX_SOCKET_PATH")]
        value = _run_json(
            runner, [*prefix, "identify", "--no-caller", "--json"],
            environment=environ, reason="session_context_unavailable:cmux_instance",
        )
        provenance = _cmux_server_provenance(value)
        if provenance["socket_path"] != explicit:
            raise ThisSessionError("session_context_mismatch:cmux_socket")
        return prefix, provenance
    if environ.get("CMUX_TAG"):
        raise ThisSessionError("session_context_unavailable:cmux_tag_requires_socket")
    value = _run_json(
        runner, [cmux, "identify", "--no-caller", "--json"],
        environment=environ, reason="session_context_unavailable:cmux_instance",
    )
    provenance = _cmux_server_provenance(value)
    live = set()
    candidates = list(
        _default_socket_candidates()
        if socket_candidates is None else socket_candidates
    )
    discovered = Path(provenance["socket_path"])
    if discovered not in candidates:
        candidates.append(discovered)
    for candidate in candidates:
        try:
            observed = _run_json(
                runner, [cmux, "--socket", str(candidate), "identify",
                         "--no-caller", "--json"], environment=environ,
                reason="cmux_socket_unreachable",
            )
            live.add(_cmux_server_provenance(observed)["socket_path"])
        except ThisSessionError:
            continue
    if live and live != {provenance["socket_path"]}:
        raise ThisSessionError("session_context_ambiguous:multiple_cmux_instances")
    return [cmux, "--socket", provenance["socket_path"]], provenance


def _bounded_ancestor_pids() -> list[int]:
    result = []
    current = os.getppid()
    for _ in range(8):
        if current <= 1 or current in result:
            break
        result.append(current)
        observed = subprocess.run(
            ["ps", "-o", "ppid=", "-p", str(current)],
            stdin=subprocess.DEVNULL, capture_output=True, text=True,
            timeout=2, check=False,
        )
        try:
            current = int(observed.stdout.strip()) if observed.returncode == 0 else 0
        except ValueError:
            break
    return result


def _resolve_cmux_ancestor_identity(
    prefix: Sequence[str], provenance: dict[str, str],
    environ: Mapping[str, str], runner: Runner, pid_chain: Sequence[int] | None,
) -> dict[str, str]:
    pairs = set()
    pids = _bounded_ancestor_pids() if pid_chain is None else pid_chain
    for pid in pids:
        try:
            value = _run_json(
                runner, [*prefix, "rpc", "agent.resolve_delivery_target",
                         json.dumps({"pid": pid,
                                     "pid_resolution": "controlling_tty"},
                                    separators=(",", ":"))],
                environment=environ, reason="session_pid_unresolved",
            )
        except ThisSessionError:
            continue
        workspace = value.get("workspace_id")
        surface = value.get("surface_id")
        if isinstance(workspace, str) and workspace and isinstance(surface, str) and surface:
            pairs.add((workspace, surface))
    if len(pairs) != 1:
        reason = "session_context_unavailable" if not pairs else "session_context_ambiguous"
        raise ThisSessionError(f"{reason}:cmux_ancestor_target")
    workspace, surface = next(iter(pairs))
    return {
        "manager": "cmux", "workspace_id": workspace, "target_id": surface,
        "cmux_socket_path": provenance["socket_path"],
    }


def _cmux_title(
    identity: dict[str, str], environ: Mapping[str, str], runner: Runner,
    which: Callable[[str], str | None],
    *, prefix: Sequence[str] | None = None,
) -> str:
    cmux = _cmux_binary(environ, which)
    if prefix is None:
        prefix = [cmux, "--socket", identity["cmux_socket_path"]]
    value = _run_json(
        runner, [*prefix, "identify", "--workspace", identity["workspace_id"],
                 "--surface", identity["target_id"], "--json"],
        environment=environ, reason="session_target_unresolved:cmux",
    )
    caller = value.get("caller")
    if not isinstance(caller, dict):
        raise ThisSessionError("session_target_unresolved:cmux")
    workspace_value = _run_json(
        runner, [*prefix, "rpc", "workspace.list", "{}"],
        environment=environ, reason="session_target_unresolved:cmux",
    )
    workspaces = workspace_value.get("workspaces")
    if not isinstance(workspaces, list):
        raise ThisSessionError("session_target_unresolved:cmux")
    workspace_matches = [
        row for row in workspaces if isinstance(row, dict)
        and (
            row.get("id") == identity["workspace_id"]
            or row.get("ref") == identity["workspace_id"]
        )
    ]
    if (
        len(workspace_matches) != 1
        or not isinstance(workspace_matches[0].get("id"), str)
        or not isinstance(workspace_matches[0].get("ref"), str)
    ):
        raise ThisSessionError("session_target_identity_mismatch:cmux")
    workspace_id = workspace_matches[0]["id"]
    workspace_ref = workspace_matches[0]["ref"]
    surface_value = _run_json(
        runner, [*prefix, "rpc", "surface.list", json.dumps({
            "workspace_id": workspace_id,
        }, separators=(",", ":"))], environment=environ,
        reason="session_target_unresolved:cmux",
    )
    surfaces = surface_value.get("surfaces")
    if not isinstance(surfaces, list):
        raise ThisSessionError("session_target_unresolved:cmux")
    matches = [
        row for row in surfaces if isinstance(row, dict)
        and (
            row.get("id") == identity["target_id"]
            or row.get("ref") == identity["target_id"]
        )
    ]
    if (
        len(matches) != 1 or not isinstance(matches[0].get("id"), str)
        or not isinstance(matches[0].get("ref"), str)
        or not isinstance(matches[0].get("pane_id"), str)
        or not isinstance(matches[0].get("pane_ref"), str)
        or not isinstance(matches[0].get("title"), str)
    ):
        raise ThisSessionError("session_target_identity_mismatch:cmux")
    expected_surface = {matches[0]["id"], matches[0]["ref"]}
    expected_workspace = {workspace_id, workspace_ref}
    caller_surface = caller.get("surface_id") or caller.get("surface_ref")
    caller_workspace = caller.get("workspace_id") or caller.get("workspace_ref")
    caller_pane = caller.get("pane_id") or caller.get("pane_ref")
    expected_pane = {matches[0]["pane_id"], matches[0]["pane_ref"]}
    if (
        caller_surface not in expected_surface
        or caller_workspace not in expected_workspace
        or caller_pane not in expected_pane
    ):
        raise ThisSessionError("session_target_identity_mismatch:cmux")
    identity["workspace_id"] = workspace_id
    identity["target_id"] = matches[0]["id"]
    return matches[0]["title"]


def _herdr_title(
    identity: dict[str, str], environ: Mapping[str, str], runner: Runner,
    which: Callable[[str], str | None],
) -> str:
    injected = environ.get("HERDR_BIN_PATH")
    herdr = injected if injected and os.path.isabs(injected) else which("herdr")
    if not herdr:
        raise ThisSessionError("session_context_unavailable:herdr_cli")
    value = _run_json(
        runner, [herdr, "tab", "get", identity["target_id"]],
        environment=environ, reason="session_target_unresolved:herdr",
    )
    response = value.get("result")
    tab = response.get("tab") if isinstance(response, dict) else None
    if (
        not isinstance(tab, dict)
        or tab.get("tab_id") != identity["target_id"]
        or tab.get("workspace_id") != identity["workspace_id"]
        or not isinstance(tab.get("label"), str)
    ):
        raise ThisSessionError("session_target_identity_mismatch:herdr")
    return tab["label"]


def _read_binding(path: Path, identity: dict[str, str]) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    connection = None
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        row = connection.execute(
            "SELECT workstream_id,provider,provider_session_id,current_event_id,"
            "updated_at FROM terminal_bindings_v1 WHERE manager=? AND namespace_sha256=? "
            "AND workspace_id=? AND target_id=?",
            (identity["manager"], identity["namespace_sha256"],
             identity["workspace_id"], identity["target_id"]),
        ).fetchone()
    except sqlite3.OperationalError as error:
        if "no such table: terminal_bindings_v1" in str(error):
            return None
        raise ThisSessionError("session_binding_store_unreadable") from error
    except sqlite3.Error as error:
        raise ThisSessionError("session_binding_store_unreadable") from error
    finally:
        if connection is not None:
            connection.close()
    if row is None:
        return None
    return {
        "workstream_id": row[0], "provider": row[1],
        "provider_session_id": row[2], "event_id": row[3],
        "updated_at": row[4],
    }


def resolve_this_session(
    *, environ: Mapping[str, str] = os.environ,
    runner: Runner = subprocess.run,
    which: Callable[[str], str | None] = shutil.which,
    binding_path: Path | None = None,
    pid_chain: Sequence[int] | None = None,
    socket_candidates: Sequence[Path] | None = None,
) -> dict[str, Any]:
    """Pure resolution: exact terminal identity plus binding/title, never focus."""
    identity = _terminal_identity(environ)
    cmux_prefix = None
    if identity is None:
        cmux = _cmux_binary(environ, which)
        cmux_prefix, provenance = _discover_cmux_instance(
            cmux, environ, runner, socket_candidates,
        )
        identity = _resolve_cmux_ancestor_identity(
            cmux_prefix, provenance, environ, runner, pid_chain,
        )
    if identity["manager"] == "cmux":
        if cmux_prefix is None:
            cmux = _cmux_binary(environ, which)
            cmux_prefix, provenance = _discover_cmux_instance(
                cmux, environ, runner, socket_candidates,
            )
        identity["namespace_sha256"] = _namespace("cmux", provenance)
    title = (
        _herdr_title(identity, environ, runner, which)
        if identity["manager"] == "herdr"
        else _cmux_title(
            identity, environ, runner, which, prefix=cmux_prefix,
        )
    )
    title_tokens = workstream_tab.tokens_in_title(title)
    if len(title_tokens) > 1:
        raise ThisSessionError("session_title_workstream_ambiguous")
    binding = _read_binding(binding_path or state_path(environ), identity)
    bound_token = binding.get("workstream_id") if binding else None
    title_token = title_tokens[0] if title_tokens else None
    if bound_token is not None:
        bound_token = workstream_tab.canonical_token(bound_token)
        if title_token is not None and title_token != bound_token:
            raise ThisSessionError("session_binding_title_mismatch")
        token = bound_token
        source = "binding" if title_token is None else "binding_and_title"
    elif title_token is not None:
        token = title_token
        source = "title"
    else:
        raise ThisSessionError(
            "session_workstream_unresolved:provide an explicit workstream token"
        )
    return {
        **identity, "workstream_id": token, "candidate_source": source,
        "observed_title": title, "prior_binding": binding,
    }


def _provider_session(environ: Mapping[str, str]) -> tuple[str | None, str | None]:
    candidates = [
        ("codex", environ.get("CODEX_SESSION_ID")),
        ("claude", environ.get("CLAUDE_SESSION_ID")),
    ]
    present = [(provider, value) for provider, value in candidates if value]
    if len(present) != 1:
        return None, None
    return present[0][0], _identity(present[0][1], f"{present[0][0]}_session_id")


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    connection = sqlite3.connect(path)
    os.chmod(path, 0o600)
    connection.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS terminal_binding_events_v1 (
            event_id TEXT PRIMARY KEY,
            manager TEXT NOT NULL, namespace_sha256 TEXT NOT NULL,
            workspace_id TEXT NOT NULL, target_id TEXT NOT NULL,
            workstream_id TEXT NOT NULL, provider TEXT,
            provider_session_id TEXT, predecessor_event_id TEXT,
            predecessor_provider_session_id TEXT, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS terminal_bindings_v1 (
            manager TEXT NOT NULL, namespace_sha256 TEXT NOT NULL,
            workspace_id TEXT NOT NULL, target_id TEXT NOT NULL,
            workstream_id TEXT NOT NULL, provider TEXT,
            provider_session_id TEXT, current_event_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (manager,namespace_sha256,workspace_id,target_id)
        );
    """)
    return connection


def record_successor_binding(
    path: Path, resolution: dict[str, Any], *, environ: Mapping[str, str],
    created_at: str,
) -> dict[str, Any]:
    provider, session_id = _provider_session(environ)
    created_at = _identity(created_at, "created_at")
    key = tuple(resolution[field] for field in (
        "manager", "namespace_sha256", "workspace_id", "target_id",
    ))
    token = workstream_tab.canonical_token(resolution["workstream_id"])
    connection = _connect(path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        prior = connection.execute(
            "SELECT workstream_id,provider,provider_session_id,current_event_id "
            "FROM terminal_bindings_v1 WHERE manager=? AND namespace_sha256=? "
            "AND workspace_id=? AND target_id=?", key,
        ).fetchone()
        if prior is not None and prior[0] != token:
            raise ThisSessionError("session_binding_conflict")
        if prior is not None and prior[1:3] == (provider, session_id):
            if connection.execute(
                "SELECT count(*) FROM terminal_binding_events_v1 WHERE event_id=?",
                (prior[3],),
            ).fetchone()[0] != 1:
                raise ThisSessionError("session_binding_history_incomplete")
            connection.commit()
            return {
                "status": "unchanged", "event_id": prior[3],
                "provider": provider, "provider_session_id": session_id,
                "predecessor_provider_session_id": None, "writes_performed": 0,
            }
        event_payload = {
            "schema_version": SCHEMA_VERSION, "manager": key[0],
            "namespace_sha256": key[1], "workspace_id": key[2],
            "target_id": key[3], "workstream_id": token,
            "provider": provider, "provider_session_id": session_id,
            "predecessor_event_id": prior[3] if prior else None,
            "predecessor_provider_session_id": prior[2] if prior else None,
            "created_at": created_at,
        }
        event_id = "wsb_" + hashlib.sha256(json.dumps(
            event_payload, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()[:32]
        connection.execute(
            "INSERT OR IGNORE INTO terminal_binding_events_v1 VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (event_id, *key, token, provider, session_id,
             event_payload["predecessor_event_id"],
             event_payload["predecessor_provider_session_id"], created_at),
        )
        observed_event = connection.execute(
            "SELECT manager,namespace_sha256,workspace_id,target_id,workstream_id,"
            "provider,provider_session_id,predecessor_event_id,"
            "predecessor_provider_session_id,created_at "
            "FROM terminal_binding_events_v1 WHERE event_id=?", (event_id,),
        ).fetchone()
        expected_event = (
            *key, token, provider, session_id,
            event_payload["predecessor_event_id"],
            event_payload["predecessor_provider_session_id"], created_at,
        )
        if observed_event != expected_event:
            raise ThisSessionError("session_binding_event_collision")
        connection.execute(
            "INSERT INTO terminal_bindings_v1 VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(manager,namespace_sha256,workspace_id,target_id) DO UPDATE SET "
            "workstream_id=excluded.workstream_id,provider=excluded.provider,"
            "provider_session_id=excluded.provider_session_id,"
            "current_event_id=excluded.current_event_id,updated_at=excluded.updated_at",
            (*key, token, provider, session_id, event_id, created_at),
        )
        connection.commit()
        return {
            "status": "bound", "event_id": event_id, "provider": provider,
            "provider_session_id": session_id,
            "predecessor_provider_session_id": event_payload[
                "predecessor_provider_session_id"
            ], "writes_performed": 1,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def resume_this_session(
    *, environ: Mapping[str, str] = os.environ,
    runner: Runner = subprocess.run,
    terminal_runner: Runner | None = None,
    which: Callable[[str], str | None] = shutil.which,
    binding_path: Path | None = None,
    resume_script: Path | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    terminal_runner = terminal_runner or runner
    path = binding_path or state_path(environ)
    resolution = resolve_this_session(
        environ=environ, runner=terminal_runner, which=which, binding_path=path,
    )
    final_resolution = resolve_this_session(
        environ=environ, runner=terminal_runner, which=which, binding_path=path,
    )
    fenced_fields = (
        "manager", "namespace_sha256", "workspace_id", "target_id",
        "workstream_id", "candidate_source", "observed_title", "prior_binding",
    )
    if any(
        final_resolution.get(field) != resolution.get(field)
        for field in fenced_fields
    ):
        raise ThisSessionError("session_context_changed")
    resolution = final_resolution
    script = resume_script or Path(__file__).with_name("workstream_resume.py")
    try:
        resumed = runner(
            [sys.executable, str(script), resolution["workstream_id"]],
            stdin=subprocess.DEVNULL, capture_output=True, text=True,
            timeout=RESUME_TIMEOUT_SECONDS, check=False, env=dict(environ),
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ThisSessionError("workstream_resume_failed") from error
    if resumed.returncode != 0:
        raise ThisSessionError(f"workstream_resume_refused:{resumed.returncode}")
    try:
        context = json.loads(resumed.stdout)
    except (json.JSONDecodeError, TypeError) as error:
        raise ThisSessionError("workstream_resume_invalid_output") from error
    if not isinstance(context, dict) or context.get("resume_authority") != "full":
        raise ThisSessionError("workstream_resume_authority_not_full")
    post_resolution = resolve_this_session(
        environ=environ, runner=terminal_runner, which=which, binding_path=path,
    )
    if any(
        post_resolution.get(field) != resolution.get(field)
        for field in fenced_fields
    ):
        raise ThisSessionError("session_context_changed")
    resolution = post_resolution
    try:
        binding = record_successor_binding(
            path, resolution, environ=environ, created_at=created_at or utc_now(),
        )
    except (OSError, sqlite3.Error) as error:
        binding = {"status": "unavailable", "reason": type(error).__name__}
    try:
        adapter_environ = dict(environ)
        target = None
        if resolution["manager"] == "cmux":
            adapter_environ["CMUX_SOCKET_PATH"] = resolution[
                "cmux_socket_path"
            ]
            target = resolution["target_id"]
        tab_result = workstream_tab.apply_title(
            resolution["workstream_id"],
            target=target, project_name=context.get("project_name"),
            environ=adapter_environ,
            runner=terminal_runner, which=which,
        )
    except workstream_tab.TabTitleError as error:
        tab_result = {"status": "unavailable", "reason": str(error)}
    context["this_session_resolution"] = {
        key: resolution[key] for key in (
            "manager", "namespace_sha256", "workspace_id", "target_id",
            "workstream_id", "candidate_source",
        )
    }
    context["resume_binding"] = binding
    context["tab_binding"] = tab_result
    return context


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    try:
        result = resume_this_session(
            created_at=os.environ.get("WORKSTREAM_BINDING_CREATED_AT")
        )
    except (ThisSessionError, workstream_tab.TabTitleError) as error:
        print(f"workstream-this-session: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
