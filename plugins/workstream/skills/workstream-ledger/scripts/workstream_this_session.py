#!/usr/bin/env python3
"""Resolve and resume the one workstream bound to this exact terminal surface."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
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
MAX_BINDING_CHAIN_EVENTS = 4096
MAX_RESUME_REFUSAL_REASON_BYTES = 512
RESUME_REFUSAL_PREFIX = "workstream resume refused: "
RESUME_REFUSAL_REASON = re.compile(r"[A-Za-z0-9_.:,><=+-]+")


class ThisSessionError(RuntimeError):
    pass


Runner = Callable[..., subprocess.CompletedProcess[str]]


def _resume_refusal_reason(stderr: object) -> str | None:
    """Expose one bounded machine reason without forwarding arbitrary stderr."""
    if not isinstance(stderr, str):
        return None
    lines = stderr.splitlines()
    if len(lines) != 1 or not lines[0].startswith(RESUME_REFUSAL_PREFIX):
        return None
    reason = lines[0][len(RESUME_REFUSAL_PREFIX):]
    if (
        not reason or not RESUME_REFUSAL_REASON.fullmatch(reason)
        or len(reason.encode("utf-8")) > MAX_RESUME_REFUSAL_REASON_BYTES
    ):
        return None
    return reason


def _identity(value: object, name: str) -> str:
    if (
        not isinstance(value, str) or not value or value != value.strip()
        or "\x00" in value or len(value.encode("utf-8")) > MAX_IDENTITY_BYTES
    ):
        raise ThisSessionError(f"session_context_invalid:{name}")
    return value


def _absolute_socket_path(value: object, name: str) -> str:
    """Return one canonical absolute socket path, never a cwd-relative key."""
    path = _identity(value, name)
    if not os.path.isabs(path) or os.path.normpath(path) != path:
        raise ThisSessionError(f"session_context_invalid:{name}")
    return path


def _namespace(manager: str, endpoint: object) -> str:
    if not isinstance(endpoint, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in endpoint.items()
    ):
        raise ThisSessionError("session_binding_provenance_mismatch")
    return workstream_tab.terminal_namespace_sha256(manager, endpoint)


def state_path(environ: Mapping[str, str]) -> Path:
    override = environ.get("WORKSTREAM_SESSION_BINDING_DB")
    return (
        Path(override).expanduser()
        if override else Path.home() / ".local/state/agent-workstream/session-bindings.sqlite3"
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _binding_timestamp(value: object, name: str) -> datetime:
    raw = _identity(value, name)
    if not raw.endswith("Z"):
        raise ThisSessionError(f"session_binding_history_invalid:{name}")
    try:
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00")
    except ValueError as error:
        raise ThisSessionError(
            f"session_binding_history_invalid:{name}"
        ) from error
    if parsed.tzinfo != timezone.utc:
        raise ThisSessionError(f"session_binding_history_invalid:{name}")
    return parsed


def _terminal_identity(environ: Mapping[str, str]) -> dict[str, Any] | None:
    try:
        manager = workstream_tab.terminal_manager(environ)
    except workstream_tab.TabTitleError as error:
        if str(error) == "herdr_environment_flag_required":
            raise ThisSessionError(
                "session_context_invalid:HERDR_ENV"
            ) from error
        raise ThisSessionError("session_context_ambiguous") from error
    if manager == "herdr":
        target = _identity(environ.get("HERDR_TAB_ID"), "HERDR_TAB_ID")
        workspace = _identity(
            environ.get("HERDR_WORKSPACE_ID"), "HERDR_WORKSPACE_ID",
        )
        endpoint = _absolute_socket_path(
            environ.get("HERDR_SOCKET_PATH"), "HERDR_SOCKET_PATH",
        )
        provenance = {"socket_path": endpoint}
        return {
            "manager": "herdr", "target_id": target,
            "workspace_id": workspace,
            "terminal_provenance": provenance,
            "namespace_sha256": _namespace("herdr", provenance),
        }
    if manager == "cmux":
        target = _identity(environ.get("CMUX_SURFACE_ID"), "CMUX_SURFACE_ID")
        workspace = _identity(
            environ.get("CMUX_WORKSPACE_ID"), "CMUX_WORKSPACE_ID",
        )
        endpoint = _absolute_socket_path(
            environ.get("CMUX_SOCKET_PATH"), "CMUX_SOCKET_PATH",
        )
        return {
            "manager": "cmux", "target_id": target,
            "workspace_id": workspace, "cmux_socket_path": endpoint,
            "terminal_provenance": {"socket_path": endpoint},
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
    return {
        "socket_path": _absolute_socket_path(
            value.get("socket_path"), "cmux_socket_path",
        ),
        "bundle_identifier": _identity(
            value.get("bundle_identifier"), "cmux_bundle_identifier",
        ),
        "app_bundle_path": _identity(
            value.get("app_bundle_path"), "cmux_app_bundle_path",
        ),
    }


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
        explicit = _absolute_socket_path(explicit, "CMUX_SOCKET_PATH")
        prefix = [cmux, "--socket", explicit]
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
) -> dict[str, Any]:
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
        "terminal_provenance": dict(provenance),
        "namespace_sha256": _namespace("cmux", provenance),
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
    if _cmux_server_provenance(value) != identity["terminal_provenance"]:
        raise ThisSessionError("session_context_mismatch:cmux_provenance")
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


def _cmux_target_identity(
    identity: dict[str, Any], environ: Mapping[str, str], runner: Runner,
    prefix: Sequence[str],
) -> None:
    """Authenticate the requested caller target without reading its title."""
    value = _run_json(
        runner, [*prefix, "identify", "--workspace", identity["workspace_id"],
                 "--surface", identity["target_id"], "--json"],
        environment=environ, reason="session_target_unresolved:cmux",
    )
    if _cmux_server_provenance(value) != identity["terminal_provenance"]:
        raise ThisSessionError("session_context_mismatch:cmux_provenance")
    caller = value.get("caller")
    if not isinstance(caller, dict):
        raise ThisSessionError("session_target_unresolved:cmux")

    def aliases(*fields: str) -> set[str]:
        return {
            caller[field] for field in fields
            if isinstance(caller.get(field), str) and caller[field]
        }

    requested_workspace = identity["workspace_id"]
    caller_workspaces = aliases("workspace_id", "workspace_ref")
    workspace_value = _run_json(
        runner, [*prefix, "rpc", "workspace.list", "{}"],
        environment=environ, reason="session_target_unresolved:cmux",
    )
    workspaces = workspace_value.get("workspaces")
    if not isinstance(workspaces, list) or not caller_workspaces:
        raise ThisSessionError("session_target_identity_mismatch:cmux")
    workspace_matches = []
    for row in workspaces:
        if not isinstance(row, dict):
            continue
        row_aliases = {row.get("id"), row.get("ref")}
        if (
            requested_workspace in row_aliases
            and caller_workspaces <= row_aliases
            and all(isinstance(row.get(field), str) and row[field]
                    for field in ("id", "ref"))
        ):
            workspace_matches.append(row)
    if len(workspace_matches) != 1:
        raise ThisSessionError("session_target_identity_mismatch:cmux")
    workspace_id = workspace_matches[0]["id"]

    requested_target = identity["target_id"]
    caller_surfaces = aliases("surface_id", "surface_ref")
    caller_panes = aliases("pane_id", "pane_ref")
    surface_value = _run_json(
        runner, [*prefix, "rpc", "surface.list", json.dumps({
            "workspace_id": workspace_id,
        }, separators=(",", ":"))], environment=environ,
        reason="session_target_unresolved:cmux",
    )
    surfaces = surface_value.get("surfaces")
    if not isinstance(surfaces, list) or not caller_surfaces or not caller_panes:
        raise ThisSessionError("session_target_identity_mismatch:cmux")
    surface_matches = []
    for row in surfaces:
        if not isinstance(row, dict):
            continue
        row_aliases = {row.get("id"), row.get("ref")}
        pane_aliases = {row.get("pane_id"), row.get("pane_ref")}
        if (
            requested_target in row_aliases
            and caller_surfaces <= row_aliases
            and caller_panes <= pane_aliases
            and all(isinstance(row.get(field), str) and row[field]
                    for field in ("id", "ref", "pane_id", "pane_ref"))
        ):
            surface_matches.append(row)
    if len(surface_matches) != 1:
        raise ThisSessionError("session_target_identity_mismatch:cmux")
    identity["target_id"] = surface_matches[0]["id"]
    identity["workspace_id"] = workspace_id


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


def _validate_binding_identity(identity: dict[str, Any]) -> None:
    """Prove a binding key is derived from the recorded terminal provenance."""
    manager = identity.get("manager")
    expected_fields = (
        {"socket_path", "bundle_identifier", "app_bundle_path"}
        if manager == "cmux" else {"socket_path"} if manager == "herdr" else None
    )
    provenance = identity.get("terminal_provenance")
    try:
        if expected_fields is None or not isinstance(provenance, dict):
            raise ThisSessionError("session_binding_provenance_mismatch")
        if set(provenance) != expected_fields:
            raise ThisSessionError("session_binding_provenance_mismatch")
        _absolute_socket_path(
            provenance["socket_path"], f"{manager}_socket_path",
        )
        for field in expected_fields - {"socket_path"}:
            _identity(provenance[field], f"{manager}_{field}")
        _identity(identity.get("workspace_id"), "binding_workspace_id")
        _identity(identity.get("target_id"), "binding_target_id")
        namespace = _identity(
            identity.get("namespace_sha256"), "binding_namespace_sha256",
        )
    except (KeyError, ThisSessionError) as error:
        raise ThisSessionError("session_binding_provenance_mismatch") from error
    if (
        len(namespace) != 64
        or any(character not in "0123456789abcdef" for character in namespace)
        or namespace != _namespace(manager, provenance)
    ):
        raise ThisSessionError("session_binding_provenance_mismatch")


def _event_digest(event: tuple[Any, ...]) -> str:
    payload = {
        "schema_version": SCHEMA_VERSION, "manager": event[0],
        "namespace_sha256": event[1], "workspace_id": event[2],
        "target_id": event[3], "workstream_id": event[4],
        "provider": event[5], "provider_session_id": event[6],
        "predecessor_event_id": event[7],
        "predecessor_provider_session_id": event[8],
        "created_at": event[9],
    }
    return "wsb_" + hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()[:32]


def _validate_binding_event_chain(
    connection: sqlite3.Connection, *, current_event_id: str,
    identity: dict[str, Any], token: str, current_provider: str | None,
    current_session_id: str | None, current_updated_at: str,
) -> None:
    """Validate one bounded immutable predecessor chain back to genesis."""
    event_id = current_event_id
    seen: set[str] = set()
    successor_time: datetime | None = None
    expected_session: str | None = None
    check_expected_session = False
    for depth in range(MAX_BINDING_CHAIN_EVENTS):
        if event_id in seen:
            raise ThisSessionError("session_binding_history_cycle")
        seen.add(event_id)
        try:
            events = connection.execute(
                "SELECT manager,namespace_sha256,workspace_id,target_id,"
                "workstream_id,provider,provider_session_id,"
                "predecessor_event_id,predecessor_provider_session_id,created_at "
                "FROM terminal_binding_events_v1 WHERE event_id=?",
                (event_id,),
            ).fetchall()
        except sqlite3.OperationalError as error:
            if "no such table: terminal_binding_events_v1" in str(error):
                raise ThisSessionError(
                    "session_binding_history_incomplete"
                ) from error
            raise
        if not events:
            raise ThisSessionError("session_binding_history_incomplete")
        if len(events) != 1:
            raise ThisSessionError("session_binding_history_ambiguous")
        event = events[0]
        if event[:5] != (
            identity["manager"], identity["namespace_sha256"],
            identity["workspace_id"], identity["target_id"], token,
        ):
            raise ThisSessionError("session_binding_history_mismatch")
        provider, session_id = event[5:7]
        if (
            provider not in {None, "codex", "claude"}
            or (provider is None) != (session_id is None)
        ):
            raise ThisSessionError("session_binding_history_invalid")
        try:
            if session_id is not None:
                _identity(session_id, "binding_provider_session_id")
            event_time = _binding_timestamp(
                event[9], "binding_event_created_at",
            )
            if event[7] is not None:
                _identity(event[7], "binding_predecessor_event_id")
            if event[8] is not None:
                _identity(
                    event[8], "binding_predecessor_provider_session_id",
                )
        except ThisSessionError as error:
            raise ThisSessionError("session_binding_history_invalid") from error
        if depth == 0 and (
            provider != current_provider or session_id != current_session_id
            or event[9] != current_updated_at
        ):
            raise ThisSessionError("session_binding_history_mismatch")
        if check_expected_session and session_id != expected_session:
            raise ThisSessionError("session_binding_predecessor_session_mismatch")
        if successor_time is not None and event_time > successor_time:
            raise ThisSessionError("session_binding_history_chronology_mismatch")
        predecessor = event[7]
        # Anonymous ownership is valid only for the first binding on an
        # unowned terminal. Once a predecessor exists, losing the provider
        # session would erase successor provenance and must fail closed.
        if provider is None and predecessor is not None:
            raise ThisSessionError("session_binding_history_invalid")
        if predecessor in seen:
            raise ThisSessionError("session_binding_history_cycle")
        if event_id != _event_digest(event):
            raise ThisSessionError("session_binding_event_digest_mismatch")
        if predecessor is None:
            if event[8] is not None:
                raise ThisSessionError(
                    "session_binding_predecessor_session_mismatch"
                )
            return
        successor_time = event_time
        expected_session = event[8]
        check_expected_session = True
        event_id = predecessor
    raise ThisSessionError("session_binding_history_over_budget")


def _validated_binding_row(
    connection: sqlite3.Connection, row: tuple[Any, ...],
    identity: dict[str, Any],
) -> dict[str, Any]:
    try:
        token, provider, session_id, event_id, updated_at, namespace = row
        canonical_token = workstream_tab.canonical_token(token)
        event_id = _identity(event_id, "binding_event_id")
        _binding_timestamp(updated_at, "binding_updated_at")
        namespace = _identity(namespace, "binding_namespace_sha256")
        if (
            len(namespace) != 64
            or any(character not in "0123456789abcdef" for character in namespace)
            or namespace != identity.get("namespace_sha256")
        ):
            raise ThisSessionError("session_binding_history_invalid")
        if canonical_token != token or provider not in {None, "codex", "claude"}:
            raise ThisSessionError("session_binding_history_invalid")
        if (provider is None) != (session_id is None):
            raise ThisSessionError("session_binding_history_invalid")
        if session_id is not None:
            _identity(session_id, "binding_provider_session_id")
    except (
        AttributeError, TypeError, ValueError, ThisSessionError,
        workstream_tab.TabTitleError,
    ) as error:
        raise ThisSessionError("session_binding_history_invalid") from error
    _validate_binding_event_chain(
        connection, current_event_id=event_id, identity=identity, token=token,
        current_provider=provider, current_session_id=session_id,
        current_updated_at=updated_at,
    )
    return {
        "workstream_id": token, "provider": provider,
        "provider_session_id": session_id, "event_id": event_id,
        "updated_at": updated_at,
    }


def _read_binding(path: Path, identity: dict[str, Any]) -> dict[str, Any] | None:
    _validate_binding_identity(identity)
    if not path.is_file():
        return None
    connection = None
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        rows = connection.execute(
            "SELECT workstream_id,provider,provider_session_id,current_event_id,"
            "updated_at,namespace_sha256 FROM terminal_bindings_v1 "
            "WHERE manager=? AND namespace_sha256=? AND workspace_id=? "
            "AND target_id=?",
            (identity["manager"], identity["namespace_sha256"],
             identity["workspace_id"], identity["target_id"]),
        ).fetchall()
        if len(rows) > 1:
            raise ThisSessionError("session_binding_ambiguous")
        return (
            None if not rows
            else _validated_binding_row(connection, rows[0], identity)
        )
    except sqlite3.OperationalError as error:
        if "no such table: terminal_bindings_v1" in str(error):
            return None
        raise ThisSessionError("session_binding_store_unreadable") from error
    except sqlite3.Error as error:
        raise ThisSessionError("session_binding_store_unreadable") from error
    finally:
        if connection is not None:
            connection.close()


def resolve_this_session(
    *, environ: Mapping[str, str] = os.environ,
    runner: Runner = subprocess.run,
    which: Callable[[str], str | None] = shutil.which,
    binding_path: Path | None = None,
    pid_chain: Sequence[int] | None = None,
    socket_candidates: Sequence[Path] | None = None,
) -> dict[str, Any]:
    """Pure resolution: exact terminal identity plus binding/title, never focus."""
    path = binding_path or state_path(environ)
    identity = _terminal_identity(environ)
    cmux_prefix: Sequence[str] | None = None
    binding = None
    title = None
    adapter_error = None
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
        identity["terminal_provenance"] = dict(provenance)
        identity["namespace_sha256"] = _namespace("cmux", provenance)
        # Authenticate the exact server, workspace, surface, and caller before
        # consulting local state.  Title access is deliberately later so an
        # exact persisted binding can survive an optional title-adapter outage.
        _cmux_target_identity(identity, environ, runner, cmux_prefix)
        binding = _read_binding(path, identity)
        try:
            title = _cmux_title(
                identity, environ, runner, which, prefix=cmux_prefix,
            )
            normalized_binding = _read_binding(path, identity)
            if binding is not None and normalized_binding != binding:
                raise ThisSessionError("session_binding_changed")
            binding = normalized_binding
        except ThisSessionError as error:
            if str(error).startswith((
                "session_target_identity_mismatch",
                "session_context_mismatch",
                "session_binding_",
            )):
                raise
            if binding is None:
                raise ThisSessionError(
                    "session_workstream_unresolved:"
                    "terminal_adapter_unavailable:cmux"
                ) from error
            adapter_error = str(error)
    else:
        # HerdR's injected endpoint/workspace/tab tuple is its exact namespace;
        # the CLI title probe is optional once that tuple has a validated row.
        binding = _read_binding(path, identity)
        try:
            title = _herdr_title(identity, environ, runner, which)
        except ThisSessionError as error:
            if str(error).startswith("session_target_identity_mismatch"):
                raise
            if binding is None:
                raise ThisSessionError(
                    "session_workstream_unresolved:"
                    "terminal_adapter_unavailable:herdr"
                ) from error
            adapter_error = str(error)
    title_tokens = workstream_tab.tokens_in_title(title) if title is not None else []
    if len(title_tokens) > 1:
        raise ThisSessionError("session_title_workstream_ambiguous")
    title_token = (
        workstream_tab.canonical_title_token(title) if title is not None else None
    )
    if title_tokens and title_token is None:
        raise ThisSessionError("session_title_workstream_noncanonical")
    bound_token = binding.get("workstream_id") if binding else None
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
        "observed_title": title, "terminal_adapter_error": adapter_error,
        "prior_binding": binding,
    }


def _provider_session(environ: Mapping[str, str]) -> tuple[str | None, str | None]:
    candidates = [
        ("codex", environ.get("CODEX_SESSION_ID")),
        ("claude", environ.get("CLAUDE_SESSION_ID")),
    ]
    present = [(provider, value) for provider, value in candidates if value]
    if len(present) > 1:
        raise ThisSessionError("session_context_ambiguous:provider_session")
    if not present:
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
    created_at: str, validate_terminal: Callable[[], None] | None = None,
) -> dict[str, Any]:
    _validate_binding_identity(resolution)
    provider, session_id = _provider_session(environ)
    created_at = _identity(created_at, "created_at")
    created_time = _binding_timestamp(created_at, "created_at")
    key = tuple(resolution[field] for field in (
        "manager", "namespace_sha256", "workspace_id", "target_id",
    ))
    token = workstream_tab.canonical_token(resolution["workstream_id"])
    connection = _connect(path)
    try:
        connection.execute("BEGIN IMMEDIATE")

        prior_rows = connection.execute(
            "SELECT workstream_id,provider,provider_session_id,current_event_id,"
            "updated_at,namespace_sha256 "
            "FROM terminal_bindings_v1 WHERE manager=? AND namespace_sha256=? "
            "AND workspace_id=? AND target_id=?", key,
        ).fetchall()
        if len(prior_rows) > 1:
            raise ThisSessionError("session_binding_ambiguous")
        prior = (
            None if not prior_rows
            else _validated_binding_row(connection, prior_rows[0], resolution)
        )
        if prior is not None and prior["workstream_id"] != token:
            raise ThisSessionError("session_binding_conflict")
        if (
            prior is not None
            and prior["provider_session_id"] is not None
            and session_id is None
        ):
            if validate_terminal is not None:
                validate_terminal()
            connection.commit()
            return {
                "status": "unavailable",
                "reason": "provider_session_identity_unavailable",
                "event_id": prior["event_id"], "provider": provider,
                "provider_session_id": session_id,
                "preserved_provider": prior["provider"],
                "preserved_provider_session_id": prior[
                    "provider_session_id"
                ],
                "predecessor_provider_session_id": prior[
                    "provider_session_id"
                ],
                "writes_performed": 0,
            }
        if prior is not None and (
            prior["provider"], prior["provider_session_id"]
        ) == (provider, session_id):
            if validate_terminal is not None:
                # Keep the terminal readback inside the same immediate
                # transaction that decides whether this binding is current.
                validate_terminal()
            connection.commit()
            return {
                "status": "unchanged", "event_id": prior["event_id"],
                "provider": provider, "provider_session_id": session_id,
                "predecessor_provider_session_id": None,
                "writes_performed": 0,
            }
        if prior is not None and created_time < _binding_timestamp(
            prior["updated_at"], "binding_updated_at",
        ):
            raise ThisSessionError("session_binding_history_chronology_mismatch")
        event_payload = {
            "schema_version": SCHEMA_VERSION, "manager": key[0],
            "namespace_sha256": key[1], "workspace_id": key[2],
            "target_id": key[3], "workstream_id": token,
            "provider": provider, "provider_session_id": session_id,
            "predecessor_event_id": prior["event_id"] if prior else None,
            "predecessor_provider_session_id": (
                prior["provider_session_id"] if prior else None
            ),
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
        _validate_binding_event_chain(
            connection, current_event_id=event_id, identity=resolution,
            token=token, current_provider=provider,
            current_session_id=session_id, current_updated_at=created_at,
        )
        connection.execute(
            "INSERT INTO terminal_bindings_v1 VALUES (?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(manager,namespace_sha256,workspace_id,target_id) DO UPDATE SET "
            "workstream_id=excluded.workstream_id,provider=excluded.provider,"
            "provider_session_id=excluded.provider_session_id,"
            "current_event_id=excluded.current_event_id,updated_at=excluded.updated_at",
            (*key, token, provider, session_id, event_id, created_at),
        )
        if validate_terminal is not None:
            # A failed external readback rolls back both the staged event and
            # current-row update, so a title race cannot leave local authority.
            validate_terminal()
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


def validate_resumed_identity(
    context: dict[str, Any], candidate_token: str,
) -> None:
    """Bind a full root response to the exact requested root or owned child."""
    candidate = workstream_tab.canonical_token(candidate_token)
    raw_root = context.get("workstream_id")
    try:
        root = workstream_tab.canonical_token(raw_root)
    except (AttributeError, workstream_tab.TabTitleError) as error:
        raise ThisSessionError("workstream_resume_identity_mismatch") from error
    focus = context.get("requested_focus")
    if root == candidate:
        if raw_root != root or focus is not None:
            raise ThisSessionError("workstream_resume_identity_mismatch")
        return
    focus_fields = {
        "kind", "identifier", "issue_id", "parent_issue_id",
        "root_identifier", "repository_key", "status",
    }
    if (
        raw_root != root or not isinstance(focus, dict)
        or set(focus) != focus_fields or focus.get("kind") != "owned_child"
        or focus.get("identifier") != candidate
        or focus.get("root_identifier") != root
    ):
        raise ThisSessionError("workstream_resume_identity_mismatch")
    try:
        for field in focus_fields - {"kind"}:
            _identity(focus[field], f"requested_focus_{field}")
    except ThisSessionError as error:
        raise ThisSessionError("workstream_resume_identity_mismatch") from error


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
        "manager", "terminal_provenance", "namespace_sha256",
        "workspace_id", "target_id", "workstream_id", "candidate_source",
        "observed_title", "terminal_adapter_error", "prior_binding",
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
        reason = _resume_refusal_reason(resumed.stderr)
        if reason is not None:
            raise ThisSessionError(f"workstream_resume_refused:{reason}")
        raise ThisSessionError(f"workstream_resume_refused:{resumed.returncode}")
    try:
        context = json.loads(resumed.stdout)
    except (json.JSONDecodeError, TypeError) as error:
        raise ThisSessionError("workstream_resume_invalid_output") from error
    if not isinstance(context, dict) or context.get("resume_authority") != "full":
        raise ThisSessionError("workstream_resume_authority_not_full")
    validate_resumed_identity(context, resolution["workstream_id"])
    binding: dict[str, Any] = {
        "status": "unavailable", "reason": "terminal_binding_not_attempted",
    }
    tab_result: dict[str, Any] = {
        "status": "unavailable", "reason": "terminal_adapter_not_attempted",
    }
    try:
        post_resolution = resolve_this_session(
            environ=environ, runner=terminal_runner, which=which,
            binding_path=path,
        )
        if any(
            post_resolution.get(field) != resolution.get(field)
            for field in fenced_fields
        ):
            raise ThisSessionError("session_context_changed")
        resolution = post_resolution
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
            expected_title=resolution["observed_title"],
            expected_workspace=resolution["workspace_id"],
            expected_provenance=resolution["terminal_provenance"],
            environ=adapter_environ,
            runner=terminal_runner, which=which,
        )
        if tab_result.get("status") not in {"updated", "unchanged"}:
            binding = {
                "status": "unavailable",
                "reason": "terminal_adapter_unavailable",
            }
        else:
            bound_resolution = resolve_this_session(
                environ=environ, runner=terminal_runner, which=which,
                binding_path=path,
            )
            binding_fence_fields = (
                "manager", "terminal_provenance", "namespace_sha256",
                "workspace_id", "target_id", "workstream_id",
                "candidate_source", "observed_title",
                "terminal_adapter_error", "prior_binding",
            )
            if (
                bound_resolution.get("manager") != resolution.get("manager")
                or bound_resolution.get("terminal_provenance")
                != resolution.get("terminal_provenance")
                or bound_resolution.get("namespace_sha256")
                != resolution.get("namespace_sha256")
                or bound_resolution.get("workspace_id")
                != resolution.get("workspace_id")
                or bound_resolution.get("target_id")
                != resolution.get("target_id")
                or bound_resolution.get("workstream_id")
                != resolution.get("workstream_id")
                or bound_resolution.get("prior_binding")
                != resolution.get("prior_binding")
                or bound_resolution.get("observed_title")
                != tab_result.get("title")
            ):
                raise ThisSessionError("session_context_changed")

            def validate_terminal() -> None:
                observed = resolve_this_session(
                    environ=environ, runner=terminal_runner, which=which,
                    binding_path=path,
                )
                if any(
                    observed.get(field) != bound_resolution.get(field)
                    for field in binding_fence_fields
                ):
                    raise ThisSessionError("session_context_changed")

            binding = record_successor_binding(
                path, bound_resolution, environ=environ,
                created_at=created_at or utc_now(),
                validate_terminal=validate_terminal,
            )
    except workstream_tab.TabTitleError as error:
        tab_result = {"status": "unavailable", "reason": str(error)}
        binding = {
            "status": "unavailable", "reason": "terminal_title_unverified",
        }
    except ThisSessionError as error:
        tab_result = {"status": "unavailable", "reason": str(error)}
        binding = {"status": "unavailable", "reason": str(error)}
    except (OSError, sqlite3.Error) as error:
        binding = {"status": "unavailable", "reason": type(error).__name__}
    context["this_session_resolution"] = {
        key: resolution[key] for key in (
            "manager", "namespace_sha256", "workspace_id", "target_id",
            "workstream_id", "candidate_source",
        )
    }
    context["this_session_resolution"]["terminal_adapter_error"] = resolution.get(
        "terminal_adapter_error"
    )
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
