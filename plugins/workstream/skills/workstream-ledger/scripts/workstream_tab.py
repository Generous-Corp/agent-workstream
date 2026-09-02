#!/usr/bin/env python3
"""Safely carry one workstream token in an existing session-manager tab."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence


SEPARATOR = " · "
TOKEN = re.compile(r"^[A-Z][A-Z0-9]*-\d+$")
TOKEN_IN_TITLE = re.compile(r"(?<![A-Z0-9])([A-Z][A-Z0-9]*-\d+)(?![A-Z0-9])", re.I)
CANONICAL_TITLE = re.compile(
    rf"^(?P<label>.*\S){re.escape(SEPARATOR)}"
    r"(?P<token>[A-Z][A-Z0-9]*-\d+)$"
)
TIMEOUT_SECONDS = 3
MAX_PROJECT_LABEL_LENGTH = 120
MAX_ANCESTOR_DEPTH = 8


class TabTitleError(RuntimeError):
    pass


@dataclass(frozen=True)
class SurfaceContext:
    surface: str
    pane: str
    workspace: str
    window: str


Runner = Callable[..., subprocess.CompletedProcess[str]]


def canonical_token(value: str) -> str:
    token = value.strip().upper()
    if not TOKEN.fullmatch(token):
        raise TabTitleError("invalid_workstream_token")
    return token


def tokens_in_title(title: str) -> list[str]:
    return [match.upper() for match in TOKEN_IN_TITLE.findall(title)]


def canonical_title_token(title: str) -> str | None:
    """Return only an exact, uppercase final `` · TEAM-#`` suffix."""
    match = CANONICAL_TITLE.fullmatch(title)
    return match.group("token") if match else None


def terminal_namespace_sha256(manager: str, provenance: Mapping[str, str]) -> str:
    """Return the shared, manager-qualified terminal binding namespace."""
    encoded = json.dumps(provenance, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(
        f"agent-workstream-terminal-binding-v1\0{manager}\0{encoded}".encode()
    ).hexdigest()


def terminal_manager(environ: Mapping[str, str]) -> str | None:
    """Detect one terminal adapter from the same provenance fields everywhere."""
    herdr_enabled = environ.get("HERDR_ENV") == "1"
    # Treat every injected HerdR field as context for ambiguity detection, but
    # never grant adapter selection without HerdR's explicit environment flag.
    herdr_fields_present = any(
        key in environ for key in (
            "HERDR_TAB_ID", "HERDR_WORKSPACE_ID", "HERDR_SOCKET_PATH",
        )
    )
    herdr_present = "HERDR_ENV" in environ or herdr_fields_present
    cmux_present = any(environ.get(key) for key in (
        "CMUX_SURFACE_ID", "CMUX_WORKSPACE_ID", "CMUX_SOCKET_PATH",
    ))
    if herdr_present and cmux_present:
        raise TabTitleError("terminal_context_ambiguous")
    if herdr_fields_present and not herdr_enabled:
        raise TabTitleError("herdr_environment_flag_required")
    if herdr_enabled:
        return "herdr"
    if cmux_present:
        return "cmux"
    return None


def project_label(value: object) -> str:
    if value is None:
        raise TabTitleError("project_name_required_for_generated_title")
    if not isinstance(value, str):
        raise TabTitleError("invalid_project_name")
    if any(ord(character) < 32 for character in value):
        raise TabTitleError("invalid_project_name")
    label = " ".join(value.split())
    if (
        not label or len(label) > MAX_PROJECT_LABEL_LENGTH
        or tokens_in_title(label)
    ):
        raise TabTitleError("invalid_project_name")
    return label


def validate_transition(
    before: str, after: str, token: str, *, project_name: str | None = None,
    automatic_title: str | None = None,
) -> None:
    """Independently guard against overwriting a human-readable title."""
    if tokens_in_title(before):
        raise TabTitleError("title_already_contains_workstream_token")
    generated = not before.strip() or automatic_title is not None
    if automatic_title is not None and before != automatic_title:
        raise TabTitleError("automatic_title_changed")
    expected = (
        f"{project_label(project_name)}{SEPARATOR}{token}"
        if generated else f"{before.rstrip()}{SEPARATOR}{token}"
    )
    if after != expected:
        raise TabTitleError("existing_title_not_preserved")
    if (
        tokens_in_title(after) != [token]
        or canonical_title_token(after) != token
    ):
        raise TabTitleError("noncanonical_title_transition")


def plan_title(
    before: str, token: str, *, project_name: str | None = None,
    automatic_title: str | None = None,
) -> tuple[str, str]:
    found = tokens_in_title(before)
    if len(found) > 1:
        raise TabTitleError("duplicate_workstream_token")
    suffix_token = canonical_title_token(before)
    if found and suffix_token is None:
        raise TabTitleError("title_contains_noncanonical_workstream_token")
    if any(value != token for value in found):
        raise TabTitleError("workstream_tab_conflict")
    if suffix_token == token:
        return "unchanged", before
    generated = not before.strip() or automatic_title is not None
    after = (
        f"{project_label(project_name)}{SEPARATOR}{token}"
        if generated else f"{before.rstrip()}{SEPARATOR}{token}"
    )
    validate_transition(
        before, after, token, project_name=project_name,
        automatic_title=automatic_title,
    )
    return "updated", after


def _run(
    runner: Runner, argv: Sequence[str], *, allow_unavailable: bool = False,
    environment: Mapping[str, str] | None = None,
    command_error: str = "cmux_command_failed",
) -> subprocess.CompletedProcess[str] | None:
    try:
        options: dict[str, Any] = {
            "stdin": subprocess.DEVNULL, "capture_output": True, "text": True,
            "timeout": TIMEOUT_SECONDS, "check": False,
        }
        if environment is not None:
            options["env"] = dict(environment)
        result = runner(list(argv), **options)
    except (OSError, subprocess.TimeoutExpired) as error:
        if allow_unavailable:
            return None
        raise TabTitleError(command_error) from error
    if result.returncode != 0:
        if allow_unavailable:
            return None
        raise TabTitleError(command_error)
    return result


def _json_result(result: subprocess.CompletedProcess[str] | None, error: str) -> dict[str, Any]:
    if result is None:
        raise TabTitleError(error)
    try:
        value = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        raise TabTitleError(error) from exc
    if not isinstance(value, dict):
        raise TabTitleError(error)
    return value


def _surface_context(
    cmux: str, target: str, runner: Runner, *,
    environment: Mapping[str, str] | None = None,
    expected_workspace: str | None = None,
    expected_provenance: Mapping[str, str] | None = None,
) -> SurfaceContext | None:
    if _run(
        runner, [cmux, "ping"], allow_unavailable=True,
        environment=environment,
    ) is None:
        return None
    result = _run(
        runner, [cmux, "identify", "--surface", target, "--json"],
        allow_unavailable=True, environment=environment,
    )
    if result is None:
        raise TabTitleError("cmux_target_unresolved")
    value = _json_result(result, "invalid_cmux_identify_response")
    caller = value.get("caller")
    if not isinstance(caller, dict):
        raise TabTitleError("cmux_target_unresolved")
    if expected_provenance is not None:
        fields = ("socket_path", "bundle_identifier", "app_bundle_path")
        if (
            set(expected_provenance) != set(fields)
            or any(value.get(field) != expected_provenance[field]
                   for field in fields)
        ):
            raise TabTitleError("cmux_provenance_changed")

    def aliases(row: Mapping[str, Any], *names: str) -> set[str]:
        return {
            row[name] for name in names
            if isinstance(row.get(name), str) and row[name]
        }

    caller_workspaces = aliases(caller, "workspace_id", "workspace_ref")
    caller_surfaces = aliases(caller, "surface_id", "surface_ref")
    caller_panes = aliases(caller, "pane_id", "pane_ref")
    window_ref = caller.get("window_ref")
    if (
        not caller_workspaces or not caller_surfaces or not caller_panes
        or not isinstance(window_ref, str) or not window_ref
    ):
        raise TabTitleError("invalid_cmux_identify_response")

    workspace_result = _run(
        runner, [cmux, "rpc", "workspace.list", "{}"],
        allow_unavailable=True, environment=environment,
    )
    if workspace_result is None:
        raise TabTitleError("cmux_target_unresolved")
    workspace_value = _json_result(
        workspace_result, "invalid_cmux_workspace_response",
    )
    workspaces = workspace_value.get("workspaces")
    if not isinstance(workspaces, list):
        raise TabTitleError("invalid_cmux_workspace_response")
    workspace_matches = []
    for row in workspaces:
        if not isinstance(row, dict):
            continue
        row_aliases = aliases(row, "id", "ref")
        if (
            caller_workspaces <= row_aliases
            and all(isinstance(row.get(field), str) and row[field]
                    for field in ("id", "ref"))
        ):
            workspace_matches.append((row, row_aliases))
    if len(workspace_matches) != 1:
        raise TabTitleError(
            "cmux_workspace_changed"
            if expected_workspace is not None
            else "cmux_target_identity_mismatch"
        )
    workspace, workspace_aliases = workspace_matches[0]
    if (
        expected_workspace is not None
        and expected_workspace not in workspace_aliases
    ):
        raise TabTitleError("cmux_workspace_changed")

    surface_result = _run(
        runner, [cmux, "rpc", "surface.list", json.dumps({
            "workspace_id": workspace["id"],
        }, separators=(",", ":"))], allow_unavailable=True,
        environment=environment,
    )
    if surface_result is None:
        raise TabTitleError("cmux_target_unresolved")
    surface_value = _json_result(
        surface_result, "invalid_cmux_surface_response",
    )
    surfaces = surface_value.get("surfaces")
    if not isinstance(surfaces, list):
        raise TabTitleError("invalid_cmux_surface_response")
    surface_matches = []
    for row in surfaces:
        if not isinstance(row, dict):
            continue
        row_aliases = aliases(row, "id", "ref")
        pane_aliases = aliases(row, "pane_id", "pane_ref")
        if (
            target in row_aliases
            and caller_surfaces <= row_aliases
            and caller_panes <= pane_aliases
            and all(isinstance(row.get(field), str) and row[field]
                    for field in ("id", "ref", "pane_id", "pane_ref"))
        ):
            surface_matches.append(row)
    if len(surface_matches) != 1:
        raise TabTitleError("cmux_target_identity_mismatch")
    surface = surface_matches[0]
    return SurfaceContext(
        surface=surface["ref"], pane=surface["pane_ref"],
        workspace=workspace["ref"], window=window_ref,
    )


def _bounded_ancestor_pids() -> list[int]:
    pids: list[int] = []
    current = os.getppid()
    for _ in range(MAX_ANCESTOR_DEPTH):
        if current <= 1 or current in pids:
            break
        pids.append(current)
        try:
            observed = subprocess.run(
                ["ps", "-o", "ppid=", "-p", str(current)],
                stdin=subprocess.DEVNULL, capture_output=True, text=True,
                timeout=2, check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            break
        try:
            current = int(observed.stdout.strip()) if observed.returncode == 0 else 0
        except ValueError:
            break
    return pids


def _resolve_cmux_tty_target(
    cmux: str, runner: Runner, environ: Mapping[str, str],
) -> str:
    pairs: set[tuple[str, str]] = set()
    for pid in _bounded_ancestor_pids():
        result = _run(
            runner, [cmux, "rpc", "agent.resolve_delivery_target", json.dumps({
                "pid": pid, "pid_resolution": "controlling_tty",
            }, separators=(",", ":"))],
            environment=environ, allow_unavailable=True,
        )
        if result is None:
            continue
        try:
            value = json.loads(result.stdout)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(value, dict):
            continue
        workspace = value.get("workspace_id")
        surface = value.get("surface_id")
        if isinstance(workspace, str) and workspace and isinstance(surface, str) and surface:
            pairs.add((workspace, surface))
    if len(pairs) != 1:
        raise TabTitleError(
            "cmux_target_unresolved" if not pairs else "cmux_target_ambiguous"
        )
    return next(iter(pairs))[1]


def _read_title(
    cmux: str, context: SurfaceContext, runner: Runner, *,
    allow_unavailable: bool = False,
    environment: Mapping[str, str] | None = None,
) -> str | None:
    result = _run(runner, [
        cmux, "list-pane-surfaces", "--pane", context.pane,
        "--workspace", context.workspace, "--window", context.window, "--json",
    ], allow_unavailable=allow_unavailable, environment=environment)
    if result is None:
        return None
    value = _json_result(result, "invalid_cmux_surface_response")
    surfaces = value.get("surfaces")
    if not isinstance(surfaces, list):
        raise TabTitleError("invalid_cmux_surface_response")
    matches = [
        item for item in surfaces
        if isinstance(item, dict) and item.get("ref") == context.surface
    ]
    if not matches and allow_unavailable:
        return None
    if len(matches) != 1 or not isinstance(matches[0].get("title"), str):
        raise TabTitleError("cmux_target_title_unresolved")
    return matches[0]["title"]


def _adapter_title_plan(
    before: str, token: str, *, project_name: str | None,
    automatic_title: str | None,
) -> tuple[str, str | None, str | None]:
    try:
        status, title = plan_title(
            before, token, project_name=project_name,
            automatic_title=automatic_title,
        )
    except TabTitleError as error:
        reason = str(error)
        if reason in {
            "project_name_required_for_generated_title",
            "invalid_project_name",
            "automatic_title_changed",
        }:
            return "unavailable", None, reason
        raise
    return status, title, None


def _herdr_tab(
    herdr: str, tab_id: str, workspace_id: str, runner: Runner,
    environ: Mapping[str, str], *, allow_unavailable: bool,
) -> dict[str, Any] | None:
    result = _run(
        runner, [herdr, "tab", "get", tab_id],
        allow_unavailable=allow_unavailable, environment=environ,
        command_error="herdr_command_failed",
    )
    if result is None:
        return None
    value = _json_result(result, "invalid_herdr_tab_response")
    response = value.get("result")
    tab = response.get("tab") if isinstance(response, dict) else None
    if (
        not isinstance(response, dict)
        or response.get("type") != "tab_info"
        or not isinstance(tab, dict)
        or not isinstance(tab.get("label"), str)
    ):
        raise TabTitleError("invalid_herdr_tab_response")
    if tab.get("tab_id") != tab_id or tab.get("workspace_id") != workspace_id:
        if allow_unavailable:
            return None
        raise TabTitleError("herdr_title_readback_mismatch")
    return tab


def _apply_herdr_title(
    token: str, *, environ: Mapping[str, str], runner: Runner,
    which: Callable[[str], str | None], project_name: str | None,
    automatic_title: str | None, expected_title: str | None,
    expected_workspace: str | None,
    expected_provenance: Mapping[str, str] | None,
) -> dict[str, Any]:
    tab_id = environ.get("HERDR_TAB_ID")
    workspace_id = environ.get("HERDR_WORKSPACE_ID")
    socket_path = environ.get("HERDR_SOCKET_PATH")
    if not all(isinstance(value, str) and value for value in (
        tab_id, workspace_id, socket_path,
    )):
        return {
            "status": "unavailable", "reason": "herdr_identity_unavailable",
            "token": token, "manager": "herdr",
        }
    if expected_workspace is not None and workspace_id != expected_workspace:
        raise TabTitleError("herdr_workspace_changed")
    if expected_provenance is not None and (
        set(expected_provenance) != {"socket_path"}
        or expected_provenance.get("socket_path") != socket_path
    ):
        raise TabTitleError("herdr_provenance_changed")
    injected_binary = environ.get("HERDR_BIN_PATH")
    herdr = (
        injected_binary
        if isinstance(injected_binary, str) and os.path.isabs(injected_binary)
        else which("herdr")
    )
    if not herdr:
        return {
            "status": "unavailable", "reason": "herdr_cli_unavailable",
            "token": token, "manager": "herdr",
        }
    before_tab = _herdr_tab(
        herdr, tab_id, workspace_id, runner, environ, allow_unavailable=True,
    )
    if before_tab is None:
        return {
            "status": "unavailable", "reason": "herdr_target_unresolved",
            "token": token, "manager": "herdr",
        }
    if expected_title is not None and before_tab["label"] != expected_title:
        raise TabTitleError("herdr_title_changed")
    status, after, unavailable_reason = _adapter_title_plan(
        before_tab["label"], token, project_name=project_name,
        automatic_title=automatic_title,
    )
    if unavailable_reason is not None:
        return {
            "status": "unavailable", "reason": unavailable_reason,
            "token": token, "manager": "herdr",
        }
    assert after is not None
    if status == "updated":
        fenced_tab = _herdr_tab(
            herdr, tab_id, workspace_id, runner, environ,
            allow_unavailable=False,
        )
        if fenced_tab is None or fenced_tab["label"] != before_tab["label"]:
            raise TabTitleError("herdr_title_changed")
        _run(
            runner, [herdr, "tab", "rename", tab_id, after],
            environment=environ,
            command_error="herdr_command_failed",
        )
        observed = _herdr_tab(
            herdr, tab_id, workspace_id, runner, environ,
            allow_unavailable=False,
        )
        if observed is None or observed["label"] != after:
            raise TabTitleError("herdr_title_readback_mismatch")
    namespace = terminal_namespace_sha256(
        "herdr", {"socket_path": socket_path},
    )
    return {
        "status": status, "token": token, "manager": "herdr",
        "tab": tab_id, "workspace": workspace_id, "title": after,
        "session_namespace_sha256": namespace,
    }


def apply_title(
    token_value: str, *, target: str | None = None,
    project_name: str | None = None, automatic_title: str | None = None,
    expected_title: str | None = None,
    expected_workspace: str | None = None,
    expected_provenance: Mapping[str, str] | None = None,
    environ: Mapping[str, str] = os.environ, runner: Runner = subprocess.run,
    which: Callable[[str], str | None] = shutil.which,
) -> dict[str, Any]:
    token = canonical_token(token_value)
    manager = terminal_manager(environ)
    if manager == "herdr":
        return _apply_herdr_title(
            token, environ=environ, runner=runner, which=which,
            project_name=project_name, automatic_title=automatic_title,
            expected_title=expected_title,
            expected_workspace=expected_workspace,
            expected_provenance=expected_provenance,
        )
    # Surface identity is authoritative; CMUX_TAB_ID is a legacy alias and can
    # contain a workspace UUID in some runtimes. Never let it shadow a surface.
    caller_explicit_target = target is not None
    target = target or environ.get("CMUX_SURFACE_ID") or environ.get("CMUX_TAB_ID")
    if not target and not any(environ.get(key) for key in (
        "CMUX_SURFACE_ID", "CMUX_TAB_ID", "CMUX_WORKSPACE_ID", "CMUX_SOCKET_PATH",
    )):
        return {"status": "unavailable", "reason": "not_in_cmux_surface", "token": token}
    injected_cmux = environ.get("CMUX_BUNDLED_CLI_PATH")
    cmux = (
        injected_cmux
        if isinstance(injected_cmux, str) and os.path.isabs(injected_cmux)
        else which("cmux")
    )
    if not cmux:
        return {"status": "unavailable", "reason": "cmux_cli_unavailable", "token": token}
    if not target:
        try:
            target = _resolve_cmux_tty_target(cmux, runner, environ)
        except TabTitleError as error:
            reason = str(error) if str(error) in {
                "cmux_target_ambiguous", "cmux_target_unresolved",
            } else "cmux_target_unresolved"
            return {"status": "unavailable", "reason": reason, "token": token}
    try:
        context = _surface_context(
            cmux, target, runner, environment=environ,
            expected_workspace=expected_workspace,
            expected_provenance=expected_provenance,
        )
    except TabTitleError as error:
        if str(error) in {
            "cmux_target_identity_mismatch", "cmux_workspace_changed",
            "cmux_provenance_changed",
        }:
            raise
        if str(error) == "cmux_target_unresolved" and not caller_explicit_target and not environ.get("CMUX_SURFACE_ID"):
            try:
                target = _resolve_cmux_tty_target(cmux, runner, environ)
                context = _surface_context(
                    cmux, target, runner, environment=environ,
                    expected_workspace=expected_workspace,
                    expected_provenance=expected_provenance,
                )
            except TabTitleError as fallback_error:
                context = None
                fallback_reason = str(fallback_error)
        else:
            return {"status": "unavailable", "reason": "cmux_target_unresolved", "token": token}
        if context is None:
            return {"status": "unavailable", "reason": locals().get(
                "fallback_reason", "cmux_target_unresolved",
            ), "token": token}
    if context is None:
        return {"status": "unavailable", "reason": "cmux_unavailable", "token": token}
    before = _read_title(
        cmux, context, runner, allow_unavailable=True, environment=environ,
    )
    if before is None:
        return {
            "status": "unavailable", "reason": "cmux_target_unresolved",
            "token": token,
        }
    if expected_title is not None and before != expected_title:
        raise TabTitleError("cmux_title_changed")
    status, after, unavailable_reason = _adapter_title_plan(
        before, token, project_name=project_name,
        automatic_title=automatic_title,
    )
    if unavailable_reason is not None:
        return {
            "status": "unavailable", "reason": unavailable_reason,
            "token": token,
        }
    assert after is not None
    if status == "updated":
        fenced_context = _surface_context(
            cmux, target, runner, environment=environ,
            expected_workspace=expected_workspace,
            expected_provenance=expected_provenance,
        )
        if fenced_context is None or fenced_context != context:
            raise TabTitleError("cmux_target_identity_mismatch")
        fenced_before = _read_title(
            cmux, fenced_context, runner, environment=environ,
        )
        if fenced_before != before:
            raise TabTitleError("cmux_title_changed")
        _run(runner, [
            cmux, "rename-tab", "--surface", fenced_context.surface,
            "--workspace", fenced_context.workspace,
            "--window", fenced_context.window,
            "--", after,
        ], environment=environ)
        observed = _read_title(
            cmux, fenced_context, runner, environment=environ,
        )
        if observed != after:
            raise TabTitleError("cmux_title_readback_mismatch")
    return {
        "status": status, "token": token, "surface": context.surface,
        "title": after,
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("token", help="canonical workstream issue token, for example GEN-37")
    value.add_argument(
        "--surface", help="explicit cmux tab/surface ref; Herdr uses inherited IDs",
    )
    value.add_argument(
        "--project-name",
        help="trusted human-readable Linear project name for generated titles",
    )
    value.add_argument(
        "--automatic-title",
        help=(
            "exact previously observed manager-generated title that may be "
            "replaced; every other nonblank title is preserved as custom"
        ),
    )
    return value


def main(argv: list[str]) -> int:
    args = parser().parse_args(argv)
    try:
        result = apply_title(
            args.token, target=args.surface, project_name=args.project_name,
            automatic_title=args.automatic_title,
        )
    except TabTitleError as error:
        print(f"workstream-tab: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
