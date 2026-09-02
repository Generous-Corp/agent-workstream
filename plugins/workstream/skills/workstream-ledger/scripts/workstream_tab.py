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


TOKEN = re.compile(r"^[A-Z][A-Z0-9]*-\d+$")
TOKEN_IN_TITLE = re.compile(r"(?<![A-Z0-9])([A-Z][A-Z0-9]*-\d+)(?![A-Z0-9])", re.I)
SEPARATOR = " · "
TIMEOUT_SECONDS = 3
MAX_PROJECT_LABEL_LENGTH = 120


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


def project_label(value: str | None) -> str:
    if value is None:
        raise TabTitleError("project_name_required_for_generated_title")
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
    if tokens_in_title(after) != [token]:
        raise TabTitleError("noncanonical_title_transition")


def plan_title(
    before: str, token: str, *, project_name: str | None = None,
    automatic_title: str | None = None,
) -> tuple[str, str]:
    found = tokens_in_title(before)
    if any(value != token for value in found):
        raise TabTitleError("workstream_tab_conflict")
    if len(found) > 1:
        raise TabTitleError("duplicate_workstream_token")
    if found == [token]:
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
    fields = {
        "surface": caller.get("surface_ref"),
        "pane": caller.get("pane_ref"),
        "workspace": caller.get("workspace_ref"),
        "window": caller.get("window_ref"),
    }
    if not all(isinstance(item, str) and item for item in fields.values()):
        raise TabTitleError("invalid_cmux_identify_response")
    return SurfaceContext(**fields)


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
    automatic_title: str | None,
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
    namespace = hashlib.sha256(socket_path.encode("utf-8")).hexdigest()
    return {
        "status": status, "token": token, "manager": "herdr",
        "tab": tab_id, "workspace": workspace_id, "title": after,
        "session_namespace_sha256": namespace,
    }


def apply_title(
    token_value: str, *, target: str | None = None,
    project_name: str | None = None, automatic_title: str | None = None,
    environ: Mapping[str, str] = os.environ, runner: Runner = subprocess.run,
    which: Callable[[str], str | None] = shutil.which,
) -> dict[str, Any]:
    token = canonical_token(token_value)
    if environ.get("HERDR_ENV") == "1":
        return _apply_herdr_title(
            token, environ=environ, runner=runner, which=which,
            project_name=project_name, automatic_title=automatic_title,
        )
    target = target or environ.get("CMUX_TAB_ID") or environ.get("CMUX_SURFACE_ID")
    if not target:
        return {"status": "unavailable", "reason": "not_in_cmux_surface", "token": token}
    cmux = which("cmux")
    if not cmux:
        return {"status": "unavailable", "reason": "cmux_cli_unavailable", "token": token}
    try:
        context = _surface_context(
            cmux, target, runner, environment=environ,
        )
    except TabTitleError as error:
        if str(error) == "cmux_target_unresolved":
            return {
                "status": "unavailable", "reason": "cmux_target_unresolved",
                "token": token,
            }
        raise
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
        _run(runner, [
            cmux, "rename-tab", "--surface", context.surface,
            "--workspace", context.workspace, "--window", context.window,
            "--", after,
        ], environment=environ)
        observed = _read_title(
            cmux, context, runner, environment=environ,
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
