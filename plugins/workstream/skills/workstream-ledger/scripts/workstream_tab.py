#!/usr/bin/env python3
"""Safely carry one workstream token in an existing cmux tab title."""

from __future__ import annotations

import argparse
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


def validate_transition(before: str, after: str, token: str) -> None:
    """Independently guard against overwriting a human-readable title."""
    if tokens_in_title(before):
        raise TabTitleError("title_already_contains_workstream_token")
    expected = token if not before.strip() else f"{before.rstrip()}{SEPARATOR}{token}"
    if after != expected:
        raise TabTitleError("existing_title_not_preserved")
    if tokens_in_title(after) != [token]:
        raise TabTitleError("noncanonical_title_transition")


def plan_title(before: str, token: str) -> tuple[str, str]:
    found = tokens_in_title(before)
    if any(value != token for value in found):
        raise TabTitleError("conflicting_workstream_token")
    if len(found) > 1:
        raise TabTitleError("duplicate_workstream_token")
    if found == [token]:
        return "unchanged", before
    after = token if not before.strip() else f"{before.rstrip()}{SEPARATOR}{token}"
    validate_transition(before, after, token)
    return "updated", after


def _run(
    runner: Runner, argv: Sequence[str], *, allow_unavailable: bool = False,
) -> subprocess.CompletedProcess[str] | None:
    try:
        result = runner(
            list(argv), stdin=subprocess.DEVNULL, capture_output=True, text=True,
            timeout=TIMEOUT_SECONDS, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        if allow_unavailable:
            return None
        raise TabTitleError("cmux_command_failed") from error
    if result.returncode != 0:
        if allow_unavailable:
            return None
        raise TabTitleError("cmux_command_failed")
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


def _surface_context(cmux: str, target: str, runner: Runner) -> SurfaceContext | None:
    if _run(runner, [cmux, "ping"], allow_unavailable=True) is None:
        return None
    result = _run(runner, [cmux, "identify", "--surface", target, "--json"])
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


def _read_title(cmux: str, context: SurfaceContext, runner: Runner) -> str:
    result = _run(runner, [
        cmux, "list-pane-surfaces", "--pane", context.pane,
        "--workspace", context.workspace, "--window", context.window, "--json",
    ])
    value = _json_result(result, "invalid_cmux_surface_response")
    surfaces = value.get("surfaces")
    if not isinstance(surfaces, list):
        raise TabTitleError("invalid_cmux_surface_response")
    matches = [
        item for item in surfaces
        if isinstance(item, dict) and item.get("ref") == context.surface
    ]
    if len(matches) != 1 or not isinstance(matches[0].get("title"), str):
        raise TabTitleError("cmux_target_title_unresolved")
    return matches[0]["title"]


def apply_title(
    token_value: str, *, target: str | None = None,
    environ: Mapping[str, str] = os.environ, runner: Runner = subprocess.run,
    which: Callable[[str], str | None] = shutil.which,
) -> dict[str, Any]:
    token = canonical_token(token_value)
    target = target or environ.get("CMUX_TAB_ID") or environ.get("CMUX_SURFACE_ID")
    if not target:
        return {"status": "unavailable", "reason": "not_in_cmux_surface", "token": token}
    cmux = which("cmux")
    if not cmux:
        return {"status": "unavailable", "reason": "cmux_cli_unavailable", "token": token}
    try:
        context = _surface_context(cmux, target, runner)
    except TabTitleError as error:
        if str(error) == "cmux_target_unresolved":
            return {
                "status": "unavailable", "reason": "cmux_target_unresolved",
                "token": token,
            }
        raise
    if context is None:
        return {"status": "unavailable", "reason": "cmux_unavailable", "token": token}
    before = _read_title(cmux, context, runner)
    status, after = plan_title(before, token)
    if status == "updated":
        _run(runner, [
            cmux, "rename-tab", "--surface", context.surface,
            "--workspace", context.workspace, "--window", context.window,
            "--", after,
        ])
        observed = _read_title(cmux, context, runner)
        if observed != after:
            raise TabTitleError("cmux_title_readback_mismatch")
    return {
        "status": status, "token": token, "surface": context.surface,
        "title": after,
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("token", help="canonical workstream issue token, for example GEN-37")
    value.add_argument("--surface", help="explicit cmux tab/surface ref; defaults to cmux environment")
    return value


def main(argv: list[str]) -> int:
    args = parser().parse_args(argv)
    try:
        result = apply_title(args.token, target=args.surface)
    except TabTitleError as error:
        print(f"workstream-tab: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
