#!/usr/bin/env python3
"""Validated, repository-local Agent Workstream configuration."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Mapping
from urllib.parse import urlparse


CONFIG_NAME = ".workstream.json"
LINEAR_TOKEN_RELATIVE_PATH = Path("agent-workstream") / "linear.token"


def load_linear_api_key(*, env: Mapping[str, str] | None = None) -> str | None:
    """Load unattended Linear auth without requiring shell initialization."""
    values = os.environ if env is None else env
    direct = values.get("LINEAR_API_KEY", "").strip()
    if direct:
        return direct
    requested = values.get("LINEAR_API_KEY_FILE", "").strip()
    if requested:
        if requested == "~" or requested.startswith("~/"):
            home = values.get("HOME", "").strip()
            if not home:
                raise ValueError("HOME is required to expand LINEAR_API_KEY_FILE")
            path = Path(home) / requested.removeprefix("~/")
        elif requested.startswith("~"):
            raise ValueError("LINEAR_API_KEY_FILE does not support named-user expansion")
        else:
            path = Path(requested)
    else:
        config_home = values.get("XDG_CONFIG_HOME", "").strip()
        home = values.get("HOME", "").strip()
        if config_home:
            path = Path(config_home) / LINEAR_TOKEN_RELATIVE_PATH
        elif home:
            path = Path(home) / ".config" / LINEAR_TOKEN_RELATIVE_PATH
        else:
            return None
    if not path.exists():
        return None
    if not path.is_file():
        raise ValueError(f"Linear token path is not a file: {path}")
    mode = path.stat().st_mode & 0o777
    if not mode & 0o400 or mode & 0o077:
        raise ValueError(
            f"Linear token file must be owner-readable and inaccessible to group/world: {path}"
        )
    if path.parent.stat().st_mode & 0o077:
        raise ValueError(
            f"Linear token directory must not be group/world accessible: {path.parent}"
        )
    token = path.read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError(f"Linear token file is empty: {path}")
    return token


def unique_object(pairs: list[tuple[str, object]]) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def validate_config(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(), object_pairs_hook=unique_object)
    except OSError as error:
        raise ValueError(f"cannot read config: {error}") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ValueError("config must be an object")
    allowed = {"schema_version", "namespace", "planning_url", "ledger", "repositories"}
    extra = set(value) - allowed
    if extra:
        raise ValueError("unknown config fields: " + ", ".join(sorted(extra)))
    if value.get("schema_version") != 1:
        raise ValueError("schema_version must be 1")
    if not isinstance(value.get("namespace"), str) or not value["namespace"].strip():
        raise ValueError("namespace must be a non-empty string")
    planning_url = value.get("planning_url")
    if planning_url is not None:
        if not isinstance(planning_url, str):
            raise ValueError("planning_url must be an absolute URL")
        parsed = urlparse(planning_url)
        if (not parsed.scheme or not parsed.netloc or re.search(r"\s", planning_url)
                or not re.fullmatch(r"[A-Za-z][A-Za-z0-9+.-]*", parsed.scheme)):
            raise ValueError("planning_url must be an absolute URL")
    ledger = value.get("ledger")
    if not isinstance(ledger, dict):
        raise ValueError("ledger must be an object")
    ledger_allowed = {"provider", "workspace_id", "team_id", "project_id"}
    if set(ledger) - ledger_allowed:
        raise ValueError("ledger contains unknown fields")
    for key in ledger_allowed:
        if not isinstance(ledger.get(key), str) or not ledger[key].strip():
            raise ValueError(f"ledger.{key} must be a non-empty string")
    repositories = value.get("repositories")
    if not isinstance(repositories, dict) or not repositories:
        raise ValueError("repositories must be a non-empty object keyed by provider:repository_id")
    for identity, repository in repositories.items():
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9+.-]*:[^\s:]+", identity):
            raise ValueError(f"invalid repository identity: {identity}")
        if not isinstance(repository, dict):
            raise ValueError(f"repositories.{identity} must be an object")
        allowed_repo = {"coordinate", "acceptance_commands"}
        if set(repository) - allowed_repo:
            raise ValueError(f"repositories.{identity} contains unknown fields")
        if not isinstance(repository.get("coordinate"), str) or not repository["coordinate"].strip():
            raise ValueError(f"repositories.{identity}.coordinate must be a non-empty string")
        commands = repository.get("acceptance_commands", [])
        if not isinstance(commands, list) or any(not isinstance(c, str) or not c.strip() for c in commands):
            raise ValueError(f"repositories.{identity}.acceptance_commands must contain non-empty strings")
    return value


def repository_root(start: Path) -> Path | None:
    """Find the enclosing Git root without consulting mutable Git configuration."""
    current = start.resolve()
    if current.is_file():
        current = current.parent
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def resolve_config_path(
    explicit: str | Path | None = None,
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    required: bool = False,
) -> Path | None:
    """Resolve an explicit config or the exact enclosing repository-root config."""
    values = os.environ if env is None else env
    requested = explicit or values.get("WORKSTREAM_CONFIG")
    if requested:
        path = Path(requested).expanduser().resolve()
        if not path.is_file():
            raise ValueError(f"workstream config not found: {path}")
        return path
    root = repository_root(cwd or Path.cwd())
    path = root / CONFIG_NAME if root else None
    if path and path.is_file():
        return path
    if required:
        location = str(path or (cwd or Path.cwd()).resolve() / CONFIG_NAME)
        raise ValueError(f"workstream config not found: {location}")
    return None


def load_config(
    explicit: str | Path | None = None,
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    required: bool = False,
) -> tuple[dict, Path] | None:
    path = resolve_config_path(explicit, cwd=cwd, env=env, required=required)
    return (validate_config(path), path) if path else None


def linear_route(config: dict) -> dict[str, str]:
    ledger = config["ledger"]
    if ledger["provider"].lower() != "linear":
        raise ValueError(f"unsupported ledger provider for Linear operation: {ledger['provider']}")
    return {
        "workspace_id": ledger["workspace_id"],
        "team_id": ledger["team_id"],
        "project_id": ledger["project_id"],
    }


def resolve_linear_route(
    *,
    config_path: str | Path | None = None,
    workspace_id: str | None = None,
    team_id: str | None = None,
    project_id: str | None = None,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> tuple[dict[str, str] | None, Path | None]:
    loaded = load_config(config_path, cwd=cwd, env=env)
    configured = linear_route(loaded[0]) if loaded else None
    explicit_values = (workspace_id, team_id, project_id)
    if configured:
        supplied = {
            "workspace_id": workspace_id,
            "team_id": team_id,
            "project_id": project_id,
        }
        for key, value in supplied.items():
            if value and value != configured[key]:
                raise ValueError(f"explicit Linear {key} conflicts with workstream config")
        return configured, loaded[1]
    if any(explicit_values) and not all(explicit_values):
        # Keep the legacy team-only path available only when no config exists.
        if not (team_id and not workspace_id and not project_id):
            raise ValueError("Linear workspace, team, and project IDs must be supplied together")
    if all(explicit_values):
        return {
            "workspace_id": workspace_id,
            "team_id": team_id,
            "project_id": project_id,
        }, None
    if team_id:
        return {"team_id": team_id}, None
    return None, None
