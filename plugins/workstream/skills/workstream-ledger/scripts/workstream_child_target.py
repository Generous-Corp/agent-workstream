#!/usr/bin/env python3
"""Authenticate one exact root-owned Linear child for local ledger writes."""

from __future__ import annotations

import argparse
import re
import uuid
from typing import Any, Callable

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_linear import (
    HttpGraphQLClient, LinearTransportError, parse_plan_revision,
    validate_issue_route,
)
from workstream_linear_projection import LinearProjectionAdapter


TOKEN = re.compile(r"[A-Z][A-Z0-9]*-\d+")
CHILD_TARGET_QUERY = """
query WorkstreamChildTarget($rootId: String!, $childId: String!) {
  root: issue(id: $rootId) {
    id identifier description parent { id identifier }
    team { id organization { id } }
    project { id }
  }
  child: issue(id: $childId) {
    id identifier parent { id identifier }
    team { id organization { id } }
    project { id }
  }
}
"""


def add_target_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("root_workstream_id", metavar="GEN-123")
    parser.add_argument("--root-issue-id", required=True, metavar="UUID")
    parser.add_argument("--child-workstream-id", required=True, metavar="GEN-124")
    parser.add_argument("--child-issue-id", required=True, metavar="UUID")
    parser.add_argument("--plan-revision", required=True, metavar="SHA256")
    parser.add_argument("--config")
    parser.add_argument("--workspace-id")
    parser.add_argument("--team-id")
    parser.add_argument("--project-id")
    parser.add_argument("--apply", action="store_true", required=True)


def _token(value: str, field: str) -> str:
    token = value.upper()
    if not TOKEN.fullmatch(token):
        raise ValueError(f"invalid {field}")
    return token


def _uuid(value: str, field: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ValueError(f"invalid {field}") from error
    if str(parsed) != value.lower():
        raise ValueError(f"invalid {field}")
    return str(parsed)


def authenticate_child_target(
    args: argparse.Namespace, *,
    client_factory: Callable[[str], Any] = HttpGraphQLClient,
) -> dict[str, Any]:
    root_token = _token(args.root_workstream_id, "root workstream token")
    child_token = _token(args.child_workstream_id, "child workstream token")
    root_issue_id = _uuid(args.root_issue_id, "root issue UUID")
    child_issue_id = _uuid(args.child_issue_id, "child issue UUID")
    if root_token == child_token or root_issue_id == child_issue_id:
        raise ValueError("root and child identities must be distinct")
    if re.fullmatch(r"[0-9a-f]{64}", args.plan_revision) is None:
        raise ValueError("invalid plan revision")
    route, _config_path = resolve_linear_route(
        config_path=args.config, workspace_id=args.workspace_id,
        team_id=args.team_id, project_id=args.project_id,
    )
    if not route or any(
        not route.get(field) for field in ("workspace_id", "team_id", "project_id")
    ):
        raise ValueError("child mutation requires an exact Linear route")
    token = load_linear_api_key()
    if not token:
        raise ValueError(
            "Linear authentication is required via LINEAR_API_KEY or the private token file"
        )
    client = client_factory(token)
    result = client.execute(CHILD_TARGET_QUERY, {
        "rootId": root_token, "childId": child_issue_id,
    })
    root = result.get("root")
    child = result.get("child")
    if not isinstance(root, dict) or not isinstance(child, dict):
        raise LinearTransportError("child_target_not_found")
    if (
        root.get("id") != root_issue_id
        or str(root.get("identifier", "")).upper() != root_token
        or root.get("parent") is not None
    ):
        raise LinearTransportError("child_target_root_identity_mismatch")
    validate_issue_route(root, **route)
    if (
        child.get("id") != child_issue_id
        or str(child.get("identifier", "")).upper() != child_token
        or (child.get("parent") or {}).get("id") != root_issue_id
        or str((child.get("parent") or {}).get("identifier", "")).upper()
        != root_token
    ):
        raise LinearTransportError("child_target_identity_mismatch")
    validate_issue_route(child, **route)
    projection = LinearProjectionAdapter(
        client, issue_id=root_token, workstream_id=root_token,
        plan_revision=args.plan_revision, **route, root_issue_id=root_issue_id,
    )
    generation = projection.select_owned_child_generation(
        description_plan_revision=parse_plan_revision(root.get("description")),
        child_workstream_id=child_token,
    )
    return {
        "client": client, "root_workstream_id": root_token,
        "root_issue_id": root_issue_id, "child_workstream_id": child_token,
        "child_issue_id": child_issue_id, "plan_revision": args.plan_revision,
        "route": route, "generation_authority": generation,
    }
