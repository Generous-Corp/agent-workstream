#!/usr/bin/env python3
"""Authenticate one exact root-owned Linear child for local ledger writes."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import uuid
from typing import Any, Callable

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_linear import (
    deterministic_issue_id, HttpGraphQLClient, issue_key,
    LinearTransportError, parse_plan_revision,
    validate_issue_route,
)
from workstream_linear_projection import LinearProjectionAdapter, LinearProjectionError


TOKEN = re.compile(r"[A-Z][A-Z0-9]*-\d+")
CHILD_TARGET_QUERY = """
query WorkstreamChildTarget($rootId: String!, $childId: String!) {
  root: issue(id: $rootId) {
    id identifier description parent { id identifier }
    team { id organization { id } }
    project { id }
  }
  child: issue(id: $childId) {
    id identifier description parent { id identifier }
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
    proposal_id: str | None = None,
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
    if (child.get("id") != child_issue_id
            or str(child.get("identifier", "")).upper() != child_token):
        raise LinearTransportError("child_target_identity_mismatch")
    projection = LinearProjectionAdapter(
        client, issue_id=root_token, workstream_id=root_token,
        plan_revision=args.plan_revision, **route, root_issue_id=root_issue_id,
    )
    description_revision = parse_plan_revision(root.get("description"))
    try:
        generation = projection.select_owned_child_generation(
            description_plan_revision=description_revision,
            child_workstream_id=child_token, child_issue_id=child_issue_id,
            proposal_id=proposal_id,
        )
    except LinearProjectionError as origin_error:
        if str(origin_error) != "child_origin_provenance_missing":
            raise
        root_key = issue_key(root)
        child_key = issue_key(child)
        marker = {"root_stable_key": root_key, "child_stable_key": child_key}
        if (
            not root_key or not child_key
            or deterministic_issue_id(**route, root_stable_key=root_key)
            != root_issue_id
            or deterministic_issue_id(
                **route, root_stable_key=root_key, child_stable_key=child_key,
            ) != child_issue_id
        ):
            raise LinearTransportError(
                "child_origin_provenance_missing"
            ) from origin_error
        selected = projection.select_generation_authority(
            description_plan_revision=description_revision,
        )
        state = projection.state()
        scope_events = [
            event for event in state.events
            if event["kind"] == "scope" and event["key"] == "root"
        ]
        scope = scope_events[-1] if scope_events else None
        owner = (state.snapshot.get("scope") or {}).get(
            "child_ownership", {}
        ).get(child_token)
        if scope is None or not isinstance(owner, str) or not owner:
            raise LinearTransportError(f"child_target_not_owned:{child_token}")
        authority = {**route, "root_issue_id": root_issue_id}
        generation = {
            **selected, "workstream_id": root_token, "authority": authority,
            "source": state.snapshot.get("source"),
            "child_repository_owner": owner,
            "scope_event_id": scope["event_id"],
            "scope_value_sha256": hashlib.sha256(json.dumps(
                scope["value"], sort_keys=True, separators=(",", ":"),
            ).encode()).hexdigest(),
            "projection_revision": state.revision,
            "child_origin": {
                "kind": "deterministic_intake_marker", **marker,
                "marker_sha256": hashlib.sha256(json.dumps(
                    marker, sort_keys=True, separators=(",", ":"),
                ).encode()).hexdigest(),
            },
        }
    try:
        validate_issue_route(child, **route)
    except LinearTransportError as error:
        raise LinearTransportError("child_native_cache_drift") from error
    if (
        (child.get("parent") or {}).get("id") != root_issue_id
        or str((child.get("parent") or {}).get("identifier", "")).upper()
        != root_token
    ):
        raise LinearTransportError("child_native_cache_drift")
    return {
        "client": client, "root_workstream_id": root_token,
        "root_issue_id": root_issue_id, "child_workstream_id": child_token,
        "child_issue_id": child_issue_id, "plan_revision": args.plan_revision,
        "route": route, "generation_authority": generation,
        "projection": projection,
        "child_origin": generation["child_origin"],
        "child_identity": {
            "identifier": child_token, "id": child_issue_id,
            "parent_issue_id": root_issue_id, "route": route,
        },
    }
