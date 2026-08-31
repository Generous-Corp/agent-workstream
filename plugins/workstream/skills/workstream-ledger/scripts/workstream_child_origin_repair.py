#!/usr/bin/env python3
"""Review and append one origin seal for an existing legacy Linear child."""

from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Callable

from workstream_child_target import _token, _uuid
from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_linear import (
    HttpGraphQLClient, LinearTransportError, parse_plan_revision,
    validate_issue_route,
)
from workstream_linear_events import COMMENTS_QUERY
from workstream_linear_events import validate_review_artifact_identity
from workstream_linear_projection import (
    child_extension_authorizations_from_comments,
    child_origin_history_frontier, legacy_child_origin_repairs_from_comments,
    LinearProjectionAdapter, LinearProjectionError, projection_prefix_frontier,
)
from workstream_child_proposal import proposal_index
from workstream_plan import source_bytes


CHILD_ORIGIN_REPAIR_QUERY = """
query WorkstreamChildOriginRepairTarget($rootId: String!, $childId: String!) {
  root: issue(id: $rootId) {
    id identifier description createdAt parent { id identifier }
    team { id organization { id } }
    project { id }
    state { id name type }
    assignee { id }
  }
  child: issue(id: $childId) {
    id identifier description createdAt
    parent { id identifier }
    team { id organization { id } }
    project { id }
    state { id name type }
    assignee { id }
  }
}
"""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def _comments(client: Any, issue_id: str, *, expected_id: str,
              expected_token: str, route: dict[str, str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    after: str | None = None
    seen: set[str] = set()
    while True:
        response = client.execute(COMMENTS_QUERY, {"issueId": issue_id, "after": after})
        issue = response.get("issue")
        if (
            not isinstance(issue, dict) or issue.get("id") != expected_id
            or str(issue.get("identifier", "")).upper() != expected_token
        ):
            raise LinearTransportError("child_origin_repair_issue_mismatch")
        validate_issue_route(issue, **route)
        connection = issue.get("comments") or {}
        nodes = connection.get("nodes")
        page_info = connection.get("pageInfo")
        if not isinstance(nodes, list) or not isinstance(page_info, dict):
            raise LinearTransportError("invalid Linear comment connection")
        result.extend(nodes)
        if not page_info.get("hasNextPage"):
            return result
        after = page_info.get("endCursor")
        if not isinstance(after, str) or not after or after in seen:
            raise LinearTransportError("invalid Linear comment pagination cursor")
        seen.add(after)


def _review_artifact(*, created_at: str, value: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "agent-workstream.existing-child-origin-review/v1",
        "created_at": created_at,
        "value": deepcopy(value),
    }


def _review_binding(identity: str, material: bytes, reviewed_at: str) -> dict[str, str]:
    match = re.fullmatch(
        r"https://github\.com/([^/]+)/([^/]+)/blob/([0-9a-f]{40})/(.+)",
        identity,
    )
    if match is None:
        raise ValueError("child_origin_review_identity_not_immutable")
    owner, repository, commit, path = match.groups()
    value = {
        "identity": identity,
        "repository": f"github.com/{owner}/{repository}",
        "commit": commit, "path": path,
        "sha256": hashlib.sha256(material).hexdigest(),
        "reviewed_at": reviewed_at,
    }
    validate_review_artifact_identity(value)
    return value


def run(
    argv: list[str], *, client_factory: Callable[[str], Any] = HttpGraphQLClient,
    source_loader: Callable[[str, str | None], tuple[bytes, str]] = source_bytes,
) -> dict[str, Any]:
    parser = argparse.ArgumentParser(
        description="Seal one reviewed, existing legacy child's immutable origin",
    )
    parser.add_argument("root_workstream_id", metavar="GEN-123")
    parser.add_argument("--root-issue-id", required=True)
    parser.add_argument("--child-workstream-id", required=True)
    parser.add_argument("--child-issue-id", required=True)
    parser.add_argument("--plan-revision", required=True)
    parser.add_argument("--created-at", required=True)
    parser.add_argument("--custodian", required=True)
    parser.add_argument("--writers-retired-at", required=True)
    parser.add_argument("--review", type=Path)
    parser.add_argument("--review-identity")
    parser.add_argument("--config")
    parser.add_argument("--workspace-id")
    parser.add_argument("--team-id")
    parser.add_argument("--project-id")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    if args.apply and (args.review is None or not args.review_identity):
        parser.error("--apply requires --review and --review-identity")

    root_token = _token(args.root_workstream_id, "root workstream token")
    child_token = _token(args.child_workstream_id, "child workstream token")
    root_issue_id = _uuid(args.root_issue_id, "root issue UUID")
    child_issue_id = _uuid(args.child_issue_id, "child issue UUID")
    if root_token == child_token or root_issue_id == child_issue_id:
        raise ValueError("root and child identities must be distinct")
    if len(args.plan_revision) != 64 or any(
        char not in "0123456789abcdef" for char in args.plan_revision
    ):
        raise ValueError("invalid plan revision")
    route, _ = resolve_linear_route(
        config_path=args.config, workspace_id=args.workspace_id,
        team_id=args.team_id, project_id=args.project_id,
    )
    if not route or any(not route.get(field) for field in (
        "workspace_id", "team_id", "project_id",
    )):
        raise ValueError("child origin repair requires an exact Linear route")
    token = load_linear_api_key()
    if not token:
        raise ValueError("Linear authentication is required")
    client = client_factory(token)
    native = client.execute(CHILD_ORIGIN_REPAIR_QUERY, {
        "rootId": root_token, "childId": child_issue_id,
    })
    root = native.get("root")
    child = native.get("child")
    if not isinstance(root, dict) or not isinstance(child, dict):
        raise LinearTransportError("child_origin_repair_target_not_found")
    authority = {**route, "root_issue_id": root_issue_id}
    if (
        root.get("id") != root_issue_id
        or str(root.get("identifier", "")).upper() != root_token
        or root.get("parent") is not None
    ):
        raise LinearTransportError("child_origin_repair_root_identity_mismatch")
    validate_issue_route(root, **route)
    root_description = root.get("description")
    if root_description is None:
        root_description = ""
    root_state = root.get("state")
    root_assignee = root.get("assignee")
    if (
        not isinstance(root_description, str)
        or not isinstance(root_state, dict)
        or set(root_state) != {"id", "name", "type"}
        or not all(isinstance(root_state.get(field), str) and root_state[field]
                   for field in ("id", "name", "type"))
        or (
            root_assignee is not None
            and (
                not isinstance(root_assignee, dict)
                or set(root_assignee) != {"id"}
                or not isinstance(root_assignee.get("id"), str)
                or not root_assignee["id"]
            )
        )
        or not isinstance(root.get("createdAt"), str) or not root["createdAt"]
    ):
        raise LinearTransportError("child_origin_repair_root_readback_incomplete")
    native_root_readback = {
        "id": root_issue_id, "identifier": root_token, "parent": None,
        "route": authority, "state": deepcopy(root_state),
        "assignee_id": root_assignee["id"] if root_assignee else None,
        "created_at": root["createdAt"],
    }
    root_description_fence = {
        "bytes": len(root_description.encode("utf-8")),
        "sha256": hashlib.sha256(root_description.encode("utf-8")).hexdigest(),
    }
    if (
        child.get("id") != child_issue_id
        or str(child.get("identifier", "")).upper() != child_token
        or (child.get("parent") or {}).get("id") != root_issue_id
        or str((child.get("parent") or {}).get("identifier", "")).upper()
        != root_token
    ):
        raise LinearTransportError("child_origin_repair_native_parent_mismatch")
    validate_issue_route(child, **route)
    description = child.get("description")
    if description is None:
        description = ""
    if not isinstance(description, str):
        raise LinearTransportError("child_origin_repair_description_invalid")
    state_readback = child.get("state")
    assignee_readback = child.get("assignee")
    if (
        not isinstance(state_readback, dict)
        or set(state_readback) != {"id", "name", "type"}
        or not all(isinstance(state_readback.get(field), str)
                   and state_readback[field] for field in ("id", "name", "type"))
        or (
            assignee_readback is not None
            and (
                not isinstance(assignee_readback, dict)
                or set(assignee_readback) != {"id"}
                or not isinstance(assignee_readback.get("id"), str)
                or not assignee_readback["id"]
            )
        )
        or not isinstance(child.get("createdAt"), str)
        or not child["createdAt"]
    ):
        raise LinearTransportError("child_origin_repair_native_readback_incomplete")
    native_child_readback = {
        "id": child_issue_id,
        "identifier": child_token,
        "parent": {"id": root_issue_id, "identifier": root_token},
        "route": route,
        "state": deepcopy(state_readback),
        "assignee_id": (
            assignee_readback["id"] if assignee_readback is not None else None
        ),
        "created_at": child["createdAt"],
        "description": {
            "bytes": len(description.encode("utf-8")),
            "sha256": hashlib.sha256(description.encode("utf-8")).hexdigest(),
        },
    }

    projection = LinearProjectionAdapter(
        client, issue_id=root_token, workstream_id=root_token,
        plan_revision=args.plan_revision, **route, root_issue_id=root_issue_id,
    )
    root_comments = projection._comments()
    description_revision = parse_plan_revision(root.get("description"))
    state = projection.state()
    repairs = [
        event for event in legacy_child_origin_repairs_from_comments(
            root_comments, workstream_id=root_token,
            description_plan_revision=description_revision,
            authenticated_route=authority,
        )
        if event["value"].get("child_issue_id") == child_issue_id
    ]
    if len(repairs) > 1:
        raise LinearProjectionError("child_origin_repair_ambiguous")
    if repairs:
        repair = repairs[0]
        base = deepcopy(repair["value"])
        base.pop("review_artifact")
        artifact = _review_artifact(created_at=repair["created_at"], value=base)
    else:
        extensions = [
            event for event in child_extension_authorizations_from_comments(
                root_comments, workstream_id=root_token,
                description_plan_revision=description_revision,
                authenticated_route=authority,
            )
            if event["value"].get("child_issue_id") == child_issue_id
        ]
        if extensions:
            raise LinearProjectionError("child_origin_already_authenticated")
        scope_events = [
            event for event in state.events
            if event["kind"] == "scope" and event["key"] == "root"
        ]
        scope = scope_events[-1] if scope_events else None
        owner = (state.snapshot.get("scope") or {}).get(
            "child_ownership", {},
        ).get(child_token)
        source = state.snapshot.get("source")
        if scope is None or not isinstance(owner, str) or not owner:
            raise LinearProjectionError(f"child_target_not_owned:{child_token}")
        if not isinstance(source, dict):
            raise LinearProjectionError("child_target_source_missing")
        generation = projection.select_child_extension_generation(
            description_plan_revision=description_revision, source=source,
        )
        child_comments = _comments(
            client, child_token, expected_id=child_issue_id,
            expected_token=child_token, route=route,
        )
        inert = proposal_index(child_comments)
        if inert:
            raise LinearProjectionError("child_origin_preexisting_inert_proposals")
        base = {
            "schema_version": 1,
            "root_issue_id": root_issue_id,
            "route": authority,
            "native_root_readback": native_root_readback,
            "native_root_readback_sha256": hashlib.sha256(
                _canonical(native_root_readback),
            ).hexdigest(),
            "root_description": root_description_fence,
            "source": source,
            "plan_revision": args.plan_revision,
            "generation_authority": generation,
            "scope_event_id": scope["event_id"],
            "scope_value_sha256": hashlib.sha256(
                _canonical(scope["value"]),
            ).hexdigest(),
            "repository_owner": owner,
            "child_workstream_id": child_token,
            "child_issue_id": child_issue_id,
            "child_parent_issue_id": root_issue_id,
            "child_route": route,
            "native_child_readback": native_child_readback,
            "native_child_readback_sha256": hashlib.sha256(
                _canonical(native_child_readback),
            ).hexdigest(),
            "root_projection_prefix": projection_prefix_frontier(
                state, root_comments,
            ),
            "root_history": child_origin_history_frontier(
                root_comments, workstream_id=root_token,
            ),
            "child_history": child_origin_history_frontier(
                child_comments, workstream_id=child_token,
            ),
            "pending_proposals": {
                "count": 0,
                "proposal_ids_sha256": hashlib.sha256(_canonical([])).hexdigest(),
            },
            "custody_writer_retirement": {
                "custodian": args.custodian,
                "previous_writers_retired": True,
                "writers_retired_at": args.writers_retired_at,
            },
            "expected_projection_revision": state.revision,
            "initial_state": "existing_scope_owned_legacy_child",
        }
        artifact = _review_artifact(created_at=args.created_at, value=base)

    if not args.apply:
        return artifact
    try:
        reviewed_material = args.review.read_bytes()
        reviewed = json.loads(reviewed_material)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("invalid child origin review artifact") from error
    if reviewed != artifact:
        raise LinearProjectionError("child_origin_review_stale_reload_required")
    fetched, fetched_identity = source_loader(
        args.review_identity, args.review_identity,
    )
    if fetched_identity != args.review_identity or fetched != reviewed_material:
        raise LinearProjectionError("child_origin_review_remote_mismatch")
    review_binding = _review_binding(
        args.review_identity, reviewed_material, artifact["created_at"],
    )

    # Re-read the child history immediately before the root CAS append.  This
    # is a fail-closed cross-resource preflight, not a claim of atomic Linear
    # transactions across two issues.
    child_comments = _comments(
        client, child_token, expected_id=child_issue_id,
        expected_token=child_token, route=route,
    )
    if child_origin_history_frontier(
        child_comments, workstream_id=child_token,
    ) != artifact["value"]["child_history"]:
        raise LinearProjectionError("child_origin_repair_child_history_changed")
    value = {
        **artifact["value"],
        "review_artifact": review_binding,
    }
    receipt = projection.repair_legacy_child_origin(
        value=value, created_at=artifact["created_at"],
    )
    post_native = client.execute(CHILD_ORIGIN_REPAIR_QUERY, {
        "rootId": root_token, "childId": child_issue_id,
    })
    post_root = post_native.get("root")
    if not isinstance(post_root, dict):
        raise LinearProjectionError(
            "child_origin_repair_postread_root_missing_authority_changed"
        )
    validate_issue_route(post_root, **route)
    post_description = post_root.get("description")
    if post_description is None:
        post_description = ""
    post_state = post_root.get("state")
    post_assignee = post_root.get("assignee")
    post_readback = {
        "id": post_root.get("id"),
        "identifier": str(post_root.get("identifier", "")).upper(),
        "parent": post_root.get("parent"), "route": authority,
        "state": deepcopy(post_state),
        "assignee_id": (
            post_assignee.get("id") if isinstance(post_assignee, dict) else None
        ),
        "created_at": post_root.get("createdAt"),
    }
    post_description_fence = {
        "bytes": (
            len(post_description.encode("utf-8"))
            if isinstance(post_description, str) else -1
        ),
        "sha256": (
            hashlib.sha256(post_description.encode("utf-8")).hexdigest()
            if isinstance(post_description, str) else ""
        ),
    }
    if (
        post_readback != value["native_root_readback"]
        or hashlib.sha256(_canonical(post_readback)).hexdigest()
        != value["native_root_readback_sha256"]
        or post_description_fence != value["root_description"]
    ):
        raise LinearProjectionError(
            "child_origin_repair_postread_root_drift_authority_changed"
        )
    return {
        "status": "repaired", "root_workstream_id": root_token,
        "child_workstream_id": child_token, "child_issue_id": child_issue_id,
        "review_artifact": review_binding,
        "receipt": receipt,
    }


def main() -> int:
    try:
        print(json.dumps(run(__import__("sys").argv[1:]), sort_keys=True))
    except (LinearTransportError, ValueError) as error:
        raise SystemExit(str(error)) from error
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
