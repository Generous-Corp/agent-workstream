#!/usr/bin/env python3
"""Create or converge one reviewed child beneath an existing Linear root."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Callable

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_graph import GraphReviewRequired
from workstream_linear import (
    canonical_native_uuid, child_content_authority,
    deterministic_existing_root_child_id, durable_description, HttpGraphQLClient,
    LinearGraphQLTransport,
    LinearTransportError,
)
from workstream_linear_projection import (
    LinearProjectionAdapter,
    LinearProjectionError,
)
from workstream_plan import plan_payload, source_access_classification


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("source", help="Markdown path, HTTPS URL, or - for stdin")
    value.add_argument(
        "--identity", required=True,
        help="canonical immutable identity for these exact plan bytes",
    )
    value.add_argument(
        "--plan-revision", required=True, metavar="SHA256",
        help="exact source SHA-256 from the reviewed plan preview",
    )
    value.add_argument(
        "--workstream-id", required=True, metavar="GEN-123",
        help="existing Linear root identifier",
    )
    value.add_argument(
        "--root-issue-id", required=True, metavar="UUID",
        help="immutable existing Linear root issue UUID",
    )
    value.add_argument(
        "--candidate-key", required=True, metavar="STABLE_KEY",
        help="one exact reviewed candidate key from the plan preview",
    )
    value.add_argument("--material-revision", required=True, type=int)
    value.add_argument("--projection-revision", required=True, type=int)
    value.add_argument(
        "--state-id", required=True,
        help="exact native Linear workflow state UUID for the new child",
    )
    assignment = value.add_mutually_exclusive_group(required=True)
    assignment.add_argument(
        "--assignee-id",
        help="exact native Linear assignee UUID for the new child",
    )
    assignment.add_argument(
        "--unassigned", action="store_true",
        help="explicitly create and validate the child without an assignee",
    )
    value.add_argument("--config", help="exact .workstream.json path")
    value.add_argument("--workspace-id")
    value.add_argument("--team-id")
    value.add_argument("--project-id")
    value.add_argument(
        "--apply", action="store_true",
        help=(
            "apply the reviewed child extension; omission avoids Linear "
            "authentication/access, but URL source retrieval may use HTTP/SSH "
            "and configured source credentials"
        ),
    )
    return value


def run(
    argv: list[str],
    *,
    client_factory: Callable[[str], Any] = HttpGraphQLClient,
) -> dict[str, Any]:
    args = parser().parse_args(argv)
    if not args.identity.strip():
        raise ValueError("plan identity must be non-empty")
    if not args.workstream_id.strip() or not args.root_issue_id.strip():
        raise ValueError("existing workstream identifier and root issue UUID are required")
    if args.material_revision < 0 or args.projection_revision < 0:
        raise ValueError("material and projection revisions must be non-negative")
    if not args.state_id.strip():
        raise ValueError("native child state ID must be non-empty")
    if args.assignee_id is not None and not args.assignee_id.strip():
        raise ValueError("native child assignee ID must be non-empty")
    canonical_state_id = canonical_native_uuid(args.state_id, kind="state")
    canonical_assignee_id = (
        canonical_native_uuid(args.assignee_id, kind="assignee")
        if args.assignee_id is not None else None
    )
    source_access = source_access_classification(args.source)

    plan = plan_payload(args.source, args.identity)
    if plan["root"]["plan_revision"] != args.plan_revision:
        raise GraphReviewRequired(
            "plan bytes changed after review; generate and review a new plan preview"
        )
    candidates = [
        item for item in plan.get("children", [])
        if item.get("key") == args.candidate_key
    ]
    if len(candidates) != 1:
        raise GraphReviewRequired(
            "reviewed candidate key did not reproduce uniquely from the plan"
        )

    route, _config_path = resolve_linear_route(
        config_path=args.config,
        workspace_id=args.workspace_id,
        team_id=args.team_id,
        project_id=args.project_id,
    )
    if not route or any(
        not route.get(field) for field in ("workspace_id", "team_id", "project_id")
    ):
        raise ValueError(
            "child extension requires an exact Linear workspace/team/project route"
        )
    candidate = candidates[0]
    child_id = deterministic_existing_root_child_id(
        workspace_id=route["workspace_id"], team_id=route["team_id"],
        project_id=route["project_id"], root_issue_id=args.root_issue_id,
        child_stable_key=args.candidate_key,
    )
    description = durable_description(
        args.candidate_key, args.plan_revision,
        next_action=candidate.get("next_action"),
        details=candidate.get("description"),
    )
    content_authority = child_content_authority(
        title=candidate["title"], description=description,
        schema_version=candidate.get("content_schema_version"),
    )
    if not args.apply:
        return {
            "schema_version": 1, "mode": "preview", "would_write": False,
            "network_access": {
                **source_access,
                "linear": "none", "linear_authentication": "none",
            },
            "workstream_id": args.workstream_id.upper(),
            "source": plan["source"], "plan_revision": args.plan_revision,
            "route": {**route, "root_issue_id": args.root_issue_id},
            "frontier": {
                "material_revision": args.material_revision,
                "projection_revision": args.projection_revision,
            },
            "candidate": {
                "key": args.candidate_key, "issue_id": child_id,
                "title": candidate["title"], "description": description,
                "content_authority": content_authority,
                "state_id": canonical_state_id,
                "assignee_id": canonical_assignee_id,
                "unassigned": args.unassigned,
            },
        }
    token = load_linear_api_key()
    if not token:
        raise ValueError(
            "Linear authentication is required via LINEAR_API_KEY or the private token file"
        )
    client = client_factory(token)
    transport = LinearGraphQLTransport(
        client,
        workspace_id=route["workspace_id"],
        team_id=route["team_id"],
        project_id=route["project_id"],
    )
    authorization = LinearProjectionAdapter(
        client,
        issue_id=args.workstream_id,
        workstream_id=args.workstream_id,
        plan_revision=args.plan_revision,
        workspace_id=route["workspace_id"],
        team_id=route["team_id"],
        project_id=route["project_id"],
        root_issue_id=args.root_issue_id,
    )
    result = transport.extend_existing_root_reviewed_child(
        plan,
        root_issue_id=args.root_issue_id,
        reviewed_candidate_key=args.candidate_key,
        source_revision=args.plan_revision,
        plan_revision=args.plan_revision,
        expected_frontier={
            "material_revision": args.material_revision,
            "projection_revision": args.projection_revision,
        },
        state_id=canonical_state_id,
        assignee_id=canonical_assignee_id,
        unassigned=args.unassigned,
        authorization_adapter=authorization,
    )
    return {
        "schema_version": 1,
        "workstream_id": args.workstream_id.upper(),
        "source": result["source"],
        "plan_revision": result["plan_revision"],
        "route": {**result["route"], "root_issue_id": args.root_issue_id},
        "frontier": result["frontier"],
        "authorization": result["authorization"],
        "initial_state": result["initial_state"],
        "receipt": result["receipt"],
    }


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(sys.argv[1:] if argv is None else argv)
    except (
        GraphReviewRequired,
        LinearProjectionError,
        LinearTransportError,
        OSError,
        TimeoutError,
        UnicodeDecodeError,
        ValueError,
    ) as error:
        print(f"workstream child extension failed: {error}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
