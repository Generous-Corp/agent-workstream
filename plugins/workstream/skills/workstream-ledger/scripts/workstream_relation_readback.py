#!/usr/bin/env python3
"""Authenticated peer-relation readback shared by resume and projection."""

from __future__ import annotations

from typing import Any

from workstream_linear import parse_plan_revision
from workstream_linear_events import LinearCommentEventAdapter
from workstream_linear_projection import reduce_projection_comments
from workstream_scope import relation_target_key


RELATION_TARGET_QUERY = """
query WorkstreamRelationTarget($issueId: String!) {
  issue(id: $issueId) {
    id identifier description
    team { id organization { id } }
    project { id }
  }
}
"""


class RelationReadbackError(ValueError):
    """A relation target could not be authenticated and reduced exactly."""


def read_relation_targets(
    client: Any, relations: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Resolve every immutable target and reduce its complete peer-edge state."""
    resolved: dict[str, dict[str, Any]] = {}
    for relation in relations:
        if not isinstance(relation, dict) or not isinstance(relation.get("target"), dict):
            raise RelationReadbackError("invalid_relation_target")
        target = relation["target"]
        key = relation_target_key(target)
        if key in resolved:
            continue
        response = client.execute(
            RELATION_TARGET_QUERY, {"issueId": target.get("issue_id")}
        )
        issue = response.get("issue") if isinstance(response, dict) else None
        team = issue.get("team") if isinstance(issue, dict) else None
        workspace_id = ((team or {}).get("organization") or {}).get("id")
        if (
            not isinstance(issue, dict)
            or issue.get("id") != target.get("issue_id")
            or issue.get("identifier") != target.get("identifier")
            or workspace_id != target.get("workspace_id")
        ):
            raise RelationReadbackError(
                f"dangling_relation_target:{target.get('identifier')}"
            )
        team_id = (team or {}).get("id")
        project_id = (issue.get("project") or {}).get("id")
        plan_revision = parse_plan_revision(issue.get("description"))
        if not all(isinstance(item, str) and item for item in (
            team_id, project_id, plan_revision,
        )):
            raise RelationReadbackError(
                f"relation_target_readback_incomplete:{target.get('identifier')}"
            )
        route = {
            "workspace_id": workspace_id, "team_id": team_id,
            "project_id": project_id, "root_issue_id": issue["id"],
        }
        comments = LinearCommentEventAdapter(
            client, issue_id=issue["identifier"], workspace_id=workspace_id,
            team_id=team_id, project_id=project_id,
        ).comments()
        projection = reduce_projection_comments(
            comments, workstream_id=issue["identifier"],
            expected_plan_revision=plan_revision, authenticated_route=route,
        ).snapshot
        resolved[key] = {
            "workspace_id": workspace_id, "issue_id": issue["id"],
            "identifier": issue["identifier"],
            "relations": projection.get("relations") or [],
        }
    return resolved


def add_relation_target_readback(
    snapshot: dict[str, Any], client: Any,
) -> dict[str, Any]:
    value = dict(snapshot)
    relations = value.get("relations") or []
    if relations:
        value["relation_targets"] = read_relation_targets(client, relations)
    return value
