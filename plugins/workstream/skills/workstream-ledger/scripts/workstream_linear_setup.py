#!/usr/bin/env python3
"""Inspect the authenticated Linear route without mutating Linear."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from workstream_config import linear_route, load_config
from workstream_linear import GraphQLClient, HttpGraphQLClient, LinearTransportError


VIEWER_QUERY = "query WorkstreamLinearViewer { viewer { id name } }"
TEAMS_QUERY = """
query WorkstreamLinearTeams($after: String) {
  teams(first: 50, after: $after) {
    nodes { id key name organization { id name } }
    pageInfo { hasNextPage endCursor }
  }
}
"""
PROJECTS_QUERY = """
query WorkstreamLinearProjects($after: String) {
  projects(first: 50, after: $after) {
    nodes { id name teams { nodes { id key name } } }
    pageInfo { hasNextPage endCursor }
  }
}
"""


def _collect(client: GraphQLClient, query: str, connection_name: str) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    after = None
    while True:
        connection = client.execute(query, {"after": after}).get(connection_name)
        if not isinstance(connection, dict) or not isinstance(connection.get("nodes"), list):
            raise LinearTransportError(f"Linear setup query returned an invalid {connection_name} inventory")
        nodes.extend(connection["nodes"])
        page_info = connection.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            return nodes
        after = page_info.get("endCursor")
        if not after:
            raise LinearTransportError(f"invalid Linear {connection_name} pagination cursor")


def inspect_route(
    client: GraphQLClient,
    *,
    team_id: str | None = None,
    project_id: str | None = None,
) -> dict[str, Any]:
    viewer = client.execute(VIEWER_QUERY, {}).get("viewer")
    teams = _collect(client, TEAMS_QUERY, "teams")
    projects = _collect(client, PROJECTS_QUERY, "projects")
    if not isinstance(viewer, dict):
        raise LinearTransportError("Linear setup query returned an invalid authenticated identity")

    selected_team = next((team for team in teams if team.get("id") == team_id), None) if team_id else None
    selected_project = (
        next((project for project in projects if project.get("id") == project_id), None)
        if project_id
        else None
    )
    if team_id and selected_team is None:
        raise LinearTransportError(f"Linear team not found: {team_id}")
    if project_id and selected_project is None:
        raise LinearTransportError(f"Linear project not found: {project_id}")
    if selected_team and selected_project:
        project_team_ids = {
            team.get("id") for team in ((selected_project.get("teams") or {}).get("nodes") or [])
        }
        if team_id not in project_team_ids:
            raise LinearTransportError("selected Linear project is not associated with the selected team")

    organizations = {
        team["organization"]["id"]: team["organization"].get("name")
        for team in teams
        if isinstance(team.get("organization"), dict) and team["organization"].get("id")
    }
    return {
        "authenticated_as": {"id": viewer.get("id"), "name": viewer.get("name")},
        "workspaces": [
            {"id": workspace_id, "name": name}
            for workspace_id, name in sorted(organizations.items(), key=lambda item: (item[1] or "", item[0]))
        ],
        "teams": sorted(teams, key=lambda team: (team.get("name") or "", team.get("id") or "")),
        "projects": sorted(projects, key=lambda project: (project.get("name") or "", project.get("id") or "")),
        "selection": {
            "valid": bool(selected_team and selected_project),
            "workspace_id": (
                (selected_team.get("organization") or {}).get("id") if selected_team else None
            ),
            "team_id": team_id,
            "project_id": project_id,
        },
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="workstream config path; defaults to repository-root .workstream.json")
    parser.add_argument("--team-id", help="validate this immutable Linear team ID")
    parser.add_argument("--project-id", help="validate this immutable Linear project ID")
    parser.add_argument("--endpoint", default="https://api.linear.app/graphql")
    args = parser.parse_args(argv)
    if bool(args.team_id) != bool(args.project_id):
        parser.error("--team-id and --project-id must be supplied together")
    token = os.environ.get("LINEAR_API_KEY", "").strip()
    if not token:
        print("workstream-linear-setup: LINEAR_API_KEY is required", file=sys.stderr)
        return 2
    try:
        loaded = load_config(args.config)
        configured = linear_route(loaded[0]) if loaded else None
        if configured:
            if args.team_id and args.team_id != configured["team_id"]:
                raise LinearTransportError("explicit Linear team_id conflicts with workstream config")
            if args.project_id and args.project_id != configured["project_id"]:
                raise LinearTransportError("explicit Linear project_id conflicts with workstream config")
        team_id = configured["team_id"] if configured else args.team_id
        project_id = configured["project_id"] if configured else args.project_id
        result = inspect_route(
            HttpGraphQLClient(token, args.endpoint),
            team_id=team_id,
            project_id=project_id,
        )
    except (OSError, LinearTransportError, ValueError) as error:
        print(f"workstream-linear-setup: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
