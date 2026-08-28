#!/usr/bin/env python3
"""Small authenticated Linear GraphQL transport for reviewed workstream graphs.

The transport is intentionally dependency-free and accepts an injectable
GraphQL client for deterministic tests.  It never creates children before the
candidate review gate and deduplicates by stable marker, not title or cwd.
Linear's API has no conditional-update primitive, so revision-fenced overwrites
fail closed. A future serialized or append-only adapter must provide the remote
CAS/replay boundary before concurrent mutations can be enabled.
"""

from __future__ import annotations

import json
import re
import ssl
import urllib.request
from typing import Any, Protocol

from workstream_http import default_ssl_context
from workstream_graph import GraphReviewRequired, build_operations


MARKER = re.compile(r"<!-- workstream-key:([^ >]+) -->")
PLAN_REVISION = re.compile(
    r"(?:^Plan revision:\s*`?([^`\s]+)`?\s*$|"
    r"\bplan revision SHA-256\s+`?([a-f0-9]{64})`?)",
    re.IGNORECASE | re.MULTILINE,
)
ROOT_REVISION = re.compile(
    r"^Ledger(?: CAS)? revision:\s*(\d+)\s*(?:\([^\n]*\))?\.?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
NEXT_ACTION = re.compile(
    r"(?:^|(?<=\.)[ \t]+)(?:[-*]\s+)?(?:\*\*)?(?:Current\s+)?next action"
    r"(?:\s*\([^)]*\))?\s*:\s*(?:\*\*)?\s*(.+?)\s*(?:\*\*)?\s*$",
    re.IGNORECASE | re.MULTILINE,
)


class LinearTransportError(RuntimeError):
    pass


class GraphQLClient(Protocol):
    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]: ...


class HttpGraphQLClient:
    def __init__(
        self,
        token: str,
        endpoint: str = "https://api.linear.app/graphql",
        *,
        ssl_context: ssl.SSLContext | None = None,
    ):
        if not token:
            raise ValueError("Linear API token is required")
        self.token = token
        self.endpoint = endpoint
        self.ssl_context = ssl_context or default_ssl_context()

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps({"query": query, "variables": variables}).encode()
        request = urllib.request.Request(
            self.endpoint, data=body,
            headers={"Authorization": self.token, "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(
            request, timeout=20, context=self.ssl_context
        ) as response:
            payload = json.load(response)
        if payload.get("errors"):
            raise LinearTransportError(json.dumps(payload["errors"], sort_keys=True))
        return payload.get("data", {})


ISSUES_QUERY = """
query WorkstreamIssues($teamId: String!, $after: String) {
  team(id: $teamId) {
    issues(first: 250, after: $after) {
      nodes {
        id identifier title description url updatedAt
        parent { id identifier }
        project { id }
        state { name type }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

ROUTE_QUERY = """
query WorkstreamRoute($teamId: String!, $projectId: String!) {
  team(id: $teamId) { id organization { id } }
  project(id: $projectId) { id teams { nodes { id } } }
}
"""

TOKEN_ROUTE_QUERY = """
query WorkstreamTokenRoute($issueId: String!) {
  issue(id: $issueId) {
    id identifier
    team { id organization { id } }
    project { id teams { nodes { id } } }
  }
}
"""

CREATE_MUTATION = """
mutation WorkstreamIssueCreate($input: IssueCreateInput!) {
  issueCreate(input: $input) { success issue { id identifier title description url updatedAt } }
}
"""

def marker(key: str) -> str:
    return f"<!-- workstream-key:{key} -->"


def issue_key(issue: dict[str, Any]) -> str | None:
    match = MARKER.search(issue.get("description") or "")
    return match.group(1) if match else None


def parse_next_action(description: str | None) -> str | None:
    """Read the durable next-action field from plain or Markdown issue text."""
    match = NEXT_ACTION.search(description or "")
    if not match:
        return None
    value = match.group(1).strip()
    if value.endswith("**"):
        value = value[:-2].rstrip()
    return value or None


def parse_plan_revision(description: str | None) -> str | None:
    match = PLAN_REVISION.search(description or "")
    return next((value for value in match.groups() if value), None) if match else None


def parse_root_revision(description: str | None) -> int:
    match = ROOT_REVISION.search(description or "")
    return int(match.group(1)) if match else 0


def validate_issue_route(
    issue: dict[str, Any],
    *,
    workspace_id: str | None,
    team_id: str | None,
    project_id: str | None,
) -> None:
    route = (workspace_id, team_id, project_id)
    if not any(route):
        return
    if not all(route):
        raise ValueError("Linear workspace, team, and project IDs must be supplied together")
    team = issue.get("team") or {}
    if team.get("id") != team_id:
        raise LinearTransportError("Linear issue is not in the configured team")
    if (team.get("organization") or {}).get("id") != workspace_id:
        raise LinearTransportError("Linear issue is not in the configured workspace")
    if (issue.get("project") or {}).get("id") != project_id:
        raise LinearTransportError("Linear issue is not in the configured project")


def bootstrap_linear_route(client: GraphQLClient, token: str) -> dict[str, str]:
    """Resolve a full authenticated route from one unambiguous issue token."""
    normalized = token.upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", normalized):
        raise LinearTransportError("invalid Linear issue token")
    issue = client.execute(TOKEN_ROUTE_QUERY, {"issueId": normalized}).get("issue")
    if not isinstance(issue, dict) or str(issue.get("identifier", "")).upper() != normalized:
        raise LinearTransportError("Linear workstream issue not found")
    team = issue.get("team") or {}
    project = issue.get("project") or {}
    workspace_id = (team.get("organization") or {}).get("id")
    team_id = team.get("id")
    project_id = project.get("id")
    project_teams = {
        item.get("id") for item in ((project.get("teams") or {}).get("nodes") or [])
    }
    if not all(isinstance(value, str) and value for value in (workspace_id, team_id, project_id)):
        raise LinearTransportError("Linear issue has no complete workspace/team/project route")
    if team_id not in project_teams:
        raise LinearTransportError("Linear issue project is not associated with its team")
    return {
        "workspace_id": workspace_id,
        "team_id": team_id,
        "project_id": project_id,
        "root_issue_id": issue["id"],
    }


def resolve_authenticated_issue_route(
    client: GraphQLClient, token: str,
    configured_route: dict[str, str] | None,
) -> dict[str, str]:
    """Bind configured routing to the authenticated issue, including its UUID."""
    observed = bootstrap_linear_route(client, token)
    if configured_route:
        for field in ("workspace_id", "team_id", "project_id"):
            if configured_route.get(field) != observed[field]:
                raise LinearTransportError(f"configured Linear route mismatches root:{field}")
    return observed


def durable_description(
    key: str,
    plan_revision: str,
    *,
    next_action: str | None = None,
    ledger_revision: int | None = None,
) -> str:
    lines = [marker(key), f"Plan revision: {plan_revision}"]
    if ledger_revision is not None:
        lines.append(f"Ledger revision: {ledger_revision}")
    if next_action:
        lines.append(f"Current next action: {next_action}")
    return "\n".join(lines)


class LinearGraphQLTransport:
    def __init__(
        self,
        client: GraphQLClient,
        *,
        team_id: str,
        workspace_id: str | None = None,
        project_id: str | None = None,
    ):
        if bool(workspace_id) != bool(project_id):
            raise ValueError("Linear workspace_id and project_id must be supplied together")
        self.client = client
        self.team_id = team_id
        self.workspace_id = workspace_id
        self.project_id = project_id
        self._route_verified = False

    @classmethod
    def from_config(
        cls,
        client: GraphQLClient,
        config_path: str | None = None,
    ) -> "LinearGraphQLTransport":
        from workstream_config import resolve_linear_route

        route, _resolved = resolve_linear_route(config_path=config_path)
        if not route or not route.get("workspace_id") or not route.get("project_id"):
            raise ValueError("a complete workstream config Linear route is required")
        return cls(
            client,
            team_id=route["team_id"],
            workspace_id=route["workspace_id"],
            project_id=route["project_id"],
        )

    def _ensure_route(self) -> None:
        if not self.project_id or self._route_verified:
            return
        result = self.client.execute(
            ROUTE_QUERY, {"teamId": self.team_id, "projectId": self.project_id}
        )
        team = result.get("team")
        project = result.get("project")
        if not isinstance(team, dict) or team.get("id") != self.team_id:
            raise LinearTransportError("configured Linear team was not found")
        if (team.get("organization") or {}).get("id") != self.workspace_id:
            raise LinearTransportError("configured Linear team is not in the configured workspace")
        if not isinstance(project, dict) or project.get("id") != self.project_id:
            raise LinearTransportError("configured Linear project was not found")
        project_team_ids = {
            item.get("id") for item in ((project.get("teams") or {}).get("nodes") or [])
        }
        if self.team_id not in project_team_ids:
            raise LinearTransportError("configured Linear project is not associated with the configured team")
        self._route_verified = True

    def _create_input(self, **values: Any) -> dict[str, Any]:
        payload = {"teamId": self.team_id, **values}
        if self.project_id:
            payload["projectId"] = self.project_id
        return payload

    def snapshot(self) -> dict[str, Any]:
        self._ensure_route()
        issues: list[dict[str, Any]] = []
        after: str | None = None
        seen_cursors: set[str] = set()
        while True:
            result = self.client.execute(
                ISSUES_QUERY, {"teamId": self.team_id, "after": after}
            )
            team = result.get("team")
            if not isinstance(team, dict):
                raise LinearTransportError("configured Linear team was not found")
            connection = team.get("issues", {})
            nodes = connection.get("nodes", [])
            if self.project_id:
                nodes = [
                    issue for issue in nodes
                    if (issue.get("project") or {}).get("id") == self.project_id
                ]
            issues.extend(nodes)
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                return {"issues": issues}
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after or after in seen_cursors:
                raise LinearTransportError("invalid Linear pagination cursor")
            seen_cursors.add(after)

    def snapshot_for_root(self, token: str) -> dict[str, Any]:
        """Build the bounded resume snapshot for one GEN root from live Linear."""
        token = token.upper()
        issues = self.snapshot()["issues"]
        root = next((issue for issue in issues if str(issue.get("identifier", "")).upper() == token), None)
        if not root:
            raise LinearTransportError(f"Linear root not found: {token}")
        description = root.get("description") or ""
        plan_revision = parse_plan_revision(description)
        children = []
        for issue in issues:
            if (issue.get("parent") or {}).get("id") != root.get("id"):
                continue
            child = dict(issue)
            state = child.pop("state", None) or {}
            child["status"] = state.get("name") or state.get("type") or "Todo"
            child["next_action"] = parse_next_action(child.get("description"))
            children.append(child)
        root_state = root.get("state") or {}
        return {
            "root": {
                "identifier": root["identifier"], "url": root.get("url"),
                "plan_revision": plan_revision,
                "revision": parse_root_revision(description),
                "status": root_state.get("name") or root_state.get("type"),
                "next_action": parse_next_action(description),
            },
            "children": children,
            "decisions": [], "provenance": [],
        }

    def apply_reviewed_plan(
        self, plan: dict[str, Any], *, accepted_keys: set[str],
        expected_revision: int | None = None,
    ) -> dict[str, Any]:
        existing = self.snapshot()["issues"]
        root_key = str(plan.get("root", {}).get("stable_key", ""))
        matching_roots = [i for i in existing if issue_key(i) == root_key]
        if len(matching_roots) > 1:
            raise LinearTransportError("duplicate_workstream_root")
        existing_root = matching_roots[0] if matching_roots else None
        children = [i for i in existing if (i.get("parent") or {}).get("id") == (existing_root or {}).get("id")]
        child_keys = [key for key in (issue_key(i) for i in children) if key]
        if len(child_keys) != len(set(child_keys)):
            raise LinearTransportError("duplicate_workstream_child")
        operations = build_operations(
            plan,
            existing_root=existing_root,
            existing_children=[{**i, "stable_key": issue_key(i)} for i in children],
            accepted_keys=accepted_keys,
        )
        if expected_revision is not None:
            raise LinearTransportError("remote_cas_unavailable")
        if existing_root:
            current_revision = parse_plan_revision(existing_root.get("description"))
            plan_revision = plan["root"]["plan_revision"]
            has_missing_child = any(
                operation["action"] == "create_child" for operation in operations
            )
            if current_revision == plan_revision and not has_missing_child:
                return {"root": existing_root, "issues": []}
            raise LinearTransportError("remote_cas_unavailable")
        applied: list[dict[str, Any]] = []
        root_id = None
        for operation in operations:
            key = operation["stable_key"]
            description = durable_description(
                key,
                plan["root"]["plan_revision"],
                next_action=operation.get("next_action"),
                ledger_revision=0 if operation["action"] == "create_root" else None,
            )
            if operation["action"] == "create_root":
                response = self.client.execute(CREATE_MUTATION, {"input": self._create_input(
                    title=operation["title"], description=description,
                )})
                issue = response.get("issueCreate", {}).get("issue")
                if not issue:
                    raise LinearTransportError("Linear root creation returned no issue")
                root_id = issue["id"]
            elif operation["action"] == "create_child":
                response = self.client.execute(CREATE_MUTATION, {"input": self._create_input(
                    title=operation["title"], description=description, parentId=root_id,
                )})
                issue = response.get("issueCreate", {}).get("issue")
            else:
                raise LinearTransportError("remote_cas_unavailable")
            if not issue:
                raise LinearTransportError(f"Linear mutation returned no issue for {key}")
            applied.append(issue)
        return {"root": next((i for i in applied if issue_key(i) == root_key), existing_root), "issues": applied}
