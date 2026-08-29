#!/usr/bin/env python3
"""Small authenticated Linear GraphQL transport for reviewed workstream graphs.

The transport is intentionally dependency-free and accepts an injectable
GraphQL client for deterministic tests.  It never creates children before the
candidate review gate and deduplicates by stable marker, not title or cwd.
Linear's API has no conditional-update primitive, so revision-fenced overwrites
fail closed. Initial reviewed intake is concurrency-safe because its immutable
issue identities are deterministic; mutable updates still require remote CAS.
"""

from __future__ import annotations

import json
import re
import ssl
import urllib.request
import uuid
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
        team { id organization { id } }
        state { name type }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

RESUME_ROOT_QUERY = """
query WorkstreamResumeRoot($issueId: String!, $after: String) {
  issue(id: $issueId) {
    id identifier title description url updatedAt
    project { id }
    team { id organization { id } }
    state { name type }
    children(first: 250, after: $after) {
      nodes {
        id identifier title description url updatedAt
        parent { id identifier }
        project { id }
        team { id organization { id } }
        state { name type }
        comments(first: 250) {
          nodes { id body createdAt updatedAt }
          pageInfo { hasNextPage endCursor }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

RESUME_COMMENTS_QUERY = """
query WorkstreamResumeComments($issueId: String!, $after: String) {
  issue(id: $issueId) {
    id identifier
    team { id organization { id } }
    project { id }
    comments(first: 250, after: $after) {
      nodes { id body createdAt updatedAt }
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
  issueCreate(input: $input) {
    success
    issue {
      id identifier title description url updatedAt
      parent { id identifier }
      project { id }
      team { id organization { id } }
    }
  }
}
"""

ISSUE_ID_NAMESPACE = uuid.UUID("3ce600c8-3d41-4b3c-b399-cc9d54629335")


def deterministic_issue_id(
    *, workspace_id: str, team_id: str, project_id: str,
    root_stable_key: str, child_stable_key: str | None = None,
) -> str:
    """Return a deterministic UUIDv4-shaped Linear intake identity.

    Linear accepts client-supplied issue IDs but its live validator requires
    UUID version 4. Derive all random payload bits from the immutable route and
    plan keys, then set only the RFC 4122 version and variant bits. This keeps
    concurrent creators on one stable ID without depending on UUIDv5 support.
    """
    fields = (workspace_id, team_id, project_id, root_stable_key)
    if any(not isinstance(value, str) or not value.strip() for value in fields):
        raise ValueError("deterministic Linear issue identity needs a complete route and root key")
    if child_stable_key is not None and (
        not isinstance(child_stable_key, str) or not child_stable_key.strip()
    ):
        raise ValueError("deterministic Linear child identity needs a stable key")
    material = json.dumps(
        ["workstream-issue-v1", *fields, child_stable_key],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    deterministic = uuid.uuid5(ISSUE_ID_NAMESPACE, material)
    return str(uuid.UUID(bytes=deterministic.bytes, version=4))

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

    def _issue_snapshot(self, query: str) -> dict[str, Any]:
        self._ensure_route()
        issues: list[dict[str, Any]] = []
        after: str | None = None
        seen_cursors: set[str] = set()
        while True:
            result = self.client.execute(
                query, {"teamId": self.team_id, "after": after}
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

    def snapshot(self) -> dict[str, Any]:
        return self._issue_snapshot(ISSUES_QUERY)

    def _remaining_resume_comments(
        self, issue: dict[str, Any], page_info: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if not isinstance(page_info, dict) or not isinstance(
            page_info.get("hasNextPage"), bool,
        ):
            raise LinearTransportError("invalid Linear comment page info")
        comments: list[dict[str, Any]] = []
        after = page_info.get("endCursor")
        seen_cursors: set[str] = set()
        while page_info.get("hasNextPage"):
            if not isinstance(after, str) or not after or after in seen_cursors:
                raise LinearTransportError("invalid Linear comment pagination cursor")
            seen_cursors.add(after)
            result = self.client.execute(
                RESUME_COMMENTS_QUERY,
                {"issueId": issue.get("identifier"), "after": after},
            )
            observed = result.get("issue")
            if not isinstance(observed, dict):
                raise LinearTransportError("Linear workstream child not found")
            if (
                observed.get("id") != issue.get("id")
                or observed.get("identifier") != issue.get("identifier")
            ):
                raise LinearTransportError("workstream_child_identity_mismatch")
            if self.workspace_id and self.team_id and self.project_id:
                validate_issue_route(
                    observed, workspace_id=self.workspace_id, team_id=self.team_id,
                    project_id=self.project_id,
                )
            connection = observed.get("comments")
            if not isinstance(connection, dict):
                raise LinearTransportError("invalid Linear comment connection")
            nodes = connection.get("nodes")
            if not isinstance(nodes, list):
                raise LinearTransportError("invalid Linear comment connection")
            comments.extend(nodes)
            page_info = connection.get("pageInfo")
            if not isinstance(page_info, dict) or not isinstance(
                page_info.get("hasNextPage"), bool,
            ):
                raise LinearTransportError("invalid Linear comment page info")
            after = page_info.get("endCursor")
        return comments

    def _resume_root_with_children(self, token: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        self._ensure_route()
        root: dict[str, Any] | None = None
        children: list[dict[str, Any]] = []
        after: str | None = None
        seen_cursors: set[str] = set()
        while True:
            result = self.client.execute(
                RESUME_ROOT_QUERY, {"issueId": token, "after": after},
            )
            observed = result.get("issue")
            if not isinstance(observed, dict):
                raise LinearTransportError(f"Linear root not found: {token}")
            if str(observed.get("identifier", "")).upper() != token:
                raise LinearTransportError("workstream_id_mismatch")
            if self.workspace_id and self.team_id and self.project_id:
                validate_issue_route(
                    observed, workspace_id=self.workspace_id, team_id=self.team_id,
                    project_id=self.project_id,
                )
            if root is None:
                root = {key: value for key, value in observed.items() if key != "children"}
            elif observed.get("id") != root.get("id"):
                raise LinearTransportError("workstream_root_identity_mismatch")
            connection = observed.get("children")
            if not isinstance(connection, dict):
                raise LinearTransportError("invalid Linear child connection")
            nodes = connection.get("nodes")
            if not isinstance(nodes, list):
                raise LinearTransportError("invalid Linear child connection")
            children.extend(nodes)
            page_info = connection.get("pageInfo")
            if not isinstance(page_info, dict) or not isinstance(
                page_info.get("hasNextPage"), bool,
            ):
                raise LinearTransportError("invalid Linear child page info")
            if not page_info.get("hasNextPage"):
                return root, children
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after or after in seen_cursors:
                raise LinearTransportError("invalid Linear child pagination cursor")
            seen_cursors.add(after)

    def snapshot_for_root(
        self, token: str, *, include_child_comments: bool = False,
        include_description: bool = False,
    ) -> dict[str, Any]:
        """Build the bounded resume snapshot for one GEN root from live Linear."""
        token = token.upper()
        if include_child_comments:
            root, owned_children = self._resume_root_with_children(token)
            issues = [root, *owned_children]
        else:
            issues = self._issue_snapshot(ISSUES_QUERY)["issues"]
            root = next((
                issue for issue in issues
                if str(issue.get("identifier", "")).upper() == token
            ), None)
        if not root:
            raise LinearTransportError(f"Linear root not found: {token}")
        description = root.get("description") or ""
        plan_revision = parse_plan_revision(description)
        children = []
        child_comments: dict[str, list[dict[str, Any]]] = {}
        for issue in issues:
            if (issue.get("parent") or {}).get("id") != root.get("id"):
                continue
            child = dict(issue)
            state = child.pop("state", None) or {}
            child["status"] = state.get("name") or state.get("type") or "Todo"
            child["status_type"] = state.get("type")
            child["next_action"] = parse_next_action(child.get("description"))
            comment_connection = child.pop("comments", None)
            terminal = {
                str(child.get("status", "")).lower(),
                str(child.get("status_type", "")).lower(),
            } & {"done", "completed", "cancelled", "canceled", "superseded"}
            if include_child_comments and not terminal:
                if not isinstance(comment_connection, dict):
                    raise LinearTransportError("missing Linear child comment connection")
                nodes = comment_connection.get("nodes")
                if not isinstance(nodes, list):
                    raise LinearTransportError("invalid Linear child comment connection")
                page_info = comment_connection.get("pageInfo")
                if not isinstance(page_info, dict) or not isinstance(
                    page_info.get("hasNextPage"), bool,
                ):
                    raise LinearTransportError("invalid Linear comment page info")
                child_comments[str(child.get("identifier", "")).upper()] = [
                    *nodes,
                    *self._remaining_resume_comments(child, page_info),
                ]
            children.append(child)
        root_state = root.get("state") or {}
        result = {
            "root": {
                "identifier": root["identifier"], "url": root.get("url"),
                "plan_revision": plan_revision,
                "revision": parse_root_revision(description),
                "status": root_state.get("name") or root_state.get("type"),
                "status_type": root_state.get("type"),
                "next_action": parse_next_action(description),
            },
            "children": children,
            "decisions": [], "provenance": [],
        }
        if include_child_comments:
            result["child_comments"] = child_comments
        if include_description:
            result["root"]["description"] = description
        return result

    def _intake_route(self) -> dict[str, str]:
        if not self.workspace_id or not self.project_id:
            raise LinearTransportError(
                "concurrent intake requires explicit workspace, team, and project IDs"
            )
        return {
            "workspace_id": self.workspace_id,
            "team_id": self.team_id,
            "project_id": self.project_id,
        }

    def _validate_intake_issue(
        self,
        issue: dict[str, Any],
        *,
        issue_id: str,
        stable_key: str,
        title: str,
        plan_revision: str,
        parent_id: str | None,
    ) -> None:
        """Validate every intake-owned immutable field after create or reload."""
        route = self._intake_route()
        description = issue.get("description") or ""
        markers = MARKER.findall(description)
        revisions = [
            value
            for match in PLAN_REVISION.findall(description)
            for value in match
            if value
        ]
        if markers != [stable_key]:
            raise LinearTransportError(
                f"intake_identity_collision:{stable_key}:stable_key"
            )
        if revisions != [plan_revision]:
            raise LinearTransportError(
                f"intake_identity_collision:{stable_key}:plan_revision"
            )
        observed = {
            "id": issue.get("id"),
            "title": issue.get("title"),
            "parent_id": (issue.get("parent") or {}).get("id"),
            "project_id": (issue.get("project") or {}).get("id"),
            "team_id": (issue.get("team") or {}).get("id"),
            "workspace_id": ((issue.get("team") or {}).get("organization") or {}).get("id"),
        }
        expected = {
            "id": issue_id,
            "title": title,
            "parent_id": parent_id,
            "project_id": route["project_id"],
            "team_id": route["team_id"],
            "workspace_id": route["workspace_id"],
        }
        for field, value in expected.items():
            if observed[field] != value:
                raise LinearTransportError(
                    f"intake_identity_collision:{stable_key}:{field}"
                )

    def _create_or_converge(
        self,
        *,
        issue_id: str,
        stable_key: str,
        title: str,
        description: str,
        plan_revision: str,
        parent_id: str | None,
    ) -> tuple[dict[str, Any], str]:
        values: dict[str, Any] = {
            "id": issue_id,
            "title": title,
            "description": description,
        }
        if parent_id is not None:
            values["parentId"] = parent_id
        issue: dict[str, Any] | None = None
        failure: Exception | None = None
        try:
            response = self.client.execute(
                CREATE_MUTATION, {"input": self._create_input(**values)}
            )
            candidate = response.get("issueCreate", {}).get("issue")
            if isinstance(candidate, dict):
                issue = candidate
        except (LinearTransportError, OSError, TimeoutError, ValueError) as error:
            # The server may have committed before the response failed. The
            # deterministic UUID makes a complete reload the only safe oracle.
            failure = error
        if issue is not None:
            self._validate_intake_issue(
                issue,
                issue_id=issue_id,
                stable_key=stable_key,
                title=title,
                plan_revision=plan_revision,
                parent_id=parent_id,
            )
            return issue, "created"

        try:
            reloaded = self.snapshot()["issues"]
        except Exception as reload_error:
            raise LinearTransportError(
                f"intake_create_unconfirmed:{stable_key}"
            ) from reload_error
        issue = next((item for item in reloaded if item.get("id") == issue_id), None)
        if not isinstance(issue, dict):
            raise LinearTransportError(
                f"intake_create_unconfirmed:{stable_key}"
            ) from failure
        self._validate_intake_issue(
            issue,
            issue_id=issue_id,
            stable_key=stable_key,
            title=title,
            plan_revision=plan_revision,
            parent_id=parent_id,
        )
        return issue, "converged"

    @staticmethod
    def _intake_receipt(
        issue: dict[str, Any], *, stable_key: str, disposition: str,
    ) -> dict[str, Any]:
        return {
            "stable_key": stable_key,
            "id": issue["id"],
            "identifier": issue.get("identifier"),
            "url": issue.get("url"),
            "title": issue.get("title"),
            "parent_id": (issue.get("parent") or {}).get("id"),
            "updated_at": issue.get("updatedAt"),
            "disposition": disposition,
        }

    def intake_reviewed_plan(
        self, plan: dict[str, Any], *, accepted_keys: set[str] | None,
    ) -> dict[str, Any]:
        """Create or converge one reviewed same-plan graph without remote updates."""
        # Review is an authorization boundary. Validate it before auth, route
        # discovery, or any other network call.
        operations = build_operations(plan, accepted_keys=accepted_keys)
        route = self._intake_route()
        self._ensure_route()
        root_operation = operations[0]
        root_key = root_operation["stable_key"]
        plan_revision = root_operation["plan_revision"]
        root_id = deterministic_issue_id(
            **route, root_stable_key=root_key
        )
        existing = self.snapshot()["issues"]
        matching_roots = [item for item in existing if issue_key(item) == root_key]
        if len(matching_roots) > 1:
            raise LinearTransportError("duplicate_workstream_root")
        root = matching_roots[0] if matching_roots else None
        root_disposition = "existing"
        if root is not None:
            if parse_plan_revision(root.get("description")) != plan_revision:
                raise LinearTransportError("remote_cas_unavailable")
            self._validate_intake_issue(
                root,
                issue_id=root_id,
                stable_key=root_key,
                title=root_operation["title"],
                plan_revision=plan_revision,
                parent_id=None,
            )
        else:
            occupied = next((item for item in existing if item.get("id") == root_id), None)
            if occupied is not None:
                self._validate_intake_issue(
                    occupied,
                    issue_id=root_id,
                    stable_key=root_key,
                    title=root_operation["title"],
                    plan_revision=plan_revision,
                    parent_id=None,
                )
                root, root_disposition = occupied, "converged"
            else:
                root, root_disposition = self._create_or_converge(
                    issue_id=root_id,
                    stable_key=root_key,
                    title=root_operation["title"],
                    description=durable_description(
                        root_key,
                        plan_revision,
                        next_action=root_operation.get("next_action"),
                        ledger_revision=0,
                    ),
                    plan_revision=plan_revision,
                    parent_id=None,
                )

        child_receipts: list[dict[str, Any]] = []
        applied: list[dict[str, Any]] = (
            [root] if root_disposition != "existing" else []
        )
        for operation in operations[1:]:
            key = operation["stable_key"]
            child_id = deterministic_issue_id(
                **route, root_stable_key=root_key, child_stable_key=key
            )
            current = self.snapshot()["issues"]
            root_children = [
                item for item in current
                if (item.get("parent") or {}).get("id") == root_id
            ]
            matching = [item for item in root_children if issue_key(item) == key]
            if len(matching) > 1:
                raise LinearTransportError("duplicate_workstream_child")
            child = matching[0] if matching else None
            disposition = "existing"
            if child is not None:
                self._validate_intake_issue(
                    child,
                    issue_id=child_id,
                    stable_key=key,
                    title=operation["title"],
                    plan_revision=plan_revision,
                    parent_id=root_id,
                )
            else:
                occupied = next((item for item in current if item.get("id") == child_id), None)
                if occupied is not None:
                    self._validate_intake_issue(
                        occupied,
                        issue_id=child_id,
                        stable_key=key,
                        title=operation["title"],
                        plan_revision=plan_revision,
                        parent_id=root_id,
                    )
                    child, disposition = occupied, "converged"
                else:
                    child, disposition = self._create_or_converge(
                        issue_id=child_id,
                        stable_key=key,
                        title=operation["title"],
                        description=durable_description(
                            key,
                            plan_revision,
                            next_action=operation.get("next_action"),
                        ),
                        plan_revision=plan_revision,
                        parent_id=root_id,
                    )
            if disposition != "existing":
                applied.append(child)
            child_receipts.append(
                self._intake_receipt(child, stable_key=key, disposition=disposition)
            )

        # A full final readback is the receipt authority; mutation responses are
        # never enough to claim that the graph converged.
        final = self.snapshot()["issues"]
        final_root = next((item for item in final if item.get("id") == root_id), None)
        if not isinstance(final_root, dict):
            raise LinearTransportError("intake_readback_missing_root")
        self._validate_intake_issue(
            final_root,
            issue_id=root_id,
            stable_key=root_key,
            title=root_operation["title"],
            plan_revision=plan_revision,
            parent_id=None,
        )
        for receipt, operation in zip(child_receipts, operations[1:]):
            child = next((item for item in final if item.get("id") == receipt["id"]), None)
            if not isinstance(child, dict):
                raise LinearTransportError(
                    f"intake_readback_missing_child:{receipt['stable_key']}"
                )
            self._validate_intake_issue(
                child,
                issue_id=receipt["id"],
                stable_key=receipt["stable_key"],
                title=operation["title"],
                plan_revision=plan_revision,
                parent_id=root_id,
            )
        root_receipt = self._intake_receipt(
            final_root, stable_key=root_key, disposition=root_disposition
        )
        return {
            "schema_version": 1,
            "plan_revision": plan_revision,
            "route": route,
            "root": final_root,
            "issues": applied,
            "receipts": {"root": root_receipt, "children": child_receipts},
        }

    def apply_reviewed_plan(
        self, plan: dict[str, Any], *, accepted_keys: set[str],
        expected_revision: int | None = None,
    ) -> dict[str, Any]:
        if self.workspace_id and self.project_id and expected_revision is None:
            return self.intake_reviewed_plan(plan, accepted_keys=accepted_keys)
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
