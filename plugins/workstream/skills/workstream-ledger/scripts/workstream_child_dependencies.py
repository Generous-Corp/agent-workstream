#!/usr/bin/env python3
"""Fenced, idempotent Linear dependencies between existing owned children.

The append-only ``child_dependency_authorization`` event is the immutable
dependency authority. Linear's client-supplied relation UUID is an idempotent
derived native cache slot, verified through the blocker's ``relations`` and the
blocked child's ``inverseRelations`` connections. Later events cannot
retroactively invalidate a won authorization; contradictions require explicit
append-only supersession and reconciliation. This transport never creates or
updates an issue, project, or workstream root.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import re
import sys
import uuid
from typing import Any

from workstream_linear import (
    GraphQLClient, HttpGraphQLClient, LinearGraphQLTransport, LinearTransportError,
    parse_plan_revision, validate_issue_route,
)
from workstream_config import load_linear_api_key
from workstream_linear_events import LinearCommentEventAdapter, reduce_event_comments
from workstream_linear_projection import LinearProjectionAdapter, reduce_projection_comments


ISSUE_TOKEN = re.compile(r"[A-Z][A-Z0-9]*-\d+")
UUID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}", re.IGNORECASE,
)
FRONTIER_FIELDS = {
    "material_revision", "projection_revision", "graph_revision", "graph_sha256",
}
AUTHORITY_FIELDS = {
    "workspace_id", "team_id", "project_id", "root_issue_id", "root_identifier",
}

RELATION_CAPABILITY_QUERY = """
query WorkstreamChildDependencyCapabilities {
  relationInput: __type(name: "IssueRelationCreateInput") {
    inputFields { name }
  }
  relationType: __type(name: "IssueRelationType") {
    enumValues { name }
  }
  issueType: __type(name: "Issue") {
    fields { name }
  }
  queryType: __type(name: "Query") {
    fields { name args { name } }
  }
}
"""

RELATION_CREATE_MUTATION = """
mutation WorkstreamChildDependencyCreate($input: IssueRelationCreateInput!) {
  issueRelationCreate(input: $input) {
    success
    issueRelation {
      id type archivedAt
      issue { id identifier }
      relatedIssue { id identifier }
    }
  }
}
"""

RELATIONS_QUERY = """
query WorkstreamChildRelations($issueId: String!, $after: String) {
  issue(id: $issueId) {
    id identifier parent { id identifier }
    team { id organization { id } }
    project { id }
    relations(first: 250, after: $after, includeArchived: true) {
      nodes {
        id type archivedAt
        issue { id identifier parent { id } team { id organization { id } } project { id } }
        relatedIssue { id identifier parent { id } team { id organization { id } } project { id } }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

INVERSE_RELATIONS_QUERY = """
query WorkstreamChildInverseRelations($issueId: String!, $after: String) {
  issue(id: $issueId) {
    id identifier parent { id identifier }
    team { id organization { id } }
    project { id }
    inverseRelations(first: 250, after: $after, includeArchived: true) {
      nodes {
        id type archivedAt
        issue { id identifier parent { id } team { id organization { id } } project { id } }
        relatedIssue { id identifier parent { id } team { id organization { id } } project { id } }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

ALL_RELATION_SLOTS_QUERY = """
query WorkstreamChildDependencySlots($after: String) {
  issueRelations(first: 250, after: $after, includeArchived: true) {
    nodes {
      id type archivedAt
      issue { id identifier }
      relatedIssue { id identifier }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


class ChildDependencyError(LinearTransportError):
    """The native dependency graph cannot be mutated or reduced safely."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _uuid4_from(value: Any) -> str:
    raw = bytearray(hashlib.sha256(_canonical(value)).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def _validate_identity(value: Any, *, label: str) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {"issue_id", "identifier"}:
        raise ChildDependencyError(f"invalid_{label}_identity")
    issue_id = value.get("issue_id")
    identifier = str(value.get("identifier", "")).upper()
    if not isinstance(issue_id, str) or not UUID.fullmatch(issue_id):
        raise ChildDependencyError(f"invalid_{label}_issue_id")
    if not ISSUE_TOKEN.fullmatch(identifier):
        raise ChildDependencyError(f"invalid_{label}_identifier")
    return {"issue_id": issue_id, "identifier": identifier}


def _validate_authority(value: Any) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != AUTHORITY_FIELDS:
        raise ChildDependencyError("invalid_dependency_authority")
    if not all(isinstance(value.get(field), str) and value[field]
               for field in AUTHORITY_FIELDS):
        raise ChildDependencyError("invalid_dependency_authority")
    result = dict(value)
    result["root_identifier"] = result["root_identifier"].upper()
    if not UUID.fullmatch(result["root_issue_id"]):
        raise ChildDependencyError("invalid_dependency_root_issue_id")
    if not ISSUE_TOKEN.fullmatch(result["root_identifier"]):
        raise ChildDependencyError("invalid_dependency_root_identifier")
    return result


def dependency_relation_id(
    *, authority: dict[str, str], blocker: dict[str, str], blocked: dict[str, str],
) -> str:
    """Return one stable Linear UUIDv4 slot for the directed child edge."""
    authority = _validate_authority(authority)
    blocker = _validate_identity(blocker, label="blocker")
    blocked = _validate_identity(blocked, label="blocked")
    if blocker == blocked:
        raise ChildDependencyError("self_dependency")
    return _uuid4_from([
        "workstream-child-dependency-relation-v1", authority,
        blocker["issue_id"], blocked["issue_id"], "blocks",
    ])


@dataclass(frozen=True)
class DependencyGraph:
    relations: tuple[dict[str, Any], ...]
    ignored_non_dependency_count: int

    @property
    def revision(self) -> int:
        return len(self.relations)


def reduce_dependency_readback(
    surfaces: dict[str, dict[str, list[dict[str, Any]]]], *,
    authority: dict[str, str], owned_children: dict[str, dict[str, str]],
) -> DependencyGraph:
    """Verify every native ``blocks`` relation and its exact inverse surface."""
    authority = _validate_authority(authority)
    if set(surfaces) != set(owned_children):
        raise ChildDependencyError("incomplete_dependency_readback")
    observed: dict[str, tuple[dict[str, Any], set[tuple[str, str]]]] = {}
    ignored = 0
    for owner_id, connections in surfaces.items():
        if not isinstance(connections, dict) or set(connections) != {
            "relations", "inverse_relations",
        }:
            raise ChildDependencyError("invalid_dependency_readback")
        for connection_name, records in connections.items():
            if not isinstance(records, list):
                raise ChildDependencyError("invalid_dependency_readback")
            for raw in records:
                if not isinstance(raw, dict):
                    raise ChildDependencyError("invalid_dependency_relation")
                if raw.get("type") != "blocks":
                    ignored += 1
                    continue
                if raw.get("archivedAt") is not None:
                    raise ChildDependencyError("archived_dependency_in_active_readback")
                relation_id = raw.get("id")
                if not isinstance(relation_id, str) or not UUID.fullmatch(relation_id):
                    raise ChildDependencyError("invalid_dependency_relation_id")
                blocker_raw = raw.get("issue") or {}
                blocked_raw = raw.get("relatedIssue") or {}
                blocker = _validate_identity({
                    "issue_id": blocker_raw.get("id"),
                    "identifier": blocker_raw.get("identifier"),
                }, label="blocker")
                blocked = _validate_identity({
                    "issue_id": blocked_raw.get("id"),
                    "identifier": blocked_raw.get("identifier"),
                }, label="blocked")
                for endpoint, label in ((blocker_raw, "blocker"), (blocked_raw, "blocked")):
                    if (endpoint.get("parent") or {}).get("id") != authority["root_issue_id"]:
                        raise ChildDependencyError(f"cross_root_dependency:{label}")
                    team = endpoint.get("team") or {}
                    if (
                        team.get("id") != authority["team_id"]
                        or (team.get("organization") or {}).get("id")
                        != authority["workspace_id"]
                        or (endpoint.get("project") or {}).get("id")
                        != authority["project_id"]
                    ):
                        raise ChildDependencyError(
                            f"dependency_endpoint_route_mismatch:{label}"
                        )
                if blocker == blocked:
                    raise ChildDependencyError("self_dependency")
                if (
                    blocker["issue_id"] not in owned_children
                    or blocked["issue_id"] not in owned_children
                ):
                    raise ChildDependencyError("cross_root_dependency")
                if (
                    owned_children[blocker["issue_id"]] != blocker
                    or owned_children[blocked["issue_id"]] != blocked
                ):
                    raise ChildDependencyError("dependency_endpoint_identity_mismatch")
                expected_id = dependency_relation_id(
                    authority=authority, blocker=blocker, blocked=blocked,
                )
                if relation_id != expected_id:
                    raise ChildDependencyError(
                        f"non_deterministic_dependency_relation_id:{relation_id}"
                    )
                expected_owner = (
                    blocker["issue_id"] if connection_name == "relations"
                    else blocked["issue_id"]
                )
                if owner_id != expected_owner:
                    raise ChildDependencyError("dependency_direction_mismatch")
                normalized = {
                    "id": relation_id, "type": "blocks", "blocker": blocker,
                    "blocked": blocked, "inverse_type": "blocked_by",
                }
                prior = observed.get(relation_id)
                if prior is None:
                    observed[relation_id] = (
                        normalized, {(owner_id, connection_name)},
                    )
                else:
                    if prior[0] != normalized:
                        raise ChildDependencyError(
                            f"conflicting_dependency_relation_id:{relation_id}"
                        )
                    surface = (owner_id, connection_name)
                    if surface in prior[1]:
                        raise ChildDependencyError(
                            f"duplicate_dependency_relation_surface:{relation_id}"
                        )
                    prior[1].add(surface)

    pair_directions: dict[frozenset[str], tuple[str, str]] = {}
    relations: list[dict[str, Any]] = []
    for relation_id, (relation, relation_surfaces) in observed.items():
        expected_surfaces = {
            (relation["blocker"]["issue_id"], "relations"),
            (relation["blocked"]["issue_id"], "inverse_relations"),
        }
        if relation_surfaces != expected_surfaces:
            raise ChildDependencyError(f"dependency_inverse_missing:{relation_id}")
        direction = (
            relation["blocker"]["issue_id"], relation["blocked"]["issue_id"],
        )
        pair = frozenset(direction)
        previous = pair_directions.get(pair)
        if previous is not None:
            reason = (
                "duplicate_dependency" if previous == direction
                else "conflicting_dependency_direction"
            )
            raise ChildDependencyError(reason)
        pair_directions[pair] = direction
        relations.append(deepcopy(relation))
    return DependencyGraph(
        relations=tuple(sorted(relations, key=lambda item: item["id"])),
        ignored_non_dependency_count=ignored,
    )


class LinearChildDependencyAdapter:
    """Create native Linear child dependencies with deterministic relation IDs."""

    supports_native_linear_relations = True
    supports_append_only_dependency_projection = True

    def __init__(
        self, client: GraphQLClient, *, workspace_id: str, team_id: str,
        project_id: str, root_issue_id: str, root_identifier: str,
        plan_revision: str,
    ):
        self.client = client
        self.authority = _validate_authority({
            "workspace_id": workspace_id, "team_id": team_id,
            "project_id": project_id, "root_issue_id": root_issue_id,
            "root_identifier": root_identifier,
        })
        if not isinstance(plan_revision, str) or not plan_revision:
            raise ChildDependencyError("invalid_dependency_plan_revision")
        self.plan_revision = plan_revision
        self._capability_verified = False
        self.authorization = LinearProjectionAdapter(
            client, issue_id=self.authority["root_identifier"],
            workstream_id=self.authority["root_identifier"],
            plan_revision=plan_revision,
            workspace_id=self.authority["workspace_id"],
            team_id=self.authority["team_id"],
            project_id=self.authority["project_id"],
            root_issue_id=self.authority["root_issue_id"],
        )

    def _assert_native_capability(self) -> None:
        if self._capability_verified:
            return
        result = self.client.execute(RELATION_CAPABILITY_QUERY, {})
        input_fields = ((result.get("relationInput") or {}).get("inputFields") or [])
        enum_values = ((result.get("relationType") or {}).get("enumValues") or [])
        issue_fields = ((result.get("issueType") or {}).get("fields") or [])
        query_fields = ((result.get("queryType") or {}).get("fields") or [])
        if not {"id", "issueId", "relatedIssueId", "type"}.issubset({
            field.get("name") for field in input_fields if isinstance(field, dict)
        }):
            raise ChildDependencyError("linear_relation_id_capability_unavailable")
        if "blocks" not in {
            value.get("name") for value in enum_values if isinstance(value, dict)
        }:
            raise ChildDependencyError("linear_blocks_relation_capability_unavailable")
        if not {"relations", "inverseRelations"}.issubset({
            field.get("name") for field in issue_fields if isinstance(field, dict)
        }):
            raise ChildDependencyError("linear_relation_readback_capability_unavailable")
        issue_relations = next(
            (field for field in query_fields
             if isinstance(field, dict) and field.get("name") == "issueRelations"),
            None,
        )
        if not isinstance(issue_relations, dict) or "includeArchived" not in {
            item.get("name") for item in issue_relations.get("args", [])
            if isinstance(item, dict)
        }:
            raise ChildDependencyError("linear_relation_slot_preflight_unavailable")
        self._capability_verified = True

    def _all_relation_slots(self) -> dict[str, dict[str, Any]]:
        slots: dict[str, dict[str, Any]] = {}
        after: str | None = None
        seen: set[str] = set()
        while True:
            response = self.client.execute(
                ALL_RELATION_SLOTS_QUERY, {"after": after},
            )
            connection = response.get("issueRelations") if isinstance(response, dict) else None
            if not isinstance(connection, dict):
                raise ChildDependencyError("invalid_dependency_slot_preflight")
            nodes = connection.get("nodes")
            page_info = connection.get("pageInfo")
            if not isinstance(nodes, list) or not isinstance(page_info, dict):
                raise ChildDependencyError("invalid_dependency_slot_preflight")
            for relation in nodes:
                relation_id = relation.get("id") if isinstance(relation, dict) else None
                if not isinstance(relation_id, str) or not UUID.fullmatch(relation_id):
                    raise ChildDependencyError("invalid_dependency_slot_preflight")
                if relation_id in slots:
                    raise ChildDependencyError("duplicate_dependency_relation_slot")
                slots[relation_id] = relation
            if not page_info.get("hasNextPage"):
                return slots
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after or after in seen:
                raise ChildDependencyError("invalid_dependency_slot_pagination_cursor")
            seen.add(after)

    def _assert_relation_slots(
        self, desired_by_id: dict[str, dict[str, Any]],
        existing_by_id: dict[str, dict[str, Any]],
    ) -> None:
        slots = self._all_relation_slots()
        for relation_id, desired in desired_by_id.items():
            occupied = slots.get(relation_id)
            if occupied is None:
                if relation_id in existing_by_id:
                    raise ChildDependencyError("dependency_slot_readback_inconsistent")
                continue
            if relation_id not in existing_by_id:
                raise ChildDependencyError(
                    f"dependency_relation_slot_occupied:{relation_id}"
                )
            if (
                occupied.get("archivedAt") is not None
                or occupied.get("type") != "blocks"
                or (occupied.get("issue") or {}).get("id")
                != desired["blocker"]["issue_id"]
                or (occupied.get("relatedIssue") or {}).get("id")
                != desired["blocked"]["issue_id"]
            ):
                raise ChildDependencyError(
                    f"dependency_relation_slot_occupied:{relation_id}"
                )

    def _assert_authorization(self, event: dict[str, Any]) -> None:
        try:
            self.authorization.assert_child_dependencies_authorized(event)
        except LinearTransportError as error:
            raise ChildDependencyError(
                "dependency_frontier_changed_reload_required"
            ) from error

    def _root_comments(self) -> list[dict[str, Any]]:
        return LinearCommentEventAdapter(
            self.client, issue_id=self.authority["root_identifier"],
            workspace_id=self.authority["workspace_id"],
            team_id=self.authority["team_id"],
            project_id=self.authority["project_id"],
        ).comments()

    def _root_and_children(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        transport = LinearGraphQLTransport(
            self.client, workspace_id=self.authority["workspace_id"],
            team_id=self.authority["team_id"], project_id=self.authority["project_id"],
        )
        root, children = transport._resume_root_with_children(
            self.authority["root_identifier"]
        )
        if root.get("id") != self.authority["root_issue_id"]:
            raise ChildDependencyError("dependency_root_identity_mismatch")
        if root.get("parent") is not None:
            raise ChildDependencyError("dependency_root_is_child")
        validate_issue_route(
            root, workspace_id=self.authority["workspace_id"],
            team_id=self.authority["team_id"], project_id=self.authority["project_id"],
        )
        if parse_plan_revision(root.get("description")) != self.plan_revision:
            raise ChildDependencyError("dependency_root_plan_revision_mismatch")
        return root, children

    def _authenticated_children(
        self, declared: list[dict[str, Any]], live: list[dict[str, Any]],
    ) -> dict[str, dict[str, str]]:
        if not isinstance(declared, list):
            raise ChildDependencyError("invalid_owned_child_set")
        identities = [_validate_identity(item, label="owned_child") for item in declared]
        ids = [item["issue_id"] for item in identities]
        identifiers = [item["identifier"] for item in identities]
        if len(ids) != len(set(ids)) or len(identifiers) != len(set(identifiers)):
            raise ChildDependencyError("ambiguous_owned_child_identity")
        live_by_id: dict[str, dict[str, Any]] = {}
        live_identifiers: set[str] = set()
        for child in live:
            identity = _validate_identity({
                "issue_id": child.get("id"), "identifier": child.get("identifier"),
            }, label="live_child")
            if (
                identity["issue_id"] in live_by_id
                or identity["identifier"] in live_identifiers
            ):
                raise ChildDependencyError("ambiguous_live_child_identity")
            live_by_id[identity["issue_id"]] = child
            live_identifiers.add(identity["identifier"])
        if set(ids) != set(live_by_id):
            raise ChildDependencyError("incomplete_owned_child_identity_set")
        result: dict[str, dict[str, str]] = {}
        for identity in identities:
            child = live_by_id[identity["issue_id"]]
            if str(child.get("identifier", "")).upper() != identity["identifier"]:
                raise ChildDependencyError(
                    f"owned_child_identity_mismatch:{identity['identifier']}"
                )
            if (child.get("parent") or {}).get("id") != self.authority["root_issue_id"]:
                raise ChildDependencyError(f"cross_root_dependency:{identity['identifier']}")
            validate_issue_route(
                child, workspace_id=self.authority["workspace_id"],
                team_id=self.authority["team_id"], project_id=self.authority["project_id"],
            )
            if parse_plan_revision(child.get("description")) != self.plan_revision:
                raise ChildDependencyError(
                    f"owned_child_plan_revision_mismatch:{identity['identifier']}"
                )
            result[identity["issue_id"]] = identity
        return result

    def _relation_connection(
        self, identity: dict[str, str], *, inverse: bool,
    ) -> list[dict[str, Any]]:
        query = INVERSE_RELATIONS_QUERY if inverse else RELATIONS_QUERY
        field = "inverseRelations" if inverse else "relations"
        result: list[dict[str, Any]] = []
        after: str | None = None
        seen: set[str] = set()
        while True:
            response = self.client.execute(
                query, {"issueId": identity["identifier"], "after": after},
            )
            issue = response.get("issue") if isinstance(response, dict) else None
            if (
                not isinstance(issue, dict)
                or issue.get("id") != identity["issue_id"]
                or str(issue.get("identifier", "")).upper() != identity["identifier"]
            ):
                raise ChildDependencyError("dependency_child_identity_mismatch")
            if (issue.get("parent") or {}).get("id") != self.authority["root_issue_id"]:
                raise ChildDependencyError("cross_root_dependency")
            validate_issue_route(
                issue, workspace_id=self.authority["workspace_id"],
                team_id=self.authority["team_id"], project_id=self.authority["project_id"],
            )
            connection = issue.get(field)
            if not isinstance(connection, dict):
                raise ChildDependencyError("invalid_dependency_connection")
            nodes = connection.get("nodes")
            page_info = connection.get("pageInfo")
            if not isinstance(nodes, list) or not isinstance(page_info, dict):
                raise ChildDependencyError("invalid_dependency_connection")
            result.extend(nodes)
            if not page_info.get("hasNextPage"):
                return result
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after or after in seen:
                raise ChildDependencyError("invalid_dependency_pagination_cursor")
            seen.add(after)

    def _relations(
        self, owned: dict[str, dict[str, str]],
    ) -> dict[str, dict[str, list[dict[str, Any]]]]:
        return {
            child_id: {
                "relations": self._relation_connection(identity, inverse=False),
                "inverse_relations": self._relation_connection(identity, inverse=True),
            }
            for child_id, identity in sorted(owned.items())
        }

    def _read(
        self, declared_children: list[dict[str, Any]],
    ) -> tuple[dict[str, int], DependencyGraph, dict[str, dict[str, str]]]:
        root, live_children = self._root_and_children()
        owned = self._authenticated_children(declared_children, live_children)
        root_comments = self._root_comments()
        material = reduce_event_comments(
            root_comments, workstream_id=self.authority["root_identifier"],
        )
        projection = reduce_projection_comments(
            root_comments, workstream_id=self.authority["root_identifier"],
            expected_plan_revision=self.plan_revision,
            authenticated_route={
                key: self.authority[key]
                for key in ("workspace_id", "team_id", "project_id", "root_issue_id")
            },
        )
        graph = reduce_dependency_readback(
            self._relations(owned), authority=self.authority, owned_children=owned,
        )
        final_root, final_children = self._root_and_children()
        final_owned = self._authenticated_children(declared_children, final_children)
        if final_root.get("id") != root.get("id") or final_owned != owned:
            raise ChildDependencyError("dependency_issue_graph_changed_during_read")
        final_comments = self._root_comments()
        try:
            final_material = reduce_event_comments(
                final_comments, workstream_id=self.authority["root_identifier"],
            )
            final_projection = reduce_projection_comments(
                final_comments, workstream_id=self.authority["root_identifier"],
                expected_plan_revision=self.plan_revision,
                authenticated_route={
                    key: self.authority[key]
                    for key in (
                        "workspace_id", "team_id", "project_id", "root_issue_id",
                    )
                },
            )
        except LinearTransportError as error:
            raise ChildDependencyError(
                "dependency_root_frontier_changed_during_read"
            ) from error
        final_graph = reduce_dependency_readback(
            self._relations(owned), authority=self.authority, owned_children=owned,
        )
        if material.events != final_material.events or projection.events != final_projection.events:
            raise ChildDependencyError("dependency_root_frontier_changed_during_read")
        if graph != final_graph:
            raise ChildDependencyError("dependency_graph_frontier_changed_during_read")
        return ({
            "material_revision": material.revision,
            "projection_revision": projection.revision,
            "graph_revision": graph.revision,
            "graph_sha256": _sha256(list(graph.relations)),
        }, graph, owned)

    @staticmethod
    def _normalize_relations(
        relations: list[dict[str, Any]], owned: dict[str, dict[str, str]],
    ) -> list[tuple[dict[str, str], dict[str, str]]]:
        if not isinstance(relations, list) or not relations:
            raise ChildDependencyError("dependency_batch_must_be_nonempty")
        directed: list[tuple[dict[str, str], dict[str, str]]] = []
        observed: set[tuple[str, str]] = set()
        unordered: dict[frozenset[str], tuple[str, str]] = {}
        for relation in relations:
            if not isinstance(relation, dict) or set(relation) != {"source", "type", "target"}:
                raise ChildDependencyError("invalid_dependency_relation")
            relation_type = relation.get("type")
            if relation_type not in {"blocks", "blocked_by"}:
                raise ChildDependencyError(f"invalid_dependency_type:{relation_type}")
            source = _validate_identity(relation.get("source"), label="source")
            target = _validate_identity(relation.get("target"), label="target")
            if source == target:
                raise ChildDependencyError("self_dependency")
            if source["issue_id"] not in owned or owned[source["issue_id"]] != source:
                raise ChildDependencyError("source_not_owned_by_root")
            if target["issue_id"] not in owned or owned[target["issue_id"]] != target:
                raise ChildDependencyError("target_not_owned_by_root")
            blocker, blocked = (
                (source, target) if relation_type == "blocks" else (target, source)
            )
            direction = (blocker["issue_id"], blocked["issue_id"])
            if direction in observed:
                raise ChildDependencyError("duplicate_dependency")
            pair = frozenset(direction)
            if pair in unordered and unordered[pair] != direction:
                raise ChildDependencyError("conflicting_dependency_direction")
            observed.add(direction)
            unordered[pair] = direction
            directed.append((blocker, blocked))
        return sorted(directed, key=lambda pair: (pair[0]["issue_id"], pair[1]["issue_id"]))

    def apply_batch(
        self, *, owned_children: list[dict[str, Any]],
        relations: list[dict[str, Any]], expected_frontier: dict[str, Any],
    ) -> dict[str, Any]:
        if (
            not isinstance(expected_frontier, dict)
            or set(expected_frontier) != FRONTIER_FIELDS
            or any(
                not isinstance(expected_frontier[field], int)
                or isinstance(expected_frontier[field], bool)
                or expected_frontier[field] < 0
                for field in (
                    "material_revision", "projection_revision", "graph_revision",
                )
            )
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(expected_frontier["graph_sha256"]),
            )
        ):
            raise ChildDependencyError("invalid_dependency_frontier")
        initial_frontier, initial_graph, owned = self._read(owned_children)
        directed = self._normalize_relations(relations, owned)
        desired = [{
            "id": dependency_relation_id(
                authority=self.authority, blocker=blocker, blocked=blocked,
            ),
            "type": "blocks", "blocker": blocker, "blocked": blocked,
            "inverse_type": "blocked_by",
        } for blocker, blocked in directed]
        desired_by_id = {relation["id"]: relation for relation in desired}
        existing_by_id = {relation["id"]: relation for relation in initial_graph.relations}
        present = set(desired_by_id) & set(existing_by_id)
        baseline_by_id = {
            relation_id: relation for relation_id, relation in existing_by_id.items()
            if relation_id not in desired_by_id
        }
        if any(existing_by_id[relation_id] != desired_by_id[relation_id]
               for relation_id in present):
            raise ChildDependencyError("conflicting_dependency_relation_id")
        existing_pairs = {
            (relation["blocker"]["issue_id"], relation["blocked"]["issue_id"]): relation
            for relation in initial_graph.relations
        }
        for desired_relation in desired:
            direction = (
                desired_relation["blocker"]["issue_id"],
                desired_relation["blocked"]["issue_id"],
            )
            reverse = (direction[1], direction[0])
            if direction in existing_pairs and existing_pairs[direction]["id"] != desired_relation["id"]:
                raise ChildDependencyError("duplicate_dependency")
            if reverse in existing_pairs:
                raise ChildDependencyError("conflicting_dependency_direction")
        if (
            initial_frontier["material_revision"] < expected_frontier["material_revision"]
            or initial_frontier["graph_revision"]
            != expected_frontier["graph_revision"] + len(present)
            or _sha256([
                baseline_by_id[item] for item in sorted(baseline_by_id)
            ]) != expected_frontier["graph_sha256"]
            or initial_frontier["projection_revision"]
            < expected_frontier["projection_revision"]
        ):
            raise ChildDependencyError("dependency_frontier_changed_reload_required")

        batch_id = "wsdb_" + _sha256([
            "workstream-child-dependency-native-batch-v1", self.authority,
            self.plan_revision, expected_frontier, desired,
        ])[:32]
        if present and (
            initial_frontier["projection_revision"]
            == expected_frontier["projection_revision"]
        ):
            raise ChildDependencyError("dependency_relation_precedes_authorization")
        self._assert_native_capability()
        self._assert_relation_slots(desired_by_id, existing_by_id)
        authorization = self.authorization.reserve_child_dependencies(
            batch_id=batch_id,
            relation_ids=sorted(desired_by_id),
            relations_sha256=_sha256(desired),
            expected_material_revision=expected_frontier["material_revision"],
            expected_projection_revision=expected_frontier["projection_revision"],
            expected_graph_revision=expected_frontier["graph_revision"],
            expected_graph_sha256=expected_frontier["graph_sha256"],
        )
        authorization_event = authorization.get("event")
        if not isinstance(authorization_event, dict):
            raise ChildDependencyError("dependency_authorization_receipt_invalid")
        authorized_frontier = dict(expected_frontier)
        authorized_frontier["projection_revision"] += 1
        known_by_id = dict(existing_by_id)
        writes: list[str] = []
        for relation in sorted(desired, key=lambda item: item["id"]):
            if relation["id"] in known_by_id:
                continue
            frontier, graph, _ = self._read(owned_children)
            observed = {item["id"]: item for item in graph.relations}
            expected_now = dict(authorized_frontier)
            expected_now["graph_revision"] += len(present) + len(writes)
            expected_now["graph_sha256"] = _sha256([
                known_by_id[item] for item in sorted(known_by_id)
            ])
            if (
                frontier["material_revision"] < expected_now["material_revision"]
                or frontier["projection_revision"]
                < expected_now["projection_revision"]
                or frontier["graph_revision"] != expected_now["graph_revision"]
                or frontier["graph_sha256"] != expected_now["graph_sha256"]
                or observed != known_by_id
            ):
                raise ChildDependencyError("dependency_frontier_changed_reload_required")
            self._assert_relation_slots(desired_by_id, observed)
            self._assert_authorization(authorization_event)
            try:
                response = self.client.execute(RELATION_CREATE_MUTATION, {"input": {
                    "id": relation["id"],
                    "issueId": relation["blocker"]["issue_id"],
                    "relatedIssueId": relation["blocked"]["issue_id"],
                    "type": "blocks",
                }})
            except (LinearTransportError, OSError, TimeoutError, ValueError) as error:
                after_frontier, after_graph, _ = self._read(owned_children)
                after = {item["id"]: item for item in after_graph.relations}
                expected_after = {**known_by_id, relation["id"]: relation}
                if after != expected_after:
                    raise ChildDependencyError("dependency_create_unconfirmed") from error
                if (
                    after_frontier["material_revision"]
                    < expected_frontier["material_revision"]
                    or after_frontier["projection_revision"]
                    < authorized_frontier["projection_revision"]
                    or after_frontier["graph_revision"] != len(expected_after)
                    or after_frontier["graph_sha256"] != _sha256([
                        expected_after[item] for item in sorted(expected_after)
                    ])
                ):
                    raise ChildDependencyError("dependency_frontier_changed_reload_required")
            else:
                created = response.get("issueRelationCreate") or {}
                native = created.get("issueRelation")
                if (
                    created.get("success") is not True or not isinstance(native, dict)
                    or native.get("id") != relation["id"] or native.get("type") != "blocks"
                    or (native.get("issue") or {}).get("id") != relation["blocker"]["issue_id"]
                    or (native.get("relatedIssue") or {}).get("id") != relation["blocked"]["issue_id"]
                ):
                    raise ChildDependencyError("dependency_create_returned_no_receipt")
            writes.append(relation["id"])
            known_by_id[relation["id"]] = relation

        final_frontier, final_graph, _ = self._read(owned_children)
        expected_final = dict(authorized_frontier)
        expected_final["graph_revision"] += len(desired)
        expected_final["graph_sha256"] = _sha256([
            known_by_id[item] for item in sorted(known_by_id)
        ])
        if (
            final_frontier["material_revision"]
            < expected_final["material_revision"]
            or final_frontier["projection_revision"]
            < expected_final["projection_revision"]
            or final_frontier["graph_revision"] != expected_final["graph_revision"]
            or final_frontier["graph_sha256"] != expected_final["graph_sha256"]
        ):
            raise ChildDependencyError("dependency_final_frontier_changed")
        self._assert_authorization(authorization_event)
        final_by_id = {relation["id"]: relation for relation in final_graph.relations}
        if any(final_by_id.get(relation_id) != relation
               for relation_id, relation in desired_by_id.items()):
            raise ChildDependencyError("dependency_final_readback_missing")
        return {
            "schema_version": 1, "authority": deepcopy(self.authority),
            "plan_revision": self.plan_revision, "batch_id": batch_id,
            "frontier_before": deepcopy(expected_frontier),
            "frontier_after": final_frontier, "writes": len(writes),
            "authorization": authorization,
            "relations": [deepcopy(final_by_id[item["id"]]) for item in desired],
            "native_linear_relations": {
                "written": bool(writes), "authority": "derived_cache",
                "idempotency": "client_supplied_deterministic_uuid_v4",
                "readback": "relations_and_inverseRelations",
            },
            "dependency_authority": "child_dependency_authorization",
        }


def apply_dependency_request(
    client: GraphQLClient, request: dict[str, Any],
) -> dict[str, Any]:
    """Apply one exact, self-contained dependency request without route inference."""
    required = {
        "schema_version", "authority", "plan_revision", "owned_children",
        "relations", "expected_frontier",
    }
    if not isinstance(request, dict) or set(request) != required:
        raise ChildDependencyError("invalid_dependency_request_fields")
    if request.get("schema_version") != 1:
        raise ChildDependencyError("invalid_dependency_request_schema")
    authority = _validate_authority(request.get("authority"))
    adapter = LinearChildDependencyAdapter(
        client,
        workspace_id=authority["workspace_id"],
        team_id=authority["team_id"],
        project_id=authority["project_id"],
        root_issue_id=authority["root_issue_id"],
        root_identifier=authority["root_identifier"],
        plan_revision=request.get("plan_revision"),
    )
    return adapter.apply_batch(
        owned_children=request.get("owned_children"),
        relations=request.get("relations"),
        expected_frontier=request.get("expected_frontier"),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Apply one fenced dependency batch to existing Linear children.",
    )
    parser.add_argument(
        "--request", required=True,
        help="Exact JSON request path, or '-' to read stdin.",
    )
    parser.add_argument(
        "--apply", action="store_true", required=True,
        help="Required acknowledgement that this invocation may create relations.",
    )
    parser.add_argument(
        "--linear-endpoint", default="https://api.linear.app/graphql",
    )
    args = parser.parse_args(argv)
    if args.request == "-":
        request = json.load(sys.stdin)
    else:
        with open(args.request, encoding="utf-8") as stream:
            request = json.load(stream)
    token = load_linear_api_key()
    if not token:
        raise ChildDependencyError("linear_api_key_required")
    receipt = apply_dependency_request(
        HttpGraphQLClient(token, args.linear_endpoint), request,
    )
    json.dump(receipt, sys.stdout, ensure_ascii=False, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
