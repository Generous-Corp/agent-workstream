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
from datetime import datetime, timezone
from typing import Any

from workstream_linear import (
    GraphQLClient, HttpGraphQLClient, LinearGraphQLTransport, LinearTransportError,
    parse_plan_revision, validate_issue_route,
)
from workstream_config import load_linear_api_key
from workstream_linear_events import LinearCommentEventAdapter, reduce_event_comments
from workstream_linear_projection import (
    dependency_material_frontier_sha256, LinearProjectionAdapter,
    LinearProjectionError, reduce_projection_comments, select_plan_generation,
)
from workstream_scope import ScopeError, validate_scope


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


def dependency_root_readback_sha256(root: dict[str, Any]) -> str:
    """Digest the native root fields shared by the resume and graph reads."""
    if not isinstance(root, dict):
        raise ChildDependencyError("invalid_dependency_root_readback")
    fields = (
        "id", "identifier", "title", "description", "url", "updatedAt",
        "parent", "team", "project", "assignee", "state",
    )
    readback = {field: deepcopy(root.get(field)) for field in fields}
    if not isinstance(readback["id"], str) or not readback["id"]:
        raise ChildDependencyError("invalid_dependency_root_readback")
    if not isinstance(readback["identifier"], str) or not readback["identifier"]:
        raise ChildDependencyError("invalid_dependency_root_readback")
    return _sha256(readback)


def dependency_comment_readback_sha256(comments: list[dict[str, Any]]) -> str:
    """Digest exact root comment receipts independent of connection order."""
    receipts = []
    seen: set[str] = set()
    for comment in comments:
        if not isinstance(comment, dict):
            raise ChildDependencyError("invalid_dependency_comment_readback")
        remote_id = comment.get("id")
        body = comment.get("body")
        if (
            not isinstance(remote_id, str) or not remote_id or remote_id in seen
            or not isinstance(body, str)
        ):
            raise ChildDependencyError("invalid_dependency_comment_readback")
        seen.add(remote_id)
        receipts.append({
            "id": remote_id, "body_sha256": hashlib.sha256(
                body.encode("utf-8")
            ).hexdigest(),
            "createdAt": comment.get("createdAt"),
            "updatedAt": comment.get("updatedAt"),
        })
    receipts.sort(key=lambda item: item["id"])
    return _sha256(receipts)


def _remote_comment_time(comment: Any) -> datetime:
    if not isinstance(comment, dict):
        raise ChildDependencyError("dependency_authorization_receipt_missing")
    value = comment.get("createdAt")
    if not isinstance(value, str) or not value:
        raise ChildDependencyError("dependency_authorization_receipt_missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ChildDependencyError("dependency_authorization_receipt_invalid") from error
    if parsed.tzinfo is None:
        raise ChildDependencyError("dependency_authorization_receipt_invalid")
    return parsed.astimezone(timezone.utc)


def authorized_dependency_graph(
    graph: DependencyGraph, projection_events: list[dict[str, Any]], *,
    authority: dict[str, str], plan_revision: str,
    observed_frontier: dict[str, Any] | None = None,
    root_readback_sha256: str | None = None,
    material_events: list[Any] | tuple[Any, ...] | None = None,
    material_remote_ids: dict[str, str] | None = None,
    projection_remote_ids: dict[str, str] | None = None,
    comments: list[dict[str, Any]] | None = None,
    owned_children: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Bind the native graph to the ordered active-generation grants."""
    authority = _validate_authority(authority)
    if not isinstance(plan_revision, str) or not plan_revision:
        raise ChildDependencyError("invalid_dependency_plan_revision")
    projection_authority = {
        key: authority[key]
        for key in ("workspace_id", "team_id", "project_id", "root_issue_id")
    }
    native_by_id = {item["id"]: item for item in graph.relations}
    if len(native_by_id) != len(graph.relations):
        raise ChildDependencyError("ambiguous_dependency_graph")
    authorizations = [
        event for event in projection_events
        if event.get("kind") == "child_dependency_authorization"
    ]
    batch_ids: set[str] = set()
    relation_grants: dict[str, str] = {}
    modeled: dict[str, dict[str, Any]] = {}
    batches: list[dict[str, Any]] = []
    comment_by_id: dict[str, dict[str, Any]] = {}
    if comments is not None:
        for comment in comments:
            remote_id = comment.get("id") if isinstance(comment, dict) else None
            if not isinstance(remote_id, str) or not remote_id or remote_id in comment_by_id:
                raise ChildDependencyError("ambiguous_dependency_authorization_receipt")
            comment_by_id[remote_id] = comment
    for event in authorizations:
        value = event.get("value")
        if (
            not isinstance(value, dict)
            or event.get("plan_revision") != plan_revision
            or value.get("plan_revision") != plan_revision
            or event.get("authority") != projection_authority
            or value.get("route") != projection_authority
            or value.get("root_issue_id") != authority["root_issue_id"]
        ):
            raise ChildDependencyError("cross_generation_dependency_authorization")
        batch_id = value.get("batch_id")
        if batch_id in batch_ids or event.get("key") != batch_id:
            raise ChildDependencyError("ambiguous_dependency_authorization")
        batch_ids.add(batch_id)
        authorized_ids = value.get("relation_ids")
        if (
            not isinstance(authorized_ids, list)
            or not authorized_ids
            or authorized_ids != sorted(set(authorized_ids))
        ):
            raise ChildDependencyError("ambiguous_dependency_authorization")
        for relation_id in authorized_ids:
            prior = relation_grants.get(relation_id)
            if prior is not None and event.get("supersedes_event_id") != prior:
                raise ChildDependencyError("duplicate_dependency_authorization")
            relation_grants[relation_id] = event.get("event_id")
        expected_material_revision = value.get("expected_material_revision")
        if material_events is not None:
            if (
                not isinstance(expected_material_revision, int)
                or isinstance(expected_material_revision, bool)
                or expected_material_revision < 0
                or expected_material_revision > len(material_events)
            ):
                raise ChildDependencyError("stale_dependency_material_frontier")
            if (
                material_remote_ids is None or projection_remote_ids is None
                or comments is None
            ):
                raise ChildDependencyError("dependency_authorization_receipt_missing")
            try:
                material_frontier_sha256 = dependency_material_frontier_sha256(
                    material_events, material_remote_ids, comments,
                    revision=expected_material_revision,
                )
            except LinearProjectionError as error:
                raise ChildDependencyError(str(error)) from error
            if (
                value.get("expected_material_frontier_sha256")
                != material_frontier_sha256
            ):
                raise ChildDependencyError("dependency_material_frontier_mismatch")
            grant_remote_id = projection_remote_ids.get(event.get("event_id"))
            grant_comment = comment_by_id.get(grant_remote_id)
            grant_time = _remote_comment_time(grant_comment)
            for index, material_event in enumerate(material_events):
                event_id = getattr(material_event, "event_id", None)
                material_comment = comment_by_id.get(material_remote_ids.get(event_id))
                material_time = _remote_comment_time(material_comment)
                if index < expected_material_revision and material_time > grant_time:
                    raise ChildDependencyError(
                        "dependency_material_event_not_ordered_before_authorization"
                    )
                if index >= expected_material_revision and material_time <= grant_time:
                    raise ChildDependencyError(
                        "dependency_material_event_not_ordered_after_authorization"
                    )
        baseline = {
            relation_id: relation for relation_id, relation in modeled.items()
            if relation_id not in authorized_ids
        }
        expected_relations = [
            baseline[item] for item in sorted(baseline)
        ]
        if (
            value.get("expected_graph_revision") != len(baseline)
            or value.get("expected_graph_sha256") != _sha256(expected_relations)
        ):
            raise ChildDependencyError("stale_dependency_authorization_frontier")
        authorized = []
        for relation_id in authorized_ids:
            relation = native_by_id.get(relation_id)
            if relation is None:
                raise ChildDependencyError("authorized_dependency_readback_missing")
            authorized.append(relation)
        authorized.sort(key=lambda item: (
            item["blocker"]["issue_id"], item["blocked"]["issue_id"],
        ))
        if value.get("relations_sha256") != _sha256(authorized):
            raise ChildDependencyError("dependency_authorization_digest_mismatch")
        for relation in authorized:
            modeled[relation["id"]] = relation
        batches.append({
            "batch_id": batch_id,
            "event_id": event.get("event_id"),
            "relation_ids": list(authorized_ids),
            "relations_sha256": value.get("relations_sha256"),
            "expected_material_revision": value.get("expected_material_revision"),
            "expected_material_frontier_sha256": value.get(
                "expected_material_frontier_sha256"
            ),
            "expected_projection_revision": value.get("expected_projection_revision"),
            "expected_graph_revision": value.get("expected_graph_revision"),
            "expected_graph_sha256": value.get("expected_graph_sha256"),
        })
    if modeled != native_by_id:
        raise ChildDependencyError("unauthorized_native_dependency")
    relations = [native_by_id[item] for item in sorted(native_by_id)]
    frontier = observed_frontier
    if (
        not isinstance(frontier, dict) or set(frontier) != FRONTIER_FIELDS
        or frontier.get("graph_revision") != len(relations)
        or frontier.get("graph_sha256") != _sha256(relations)
    ):
        raise ChildDependencyError("invalid_dependency_observed_frontier")
    root_digest = root_readback_sha256
    if not re.fullmatch(r"[0-9a-f]{64}", str(root_digest)):
        raise ChildDependencyError("invalid_dependency_root_readback")
    result = {
        "schema_version": 1,
        "authority": "child_dependency_authorization",
        "plan_revision": plan_revision,
        "route": projection_authority,
        "revision": len(relations),
        "sha256": _sha256(relations),
        "authorization_batches": batches,
        "relations": deepcopy(relations),
        "native_readback": "relations_and_inverseRelations",
        "ignored_non_dependency_count": graph.ignored_non_dependency_count,
        "observed_frontier": deepcopy(frontier),
        "root_readback_sha256": root_digest,
    }
    if (
        comments is not None and owned_children is not None
        and (relations or batches)
    ):
        identities = sorted(
            [_validate_identity(item, label="owned_child") for item in owned_children],
            key=lambda item: (item["identifier"], item["issue_id"]),
        )
        if len({item["issue_id"] for item in identities}) != len(identities):
            raise ChildDependencyError("ambiguous_owned_child_identity")
        result["validation_authority"] = {
            "owned_children": identities,
            "comments": deepcopy(comments),
        }
    return result


def validate_authorized_dependency_graph_surface(
    value: Any, projection_events: list[dict[str, Any]], *,
    authority: dict[str, str], plan_revision: str,
    expected_frontier: dict[str, Any] | None = None,
    expected_root_readback_sha256: str | None = None,
) -> dict[str, Any]:
    """Validate a resume dependency surface against its active grants."""
    validate_dependency_graph_summary(
        value, authority=authority, plan_revision=plan_revision,
        expected_frontier=expected_frontier,
        expected_root_readback_sha256=expected_root_readback_sha256,
    )
    proof = value.get("validation_authority")
    if proof is not None:
        return validate_dependency_graph_authority(
            value, authority=authority, plan_revision=plan_revision,
            expected_frontier=expected_frontier,
            expected_root_readback_sha256=expected_root_readback_sha256,
            expected_projection_events=projection_events,
        )
    relations = value["relations"]
    expected = authorized_dependency_graph(
        DependencyGraph(
            tuple(relations), value["ignored_non_dependency_count"],
        ), projection_events,
        authority=authority, plan_revision=plan_revision,
        observed_frontier=value["observed_frontier"],
        root_readback_sha256=value["root_readback_sha256"],
    )
    if value != expected:
        raise ChildDependencyError("dependency_graph_surface_mismatch")
    return expected


def validate_dependency_graph_authority(
    value: Any, *, authority: dict[str, str], plan_revision: str,
    expected_frontier: dict[str, Any] | None = None,
    expected_root_readback_sha256: str | None = None,
    expected_projection_events: list[dict[str, Any]] | None = None,
    expected_material_event_ids: list[str] | None = None,
    expected_owned_identifiers: set[str] | None = None,
    expected_owned_children: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Re-run canonical reducers from the authority carried by a resume graph."""
    validate_dependency_graph_summary(
        value, authority=authority, plan_revision=plan_revision,
        expected_frontier=expected_frontier,
        expected_root_readback_sha256=expected_root_readback_sha256,
    )
    proof = value.get("validation_authority")
    if proof is None and not value["relations"] and not value["authorization_batches"]:
        return deepcopy(value)
    if not isinstance(proof, dict) or set(proof) != {"owned_children", "comments"}:
        raise ChildDependencyError("dependency_validation_authority_missing")
    raw_owned = proof.get("owned_children")
    comments = proof.get("comments")
    if not isinstance(raw_owned, list) or not isinstance(comments, list):
        raise ChildDependencyError("dependency_validation_authority_invalid")
    owned = sorted(
        [_validate_identity(item, label="owned_child") for item in raw_owned],
        key=lambda item: (item["identifier"], item["issue_id"]),
    )
    if owned != raw_owned or len({item["issue_id"] for item in owned}) != len(owned):
        raise ChildDependencyError("dependency_owned_child_authority_invalid")
    if (
        expected_owned_identifiers is not None
        and {item["identifier"] for item in owned} != expected_owned_identifiers
    ):
        raise ChildDependencyError("dependency_owned_child_set_mismatch")
    owned_by_id = {item["issue_id"]: item for item in owned}
    if expected_owned_children is not None:
        for child in expected_owned_children:
            identifier = str(child.get("identifier", "")).upper()
            issue_id = child.get("issue_id", child.get("id"))
            matches = [item for item in owned if item["identifier"] == identifier]
            if (
                len(matches) != 1 or not isinstance(issue_id, str)
                or matches[0]["issue_id"] != issue_id
            ):
                raise ChildDependencyError("dependency_owned_child_identity_mismatch")
    for relation in value["relations"]:
        for endpoint in (relation["blocker"], relation["blocked"]):
            if owned_by_id.get(endpoint["issue_id"]) != endpoint:
                raise ChildDependencyError("dependency_endpoint_not_owned")
    material = reduce_event_comments(
        comments, workstream_id=authority["root_identifier"],
    )
    if (
        expected_material_event_ids is not None
        and [event.event_id for event in material.events]
        != expected_material_event_ids
    ):
        raise ChildDependencyError("dependency_material_authority_mismatch")
    projection = reduce_projection_comments(
        comments, workstream_id=authority["root_identifier"],
        expected_plan_revision=plan_revision,
        authenticated_route={
            key: authority[key]
            for key in ("workspace_id", "team_id", "project_id", "root_issue_id")
        },
    )
    if (
        expected_projection_events is not None
        and [
            event for event in projection.events
            if event.get("kind") == "child_dependency_authorization"
        ] != [
            event for event in expected_projection_events
            if event.get("kind") == "child_dependency_authorization"
        ]
    ):
        raise ChildDependencyError("dependency_projection_authority_mismatch")
    expected = authorized_dependency_graph(
        DependencyGraph(
            tuple(value["relations"]), value["ignored_non_dependency_count"],
        ),
        projection.events, authority=authority, plan_revision=plan_revision,
        observed_frontier=value["observed_frontier"],
        root_readback_sha256=value["root_readback_sha256"],
        material_events=material.events,
        material_remote_ids=material.remote_ids,
        projection_remote_ids=projection.remote_ids,
        comments=comments, owned_children=owned,
    )
    if value != expected:
        raise ChildDependencyError("dependency_graph_surface_mismatch")
    return expected


def validate_dependency_graph_summary(
    value: Any, *, authority: dict[str, str], plan_revision: str,
    expected_frontier: dict[str, Any] | None = None,
    expected_root_readback_sha256: str | None = None,
) -> dict[str, Any]:
    """Validate graph semantics from its canonical grant summaries."""
    required = {
        "schema_version", "authority", "plan_revision", "route", "revision",
        "sha256", "authorization_batches", "relations", "native_readback",
        "ignored_non_dependency_count",
        "observed_frontier", "root_readback_sha256",
    }
    if (
        not isinstance(value, dict)
        or set(value) not in (required, required | {"validation_authority"})
    ):
        raise ChildDependencyError("invalid_dependency_graph_surface")
    relations = value.get("relations")
    ignored = value.get("ignored_non_dependency_count")
    if (
        value.get("schema_version") != 1
        or value.get("authority") != "child_dependency_authorization"
        or value.get("plan_revision") != plan_revision
        or value.get("route") != {
            key: authority[key]
            for key in ("workspace_id", "team_id", "project_id", "root_issue_id")
        }
        or value.get("native_readback") != "relations_and_inverseRelations"
        or not isinstance(relations, list)
        or not isinstance(ignored, int) or isinstance(ignored, bool) or ignored < 0
        or not isinstance(value.get("observed_frontier"), dict)
        or set(value["observed_frontier"]) != FRONTIER_FIELDS
        or not re.fullmatch(
            r"[0-9a-f]{64}", str(value.get("root_readback_sha256", "")),
        )
    ):
        raise ChildDependencyError("invalid_dependency_graph_surface")
    if expected_frontier is not None and value["observed_frontier"] != expected_frontier:
        raise ChildDependencyError("dependency_resume_frontier_mismatch")
    if (
        expected_root_readback_sha256 is not None
        and value["root_readback_sha256"] != expected_root_readback_sha256
    ):
        raise ChildDependencyError("dependency_resume_root_mismatch")
    normalized: list[dict[str, Any]] = []
    directions: dict[frozenset[str], tuple[str, str]] = {}
    for relation in relations:
        if not isinstance(relation, dict) or set(relation) != {
            "id", "type", "blocker", "blocked", "inverse_type",
        }:
            raise ChildDependencyError("invalid_dependency_graph_relation")
        blocker = _validate_identity(relation.get("blocker"), label="blocker")
        blocked = _validate_identity(relation.get("blocked"), label="blocked")
        if blocker == blocked:
            raise ChildDependencyError("self_dependency")
        relation_id = dependency_relation_id(
            authority=authority, blocker=blocker, blocked=blocked,
        )
        if (
            relation.get("id") != relation_id
            or relation.get("type") != "blocks"
            or relation.get("inverse_type") != "blocked_by"
        ):
            raise ChildDependencyError("invalid_dependency_graph_relation")
        direction = (blocker["issue_id"], blocked["issue_id"])
        pair = frozenset(direction)
        prior = directions.get(pair)
        if prior is not None:
            reason = (
                "duplicate_dependency" if prior == direction
                else "conflicting_dependency_direction"
            )
            raise ChildDependencyError(reason)
        directions[pair] = direction
        normalized.append({
            "id": relation_id, "type": "blocks", "blocker": blocker,
            "blocked": blocked, "inverse_type": "blocked_by",
        })
    normalized.sort(key=lambda item: item["id"])
    if normalized != relations:
        raise ChildDependencyError("noncanonical_dependency_graph")
    if (
        value.get("revision") != len(normalized)
        or value.get("sha256") != _sha256(normalized)
        or value["observed_frontier"].get("graph_revision") != len(normalized)
        or value["observed_frontier"].get("graph_sha256") != _sha256(normalized)
    ):
        raise ChildDependencyError("dependency_graph_surface_mismatch")
    native_by_id = {item["id"]: item for item in normalized}
    modeled: dict[str, dict[str, Any]] = {}
    seen_batches: set[str] = set()
    seen_relations: set[str] = set()
    last_projection_revision = -1
    batches = value.get("authorization_batches")
    if not isinstance(batches, list):
        raise ChildDependencyError("invalid_dependency_graph_surface")
    batch_fields = {
        "batch_id", "event_id", "relation_ids", "relations_sha256",
        "expected_material_revision", "expected_material_frontier_sha256",
        "expected_projection_revision", "expected_graph_revision",
        "expected_graph_sha256",
    }
    for batch in batches:
        if not isinstance(batch, dict) or set(batch) != batch_fields:
            raise ChildDependencyError("invalid_dependency_authorization_batch")
        batch_id = batch.get("batch_id")
        relation_ids = batch.get("relation_ids")
        material_revision = batch.get("expected_material_revision")
        projection_revision = batch.get("expected_projection_revision")
        if (
            not re.fullmatch(r"wsdb_[0-9a-f]{32}", str(batch_id))
            or batch_id in seen_batches
            or not re.fullmatch(r"wsp_[0-9a-f]{32}", str(batch.get("event_id")))
            or not isinstance(relation_ids, list) or not relation_ids
            or relation_ids != sorted(set(relation_ids))
            or any(item in seen_relations for item in relation_ids)
            or not isinstance(material_revision, int)
            or isinstance(material_revision, bool) or material_revision < 0
            or material_revision > value["observed_frontier"]["material_revision"]
            or not isinstance(projection_revision, int)
            or isinstance(projection_revision, bool)
            or projection_revision <= last_projection_revision
            or projection_revision >= value["observed_frontier"]["projection_revision"]
            or not re.fullmatch(
                r"[0-9a-f]{64}",
                str(batch.get("expected_material_frontier_sha256", "")),
            )
        ):
            raise ChildDependencyError("invalid_dependency_authorization_batch")
        baseline = [modeled[item] for item in sorted(modeled)]
        if (
            batch.get("expected_graph_revision") != len(baseline)
            or batch.get("expected_graph_sha256") != _sha256(baseline)
        ):
            raise ChildDependencyError("stale_dependency_authorization_frontier")
        granted = []
        for relation_id in relation_ids:
            relation = native_by_id.get(relation_id)
            if relation is None:
                raise ChildDependencyError("authorized_dependency_readback_missing")
            granted.append(relation)
        granted.sort(key=lambda item: (
            item["blocker"]["issue_id"], item["blocked"]["issue_id"],
        ))
        if batch.get("relations_sha256") != _sha256(granted):
            raise ChildDependencyError("dependency_authorization_digest_mismatch")
        expected_batch_id = "wsdb_" + _sha256([
            "workstream-child-dependency-native-batch-v1", authority,
            plan_revision, {
                "material_revision": batch["expected_material_revision"],
                "projection_revision": batch["expected_projection_revision"],
                "graph_revision": batch["expected_graph_revision"],
                "graph_sha256": batch["expected_graph_sha256"],
            }, granted,
        ])[:32]
        if batch_id != expected_batch_id:
            raise ChildDependencyError("dependency_authorization_batch_id_mismatch")
        for relation in granted:
            modeled[relation["id"]] = relation
        seen_batches.add(batch_id)
        seen_relations.update(relation_ids)
        last_projection_revision = projection_revision
    if modeled != native_by_id:
        raise ChildDependencyError("unauthorized_native_dependency")
    return deepcopy(value)


def rebind_authenticated_dependency_graph(
    snapshot: dict[str, Any], comments: list[dict[str, Any]],
    base_surface: dict[str, Any], *, authority: dict[str, str],
    plan_revision: str,
) -> dict[str, Any]:
    """Bind one proven native graph to an exact prospective comment frontier."""
    validate_dependency_graph_summary(
        base_surface, authority=authority, plan_revision=plan_revision,
    )
    root = snapshot.get("root") if isinstance(snapshot, dict) else None
    root_sha256 = dependency_root_readback_sha256(root)
    if base_surface.get("root_readback_sha256") != root_sha256:
        raise ChildDependencyError("dependency_resume_root_mismatch")
    material = reduce_event_comments(
        comments, workstream_id=authority["root_identifier"],
    )
    projection = reduce_projection_comments(
        comments, workstream_id=authority["root_identifier"],
        expected_plan_revision=plan_revision,
        authenticated_route={
            key: authority[key]
            for key in ("workspace_id", "team_id", "project_id", "root_issue_id")
        },
    )
    graph = DependencyGraph(
        tuple(deepcopy(base_surface["relations"])),
        base_surface["ignored_non_dependency_count"],
    )
    return authorized_dependency_graph(
        graph, projection.events, authority=authority,
        plan_revision=plan_revision,
        observed_frontier={
            "material_revision": material.revision,
            "projection_revision": projection.revision,
            "graph_revision": graph.revision,
            "graph_sha256": _sha256(list(graph.relations)),
        },
        root_readback_sha256=root_sha256,
        material_events=material.events,
        material_remote_ids=material.remote_ids,
        projection_remote_ids=projection.remote_ids,
        comments=comments,
        owned_children=(base_surface.get("validation_authority") or {}).get(
            "owned_children", []
        ),
    )


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
        return root, children

    def _authenticated_children(
        self, declared: list[dict[str, Any]] | None,
        live: list[dict[str, Any]], *,
        generation: dict[str, Any], projection: Any,
    ) -> dict[str, dict[str, str]]:
        if declared is not None and not isinstance(declared, list):
            raise ChildDependencyError("invalid_owned_child_set")
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
            if (child.get("parent") or {}).get("id") != self.authority["root_issue_id"]:
                raise ChildDependencyError(
                    f"cross_root_dependency:{identity['identifier']}"
                )
            validate_issue_route(
                child, workspace_id=self.authority["workspace_id"],
                team_id=self.authority["team_id"], project_id=self.authority["project_id"],
            )

        if generation.get("authority_origin") != "generation_transition":
            if generation.get("description_plan_revision") != self.plan_revision:
                raise ChildDependencyError("dependency_root_plan_revision_mismatch")
            expected_ids = set(live_by_id)
        else:
            scope = projection.snapshot.get("scope")
            target_ids = {
                issue_id for issue_id, child in live_by_id.items()
                if parse_plan_revision(child.get("description")) == self.plan_revision
            }
            if scope is None and not target_ids:
                # Rolling upgrade: an inactive, not-yet-projected generation
                # cannot own predecessor children and therefore authenticates
                # the canonical empty graph.
                expected_ids = set()
            else:
                if not isinstance(scope, dict):
                    raise ChildDependencyError("active_generation_scope_invalid:missing")
                try:
                    validate_scope(
                        scope, root_id=self.authority["root_identifier"],
                        child_ids=live_identifiers,
                    )
                except ScopeError as error:
                    raise ChildDependencyError(
                        f"active_generation_scope_invalid:{error}"
                    ) from error
                if any(
                    scope["linear"].get(field) != self.authority[field]
                    for field in (
                        "workspace_id", "team_id", "project_id", "root_issue_id",
                    )
                ):
                    raise ChildDependencyError("active_generation_scope_route_mismatch")
                expected_ids = target_ids
        identities = (
            [live_by_id[issue_id] for issue_id in sorted(expected_ids)]
            if declared is None else declared
        )
        identities = [
            _validate_identity({
                "issue_id": item.get("issue_id", item.get("id")),
                "identifier": item.get("identifier"),
            }, label="owned_child")
            for item in identities
        ]
        ids = [item["issue_id"] for item in identities]
        identifiers = [item["identifier"] for item in identities]
        if len(ids) != len(set(ids)) or len(identifiers) != len(set(identifiers)):
            raise ChildDependencyError("ambiguous_owned_child_identity")
        if set(ids) != expected_ids:
            raise ChildDependencyError("incomplete_owned_child_identity_set")
        result: dict[str, dict[str, str]] = {}
        for identity in identities:
            child = live_by_id[identity["issue_id"]]
            if str(child.get("identifier", "")).upper() != identity["identifier"]:
                raise ChildDependencyError(
                    f"owned_child_identity_mismatch:{identity['identifier']}"
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

    def _read_state(
        self, declared_children: list[dict[str, Any]] | None,
    ) -> tuple[
        dict[str, Any], DependencyGraph, dict[str, dict[str, str]], Any, Any,
        list[dict[str, Any]], dict[str, Any],
    ]:
        root, live_children = self._root_and_children()
        root_comments = self._root_comments()
        description_plan_revision = parse_plan_revision(root.get("description"))
        generation = select_plan_generation(
            root_comments, workstream_id=self.authority["root_identifier"],
            description_plan_revision=description_plan_revision,
            authenticated_route={
                key: self.authority[key]
                for key in ("workspace_id", "team_id", "project_id", "root_issue_id")
            },
        )
        if generation["plan_revision"] != self.plan_revision:
            raise ChildDependencyError("dependency_plan_generation_not_selected")
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
        owned = self._authenticated_children(
            declared_children, live_children,
            generation=generation, projection=projection,
        )
        graph = reduce_dependency_readback(
            self._relations(owned), authority=self.authority, owned_children=owned,
        )
        final_root, final_children = self._root_and_children()
        final_comments = self._root_comments()
        try:
            final_generation = select_plan_generation(
                final_comments, workstream_id=self.authority["root_identifier"],
                description_plan_revision=parse_plan_revision(
                    final_root.get("description")
                ),
                authenticated_route={
                    key: self.authority[key]
                    for key in (
                        "workspace_id", "team_id", "project_id", "root_issue_id",
                    )
                },
            )
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
        final_owned = self._authenticated_children(
            declared_children, final_children,
            generation=final_generation, projection=final_projection,
        )
        if (
            dependency_root_readback_sha256(final_root)
            != dependency_root_readback_sha256(root)
            or final_generation != generation
            or final_owned != owned
        ):
            raise ChildDependencyError("dependency_issue_graph_changed_during_read")
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
        }, graph, owned, projection, material, root_comments, root)

    def _read(
        self, declared_children: list[dict[str, Any]],
    ) -> tuple[dict[str, int], DependencyGraph, dict[str, dict[str, str]]]:
        frontier, graph, owned, _projection, _material, _comments, _root = (
            self._read_state(declared_children)
        )
        return frontier, graph, owned

    def read_authorized_graph(
        self, *, expected_material_revision: int | None = None,
        expected_projection_revision: int | None = None,
        expected_root_readback_sha256: str | None = None,
    ) -> dict[str, Any]:
        """Read the active child graph and bind every edge to its grant."""
        frontier, graph, owned, projection, material, comments, root = (
            self._read_state(None)
        )
        if (
            expected_material_revision is not None
            and frontier["material_revision"] != expected_material_revision
        ):
            raise ChildDependencyError("dependency_resume_frontier_mismatch")
        if (
            expected_projection_revision is not None
            and frontier["projection_revision"] != expected_projection_revision
        ):
            raise ChildDependencyError("dependency_resume_frontier_mismatch")
        root_sha256 = dependency_root_readback_sha256(root)
        if (
            expected_root_readback_sha256 is not None
            and root_sha256 != expected_root_readback_sha256
        ):
            raise ChildDependencyError("dependency_resume_root_mismatch")
        return authorized_dependency_graph(
            graph, projection.events, authority=self.authority,
            plan_revision=self.plan_revision,
            observed_frontier=frontier,
            root_readback_sha256=root_sha256,
            material_events=material.events,
            material_remote_ids=material.remote_ids,
            projection_remote_ids=projection.remote_ids,
            comments=comments,
            owned_children=list(owned.values()),
        )

    def read_authorized_graph_for_snapshot(
        self, snapshot: dict[str, Any], comments: list[dict[str, Any]], *,
        reread: Any, generation_selector_plan_revision: str | None = None,
    ) -> dict[str, Any]:
        """Authenticate native relations against one caller-stable resume read."""
        def reduce_inputs(
            current_snapshot: dict[str, Any], current_comments: list[dict[str, Any]],
        ) -> tuple[Any, Any, dict[str, dict[str, str]], dict[str, Any]]:
            root = current_snapshot.get("root")
            children = current_snapshot.get("children")
            if not isinstance(root, dict) or not isinstance(children, list):
                raise ChildDependencyError("invalid_dependency_issue_graph")
            if root.get("id") != self.authority["root_issue_id"]:
                raise ChildDependencyError("dependency_root_identity_mismatch")
            validate_issue_route(
                root, workspace_id=self.authority["workspace_id"],
                team_id=self.authority["team_id"],
                project_id=self.authority["project_id"],
            )
            description_plan_revision = (
                generation_selector_plan_revision
                if generation_selector_plan_revision is not None
                else parse_plan_revision(root.get("description"))
            )
            selected = select_plan_generation(
                current_comments,
                workstream_id=self.authority["root_identifier"],
                description_plan_revision=description_plan_revision,
                authenticated_route={
                    key: self.authority[key] for key in (
                        "workspace_id", "team_id", "project_id", "root_issue_id",
                    )
                },
            )
            projection = reduce_projection_comments(
                current_comments,
                workstream_id=self.authority["root_identifier"],
                expected_plan_revision=self.plan_revision,
                authenticated_route={
                    key: self.authority[key] for key in (
                        "workspace_id", "team_id", "project_id", "root_issue_id",
                    )
                },
            )
            generation = selected
            if selected["plan_revision"] != self.plan_revision:
                generation = {
                    **selected, "authority_origin": "generation_transition",
                }
            owned = self._authenticated_children(
                None, children, generation=generation, projection=projection,
            )
            material = reduce_event_comments(
                current_comments,
                workstream_id=self.authority["root_identifier"],
            )
            return material, projection, owned, root

        material, projection, owned, root = reduce_inputs(snapshot, comments)
        graph = reduce_dependency_readback(
            self._relations(owned), authority=self.authority,
            owned_children=owned,
        )
        final_snapshot, final_comments = reread()
        final_material, final_projection, final_owned, final_root = reduce_inputs(
            final_snapshot, final_comments,
        )
        final_graph = reduce_dependency_readback(
            self._relations(final_owned), authority=self.authority,
            owned_children=final_owned,
        )
        if (
            dependency_root_readback_sha256(root)
            != dependency_root_readback_sha256(final_root)
            or dependency_comment_readback_sha256(comments)
            != dependency_comment_readback_sha256(final_comments)
            or material.events != final_material.events
            or projection.events != final_projection.events
            or owned != final_owned or graph != final_graph
        ):
            raise ChildDependencyError("dependency_graph_frontier_changed_during_read")
        return authorized_dependency_graph(
            graph, projection.events, authority=self.authority,
            plan_revision=self.plan_revision,
            observed_frontier={
                "material_revision": material.revision,
                "projection_revision": projection.revision,
                "graph_revision": graph.revision,
                "graph_sha256": _sha256(list(graph.relations)),
            },
            root_readback_sha256=dependency_root_readback_sha256(root),
            material_events=material.events,
            material_remote_ids=material.remote_ids,
            projection_remote_ids=projection.remote_ids,
            comments=comments,
            owned_children=list(owned.values()),
        )

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
