#!/usr/bin/env python3
"""Append-only Linear projection for the complete workstream resume surface.

Each projection change is an immutable Linear comment.  Mutable current views
are derived by reducing the complete paginated comment stream; replacement of
a keyed value must name the exact event it supersedes.  This keeps scope,
relations, choices, evidence, provenance, and continuation disposition out of
unfenced issue-description overwrites.
"""

from __future__ import annotations

import base64
import binascii
from copy import deepcopy
import hashlib
import hmac
import json
import os
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from workstream_linear import (
    bootstrap_linear_route, GraphQLClient, HttpGraphQLClient, LinearTransportError,
    validate_issue_route,
)
from workstream_linear_events import (
    COMMENT_CREATE_MUTATION, COMMENTS_QUERY, reduce_event_comments,
    material_frontier, validate_review_artifact_identity,
)


COMMENT_CREATE_CAPABILITY_QUERY = """
query WorkstreamProjectionCommentCreateCapability {
  __type(name: "CommentCreateInput") { inputFields { name } }
}
"""
CHILD_ORIGIN_NATIVE_QUERY = """
query WorkstreamChildOriginNativeReadback($childId: String!) {
  issue(id: $childId) {
    id identifier description createdAt
    parent { id identifier }
    team { id organization { id } }
    project { id }
    state { id name type }
    assignee { id }
  }
}
"""
ROOT_ORIGIN_NATIVE_QUERY = """
query WorkstreamRootOriginNativeReadback($rootId: String!) {
  issue(id: $rootId) {
    id identifier description createdAt parent { id identifier }
    team { id organization { id } }
    project { id }
    state { id name type }
    assignee { id }
  }
}
"""

PROJECTION_PREFIX = "<!-- workstream-projection:v1:"
PROJECTION_RE = re.compile(r"<!-- workstream-projection:v1:([A-Za-z0-9_-]+) -->")
KINDS = {
    "scope", "relation", "choice", "evidence_contract", "source",
    "provenance", "disposition", "closure_review", "lifecycle", "cas_activation",
    "quarantine_disposition", "child_closure",
    "child_extension_authorization", "child_dependency_authorization",
    "child_mutation_authorization", "existing_child_origin_seal",
    "identity_history_seal",
    "generation_genesis", "generation_candidate_seal", "generation_transition",
    "generation_abort",
}
SINGLETON_KINDS = {
    "scope", "source", "disposition", "lifecycle", "cas_activation",
    "quarantine_disposition",
}
TOMBSTONE = {"_projection_tombstone": True}
AUTHORITY_FIELDS = {"workspace_id", "team_id", "project_id", "root_issue_id"}
LEGACY_DIGEST_KIND_FULL_EVENTS = "canonical-full-events-v1"
GENERATION_FRONTIER_FIELDS = {
    "plan_revision", "source_event_id", "source_identity", "source_sha256",
    "material_revision", "checkpoint_event_ids", "projection_revision",
    "checkpoint_events_sha256", "projection_frontier_event_id",
    "projection_events_sha256",
}
GENERATION_SOURCE_FIELDS = {"identity", "sha256"}
GENERATION_RETIREMENT_FIELDS = {
    "predecessor_plan_revision", "retired_at", "retired_writer_epoch",
    "provenance_event_ids", "checkpoint_event_ids", "declaration_sha256",
}
GENERATION_SEAL_FIELDS = {
    "schema_version", "reservation_id", "reservation_sha256", "from", "to",
    "source", "graph_frontier_sha256", "candidate_resume_sha256",
    "retirement", "previous_control_event_id", "activation_epoch",
}
GENERATION_CONTROL_FIELDS = {
    *GENERATION_SEAL_FIELDS, "candidate_seal_event_id",
    "candidate_seal_sha256",
}
GENERATION_CONTROL_V3_FIELDS = {
    *GENERATION_CONTROL_FIELDS,
    "activation_checkpoint", "activation_checkpoint_sha256",
}


class LinearProjectionError(LinearTransportError):
    """The remote projection cannot be persisted or reduced without guessing."""


def _valid_review_artifact(value: Any) -> bool:
    try:
        validate_review_artifact_identity(value)
    except ValueError:
        return False
    return True


def _projection_receipt(comment: dict[str, Any]) -> dict[str, Any]:
    return {key: comment.get(key) for key in ("id", "createdAt", "updatedAt", "body")}


def projection_prefix_sha256(
    events: list[dict[str, Any]], comments_by_event_id: Mapping[str, dict[str, Any]],
    through_event_id: str,
) -> str:
    prefix: list[dict[str, Any]] = []
    found = False
    for event in events:
        comment = comments_by_event_id.get(event["event_id"])
        if not isinstance(comment, dict):
            raise LinearProjectionError("identity_history_seal_receipt_missing")
        receipt = _projection_receipt(comment)
        body = receipt.pop("body")
        if not isinstance(body, str):
            raise LinearProjectionError("identity_history_seal_receipt_missing")
        prefix.append({
            "event": event,
            "receipt": {
                **receipt,
                "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
            },
        })
        if event["event_id"] == through_event_id:
            found = True
            break
    if not found:
        raise LinearProjectionError("identity_history_seal_target_missing")
    target = next(event for event in events if event["event_id"] == through_event_id)
    return hashlib.sha256(_canonical([
        "identity-history-prefix-v1",
        target.get("authority"), target["workstream_id"], target["plan_revision"],
        target["expected_revision"], prefix,
    ])).hexdigest()


def projection_prefix_frontier(
    state: "ReducedProjection", comments: list[dict[str, Any]],
    *, through_event_id: str | None = None,
) -> dict[str, Any]:
    """Bind the complete projection prefix and exact remote receipts."""
    events = list(state.events)
    if not events:
        raise LinearProjectionError("child_origin_projection_prefix_empty")
    comments_by_remote = {
        item.get("id"): item for item in comments
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    comments_by_event = {
        event["event_id"]: comments_by_remote.get(
            state.remote_ids.get(event["event_id"])
        )
        for event in events
    }
    through = through_event_id or events[-1]["event_id"]
    matching_indexes = [
        index for index, event in enumerate(events)
        if event["event_id"] == through
    ]
    if len(matching_indexes) != 1:
        raise LinearProjectionError("child_origin_projection_prefix_missing")
    return {
        "revision": matching_indexes[0] + 1,
        "through_event_id": through,
        "sha256": projection_prefix_sha256(events, comments_by_event, through),
    }


def _validated_identity_history_seals(
    events: list[dict[str, Any]],
    comments_by_event_id: Mapping[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    from workstream_scope import (
        repository_key, ScopeError,
        validate_authenticated_legacy_repository_identity_transition,
        validate_repository_identity_transition,
    )

    positions = {event["event_id"]: index for index, event in enumerate(events)}
    scope_events = [
        event for event in events
        if event["kind"] == "scope" and event["key"] == "root"
        and event["value"] != TOMBSTONE
    ]
    previous_scope = {
        scope_events[index]["event_id"]: scope_events[index - 1]
        for index in range(1, len(scope_events))
    }
    seals: dict[str, dict[str, Any]] = {}
    for event in events:
        if event["kind"] != "identity_history_seal":
            continue
        value = event["value"]
        target_id = value["sealed_scope_event_id"]
        transitions = value["legacy_transitions"]
        transition_ids = [item["transition_scope_event_id"] for item in transitions]
        target = next(
            (item for item in scope_events if item["event_id"] == target_id), None,
        )
        frontier_position = positions[event["event_id"]] - 1
        frontier = events[frontier_position] if frontier_position >= 0 else None
        scope_frontier = [
            item for item in scope_events
            if positions[item["event_id"]] < positions[event["event_id"]]
        ]
        source_event = next((
            item for item in reversed(events[:positions[event["event_id"]]])
            if item["kind"] == "source" and item["key"] == "root"
            and item["value"] != TOMBSTONE
        ), None)
        if (
            target is None
            or not transition_ids
            or not scope_frontier
            or target["event_id"] != scope_frontier[-1]["event_id"]
            or positions[event["event_id"]] <= positions[target_id]
            or any(transition_id in seals for transition_id in transition_ids)
            or source_event is None
            or value["source_identity"]
            != (source_event["value"].get("identity") or source_event["value"].get("url"))
            or value["source_sha256"] != source_event["value"].get("sha256")
            or value["sealed_scope_value_sha256"]
            != hashlib.sha256(_canonical(target["value"])).hexdigest()
            or frontier is None
            or value["sealed_projection_frontier_event_id"]
            != frontier["event_id"]
            or value["sealed_projection_frontier_event_sha256"]
            != hashlib.sha256(_canonical(frontier)).hexdigest()
            or value["legacy_projection_prefix_sha256"]
            != projection_prefix_sha256(
                events, comments_by_event_id, frontier["event_id"],
            )
        ):
            raise LinearProjectionError("identity_history_seal_frontier_mismatch")
        expected_transitions: list[dict[str, str]] = []
        for transition in scope_frontier[1:]:
            predecessor = previous_scope[transition["event_id"]]
            try:
                validate_repository_identity_transition(
                    predecessor["value"], transition["value"],
                )
            except ScopeError:
                try:
                    validate_authenticated_legacy_repository_identity_transition(
                        predecessor["value"], transition["value"],
                    )
                except ScopeError as error:
                    raise LinearProjectionError(
                        "identity_history_seal_transition_mismatch"
                    ) from error
                expected_transitions.append({
                    "predecessor_scope_event_id": predecessor["event_id"],
                    "predecessor_scope_value_sha256": hashlib.sha256(
                        _canonical(predecessor["value"])
                    ).hexdigest(),
                    "transition_scope_event_id": transition["event_id"],
                    "transition_scope_value_sha256": hashlib.sha256(
                        _canonical(transition["value"])
                    ).hexdigest(),
                })
            else:
                continue
        if transitions != expected_transitions:
            raise LinearProjectionError("identity_history_seal_transition_mismatch")
        repositories = value["repositories"]
        proofs = {item["repository_key"]: item for item in repositories}
        scoped: dict[str, dict[str, Any]] = {}
        try:
            for repository in target["value"]["repositories"]:
                scoped[repository_key(repository)] = repository
        except (KeyError, TypeError, ValueError) as error:
            raise LinearProjectionError("identity_history_seal_scope_invalid") from error
        if set(proofs) != set(scoped):
            raise LinearProjectionError("identity_history_seal_repository_mismatch")
        for key, repository in scoped.items():
            proof = proofs[key]
            expected_routes = sorted([
                repository["slug"], *repository.get("aliases", []),
            ])
            if (
                proof["provider_repository_id"]
                != repository.get("provider_repository_id")
                or proof["canonical_slug"] != repository.get("slug")
                or [item["requested_slug"] for item in proof["routes"]]
                != expected_routes
            ):
                raise LinearProjectionError("identity_history_seal_repository_mismatch")
        for transition_id in transition_ids:
            seals[transition_id] = event
    return seals


def projection_slot_id(
    workstream_id: str, plan_revision: str, revision: int,
    authority: dict[str, str],
) -> str:
    """Return one UUIDv4-shaped remote create slot for a projection revision."""
    if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", workstream_id.upper()):
        raise LinearProjectionError("invalid_projection_workstream")
    if not isinstance(plan_revision, str) or not plan_revision:
        raise LinearProjectionError("projection_missing:plan_revision")
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
        raise LinearProjectionError("invalid_projection_revision")
    validate_projection_authority(authority)
    material = _canonical([
        "workstream-projection-slot-v2", authority, workstream_id.upper(),
        plan_revision, revision,
    ])
    raw = bytearray(hashlib.sha256(material).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def child_origin_history_frontier(
    comments: list[dict[str, Any]], *, workstream_id: str,
) -> dict[str, Any]:
    """Digest the exact authoritative material/checkpoint history for review."""
    from workstream_linear_checkpoints import reduce_checkpoint_comments

    material = reduce_event_comments(comments, workstream_id=workstream_id)
    checkpoints = reduce_checkpoint_comments(comments, workstream_id=workstream_id)
    material_values = [
        {
            "event_id": item.event_id, "workstream_id": item.workstream_id,
            "kind": item.kind, "source": item.source, "payload": item.payload,
            "expected_revision": item.expected_revision,
            "created_at": item.created_at,
        }
        for item in material.events
    ]
    checkpoint_values = list(checkpoints.checkpoints)
    comments_by_id = {
        item.get("id"): item for item in comments
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }

    def receipts(event_ids: list[str], remote_ids: dict[str, str]) -> list[dict[str, str]]:
        result: list[dict[str, str]] = []
        for event_id in event_ids:
            remote_id = remote_ids.get(event_id)
            comment = comments_by_id.get(remote_id)
            body = comment.get("body") if isinstance(comment, dict) else None
            if not isinstance(remote_id, str) or not isinstance(body, str):
                raise LinearProjectionError("child_origin_history_receipt_missing")
            result.append({
                "event_id": event_id, "remote_id": remote_id,
                "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
            })
        return result

    material_ids = [item["event_id"] for item in material_values]
    checkpoint_ids = [item["event_id"] for item in checkpoint_values]
    checkpoint_frontier = {
        "algorithm": "checkpoint-reducer-order-v1",
        "count": len(checkpoint_values),
        "revision": max(
            (item["root_revision"] for item in checkpoint_values), default=0,
        ),
        "event_ids_reducer_order_sha256": hashlib.sha256(
            _canonical(checkpoint_ids)
        ).hexdigest(),
        "event_ids_sorted_set_sha256": hashlib.sha256(
            _canonical(sorted(set(checkpoint_ids)))
        ).hexdigest(),
        "checkpoints_sha256": hashlib.sha256(
            _canonical(checkpoint_values)
        ).hexdigest(),
    }
    return {
        "material_frontier": material_frontier(material),
        "material_receipts": receipts(material_ids, material.remote_ids),
        "checkpoint_frontier": checkpoint_frontier,
        "checkpoint_receipts": receipts(checkpoint_ids, checkpoints.remote_ids),
    }


def canonical_child_origin_native_readback(
    child: dict[str, Any], *, child_workstream_id: str, child_issue_id: str,
    root_workstream_id: str, root_issue_id: str, route: dict[str, str],
) -> dict[str, Any]:
    """Validate and compact one authenticated native child readback."""
    if (
        not isinstance(child, dict) or child.get("id") != child_issue_id
        or str(child.get("identifier", "")).upper() != child_workstream_id
        or (child.get("parent") or {}).get("id") != root_issue_id
        or str((child.get("parent") or {}).get("identifier", "")).upper()
        != root_workstream_id
    ):
        raise LinearProjectionError("child_origin_repair_native_parent_mismatch")
    validate_issue_route(child, **route)
    description = child.get("description")
    if description is None:
        description = ""
    state = child.get("state")
    assignee = child.get("assignee")
    if (
        not isinstance(description, str)
        or not isinstance(state, dict) or set(state) != {"id", "name", "type"}
        or not all(isinstance(state.get(field), str) and state[field]
                   for field in ("id", "name", "type"))
        or (
            assignee is not None
            and (
                not isinstance(assignee, dict) or set(assignee) != {"id"}
                or not isinstance(assignee.get("id"), str) or not assignee["id"]
            )
        )
        or not isinstance(child.get("createdAt"), str) or not child["createdAt"]
    ):
        raise LinearProjectionError("child_origin_repair_native_readback_incomplete")
    return {
        "id": child_issue_id, "identifier": child_workstream_id,
        "parent": {"id": root_issue_id, "identifier": root_workstream_id},
        "route": deepcopy(route), "state": deepcopy(state),
        "assignee_id": assignee["id"] if assignee is not None else None,
        "created_at": child["createdAt"],
        "description": {
            "bytes": len(description.encode("utf-8")),
            "sha256": hashlib.sha256(description.encode("utf-8")).hexdigest(),
        },
    }


def canonical_root_origin_native_readback(
    root: dict[str, Any], *, root_workstream_id: str, root_issue_id: str,
    authority: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate and compact the native root plus its separate prose fence."""
    if (
        not isinstance(root, dict) or root.get("id") != root_issue_id
        or str(root.get("identifier", "")).upper() != root_workstream_id
        or root.get("parent") is not None
    ):
        raise LinearProjectionError("child_origin_repair_root_identity_mismatch")
    validate_issue_route(root, **{
        key: authority[key] for key in ("workspace_id", "team_id", "project_id")
    })
    description = root.get("description")
    if description is None:
        description = ""
    state = root.get("state")
    assignee = root.get("assignee")
    if (
        not isinstance(description, str)
        or not isinstance(state, dict) or set(state) != {"id", "name", "type"}
        or not all(isinstance(state.get(field), str) and state[field]
                   for field in ("id", "name", "type"))
        or (
            assignee is not None
            and (
                not isinstance(assignee, dict) or set(assignee) != {"id"}
                or not isinstance(assignee.get("id"), str) or not assignee["id"]
            )
        )
        or not isinstance(root.get("createdAt"), str) or not root["createdAt"]
    ):
        raise LinearProjectionError("child_origin_repair_root_readback_incomplete")
    readback = {
        "id": root_issue_id, "identifier": root_workstream_id, "parent": None,
        "route": deepcopy(authority), "state": deepcopy(state),
        "assignee_id": assignee["id"] if assignee is not None else None,
        "created_at": root["createdAt"],
    }
    description_fence = {
        "bytes": len(description.encode("utf-8")),
        "sha256": hashlib.sha256(description.encode("utf-8")).hexdigest(),
    }
    return readback, description_fence


def validate_existing_child_origin_root_snapshot(
    client: GraphQLClient, repair: dict[str, Any],
) -> None:
    """Revalidate the complete reviewed native root immediately before sealing."""
    value = repair["value"]
    result = client.execute(
        ROOT_ORIGIN_NATIVE_QUERY, {"rootId": value["root_issue_id"]},
    )
    readback, description = canonical_root_origin_native_readback(
        result.get("issue"),
        root_workstream_id=repair["workstream_id"],
        root_issue_id=value["root_issue_id"], authority=value["route"],
    )
    if (
        readback != value["native_root_readback"]
        or hashlib.sha256(_canonical(readback)).hexdigest()
        != value["native_root_readback_sha256"]
        or description != value["root_description"]
    ):
        raise LinearProjectionError("child_origin_repair_native_root_drift")


def validate_existing_child_origin_root_identity(
    client: GraphQLClient, repair: dict[str, Any],
) -> None:
    """Revalidate only immutable root identity at a later consumer boundary."""
    value = repair["value"]
    root = client.execute(
        ROOT_ORIGIN_NATIVE_QUERY, {"rootId": value["root_issue_id"]},
    ).get("issue")
    if (
        not isinstance(root, dict)
        or root.get("id") != value["root_issue_id"]
        or str(root.get("identifier", "")).upper() != repair["workstream_id"]
        or root.get("parent") is not None
    ):
        raise LinearProjectionError("child_origin_repair_root_identity_drift")
    validate_issue_route(root, **{
        key: value["route"][key]
        for key in ("workspace_id", "team_id", "project_id")
    })
    if root.get("createdAt") != value["native_root_readback"]["created_at"]:
        raise LinearProjectionError("child_origin_repair_root_identity_drift")


def _immutable(event: dict[str, Any]) -> dict[str, Any]:
    return {key: deepcopy(value) for key, value in event.items() if key != "event_id"}


def _event_id(event: dict[str, Any]) -> str:
    return "wsp_" + hashlib.sha256(_canonical(_immutable(event))).hexdigest()[:32]


def _activation_digest_candidates(
    legacy_event_ids: list[str], accepted_legacy: list[dict[str, Any]],
) -> tuple[str, str]:
    return (
        hashlib.sha256(_canonical(sorted(legacy_event_ids))).hexdigest(),
        hashlib.sha256(_canonical(accepted_legacy)).hexdigest(),
    )


def _activation_legacy_digest_is_valid(
    value: dict[str, Any], accepted_legacy: list[dict[str, Any]],
) -> bool:
    historical_ids_digest, full_events_digest = _activation_digest_candidates(
        value["legacy_event_ids"], accepted_legacy,
    )
    observed_digest = value["legacy_events_sha256"]
    if "legacy_digest_kind" in value:
        return observed_digest == full_events_digest
    return sum((
        observed_digest == historical_ids_digest,
        observed_digest == full_events_digest,
    )) == 1


def validate_projection_authority(authority: dict[str, Any]) -> None:
    if not isinstance(authority, dict) or set(authority) != AUTHORITY_FIELDS:
        raise LinearProjectionError("invalid_projection_authority")
    if not all(isinstance(authority[field], str) and authority[field] for field in AUTHORITY_FIELDS):
        raise LinearProjectionError("invalid_projection_authority")


def build_projection_event(
    *, workstream_id: str, kind: str, key: str, value: dict[str, Any],
    plan_revision: str, expected_revision: int, created_at: str,
    supersedes_event_id: str | None = None, authority: dict[str, str] | None = None,
) -> dict[str, Any]:
    event = {
        "schema_version": 2,
        "workstream_id": workstream_id.upper(),
        "kind": kind,
        "key": key,
        "value": deepcopy(value),
        "plan_revision": plan_revision,
        "expected_revision": expected_revision,
        "created_at": created_at,
        "supersedes_event_id": supersedes_event_id,
        "authority": deepcopy(authority),
    }
    event["event_id"] = _event_id(event)
    validate_projection_event(event)
    return event


def validate_projection_event(event: dict[str, Any]) -> None:
    required = {
        "schema_version", "event_id", "workstream_id", "kind", "key",
        "value", "plan_revision", "expected_revision", "created_at",
        "supersedes_event_id",
    }
    schema_version = event.get("schema_version")
    if schema_version == 2:
        required.add("authority")
    if set(event) != required or schema_version not in {1, 2}:
        raise LinearProjectionError("invalid_projection_event_fields")
    if schema_version == 2:
        validate_projection_authority(event["authority"])
    if event.get("kind") not in KINDS:
        raise LinearProjectionError("invalid_projection_kind")
    for field in ("event_id", "workstream_id", "key", "plan_revision", "created_at"):
        if not isinstance(event.get(field), str) or not event[field].strip():
            raise LinearProjectionError(f"projection_missing:{field}")
    if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", event["workstream_id"]):
        raise LinearProjectionError("invalid_projection_workstream")
    if not isinstance(event.get("value"), dict):
        raise LinearProjectionError("invalid_projection_value")
    value = event["value"]
    tombstone = value == TOMBSTONE
    if event["kind"] == "generation_abort":
        if (
            schema_version != 2 or tombstone
            or event["supersedes_event_id"] is not None
            or event["key"] != value.get("reservation_id")
            or set(value) != {
                "schema_version", "reservation_id", "reservation_sha256",
                "reason", "original_projection_revision",
                "intervening_event_ids", "intervening_events_sha256",
                "original_occupant_event_id",
            }
            or value.get("schema_version") != schema_version
            or not re.fullmatch(
                r"wsgr_[0-9a-f]{32}", str(value.get("reservation_id", ""))
            )
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(value.get("reservation_sha256", ""))
            )
            or not isinstance(value.get("reason"), str) or not value["reason"]
            or not isinstance(value.get("original_projection_revision"), int)
            or isinstance(value.get("original_projection_revision"), bool)
            or value["original_projection_revision"] < 0
            or not isinstance(value.get("intervening_event_ids"), list)
            or not all(isinstance(item, str) and item
                       for item in value["intervening_event_ids"])
            or len(value["intervening_event_ids"])
            != len(set(value["intervening_event_ids"]))
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(value.get("intervening_events_sha256", ""))
            )
            or (
                value.get("original_occupant_event_id") is not None
                and not re.fullmatch(
                    r"wsp_[0-9a-f]{32}",
                    str(value["original_occupant_event_id"]),
                )
            )
        ):
            raise LinearProjectionError("invalid_generation_abort")
    if event["kind"] in {
        "generation_genesis", "generation_candidate_seal", "generation_transition",
    }:
        error_name = f"invalid_{event['kind']}"
        required_fields = (
            GENERATION_SEAL_FIELDS
            if event["kind"] == "generation_candidate_seal"
            else (
                GENERATION_CONTROL_V3_FIELDS
                if event["kind"] == "generation_transition"
                and value.get("schema_version") == 3
                else GENERATION_CONTROL_FIELDS
            )
        )
        if (
            schema_version != 2
            or tombstone
            or event["supersedes_event_id"] is not None
            or set(value) != required_fields
            or value.get("schema_version") not in (
                {2, 3} if event["kind"] == "generation_transition" else {2}
            )
            or not isinstance(value.get("from"), dict)
            or not isinstance(value.get("to"), dict)
            or set(value["from"]) != GENERATION_FRONTIER_FIELDS
            or set(value["to"]) != GENERATION_FRONTIER_FIELDS
            or not isinstance(value.get("source"), dict)
            or set(value["source"]) != GENERATION_SOURCE_FIELDS
            or not isinstance(value.get("retirement"), dict)
            or set(value["retirement"]) != GENERATION_RETIREMENT_FIELDS
            or not re.fullmatch(r"wsgr_[0-9a-f]{32}", str(value.get("reservation_id", "")))
            or not all(re.fullmatch(r"[0-9a-f]{64}", str(value.get(field, "")))
                       for field in (
                           "reservation_sha256", "graph_frontier_sha256",
                           "candidate_resume_sha256",
                       ))
            or not isinstance(value.get("activation_epoch"), int)
            or isinstance(value.get("activation_epoch"), bool)
            or value["activation_epoch"] < 0
        ):
            raise LinearProjectionError(error_name)
        for side in ("from", "to"):
            frontier = value[side]
            if (
                not all(isinstance(frontier.get(field), str) and frontier[field]
                        for field in (
                            "plan_revision", "source_event_id", "source_identity",
                            "source_sha256", "checkpoint_events_sha256",
                            "projection_frontier_event_id", "projection_events_sha256",
                        ))
                or not all(re.fullmatch(r"[0-9a-f]{64}", frontier[field])
                           for field in (
                               "plan_revision", "source_sha256",
                               "checkpoint_events_sha256", "projection_events_sha256",
                           ))
                or frontier["source_sha256"] != frontier["plan_revision"]
                or not isinstance(frontier.get("material_revision"), int)
                or isinstance(frontier.get("material_revision"), bool)
                or frontier["material_revision"] < 0
                or not isinstance(frontier.get("projection_revision"), int)
                or isinstance(frontier.get("projection_revision"), bool)
                or frontier["projection_revision"] <= 0
                or not isinstance(frontier.get("checkpoint_event_ids"), list)
                or frontier["checkpoint_event_ids"]
                != sorted(set(frontier["checkpoint_event_ids"]))
                or not all(isinstance(item, str) and item
                           for item in frontier["checkpoint_event_ids"])
                or frontier["checkpoint_events_sha256"]
                != hashlib.sha256(_canonical(frontier["checkpoint_event_ids"])).hexdigest()
            ):
                raise LinearProjectionError(error_name)
        retirement = value["retirement"]
        if (
            not re.fullmatch(r"[0-9a-f]{64}", str(retirement.get("predecessor_plan_revision", "")))
            or not isinstance(retirement.get("retired_at"), str)
            or not retirement["retired_at"]
            or not isinstance(retirement.get("retired_writer_epoch"), int)
            or isinstance(retirement.get("retired_writer_epoch"), bool)
            or retirement["retired_writer_epoch"] < 0
            or any(
                not isinstance(retirement.get(field), list)
                or retirement[field] != sorted(set(retirement[field]))
                or not all(isinstance(item, str) and item for item in retirement[field])
                for field in ("provenance_event_ids", "checkpoint_event_ids")
            )
            or not re.fullmatch(r"[0-9a-f]{64}", str(retirement.get("declaration_sha256", "")))
            or value["source"].get("sha256") != value["to"]["plan_revision"]
            or value["source"].get("identity") != value["to"]["source_identity"]
            or retirement["predecessor_plan_revision"] != value["from"]["plan_revision"]
            or retirement["retired_writer_epoch"] != value["activation_epoch"]
        ):
            raise LinearProjectionError(error_name)
        previous = value["previous_control_event_id"]
        if previous is not None and not re.fullmatch(r"wsp_[0-9a-f]{32}", str(previous)):
            raise LinearProjectionError(error_name)
        if event["kind"] == "generation_genesis":
            if (
                event["key"] != "root" or previous is not None
                or value["activation_epoch"] != 0
                or value["from"] != value["to"]
                or event["plan_revision"] != value["to"]["plan_revision"]
                or event["expected_revision"] != value["to"]["projection_revision"]
                or value["candidate_seal_event_id"] is not None
                or value["candidate_seal_sha256"] is not None
            ):
                raise LinearProjectionError(error_name)
        elif event["kind"] == "generation_candidate_seal":
            if (
                event["key"] != value["reservation_id"]
                or event["plan_revision"] != value["to"]["plan_revision"]
                or event["expected_revision"] != value["to"]["projection_revision"]
                or value["to"]["plan_revision"] == value["from"]["plan_revision"]
            ):
                raise LinearProjectionError(error_name)
        elif (
            event["key"] != "root"
            or event["plan_revision"] != value["from"]["plan_revision"]
            or event["expected_revision"] != value["from"]["projection_revision"]
            or value["to"]["plan_revision"] == value["from"]["plan_revision"]
            or not re.fullmatch(r"wsp_[0-9a-f]{32}", str(value.get("candidate_seal_event_id", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("candidate_seal_sha256", "")))
        ):
            raise LinearProjectionError(error_name)
        if (
            event["kind"] == "generation_transition"
            and value.get("schema_version") == 3
        ):
            from workstream_checkpoint import validate_checkpoint

            checkpoint = value.get("activation_checkpoint")
            try:
                if not isinstance(checkpoint, dict):
                    raise ValueError("checkpoint missing")
                validate_checkpoint(checkpoint)
            except (ValueError, TypeError) as error:
                raise LinearProjectionError(error_name) from error
            if (
                checkpoint["workstream_id"] != event["workstream_id"]
                or checkpoint["plan_revision"] != value["to"]["plan_revision"]
                or checkpoint["root_revision"] != value["to"]["material_revision"]
                or checkpoint["acknowledgement"] != {
                    "state": "pending", "remote_id": None,
                    "applied_revision": None,
                }
                or checkpoint["event_id"] not in value["to"]["checkpoint_event_ids"]
                or value["activation_checkpoint_sha256"]
                != hashlib.sha256(_canonical(checkpoint)).hexdigest()
            ):
                raise LinearProjectionError(error_name)
    if event["kind"] == "cas_activation":
        historical_fields = {"legacy_event_ids", "legacy_events_sha256"}
        tagged_fields = {*historical_fields, "legacy_digest_kind"}
        fields = set(value)
        if tombstone or (fields != historical_fields and fields != tagged_fields):
            raise LinearProjectionError("invalid_projection_cas_activation")
        legacy_ids = value["legacy_event_ids"]
        if (
            event["key"] != "root"
            or not isinstance(legacy_ids, list)
            or len(legacy_ids) != len(set(legacy_ids))
            or not all(isinstance(item, str) and item for item in legacy_ids)
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(value["legacy_events_sha256"])
            )
        ):
            raise LinearProjectionError("invalid_projection_cas_activation")
        if fields == tagged_fields and (
            value["legacy_digest_kind"] != LEGACY_DIGEST_KIND_FULL_EVENTS
        ):
            raise LinearProjectionError("invalid_projection_cas_activation")
    if event["kind"] == "quarantine_disposition":
        required_disposition = {
            "event_ids", "events_sha256", "review_artifact_identity",
            "review_artifact_sha256", "reviewed_at",
        }
        event_ids = value.get("event_ids") if isinstance(value, dict) else None
        if (
            tombstone
            or set(value) != required_disposition
            or event["key"] != "root"
            or not isinstance(event_ids, list)
            or not event_ids
            or event_ids != sorted(set(event_ids))
            or not all(isinstance(item, str) and item for item in event_ids)
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("events_sha256", "")))
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(value.get("review_artifact_sha256", ""))
            )
            or not all(
                isinstance(value.get(field), str) and value[field]
                for field in ("review_artifact_identity", "reviewed_at")
            )
        ):
            raise LinearProjectionError("invalid_projection_quarantine_disposition")
    if event["kind"] == "identity_history_seal":
        required_seal = {
            "schema_version", "root_issue_id", "plan_revision",
            "source_identity", "source_sha256", "expected_material_revision",
            "expected_projection_revision",
            "sealed_scope_event_id", "sealed_scope_value_sha256",
            "legacy_transitions",
            "sealed_projection_frontier_event_id",
            "sealed_projection_frontier_event_sha256",
            "legacy_projection_prefix_sha256", "repositories",
            "repositories_sha256", "observed_at",
        }
        repositories = value.get("repositories") if isinstance(value, dict) else None
        valid_repositories = (
            isinstance(repositories, list)
            and bool(repositories)
            and repositories == sorted(
                repositories, key=lambda item: str(item.get("repository_key", "")),
            )
            and len({item.get("repository_key") for item in repositories})
            == len(repositories)
            and all(
                isinstance(item, dict)
                and set(item) == {
                    "repository_key", "provider_repository_id", "canonical_slug",
                    "routes",
                }
                and isinstance(item.get("repository_key"), str)
                and bool(item["repository_key"])
                and isinstance(item.get("provider_repository_id"), str)
                and bool(item["provider_repository_id"])
                and isinstance(item.get("canonical_slug"), str)
                and bool(item["canonical_slug"])
                and isinstance(item.get("routes"), list)
                and bool(item["routes"])
                and item["routes"] == sorted(
                    item["routes"], key=lambda route: str(route.get("requested_slug", "")),
                )
                and len({route.get("requested_slug") for route in item["routes"]})
                == len(item["routes"])
                and all(
                    isinstance(route, dict)
                    and set(route) == {
                        "requested_slug", "resolved_slug", "provider_repository_id",
                        "requested_response_url", "canonical_response_url",
                        "redirect_count", "authenticated",
                    }
                    and route.get("authenticated") is True
                    and route.get("provider_repository_id")
                    == item["provider_repository_id"]
                    and route.get("resolved_slug") == item["canonical_slug"]
                    and isinstance(route.get("requested_slug"), str)
                    and bool(route["requested_slug"])
                    and isinstance(route.get("requested_response_url"), str)
                    and bool(route["requested_response_url"])
                    and isinstance(route.get("canonical_response_url"), str)
                    and bool(route["canonical_response_url"])
                    and isinstance(route.get("redirect_count"), int)
                    and not isinstance(route.get("redirect_count"), bool)
                    and route["redirect_count"] in {0, 1}
                    for route in item["routes"]
                )
                for item in repositories
            )
        )
        if (
            schema_version != 2
            or tombstone
            or set(value) != required_seal
            or event["key"] != value.get("sealed_scope_event_id")
            or value.get("root_issue_id") != event["authority"]["root_issue_id"]
            or value.get("plan_revision") != event["plan_revision"]
            or value.get("source_sha256") != event["plan_revision"]
            or not isinstance(value.get("source_identity"), str)
            or not value["source_identity"]
            or not isinstance(value.get("expected_material_revision"), int)
            or isinstance(value.get("expected_material_revision"), bool)
            or value["expected_material_revision"] < 0
            or value.get("expected_projection_revision") != event["expected_revision"]
            or not all(
                re.fullmatch(r"wsp_[0-9a-f]{32}", str(value.get(field, "")))
                for field in (
                    "sealed_scope_event_id",
                    "sealed_projection_frontier_event_id",
                )
            )
            or not isinstance(value.get("legacy_transitions"), list)
            or not value["legacy_transitions"]
            or not all(
                isinstance(item, dict)
                and set(item) == {
                    "predecessor_scope_event_id",
                    "predecessor_scope_value_sha256",
                    "transition_scope_event_id",
                    "transition_scope_value_sha256",
                }
                and all(
                    re.fullmatch(r"wsp_[0-9a-f]{32}", str(item.get(field, "")))
                    for field in (
                        "predecessor_scope_event_id", "transition_scope_event_id",
                    )
                )
                and all(
                    re.fullmatch(r"[0-9a-f]{64}", str(item.get(field, "")))
                    for field in (
                        "predecessor_scope_value_sha256",
                        "transition_scope_value_sha256",
                    )
                )
                for item in value["legacy_transitions"]
            )
            or len({
                item["transition_scope_event_id"]
                for item in value["legacy_transitions"]
            }) != len(value["legacy_transitions"])
            or not all(
                re.fullmatch(r"[0-9a-f]{64}", str(value.get(field, "")))
                for field in (
                    "sealed_scope_value_sha256",
                    "sealed_projection_frontier_event_sha256",
                    "legacy_projection_prefix_sha256", "repositories_sha256",
                )
            )
            or not isinstance(value.get("observed_at"), str)
            or not value["observed_at"]
            or not valid_repositories
            or value.get("repositories_sha256")
            != hashlib.sha256(_canonical(repositories)).hexdigest()
        ):
            raise LinearProjectionError("invalid_projection_identity_history_seal")
    if event["kind"] == "child_extension_authorization":
        legacy_authorization = {
            "root_issue_id", "route", "source", "plan_revision",
            "reviewed_candidate_key", "child_issue_id",
            "expected_material_revision", "expected_projection_revision",
            "initial_state",
        }
        current_authorization = {
            *legacy_authorization, "native_initialization",
            "generation_authority", "native_validation_sha256",
        }
        route = value.get("route") if isinstance(value, dict) else None
        source = value.get("source") if isinstance(value, dict) else None
        native = (
            value.get("native_initialization")
            if isinstance(value, dict) else None
        )
        generation = (
            value.get("generation_authority")
            if isinstance(value, dict) else None
        )
        generation_valid = (
            isinstance(generation, dict)
            and set(generation) == {
                "plan_revision", "description_plan_revision",
                "transition_tip_event_id", "activation_epoch",
                "authority_origin", "workstream_id", "authority", "source",
            }
            and generation.get("plan_revision") == event["plan_revision"]
            and generation.get("workstream_id") == event["workstream_id"]
            and generation.get("authority") == event["authority"]
            and generation.get("source") == source
            and generation.get("authority_origin") in {
                "legacy_description", "generation_genesis",
                "generation_transition",
            }
            and (
                generation.get("transition_tip_event_id") is None
                if generation.get("authority_origin") == "legacy_description"
                else re.fullmatch(
                    r"wsp_[0-9a-f]{32}",
                    str(generation.get("transition_tip_event_id", "")),
                ) is not None
            )
            and (
                generation.get("activation_epoch") is None
                if generation.get("authority_origin") == "legacy_description"
                else isinstance(generation.get("activation_epoch"), int)
                and not isinstance(generation.get("activation_epoch"), bool)
                and generation["activation_epoch"] >= 0
            )
            and (
                generation.get("description_plan_revision") is None
                or isinstance(generation["description_plan_revision"], str)
            )
        )
        native_valid = (
            set(value) == legacy_authorization
            or (
                set(value) == current_authorization
                and isinstance(native, dict)
                and set(native) == {"state_id", "assignee_id"}
                and isinstance(native.get("state_id"), str)
                and bool(native["state_id"].strip())
                and (
                    native.get("assignee_id") is None
                    or (
                        isinstance(native["assignee_id"], str)
                        and bool(native["assignee_id"].strip())
                    )
                )
                and generation_valid
                and re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(value.get("native_validation_sha256", "")),
                ) is not None
            )
        )
        if (
            schema_version != 2
            or tombstone
            or not native_valid
            or event["key"] != value.get("child_issue_id")
            or value.get("root_issue_id") != event["authority"]["root_issue_id"]
            or route != event["authority"]
            or not isinstance(source, dict)
            or set(source) != {"identity", "sha256"}
            or not all(isinstance(source.get(field), str) and source[field]
                       for field in ("identity", "sha256"))
            or source.get("sha256") != event["plan_revision"]
            or value.get("plan_revision") != event["plan_revision"]
            or not isinstance(value.get("reviewed_candidate_key"), str)
            or not value["reviewed_candidate_key"]
            or not isinstance(value.get("child_issue_id"), str)
            or not value["child_issue_id"]
            or value.get("expected_projection_revision")
            != event["expected_revision"]
            or not isinstance(value.get("expected_material_revision"), int)
            or isinstance(value.get("expected_material_revision"), bool)
            or value["expected_material_revision"] < 0
            or value.get("initial_state") != "planned_pending_projection"
        ):
            raise LinearProjectionError("invalid_child_extension_authorization")
    if event["kind"] == "existing_child_origin_seal":
        required = {
            "schema_version", "root_issue_id", "route",
            "native_root_readback", "native_root_readback_sha256",
            "root_description", "source",
            "plan_revision", "generation_authority", "scope_event_id",
            "scope_value_sha256", "repository_owner",
            "child_workstream_id", "child_issue_id", "child_parent_issue_id",
            "child_route", "native_child_readback",
            "native_child_readback_sha256", "root_projection_prefix",
            "root_history", "child_history", "pending_proposals",
            "custody_writer_retirement",
            "review_artifact", "expected_projection_revision",
            "initial_state",
        }
        generation = value.get("generation_authority")
        histories = [value.get("root_history"), value.get("child_history")]
        native_child = value.get("native_child_readback")
        native_root = value.get("native_root_readback")
        root_description = value.get("root_description")
        native_description = (
            native_child.get("description")
            if isinstance(native_child, dict) else None
        )
        native_state = (
            native_child.get("state") if isinstance(native_child, dict) else None
        )
        native_valid = (
            isinstance(native_root, dict)
            and set(native_root) == {
                "id", "identifier", "parent", "route", "state",
                "assignee_id", "created_at",
            }
            and native_root.get("id") == value.get("root_issue_id")
            and native_root.get("identifier") == event["workstream_id"]
            and native_root.get("parent") is None
            and native_root.get("route") == event["authority"]
            and isinstance(native_root.get("state"), dict)
            and set(native_root["state"]) == {"id", "name", "type"}
            and all(isinstance(native_root["state"].get(field), str)
                    and native_root["state"][field]
                    for field in ("id", "name", "type"))
            and (
                native_root.get("assignee_id") is None
                or isinstance(native_root.get("assignee_id"), str)
                and bool(native_root["assignee_id"])
            )
            and isinstance(native_root.get("created_at"), str)
            and bool(native_root["created_at"])
            and re.fullmatch(
                r"[0-9a-f]{64}",
                str(value.get("native_root_readback_sha256", "")),
            ) is not None
            and value.get("native_root_readback_sha256")
            == hashlib.sha256(_canonical(native_root)).hexdigest()
            and isinstance(root_description, dict)
            and set(root_description) == {"bytes", "sha256"}
            and isinstance(root_description.get("bytes"), int)
            and not isinstance(root_description.get("bytes"), bool)
            and root_description["bytes"] >= 0
            and re.fullmatch(
                r"[0-9a-f]{64}", str(root_description.get("sha256", "")),
            ) is not None
            and
            isinstance(native_child, dict)
            and set(native_child) == {
                "id", "identifier", "parent", "route", "state",
                "assignee_id", "created_at", "description",
            }
            and native_child.get("id") == value.get("child_issue_id")
            and native_child.get("identifier") == value.get("child_workstream_id")
            and native_child.get("parent") == {
                "id": value.get("root_issue_id"),
                "identifier": event["workstream_id"],
            }
            and native_child.get("route") == value.get("child_route")
            and isinstance(native_state, dict)
            and set(native_state) == {"id", "name", "type"}
            and all(isinstance(native_state.get(field), str) and native_state[field]
                    for field in ("id", "name", "type"))
            and (
                native_child.get("assignee_id") is None
                or isinstance(native_child.get("assignee_id"), str)
                and bool(native_child["assignee_id"])
            )
            and isinstance(native_child.get("created_at"), str)
            and bool(native_child["created_at"])
            and isinstance(native_description, dict)
            and set(native_description) == {"bytes", "sha256"}
            and isinstance(native_description.get("bytes"), int)
            and not isinstance(native_description.get("bytes"), bool)
            and native_description["bytes"] >= 0
            and re.fullmatch(
                r"[0-9a-f]{64}", str(native_description.get("sha256", "")),
            ) is not None
            and re.fullmatch(
                r"[0-9a-f]{64}",
                str(value.get("native_child_readback_sha256", "")),
            ) is not None
            and value.get("native_child_readback_sha256")
            == hashlib.sha256(_canonical(native_child)).hexdigest()
        )
        projection_prefix = value.get("root_projection_prefix")
        inert_frontier = value.get("pending_proposals")
        custody = value.get("custody_writer_retirement")
        control_valid = (
            isinstance(projection_prefix, dict)
            and set(projection_prefix) == {
                "revision", "through_event_id", "sha256",
            }
            and isinstance(projection_prefix.get("revision"), int)
            and not isinstance(projection_prefix.get("revision"), bool)
            and projection_prefix["revision"] >= 0
            and re.fullmatch(
                r"wsp_[0-9a-f]{32}",
                str(projection_prefix.get("through_event_id", "")),
            ) is not None
            and re.fullmatch(
                r"[0-9a-f]{64}", str(projection_prefix.get("sha256", "")),
            ) is not None
            and isinstance(inert_frontier, dict)
            and set(inert_frontier) == {"count", "proposal_ids_sha256"}
            and inert_frontier.get("count") == 0
            and inert_frontier.get("proposal_ids_sha256")
            == hashlib.sha256(_canonical([])).hexdigest()
            and isinstance(custody, dict)
            and set(custody) == {
                "custodian", "previous_writers_retired", "writers_retired_at",
            }
            and isinstance(custody.get("custodian"), str)
            and bool(custody["custodian"])
            and custody.get("previous_writers_retired") is True
            and isinstance(custody.get("writers_retired_at"), str)
            and bool(custody["writers_retired_at"])
        )
        histories_valid = all(
            isinstance(history, dict)
            and set(history) == {
                "material_frontier", "material_receipts",
                "checkpoint_frontier", "checkpoint_receipts",
            }
            and isinstance(history.get("material_frontier"), dict)
            and set(history["material_frontier"]) == {
                "algorithm", "revision", "event_ids_reducer_order_sha256",
                "events_sha256", "remote_map_sha256",
            }
            and history["material_frontier"].get("algorithm")
            == "raw-reducer-order-v1"
            and isinstance(history["material_frontier"].get("revision"), int)
            and not isinstance(history["material_frontier"]["revision"], bool)
            and history["material_frontier"]["revision"] >= 0
            and isinstance(history.get("checkpoint_frontier"), dict)
            and set(history["checkpoint_frontier"]) == {
                "algorithm", "count", "revision",
                "event_ids_reducer_order_sha256",
                "event_ids_sorted_set_sha256", "checkpoints_sha256",
            }
            and history["checkpoint_frontier"].get("algorithm")
            == "checkpoint-reducer-order-v1"
            and all(
                isinstance(history["checkpoint_frontier"].get(field), int)
                and not isinstance(history["checkpoint_frontier"][field], bool)
                and history["checkpoint_frontier"][field] >= 0
                for field in ("count", "revision")
            )
            and all(
                isinstance(history.get(receipt_field), list)
                and all(
                    isinstance(item, dict)
                    and set(item) == {"event_id", "remote_id", "body_sha256"}
                    and isinstance(item.get("remote_id"), str)
                    and bool(item["remote_id"])
                    and re.fullmatch(
                        r"[0-9a-f]{64}", str(item.get("body_sha256", "")),
                    ) is not None
                    for item in history[receipt_field]
                )
                for receipt_field in (
                    "material_receipts", "checkpoint_receipts",
                )
            )
            and len(history["material_receipts"])
            == history["material_frontier"]["revision"]
            and len(history["checkpoint_receipts"])
            == history["checkpoint_frontier"]["count"]
            and all(re.fullmatch(r"[0-9a-f]{64}", str(frontier.get(field, "")))
                    for frontier, fields in (
                        (history["material_frontier"], (
                            "event_ids_reducer_order_sha256", "events_sha256",
                            "remote_map_sha256",
                        )),
                        (history["checkpoint_frontier"], (
                            "event_ids_reducer_order_sha256",
                            "event_ids_sorted_set_sha256", "checkpoints_sha256",
                        )),
                    ) for field in fields)
            and history["material_frontier"]["event_ids_reducer_order_sha256"]
            == hashlib.sha256(_canonical([
                item["event_id"] for item in history["material_receipts"]
            ])).hexdigest()
            and history["material_frontier"]["remote_map_sha256"]
            == hashlib.sha256(_canonical({
                item["event_id"]: item["remote_id"]
                for item in history["material_receipts"]
            })).hexdigest()
            and history["checkpoint_frontier"]["event_ids_reducer_order_sha256"]
            == hashlib.sha256(_canonical([
                item["event_id"] for item in history["checkpoint_receipts"]
            ])).hexdigest()
            and history["checkpoint_frontier"]["event_ids_sorted_set_sha256"]
            == hashlib.sha256(_canonical(sorted({
                item["event_id"] for item in history["checkpoint_receipts"]
            }))).hexdigest()
            for history in histories
        )
        generation_valid = (
            isinstance(generation, dict)
            and set(generation) == {
                "plan_revision", "description_plan_revision",
                "transition_tip_event_id", "activation_epoch",
                "authority_origin", "workstream_id", "authority", "source",
            }
            and generation.get("plan_revision") == event["plan_revision"]
            and generation.get("workstream_id") == event["workstream_id"]
            and generation.get("authority") == event["authority"]
            and generation.get("source") == value.get("source")
            and generation.get("authority_origin") in {
                "legacy_description", "generation_genesis",
                "generation_transition",
            }
            and (
                generation.get("transition_tip_event_id") is None
                if generation.get("authority_origin") == "legacy_description"
                else re.fullmatch(
                    r"wsp_[0-9a-f]{32}",
                    str(generation.get("transition_tip_event_id", "")),
                ) is not None
            )
            and (
                generation.get("activation_epoch") is None
                if generation.get("authority_origin") == "legacy_description"
                else isinstance(generation.get("activation_epoch"), int)
                and not isinstance(generation.get("activation_epoch"), bool)
                and generation["activation_epoch"] >= 0
            )
        )
        if (
            schema_version != 2 or tombstone or set(value) != required
            or value.get("schema_version") != 1
            or event["supersedes_event_id"] is not None
            or event["key"] != value.get("child_issue_id")
            or value.get("root_issue_id") != event["authority"]["root_issue_id"]
            or value.get("route") != event["authority"]
            or value.get("child_parent_issue_id") != value.get("root_issue_id")
            or value.get("child_route") != {
                key: event["authority"][key]
                for key in ("workspace_id", "team_id", "project_id")
            }
            or value.get("plan_revision") != event["plan_revision"]
            or value.get("expected_projection_revision")
            != event["expected_revision"]
            or not isinstance(value.get("source"), dict)
            or set(value["source"]) != {"identity", "sha256"}
            or value["source"].get("sha256") != event["plan_revision"]
            or not re.fullmatch(
                r"[A-Z][A-Z0-9]*-\d+",
                str(value.get("child_workstream_id", "")),
            )
            or not re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
                r"[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                str(value.get("child_issue_id", "")), re.IGNORECASE,
            )
            or not all(isinstance(value.get(field), str) and value[field]
                       for field in (
                           "scope_event_id", "scope_value_sha256",
                           "repository_owner",
                       ))
            or re.fullmatch(
                r"[0-9a-f]{64}", str(value.get("scope_value_sha256", "")),
            ) is None
            or not _valid_review_artifact(value.get("review_artifact"))
            or value.get("initial_state")
            != "existing_scope_owned_legacy_child"
            or not histories_valid or not generation_valid or not native_valid
            or not control_valid
        ):
            raise LinearProjectionError("invalid_existing_child_origin_seal")
    if event["kind"] == "child_dependency_authorization":
        required_authorization = {
            "root_issue_id", "route", "plan_revision", "batch_id",
            "relation_ids", "relations_sha256", "expected_material_revision",
            "expected_projection_revision", "expected_graph_revision",
            "expected_graph_sha256",
            "initial_state",
        }
        route = value.get("route") if isinstance(value, dict) else None
        relation_ids = value.get("relation_ids") if isinstance(value, dict) else None
        if (
            schema_version != 2
            or tombstone
            or set(value) != required_authorization
            or event["key"] != value.get("batch_id")
            or not re.fullmatch(r"wsdb_[0-9a-f]{32}", str(value.get("batch_id", "")))
            or value.get("root_issue_id") != event["authority"]["root_issue_id"]
            or route != event["authority"]
            or value.get("plan_revision") != event["plan_revision"]
            or value.get("expected_projection_revision") != event["expected_revision"]
            or not isinstance(relation_ids, list)
            or not relation_ids
            or relation_ids != sorted(set(relation_ids))
            or not all(re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
                r"[89ab][0-9a-f]{3}-[0-9a-f]{12}", str(item), re.IGNORECASE,
            ) for item in relation_ids)
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("relations_sha256", "")))
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(value.get("expected_graph_sha256", "")),
            )
            or any(
                not isinstance(value.get(field), int)
                or isinstance(value.get(field), bool)
                or value[field] < 0
                for field in ("expected_material_revision", "expected_graph_revision")
            )
            or value.get("initial_state") != "owned_children_validated"
        ):
            raise LinearProjectionError("invalid_child_dependency_authorization")
    if event["kind"] == "child_mutation_authorization":
        required = {
            "root_issue_id", "route", "source", "plan_revision",
            "generation_authority", "scope_event_id", "scope_value_sha256",
            "child_origin",
            "repository_owner", "child_workstream_id", "child_issue_id",
            "child_parent_issue_id", "child_route", "mutation_kind",
            "proposal_id", "proposal_remote_id", "record_sha256",
            "expected_material_revision", "predecessor_event_id",
        }
        mutation_generation = value.get("generation_authority")
        if (
            schema_version != 2 or tombstone or set(value) != required
            or event["key"] != value.get("proposal_id")
            or value.get("root_issue_id") != event["authority"]["root_issue_id"]
            or value.get("route") != event["authority"]
            or value.get("child_route") != {
                key: event["authority"][key]
                for key in ("workspace_id", "team_id", "project_id")
            }
            or value.get("child_parent_issue_id") != value.get("root_issue_id")
            or value.get("plan_revision") != event["plan_revision"]
            or not re.fullmatch(r"wscp_[0-9a-f]{32}", str(value.get("proposal_id", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("record_sha256", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("scope_value_sha256", "")))
            or not re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
                r"[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                str(value.get("child_issue_id", "")), re.IGNORECASE,
            )
            or not re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
                r"[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                str(value.get("proposal_remote_id", "")), re.IGNORECASE,
            )
            or not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(
                value.get("child_workstream_id", "")
            ))
            or value.get("mutation_kind") not in {"event", "checkpoint"}
            or not isinstance(value.get("child_origin"), dict)
            or not all(isinstance(value.get(field), str) and value[field]
                       for field in (
                           "scope_event_id", "scope_value_sha256",
                           "repository_owner", "child_workstream_id",
                           "child_issue_id", "proposal_remote_id",
                       ))
            or not isinstance(value.get("expected_material_revision"), int)
            or isinstance(value.get("expected_material_revision"), bool)
            or value["expected_material_revision"] < 0
            or (value.get("predecessor_event_id") is not None
                and not isinstance(value["predecessor_event_id"], str))
            or not isinstance(value.get("source"), dict)
            or set(value["source"]) != {"identity", "sha256"}
            or value["source"].get("sha256") != event["plan_revision"]
            or not isinstance(mutation_generation, dict)
            or set(mutation_generation) != {
                "plan_revision", "description_plan_revision",
                "transition_tip_event_id", "activation_epoch",
                "authority_origin", "workstream_id", "authority", "source",
            }
            or mutation_generation.get("plan_revision") != event["plan_revision"]
            or mutation_generation.get("workstream_id") != event["workstream_id"]
            or mutation_generation.get("authority") != event["authority"]
            or mutation_generation.get("source") != value["source"]
            or mutation_generation.get("authority_origin") not in {
                "legacy_description", "generation_genesis", "generation_transition",
            }
            or (
                mutation_generation.get("transition_tip_event_id") is None
                if mutation_generation.get("authority_origin") == "legacy_description"
                else re.fullmatch(
                    r"wsp_[0-9a-f]{32}", str(
                        mutation_generation.get("transition_tip_event_id", "")
                    )
                ) is not None
            ) is not True
            or (
                mutation_generation.get("activation_epoch") is None
                if mutation_generation.get("authority_origin") == "legacy_description"
                else isinstance(mutation_generation.get("activation_epoch"), int)
                and not isinstance(mutation_generation.get("activation_epoch"), bool)
                and mutation_generation["activation_epoch"] >= 0
            ) is not True
        ):
            raise LinearProjectionError("invalid_child_mutation_authorization")
    if event["kind"] == "source" and not tombstone:
        if not re.fullmatch(r"[0-9a-f]{64}", str(value.get("sha256", ""))):
            raise LinearProjectionError("invalid_projection_source_digest")
        if not any(isinstance(value.get(field), str) and value[field].strip()
                   for field in ("url", "identity")):
            raise LinearProjectionError("invalid_projection_source_identity")
    if event["kind"] == "provenance" and not tombstone:
        for field in ("agent", "machine", "session_id"):
            if not isinstance(value.get(field), str) or not value[field].strip():
                raise LinearProjectionError(f"invalid_projection_provenance:{field}")
    if event["kind"] == "closure_review" and not tombstone:
        review_fields_v1 = {
            "schema_version", "workstream_id", "snapshot_sha256",
            "closure_input_sha256", "repository_key", "exact_head", "verdict",
            "reviewer_agent", "reviewer_session_id", "implementer_session_id",
            "reviewed_at", "review_artifact_identity", "review_artifact_sha256",
            "trust_boundary", "procedural_independence",
        }
        review_fields_v2 = {
            "schema_version", "workstream_id", "snapshot_sha256",
            "closure_input_sha256", "repository_heads", "repository_truth_sha256",
            "verdict", "reviewer_agent", "reviewer_session_id",
            "implementer_session_id", "reviewed_at", "review_artifact_identity",
            "review_artifact_sha256", "trust_boundary", "procedural_independence",
        }
        review_version = value.get("schema_version")
        if not (
            (review_version == 1 and set(value) == review_fields_v1)
            or (review_version == 2 and set(value) == review_fields_v2)
        ):
            raise LinearProjectionError("invalid_projection_closure_review")
        if (
            event["key"] != value.get("snapshot_sha256")
            or value.get("workstream_id") != event["workstream_id"]
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("snapshot_sha256", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("closure_input_sha256", "")))
            or value.get("verdict") != "pass"
            or value.get("trust_boundary") != "shared_linear_credential"
            or value.get("procedural_independence") is not True
            or value.get("reviewer_session_id") == value.get("implementer_session_id")
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(value.get("review_artifact_sha256", ""))
            )
            or not all(isinstance(value.get(field), str) and value[field]
                       for field in ("reviewer_agent", "reviewer_session_id",
                                     "implementer_session_id", "reviewed_at",
                                     "review_artifact_identity"))
        ):
            raise LinearProjectionError("invalid_projection_closure_review")
        if review_version == 1 and (
            not isinstance(value.get("repository_key"), str)
            or not value["repository_key"]
            or not re.fullmatch(
                r"[0-9a-f]{40}(?:[0-9a-f]{24})?", str(value.get("exact_head", ""))
            )
        ):
            raise LinearProjectionError("invalid_projection_closure_review")
        if review_version == 2:
            heads = value.get("repository_heads")
            if (
                not isinstance(heads, dict) or len(heads) < 2
                or not all(isinstance(key, str) and key for key in heads)
                or not all(re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", str(head))
                           for head in heads.values())
                or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("repository_truth_sha256", "")))
            ):
                raise LinearProjectionError("invalid_projection_closure_review")
    if event["kind"] == "disposition" and not tombstone:
        if value.get("disposition") not in {"attach", "create_successor"}:
            raise LinearProjectionError("invalid_projection_disposition")
        if not isinstance(value.get("remote_head"), str) or not re.fullmatch(
            r"[0-9a-f]{40}(?:[0-9a-f]{24})?", value["remote_head"]
        ):
            raise LinearProjectionError("invalid_projection_disposition_head")
        if "recovered_from_checkpoint" not in value or (
            value["recovered_from_checkpoint"] is not None
            and not isinstance(value["recovered_from_checkpoint"], str)
        ):
            raise LinearProjectionError("invalid_projection_disposition_checkpoint")
    if event["kind"] == "lifecycle" and not tombstone:
        required_v1 = {
            "status", "github", "shipyard_receipt", "closure_input_sha256",
            "snapshot_sha256", "independent_review", "closure_receipt_sha256",
        }
        required_v2 = {
            "status", "repositories", "repository_truth_sha256",
            "closure_input_sha256", "snapshot_sha256", "independent_review",
            "closure_receipt_sha256",
        }
        if set(value) not in (required_v1, required_v2):
            raise LinearProjectionError("invalid_projection_lifecycle_fields")
        if value["status"] not in {"In Progress", "Landed — acceptance review required", "Done"}:
            raise LinearProjectionError("invalid_projection_lifecycle_status")
        if set(value) == required_v1 and (
            not isinstance(value["github"], dict)
            or not isinstance(value["shipyard_receipt"], dict)
        ):
            raise LinearProjectionError("invalid_projection_lifecycle:repositories")
        repository_truths = (
            [{"repository_key": value["shipyard_receipt"].get("repository_key"),
              "github": value["github"], "shipyard_receipt": value["shipyard_receipt"]}]
            if set(value) == required_v1 else value["repositories"]
        )
        if (
            not isinstance(repository_truths, list)
            or len(repository_truths) < (1 if set(value) == required_v1 else 2)
        ):
            raise LinearProjectionError("invalid_projection_lifecycle:repositories")
        seen_repository_keys: set[str] = set()
        for truth in repository_truths:
            if not isinstance(truth, dict) or set(truth) != {
                "repository_key", "github", "shipyard_receipt",
            }:
                raise LinearProjectionError("invalid_projection_lifecycle:repositories")
            repository_key_value = truth["repository_key"]
            if not isinstance(repository_key_value, str) or not repository_key_value or repository_key_value in seen_repository_keys:
                raise LinearProjectionError("invalid_projection_lifecycle:repositories")
            seen_repository_keys.add(repository_key_value)
            github = truth["github"]
            shipyard = truth["shipyard_receipt"]
            if not isinstance(github, dict) or set(github) != {
            "repository", "provider_repository_id", "pr_number", "pr_head",
            "merged", "merge_sha",
            }:
                raise LinearProjectionError("invalid_projection_lifecycle:github")
            if (
                not re.fullmatch(r"[a-z0-9_.-]+/[a-z0-9_.-]+", str(github["repository"]))
                or not isinstance(github["provider_repository_id"], str)
                or not github["provider_repository_id"]
                or not isinstance(github["pr_number"], int) or github["pr_number"] <= 0
                or not re.fullmatch(r"[0-9a-f]{40}", str(github["pr_head"]))
                or github["merged"] is not True
                or not re.fullmatch(r"[0-9a-f]{40}", str(github["merge_sha"]))
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:github")
            if not isinstance(shipyard, dict) or set(shipyard) != {
            "schema_version", "repository", "repository_key", "pr_number", "head",
            "disposition", "receipt_id", "receipt_sha256",
            } or shipyard.get("schema_version") != 1:
                raise LinearProjectionError("invalid_projection_lifecycle:shipyard_receipt")
            shipyard_digest = hashlib.sha256(_canonical({
                key: item for key, item in shipyard.items() if key != "receipt_sha256"
            })).hexdigest()
            if (
                shipyard.get("repository") != github["repository"]
                or shipyard.get("repository_key") != repository_key_value
                or shipyard.get("pr_number") != github["pr_number"]
                or shipyard.get("head") != github["pr_head"]
                or shipyard.get("disposition") not in {"merged", "already_merged", "landed"}
                or not isinstance(shipyard.get("receipt_id"), str) or not shipyard["receipt_id"]
                or shipyard.get("receipt_sha256") != shipyard_digest
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:shipyard_receipt")
        if set(value) == required_v2 and value.get("repository_truth_sha256") != hashlib.sha256(
            _canonical(repository_truths)
        ).hexdigest():
            raise LinearProjectionError("invalid_projection_lifecycle:repository_truth_sha256")
        if not re.fullmatch(r"[0-9a-f]{64}", str(value["closure_input_sha256"])):
            raise LinearProjectionError("invalid_projection_lifecycle:closure_input_sha256")
        if not re.fullmatch(r"[0-9a-f]{64}", str(value["snapshot_sha256"])):
            raise LinearProjectionError("invalid_projection_lifecycle:snapshot_sha256")
        if value["status"] == "Done":
            if not isinstance(value["independent_review"], dict):
                raise LinearProjectionError("done_requires_independent_review")
            review = value["independent_review"]
            review_v1 = {
                "schema_version", "workstream_id", "snapshot_sha256",
                "closure_input_sha256", "repository_key", "exact_head", "verdict",
                "reviewer_agent", "reviewer_session_id", "implementer_session_id",
                "reviewed_at", "review_artifact_identity", "review_artifact_sha256",
                "trust_boundary", "procedural_independence",
            }
            review_v2 = {
                "schema_version", "workstream_id", "snapshot_sha256",
                "closure_input_sha256", "repository_heads", "repository_truth_sha256",
                "verdict", "reviewer_agent", "reviewer_session_id",
                "implementer_session_id", "reviewed_at", "review_artifact_identity",
                "review_artifact_sha256", "trust_boundary", "procedural_independence",
            }
            aggregate_heads = {
                truth["repository_key"]: truth["github"]["pr_head"]
                for truth in repository_truths
            }
            if not (
                (set(value) == required_v1 and set(review) == review_v1 and review.get("schema_version") == 1)
                or (set(value) == required_v2 and set(review) == review_v2 and review.get("schema_version") == 2)
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:independent_review")
            if (
                review.get("workstream_id") != event["workstream_id"]
                or review.get("snapshot_sha256") != value["snapshot_sha256"]
                or review.get("closure_input_sha256") != value["closure_input_sha256"]
                or review.get("verdict") != "pass"
                or review.get("trust_boundary") != "shared_linear_credential"
                or review.get("procedural_independence") is not True
                or review.get("reviewer_session_id") == review.get("implementer_session_id")
                or not re.fullmatch(r"[0-9a-f]{64}", str(review.get("snapshot_sha256", "")))
                or not re.fullmatch(
                    r"[0-9a-f]{64}", str(review.get("review_artifact_sha256", ""))
                )
                or not all(isinstance(review.get(field), str) and review[field]
                           for field in ("reviewer_agent", "reviewer_session_id",
                                         "implementer_session_id", "reviewed_at",
                                         "review_artifact_identity"))
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:independent_review")
            if set(value) == required_v1 and (
                review.get("repository_key") != repository_truths[0]["repository_key"]
                or review.get("exact_head") != repository_truths[0]["github"]["pr_head"]
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:independent_review")
            if set(value) == required_v2 and (
                review.get("repository_heads") != aggregate_heads
                or review.get("repository_truth_sha256") != value["repository_truth_sha256"]
            ):
                raise LinearProjectionError("invalid_projection_lifecycle:independent_review")
            if not re.fullmatch(r"[0-9a-f]{64}", str(value["closure_receipt_sha256"])):
                raise LinearProjectionError("done_requires_closure_receipt")
        elif value["independent_review"] is not None or value["closure_receipt_sha256"] is not None:
            raise LinearProjectionError("non_done_lifecycle_has_closure_receipt")
    if event["kind"] == "choice" and not tombstone and value.get("event_id") != event["key"]:
        raise LinearProjectionError("projection_choice_key_mismatch")
    if event["kind"] == "evidence_contract" and not tombstone:
        if value.get("slice_id") != event["key"]:
            raise LinearProjectionError("projection_evidence_key_mismatch")
        if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(value.get("owning_child", ""))):
            raise LinearProjectionError("projection_evidence_owner_invalid")
    if event["kind"] == "child_closure" and not tombstone:
        required_closure = {
            "schema_version", "child_identifier", "child_issue_id",
            "parent_issue_id", "workspace_id", "team_id", "project_id",
            "assignee_id", "state_id", "state_name", "state_type",
            "plan_revision", "repository_key", "exact_head",
            "evidence_heads", "evidence_receipts_sha256",
            "child_readback_sha256",
        }
        evidence_heads = value.get("evidence_heads")
        valid_evidence_heads = (
            isinstance(evidence_heads, list)
            and bool(evidence_heads)
            and all(
                isinstance(item, dict)
                and set(item) == {"key", "event_id", "value_sha256"}
                and all(isinstance(item.get(field), str) and item[field]
                        for field in ("key", "event_id"))
                and re.fullmatch(r"[0-9a-f]{64}", str(item.get("value_sha256", "")))
                for item in evidence_heads
            )
        )
        if (
            set(value) != required_closure
            or value.get("schema_version") not in {1, 2}
            or event["key"] != value.get("child_identifier")
            or value.get("plan_revision") != event["plan_revision"]
            or value.get("state_type") != "completed"
            or not valid_evidence_heads
            or evidence_heads != sorted(
                evidence_heads, key=lambda item: (item.get("key", ""), item.get("event_id", ""))
            )
            or not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", str(value.get("exact_head", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("evidence_receipts_sha256", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("child_readback_sha256", "")))
            or not all(
                isinstance(value.get(field), str) and value[field]
                for field in (
                    "child_identifier", "child_issue_id", "parent_issue_id",
                    "workspace_id", "team_id", "project_id",
                    "state_id", "state_name", "repository_key",
                )
            )
            or (
                value.get("schema_version") == 1
                and not (
                    isinstance(value.get("assignee_id"), str)
                    and bool(value["assignee_id"])
                )
            )
            or (
                value.get("schema_version") == 2
                and not (
                    value.get("assignee_id") is None
                    or (
                        isinstance(value.get("assignee_id"), str)
                        and bool(value["assignee_id"])
                    )
                )
            )
        ):
            raise LinearProjectionError("invalid_projection_child_closure")
    revision = event.get("expected_revision")
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
        raise LinearProjectionError("invalid_projection_revision")
    supersedes = event.get("supersedes_event_id")
    if supersedes is not None and (not isinstance(supersedes, str) or not supersedes):
        raise LinearProjectionError("invalid_projection_supersedes")
    if event.get("event_id") != _event_id(event):
        raise LinearProjectionError("projection_event_id_mismatch")


def encode_projection_comment(event: dict[str, Any]) -> str:
    validate_projection_event(event)
    material = _canonical(event)
    envelope = {
        "event": event,
        "sha256": hashlib.sha256(material).hexdigest(),
    }
    encoded = base64.urlsafe_b64encode(_canonical(envelope)).decode("ascii").rstrip("=")
    return f"{PROJECTION_PREFIX}{encoded} -->"


def _decode_projection(encoded: str) -> dict[str, Any]:
    try:
        envelope = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        if set(envelope) != {"event", "sha256"}:
            raise ValueError("unexpected envelope fields")
        event = envelope["event"]
        digest = envelope["sha256"]
        if not isinstance(event, dict) or not isinstance(digest, str):
            raise ValueError("invalid envelope")
        if not hmac.compare_digest(digest, hashlib.sha256(_canonical(event)).hexdigest()):
            raise ValueError("digest mismatch")
        validate_projection_event(event)
        return event
    except (
        binascii.Error, json.JSONDecodeError, KeyError, TypeError, ValueError,
        LinearProjectionError,
    ) as error:
        raise LinearProjectionError("malformed_projection_marker") from error


@dataclass(frozen=True)
class ReducedProjection:
    workstream_id: str
    revision: int
    events: tuple[dict[str, Any], ...]
    remote_ids: dict[str, str]
    snapshot: dict[str, Any]


def child_extension_authorizations_from_comments(
    comments: list[dict[str, Any]], *, workstream_id: str,
    description_plan_revision: str | None,
    authenticated_route: dict[str, str],
) -> list[dict[str, Any]]:
    """Return immutable child origins from current and sealed generations."""
    selected = select_plan_generation(
        comments, workstream_id=workstream_id,
        description_plan_revision=description_plan_revision,
        authenticated_route=authenticated_route,
    )
    from workstream_generation import generation_controls
    controls = generation_controls(comments)
    plans = {selected["plan_revision"]}
    plans.update(
        frontier["plan_revision"] for control in controls
        for frontier in (control["value"]["from"], control["value"]["to"])
    )
    result = []
    for plan in plans:
        state = reduce_projection_comments(
            comments, workstream_id=workstream_id,
            expected_plan_revision=plan, authenticated_route=authenticated_route,
        )
        events = list(state.events)
        if plan != selected["plan_revision"]:
            retirements = [
                control for control in controls
                if control["kind"] == "generation_transition"
                and control["value"]["from"]["plan_revision"] == plan
            ]
            if len(retirements) != 1:
                continue
            events = events[:retirements[0]["value"]["from"]["projection_revision"]]
        result.extend(
            event for event in events
            if event["kind"] == "child_extension_authorization"
        )
    return result


def legacy_child_origin_repairs_from_comments(
    comments: list[dict[str, Any]], *, workstream_id: str,
    description_plan_revision: str | None,
    authenticated_route: dict[str, str],
) -> list[dict[str, Any]]:
    """Return reviewed legacy child-origin seals from active/sealed generations."""
    selected = select_plan_generation(
        comments, workstream_id=workstream_id,
        description_plan_revision=description_plan_revision,
        authenticated_route=authenticated_route,
    )
    from workstream_generation import generation_controls

    controls = generation_controls(comments)
    plans = {selected["plan_revision"]}
    plans.update(
        frontier["plan_revision"] for control in controls
        for frontier in (control["value"]["from"], control["value"]["to"])
    )
    result: list[dict[str, Any]] = []
    for plan in plans:
        state = reduce_projection_comments(
            comments, workstream_id=workstream_id,
            expected_plan_revision=plan, authenticated_route=authenticated_route,
        )
        events = list(state.events)
        if plan != selected["plan_revision"]:
            retirements = [
                control for control in controls
                if control["kind"] == "generation_transition"
                and control["value"]["from"]["plan_revision"] == plan
            ]
            if len(retirements) != 1:
                continue
            events = events[:retirements[0]["value"]["from"]["projection_revision"]]
        result.extend(
            event for event in events
            if event["kind"] == "existing_child_origin_seal"
        )
    return result


def validate_child_origin_value(
    value: dict[str, Any], *, extension_origins: list[dict[str, Any]],
    repair_origins: list[dict[str, Any]],
    authenticated_route: dict[str, str],
) -> None:
    """Validate one mutation grant's immutable child-creation provenance."""
    origin = value.get("child_origin")
    if not isinstance(origin, dict):
        raise LinearProjectionError("child_origin_provenance_missing")
    if origin.get("kind") == "child_extension_authorization":
        matches = [
            event for event in extension_origins
            if event["event_id"] == origin.get("event_id")
        ]
        if len(matches) != 1:
            raise LinearProjectionError("child_origin_authorization_missing")
        origin_value = matches[0]["value"]
        from workstream_linear import deterministic_existing_root_child_id
        expected_id = deterministic_existing_root_child_id(
            workspace_id=authenticated_route["workspace_id"],
            team_id=authenticated_route["team_id"],
            project_id=authenticated_route["project_id"],
            root_issue_id=authenticated_route["root_issue_id"],
            child_stable_key=origin_value.get("reviewed_candidate_key"),
        )
        if (
            set(origin) != {"kind", "event_id", "value_sha256", "candidate_key"}
            or origin.get("candidate_key")
            != origin_value.get("reviewed_candidate_key")
            or origin.get("value_sha256")
            != hashlib.sha256(_canonical(origin_value)).hexdigest()
            or origin_value.get("root_issue_id")
            != authenticated_route["root_issue_id"]
            or origin_value.get("route") != authenticated_route
            or expected_id != value.get("child_issue_id")
        ):
            raise LinearProjectionError("child_origin_authorization_invalid")
        return
    if origin.get("kind") == "deterministic_intake_marker":
        from workstream_linear import deterministic_issue_id
        marker = {
            "root_stable_key": origin.get("root_stable_key"),
            "child_stable_key": origin.get("child_stable_key"),
        }
        if (
            set(origin) != {
                "kind", "root_stable_key", "child_stable_key", "marker_sha256",
            }
            or origin.get("marker_sha256")
            != hashlib.sha256(_canonical(marker)).hexdigest()
            or deterministic_issue_id(
                workspace_id=authenticated_route["workspace_id"],
                team_id=authenticated_route["team_id"],
                project_id=authenticated_route["project_id"],
                root_stable_key=origin.get("root_stable_key"),
            ) != authenticated_route["root_issue_id"]
            or deterministic_issue_id(
                workspace_id=authenticated_route["workspace_id"],
                team_id=authenticated_route["team_id"],
                project_id=authenticated_route["project_id"],
                root_stable_key=origin.get("root_stable_key"),
                child_stable_key=origin.get("child_stable_key"),
            ) != value.get("child_issue_id")
        ):
            raise LinearProjectionError("child_origin_intake_marker_invalid")
        return
    if origin.get("kind") == "existing_child_origin_seal":
        matches = [
            event for event in repair_origins
            if event["event_id"] == origin.get("event_id")
        ]
        if len(matches) != 1:
            raise LinearProjectionError("child_origin_repair_missing")
        repair = matches[0]
        repair_value = repair["value"]
        if (
            set(origin) != {
                "kind", "event_id", "value_sha256", "child_workstream_id",
            }
            or origin.get("value_sha256")
            != hashlib.sha256(_canonical(repair_value)).hexdigest()
            or origin.get("child_workstream_id")
            != repair_value.get("child_workstream_id")
            or repair.get("authority") != authenticated_route
            or repair_value.get("route") != authenticated_route
            or repair_value.get("root_issue_id")
            != authenticated_route["root_issue_id"]
            or repair_value.get("child_issue_id") != value.get("child_issue_id")
            or repair_value.get("child_workstream_id")
            != value.get("child_workstream_id")
        ):
            raise LinearProjectionError("child_origin_repair_invalid")
        return
    raise LinearProjectionError("child_origin_provenance_invalid")


def child_mutation_authorizations_from_comments(
    comments: list[dict[str, Any]], *, workstream_id: str,
    description_plan_revision: str | None,
    authenticated_route: dict[str, str],
) -> list[dict[str, Any]]:
    """Return current or sealed-retired child proposal activations."""
    selected = select_plan_generation(
        comments, workstream_id=workstream_id,
        description_plan_revision=description_plan_revision,
        authenticated_route=authenticated_route,
    )
    from workstream_generation import generation_controls

    controls = generation_controls(comments)
    plans = {selected["plan_revision"]}
    plans.update(
        frontier["plan_revision"] for control in controls
        for frontier in (control["value"]["from"], control["value"]["to"])
    )
    result: list[dict[str, Any]] = []
    extension_origins = child_extension_authorizations_from_comments(
        comments, workstream_id=workstream_id,
        description_plan_revision=description_plan_revision,
        authenticated_route=authenticated_route,
    )
    repair_origins = legacy_child_origin_repairs_from_comments(
        comments, workstream_id=workstream_id,
        description_plan_revision=description_plan_revision,
        authenticated_route=authenticated_route,
    )
    for plan in plans:
        state = reduce_projection_comments(
            comments, workstream_id=workstream_id,
            expected_plan_revision=plan, authenticated_route=authenticated_route,
        )
        events = list(state.events)
        if plan != selected["plan_revision"]:
            retirements = [
                control for control in controls
                if control["kind"] == "generation_transition"
                and control["value"]["from"]["plan_revision"] == plan
            ]
            if len(retirements) != 1:
                continue
            events = events[:retirements[0]["value"]["from"]["projection_revision"]]
        for index, event in enumerate(events):
            if event["kind"] != "child_mutation_authorization":
                continue
            value = event["value"]
            proof = value["generation_authority"]
            if proof["plan_revision"] != plan or proof["source"] != value["source"]:
                raise LinearProjectionError(
                    "child_mutation_generation_proof_invalid"
                )
            if plan == selected["plan_revision"]:
                expected_proof = {
                    **selected, "workstream_id": workstream_id,
                    "authority": authenticated_route,
                    "source": state.snapshot.get("source"),
                }
                if proof != expected_proof:
                    raise LinearProjectionError(
                        "child_mutation_generation_proof_invalid"
                    )
            elif proof["transition_tip_event_id"] is None:
                if (
                    proof["authority_origin"] != "legacy_description"
                    or proof["description_plan_revision"] != plan
                ):
                    raise LinearProjectionError(
                        "child_mutation_generation_proof_invalid"
                    )
            else:
                tips = [
                    control for control in controls
                    if control["event_id"] == proof["transition_tip_event_id"]
                ]
                if (
                    len(tips) != 1
                    or tips[0]["value"]["to"]["plan_revision"] != plan
                    or tips[0]["kind"] != proof["authority_origin"]
                    or tips[0]["value"]["activation_epoch"]
                    != proof["activation_epoch"]
                    or tips[0]["value"].get("source") != proof["source"]
                ):
                    raise LinearProjectionError(
                        "child_mutation_generation_proof_invalid"
                    )
            scopes = [
                item for item in events[:index]
                if item["kind"] == "scope" and item["key"] == "root"
            ]
            scope = scopes[-1] if scopes else None
            if (
                scope is None or scope["event_id"] != value["scope_event_id"]
                or hashlib.sha256(_canonical(scope["value"])).hexdigest()
                != value["scope_value_sha256"]
                or scope["value"].get("child_ownership", {}).get(
                    value["child_workstream_id"]
                ) != value["repository_owner"]
            ):
                raise LinearProjectionError("child_mutation_scope_proof_invalid")
            validate_child_origin_value(
                value, extension_origins=[
                    origin for origin in extension_origins
                    if origin["plan_revision"] != plan
                    or origin in events[:index]
                ],
                repair_origins=[
                    origin for origin in repair_origins
                    if origin["plan_revision"] != plan
                    or origin in events[:index]
                ],
                authenticated_route=authenticated_route,
            )
            result.append(event)
    return result


def _reduce_projection_comments_impl(
    comments: list[dict[str, Any]], *, workstream_id: str,
    expected_plan_revision: str,
    authenticated_route: dict[str, str] | None = None,
    authenticated_source: dict[str, Any] | None = None,
    permit_unsealed_legacy_candidates: bool = False,
) -> ReducedProjection:
    observed: dict[str, tuple[dict[str, Any], str, bytes, dict[str, Any]]] = {}
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str):
            raise LinearProjectionError("malformed_projection_marker")
        if PROJECTION_PREFIX not in body:
            continue
        matches = PROJECTION_RE.findall(body)
        if len(matches) != 1 or body.count(PROJECTION_PREFIX) != 1:
            raise LinearProjectionError("malformed_projection_marker")
        event = _decode_projection(matches[0])
        if event["workstream_id"] != workstream_id:
            raise LinearProjectionError("workstream_id_mismatch")
        signature = _canonical(event)
        previous = observed.get(event["event_id"])
        if previous:
            reason = "duplicate_projection_event_id" if previous[2] == signature else "conflicting_projection_event_id"
            raise LinearProjectionError(f"{reason}:{event['event_id']}")
        remote_id = comment.get("id")
        if not isinstance(remote_id, str) or not remote_id:
            raise LinearProjectionError("projection_comment_missing_remote_id")
        observed[event["event_id"]] = (event, remote_id, signature, comment)

    history = sorted(
        (item[0] for item in observed.values()),
        key=lambda item: (
            item["plan_revision"], item["expected_revision"],
            item["created_at"], item["event_id"],
        ),
    )
    generation = [
        event for event in history if event["plan_revision"] == expected_plan_revision
    ]
    stale_events = [
        event for event in history if event["plan_revision"] != expected_plan_revision
    ]
    legacy = sorted(
        (event for event in generation if event["schema_version"] == 1),
        key=lambda item: (item["expected_revision"], item["created_at"], item["event_id"]),
    )
    modern = sorted(
        (event for event in generation if event["schema_version"] == 2),
        key=lambda item: (item["expected_revision"], item["created_at"], item["event_id"]),
    )
    quarantined: list[dict[str, Any]] = []
    accepted_legacy = legacy
    if modern:
        activation = modern[0]
        if activation["expected_revision"] == 0:
            if activation["kind"] == "cas_activation":
                raise LinearProjectionError(
                    "projection_v2_activation_without_legacy"
                )
            accepted_legacy = []
            quarantined = legacy
        else:
            if activation["kind"] != "cas_activation":
                raise LinearProjectionError("projection_v2_activation_required")
            reviewed_ids = activation["value"]["legacy_event_ids"]
            by_id = {event["event_id"]: event for event in legacy}
            if len(reviewed_ids) != activation["expected_revision"] or any(
                event_id not in by_id for event_id in reviewed_ids
            ):
                raise LinearProjectionError("projection_v2_activation_legacy_mismatch")
            accepted_legacy = sorted(
                (by_id[event_id] for event_id in reviewed_ids),
                key=lambda item: (
                    item["expected_revision"], item["created_at"], item["event_id"],
                ),
            )
            if (
                "legacy_digest_kind" in activation["value"]
                and reviewed_ids
                != [event["event_id"] for event in accepted_legacy]
            ):
                raise LinearProjectionError(
                    "projection_v2_activation_legacy_order_mismatch"
                )
            reviewed = set(reviewed_ids)
            quarantined = [event for event in legacy if event["event_id"] not in reviewed]
            if not _activation_legacy_digest_is_valid(
                activation["value"], accepted_legacy,
            ):
                raise LinearProjectionError("projection_v2_activation_legacy_digest_mismatch")
    for index, event in enumerate(accepted_legacy):
        if event["expected_revision"] > index:
            raise LinearProjectionError(
                f"projection_revision_ahead:{event['event_id']}:{event['expected_revision']}:{index}"
            )
    for offset, event in enumerate(modern):
        index = len(accepted_legacy) + offset
        if event["expected_revision"] != index:
            raise LinearProjectionError(
                f"projection_revision_mismatch:{event['event_id']}:{event['expected_revision']}:{index}"
            )
        authority = event["authority"]
        if authenticated_route is not None:
            for field in AUTHORITY_FIELDS:
                if authority[field] != authenticated_route.get(field):
                    raise LinearProjectionError(f"projection_route_mismatch:{field}")
        if observed[event["event_id"]][1] != projection_slot_id(
            workstream_id, event["plan_revision"], index, authority,
        ):
            raise LinearProjectionError(f"projection_slot_identity_mismatch:{event['event_id']}")
    events = [*accepted_legacy, *modern]
    comments_by_event_id = {
        event_id: item[3] for event_id, item in observed.items()
    }
    identity_history_seals = _validated_identity_history_seals(
        events, comments_by_event_id,
    )
    identity_history_candidates: list[dict[str, Any]] = []
    active: dict[tuple[str, str], dict[str, Any]] = {}
    heads: dict[tuple[str, str], dict[str, Any]] = {}
    for index, event in enumerate(events):
        identity = (event["kind"], event["key"])
        current = heads.get(identity)
        supersedes = event["supersedes_event_id"]
        if current is None and supersedes is not None:
            raise LinearProjectionError(f"projection_supersedes_missing:{event['event_id']}")
        if current is not None and supersedes != current["event_id"]:
            raise LinearProjectionError(f"projection_concurrent_conflict:{event['kind']}:{event['key']}")
        if identity == ("scope", "root") and current is not None and event["value"] != TOMBSTONE:
            from workstream_scope import (
                ScopeError,
                validate_authenticated_legacy_repository_identity_transition,
                validate_repository_identity_transition,
            )

            try:
                validate_repository_identity_transition(current["value"], event["value"])
            except ScopeError as error:
                sealed = event["event_id"] in identity_history_seals
                if not sealed and not permit_unsealed_legacy_candidates:
                    raise LinearProjectionError(str(error)) from error
                try:
                    validate_authenticated_legacy_repository_identity_transition(
                        current["value"], event["value"],
                    )
                except ScopeError as legacy_error:
                    raise LinearProjectionError(str(legacy_error)) from legacy_error
                if not sealed:
                    identity_history_candidates.append({
                        "predecessor_scope_event_id": current["event_id"],
                        "predecessor_scope_value_sha256": hashlib.sha256(
                            _canonical(current["value"])
                        ).hexdigest(),
                        "sealed_scope_event_id": event["event_id"],
                        "sealed_scope_value_sha256": hashlib.sha256(
                            _canonical(event["value"])
                        ).hexdigest(),
                        "legacy_projection_prefix_sha256": projection_prefix_sha256(
                            events, comments_by_event_id, event["event_id"],
                        ),
                    })
        heads[identity] = event
        if event["value"] == TOMBSTONE:
            active.pop(identity, None)
        else:
            active[identity] = event

    by_kind: dict[str, list[dict[str, Any]]] = {kind: [] for kind in KINDS}
    for (kind, _key), event in active.items():
        by_kind[kind].append(deepcopy(event["value"]))
    for kind in by_kind:
        by_kind[kind].sort(key=lambda value: _canonical(value))
    for kind in SINGLETON_KINDS:
        if len(by_kind[kind]) > 1:
            raise LinearProjectionError(f"multiple_projection_singletons:{kind}")
    source = by_kind["source"][0] if by_kind["source"] else None
    if source is not None and source.get("sha256") != expected_plan_revision:
        raise LinearProjectionError("projection_source_plan_mismatch")
    if authenticated_source is not None and source is not None:
        source_identity = source.get("identity") or source.get("url")
        if source_identity != authenticated_source.get("identity"):
            raise LinearProjectionError("projection_source_identity_mismatch")
        if source.get("sha256") != authenticated_source.get("sha256"):
            raise LinearProjectionError("projection_source_bytes_mismatch")
    scope = by_kind["scope"][0] if by_kind["scope"] else None
    if authenticated_route is not None and scope is not None:
        linear = scope.get("linear") or {}
        for field in ("workspace_id", "team_id", "project_id", "root_issue_id"):
            if linear.get(field) != authenticated_route.get(field):
                raise LinearProjectionError(f"projection_route_mismatch:{field}")

    quarantine_disposition = (
        by_kind["quarantine_disposition"][0]
        if by_kind["quarantine_disposition"] else None
    )
    retired_quarantine_ids: set[str] = set()
    if quarantine_disposition is not None:
        retired_quarantine_ids = set(quarantine_disposition["event_ids"])
        quarantined_by_id = {event["event_id"]: event for event in quarantined}
        if not retired_quarantine_ids.issubset(quarantined_by_id):
            raise LinearProjectionError("quarantine_disposition_unknown_event")
        reviewed_events = [
            quarantined_by_id[event_id]
            for event_id in sorted(retired_quarantine_ids)
        ]
        if quarantine_disposition["events_sha256"] != hashlib.sha256(
            _canonical(reviewed_events)
        ).hexdigest():
            raise LinearProjectionError("quarantine_disposition_digest_mismatch")
    unresolved_quarantine = [
        event for event in quarantined
        if event["event_id"] not in retired_quarantine_ids
    ]

    snapshot = {
        "scope": scope,
        "relations": by_kind["relation"],
        "choice_events": by_kind["choice"],
        "evidence_contracts": by_kind["evidence_contract"],
        "child_closures": by_kind["child_closure"],
        "source": source,
        "provenance": by_kind["provenance"],
        "closure_reviews": by_kind["closure_review"],
        "disposition": by_kind["disposition"][0] if by_kind["disposition"] else None,
        "lifecycle": by_kind["lifecycle"][0] if by_kind["lifecycle"] else None,
        "quarantine_disposition": quarantine_disposition,
        "projection_events": [deepcopy(event) for event in events],
        "projection_history": [deepcopy(event) for event in stale_events],
        "projection_quarantined": [deepcopy(event) for event in quarantined],
        "projection_unresolved_quarantine": [
            deepcopy(event) for event in unresolved_quarantine
        ],
        "projection_revision": len(events),
        "projection_recovery": {
            "state": (
                "current" if any(by_kind.values())
                else "stale_plan" if stale_events
                else "not_found"
            ),
            "stale_plan_count": len(stale_events),
        },
    }
    if identity_history_candidates:
        final_scope = heads.get(("scope", "root"))
        frontier = events[-1]
        if final_scope is None:
            raise LinearProjectionError("identity_history_candidate_scope_missing")
        snapshot["identity_history_candidates"] = [{
            "legacy_transitions": [{
                "predecessor_scope_event_id": item["predecessor_scope_event_id"],
                "predecessor_scope_value_sha256": item[
                    "predecessor_scope_value_sha256"
                ],
                "transition_scope_event_id": item["sealed_scope_event_id"],
                "transition_scope_value_sha256": item["sealed_scope_value_sha256"],
            } for item in identity_history_candidates],
            "sealed_scope_event_id": final_scope["event_id"],
            "sealed_scope_value_sha256": hashlib.sha256(
                _canonical(final_scope["value"])
            ).hexdigest(),
            "sealed_projection_frontier_event_id": frontier["event_id"],
            "sealed_projection_frontier_event_sha256": hashlib.sha256(
                _canonical(frontier)
            ).hexdigest(),
            "legacy_projection_prefix_sha256": projection_prefix_sha256(
                events, comments_by_event_id, frontier["event_id"],
            ),
        }]
    return ReducedProjection(
        workstream_id=workstream_id, revision=len(events), events=tuple(events),
        remote_ids={event_id: item[1] for event_id, item in observed.items()},
        snapshot=snapshot,
    )


def reduce_projection_comments(
    comments: list[dict[str, Any]], *, workstream_id: str,
    expected_plan_revision: str,
    authenticated_route: dict[str, str] | None = None,
    authenticated_source: dict[str, Any] | None = None,
) -> ReducedProjection:
    return _reduce_projection_comments_impl(
        comments, workstream_id=workstream_id,
        expected_plan_revision=expected_plan_revision,
        authenticated_route=authenticated_route,
        authenticated_source=authenticated_source,
        permit_unsealed_legacy_candidates=False,
    )


def _generation_checkpoint_ids(
    comments: list[dict[str, Any]], *, workstream_id: str, plan_revision: str,
    authorized_activation_event_ids: frozenset[str] = frozenset(),
) -> list[str]:
    from workstream_linear_checkpoints import reduce_checkpoint_comments

    checkpoints = reduce_checkpoint_comments(
        comments, workstream_id=workstream_id,
    ).checkpoints
    result = {
        item["event_id"] for item in checkpoints
        if item["plan_revision"] == plan_revision
    }
    if authorized_activation_event_ids:
        for comment in comments:
            body = comment.get("body") or ""
            if not isinstance(body, str) or PROJECTION_PREFIX not in body:
                continue
            matches = PROJECTION_RE.findall(body)
            if len(matches) != 1 or body.count(PROJECTION_PREFIX) != 1:
                raise LinearProjectionError("malformed_projection_marker")
            event = _decode_projection(matches[0])
            if event["event_id"] not in authorized_activation_event_ids:
                continue
            checkpoint = event.get("value", {}).get("activation_checkpoint")
            if (
                event.get("kind") != "generation_transition"
                or checkpoint is None
                or checkpoint.get("plan_revision") != plan_revision
            ):
                continue
            result.add(checkpoint["event_id"])
    return sorted(result)


def _generation_frontier(
    state: ReducedProjection, comments: list[dict[str, Any]], *,
    plan_revision: str, projection_revision: int | None = None,
    material_revision: int,
    authorized_activation_event_ids: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    revision = state.revision if projection_revision is None else projection_revision
    if revision <= 0 or revision > state.revision:
        raise LinearProjectionError("generation_transition_frontier_incomplete")
    events = list(state.events[:revision])
    heads: dict[tuple[str, str], dict[str, Any]] = {}
    for event in events:
        heads[(event["kind"], event["key"])] = event
    source_event = heads.get(("source", "root"))
    scope_event = heads.get(("scope", "root"))
    disposition_event = heads.get(("disposition", "root"))
    active_provenance = [
        event for (kind, _key), event in heads.items()
        if kind == "provenance" and event["value"] != TOMBSTONE
    ]
    if (
        source_event is None or source_event["value"] == TOMBSTONE
        or scope_event is None or scope_event["value"] == TOMBSTONE
        or disposition_event is None or disposition_event["value"] == TOMBSTONE
        or not active_provenance
    ):
        raise LinearProjectionError("generation_transition_target_incomplete")
    source = source_event["value"]
    identity = source.get("identity") or source.get("url")
    if (
        not isinstance(identity, str) or not identity
        or source.get("sha256") != plan_revision
    ):
        raise LinearProjectionError("generation_transition_source_incomplete")
    checkpoint_event_ids = _generation_checkpoint_ids(
        comments, workstream_id=state.workstream_id,
        plan_revision=plan_revision,
        authorized_activation_event_ids=authorized_activation_event_ids,
    )
    return {
        "plan_revision": plan_revision,
        "source_event_id": source_event["event_id"],
        "source_identity": identity,
        "source_sha256": source["sha256"],
        "material_revision": material_revision,
        "checkpoint_event_ids": checkpoint_event_ids,
        "checkpoint_events_sha256": hashlib.sha256(
            _canonical(checkpoint_event_ids)
        ).hexdigest(),
        "projection_revision": revision,
        "projection_frontier_event_id": events[-1]["event_id"],
        "projection_events_sha256": hashlib.sha256(_canonical(events)).hexdigest(),
    }


def _assert_generation_frontier(
    expected: dict[str, Any], state: ReducedProjection,
    comments: list[dict[str, Any]], *, material_revision: int,
    exact_checkpoints: bool,
    authorized_activation_event_ids: frozenset[str] = frozenset(),
) -> None:
    if expected["material_revision"] > material_revision:
        raise LinearProjectionError("generation_transition_frontier_mismatch")
    observed = _generation_frontier(
        state, comments, plan_revision=expected["plan_revision"],
        projection_revision=expected["projection_revision"],
        material_revision=expected["material_revision"],
        authorized_activation_event_ids=authorized_activation_event_ids,
    )
    observed_checkpoints = observed.pop("checkpoint_event_ids")
    observed.pop("checkpoint_events_sha256")
    expected_without_checkpoints = dict(expected)
    expected_checkpoints = expected_without_checkpoints.pop("checkpoint_event_ids")
    expected_without_checkpoints.pop("checkpoint_events_sha256")
    checkpoints_match = (
        observed_checkpoints == expected_checkpoints
        if exact_checkpoints
        else set(expected_checkpoints).issubset(observed_checkpoints)
    )
    if observed != expected_without_checkpoints or not checkpoints_match:
        raise LinearProjectionError("generation_transition_frontier_mismatch")


def select_plan_generation(
    comments: list[dict[str, Any]], *, workstream_id: str,
    description_plan_revision: str | None,
    authenticated_route: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Select the append-only authority tip, preserving description-only roots."""
    material_revision = reduce_event_comments(
        comments, workstream_id=workstream_id,
    ).revision
    encoded_events: list[dict[str, Any]] = []
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str):
            raise LinearProjectionError("malformed_projection_marker")
        if PROJECTION_PREFIX not in body:
            continue
        matches = PROJECTION_RE.findall(body)
        if len(matches) != 1 or body.count(PROJECTION_PREFIX) != 1:
            raise LinearProjectionError("malformed_projection_marker")
        event = _decode_projection(matches[0])
        if event["workstream_id"] != workstream_id:
            raise LinearProjectionError("workstream_id_mismatch")
        encoded_events.append(event)
    transitions = [
        event for event in encoded_events
        if event["kind"] == "generation_transition"
    ]
    geneses = [
        event for event in encoded_events if event["kind"] == "generation_genesis"
    ]
    controls = [*geneses, *transitions]
    if not controls:
        if not isinstance(description_plan_revision, str) or not description_plan_revision:
            raise LinearProjectionError("generation_description_plan_missing_bootstrap_required")
        return {
            "plan_revision": description_plan_revision,
            "description_plan_revision": description_plan_revision,
            "transition_tip_event_id": None,
            "activation_epoch": None,
            "authority_origin": "legacy_description",
        }

    plan_revisions = {
        frontier["plan_revision"]
        for event in controls
        for frontier in (event["value"]["from"], event["value"]["to"])
    }
    states = {
        revision: reduce_projection_comments(
            comments, workstream_id=workstream_id,
            expected_plan_revision=revision,
            authenticated_route=authenticated_route,
        )
        for revision in plan_revisions
    }
    by_id = {event["event_id"]: event for event in controls}
    if len(by_id) != len(controls):
        raise LinearProjectionError("generation_control_duplicate")
    roots = [
        event for event in controls
        if event["value"]["previous_control_event_id"] is None
    ]
    if len(roots) != 1:
        raise LinearProjectionError("generation_control_root_ambiguous")
    if geneses and (len(geneses) != 1 or roots[0]["kind"] != "generation_genesis"):
        raise LinearProjectionError("generation_genesis_ambiguous")
    children: dict[str, list[dict[str, Any]]] = {}
    for event in controls:
        previous = event["value"]["previous_control_event_id"]
        if previous is None:
            continue
        if previous not in by_id:
            raise LinearProjectionError("generation_control_orphan")
        children.setdefault(previous, []).append(event)
    if any(len(items) != 1 for items in children.values()):
        raise LinearProjectionError("generation_control_fork")

    ordered: list[dict[str, Any]] = []
    current = roots[0]
    seen: set[str] = set()
    while True:
        if current["event_id"] in seen:
            raise LinearProjectionError("generation_control_cycle")
        seen.add(current["event_id"])
        ordered.append(current)
        successors = children.get(current["event_id"], [])
        if not successors:
            break
        current = successors[0]
    if len(seen) != len(controls):
        raise LinearProjectionError("generation_control_orphan_or_cycle")

    previous = None
    authorized_activation_event_ids: set[str] = set()
    for event in ordered:
        value = event["value"]
        if previous is not None and (
            value["from"]["plan_revision"]
            != previous["value"]["to"]["plan_revision"]
            or value["activation_epoch"]
            != previous["value"]["activation_epoch"] + 1
        ):
            raise LinearProjectionError("generation_transition_chain_discontinuous")
        from_state = states[value["from"]["plan_revision"]]
        provenance_heads: dict[str, dict[str, Any]] = {}
        for predecessor_event in from_state.events[
            :value["from"]["projection_revision"]
        ]:
            if predecessor_event["kind"] == "provenance":
                provenance_heads[predecessor_event["key"]] = predecessor_event
        expected_provenance_ids = sorted(
            item["event_id"] for item in provenance_heads.values()
            if item["value"] != TOMBSTONE
        )
        if (
            value["retirement"]["provenance_event_ids"]
            != expected_provenance_ids
            or value["retirement"]["checkpoint_event_ids"]
            != value["from"]["checkpoint_event_ids"]
        ):
            raise LinearProjectionError("generation_retirement_frontier_mismatch")
        _assert_generation_frontier(
            value["from"], from_state, comments,
            material_revision=material_revision,
            exact_checkpoints=(event["kind"] != "generation_genesis"),
            authorized_activation_event_ids=frozenset(
                authorized_activation_event_ids
            ),
        )
        if event["kind"] == "generation_genesis":
            position = value["from"]["projection_revision"]
            if (
                len(from_state.events) < position + 1
                or from_state.events[position] != event
            ):
                raise LinearProjectionError("generation_genesis_not_at_frontier")
            previous = event
            continue
        to_state = states[value["to"]["plan_revision"]]
        _assert_generation_frontier(
            value["to"], to_state, comments,
            material_revision=material_revision, exact_checkpoints=False,
            authorized_activation_event_ids=frozenset({
                *authorized_activation_event_ids, event["event_id"],
            }),
        )
        position = value["from"]["projection_revision"]
        if (
            len(from_state.events) != position + 1
            or from_state.events[position] != event
        ):
            raise LinearProjectionError("generation_transition_not_last")
        seal_id = value["candidate_seal_event_id"]
        seal = next(
            (item for item in to_state.events if item["event_id"] == seal_id), None,
        )
        if (
            seal is None or seal["kind"] != "generation_candidate_seal"
            or seal["value"]["reservation_id"] != value["reservation_id"]
            or seal["value"]["reservation_sha256"] != value["reservation_sha256"]
            or seal["value"]["previous_control_event_id"]
            != value["previous_control_event_id"]
            or seal["value"]["activation_epoch"] != value["activation_epoch"]
            or value["candidate_seal_sha256"]
            != hashlib.sha256(_canonical(seal)).hexdigest()
            or to_state.events[value["to"]["projection_revision"] - 1] != seal
        ):
            raise LinearProjectionError("generation_candidate_seal_mismatch")
        # A retired predecessor may not acquire new checkpoints after activation.
        if _generation_checkpoint_ids(
            comments, workstream_id=workstream_id,
            plan_revision=value["from"]["plan_revision"],
            authorized_activation_event_ids=frozenset({
                *authorized_activation_event_ids, event["event_id"],
            }),
        ) != value["from"]["checkpoint_event_ids"]:
            raise LinearProjectionError("generation_transition_predecessor_checkpoint_changed")
        authorized_activation_event_ids.add(event["event_id"])
        previous = event

    tip = ordered[-1]
    return {
        "plan_revision": tip["value"]["to"]["plan_revision"],
        "description_plan_revision": description_plan_revision,
        "transition_tip_event_id": tip["event_id"],
        "activation_epoch": tip["value"]["activation_epoch"],
        "authority_origin": tip["kind"],
    }


def _inspect_unsealed_identity_history(
    comments: list[dict[str, Any]], *, workstream_id: str,
    expected_plan_revision: str, authenticated_route: dict[str, str],
    authenticated_source: dict[str, Any] | None, material_revision: int,
) -> dict[str, Any]:
    """Return bounded migration metadata without exposing an executable snapshot."""
    reduced = _reduce_projection_comments_impl(
        comments, workstream_id=workstream_id,
        expected_plan_revision=expected_plan_revision,
        authenticated_route=authenticated_route,
        authenticated_source=authenticated_source,
        permit_unsealed_legacy_candidates=True,
    )
    candidates = reduced.snapshot.get("identity_history_candidates") or []
    if len(candidates) != 1:
        raise LinearProjectionError("identity_history_candidate_ambiguous")
    source = reduced.snapshot.get("source") or {}
    return {
        "schema_version": 1,
        "disposition": "partial_reconcile_required",
        "resume_authority": "none",
        "workstream_id": workstream_id,
        "authority": dict(authenticated_route),
        "source": {"identity": source.get("identity"), "sha256": source.get("sha256")},
        "plan_revision": expected_plan_revision,
        "material_revision": material_revision,
        "projection_revision": reduced.revision,
        "candidate": candidates[0],
        "remediation": "run repository-identity-seal preview, review, then apply",
    }


def inspect_unsealed_identity_history(
    comments: list[dict[str, Any]], *, workstream_id: str,
    expected_plan_revision: str, authenticated_route: dict[str, str],
    authenticated_source: dict[str, Any], material_revision: int,
) -> dict[str, Any]:
    if not isinstance(authenticated_source, dict):
        raise LinearProjectionError("identity_history_authenticated_source_required")
    return _inspect_unsealed_identity_history(
        comments, workstream_id=workstream_id,
        expected_plan_revision=expected_plan_revision,
        authenticated_route=authenticated_route,
        authenticated_source=authenticated_source,
        material_revision=material_revision,
    )


class LinearProjectionAdapter:
    supports_atomic_cas = False
    supports_append_only_events = True

    def __init__(
        self, client: GraphQLClient, *, issue_id: str, workstream_id: str,
        plan_revision: str, workspace_id: str | None = None,
        team_id: str | None = None, project_id: str | None = None,
        root_issue_id: str | None = None,
    ):
        self.client = client
        self.issue_id = issue_id
        self.workstream_id = workstream_id.upper()
        self.plan_revision = plan_revision
        self.workspace_id = workspace_id
        self.team_id = team_id
        self.project_id = project_id
        self.root_issue_id = root_issue_id
        self._comment_id_capability_verified = False
        if any((workspace_id, team_id, project_id, root_issue_id)) and not all(
            (workspace_id, team_id, project_id, root_issue_id)
        ):
            raise ValueError("Linear workspace, team, project, and root issue IDs must be supplied together")

    @property
    def authority(self) -> dict[str, str]:
        authority = {
            "workspace_id": self.workspace_id,
            "team_id": self.team_id,
            "project_id": self.project_id,
            "root_issue_id": self.root_issue_id,
        }
        validate_projection_authority(authority)
        return authority  # type: ignore[return-value]

    def _assert_comment_id_capability(self) -> None:
        if self._comment_id_capability_verified:
            return
        result = self.client.execute(COMMENT_CREATE_CAPABILITY_QUERY, {})
        fields = ((result.get("__type") or {}).get("inputFields") or [])
        if not isinstance(fields, list) or "id" not in {
            field.get("name") for field in fields if isinstance(field, dict)
        }:
            raise LinearProjectionError("linear_comment_create_id_capability_unavailable")
        self._comment_id_capability_verified = True

    def activate_v2(
        self, *, created_at: str, expected_revision: int | None = None,
        expected_legacy_event_ids: list[str] | None = None,
        expected_legacy_events_sha256: str | None = None,
    ) -> dict[str, Any] | None:
        """Fence reviewed v1 history before accepting any v2 CAS writes."""
        before = self.state()
        if any(event["schema_version"] == 2 for event in before.events) or not before.events:
            return None
        legacy_ids = [event["event_id"] for event in before.events]
        legacy_events_sha256 = hashlib.sha256(
            _canonical(list(before.events))
        ).hexdigest()
        if (
            expected_revision is not None and before.revision != expected_revision
        ) or (
            expected_legacy_event_ids is not None
            and legacy_ids != expected_legacy_event_ids
        ) or (
            expected_legacy_events_sha256 is not None
            and legacy_events_sha256 != expected_legacy_events_sha256
        ):
            raise LinearProjectionError("projection_v2_activation_stale_reload_required")
        event = build_projection_event(
            workstream_id=self.workstream_id, kind="cas_activation", key="root",
            value={
                "legacy_digest_kind": LEGACY_DIGEST_KIND_FULL_EVENTS,
                "legacy_event_ids": legacy_ids,
                "legacy_events_sha256": legacy_events_sha256,
            },
            plan_revision=self.plan_revision, expected_revision=before.revision,
            created_at=created_at, authority=self.authority,
        )
        return self.append(event)

    def activate_generation(
        self, *, target_plan_revision: str, created_at: str,
        predecessor_sessions_retired: bool,
    ) -> dict[str, Any]:
        """Deprecated unsafe entrypoint retained only for a clear refusal."""
        raise LinearProjectionError(
            "generation_cli_required:use_workstream_generation_activate_with_"
            "strict_resume_and_durable_retirement_proof"
        )

    @classmethod
    def from_env(
        cls, *, issue_id: str, workstream_id: str, plan_revision: str,
        env: dict[str, str] | None = None, config_path: str | None = None,
    ) -> "LinearProjectionAdapter":
        from workstream_config import load_linear_api_key, resolve_linear_route

        values = os.environ if env is None else env
        token = load_linear_api_key(env=values)
        if not token:
            raise LinearProjectionError("linear_auth_unavailable")
        client = HttpGraphQLClient(token)
        route, _resolved = resolve_linear_route(config_path=config_path, env=values)
        if not route:
            route = bootstrap_linear_route(client, workstream_id)
        return cls(
            client, issue_id=issue_id, workstream_id=workstream_id,
            plan_revision=plan_revision, workspace_id=route.get("workspace_id"),
            team_id=route.get("team_id"), project_id=route.get("project_id"),
            root_issue_id=route.get("root_issue_id"),
        )

    def _comments(self) -> list[dict[str, Any]]:
        comments: list[dict[str, Any]] = []
        after: str | None = None
        seen: set[str] = set()
        while True:
            result = self.client.execute(COMMENTS_QUERY, {"issueId": self.issue_id, "after": after})
            issue = result.get("issue")
            if not issue or issue.get("identifier") != self.workstream_id:
                raise LinearProjectionError("Linear workstream issue not found or mismatched")
            if self.root_issue_id and issue.get("id") != self.root_issue_id:
                raise LinearProjectionError("projection_route_mismatch:root_issue_id")
            validate_issue_route(
                issue, workspace_id=self.workspace_id, team_id=self.team_id,
                project_id=self.project_id,
            )
            connection = issue.get("comments") or {}
            nodes = connection.get("nodes")
            page_info = connection.get("pageInfo")
            if not isinstance(nodes, list) or not isinstance(page_info, dict):
                raise LinearProjectionError("invalid Linear comment connection")
            comments.extend(nodes)
            if not page_info.get("hasNextPage"):
                return comments
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after or after in seen:
                raise LinearProjectionError("invalid Linear comment pagination cursor")
            seen.add(after)

    def state(self) -> ReducedProjection:
        return reduce_projection_comments(
            self._comments(), workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route={
                "workspace_id": self.workspace_id,
                "team_id": self.team_id,
                "project_id": self.project_id,
                "root_issue_id": self.root_issue_id,
            } if all((self.workspace_id, self.team_id, self.project_id, self.root_issue_id)) else None,
        )

    def select_child_extension_generation(
        self, *, description_plan_revision: str | None,
        source: dict[str, str], reviewed_candidate_key: str | None = None,
        child_issue_id: str | None = None,
        native_initialization: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Authenticate the one generation allowed to extend this root."""
        comments = self._comments()
        try:
            return self._select_child_extension_generation(
                comments, description_plan_revision=description_plan_revision,
                source=source,
            )
        except LinearProjectionError as current_error:
            if not all((reviewed_candidate_key, child_issue_id)) or not isinstance(
                native_initialization, dict
            ):
                raise
            state = reduce_projection_comments(
                comments, workstream_id=self.workstream_id,
                expected_plan_revision=self.plan_revision,
                authenticated_route=self.authority,
            )
            matching = [
                event for event in state.events
                if event["kind"] == "child_extension_authorization"
                and event["key"] == child_issue_id
            ]
            if len(matching) != 1:
                raise current_error
            event = matching[0]
            value = event.get("value")
            if (
                not isinstance(value, dict)
                or value.get("source") != source
                or value.get("reviewed_candidate_key") != reviewed_candidate_key
                or value.get("child_issue_id") != child_issue_id
                or value.get("native_initialization") != native_initialization
                or not isinstance(value.get("generation_authority"), dict)
            ):
                raise current_error
            self._assert_child_extension_generation_authority(
                event, comments, value["generation_authority"],
            )
            return deepcopy(value["generation_authority"])

    def select_generation_authority(
        self, *, description_plan_revision: str | None,
    ) -> dict[str, Any]:
        """Return only the currently selected route-authenticated generation."""
        selected = select_plan_generation(
            self._comments(), workstream_id=self.workstream_id,
            description_plan_revision=description_plan_revision,
            authenticated_route=self.authority,
        )
        if selected["plan_revision"] != self.plan_revision:
            raise LinearProjectionError("plan_generation_not_selected")
        return selected

    def select_owned_child_generation(
        self, *, description_plan_revision: str | None,
        child_workstream_id: str, child_issue_id: str,
        proposal_id: str | None = None,
    ) -> dict[str, Any]:
        """Authenticate a current generation and its exact child owner."""
        comments = self._comments()
        selected = select_plan_generation(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=description_plan_revision,
            authenticated_route=self.authority,
        )
        state = reduce_projection_comments(
            comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        from workstream_linear import deterministic_existing_root_child_id
        origins = [
            event for event in child_extension_authorizations_from_comments(
                comments, workstream_id=self.workstream_id,
                description_plan_revision=description_plan_revision,
                authenticated_route=self.authority,
            )
            if event["value"].get("child_issue_id") == child_issue_id
        ]
        repairs = [
            event for event in legacy_child_origin_repairs_from_comments(
                comments, workstream_id=self.workstream_id,
                description_plan_revision=description_plan_revision,
                authenticated_route=self.authority,
            )
            if event["value"].get("child_issue_id") == child_issue_id
        ]
        if len(origins) + len(repairs) > 1:
            raise LinearProjectionError("child_origin_authorization_ambiguous")
        child_origin = None
        if origins:
            origin = origins[0]
            origin_value = origin["value"]
            if (
                deterministic_existing_root_child_id(
                    workspace_id=self.workspace_id, team_id=self.team_id,
                    project_id=self.project_id, root_issue_id=self.root_issue_id,
                    child_stable_key=origin_value["reviewed_candidate_key"],
                ) != child_issue_id
                or origin["authority"] != self.authority
                or origin_value.get("root_issue_id") != self.root_issue_id
                or origin_value.get("route") != self.authority
            ):
                raise LinearProjectionError("child_origin_authorization_invalid")
            child_origin = {
                "kind": "child_extension_authorization",
                "event_id": origin["event_id"],
                "value_sha256": hashlib.sha256(
                    _canonical(origin_value)
                ).hexdigest(),
                "candidate_key": origin_value["reviewed_candidate_key"],
            }
        elif repairs:
            repair = repairs[0]
            repair_value = repair["value"]
            if (
                repair["authority"] != self.authority
                or repair_value.get("root_issue_id") != self.root_issue_id
                or repair_value.get("route") != self.authority
                or repair_value.get("child_workstream_id")
                != child_workstream_id
                or repair_value.get("child_parent_issue_id")
                != self.root_issue_id
            ):
                raise LinearProjectionError("child_origin_repair_invalid")
            child_origin = {
                "kind": "existing_child_origin_seal",
                "event_id": repair["event_id"],
                "value_sha256": hashlib.sha256(
                    _canonical(repair_value)
                ).hexdigest(),
                "child_workstream_id": child_workstream_id,
            }
        scope = state.snapshot.get("scope")
        owner = (
            scope.get("child_ownership", {}).get(child_workstream_id)
            if isinstance(scope, dict) else None
        )
        if (
            selected["plan_revision"] != self.plan_revision
            or not isinstance(owner, str) or not owner.strip()
        ):
            matching = [
                event for event in state.events
                if event["kind"] == "child_mutation_authorization"
                and event["key"] == proposal_id
                and event["value"].get("child_workstream_id") == child_workstream_id
            ]
            if len(matching) != 1:
                reason = (
                    "plan_generation_not_selected"
                    if selected["plan_revision"] != self.plan_revision
                    else f"child_target_not_owned:{child_workstream_id}"
                )
                raise LinearProjectionError(reason)
            event = matching[0]
            self._assert_child_mutation_authorization(event, comments)
            value = event["value"]
            result = {
                **deepcopy(value["generation_authority"]),
                "child_repository_owner": value["repository_owner"],
                "scope_event_id": value["scope_event_id"],
                "scope_value_sha256": value["scope_value_sha256"],
                "projection_revision": state.revision,
            }
            result["child_origin"] = deepcopy(
                child_origin or value.get("child_origin")
            )
            if not isinstance(result["child_origin"], dict):
                raise LinearProjectionError("child_origin_provenance_missing")
            return result
        scope_events = [
            event for event in state.events
            if event["kind"] == "scope" and event["key"] == "root"
        ]
        if not scope_events:
            raise LinearProjectionError("child_target_scope_missing")
        scope_event = scope_events[-1]
        source = state.snapshot.get("source")
        if not isinstance(source, dict):
            raise LinearProjectionError("child_target_source_missing")
        generation = self._select_child_extension_generation(
            comments, description_plan_revision=description_plan_revision,
            source=source,
        )
        if child_origin is None:
            raise LinearProjectionError("child_origin_provenance_missing")
        return {
            **generation, "child_repository_owner": owner,
            "scope_event_id": scope_event["event_id"],
            "scope_value_sha256": hashlib.sha256(
                _canonical(scope_event["value"])
            ).hexdigest(),
            "projection_revision": state.revision,
            "child_origin": child_origin,
        }

    def _select_child_extension_generation(
        self, comments: list[dict[str, Any]], *,
        description_plan_revision: str | None, source: dict[str, str],
    ) -> dict[str, Any]:
        selected = select_plan_generation(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=description_plan_revision,
            authenticated_route=self.authority,
        )
        if selected["plan_revision"] != self.plan_revision:
            raise LinearProjectionError(
                "child_extension_plan_generation_not_selected"
            )

        selected_source = deepcopy(source)
        tip_id = selected["transition_tip_event_id"]
        if tip_id is not None:
            controls: list[dict[str, Any]] = []
            for comment in comments:
                body = comment.get("body") or ""
                if not isinstance(body, str) or PROJECTION_PREFIX not in body:
                    continue
                matches = PROJECTION_RE.findall(body)
                if len(matches) != 1 or body.count(PROJECTION_PREFIX) != 1:
                    raise LinearProjectionError("malformed_projection_marker")
                event = _decode_projection(matches[0])
                if event["event_id"] == tip_id:
                    controls.append(event)
            if len(controls) != 1:
                raise LinearProjectionError(
                    "child_extension_generation_tip_ambiguous"
                )
            selected_source = controls[0].get("value", {}).get("source")
            if selected_source != source:
                raise LinearProjectionError(
                    "child_extension_generation_source_mismatch"
                )
        elif source.get("sha256") != description_plan_revision:
            raise LinearProjectionError(
                "child_extension_legacy_source_mismatch"
            )

        return {
            **selected,
            "workstream_id": self.workstream_id,
            "authority": deepcopy(self.authority),
            "source": deepcopy(selected_source),
        }

    def append(
        self, event: dict[str, Any], *,
        expected_quarantine_count: int | None = None,
        expected_quarantine_sha256: str | None = None,
        expected_material_revision: int | None = None,
        allowed_generation_reservation_id: str | None = None,
        allow_retired_generation_control: bool = False,
    ) -> dict[str, Any]:
        validate_projection_event(event)
        if (expected_quarantine_count is None) != (
            expected_quarantine_sha256 is None
        ):
            raise LinearProjectionError("projection_quarantine_fence_incomplete")
        if event["workstream_id"] != self.workstream_id or event["plan_revision"] != self.plan_revision:
            raise LinearProjectionError("projection_route_or_plan_mismatch")
        if expected_material_revision is not None and (
            not isinstance(expected_material_revision, int)
            or isinstance(expected_material_revision, bool)
            or expected_material_revision < 0
        ):
            raise LinearProjectionError("invalid_projection_material_frontier")
        if allow_retired_generation_control and event["kind"] not in {
            "generation_genesis", "generation_transition",
        }:
            raise LinearProjectionError("invalid_generation_control_bypass")
        if allowed_generation_reservation_id is not None and (
            event["kind"] not in {
                "generation_genesis", "generation_candidate_seal",
                "generation_transition",
            }
            or event["value"].get("reservation_id")
            != allowed_generation_reservation_id
        ):
            raise LinearProjectionError("invalid_generation_reservation_bypass")
        before_comments = self._comments()
        try:
            from workstream_linear_events import pending_ledger_reservations
            from workstream_generation import (
                assert_generation_write_authority,
                assert_no_pending_generation_reservation,
            )
            material_reservations = pending_ledger_reservations(
                before_comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
                current_plan_revision=self.plan_revision,
            )
            if material_reservations and not (
                len(material_reservations) == 1
                and material_reservations[0]["intent_event"] == event
            ):
                raise LinearProjectionError(
                    "projection_material_boundary_reserved:"
                    + material_reservations[0]["intent_event"]["event_id"]
                )
            assert_no_pending_generation_reservation(
                before_comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
                allowed_reservation_id=allowed_generation_reservation_id,
            )
            if not allow_retired_generation_control:
                assert_generation_write_authority(
                    before_comments, workstream_id=self.workstream_id,
                    plan_revision=self.plan_revision,
                    authenticated_route=self.authority,
                    allow_unactivated_candidate_projection=True,
                )
        except LinearTransportError as error:
            raise LinearProjectionError(str(error)) from error
        if expected_material_revision is not None:
            from workstream_linear_events import reduce_event_comments

            before = reduce_projection_comments(
                before_comments, workstream_id=self.workstream_id,
                expected_plan_revision=self.plan_revision,
                authenticated_route=self.authority if all((
                    self.workspace_id, self.team_id, self.project_id,
                    self.root_issue_id,
                )) else None,
            )
            material = reduce_event_comments(
                before_comments, workstream_id=self.workstream_id,
            )
            if material.revision != expected_material_revision:
                raise LinearProjectionError(
                    "projection_material_frontier_stale_reload_required"
                )
        else:
            # Keep the public state observation boundary used by reconcile
            # race fences. The separate complete read above belongs to the
            # generation guard and must not erase that observation point.
            before = self.state()
        if expected_quarantine_count is not None or expected_quarantine_sha256 is not None:
            quarantine = before.snapshot.get("projection_quarantined") or []
            if (
                len(quarantine) != expected_quarantine_count
                or hashlib.sha256(_canonical(quarantine)).hexdigest()
                != expected_quarantine_sha256
            ):
                raise LinearProjectionError(
                    "projection_quarantine_changed_reload_required"
                )
        existing_id = before.remote_ids.get(event["event_id"])
        if existing_id:
            existing = next(item for item in before.events if item["event_id"] == event["event_id"])
            if existing != event:
                raise LinearProjectionError(f"conflicting_projection_event_id:{event['event_id']}")
            return {"event_id": event["event_id"], "remote_id": existing_id, "revision": before.revision}
        if event["expected_revision"] != before.revision:
            raise LinearProjectionError("projection_slot_stale_reload_required")
        if event["schema_version"] != 2 or event.get("authority") != self.authority:
            raise LinearProjectionError("projection_append_authority_mismatch")
        if (
            any(item["schema_version"] == 1 for item in before.events)
            and not any(item["schema_version"] == 2 for item in before.events)
            and event["kind"] != "cas_activation"
        ):
            raise LinearProjectionError("projection_v2_activation_required")
        current = next(
            (
                item for item in reversed(before.events)
                if item["kind"] == event["kind"] and item["key"] == event["key"]
            ),
            None,
        )
        if current is None and event["supersedes_event_id"] is not None:
            raise LinearProjectionError("projection_supersedes_missing")
        if current is not None and event["supersedes_event_id"] != current["event_id"]:
            raise LinearProjectionError(
                f"projection_concurrent_conflict:{event['kind']}:{event['key']}"
            )
        if (
            event["kind"] == "scope" and event["key"] == "root"
            and current is not None and event["value"] != TOMBSTONE
        ):
            from workstream_scope import ScopeError, validate_repository_identity_transition

            try:
                validate_repository_identity_transition(current["value"], event["value"])
            except ScopeError as error:
                raise LinearProjectionError(str(error)) from error
        slot_id = projection_slot_id(
            self.workstream_id, self.plan_revision, event["expected_revision"],
            self.authority,
        )
        self._assert_comment_id_capability()
        try:
            response = self.client.execute(
                COMMENT_CREATE_MUTATION,
                {"input": {
                    "id": slot_id, "issueId": self.issue_id,
                    "body": encode_projection_comment(event),
                }},
            )
        except (LinearTransportError, OSError, TimeoutError):
            # A deterministic create-ID collision is the remote CAS loser path.
            # Reload before deciding whether this is identical replay or a
            # conflicting winner; an unavailable reload preserves the original
            # transport failure and never attempts another write.
            after_error = self.state()
            winner = next(
                (item for item in after_error.events
                 if after_error.remote_ids.get(item["event_id"]) == slot_id),
                None,
            )
            if winner == event:
                return {
                    "event_id": event["event_id"], "remote_id": slot_id,
                    "revision": after_error.revision,
                }
            if winner is not None:
                raise LinearProjectionError("projection_slot_lost_reload_required")
            raise
        created = response.get("commentCreate") or {}
        comment = created.get("comment")
        if (created.get("success") is not True or not comment
                or comment.get("id") != slot_id):
            raise LinearProjectionError("Linear comment creation returned no durable receipt")
        after = self.state()
        if expected_quarantine_count is not None or expected_quarantine_sha256 is not None:
            quarantine = after.snapshot.get("projection_quarantined") or []
            if (
                len(quarantine) != expected_quarantine_count
                or hashlib.sha256(_canonical(quarantine)).hexdigest()
                != expected_quarantine_sha256
            ):
                raise LinearProjectionError(
                    "projection_quarantine_changed_reload_required"
                )
        if after.remote_ids.get(event["event_id"]) != comment["id"]:
            raise LinearProjectionError("projection_append_not_observed")
        return {"event_id": event["event_id"], "remote_id": comment["id"], "revision": after.revision}

    @staticmethod
    def _remote_time(comment: dict[str, Any]) -> datetime:
        value = comment.get("createdAt")
        if not isinstance(value, str) or not value:
            raise LinearProjectionError("child_extension_remote_order_missing")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise LinearProjectionError("child_extension_remote_order_invalid") from error
        if parsed.tzinfo is None:
            raise LinearProjectionError("child_extension_remote_order_invalid")
        return parsed.astimezone(timezone.utc)

    def _assert_child_extension_authorization(
        self, event: dict[str, Any], comments: list[dict[str, Any]],
    ) -> dict[str, Any]:
        value = event.get("value")
        if (
            not isinstance(value, dict)
            or not isinstance(value.get("source"), dict)
            or not isinstance(value.get("generation_authority"), dict)
        ):
            raise LinearProjectionError(
                "child_extension_authorization_source_invalid"
            )
        self._assert_child_extension_generation_authority(
            event, comments, value["generation_authority"],
        )
        state = reduce_projection_comments(
            comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        matching = [
            item for item in state.events
            if item["kind"] == "child_extension_authorization"
            and item["key"] == event["key"]
        ]
        if not matching or matching[-1] != event:
            raise LinearProjectionError(
                "child_extension_authorization_superseded_or_conflicting"
            )
        remote_id = state.remote_ids.get(event["event_id"])
        comment = next(
            (item for item in comments if item.get("id") == remote_id), None
        )
        if not isinstance(comment, dict):
            raise LinearProjectionError("child_extension_authorization_readback_missing")
        return {"event": event, "remote_id": remote_id, "revision": state.revision}

    def _assert_child_extension_generation_authority(
        self, event: dict[str, Any], comments: list[dict[str, Any]],
        generation_authority: dict[str, Any],
    ) -> None:
        source = event["value"]["source"]
        selected = select_plan_generation(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=generation_authority.get(
                "description_plan_revision"
            ),
            authenticated_route=self.authority,
        )
        if selected["plan_revision"] == self.plan_revision:
            current = self._select_child_extension_generation(
                comments,
                description_plan_revision=generation_authority.get(
                    "description_plan_revision"
                ),
                source=source,
            )
            if current != generation_authority:
                raise LinearProjectionError(
                    "child_extension_generation_authority_changed"
                )
            return

        from workstream_generation import generation_controls

        controls = sorted(
            generation_controls(comments),
            key=lambda item: item["value"]["activation_epoch"],
        )
        proof_tip = generation_authority.get("transition_tip_event_id")
        if proof_tip is None:
            if (
                generation_authority.get("authority_origin")
                != "legacy_description"
                or generation_authority.get("description_plan_revision")
                != self.plan_revision
            ):
                raise LinearProjectionError(
                    "child_extension_generation_authority_invalid"
                )
        else:
            proof_controls = [
                item for item in controls if item["event_id"] == proof_tip
            ]
            if (
                len(proof_controls) != 1
                or proof_controls[0]["value"]["to"]["plan_revision"]
                != self.plan_revision
                or proof_controls[0]["value"]["activation_epoch"]
                != generation_authority.get("activation_epoch")
                or proof_controls[0]["kind"]
                != generation_authority.get("authority_origin")
                or proof_controls[0]["value"].get("source") != source
            ):
                raise LinearProjectionError(
                    "child_extension_generation_authority_invalid"
                )
        retirements = [
            item for item in controls
            if item["kind"] == "generation_transition"
            and item["value"]["from"]["plan_revision"] == self.plan_revision
        ]
        if len(retirements) != 1:
            raise LinearProjectionError(
                "child_extension_generation_retirement_ambiguous"
            )
        retirement = retirements[0]
        state = reduce_projection_comments(
            comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        prefix = state.events[
            :retirement["value"]["from"]["projection_revision"]
        ]
        if event not in prefix:
            raise LinearProjectionError(
                "child_extension_grant_not_in_retirement_frontier"
            )

    def reserve_child_extension(
        self, *, source: dict[str, str], reviewed_candidate_key: str,
        child_issue_id: str, expected_material_revision: int,
        expected_projection_revision: int,
        native_initialization: dict[str, Any],
        generation_authority: dict[str, Any], native_validation_sha256: str,
        require_existing: bool = False,
    ) -> dict[str, Any]:
        """Win one durable projection-CAS authorization before child creation."""
        if (
            not isinstance(expected_material_revision, int)
            or isinstance(expected_material_revision, bool)
            or expected_material_revision < 0
        ):
            raise LinearProjectionError("invalid_child_extension_material_frontier")
        if (
            not isinstance(expected_projection_revision, int)
            or isinstance(expected_projection_revision, bool)
            or expected_projection_revision < 0
        ):
            raise LinearProjectionError("invalid_child_extension_projection_frontier")
        if (
            not isinstance(native_initialization, dict)
            or set(native_initialization) != {"state_id", "assignee_id"}
            or not isinstance(native_initialization.get("state_id"), str)
            or not native_initialization["state_id"].strip()
            or (
                native_initialization.get("assignee_id") is not None
                and (
                    not isinstance(native_initialization["assignee_id"], str)
                    or not native_initialization["assignee_id"].strip()
                )
            )
        ):
            raise LinearProjectionError(
                "invalid_child_extension_native_initialization"
            )
        before_comments = self._comments()
        from workstream_linear_events import reduce_event_comments

        material_before = reduce_event_comments(
            before_comments, workstream_id=self.workstream_id
        )
        before = reduce_projection_comments(
            before_comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        static_value = {
            "root_issue_id": self.root_issue_id,
            "route": self.authority,
            "source": deepcopy(source),
            "plan_revision": self.plan_revision,
            "reviewed_candidate_key": reviewed_candidate_key,
            "child_issue_id": child_issue_id,
            "initial_state": "planned_pending_projection",
            "native_initialization": deepcopy(native_initialization),
            "generation_authority": deepcopy(generation_authority),
            "native_validation_sha256": native_validation_sha256,
        }
        matching = [
            item for item in before.events
            if item["kind"] == "child_extension_authorization"
            and item["key"] == child_issue_id
        ]
        if len(matching) > 1:
            raise LinearProjectionError(
                "child_extension_authorization_ambiguous"
            )
        if matching:
            event = matching[0]
            value = event.get("value")
            legacy_fields = {
                "root_issue_id", "route", "source", "plan_revision",
                "reviewed_candidate_key", "child_issue_id",
                "expected_material_revision", "expected_projection_revision",
                "initial_state",
            }
            if isinstance(value, dict) and set(value) == legacy_fields:
                if not require_existing:
                    raise LinearProjectionError(
                        "legacy_authorization_requires_existing_child"
                    )
                expected_legacy = {
                    "root_issue_id": self.root_issue_id, "route": self.authority,
                    "source": source, "plan_revision": self.plan_revision,
                    "reviewed_candidate_key": reviewed_candidate_key,
                    "child_issue_id": child_issue_id,
                    "initial_state": "planned_pending_projection",
                }
                if {key: value.get(key) for key in expected_legacy} != expected_legacy:
                    raise LinearProjectionError(
                        "child_extension_authorization_superseded_or_conflicting"
                    )
                self._select_child_extension_generation(
                    before_comments,
                    description_plan_revision=generation_authority.get(
                        "description_plan_revision"
                    ), source=source,
                )
                remote_id = before.remote_ids.get(event["event_id"])
                if not isinstance(remote_id, str):
                    raise LinearProjectionError(
                        "child_extension_authorization_readback_missing"
                    )
                return {
                    "event": event, "remote_id": remote_id,
                    "revision": before.revision,
                    "disposition": "legacy_existing",
                }
            if (
                not isinstance(value, dict)
                or {
                    key: value.get(key) for key in static_value
                } != static_value
                or not isinstance(value.get("expected_material_revision"), int)
                or isinstance(value.get("expected_material_revision"), bool)
                or not isinstance(value.get("expected_projection_revision"), int)
                or isinstance(value.get("expected_projection_revision"), bool)
                or event.get("expected_revision")
                != value["expected_projection_revision"]
            ):
                raise LinearProjectionError(
                    "child_extension_authorization_superseded_or_conflicting"
                )
            self._assert_child_extension_generation_authority(
                event, before_comments, generation_authority,
            )
            if material_before.revision != expected_material_revision:
                raise LinearProjectionError(
                    "child_extension_material_frontier_stale_reload_required"
                )
            if before.revision != expected_projection_revision:
                raise LinearProjectionError(
                    "child_extension_projection_frontier_stale_reload_required"
                )
            after_comments = before_comments
            disposition = "existing"
        else:
            if require_existing:
                raise LinearProjectionError(
                    "child_extension_preexisting_child_without_authorization"
                )
            selected_generation = self._select_child_extension_generation(
                before_comments,
                description_plan_revision=generation_authority.get(
                    "description_plan_revision"
                ) if isinstance(generation_authority, dict) else None,
                source=source,
            )
            if selected_generation != generation_authority:
                raise LinearProjectionError(
                    "child_extension_generation_authority_changed"
                )
            if material_before.revision != expected_material_revision:
                raise LinearProjectionError(
                    "child_extension_material_frontier_stale_reload_required"
                )
            if before.revision != expected_projection_revision:
                raise LinearProjectionError(
                    "child_extension_projection_frontier_stale_reload_required"
                )
            value = {
                **static_value,
                "expected_material_revision": expected_material_revision,
                "expected_projection_revision": expected_projection_revision,
            }
            event = build_projection_event(
                workstream_id=self.workstream_id,
                kind="child_extension_authorization", key=child_issue_id,
                value=value, plan_revision=self.plan_revision,
                expected_revision=expected_projection_revision,
                created_at="1970-01-01T00:00:00Z", authority=self.authority,
            )
            self.append(event)
            after_comments = self._comments()
            disposition = "created"
        receipt = self._assert_child_extension_authorization(event, after_comments)

        authorization_comment = next(
            item for item in after_comments
            if item.get("id") == receipt["remote_id"]
        )
        authorization_time = self._remote_time(authorization_comment)
        material_after = reduce_event_comments(
            after_comments, workstream_id=self.workstream_id
        )
        grant_material_revision = event["value"]["expected_material_revision"]
        for delta in material_after.events[grant_material_revision:]:
            remote_id = material_after.remote_ids.get(delta.event_id)
            comment = next(
                (item for item in after_comments if item.get("id") == remote_id), None
            )
            if not isinstance(comment, dict) or self._remote_time(comment) <= authorization_time:
                raise LinearProjectionError(
                    "child_extension_material_preceded_authorization_reload_required"
                )
        return {**receipt, "disposition": disposition}

    def replay_legacy_child_extension(
        self, *, source: dict[str, str], reviewed_candidate_key: str,
        child_issue_id: str, require_existing: bool,
    ) -> dict[str, Any] | None:
        """Replay a 0.4.29 grant without consulting unrelated native providers."""
        comments = self._comments()
        state = reduce_projection_comments(
            comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        matching = [
            event for event in state.events
            if event["kind"] == "child_extension_authorization"
            and event["key"] == child_issue_id
        ]
        if len(matching) > 1:
            raise LinearProjectionError("child_extension_authorization_ambiguous")
        if not matching:
            return None
        event = matching[0]
        value = event.get("value")
        legacy_fields = {
            "root_issue_id", "route", "source", "plan_revision",
            "reviewed_candidate_key", "child_issue_id",
            "expected_material_revision", "expected_projection_revision",
            "initial_state",
        }
        if not isinstance(value, dict) or set(value) != legacy_fields:
            return None
        if not require_existing:
            raise LinearProjectionError(
                "legacy_authorization_requires_existing_child"
            )
        expected = {
            "root_issue_id": self.root_issue_id, "route": self.authority,
            "source": source, "plan_revision": self.plan_revision,
            "reviewed_candidate_key": reviewed_candidate_key,
            "child_issue_id": child_issue_id,
            "initial_state": "planned_pending_projection",
        }
        if {key: value.get(key) for key in expected} != expected:
            raise LinearProjectionError(
                "child_extension_authorization_superseded_or_conflicting"
            )
        remote_id = state.remote_ids.get(event["event_id"])
        if not isinstance(remote_id, str) or not any(
            comment.get("id") == remote_id for comment in comments
        ):
            raise LinearProjectionError(
                "child_extension_authorization_readback_missing"
            )
        return {
            "event": event, "remote_id": remote_id,
            "revision": state.revision, "disposition": "legacy_existing",
        }

    def assert_child_extension_authorized(
        self, event: dict[str, Any],
    ) -> dict[str, Any]:
        """Re-read the exact durable grant; later unrelated events stay valid."""
        return self._assert_child_extension_authorization(event, self._comments())

    def repair_legacy_child_origin(
        self, *, value: dict[str, Any], created_at: str,
    ) -> dict[str, Any]:
        """Append one reviewed seal for an existing nondeterministic child."""
        comments = self._comments()
        state = reduce_projection_comments(
            comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        child_issue_id = value.get("child_issue_id")
        matching = [
            event for event in state.events
            if event["kind"] == "existing_child_origin_seal"
            and event["key"] == child_issue_id
        ]
        if len(matching) > 1:
            raise LinearProjectionError("child_origin_repair_ambiguous")
        event = build_projection_event(
            workstream_id=self.workstream_id,
            kind="existing_child_origin_seal", key=str(child_issue_id),
            value=value, plan_revision=self.plan_revision,
            expected_revision=value.get("expected_projection_revision"),
            created_at=created_at, authority=self.authority,
        )
        if matching:
            if matching[0] != event:
                raise LinearProjectionError("child_origin_repair_conflicting")
            remote_id = state.remote_ids.get(event["event_id"])
            if not isinstance(remote_id, str):
                raise LinearProjectionError("child_origin_repair_readback_missing")
            return {
                "event": event, "event_id": event["event_id"],
                "remote_id": remote_id, "revision": state.revision,
                "disposition": "existing",
            }
        selected = self._select_child_extension_generation(
            comments,
            description_plan_revision=value["generation_authority"].get(
                "description_plan_revision"
            ),
            source=value["source"],
        )
        if selected != value["generation_authority"]:
            raise LinearProjectionError("child_origin_repair_generation_changed")
        scope_events = [
            item for item in state.events
            if item["kind"] == "scope" and item["key"] == "root"
        ]
        scope = scope_events[-1] if scope_events else None
        if (
            scope is None
            or scope["event_id"] != value.get("scope_event_id")
            or hashlib.sha256(_canonical(scope["value"])).hexdigest()
            != value.get("scope_value_sha256")
            or scope["value"].get("child_ownership", {}).get(
                value.get("child_workstream_id")
            ) != value.get("repository_owner")
        ):
            raise LinearProjectionError("child_origin_repair_scope_changed")
        if state.snapshot.get("source") != value.get("source"):
            raise LinearProjectionError("child_origin_repair_source_changed")
        if state.revision != value.get("expected_projection_revision"):
            raise LinearProjectionError("child_origin_repair_projection_changed")
        if projection_prefix_frontier(
            state, comments,
        ) != value.get("root_projection_prefix"):
            raise LinearProjectionError("child_origin_repair_projection_prefix_changed")
        if child_origin_history_frontier(
            comments, workstream_id=self.workstream_id,
        ) != value.get("root_history"):
            raise LinearProjectionError("child_origin_repair_root_history_changed")
        native_result = self.client.execute(
            CHILD_ORIGIN_NATIVE_QUERY, {"childId": child_issue_id},
        )
        native_child = native_result.get("issue")
        expected_native = canonical_child_origin_native_readback(
            native_child,
            child_workstream_id=value["child_workstream_id"],
            child_issue_id=child_issue_id,
            root_workstream_id=self.workstream_id,
            root_issue_id=self.root_issue_id,
            route={key: self.authority[key] for key in (
                "workspace_id", "team_id", "project_id",
            )},
        )
        if (
            expected_native != value.get("native_child_readback")
            or hashlib.sha256(_canonical(expected_native)).hexdigest()
            != value.get("native_child_readback_sha256")
        ):
            raise LinearProjectionError("child_origin_repair_native_readback_changed")
        from workstream_child_proposal import _comments as child_comments, proposal_index

        child_before = child_comments(self.client, value["child_workstream_id"])
        if proposal_index(child_before):
            raise LinearProjectionError("child_origin_preexisting_inert_proposals")
        if child_origin_history_frontier(
            child_before, workstream_id=value["child_workstream_id"],
        ) != value.get("child_history"):
            raise LinearProjectionError("child_origin_repair_child_history_changed")
        validate_existing_child_origin_root_snapshot(self.client, event)
        receipt = self.append(
            event,
            expected_material_revision=value["root_history"][
                "material_frontier"
            ]["revision"],
        )
        after_comments = self._comments()
        after = reduce_projection_comments(
            after_comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        observed = [
            item for item in after.events
            if item["kind"] == "existing_child_origin_seal"
            and item["key"] == child_issue_id
        ]
        if observed != [event]:
            raise LinearProjectionError("child_origin_repair_readback_mismatch")
        if (
            after.revision != value["expected_projection_revision"] + 1
            or not after.events
            or after.events[-1] != event
            or projection_prefix_frontier(
                after, after_comments,
                through_event_id=value["root_projection_prefix"][
                    "through_event_id"
                ],
            ) != value["root_projection_prefix"]
            or child_origin_history_frontier(
                after_comments, workstream_id=self.workstream_id,
            ) != value["root_history"]
        ):
            raise LinearProjectionError(
                "child_origin_repair_postread_root_drift_authority_changed"
            )
        child_after = child_comments(self.client, value["child_workstream_id"])
        if (
            proposal_index(child_after)
            or child_origin_history_frontier(
                child_after, workstream_id=value["child_workstream_id"],
            ) != value["child_history"]
        ):
            raise LinearProjectionError(
                "child_origin_repair_postread_child_drift_authority_changed"
            )
        native_after = canonical_child_origin_native_readback(
            self.client.execute(
                CHILD_ORIGIN_NATIVE_QUERY, {"childId": child_issue_id},
            ).get("issue"),
            child_workstream_id=value["child_workstream_id"],
            child_issue_id=child_issue_id,
            root_workstream_id=self.workstream_id,
            root_issue_id=self.root_issue_id,
            route={key: self.authority[key] for key in (
                "workspace_id", "team_id", "project_id",
            )},
        )
        if native_after != value["native_child_readback"]:
            raise LinearProjectionError(
                "child_origin_repair_postread_native_drift_authority_changed"
            )
        return {
            **receipt, "event": event, "disposition": "created",
        }

    def reserve_child_mutation(
        self, *, proposal: dict[str, Any], proposal_remote_id: str,
        child_identity: dict[str, Any], generation_authority: dict[str, Any],
        scope_event_id: str, scope_value_sha256: str, repository_owner: str,
        child_origin: dict[str, Any],
        expected_projection_revision: int,
        publish_intent: bool = False,
    ) -> dict[str, Any]:
        """Reserve or activate one child proposal through the root CAS."""
        from workstream_child_proposal import proposal_slot_id

        if (
            proposal.get("child_workstream_id") != child_identity.get("identifier")
            or proposal.get("child_issue_id") != child_identity.get("id")
            or proposal.get("plan_revision") != self.plan_revision
            or proposal_remote_id != proposal_slot_id(
                child_identity.get("id"), proposal.get("proposal_id")
            )
        ):
            raise LinearProjectionError("child_mutation_proposal_identity_mismatch")
        comments = self._comments()
        state = reduce_projection_comments(
            comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        source = state.snapshot.get("source")
        value = {
            "root_issue_id": self.root_issue_id, "route": self.authority,
            "source": deepcopy(source), "plan_revision": self.plan_revision,
            "generation_authority": deepcopy(generation_authority),
            "scope_event_id": scope_event_id,
            "scope_value_sha256": scope_value_sha256,
            "repository_owner": repository_owner,
            "child_origin": deepcopy(child_origin),
            "child_workstream_id": child_identity["identifier"],
            "child_issue_id": child_identity["id"],
            "child_parent_issue_id": child_identity["parent_issue_id"],
            "child_route": deepcopy(child_identity["route"]),
            "mutation_kind": proposal["kind"],
            "proposal_id": proposal["proposal_id"],
            "proposal_remote_id": proposal_remote_id,
            "record_sha256": proposal["record_sha256"],
            "expected_material_revision": (
                proposal["record"]["expected_revision"]
                if proposal["kind"] == "event"
                else proposal["record"]["root_revision"]
            ),
            "predecessor_event_id": (
                None if proposal["kind"] == "event"
                else proposal["record"].get("predecessor_event_id")
            ),
        }
        matching = [
            event for event in state.events
            if event["kind"] == "child_mutation_authorization"
            and event["key"] == proposal["proposal_id"]
        ]
        if len(matching) > 1:
            raise LinearProjectionError("child_mutation_authorization_ambiguous")
        if matching:
            event = matching[0]
            if event.get("value") != value:
                raise LinearProjectionError("child_mutation_authorization_conflict")
            self._assert_child_mutation_authorization(event, comments)
            self._assert_child_mutation_proposal(event, proposal)
            return {"event": event, "disposition": "existing"}
        from workstream_child_proposal import (
            _comments as child_comments, authorized_child_comments,
        )
        from workstream_linear_events import reduce_event_comments
        from workstream_linear_checkpoints import reduce_checkpoint_comments

        existing_authorizations = child_mutation_authorizations_from_comments(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=generation_authority.get(
                "description_plan_revision"
            ), authenticated_route=self.authority,
        )
        origin_repairs = legacy_child_origin_repairs_from_comments(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=generation_authority.get(
                "description_plan_revision"
            ), authenticated_route=self.authority,
        )
        active_comments = authorized_child_comments(
            child_comments(self.client, child_identity["identifier"]),
            existing_authorizations, origin_repairs,
            child_workstream_id=child_identity["identifier"],
            child_issue_id=child_identity["id"],
        )
        material = reduce_event_comments(
            active_comments, workstream_id=child_identity["identifier"],
        )
        checkpoints = reduce_checkpoint_comments(
            active_comments, workstream_id=child_identity["identifier"],
        )
        current = [
            item for item in checkpoints.checkpoints
            if item["plan_revision"] == self.plan_revision
        ]
        if current:
            from workstream_checkpoint import CheckpointError, recover_latest
            try:
                recover_latest(
                    current, child_identity["identifier"],
                    expected_plan_revision=self.plan_revision,
                )
            except CheckpointError as error:
                raise LinearProjectionError(
                    f"child_mutation_checkpoint_history_invalid:{error}"
                ) from error
        if proposal["kind"] == "event":
            if any(
                event.event_id == proposal["record"]["event_id"]
                for event in material.events
            ):
                raise LinearProjectionError(
                    "child_mutation_event_id_already_authorized"
                )
            if proposal["record"]["expected_revision"] != material.revision:
                raise LinearProjectionError(
                    "child_mutation_material_frontier_stale_reload_required"
                )
        else:
            expected_predecessor = (
                sorted(current, key=lambda item: (
                    item["root_revision"], item["event_id"],
                ))[-1]["event_id"] if current else None
            )
            if (
                proposal["record"]["root_revision"] != material.revision
                or proposal["record"].get("predecessor_event_id")
                != expected_predecessor
                or (
                    current
                    and proposal["record"]["root_revision"]
                    <= max(item["root_revision"] for item in current)
                )
            ):
                raise LinearProjectionError(
                    "child_mutation_checkpoint_frontier_stale_reload_required"
                )
        selected = self._select_child_extension_generation(
            comments,
            description_plan_revision=generation_authority.get(
                "description_plan_revision"
            ), source=source,
        )
        if selected != generation_authority:
            raise LinearProjectionError("child_mutation_generation_changed")
        if state.revision != expected_projection_revision:
            raise LinearProjectionError(
                "child_mutation_projection_frontier_stale_reload_required"
            )
        scope_events = [
            item for item in state.events
            if item["kind"] == "scope" and item["key"] == "root"
        ]
        scope = scope_events[-1] if scope_events else None
        if (
            scope is None or scope["event_id"] != scope_event_id
            or hashlib.sha256(_canonical(scope["value"])).hexdigest()
            != scope_value_sha256
            or scope["value"].get("child_ownership", {}).get(
                child_identity["identifier"]
            ) != repository_owner
        ):
            raise LinearProjectionError("child_mutation_scope_changed")
        self._assert_child_origin(
            value, state.events, before_index=state.revision, comments=comments,
        )
        event = build_projection_event(
            workstream_id=self.workstream_id,
            kind="child_mutation_authorization", key=proposal["proposal_id"],
            value=value, plan_revision=self.plan_revision,
            expected_revision=state.revision,
            created_at="1970-01-01T00:00:00Z", authority=self.authority,
        )
        reservation = self._reserve_child_mutation_intent(event)
        if publish_intent:
            return {
                "event": event, "reservation": reservation,
                "disposition": "reserved",
            }
        self._assert_child_mutation_proposal_value(value, proposal)
        self.append(event)
        after = self._comments()
        self._assert_child_mutation_authorization(event, after)
        self._assert_child_mutation_proposal(event, proposal)
        from workstream_linear_events import pending_ledger_reservations

        if any(
            item["intent_event"] == event for item in pending_ledger_reservations(
                after, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
                current_plan_revision=self.plan_revision,
            )
        ):
            raise LinearProjectionError("child_mutation_reservation_not_released")
        return {"event": event, "disposition": "created"}

    def _reserve_child_mutation_intent(
        self, event: dict[str, Any],
    ) -> dict[str, Any]:
        """Serialize child publication against generation authority changes."""
        from workstream_generation import assert_no_pending_generation_reservation
        from workstream_linear_checkpoints import reduce_checkpoint_comments
        from workstream_linear_events import (
            encode_ledger_reservation, ledger_boundary_slot_id,
            ledger_serialization_frontier, pending_ledger_reservations,
        )

        comments = self._comments()
        pending = pending_ledger_reservations(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
            current_plan_revision=self.plan_revision,
        )
        exact = [item for item in pending if item["intent_event"] == event]
        if len(exact) == 1:
            return exact[0]
        if exact or pending:
            raise LinearProjectionError("child_mutation_serialization_reserved")
        assert_no_pending_generation_reservation(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        )
        state = reduce_projection_comments(
            comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        if state.revision != event["expected_revision"]:
            raise LinearProjectionError(
                "child_mutation_projection_frontier_stale_reload_required"
            )
        material = reduce_event_comments(
            comments, workstream_id=self.workstream_id,
        )
        checkpoints = reduce_checkpoint_comments(
            comments, workstream_id=self.workstream_id,
        )
        frontier = ledger_serialization_frontier(
            sorted(item["event_id"] for item in checkpoints.checkpoints),
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
            current_plan_revision=self.plan_revision,
            material_revision=material.revision,
        )
        reservation = {
            "schema_version": 1, "workstream_id": self.workstream_id,
            "material_revision": material.revision,
            "intent_kind": "child_mutation_projection",
            "plan_revision": self.plan_revision,
            "projection_revision": state.revision,
            "projection_frontier_ids": [
                state.remote_ids[item["event_id"]] for item in state.events
            ],
            "frontier_ids": frontier, "authority": self.authority,
            "intent_event": event,
            "intent_sha256": hashlib.sha256(_canonical(event)).hexdigest(),
        }
        slot = ledger_boundary_slot_id(
            self.workstream_id, material.revision, frontier, self.authority,
        )
        body = encode_ledger_reservation(reservation)
        self._assert_comment_id_capability()
        try:
            response = self.client.execute(COMMENT_CREATE_MUTATION, {"input": {
                "id": slot, "issueId": self.issue_id, "body": body,
            }})
        except (LinearTransportError, OSError, TimeoutError):
            response = None
        after = self._comments()
        observed = [
            item for item in pending_ledger_reservations(
                after, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
                current_plan_revision=self.plan_revision,
            )
            if item["intent_event"] == event
        ]
        if len(observed) != 1:
            raise LinearProjectionError(
                "child_mutation_serialization_slot_lost_reload_required"
            )
        if response is not None:
            created = response.get("commentCreate") or {}
            comment = created.get("comment") or {}
            if (
                created.get("success") is not True
                or comment.get("id") != slot or comment.get("body") != body
            ):
                raise LinearProjectionError(
                    "child_mutation_reservation_unconfirmed"
                )
        return observed[0]

    def _assert_child_mutation_proposal_value(
        self, value: dict[str, Any], proposal: dict[str, Any],
    ) -> None:
        from workstream_child_proposal import _comments, proposal_index

        comments = _comments(self.client, value["child_workstream_id"])
        found = proposal_index(comments).get(value["proposal_id"])
        if (
            found is None or found[0] != proposal
            or found[1].get("id") != value["proposal_remote_id"]
        ):
            raise LinearProjectionError("child_mutation_proposal_missing_or_mismatch")

    def _assert_child_mutation_proposal(
        self, event: dict[str, Any], proposal: dict[str, Any],
    ) -> None:
        self._assert_child_mutation_proposal_value(event["value"], proposal)

    def _assert_child_mutation_authorization(
        self, event: dict[str, Any], comments: list[dict[str, Any]],
    ) -> None:
        value = event["value"]
        self._assert_child_extension_generation_authority(
            event, comments, value["generation_authority"],
        )
        state = reduce_projection_comments(
            comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        try:
            index = list(state.events).index(event)
        except ValueError as error:
            raise LinearProjectionError(
                "child_mutation_authorization_missing"
            ) from error
        scope_heads = [
            item for item in state.events[:index]
            if item["kind"] == "scope" and item["key"] == "root"
        ]
        scope = scope_heads[-1] if scope_heads else None
        if (
            scope is None or scope["event_id"] != value["scope_event_id"]
            or hashlib.sha256(_canonical(scope["value"])).hexdigest()
            != value["scope_value_sha256"]
            or scope["value"].get("child_ownership", {}).get(
                value["child_workstream_id"]
            ) != value["repository_owner"]
        ):
            raise LinearProjectionError("child_mutation_scope_proof_invalid")
        self._assert_child_origin(
            value, state.events, before_index=index, comments=comments,
        )

    def _assert_child_origin(
        self, value: dict[str, Any], events: Any, *, before_index: int,
        comments: list[dict[str, Any]],
    ) -> None:
        """Bind child authority to immutable root provenance, never native caches."""
        extension_origins = child_extension_authorizations_from_comments(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=value["generation_authority"].get(
                "description_plan_revision"
            ), authenticated_route=self.authority,
        )
        repair_origins = legacy_child_origin_repairs_from_comments(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=value["generation_authority"].get(
                "description_plan_revision"
            ), authenticated_route=self.authority,
        )
        current_events = list(events)[:before_index]
        validate_child_origin_value(
            value, extension_origins=[
                origin for origin in extension_origins
                if origin["plan_revision"] != self.plan_revision
                or origin in current_events
            ], repair_origins=[
                origin for origin in repair_origins
                if origin["plan_revision"] != self.plan_revision
                or origin in current_events
            ], authenticated_route=self.authority,
        )
        if value["child_origin"].get("kind") == "existing_child_origin_seal":
            matches = [
                origin for origin in repair_origins
                if origin["event_id"] == value["child_origin"].get("event_id")
            ]
            if len(matches) != 1:
                raise LinearProjectionError("child_origin_repair_ambiguous")
            validate_existing_child_origin_root_identity(self.client, matches[0])

    def child_mutation_authorizations(self) -> list[dict[str, Any]]:
        comments = self._comments()
        state = reduce_projection_comments(
            comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        result = [
            event for event in state.events
            if event["kind"] == "child_mutation_authorization"
        ]
        for event in result:
            self._assert_child_mutation_authorization(event, comments)
        return result

    def _assert_child_dependency_authorization(
        self, event: dict[str, Any], comments: list[dict[str, Any]],
    ) -> dict[str, Any]:
        state = reduce_projection_comments(
            comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        matching = [
            item for item in state.events
            if item["kind"] == "child_dependency_authorization"
            and item["key"] == event["key"]
        ]
        if not matching or matching[-1] != event:
            raise LinearProjectionError(
                "child_dependency_authorization_superseded_or_conflicting"
            )
        remote_id = state.remote_ids.get(event["event_id"])
        if not any(item.get("id") == remote_id for item in comments):
            raise LinearProjectionError(
                "child_dependency_authorization_readback_missing"
            )
        return {"event": event, "remote_id": remote_id, "revision": state.revision}

    def reserve_child_dependencies(
        self, *, batch_id: str, relation_ids: list[str], relations_sha256: str,
        expected_material_revision: int, expected_projection_revision: int,
        expected_graph_revision: int, expected_graph_sha256: str,
    ) -> dict[str, Any]:
        """Win a durable projection-CAS grant before native relation creation."""
        before_comments = self._comments()
        from workstream_linear_events import reduce_event_comments

        material = reduce_event_comments(
            before_comments, workstream_id=self.workstream_id,
        )
        before = reduce_projection_comments(
            before_comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )
        value = {
            "root_issue_id": self.root_issue_id,
            "route": self.authority,
            "plan_revision": self.plan_revision,
            "batch_id": batch_id,
            "relation_ids": sorted(relation_ids),
            "relations_sha256": relations_sha256,
            "expected_material_revision": expected_material_revision,
            "expected_projection_revision": expected_projection_revision,
            "expected_graph_revision": expected_graph_revision,
            "expected_graph_sha256": expected_graph_sha256,
            "initial_state": "owned_children_validated",
        }
        event = build_projection_event(
            workstream_id=self.workstream_id,
            kind="child_dependency_authorization", key=batch_id,
            value=value, plan_revision=self.plan_revision,
            expected_revision=expected_projection_revision,
            created_at="1970-01-01T00:00:00Z", authority=self.authority,
        )
        existing = next(
            (item for item in before.events if item["event_id"] == event["event_id"]),
            None,
        )
        if existing is None:
            if material.revision != expected_material_revision:
                raise LinearProjectionError(
                    "child_dependency_material_frontier_stale_reload_required"
                )
            if before.revision != expected_projection_revision:
                raise LinearProjectionError(
                    "child_dependency_projection_frontier_stale_reload_required"
                )
            self.append(event)
            after_comments = self._comments()
        else:
            if existing != event:
                raise LinearProjectionError(
                    "child_dependency_authorization_superseded_or_conflicting"
                )
            after_comments = before_comments
        receipt = self._assert_child_dependency_authorization(event, after_comments)
        authorization_comment = next(
            item for item in after_comments
            if item.get("id") == receipt["remote_id"]
        )
        authorization_time = self._remote_time(authorization_comment)
        material_after = reduce_event_comments(
            after_comments, workstream_id=self.workstream_id,
        )
        for delta in material_after.events[expected_material_revision:]:
            remote_id = material_after.remote_ids.get(delta.event_id)
            comment = next(
                (item for item in after_comments if item.get("id") == remote_id), None
            )
            if (
                not isinstance(comment, dict)
                or self._remote_time(comment) <= authorization_time
            ):
                raise LinearProjectionError(
                    "child_dependency_material_preceded_authorization_reload_required"
                )
        return receipt

    def assert_child_dependencies_authorized(
        self, event: dict[str, Any],
    ) -> dict[str, Any]:
        """Re-read the exact durable native-dependency grant."""
        return self._assert_child_dependency_authorization(event, self._comments())
