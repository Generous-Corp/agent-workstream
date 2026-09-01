#!/usr/bin/env python3
"""Validate and compact a Linear-backed workstream snapshot for recovery.

The transport that obtains the snapshot may be Linear MCP, a future CLI, or a
repository-specific adapter. This command is deliberately transport-neutral:
it validates the durable join and refuses ambiguous/stale/incomplete input
before an agent edits anything.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_checkpoint import CheckpointError, recover_latest, validate_checkpoint
from workstream_linear import (
    HttpGraphQLClient, LinearGraphQLTransport,
    LinearTransportError,
    resolve_authenticated_issue_route,
)
from workstream_linear_checkpoints import (
    LinearCheckpointError,
    reduce_checkpoint_comments,
)
from workstream_linear_events import (
    LinearCommentEventAdapter,
    LinearEventError,
    apply_material_semantic_repairs,
    ledger_serialization_frontier,
    reduce_event_comments,
)
from workstream_linear_projection import (
    _inspect_unsealed_identity_history, inspect_unsealed_identity_history,
    LinearProjectionError,
    reduce_projection_comments, select_plan_generation, TOMBSTONE,
    validate_projection_event,
)
from workstream_plan import plan_payload
from workstream_relation_readback import read_relation_targets
from workstream_choices import ChoiceError, reduce_choices
from workstream_evidence import evidence_errors
from workstream_child_closure import (
    canonical_digest, evidence_receipts_sha256, terminal_child_readback,
    ChildClosureError,
)
from workstream_child_dependencies import (
    ChildDependencyError, LinearChildDependencyAdapter,
    dependency_root_readback_sha256,
    validate_authorized_dependency_graph_surface,
)
from workstream_scope import (
    repository_key, ScopeError, validate_relations, validate_scope,
)
from workstream_projection_history import (
    closure_bound_historical_evidence, ProjectionHistoryError,
)


TOKEN = re.compile(r"\b[A-Z][A-Z0-9]*-\d+\b", re.I)
MAX_WORKSTREAM_IDENTIFIER_BYTES = 128
MAX_PLAN_REVISION_BYTES = 256
MAX_REVISION = (1 << 63) - 1
TERMINAL = {"done", "completed", "cancelled", "canceled", "superseded"}
MATERIAL_OBLIGATION_TERMS = ("requirement", "blocker", "blocked", "followup", "decision")
MATERIAL_OBLIGATION_KEYS = {
    "requirement", "requirements", "blocker", "blockers",
    "followup", "followups", "decision", "decisions",
}
RAW_TRANSCRIPT_KEYS = {"raw_transcript", "transcript"}


class ResumeError(ValueError):
    pass


def _is_terminal(item: dict[str, Any]) -> bool:
    return any(
        str(item.get(field, "")).lower() in TERMINAL
        for field in ("status_type", "status")
    )


def _without_raw_transcripts(value: Any) -> Any:
    """Exclude transcript bodies from every default and audit resume payload."""
    if isinstance(value, dict):
        return {
            key: _without_raw_transcripts(item)
            for key, item in value.items()
            if str(key).lower().replace("-", "_") not in RAW_TRANSCRIPT_KEYS
        }
    if isinstance(value, list):
        return [_without_raw_transcripts(item) for item in value]
    return value


def closure_snapshot_digest(snapshot: dict[str, Any]) -> str:
    """Digest semantic closure input, excluding review/lifecycle self-writes."""
    material = deepcopy(snapshot)
    material.pop("lifecycle", None)
    material.pop("closure_reviews", None)
    material.pop("lifecycle_recovery", None)
    root = material.get("root")
    if isinstance(root, dict):
        issue_status = root.pop("issue_status", root.get("status"))
        root["status"] = issue_status
        root.pop("closure_receipt", None)
        root.pop("description_plan_revision", None)
        root.pop("generation_transition_tip_event_id", None)
        root.pop("generation_activation_epoch", None)
        root.pop("generation_authority_origin", None)
        root.pop("quarantined_legacy_writes", None)
    events = material.get("projection_events")
    if isinstance(events, list):
        material["projection_events"] = [
            event for event in events
            if event.get("kind") not in {"closure_review", "lifecycle"}
        ]
        material["projection_revision"] = len(material["projection_events"])
        recovery = material.get("projection_recovery")
        if isinstance(recovery, dict) and not material["projection_events"]:
            recovery["state"] = (
                "stale_plan" if material.get("projection_history") else "not_found"
            )
    return hashlib.sha256(json.dumps(
        material, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _event_record(event: Any) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "workstream_id": event.workstream_id,
        "kind": event.kind,
        "source": event.source,
        "payload": event.payload,
        "expected_revision": event.expected_revision,
        "created_at": event.created_at,
    }


def _checkpoint_repair_frontier(
    checkpoints: Any, *, count: int | None = None,
) -> dict[str, Any]:
    records = list(checkpoints.checkpoints)
    if count is not None:
        if type(count) is not int or count < 0:
            raise ResumeError("invalid_repair_checkpoint_frontier_count")
        records = records[:count]
    ids = [item["event_id"] for item in records]
    return {
        "algorithm": "checkpoint-reducer-order-v1",
        "count": len(records),
        "revision": max((item["root_revision"] for item in records), default=0),
        "event_ids_reducer_order_sha256": hashlib.sha256(json.dumps(
            ids, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
        "event_ids_sorted_set_sha256": hashlib.sha256(json.dumps(
            sorted(set(ids)), ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        ).encode()).hexdigest(),
        "checkpoints_sha256": hashlib.sha256(json.dumps(
            records, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
    }


def _projection_repair_frontier(
    projection: Any, *, revision: int | None = None,
) -> dict[str, Any]:
    records = list(projection.events)
    if revision is not None:
        if type(revision) is not int or revision < 0:
            raise ResumeError("invalid_repair_projection_frontier_revision")
        records = records[:revision]
    return {
        "algorithm": "active-projection-reducer-order-v1",
        "revision": len(records),
        "frontier_event_id": records[-1]["event_id"] if records else None,
        "events_sha256": hashlib.sha256(json.dumps(
            records, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
    }


def _generation_repair_binding(root: dict[str, Any]) -> dict[str, Any]:
    return {
        "plan_revision": root.get("plan_revision"),
        "transition_tip_event_id": root.get("generation_transition_tip_event_id"),
        "activation_epoch": root.get("generation_activation_epoch"),
        "authority_origin": root.get("generation_authority_origin"),
    }


def _issue_graph_repair_frontier(
    snapshot: dict[str, Any], relations: list[dict[str, Any]],
    relation_targets: dict[str, Any] | None,
) -> dict[str, Any]:
    def native(issue: Any) -> dict[str, Any]:
        if not isinstance(issue, dict):
            return {}
        state = issue.get("state")
        if not isinstance(state, dict):
            state = {
                "id": issue.get("state_id"), "name": issue.get("status"),
                "type": issue.get("status_type"),
            }
        return {
            key: issue.get(key) for key in (
                "id", "identifier", "url", "title", "parent", "team", "project",
                "assignee", "archivedAt", "updatedAt", "description",
                "next_action", "revision", "plan_revision", "status",
                "status_type", "state_id",
            )
        } | {"state": state}
    graph = {
        "root": native(snapshot.get("root")),
        "children": sorted(
            (native(item) for item in snapshot.get("children", [])),
            key=lambda item: (str(item.get("identifier")), str(item.get("id"))),
        ),
        "relations": relations,
        "relation_targets": relation_targets or {},
    }
    return {
        "algorithm": "authenticated-root-children-relations-v1",
        "issues": graph,
        "sha256": hashlib.sha256(json.dumps(
            graph, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
    }


def _event_next_actions(event: dict[str, Any]) -> set[str]:
    if event["kind"] == "material_semantic_repair":
        return set()
    payloads = [event["payload"]]
    if event["kind"] == "material_boundary":
        changes = event["payload"].get("changes")
        if not isinstance(changes, list):
            raise ResumeError(f"malformed_material_boundary:{event['event_id']}")
        for change in changes:
            if (
                not isinstance(change, dict)
                or not isinstance(change.get("kind"), str)
                or not change["kind"]
                or not isinstance(change.get("payload"), dict)
            ):
                raise ResumeError(f"malformed_material_boundary:{event['event_id']}")
            payloads.append(change["payload"])
    actions: set[str] = set()
    for payload in payloads:
        if "next_action" not in payload:
            continue
        value = payload["next_action"]
        if not isinstance(value, str) or not value.strip():
            raise ResumeError(f"invalid_event_next_action:{event['event_id']}")
        actions.add(value.strip())
    if len(actions) > 1:
        raise ResumeError(f"conflicting_event_next_action:{event['event_id']}")
    return actions


def _event_payloads(event: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    if event["kind"] == "material_semantic_repair":
        return []
    payloads = [(event["kind"], event["payload"])]
    if event["kind"] == "material_boundary":
        changes = event["payload"].get("changes")
        if not isinstance(changes, list):
            raise ResumeError(f"malformed_material_boundary:{event['event_id']}")
        for change in changes:
            if (
                not isinstance(change, dict)
                or not isinstance(change.get("kind"), str)
                or not change["kind"]
                or not isinstance(change.get("payload"), dict)
            ):
                raise ResumeError(f"malformed_material_boundary:{event['event_id']}")
            payloads.append((change["kind"], change["payload"]))
    return payloads


def _event_blockers(event: dict[str, Any]) -> list[dict[str, Any] | None]:
    blockers: list[dict[str, Any] | None] = []
    for kind, payload in _event_payloads(event):
        if "blocker" in payload:
            value = payload["blocker"]
        elif kind.lower() in {"blocker", "blocked"}:
            value = payload
        else:
            continue
        # Early ledger writers stored a concise blocker as a non-empty string.
        # Preserve that append-only history in the current structured surface
        # instead of making an otherwise valid historical workstream unreadable.
        if isinstance(value, str) and value.strip():
            value = {"text": value.strip()}
        if value is not None and (not isinstance(value, dict) or not value):
            raise ResumeError(f"invalid_event_blocker:{event['event_id']}")
        blockers.append(value)
    return blockers


def _resolved_next_action(
    events: list[dict[str, Any]], current: str | None,
    checkpoint: dict[str, Any] | None,
) -> str | None:
    """Let an acknowledged checkpoint fence older material instructions."""
    if current is not None and (not isinstance(current, str) or not current.strip()):
        raise ResumeError("invalid_root_next_action")
    checkpoint_revision = 0
    resolved = current.strip() if current is not None else None
    if checkpoint is not None:
        checkpoint_revision = checkpoint["root_revision"]
        resolved = checkpoint["next_action"]
    next_actions_by_revision: dict[int, set[str]] = {}
    for index, event in enumerate(events):
        for value in _event_next_actions(event):
            expected = event["expected_revision"]
            actions = next_actions_by_revision.setdefault(expected, set())
            actions.add(value)
            if len(actions) > 1:
                raise ResumeError(f"conflicting_concurrent_next_action:{expected}")
            if index >= checkpoint_revision:
                resolved = value
    return resolved


def _resolved_blocker(
    events: list[dict[str, Any]], current: dict[str, Any] | None,
    checkpoint: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if current is not None and not isinstance(current, dict):
        raise ResumeError("invalid_child_blocker")
    checkpoint_revision = 0
    resolved = deepcopy(current)
    if checkpoint is not None:
        checkpoint_revision = checkpoint["root_revision"]
        resolved = deepcopy(checkpoint["blocker"])
    blockers_by_revision: dict[int, set[str]] = {}
    for index, event in enumerate(events):
        for value in _event_blockers(event):
            encoded = json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            )
            values = blockers_by_revision.setdefault(event["expected_revision"], set())
            values.add(encoded)
            if len(values) > 1:
                raise ResumeError(
                    f"conflicting_concurrent_blocker:{event['expected_revision']}"
                )
            if index >= checkpoint_revision:
                resolved = deepcopy(value)
    return resolved


def _validate_child_route(
    child: dict[str, Any], *, token: str, authenticated_route: dict[str, str],
) -> list[dict[str, Any]]:
    if str(child.get("identifier", "")).upper() != token:
        raise ResumeError(f"child_identity_mismatch:{token}")
    if not isinstance(child.get("id"), str) or not child["id"]:
        raise ResumeError(f"child_identity_missing:{token}")
    observed = {
        "parent_issue_id": (child.get("parent") or {}).get("id"),
        "team_id": (child.get("team") or {}).get("id"),
        "workspace_id": ((child.get("team") or {}).get("organization") or {}).get("id"),
        "project_id": (child.get("project") or {}).get("id"),
    }
    expected = {
        "parent_issue_id": authenticated_route.get("root_issue_id"),
        "team_id": authenticated_route.get("team_id"),
        "workspace_id": authenticated_route.get("workspace_id"),
        "project_id": authenticated_route.get("project_id"),
    }
    return [{
        "kind": "native_child_cache_drift", "field": field,
        "expected": expected[field], "observed": observed[field],
        "reconciliation_required": True,
    } for field in expected if observed[field] != expected[field]]


def _recover_checkpoint_generations(
    checkpoints: list[dict[str, Any]], token: str, *, error_prefix: str,
) -> dict[str, dict[str, Any]]:
    """Validate every immutable plan generation and return each chain tip."""
    generations: dict[str, list[dict[str, Any]]] = {}
    for checkpoint in checkpoints:
        revision = checkpoint.get("plan_revision")
        if not isinstance(revision, str) or not revision:
            raise ResumeError(f"{error_prefix}:{token}:invalid_plan_revision")
        generations.setdefault(revision, []).append(checkpoint)
    recovered: dict[str, dict[str, Any]] = {}
    for revision, records in generations.items():
        try:
            recovered[revision] = recover_latest(
                records, token, expected_plan_revision=revision,
            )
        except CheckpointError as error:
            raise ResumeError(f"{error_prefix}:{token}:{error}") from error
    return recovered


def add_child_material_history(
    snapshot: dict[str, Any], child_comments: dict[str, list[dict[str, Any]]],
    *, authenticated_route: dict[str, str],
    root_comments: list[dict[str, Any]] | None = None,
    proposal_plan_revision: str | None = None,
) -> dict[str, Any]:
    """Reduce every nonterminal child log without mixing child/root authority."""
    if not isinstance(child_comments, dict):
        raise ResumeError("invalid_child_comment_collection")
    result = dict(snapshot)
    result["children"] = []
    nonterminal_tokens = {
        str(child.get("identifier", "")).upper()
        for child in snapshot.get("children", [])
        if not _is_terminal(child)
    }
    if set(child_comments) != nonterminal_tokens:
        raise ResumeError("incomplete_child_comment_collection")
    plan_revision = (snapshot.get("root") or {}).get("plan_revision")
    # Ordinary resume classifies proposals against the active plan. Projection
    # preparation can explicitly select the still-active predecessor while it
    # evaluates an inactive target; description provenance cannot distinguish
    # that case from a completed structured transition.
    selected_proposal_plan_revision = proposal_plan_revision or plan_revision
    authorizations: list[dict[str, Any]] = []
    origin_repairs: list[dict[str, Any]] = []
    if root_comments is not None:
        from workstream_linear_projection import (
            child_mutation_authorizations_from_comments,
            legacy_child_origin_repairs_from_comments,
        )
        authorizations = child_mutation_authorizations_from_comments(
            root_comments,
            workstream_id=(snapshot.get("root") or {})["identifier"],
            description_plan_revision=(snapshot.get("root") or {}).get(
                "description_plan_revision", plan_revision
            ), authenticated_route=authenticated_route,
        )
        origin_repairs = legacy_child_origin_repairs_from_comments(
            root_comments,
            workstream_id=(snapshot.get("root") or {})["identifier"],
            description_plan_revision=(snapshot.get("root") or {}).get(
                "description_plan_revision", plan_revision
            ), authenticated_route=authenticated_route,
        )
    for source_child in snapshot.get("children", []):
        child = dict(source_child)
        token = str(child.get("identifier", "")).upper()
        cache_drift = _validate_child_route(
            child, token=token, authenticated_route=authenticated_route,
        )
        if cache_drift:
            child["reconciliation_blockers"] = cache_drift
        if _is_terminal(child):
            result["children"].append(child)
            continue
        comments = child_comments[token]
        if not isinstance(comments, list):
            raise ResumeError(f"invalid_child_comment_collection:{token}")
        from workstream_child_proposal import (
            authorized_child_comments, pending_proposal_obligations,
        )
        pending_proposals = pending_proposal_obligations(
            comments, authorizations, child_workstream_id=token,
            child_issue_id=child["id"],
            plan_revision=selected_proposal_plan_revision,
        )
        if pending_proposals:
            child["pending_child_proposals"] = pending_proposals
        if authorizations or origin_repairs:
            comments = authorized_child_comments(
                comments, authorizations, origin_repairs,
                child_workstream_id=token, child_issue_id=child["id"],
            )
        event_log = reduce_event_comments(comments, workstream_id=token)
        checkpoint_log = reduce_checkpoint_comments(comments, workstream_id=token)
        if not event_log.events and not checkpoint_log.checkpoints:
            result["children"].append(child)
            continue
        events = [_event_record(event) for event in event_log.events]
        checkpoints = list(checkpoint_log.checkpoints)
        recovered_generations = _recover_checkpoint_generations(
            checkpoints, token, error_prefix="invalid_child_checkpoint_history",
        )
        current_checkpoints = [
            checkpoint for checkpoint in checkpoint_log.checkpoints
            if checkpoint["plan_revision"] == plan_revision
        ]
        stale_checkpoint_count = len(checkpoint_log.checkpoints) - len(current_checkpoints)
        latest_checkpoint = None
        if current_checkpoints:
            latest_checkpoint = recovered_generations[plan_revision]
            if latest_checkpoint["root_revision"] > event_log.revision:
                raise ResumeError(
                    f"child_checkpoint_ahead_of_material_event_log:{token}"
                )
        child["issue_next_action"] = child.get("next_action")
        child["material_events"] = events
        child["material_event_revision"] = event_log.revision
        child["checkpoint_history"] = checkpoints
        child["latest_checkpoint"] = latest_checkpoint
        child["checkpoint_recovery"] = {
            "state": (
                "current" if current_checkpoints
                else "stale_plan" if stale_checkpoint_count
                else "not_found"
            ),
            "stale_plan_count": stale_checkpoint_count,
        }
        child["next_action"] = _resolved_next_action(
            events, child.get("next_action"), latest_checkpoint,
        )
        child["blocker"] = _resolved_blocker(
            events, child.get("blocker"), latest_checkpoint,
        )
        result["children"].append(child)
    result.pop("child_comments", None)
    return result


def add_live_child_material_history(
    snapshot: dict[str, Any], *, authenticated_route: dict[str, str],
    root_comments: list[dict[str, Any]],
    proposal_plan_revision: str | None = None,
) -> dict[str, Any]:
    """Join the transport's complete nonterminal-child comment collection.

    ``LinearGraphQLTransport.snapshot_for_root(include_child_comments=True)``
    is the single transport boundary for ordinary resume and every strict
    projection validation. Requiring the paired root comments prevents those
    consumers from omitting child-mutation authorizations while joining child
    material and checkpoint authority.
    """
    return add_child_material_history(
        snapshot, snapshot.get("child_comments"),
        authenticated_route=authenticated_route, root_comments=root_comments,
        proposal_plan_revision=proposal_plan_revision,
    )


def _uncheckpointed_material_obligations(
    events: list[dict[str, Any]], checkpoint_revision: int,
) -> list[dict[str, Any]]:
    """Preserve unresolved intent after the last acknowledged checkpoint.

    Evidence and progress remain available through the history digest/full audit
    mode. Requirements, blockers, follow-ups, and decisions cannot be replaced
    by a digest because a fresh agent must be able to act on their exact text.
    """
    result: list[dict[str, Any]] = []

    def append(event: dict[str, Any], kind: str, payload: dict[str, Any]) -> None:
        lowered = kind.lower()
        selected = payload if any(term in lowered for term in MATERIAL_OBLIGATION_TERMS) else {
            key: value for key, value in payload.items() if key in MATERIAL_OBLIGATION_KEYS
        }
        if selected:
            result.append({
                "event_id": event["event_id"], "kind": kind, "payload": selected,
            })

    for index, event in enumerate(events):
        # expected_revision is the writer's observation and may be shared by
        # concurrent appenders. The canonical reduced-log position is what a
        # checkpoint root_revision actually fences.
        if index < checkpoint_revision:
            continue
        if event["kind"] == "material_semantic_repair":
            continue
        append(event, event["kind"], event["payload"])
        if event["kind"] == "material_boundary":
            for change in event["payload"]["changes"]:
                append(event, change["kind"], change["payload"])
    return result


def _compact_stale_plan_obligations(
    events: list[dict[str, Any]], checkpoint_history: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fence acknowledged predecessor intent without granting stale authority.

    A stale checkpoint cannot supply current status or next action, but its
    root revision still proves which prefix of the same validated child ledger
    was acknowledged.  Digest that acknowledged prefix and keep every event
    after the fence exact.  The native child graph remains current execution
    authority, and full-history mode remains the lossless audit surface.
    """
    checkpoint_revision = max(
        (checkpoint["root_revision"] for checkpoint in checkpoint_history),
        default=0,
    )
    if checkpoint_revision > len(events):
        raise ResumeError(
            "child_stale_checkpoint_ahead_of_material_event_log:"
            f"{checkpoint_revision}>{len(events)}"
        )
    acknowledged = _uncheckpointed_material_obligations(
        events[:checkpoint_revision], 0,
    )
    obligations = _uncheckpointed_material_obligations(
        events, checkpoint_revision,
    )
    encoded = json.dumps(
        acknowledged, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()
    return obligations, {
        "checkpoint_root_revision": checkpoint_revision,
        "acknowledged_count": len(acknowledged),
        "uncheckpointed_count": len(obligations),
        "sha256": hashlib.sha256(encoded).hexdigest(),
    }


def _compact_checkpoint(checkpoint: dict[str, Any] | None) -> dict[str, Any] | None:
    if checkpoint is None:
        return None
    evidence = checkpoint["evidence"]
    provenance = checkpoint["provenance_chain"]
    result = {
        key: checkpoint[key]
        for key in (
            "workstream_id", "checkpoint_event_id", "root_revision",
            "plan_revision", "status", "exact_head", "next_action",
            "worktree", "acknowledgement",
        )
    }
    result["evidence"] = {
        "count": len(evidence),
        "sha256": hashlib.sha256(json.dumps(
            evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
    }
    latest_provenance = provenance[-1]
    result["provenance"] = {
        "count": len(provenance),
        "latest": {
            key: latest_provenance[key]
            for key in (
                "event_id", "agent", "provider", "machine", "session_id",
                "worktree",
            )
            if latest_provenance.get(key) is not None
        },
        "sha256": hashlib.sha256(json.dumps(
            provenance, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
    }
    return result


def _checkpoint_item_count(checkpoint: dict[str, Any] | None) -> int:
    """Count variable-length checkpoint collections present in resume output."""
    if checkpoint is None:
        return 0
    evidence = checkpoint.get("evidence", [])
    if isinstance(evidence, dict):
        evidence_count = evidence.get("count", len(evidence.get("items", [])))
    else:
        evidence_count = len(evidence) if isinstance(evidence, list) else 0
    provenance = checkpoint.get(
        "provenance_chain", checkpoint.get("provenance", []),
    )
    if isinstance(provenance, dict):
        provenance_count = provenance.get("count", 0)
    else:
        provenance_count = len(provenance) if isinstance(provenance, list) else 0
    return (
        evidence_count if isinstance(evidence_count, int) else 0
    ) + (provenance_count if isinstance(provenance_count, int) else 0)


def _checkpoint_history_item_count(checkpoints: list[dict[str, Any]]) -> int:
    """Count checkpoint records and every evidence item emitted with them."""
    return len(checkpoints) + sum(
        len(checkpoint.get("evidence", []))
        for checkpoint in checkpoints
    )


def _compact_scope(scope: dict[str, Any] | None) -> dict[str, Any] | None:
    if scope is None:
        return None
    linear = scope["linear"]
    repositories = []
    for repository in scope["repositories"]:
        repositories.append({
            key: repository[key]
            for key in (
                "slug", "aliases", "exact_head", "provider_repository_id",
            )
            if key in repository
        })
    return {
        "namespace": scope["namespace"],
        "linear": {
            key: linear[key]
            for key in ("workspace_id", "team_id", "project_id", "root_issue_id")
        },
        "primary_repository": scope["primary_repository"],
        "repositories": repositories,
        "child_ownership": scope["child_ownership"],
        "validated_sha256": hashlib.sha256(json.dumps(
            scope, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
    }


def _active_projection_heads(
    projection_events: list[dict[str, Any]], kind: str,
) -> list[dict[str, str]]:
    active: dict[str, dict[str, str]] = {}
    for event in projection_events:
        if event.get("kind") != kind:
            continue
        key = event.get("key")
        if not isinstance(key, str):
            continue
        if event.get("value") == TOMBSTONE:
            active.pop(key, None)
            continue
        active[key] = {
            "key": key,
            "event_id": event["event_id"],
            "value_sha256": canonical_digest(event["value"]),
        }
    return list(active.values())


def _projection_head_for_value(
    value: dict[str, Any], active_heads: list[dict[str, str]], label: str,
) -> dict[str, str] | None:
    digest = canonical_digest(value)
    matches = [head for head in active_heads if head["value_sha256"] == digest]
    if len(matches) > 1:
        raise ResumeError(f"{label}_projection_head_ambiguous")
    return matches[0] if matches else None


def _compact_evidence_contracts(
    contracts: list[dict[str, Any]], projection_events: list[dict[str, Any]],
    *, require_projection_authority: bool,
) -> list[dict[str, Any]]:
    """Return digest-bound routing facts without embedding receipt prose.

    Full contracts have already passed projection, scope, exact-head, and
    terminal-closure validation before this function runs.  Default resume
    needs their stable identity and authority bindings, not every receipt body.
    ``--include-history`` remains the explicit audit surface for those bodies.
    """
    active_heads = _active_projection_heads(projection_events, "evidence_contract")

    result = []
    for contract in contracts:
        summary = {
            key: contract[key]
            for key in (
                "slice_id", "owning_child", "repository_key", "exact_head",
            )
        }
        summary["receipt_count"] = sum(
            len(layer.get("receipts", []))
            for layer in contract["layers"].values()
        )
        summary["contract_sha256"] = canonical_digest(contract)
        summary["evidence_receipts_sha256"] = evidence_receipts_sha256([contract])
        head = _projection_head_for_value(
            contract, active_heads,
            f"evidence_compaction:{contract['slice_id']}",
        )
        if require_projection_authority and head is None:
            raise ResumeError(
                f"evidence_compaction_projection_head_missing:{contract['slice_id']}"
            )
        if head is not None:
            # Do not infer that a projection key equals slice_id.  The exact
            # active event tuple is what closure authority binds.
            summary["projection_head"] = head
        result.append(summary)
    return result


def _compact_child_closures(
    closures: list[dict[str, Any]], projection_events: list[dict[str, Any]],
    *, require_projection_authority: bool,
) -> list[dict[str, Any]]:
    active_heads = _active_projection_heads(projection_events, "child_closure")
    result = []
    for closure in closures:
        summary = {
            key: closure[key]
            for key in (
                "child_identifier", "repository_key", "exact_head",
                "assignee_id", "state_name", "state_type", "child_readback_sha256",
            )
        }
        summary["evidence_head_count"] = len(closure["evidence_heads"])
        summary["evidence_heads_sha256"] = canonical_digest(
            closure["evidence_heads"]
        )
        head = _projection_head_for_value(
            closure, active_heads,
            f"child_closure_compaction:{closure['child_identifier']}",
        )
        if require_projection_authority and head is None:
            raise ResumeError(
                "child_closure_compaction_projection_head_missing:"
                + closure["child_identifier"]
            )
        if head is not None:
            summary["projection_head"] = head
        result.append(summary)
    return result


def _compact_description(description: Any) -> dict[str, Any] | None:
    if not isinstance(description, str):
        return None
    encoded = description.encode("utf-8")
    return {
        "bytes": len(encoded),
        "sha256": hashlib.sha256(encoded).hexdigest(),
    }


def _compact_child(child: dict[str, Any]) -> dict[str, Any]:
    result = {
        key: child[key]
        for key in (
            "id", "identifier", "title", "url", "status", "status_type",
            "state_id", "assignee", "owner", "updatedAt", "next_action",
            "blocker", "review_condition", "material_event_revision",
            "checkpoint_recovery", "uncheckpointed_material_obligations",
            "pending_child_proposals", "reconciliation_blockers",
        )
        if key in child
    }
    description = _compact_description(child.get("description"))
    if description is not None:
        result["description_summary"] = description
    return result


def _compact_provenance(
    items: list[dict[str, Any]], projection_events: list[dict[str, Any]],
) -> dict[str, Any]:
    encoded = json.dumps(
        items, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()
    candidates = [
        item for item in items
        if isinstance(item, dict) and item.get("worktree")
    ]
    latest = candidates[0] if len(candidates) == 1 else None
    head = (
        _projection_head_for_value(
            latest, _active_projection_heads(projection_events, "provenance"),
            "provenance_compaction",
        )
        if latest is not None else None
    )
    return {
        "count": len(items),
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "worktree_authority_count": len(candidates),
        "worktree_authority_ambiguous": len(candidates) > 1,
        "latest": ({
            key: latest[key]
            for key in ("agent", "machine", "session_id", "worktree")
            if latest.get(key) is not None
        } if latest is not None else None),
        "latest_projection_head": head,
    }


def add_material_history(
    snapshot: dict[str, Any], comments: list[dict[str, Any]], token: str,
    *, authenticated_route: dict[str, str] | None = None,
    authenticated_source: dict[str, Any] | None = None,
    permit_stale_lifecycle_for_reconcile: bool = False,
    relation_target_resolver: (
        Callable[[list[dict[str, Any]]], dict[str, dict[str, Any]]] | None
    ) = None,
) -> dict[str, Any]:
    """Join one complete Linear comment read to the issue-graph snapshot."""
    result = dict(snapshot)
    result["root"] = dict(snapshot.get("root") or {})
    from workstream_generation import generation_quarantine_metadata

    result["root"]["quarantined_legacy_writes"] = generation_quarantine_metadata(
        comments, workstream_id=token,
    )
    event_log = reduce_event_comments(comments, workstream_id=token)
    plan_revision = result["root"].get("plan_revision")
    projection_log = reduce_projection_comments(
        comments, workstream_id=token, expected_plan_revision=plan_revision,
        authenticated_route=authenticated_route,
        authenticated_source=authenticated_source,
    )
    selected_checkpoints = None
    transition_tip = result["root"].get("generation_transition_tip_event_id")
    if transition_tip is not None:
        if authenticated_route is None:
            raise ResumeError("generation_activation_checkpoint_route_missing")
        from workstream_generation import selected_activation_checkpoints

        selected_checkpoints = selected_activation_checkpoints(
            comments, workstream_id=token,
            transition_event_id=transition_tip,
            active_plan_revision=plan_revision,
            authenticated_route=authenticated_route,
        )
    checkpoint_log = reduce_checkpoint_comments(
        comments, workstream_id=token,
        selected_activation_checkpoints=selected_checkpoints,
    )
    projected_relations = projection_log.snapshot.get("relations") or []
    repair_relation_targets = (
        relation_target_resolver(projected_relations)
        if projected_relations and relation_target_resolver is not None else {}
    )
    issue_graph_frontier = _issue_graph_repair_frontier(
        result, projected_relations, repair_relation_targets,
    )
    if any(event.kind == "material_semantic_repair" for event in event_log.events):
        if authenticated_route is None:
            raise ResumeError("material_semantic_repair_authority_missing")
        try:
            repair_control = next(
                event for event in event_log.events
                if event.kind == "material_semantic_repair"
            )
            repair_payload = repair_control.payload
            bound_checkpoint_count = (
                repair_payload.get("checkpoint_frontier", {}).get("count")
            )
            bound_projection_revision = (
                repair_payload.get("projection_frontier", {}).get("revision")
            )
            ledger_frontier = ledger_serialization_frontier(
                sorted(item["event_id"] for item in checkpoint_log.checkpoints),
                comments, workstream_id=token,
                authenticated_route=authenticated_route,
                current_plan_revision=plan_revision,
                material_revision=repair_control.expected_revision,
            )
            event_log = apply_material_semantic_repairs(
                event_log, comments,
                checkpoint_frontier=_checkpoint_repair_frontier(
                    checkpoint_log, count=bound_checkpoint_count,
                ),
                projection_frontier=_projection_repair_frontier(
                    projection_log, revision=bound_projection_revision,
                ),
                generation=_generation_repair_binding(result["root"]),
                authenticated_route=authenticated_route,
                authenticated_source=authenticated_source or {},
                issue_graph_frontier=issue_graph_frontier,
                ledger_serialization_frontier_value=ledger_frontier,
                validate_live_fences=False,
            )
        except LinearEventError as error:
            raise ResumeError(str(error)) from error
    else:
        # Full-authority resume must not interpret malformed historical
        # boundaries until a later, fully bound repair control exists.
        try:
            event_log = apply_material_semantic_repairs(
                event_log, comments,
                checkpoint_frontier={}, projection_frontier={}, generation={},
                authenticated_route={}, authenticated_source={},
                issue_graph_frontier={},
                ledger_serialization_frontier_value=[],
            )
        except LinearEventError as error:
            raise ResumeError(str(error)) from error
    events = [_event_record(event) for event in event_log.events]

    result["material_events"] = events
    result["raw_material_events"] = [
        _event_record(event) for event in (event_log.raw_events or event_log.events)
    ]
    result["material_semantic_repairs"] = list(event_log.repair_bindings)
    result["material_event_revision"] = event_log.revision
    result.update(projection_log.snapshot)
    relations = result.get("relations") or []
    if relations and relation_target_resolver is not None:
        result["relation_targets"] = repair_relation_targets
    result["authenticated_route"] = dict(authenticated_route) if authenticated_route else None
    result["authenticated_source"] = (
        dict(authenticated_source) if authenticated_source else None
    )
    result["root"]["issue_revision"] = result["root"].get("revision", 0)
    result["root"]["revision"] = event_log.revision
    result["latest_checkpoint"] = None
    checkpoints = list(checkpoint_log.checkpoints)
    recovered_generations = _recover_checkpoint_generations(
        checkpoints, token, error_prefix="invalid_checkpoint_history",
    )
    current_checkpoints = [
        checkpoint for checkpoint in checkpoint_log.checkpoints
        if checkpoint["plan_revision"] == result["root"].get("plan_revision")
    ]
    stale_checkpoint_count = len(checkpoint_log.checkpoints) - len(current_checkpoints)
    result["checkpoint_recovery"] = {
        "state": (
            "current" if current_checkpoints
            else "stale_plan" if stale_checkpoint_count
            else "not_found"
        ),
        "stale_plan_count": stale_checkpoint_count,
    }
    if current_checkpoints:
        result["latest_checkpoint"] = recovered_generations[
            result["root"].get("plan_revision")
        ]
        if result["latest_checkpoint"]["root_revision"] > event_log.revision:
            raise ResumeError("checkpoint_ahead_of_material_event_log")
    result["root"]["next_action"] = _resolved_next_action(
        events, result["root"].get("next_action"), result["latest_checkpoint"],
    )
    lifecycle = projection_log.snapshot.get("lifecycle")
    if lifecycle is not None:
        observed_snapshot_sha256 = closure_snapshot_digest(result)
        if lifecycle.get("snapshot_sha256") != observed_snapshot_sha256:
            unresolved_quarantine = result.get("projection_unresolved_quarantine") or []
            if not permit_stale_lifecycle_for_reconcile and not unresolved_quarantine:
                raise ResumeError("lifecycle_snapshot_stale_reconcile_required")
            result["lifecycle_recovery"] = {
                "state": (
                    "blocked_unresolved_quarantine"
                    if unresolved_quarantine else "stale_snapshot"
                ),
                "lifecycle_snapshot_sha256": lifecycle.get("snapshot_sha256"),
                "observed_snapshot_sha256": observed_snapshot_sha256,
            }
            if unresolved_quarantine:
                result["root"]["next_action"] = (
                    "Review and disposition quarantined v1 projection events"
                )
        else:
            result["root"]["issue_status"] = result["root"].get("status")
            result["root"]["status"] = lifecycle["status"]
            result["root"]["closure_receipt"] = lifecycle.get("closure_receipt_sha256")
    return result


def extract_token(value: str) -> str:
    """Resolve one distinct workstream token from a token, URL, or tab title."""
    tokens = {match.group(0).upper() for match in TOKEN.finditer(value or "")}
    if not tokens:
        raise ResumeError("missing_workstream_token")
    if len(tokens) != 1:
        raise ResumeError("multiple_workstream_tokens:" + ",".join(sorted(tokens)))
    return next(iter(tokens))


def validate_snapshot(
    snapshot: dict[str, Any], token: str | None = None, *,
    require_projection_authority: bool = False,
    expected_missing_terminal_closures: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    if expected_missing_terminal_closures and not require_projection_authority:
        raise ResumeError(
            "expected_missing_terminal_closures_requires_projection_authority"
        )
    source_root = snapshot.get("root")
    if not isinstance(source_root, dict):
        raise ResumeError("missing root")
    root = dict(source_root)
    identifier = root.get("identifier") or root.get("id")
    if not isinstance(identifier, str) or not TOKEN.fullmatch(identifier.upper()):
        raise ResumeError("root must contain one Linear issue identifier")
    if len(identifier.encode("utf-8")) > MAX_WORKSTREAM_IDENTIFIER_BYTES:
        raise ResumeError("workstream identifier exceeds schema byte limit")
    if token and identifier.upper() != extract_token(token):
        raise ResumeError("token/root mismatch")
    for field in ("url", "plan_revision", "revision"):
        if field not in root or root[field] in (None, ""):
            raise ResumeError(f"root missing {field}")
    if (
        not isinstance(root["plan_revision"], str)
        or len(root["plan_revision"].encode("utf-8")) > MAX_PLAN_REVISION_BYTES
    ):
        raise ResumeError("plan revision exceeds schema byte limit")
    if (
        type(root["revision"]) is not int
        or not 0 <= root["revision"] <= MAX_REVISION
    ):
        raise ResumeError("root revision must be a non-negative 64-bit integer")
    if "issue_revision" in root and (
        type(root["issue_revision"]) is not int
        or not 0 <= root["issue_revision"] <= MAX_REVISION
    ):
        raise ResumeError("issue revision must be a non-negative 64-bit integer")
    root_next_action = root.get("next_action")
    if "next_action" in root and (
        not isinstance(root_next_action, str) or not root_next_action.strip()
    ):
        raise ResumeError("invalid_root_next_action")
    if not _is_terminal(root) and not root_next_action:
        raise ResumeError("nonterminal root missing next_action")
    events_present = "material_events" in snapshot
    revision_present = "material_event_revision" in snapshot
    if events_present != revision_present:
        raise ResumeError("material_event_surface_incomplete")
    material_events = snapshot.get("material_events", [])
    if not isinstance(material_events, list):
        raise ResumeError("material_events must be a list")
    event_ids: set[str] = set()
    for index, event in enumerate(material_events):
        if not isinstance(event, dict):
            raise ResumeError(f"invalid_material_event:{index}")
        required = {
            "event_id", "workstream_id", "kind", "source", "payload",
            "expected_revision", "created_at",
        }
        if set(event) != required:
            raise ResumeError(f"invalid_material_event_fields:{index}")
        if event["workstream_id"] != identifier.upper():
            raise ResumeError(f"material_event_workstream_mismatch:{index}")
        if not all(
            isinstance(event[field], str) and event[field]
            for field in ("event_id", "kind", "source", "created_at")
        ) or not isinstance(event["payload"], dict):
            raise ResumeError(f"invalid_material_event:{index}")
        if event["event_id"] in event_ids:
            raise ResumeError(f"duplicate_material_event:{event['event_id']}")
        event_ids.add(event["event_id"])
        expected = event["expected_revision"]
        if not isinstance(expected, int) or expected < 0 or expected > index:
            raise ResumeError(f"invalid_material_event_revision:{event['event_id']}")
        _event_next_actions(event)
    material_revision = snapshot.get("material_event_revision")
    if events_present and (
        material_revision != len(material_events) or root["revision"] != material_revision
    ):
        raise ResumeError("material_event_revision_mismatch")
    latest_checkpoint = snapshot.get("latest_checkpoint")
    checkpoint_recovery = snapshot.get("checkpoint_recovery")
    if checkpoint_recovery is not None:
        if (
            not isinstance(checkpoint_recovery, dict)
            or set(checkpoint_recovery) != {"state", "stale_plan_count"}
            or checkpoint_recovery.get("state") not in {"current", "stale_plan", "not_found"}
            or not isinstance(checkpoint_recovery.get("stale_plan_count"), int)
            or checkpoint_recovery["stale_plan_count"] < 0
        ):
            raise ResumeError("invalid_checkpoint_recovery")
        if checkpoint_recovery["state"] == "current" and latest_checkpoint is None:
            raise ResumeError("current_checkpoint_missing")
        if checkpoint_recovery["state"] != "current" and latest_checkpoint is not None:
            raise ResumeError("unexpected_latest_checkpoint")
    if latest_checkpoint is not None:
        if not isinstance(latest_checkpoint, dict):
            raise ResumeError("latest_checkpoint must be an object or null")
        if latest_checkpoint.get("workstream_id") != identifier.upper():
            raise ResumeError("checkpoint_workstream_mismatch")
        if latest_checkpoint.get("plan_revision") != root["plan_revision"]:
            raise ResumeError("checkpoint_plan_drift")
        required_checkpoint = {
            "workstream_id", "checkpoint_event_id", "root_revision", "plan_revision",
            "status", "exact_head", "evidence", "blocker", "next_action", "worktree",
            "acknowledgement", "provenance_chain",
        }
        if set(latest_checkpoint) != required_checkpoint:
            raise ResumeError("invalid_latest_checkpoint_fields")
        checkpoint_revision = latest_checkpoint.get("root_revision")
        acknowledgement = latest_checkpoint.get("acknowledgement")
        provenance_chain = latest_checkpoint.get("provenance_chain")
        if (
            not isinstance(checkpoint_revision, int)
            or checkpoint_revision < 0
            or checkpoint_revision > root["revision"]
            or not isinstance(latest_checkpoint.get("next_action"), str)
            or not latest_checkpoint["next_action"].strip()
            or not isinstance(latest_checkpoint.get("evidence"), list)
            or not isinstance(latest_checkpoint.get("worktree"), dict)
            or not isinstance(acknowledgement, dict)
            or acknowledgement.get("state") != "remote_acknowledged"
            or not isinstance(acknowledgement.get("remote_id"), str)
            or not acknowledgement["remote_id"]
            or not isinstance(acknowledgement.get("applied_revision"), int)
            or acknowledgement["applied_revision"] < checkpoint_revision
            or not isinstance(provenance_chain, list)
            or not provenance_chain
            or provenance_chain[-1].get("event_id")
            != latest_checkpoint.get("checkpoint_event_id")
            or provenance_chain[-1].get("worktree") != latest_checkpoint["worktree"]
        ):
            raise ResumeError("invalid_latest_checkpoint")
    root["next_action"] = _resolved_next_action(
        material_events, root.get("next_action"), latest_checkpoint,
    )
    if not _is_terminal(root) and not root.get("next_action"):
        raise ResumeError("nonterminal root missing next_action")
    children = snapshot.get("children")
    if not isinstance(children, list):
        raise ResumeError("children must be a list")
    keys: set[str] = set()
    for child in children:
        if not isinstance(child, dict) or not child.get("identifier") or not child.get("title"):
            raise ResumeError("every child needs identifier and title")
        key = str(child["identifier"]).upper()
        if key in keys:
            raise ResumeError(f"duplicate child:{key}")
        keys.add(key)
        if not _is_terminal(child) and not child.get("next_action"):
            raise ResumeError(f"nonterminal child missing next_action:{key}")
        child_events_present = "material_events" in child
        child_revision_present = "material_event_revision" in child
        if child_events_present != child_revision_present:
            raise ResumeError(f"child_material_event_surface_incomplete:{key}")
        if child_events_present:
            checkpoint_history = child.get("checkpoint_history")
            if checkpoint_history is not None:
                if not isinstance(checkpoint_history, list):
                    raise ResumeError(f"invalid_child_checkpoint_history:{key}")
                checkpoint_keys: list[tuple[int, str]] = []
                for checkpoint in checkpoint_history:
                    if not isinstance(checkpoint, dict):
                        raise ResumeError(f"invalid_child_checkpoint_history:{key}")
                    try:
                        validate_checkpoint(checkpoint)
                    except CheckpointError as error:
                        raise ResumeError(
                            f"invalid_child_checkpoint_history:{key}:{error}"
                        ) from error
                    if (
                        checkpoint["workstream_id"] != key
                        or checkpoint["acknowledgement"]["state"]
                        != "remote_acknowledged"
                    ):
                        raise ResumeError(f"invalid_child_checkpoint_history:{key}")
                    checkpoint_keys.append(
                        (checkpoint["root_revision"], checkpoint["event_id"])
                    )
                if checkpoint_keys != sorted(checkpoint_keys):
                    raise ResumeError(f"unordered_child_checkpoint_history:{key}")
                recovered_generations = _recover_checkpoint_generations(
                    checkpoint_history, key,
                    error_prefix="invalid_child_checkpoint_history",
                )
                current_checkpoints = [
                    checkpoint for checkpoint in checkpoint_history
                    if checkpoint["plan_revision"] == root["plan_revision"]
                ]
                recovery = child.get("checkpoint_recovery") or {}
                if recovery.get("stale_plan_count") != (
                    len(checkpoint_history) - len(current_checkpoints)
                ):
                    raise ResumeError(f"child_checkpoint_history_count_mismatch:{key}")
                if current_checkpoints:
                    recovered = recovered_generations[root["plan_revision"]]
                    if recovered != child.get("latest_checkpoint"):
                        raise ResumeError(f"child_checkpoint_history_tip_mismatch:{key}")
                elif child.get("latest_checkpoint") is not None:
                    raise ResumeError(f"child_checkpoint_history_tip_mismatch:{key}")
            child_root = {
                "identifier": key,
                "url": child.get("url"),
                "plan_revision": root["plan_revision"],
                "revision": child.get("material_event_revision"),
                "status": child.get("status"),
                "next_action": child.get("next_action"),
            }
            validate_snapshot({
                "root": child_root,
                "children": [],
                "material_events": child.get("material_events"),
                "material_event_revision": child.get("material_event_revision"),
                "latest_checkpoint": child.get("latest_checkpoint"),
                "checkpoint_recovery": child.get("checkpoint_recovery"),
            }, key)
    choice_events = snapshot.get("choice_events", [])
    if not isinstance(choice_events, list):
        raise ResumeError("choice_events must be a list")
    projection_events = snapshot.get("projection_events", [])
    projection_history = snapshot.get("projection_history", [])
    projection_quarantined = snapshot.get("projection_quarantined", [])
    projection_unresolved_quarantine = snapshot.get(
        "projection_unresolved_quarantine", projection_quarantined,
    )
    projection_revision = snapshot.get("projection_revision")
    if not isinstance(projection_events, list):
        raise ResumeError("projection_events must be a list")
    if not isinstance(projection_history, list):
        raise ResumeError("projection_history must be a list")
    if not isinstance(projection_quarantined, list):
        raise ResumeError("projection_quarantined must be a list")
    if not isinstance(projection_unresolved_quarantine, list):
        raise ResumeError("projection_unresolved_quarantine must be a list")
    if projection_revision is not None and projection_revision != len(projection_events):
        raise ResumeError("projection_revision_mismatch")
    for index, event in enumerate(projection_history):
        try:
            validate_projection_event(event)
        except LinearProjectionError as error:
            raise ResumeError(f"invalid_projection_event:{index}:{error}") from error
        if event["workstream_id"] != identifier.upper():
            raise ResumeError(f"projection_workstream_mismatch:{index}")
    if any(event["plan_revision"] == root["plan_revision"] for event in projection_history):
        raise ResumeError("projection_stale_history_contains_current_generation")
    for index, event in enumerate(projection_quarantined):
        try:
            validate_projection_event(event)
        except LinearProjectionError as error:
            raise ResumeError(f"invalid_quarantined_projection_event:{index}:{error}") from error
        if (
            event["workstream_id"] != identifier.upper()
            or event["plan_revision"] != root["plan_revision"]
            or event["schema_version"] != 1
        ):
            raise ResumeError(f"invalid_quarantined_projection_event:{index}")
    quarantined_by_id = {
        event["event_id"]: event for event in projection_quarantined
    }
    unresolved_ids = {
        event.get("event_id") for event in projection_unresolved_quarantine
        if isinstance(event, dict)
    }
    if (
        len(unresolved_ids) != len(projection_unresolved_quarantine)
        or not unresolved_ids.issubset(quarantined_by_id)
    ):
        raise ResumeError("invalid_projection_unresolved_quarantine")
    quarantine_disposition = snapshot.get("quarantine_disposition")
    retired_ids = set()
    if quarantine_disposition is not None:
        retired_ids = set(quarantine_disposition.get("event_ids") or [])
        if not retired_ids.issubset(quarantined_by_id):
            raise ResumeError("invalid_quarantine_disposition")
        reviewed_events = [quarantined_by_id[event_id] for event_id in sorted(retired_ids)]
        if quarantine_disposition.get("events_sha256") != hashlib.sha256(json.dumps(
            reviewed_events, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest():
            raise ResumeError("invalid_quarantine_disposition")
    if unresolved_ids != set(quarantined_by_id) - retired_ids:
        raise ResumeError("invalid_projection_unresolved_quarantine")
    modern_seen = False
    for index, event in enumerate(projection_events):
        if event["plan_revision"] != root["plan_revision"]:
            raise ResumeError(f"projection_plan_drift:{index}")
        if event["schema_version"] == 2:
            modern_seen = True
        if (
            (modern_seen and event["expected_revision"] != index)
            or (not modern_seen and event["expected_revision"] > index)
        ):
            raise ResumeError(f"projection_revision_mismatch:{index}")
    projection_recovery = snapshot.get("projection_recovery")
    if projection_recovery is not None and (
        not isinstance(projection_recovery, dict)
        or set(projection_recovery) != {"state", "stale_plan_count"}
        or projection_recovery.get("state") not in {"current", "stale_plan", "not_found"}
        or not isinstance(projection_recovery.get("stale_plan_count"), int)
        or projection_recovery["stale_plan_count"] < 0
    ):
        raise ResumeError("invalid_projection_recovery")
    lifecycle_recovery = snapshot.get("lifecycle_recovery")
    if lifecycle_recovery is not None and (
        not isinstance(lifecycle_recovery, dict)
        or set(lifecycle_recovery) != {
            "state", "lifecycle_snapshot_sha256", "observed_snapshot_sha256",
        }
        or lifecycle_recovery.get("state") not in {
            "stale_snapshot", "blocked_unresolved_quarantine",
        }
        or not all(
            isinstance(lifecycle_recovery.get(field), str)
            and re.fullmatch(r"[0-9a-f]{64}", lifecycle_recovery[field])
            for field in ("lifecycle_snapshot_sha256", "observed_snapshot_sha256")
        )
    ):
        raise ResumeError("invalid_lifecycle_recovery")
    authenticated_route = snapshot.get("authenticated_route")
    route_fields = {
        "workspace_id", "team_id", "project_id", "root_issue_id",
    }
    if authenticated_route is not None and (
        not isinstance(authenticated_route, dict)
        or set(authenticated_route) != route_fields
        or not all(
            isinstance(authenticated_route[field], str)
            and authenticated_route[field]
            for field in route_fields
        )
    ):
        raise ResumeError("invalid_authenticated_route")
    active: dict[tuple[str, str], dict[str, Any]] = {}
    if projection_events:
        if authenticated_route is None:
            raise ResumeError("projection_authenticated_route_missing")
        if projection_recovery.get("state") != "current":
            raise ResumeError("projection_not_current")
        if projection_recovery["stale_plan_count"] != len(projection_history):
            raise ResumeError("projection_stale_plan_count_mismatch")
        if snapshot.get("scope") is None or snapshot.get("source") is None:
            raise ResumeError("projection_authority_missing")
        if not snapshot.get("provenance"):
            raise ResumeError("projection_provenance_missing")
        if snapshot.get("disposition") is None:
            raise ResumeError("projection_disposition_missing")
        heads: dict[tuple[str, str], dict[str, Any]] = {}
        for event in projection_events:
            identity = (event["kind"], event["key"])
            current = heads.get(identity)
            if current is None and event["supersedes_event_id"] is not None:
                raise ResumeError(f"projection_supersedes_missing:{event['event_id']}")
            if current is not None and event["supersedes_event_id"] != current["event_id"]:
                raise ResumeError(f"projection_concurrent_conflict:{event['kind']}:{event['key']}")
            heads[identity] = event
            if event["value"] == TOMBSTONE:
                active.pop(identity, None)
            else:
                active[identity] = event
        for kind, field in (("scope", "scope"), ("source", "source"),
                            ("disposition", "disposition"), ("lifecycle", "lifecycle"),
                            ("quarantine_disposition", "quarantine_disposition")):
            values = [event["value"] for (event_kind, _), event in active.items()
                      if event_kind == kind]
            if kind in {"lifecycle", "quarantine_disposition"} and not values and snapshot.get(field) is None:
                continue
            if len(values) != 1 or values[0] != snapshot.get(field):
                raise ResumeError(f"projection_current_view_mismatch:{field}")
        lifecycle = snapshot.get("lifecycle")
        if lifecycle is not None and (
            root.get("status") != lifecycle["status"]
            or root.get("closure_receipt") != lifecycle.get("closure_receipt_sha256")
        ) and not (
            isinstance(lifecycle_recovery, dict)
            and lifecycle_recovery.get("state") == "blocked_unresolved_quarantine"
            and projection_unresolved_quarantine
        ):
            raise ResumeError("projection_current_view_mismatch:lifecycle_root")
        for kind, field in (("relation", "relations"), ("choice", "choice_events"),
                            ("evidence_contract", "evidence_contracts"),
                            ("child_closure", "child_closures"),
                            ("provenance", "provenance"),
                            ("closure_review", "closure_reviews")):
            values = [event["value"] for (event_kind, _), event in active.items()
                      if event_kind == kind]
            values.sort(key=lambda value: json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ))
            if values != snapshot.get(field):
                raise ResumeError(f"projection_current_view_mismatch:{field}")
        if require_projection_authority:
            disposition = snapshot.get("disposition")
            expected_checkpoint = (
                latest_checkpoint.get("checkpoint_event_id")
                if isinstance(latest_checkpoint, dict) else None
            )
            recovered_checkpoint = (
                disposition.get("recovered_from_checkpoint")
                if isinstance(disposition, dict) else None
            )
            if recovered_checkpoint != expected_checkpoint:
                raise ResumeError(
                    "disposition_checkpoint_stale_reconcile_required"
                )
    dependency_graph = snapshot.get("dependency_graph")
    if require_projection_authority and dependency_graph is None:
        raise ResumeError("authenticated_dependency_graph_missing")
    if dependency_graph is not None:
        if not isinstance(authenticated_route, dict):
            raise ResumeError("dependency_graph_authenticated_route_missing")
        try:
            dependency_graph = validate_authorized_dependency_graph_surface(
                dependency_graph, projection_events,
                authority={
                    **authenticated_route,
                    "root_identifier": identifier.upper(),
                },
                plan_revision=root["plan_revision"],
                expected_frontier={
                    "material_revision": (
                        material_revision
                        if isinstance(material_revision, int)
                        else len(material_events)
                    ),
                    "projection_revision": (
                        projection_revision
                        if isinstance(projection_revision, int)
                        else len(projection_events)
                    ),
                    "graph_revision": dependency_graph.get("revision"),
                    "graph_sha256": dependency_graph.get("sha256"),
                },
                expected_root_readback_sha256=(
                    dependency_root_readback_sha256(root)
                ),
            )
        except ChildDependencyError as error:
            raise ResumeError(str(error)) from error
    try:
        choice_view = reduce_choices(choice_events)
        scope = snapshot.get("scope")
        if scope is not None:
            validate_scope(scope, root_id=identifier.upper(), child_ids=keys)
            if authenticated_route:
                for field in ("workspace_id", "team_id", "project_id", "root_issue_id"):
                    if scope["linear"].get(field) != authenticated_route.get(field):
                        raise ResumeError(f"projection_route_mismatch:{field}")
        source = snapshot.get("source")
        if source is not None:
            if not isinstance(source, dict) or source.get("sha256") != root["plan_revision"]:
                raise ResumeError("projection_source_plan_mismatch")
            authenticated_source = snapshot.get("authenticated_source")
            if authenticated_source is not None:
                source_identity = source.get("identity") or source.get("url")
                if source_identity != authenticated_source.get("identity"):
                    raise ResumeError("projection_source_identity_mismatch")
                if source.get("sha256") != authenticated_source.get("sha256"):
                    raise ResumeError("projection_source_bytes_mismatch")
        relations = snapshot.get("relations", [])
        validate_relations(
            relations, root_id=identifier.upper(),
            workspace_id=scope.get("linear", {}).get("workspace_id") if scope else None,
            root_issue_id=scope.get("linear", {}).get("root_issue_id") if scope else None,
        )
        evidence_contracts = snapshot.get("evidence_contracts", [])
        if not isinstance(evidence_contracts, list):
            raise ResumeError("evidence_contracts must be a list")
        historical_evidence_event_ids: frozenset[str] = frozenset()
        if scope is not None and projection_events:
            try:
                historical_evidence_event_ids = (
                    closure_bound_historical_evidence(
                        projection_events, scope, projection_history,
                        selected_transition_tip_event_id=root.get(
                            "generation_transition_tip_event_id"
                        ),
                    )
                )
            except ProjectionHistoryError as error:
                raise ResumeError(str(error)) from error
        for index, contract in enumerate(evidence_contracts):
            errors = evidence_errors(contract)
            if errors:
                raise ResumeError(f"invalid_evidence_contract:{index}:" + ",".join(errors))
            if contract.get("plan_revision") != root["plan_revision"]:
                raise ResumeError(f"evidence_plan_drift:{index}")
            if contract.get("owning_child") not in keys:
                raise ResumeError(f"evidence_owner_missing:{index}")
            if scope is not None:
                owned_key = scope["child_ownership"][contract["owning_child"]]
                if contract.get("repository_key") != owned_key:
                    raise ResumeError(f"evidence_repository_mismatch:{index}")
                scoped_repository = next(
                    item for item in scope["repositories"] if repository_key(item) == owned_key
                )
                if contract.get("repository") not in [scoped_repository["slug"], *scoped_repository.get("aliases", [])]:
                    raise ResumeError(f"evidence_repository_route_unknown:{index}")
                evidence_event = active.get((
                    "evidence_contract", str(contract.get("slice_id", "")),
                ))
                if (
                    contract.get("exact_head") != scoped_repository["exact_head"]
                    and (
                        evidence_event is None
                        or evidence_event.get("event_id")
                        not in historical_evidence_event_ids
                    )
                ):
                    raise ResumeError(f"evidence_head_mismatch:{index}")
        child_closures = snapshot.get("child_closures", [])
        if not isinstance(child_closures, list):
            raise ResumeError("child_closures must be a list")
        child_by_identifier = {
            str(child.get("identifier", "")).upper(): child for child in children
        }
        closure_ids: set[str] = set()
        for index, closure in enumerate(child_closures):
            child_id = str(closure.get("child_identifier", "")).upper()
            if child_id in closure_ids:
                raise ResumeError(f"duplicate_child_closure:{child_id}")
            closure_ids.add(child_id)
            child = child_by_identifier.get(child_id)
            if child is None:
                raise ResumeError(f"child_closure_child_missing:{index}")
            try:
                readback = terminal_child_readback(child)
            except ChildClosureError as error:
                raise ResumeError(f"child_closure_readback_invalid:{index}:{error}") from error
            if (
                canonical_digest(readback) != closure.get("child_readback_sha256")
                or any(closure.get(field) != readback[field] for field in readback)
            ):
                raise ResumeError(f"child_closure_readback_mismatch:{index}")
            if scope is None or scope["child_ownership"].get(child_id) != closure.get("repository_key"):
                raise ResumeError(f"child_closure_ownership_mismatch:{index}")
            scoped_repository = next((
                repository for repository in scope["repositories"]
                if repository_key(repository) == closure.get("repository_key")
            ), None)
            historical_closure = (
                bool(closure.get("evidence_heads"))
                and all(
                    head.get("event_id") in historical_evidence_event_ids
                    for head in closure.get("evidence_heads", [])
                )
            )
            if scoped_repository is None or (
                scoped_repository.get("exact_head") != closure.get("exact_head")
                and not historical_closure
            ):
                raise ResumeError(f"child_closure_repository_mismatch:{index}")
            contracts: list[dict[str, Any]] = []
            current_evidence_heads = [
                {
                    "key": key,
                    "event_id": event["event_id"],
                    "value_sha256": canonical_digest(event["value"]),
                }
                for (kind, key), event in active.items()
                if kind == "evidence_contract"
                and event["value"].get("owning_child") == child_id
            ]
            current_evidence_heads.sort(
                key=lambda item: (item["key"], item["event_id"])
            )
            if current_evidence_heads != closure.get("evidence_heads"):
                raise ResumeError(f"child_closure_evidence_set_mismatch:{index}")
            for head in closure.get("evidence_heads", []):
                event = active.get(("evidence_contract", head.get("key")))
                if (
                    event is None
                    or event.get("event_id") != head.get("event_id")
                    or canonical_digest(event.get("value")) != head.get("value_sha256")
                ):
                    raise ResumeError(f"child_closure_evidence_head_mismatch:{index}")
                contract = event["value"]
                if (
                    contract.get("owning_child") != child_id
                    or contract.get("repository_key") != closure.get("repository_key")
                    or contract.get("exact_head") != closure.get("exact_head")
                    or evidence_errors(contract)
                ):
                    raise ResumeError(f"child_closure_evidence_invalid:{index}")
                contracts.append(contract)
            if evidence_receipts_sha256(contracts) != closure.get("evidence_receipts_sha256"):
                raise ResumeError(f"child_closure_receipts_mismatch:{index}")
        missing_terminal_closures: set[str] = set()
        if projection_events and scope is not None:
            for child_id, child in child_by_identifier.items():
                status_type = str(
                    child.get("status_type") or child.get("status") or ""
                ).lower()
                if (
                    status_type in {"completed", "done"}
                    and child_id in scope["child_ownership"]
                    and child_id not in closure_ids
                ):
                    missing_terminal_closures.add(child_id)
        if missing_terminal_closures != set(expected_missing_terminal_closures):
            if not expected_missing_terminal_closures and missing_terminal_closures:
                raise ResumeError(
                    "completed_owned_child_closure_missing:"
                    + sorted(missing_terminal_closures)[0]
                )
            raise ResumeError(
                "completed_owned_child_closure_set_mismatch:"
                + ",".join(sorted(
                    missing_terminal_closures
                    ^ set(expected_missing_terminal_closures)
                ))
            )
        for choice_id, view in choice_view.items():
            event = view["record"]
            if event["workstream_id"] != identifier.upper():
                raise ResumeError(f"choice_workstream_mismatch:{choice_id}")
            if event["owning_child"] not in keys:
                raise ResumeError(f"choice_owner_missing:{choice_id}")
            if scope is not None:
                if event["namespace"] != scope["namespace"]:
                    raise ResumeError(f"choice_namespace_mismatch:{choice_id}")
                owned_key = scope["child_ownership"][event["owning_child"]]
                if owned_key != event["repository_key"]:
                    raise ResumeError(f"choice_repository_mismatch:{choice_id}")
                scoped_repository = next(
                    item for item in scope["repositories"]
                    if repository_key(item) == owned_key
                )
                if event["repository"] not in [scoped_repository["slug"], *scoped_repository.get("aliases", [])]:
                    raise ResumeError(f"choice_repository_route_unknown:{choice_id}")
        availability = {
            field: "available" if field in snapshot and snapshot.get(field) is not None
            else "transport_unimplemented"
            for field in (
                "scope", "relations", "choice_events", "evidence_contracts",
                "material_events", "dependency_graph",
            )
        }
        availability["child_closures"] = "available"
        availability["latest_checkpoint"] = (
            "available" if "latest_checkpoint" in snapshot else "transport_unimplemented"
        )
    except (ChildDependencyError, ChoiceError, ScopeError) as error:
        raise ResumeError(str(error)) from error
    if require_projection_authority:
        if not projection_events:
            raise ResumeError("projection_authority_absent")
        if snapshot.get("authenticated_source") is None:
            raise ResumeError("projection_source_bytes_unverified")
    repairs = snapshot.get("material_semantic_repairs", [])
    raw_material_events = snapshot.get("raw_material_events", material_events)
    if not isinstance(repairs, list) or not isinstance(raw_material_events, list):
        raise ResumeError("invalid_material_semantic_repair_surface")
    return {"root": root, "children": children, "decisions": snapshot.get("decisions", []),
            "choice_events": choice_events, "scope": scope,
            "relations": relations, "evidence_contracts": evidence_contracts,
            "child_closures": snapshot.get("child_closures", []),
            "surface_availability": availability,
            "provenance": snapshot.get("provenance", []),
            "material_events": material_events,
            "raw_material_events": raw_material_events,
            "material_semantic_repairs": repairs,
            "material_event_revision": material_revision,
            "latest_checkpoint": latest_checkpoint,
            "checkpoint_recovery": checkpoint_recovery,
            "source": snapshot.get("source"),
            "disposition": snapshot.get("disposition"),
            "projection_events": projection_events,
            "projection_history": projection_history,
            "projection_quarantined": projection_quarantined,
            "projection_unresolved_quarantine": projection_unresolved_quarantine,
            "quarantine_disposition": quarantine_disposition,
            "projection_revision": projection_revision,
            "projection_recovery": projection_recovery,
            "lifecycle_recovery": lifecycle_recovery,
            "authenticated_route": authenticated_route,
            "dependency_graph": dependency_graph,
            "authenticated_source": snapshot.get("authenticated_source")}


DEFAULT_RESUME_MAX_BYTES = 24 * 1024

_VERBOSE_CURRENT_TEXT_KEYS = {
    "blocker", "blockers", "body", "cons", "context", "decision",
    "decisions", "description", "detail", "details", "followup",
    "followups", "message", "next_action", "note", "notes", "pros",
    "rationale", "reason", "recommendation", "requirement", "requirements",
    "review_condition", "summary", "text", "title",
}
_CURRENT_DETAIL_EXCERPT_LIMITS = (768, 384, 192, 96, 48)


def _default_output_text(context: dict[str, Any]) -> str:
    """Serialize exactly as ordinary CLI stdout, including its newline."""
    return json.dumps(
        context, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ) + "\n"


def _default_output_bytes(context: dict[str, Any]) -> bytes:
    return _default_output_text(context).encode("utf-8")


def _utf8_head_tail(value: str, limit: int) -> str:
    """Return a deterministic, UTF-8-safe actionable excerpt."""
    marker = " …[audit detail deferred]… "
    if len(value.encode("utf-8")) <= limit:
        return value
    # Character slicing cannot split UTF-8. Tighten until the encoded excerpt
    # fits the selected tier; the fixed marker makes omission unambiguous.
    retained = max(2, limit - len(marker.encode("utf-8")))
    head = retained // 2
    tail = retained - head
    excerpt = value[:head] + marker + value[-tail:]
    while len(excerpt.encode("utf-8")) > limit and (head > 1 or tail > 1):
        if head >= tail and head > 1:
            head -= 1
        elif tail > 1:
            tail -= 1
        excerpt = value[:head] + marker + value[-tail:]
    return excerpt


def _compact_verbose_current_detail(
    context: dict[str, Any], *, excerpt_limit: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Bound verbose current prose without weakening validated authority.

    This runs only after ``validate_snapshot`` has authenticated and checked the
    complete immutable history. Exact identifiers, revisions, routes, digests,
    heads, statuses, and collection membership are never candidates. Long
    current prose retains its actionable head and tail; the omitted exact bytes
    remain available through the explicit full-history audit invocation.
    """
    deferred: list[dict[str, Any]] = []

    def pointer(path: tuple[str, ...]) -> str:
        return "/" + "/".join(
            component.replace("~", "~0").replace("/", "~1")
            for component in path
        )

    def visit(value: Any, path: tuple[str, ...], eligible: bool = False) -> Any:
        if isinstance(value, dict):
            return {
                key: visit(
                    item, (*path, str(key)),
                    str(key).lower() in _VERBOSE_CURRENT_TEXT_KEYS,
                )
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [visit(item, (*path, str(index)), eligible)
                    for index, item in enumerate(value)]
        if not eligible or not isinstance(value, str):
            return value
        encoded = value.encode("utf-8")
        if len(encoded) <= excerpt_limit:
            return value
        excerpt = _utf8_head_tail(value, excerpt_limit)
        deferred.append({
            "path": list(path), "json_pointer": pointer(path),
            "utf8_bytes": len(encoded),
            "sha256": hashlib.sha256(encoded).hexdigest(),
        })
        return excerpt

    compacted = visit(context, ())
    deferred_bytes = sum(item["utf8_bytes"] for item in deferred)
    retained_bytes = sum(
        len(_value_at_path(compacted, item["path"]).encode("utf-8"))
        for item in deferred
    )
    fields_manifest = [{
        "json_pointer": item["json_pointer"],
        "utf8_bytes": item["utf8_bytes"],
        "sha256": item["sha256"],
    } for item in deferred]
    summary = {
        "state": "verbose_current_detail_deferred",
        "algorithm": "utf8-actionable-head-tail-v1",
        "field_count": len(deferred),
        "original_utf8_bytes": deferred_bytes,
        "retained_utf8_bytes": retained_bytes,
        "fields_sha256": hashlib.sha256(json.dumps(
            fields_manifest, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        ).encode()).hexdigest(),
        "fields": fields_manifest,
    }
    return compacted, summary


def _value_at_path(value: Any, path: list[str]) -> Any:
    for component in path:
        value = value[int(component)] if isinstance(value, list) else value[component]
    return value


def _bounded_semantic(value: Any, *, text_limit: int) -> Any:
    """Keep executable semantics from one already-validated current record."""
    semantic_keys = {
        "acknowledgement", "agent", "assignee", "blocker", "branch",
        "checkpoint_event_id", "child_identifier", "decision", "decisions",
        "disposition", "event_id", "exact_head", "followup", "followups",
        "head", "id", "identifier", "key", "kind", "machine", "mode",
        "name", "next_action", "owner", "path", "payload", "reason",
        "recommendation", "recovered_from_checkpoint", "remote_head",
        "repository", "repository_key", "requirement", "requirements",
        "review_condition", "root_revision", "session_id", "state",
        "status", "status_type", "target", "text", "title", "type",
        "url", "workstream_id", "worktree",
    }
    if isinstance(value, str):
        return _utf8_head_tail(value, text_limit)
    if isinstance(value, list):
        return [_bounded_semantic(item, text_limit=text_limit) for item in value]
    if isinstance(value, dict):
        selected = {
            key: _bounded_semantic(item, text_limit=text_limit)
            for key, item in value.items() if str(key).lower() in semantic_keys
        }
        if selected:
            return selected
        # Unknown structured prose has no executable key. Its exact canonical
        # value is still digest-bound by the deferred field manifest.
        return {"deferred_structured_detail": True}
    return value


def _canonical_field_record(path: str, value: Any) -> dict[str, Any]:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()
    record = {
        "json_pointer": path, "utf8_bytes": len(encoded),
        "sha256": hashlib.sha256(encoded).hexdigest(),
    }
    if isinstance(value, (list, dict)):
        record["item_count"] = len(value)
    return record


def _bounded_authority_envelope(
    context: dict[str, Any], *, text_limit: int,
) -> dict[str, Any]:
    """Return a small full-authority execution frontier plus audit locators."""
    scope = context.get("scope") or {}
    ownership = scope.get("child_ownership") or {}
    children = []
    for child in context.get("children", []):
        summary = _bounded_semantic(child, text_limit=text_limit)
        identifier = child.get("identifier") if isinstance(child, dict) else None
        if isinstance(summary, dict) and identifier in ownership:
            summary["repository_key"] = ownership[identifier]
        children.append(summary)
    frontier = {
        "root": _bounded_semantic({
            key: context.get(key) for key in (
                "workstream_id", "status", "next_action", "blocker",
            )
        }, text_limit=text_limit),
        "children": children,
        "obligations": _bounded_semantic(
            context.get("uncheckpointed_material_obligations", []),
            text_limit=text_limit,
        ),
        "decisions": _bounded_semantic(
            context.get("decisions", []), text_limit=text_limit,
        ),
        "choices": _bounded_semantic(
            context.get("choice_events", []), text_limit=text_limit,
        ),
        "dependencies": _bounded_semantic(
            context.get("relations", []), text_limit=text_limit,
        ),
        # Native child ordering is distinct from cross-workstream relations.
        # Keep its already-validated semantic surface in every executable
        # representation; consumers must hydrate before mutation, but should
        # never mistake an omitted graph for an empty graph.
        "child_dependency_graph": deepcopy(context.get("dependency_graph")),
        "checkpoint": _bounded_semantic(
            context.get("latest_checkpoint"), text_limit=text_limit,
        ),
        "disposition": _bounded_semantic(
            context.get("disposition"), text_limit=text_limit,
        ),
    }
    keep = (
        "context_schema", "workstream_id", "context_url", "plan_revision",
        "description_plan_revision", "generation_transition_tip_event_id",
        "generation_activation_epoch", "generation_authority_origin",
        "quarantined_legacy_writes", "root_revision", "issue_revision",
        "status", "material_event_revision", "checkpoint_recovery",
        "surface_availability", "projection_revision", "projection_recovery",
        "lifecycle_recovery", "projection_quarantine", "authenticated_route",
        "authenticated_source", "history", "resume_authority",
    )
    deferred_names = sorted(
        set(context) - set(keep) - {"deferred_audit_detail"}
    )
    deferred = [
        _canonical_field_record("/" + name, context.get(name))
        for name in deferred_names
    ]
    result = {key: context.get(key) for key in keep if key in context}
    result["context_schema"] = dict(result["context_schema"])
    result["context_schema"]["envelope"] = "bounded_authority_v1"
    result["execution_frontier"] = frontier
    result["deferred_audit_detail"] = {
        "state": "bounded_authority_envelope",
        "hydration_required_before_action": True,
        "algorithm": "validated-current-frontier-v1",
        "full_context_sha256": hashlib.sha256(json.dumps(
            context, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
        "fields": deferred,
        "fields_sha256": hashlib.sha256(json.dumps(
            deferred, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
    }
    return result


def _fixed_frontier_authority_envelope(
    context: dict[str, Any], *, token: str,
) -> dict[str, Any]:
    """Hard-bound the complete <=100-item execution frontier.

    Each variable current record maps to one fixed-schema record with at most
    six bounded text slots. Thus cardinality cannot reintroduce an unbounded
    recursive object after the validated item gate has passed.
    """
    limit = 24
    truncated_cell_count = 0

    def brief(value: Any) -> Any:
        nonlocal truncated_cell_count
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if not isinstance(value, str):
            value = json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            )
        encoded = value.encode("utf-8")
        if len(json.dumps(value, ensure_ascii=False).encode("utf-8")) <= limit:
            return value
        truncated_cell_count += 1
        marker = "~#" + hashlib.sha256(encoded).hexdigest()[:8]
        prefix = value
        while prefix and len(json.dumps(
            prefix + marker, ensure_ascii=False,
        ).encode("utf-8")) > limit:
            prefix = prefix[:-1]
        return prefix + marker

    def field(record: Any, *names: str) -> Any:
        if not isinstance(record, dict):
            return None
        for name in names:
            if record.get(name) is not None:
                return record[name]
        return None

    children = [[
        brief(field(item, "identifier", "id")),
        brief(field(item, "status_type", "status")),
        brief(field(item, "owner", "assignee")),
        brief((context.get("scope") or {}).get(
            "child_ownership", {},
        ).get(field(item, "identifier"))),
        brief(field(item, "next_action")),
        brief(field(item, "blocker")),
    ] for item in context.get("children", [])]
    obligations = [[
        ["root", index], None, brief(field(item, "event_id")),
        brief(field(item, "kind")),
        brief(field(item, "payload")),
    ] for index, item in enumerate(
        context.get("uncheckpointed_material_obligations", [])
    )]
    for child_index, child in enumerate(context.get("children", [])):
        child_id = brief(field(child, "identifier", "id"))
        for item_index, item in enumerate(
            child.get("uncheckpointed_material_obligations", [])
        ):
            obligations.append([
                ["child", child_index, item_index], child_id,
                brief(field(item, "event_id")),
                brief(field(item, "kind")), brief(field(item, "payload")),
            ])
        for item_index, item in enumerate(child.get("pending_child_proposals", [])):
            obligations.append([
                ["proposal", child_index, item_index], child_id,
                brief(field(item, "event_id", "proposal_id", "id")),
                "pending_child_proposal", brief(item),
            ])
    decisions = [[
        brief(field(item, "id", "event_id", "key")),
        brief(field(item, "status")),
        brief(field(
            item, "decision", "recommendation", "text", "reason", "payload",
        )),
    ] for item in context.get("decisions", [])]
    choices = [[
        brief(field(item, "choice_id", "id", "event_id", "key")),
        brief(field(item, "status")),
        brief(field(
            item, "decision", "recommendation", "text", "reason", "payload",
        )),
    ] for item in context.get("choice_events", [])]
    dependencies = [[
        brief(field(item, "type", "kind")),
        brief(field(field(item, "target"), "identifier", "issue_id", "id")),
    ] for item in context.get("relations", [])]
    dependency_graph = context.get("dependency_graph") or {}
    child_dependencies = [[
        field(item, "id"),
        field(field(item, "blocker"), "identifier", "issue_id"),
        field(field(item, "blocked"), "identifier", "issue_id"),
    ] for item in dependency_graph.get("relations", [])]
    child_dependency_authority = {
        key: dependency_graph.get(key) for key in (
            "authority", "plan_revision", "route", "revision", "sha256",
            "observed_frontier", "root_readback_sha256",
        )
    }
    child_dependency_authority["authorization_batches_sha256"] = hashlib.sha256(
        json.dumps(
            dependency_graph.get("authorization_batches", []),
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()
    ).hexdigest()
    all_fields = [
        _canonical_field_record("/" + name, value)
        for name, value in sorted(context.items())
        if name != "deferred_audit_detail"
    ]
    root = {
        "status": brief(context.get("status")),
        "next": brief(context.get("next_action")),
        "blocker": brief(context.get("blocker")),
    }
    checkpoint_brief = brief(context.get("latest_checkpoint"))
    disposition_brief = brief(context.get("disposition"))
    route_brief = {
        key: brief(value)
        for key, value in (context.get("authenticated_route") or {}).items()
    }
    source_brief = {
        key: brief(value)
        for key, value in (context.get("authenticated_source") or {}).items()
        if key in {"identity", "sha256"}
    }
    result = {
        "context_schema": {
            "name": "agent-workstream.resume-context", "version": 2,
            "representation": "compact_validated",
            "envelope": "fixed_frontier_authority_v1",
        },
        "workstream_id": token,
        "context_url": brief(context.get("context_url")),
        "plan_revision": context.get("plan_revision"),
        "root_revision": context.get("root_revision"),
        "material_event_revision": context.get("material_event_revision"),
        "resume_authority": context.get("resume_authority"),
        "authority_scope": {
            "history_validation": "complete_authenticated",
            "execution_frontier": "complete_digest_bound_excerpts",
            "item_count": (
                1 + len(children) + len(obligations) + len(decisions)
                + len(choices) + len(dependencies) + len(child_dependencies)
            ),
            "omitted_items_claimed_executable": False,
            "truncated_cell_count": truncated_cell_count,
            "truncated_cell_marker": "~#<sha256-prefix>",
            "truncated_cell_rule": "hydrate selected source row before action",
        },
        "execution_frontier": {
            "root": root, "children": children, "obligations": obligations,
            "decisions": decisions, "choices": choices,
            "dependencies": dependencies,
            "child_dependency_graph": {
                "authority": child_dependency_authority,
                "relations": child_dependencies,
            },
            "columns": {
                "children": ["id", "status", "owner", "repository", "next", "blocker"],
                "obligations": ["source", "child", "id", "kind", "action"],
                "decisions": ["id", "status", "action"],
                "choices": ["id", "status", "action"],
                "dependencies": ["type", "target"],
                "child_dependency_graph.relations": ["id", "blocker", "blocked"],
            },
            "checkpoint": checkpoint_brief,
            "disposition": disposition_brief,
        },
        "authenticated_route": route_brief,
        "authenticated_source": source_brief,
        "deferred_audit_detail": {
            "state": "fixed_frontier_authority_envelope",
            "hydration_required_before_action": True,
            "algorithm": "fixed-six-slot-frontier-v1",
            "fields": all_fields,
            "fields_sha256": hashlib.sha256(json.dumps(
                all_fields, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode()).hexdigest(),
            "full_context_sha256": hashlib.sha256(json.dumps(
                context, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode()).hexdigest(),
            "hydration_selectors": {
                "root": "{status:.status,next_action:.next_action,blocker:.blocker}",
                "children": ".children[<row>]",
                "decisions": ".decisions[<row>]",
                "choices": ".choice_events[<row>]",
                "dependencies": ".relations[<row>]",
                "child_dependency_graph": ".dependency_graph",
                "checkpoint": ".latest_checkpoint",
                "disposition": ".disposition",
            },
            "obligation_selector_rules": {
                "root": ".uncheckpointed_material_obligations[source[1]]",
                "child": ".children[source[1]].uncheckpointed_material_obligations[source[2]]",
                "proposal": ".children[source[1]].pending_child_proposals[source[2]]",
            },
            "hydration_recipe": (
                "resolve audit_route.launcher, append audit_route.args, pipe "
                "its compact_validated JSON to "
                "jq -c '<hydration selector or obligation rule>', "
                "then verify the owning top-level field SHA-256"
            ),
        },
    }
    return result


def compact_context(
    snapshot: dict[str, Any], token: str, max_bytes: int = DEFAULT_RESUME_MAX_BYTES,
    max_items: int = 100, *, require_projection_authority: bool = False,
    include_history: bool = False,
    expected_missing_terminal_closures: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    normalized_token = extract_token(token)
    clean = validate_snapshot(
        snapshot, normalized_token,
        require_projection_authority=require_projection_authority,
        expected_missing_terminal_closures=expected_missing_terminal_closures,
    )
    root = clean["root"]

    def history_summary(
        events: list[dict[str, Any]], *, include_latest: bool = True,
    ) -> dict[str, Any]:
        encoded = json.dumps(
            events, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()
        latest = events[-1] if events else None
        summary = {
            "count": len(events),
            "sha256": hashlib.sha256(encoded).hexdigest(),
        }
        if include_latest:
            summary["latest"] = ({
                key: latest[key]
                for key in ("event_id", "kind", "key", "created_at")
                if latest.get(key) is not None
            } if latest else None)
        return summary

    children = []
    child_material_item_count = 0
    for child in clean["children"]:
        if _is_terminal(child):
            continue
        compact_child = dict(child) if include_history else _compact_child(child)
        child_material_item_count += len(child.get("pending_child_proposals", []))
        if "material_events" in child:
            checkpoint_revision = (
                child["latest_checkpoint"]["root_revision"]
                if child.get("latest_checkpoint") is not None else 0
            )
            obligations = _uncheckpointed_material_obligations(
                child["material_events"], checkpoint_revision,
            )
            obligation_summary = None
            if (
                not include_history
                and child.get("checkpoint_recovery", {}).get("state")
                == "stale_plan"
            ):
                obligations, obligation_summary = (
                    _compact_stale_plan_obligations(
                        child["material_events"],
                        child.get("checkpoint_history", []),
                    )
                )
            compact_child["latest_checkpoint"] = (
                child.get("latest_checkpoint") if include_history
                else _compact_checkpoint(child.get("latest_checkpoint"))
            )
            compact_child["uncheckpointed_material_obligations"] = obligations
            if obligation_summary is not None:
                compact_child["stale_plan_material_obligations"] = (
                    obligation_summary
                )
            compact_child["history"] = {
                "included": include_history,
                "material_events": history_summary(child["material_events"]),
            }
            checkpoint_history = child.get("checkpoint_history", [])
            if "checkpoint_history" in child:
                compact_child["history"]["checkpoints"] = history_summary(
                    checkpoint_history
                )
            if include_history:
                compact_child["material_events"] = child["material_events"]
            child_material_item_count += len(obligations)
            child_material_item_count += len(
                child["material_events"] if include_history else []
            )
            child_material_item_count += _checkpoint_item_count(
                compact_child.get("latest_checkpoint")
            )
            if include_history:
                child_material_item_count += _checkpoint_history_item_count(
                    checkpoint_history
                )
        children.append(compact_child)

    history = {
        "included": include_history,
        "material_events": history_summary(clean["material_events"]),
        # Raw events are an audit surface.  Their count and digest prove which
        # validated history was compacted; the normalized material summary
        # already carries the actionable latest-event pointer.  Full-history
        # mode still emits every raw event verbatim.
        "raw_material_events": history_summary(
            clean["raw_material_events"], include_latest=False,
        ),
        "projection_events": history_summary(clean["projection_events"]),
        "projection_history": history_summary(clean["projection_history"]),
        "projection_quarantined": history_summary(clean["projection_quarantined"]),
        "projection_unresolved_quarantine": history_summary(
            clean["projection_unresolved_quarantine"]
        ),
    }
    checkpoint_revision = (
        clean["latest_checkpoint"]["root_revision"]
        if clean["latest_checkpoint"] is not None else 0
    )
    material_obligations = _uncheckpointed_material_obligations(
        clean["material_events"], checkpoint_revision,
    )
    context = {
        "context_schema": {
            "name": "agent-workstream.resume-context",
            "version": 2,
            "representation": (
                "full_validated" if include_history else "compact_validated"
            ),
        },
        "workstream_id": root["identifier"].upper(),
        "context_url": root["url"],
        "plan_revision": root["plan_revision"],
        "description_plan_revision": root.get(
            "description_plan_revision", root["plan_revision"],
        ),
        "generation_transition_tip_event_id": root.get(
            "generation_transition_tip_event_id"
        ),
        "generation_activation_epoch": root.get("generation_activation_epoch"),
        "generation_authority_origin": root.get("generation_authority_origin"),
        "quarantined_legacy_writes": root.get("quarantined_legacy_writes", {
            "count": 0,
            "sha256": hashlib.sha256(b"[]").hexdigest(),
        }),
        "root_revision": root["revision"],
        "issue_revision": root.get("issue_revision"),
        "status": root.get("status"),
        "next_action": root.get("next_action"),
        "blocker": root.get("blocker"),
        "children": children,
        "decisions": clean["decisions"],
        "choice_events": clean["choice_events"],
        "scope": clean["scope"] if include_history else _compact_scope(clean["scope"]),
        "relations": clean["relations"],
        "dependency_graph": clean["dependency_graph"],
        "evidence_contracts": (
            clean["evidence_contracts"] if include_history
            else _compact_evidence_contracts(
                clean["evidence_contracts"], clean["projection_events"],
                require_projection_authority=require_projection_authority,
            )
        ),
        "child_closures": (
            clean["child_closures"] if include_history
            else _compact_child_closures(
                clean["child_closures"], clean["projection_events"],
                require_projection_authority=require_projection_authority,
            )
        ),
        "surface_availability": clean["surface_availability"],
        "provenance": (
            clean["provenance"] if include_history
            else _compact_provenance(
                clean["provenance"], clean["projection_events"],
            )
        ),
        "material_event_revision": clean["material_event_revision"],
        "material_semantic_repair": history_summary(
            clean["material_semantic_repairs"]
        ),
        "latest_checkpoint": (
            clean["latest_checkpoint"] if include_history
            else _compact_checkpoint(clean["latest_checkpoint"])
        ),
        "checkpoint_recovery": clean["checkpoint_recovery"],
        "uncheckpointed_material_obligations": material_obligations,
        "source": clean.get("source"),
        "disposition": clean.get("disposition"),
        "projection_revision": clean["projection_revision"],
        "projection_recovery": clean["projection_recovery"],
        "lifecycle_recovery": clean["lifecycle_recovery"],
        "projection_quarantine": history_summary(
            clean["projection_unresolved_quarantine"]
        ),
        "quarantine_disposition": clean["quarantine_disposition"],
        "authenticated_route": clean["authenticated_route"],
        "authenticated_source": clean["authenticated_source"],
        "history": history,
        "resume_authority": (
            "partial_terminal_closure_required"
            if expected_missing_terminal_closures
            else ("full" if require_projection_authority else "inspection_only")
        ),
        "deferred_audit_detail": {"state": "none"},
    }
    if include_history:
        context["material_events"] = clean["material_events"]
        context["raw_material_events"] = clean["raw_material_events"]
        context["material_semantic_repairs"] = clean[
            "material_semantic_repairs"
        ]
        context["projection_events"] = clean["projection_events"]
        context["projection_history"] = clean["projection_history"]
        context["projection_quarantined"] = clean["projection_quarantined"]
        context["projection_unresolved_quarantine"] = clean[
            "projection_unresolved_quarantine"
        ]
    context = _without_raw_transcripts(context)
    item_count = child_material_item_count + _checkpoint_item_count(
        context["latest_checkpoint"]
    ) + sum(
        len(value) for value in (
            context["children"], context["decisions"], context["choice_events"],
            context["relations"],
            context["evidence_contracts"],
            context["child_closures"],
            context["uncheckpointed_material_obligations"],
            context.get("material_events", []),
            context.get("raw_material_events", []),
            context.get("material_semantic_repairs", []),
            context.get("projection_events", []),
            context.get("projection_history", []),
            context.get("projection_quarantined", []),
            context.get("projection_unresolved_quarantine", []),
        )
    ) + len(clean["provenance"]) + (
        len((clean["dependency_graph"] or {}).get("relations", []))
        + len((clean["dependency_graph"] or {}).get("authorization_batches", []))
    )
    if max_items < 0 or item_count > max_items:
        raise ResumeError(f"resume_context_over_item_budget:{item_count}>{max_items}")
    encoded = _default_output_bytes(context)
    if (
        len(encoded) > max_bytes
        and not include_history
        and max_bytes == DEFAULT_RESUME_MAX_BYTES
    ):
        original_bytes = len(encoded)
        audit_route = {
            "command": (
                f"workstreamctl resume {normalized_token} "
                "--max-bytes 2147483647 --max-items 2147483647"
            ),
            "command_role": "display_only",
            "launcher": "current_workstream_resume_skill_script",
            "args": [
                normalized_token, "--max-bytes", "2147483647",
                "--max-items", "2147483647",
            ],
            "representation": "compact_validated",
            "locator_format": "RFC6901 JSON Pointer",
        }
        full_history_route = {
            "command": (
                f"workstreamctl resume {normalized_token} --include-history "
                "--max-bytes 2147483647 --max-items 2147483647"
            ),
            "command_role": "display_only",
            "launcher": "current_workstream_resume_skill_script",
            "args": [
                normalized_token, "--include-history", "--max-bytes",
                "2147483647", "--max-items", "2147483647",
            ],
            "representation": "full_validated",
        }
        for excerpt_limit in _CURRENT_DETAIL_EXCERPT_LIMITS:
            candidate, summary = _compact_verbose_current_detail(
                context, excerpt_limit=excerpt_limit,
            )
            if summary["field_count"] == 0:
                break
            summary["original_context_bytes"] = original_bytes
            summary["full_context_sha256"] = hashlib.sha256(json.dumps(
                context, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode()).hexdigest()
            summary["audit_route"] = audit_route
            summary["full_history_route"] = full_history_route
            summary["hydration_required_before_action"] = True
            candidate["context_schema"] = dict(candidate["context_schema"])
            candidate["context_schema"]["envelope"] = (
                "verbose_current_detail_v1"
            )
            candidate["deferred_audit_detail"] = summary
            candidate_encoded = _default_output_bytes(candidate)
            if len(candidate_encoded) <= max_bytes:
                context = candidate
                encoded = candidate_encoded
                break
        if len(encoded) > max_bytes:
            for excerpt_limit in (128, 96, 64, 48):
                candidate = _bounded_authority_envelope(
                    context, text_limit=excerpt_limit,
                )
                candidate["deferred_audit_detail"]["original_context_bytes"] = (
                    original_bytes
                )
                candidate["deferred_audit_detail"]["audit_route"] = audit_route
                candidate["deferred_audit_detail"]["full_history_route"] = (
                    full_history_route
                )
                candidate_encoded = _default_output_bytes(candidate)
                if len(candidate_encoded) <= max_bytes:
                    context = candidate
                    encoded = candidate_encoded
                    break
        if len(encoded) > max_bytes:
            candidate = _fixed_frontier_authority_envelope(
                context, token=normalized_token,
            )
            candidate["deferred_audit_detail"]["original_context_bytes"] = (
                original_bytes
            )
            candidate["deferred_audit_detail"]["audit_route"] = audit_route
            candidate["deferred_audit_detail"]["full_history_route"] = (
                full_history_route
            )
            candidate_encoded = _default_output_bytes(candidate)
            if len(candidate_encoded) <= max_bytes:
                context = candidate
                encoded = candidate_encoded
    if len(encoded) > max_bytes:
        raise ResumeError(f"resume_context_over_budget:{len(encoded)}>{max_bytes}")
    return context


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("token", help="stable Linear root issue identifier")
    parser.add_argument("snapshot", nargs="?", help="JSON path or - for a Linear snapshot")
    parser.add_argument("--config", help="workstream config path; defaults to repository-root .workstream.json")
    parser.add_argument("--linear-workspace-id", help="explicit immutable Linear workspace ID")
    parser.add_argument("--linear-team-id", help="explicit immutable Linear team ID")
    parser.add_argument("--linear-project-id", help="explicit immutable Linear project ID")
    parser.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_RESUME_MAX_BYTES)
    parser.add_argument("--max-items", type=int, default=100)
    parser.add_argument(
        "--include-history", action="store_true",
        help="include complete validated material/projection history instead of digests and counts",
    )
    parser.add_argument(
        "--plan-source",
        help="authenticated path or URL whose exact bytes must match the projected source",
    )
    parser.add_argument(
        "--plan-identity",
        help="immutable canonical identity for --plan-source (defaults to the source argument)",
    )
    parser.add_argument(
        "--inspection-only", action="store_true",
        help="inspect legacy/incomplete state without claiming full-authority resume",
    )
    args = parser.parse_args()
    try:
        token = extract_token(args.token)
        authenticated_source = None
        if args.snapshot is not None and not args.inspection_only:
            raise ResumeError("snapshot_input_requires_inspection_only")
        if args.snapshot is None:
            route, _config_path = resolve_linear_route(
                config_path=args.config,
                workspace_id=args.linear_workspace_id,
                team_id=args.linear_team_id,
                project_id=args.linear_project_id,
            )
            api_key = load_linear_api_key()
            if not api_key:
                raise ResumeError(
                    "Linear auth is required: set LINEAR_API_KEY, LINEAR_API_KEY_FILE, "
                    "or install ~/.config/agent-workstream/linear.token"
                )
            client = HttpGraphQLClient(api_key, args.linear_endpoint)
            route = resolve_authenticated_issue_route(client, token, route)
            transport = LinearGraphQLTransport(
                client,
                team_id=route["team_id"],
                workspace_id=route.get("workspace_id"),
                project_id=route.get("project_id"),
            )
            live_graph_snapshot = transport.snapshot_for_root(
                token, include_child_comments=True, include_description=True,
            )
            complete_route = route if all(
                route.get(field) for field in ("workspace_id", "team_id", "project_id")
            ) else {}
            comments = LinearCommentEventAdapter(
                client, issue_id=token,
                team_id=complete_route.get("team_id"),
                workspace_id=complete_route.get("workspace_id"),
                project_id=complete_route.get("project_id"),
            ).comments()
            generation = select_plan_generation(
                comments, workstream_id=token,
                description_plan_revision=live_graph_snapshot["root"]["plan_revision"],
                authenticated_route=route,
            )
            live_graph_snapshot["root"]["plan_revision"] = generation["plan_revision"]
            live_graph_snapshot["root"]["description_plan_revision"] = generation[
                "description_plan_revision"
            ]
            live_graph_snapshot["root"]["generation_transition_tip_event_id"] = (
                generation["transition_tip_event_id"]
            )
            live_graph_snapshot["root"]["generation_activation_epoch"] = (
                generation["activation_epoch"]
            )
            live_graph_snapshot["root"]["generation_authority_origin"] = (
                generation["authority_origin"]
            )
            from workstream_linear_projection import (
                child_mutation_authorizations_from_comments,
            )
            mutation_authorizations = child_mutation_authorizations_from_comments(
                comments, workstream_id=token,
                description_plan_revision=generation[
                    "description_plan_revision"
                ], authenticated_route=route,
            )
            if mutation_authorizations:
                live_graph_snapshot = transport.recover_authorized_children(
                    live_graph_snapshot, mutation_authorizations,
                )
            live_graph_snapshot = add_live_child_material_history(
                live_graph_snapshot, authenticated_route=route,
                root_comments=comments,
            )
            # This first join discovers the projected plan source before its
            # bytes can be authenticated. Lifecycle validation is necessarily
            # provisional here; the full-authority join below repeats the same
            # live inputs with authenticated_source and enforces it strictly.
            try:
                snapshot = add_material_history(
                    live_graph_snapshot, comments, token, authenticated_route=route,
                    permit_stale_lifecycle_for_reconcile=not args.inspection_only,
                    relation_target_resolver=lambda relations: read_relation_targets(
                        client, relations,
                    ),
                )
            except LinearProjectionError as error:
                if not str(error).startswith("repository_identity_history_regressed:"):
                    raise
                material_revision = reduce_event_comments(
                    comments, workstream_id=token,
                ).revision
                provisional = _inspect_unsealed_identity_history(
                    comments, workstream_id=token,
                    expected_plan_revision=live_graph_snapshot["root"]["plan_revision"],
                    authenticated_route=route, authenticated_source=None,
                    material_revision=material_revision,
                )
                projected_source = provisional["source"]
                source_location = args.plan_source or projected_source.get("identity")
                if not source_location:
                    raise ResumeError("identity_history_reconcile_plan_source_missing")
                authenticated_source = plan_payload(
                    source_location, args.plan_identity or projected_source.get("identity"),
                )["source"]
                partial = inspect_unsealed_identity_history(
                    comments, workstream_id=token,
                    expected_plan_revision=live_graph_snapshot["root"]["plan_revision"],
                    authenticated_route=route,
                    authenticated_source=authenticated_source,
                    material_revision=material_revision,
                )
                json.dump(partial, sys.stdout, ensure_ascii=False, sort_keys=True, indent=2)
                sys.stdout.write("\n")
                return 3
        else:
            raw = (
                sys.stdin.read() if args.snapshot == "-"
                else Path(args.snapshot).read_text(encoding="utf-8")
            )
            snapshot = json.loads(raw)
        if not args.inspection_only:
            projected_source = snapshot.get("source") or {}
            projected_identity = projected_source.get("identity") or projected_source.get("url")
            source_location = args.plan_source or projected_identity
            if not source_location:
                raise ResumeError("full-authority resume has no projected plan source")
            authenticated_source = plan_payload(
                source_location, args.plan_identity or projected_identity
            )["source"]
            if args.snapshot is None:
                snapshot = add_material_history(
                    live_graph_snapshot, comments, token, authenticated_route=route,
                    authenticated_source=authenticated_source,
                    relation_target_resolver=lambda relations: read_relation_targets(
                        client, relations,
                    ),
                )
                snapshot["dependency_graph"] = LinearChildDependencyAdapter(
                    client,
                    workspace_id=route["workspace_id"],
                    team_id=route["team_id"],
                    project_id=route["project_id"],
                    root_issue_id=route["root_issue_id"],
                    root_identifier=token,
                    plan_revision=generation["plan_revision"],
                ).read_authorized_graph(
                    expected_material_revision=snapshot["material_event_revision"],
                    expected_projection_revision=snapshot["projection_revision"],
                    expected_root_readback_sha256=(
                        dependency_root_readback_sha256(snapshot["root"])
                    ),
                )
            else:
                snapshot["authenticated_source"] = authenticated_source
        output = compact_context(
            snapshot, token, args.max_bytes, args.max_items,
            require_projection_authority=not args.inspection_only,
            include_history=args.include_history,
        )
    except (
        OSError, json.JSONDecodeError, ResumeError, LinearTransportError,
        LinearEventError, LinearCheckpointError, LinearProjectionError,
        CheckpointError, ValueError,
    ) as error:
        print(f"workstream resume refused: {error}", file=sys.stderr)
        return 2
    sys.stdout.write(_default_output_text(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
