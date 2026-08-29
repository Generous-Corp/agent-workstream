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
    reduce_event_comments,
)
from workstream_linear_projection import (
    LinearProjectionError, reduce_projection_comments, TOMBSTONE,
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
from workstream_scope import (
    is_full_oid, repository_key, ScopeError, validate_relations, validate_scope,
)


TOKEN = re.compile(r"\b[A-Z][A-Z0-9]*-\d+\b", re.I)
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


def _event_next_actions(event: dict[str, Any]) -> set[str]:
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
) -> None:
    if str(child.get("identifier", "")).upper() != token:
        raise ResumeError(f"child_identity_mismatch:{token}")
    if not isinstance(child.get("id"), str) or not child["id"]:
        raise ResumeError(f"child_identity_missing:{token}")
    parent = child.get("parent") or {}
    if parent.get("id") != authenticated_route.get("root_issue_id"):
        raise ResumeError(f"child_parent_route_mismatch:{token}")
    team = child.get("team") or {}
    if team.get("id") != authenticated_route.get("team_id"):
        raise ResumeError(f"child_route_mismatch:{token}:team_id")
    if (team.get("organization") or {}).get("id") != authenticated_route.get("workspace_id"):
        raise ResumeError(f"child_route_mismatch:{token}:workspace_id")
    if (child.get("project") or {}).get("id") != authenticated_route.get("project_id"):
        raise ResumeError(f"child_route_mismatch:{token}:project_id")


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
    for source_child in snapshot.get("children", []):
        child = dict(source_child)
        token = str(child.get("identifier", "")).upper()
        _validate_child_route(child, token=token, authenticated_route=authenticated_route)
        if _is_terminal(child):
            result["children"].append(child)
            continue
        comments = child_comments[token]
        if not isinstance(comments, list):
            raise ResumeError(f"invalid_child_comment_collection:{token}")
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
        append(event, event["kind"], event["payload"])
        if event["kind"] == "material_boundary":
            for change in event["payload"]["changes"]:
                append(event, change["kind"], change["payload"])
    return result


def _compact_checkpoint(checkpoint: dict[str, Any] | None) -> dict[str, Any] | None:
    if checkpoint is None:
        return None
    evidence = checkpoint["evidence"]
    provenance = checkpoint["provenance_chain"]
    result = {
        key: checkpoint[key]
        for key in (
            "checkpoint_event_id", "root_revision", "status", "exact_head",
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
            for key in ("agent", "machine", "session_id")
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
        )
        if key in child
    }
    description = _compact_description(child.get("description"))
    if description is not None:
        result["description_summary"] = description
    return result


def _compact_provenance(
    items: list[dict[str, Any]], projection_events: list[dict[str, Any]],
    scope: dict[str, Any] | None,
) -> dict[str, Any]:
    encoded = json.dumps(
        items, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()
    active: dict[str, tuple[int, dict[str, Any]]] = {}
    for index, event in enumerate(projection_events):
        if event.get("kind") != "provenance":
            continue
        key = event.get("key")
        if not isinstance(key, str):
            continue
        if event.get("value") == TOMBSTONE:
            active.pop(key, None)
        else:
            active[key] = (index, event)
    valid_heads = {
        repository.get("exact_head")
        for repository in (scope or {}).get("repositories", [])
    }
    item_digests = {canonical_digest(item) for item in items}
    bound = []
    for candidate in active.values():
        value = candidate[1]["value"]
        worktree = value.get("worktree") if isinstance(value, dict) else None
        if (
            isinstance(worktree, dict)
            and is_full_oid(str(worktree.get("head") or ""))
            and worktree.get("head") in valid_heads
            and canonical_digest(value) in item_digests
        ):
            bound.append(candidate)
    latest_event = max(bound, default=None, key=lambda item: item[0])
    latest = latest_event[1]["value"] if latest_event is not None else None
    return {
        "count": len(items),
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "latest": ({
            key: latest[key]
            for key in ("agent", "machine", "session_id", "worktree")
            if latest.get(key) is not None
        } if latest is not None else None),
        "latest_projection_head": ({
            "key": latest_event[1]["key"],
            "event_id": latest_event[1]["event_id"],
            "value_sha256": canonical_digest(latest),
        } if latest is not None else None),
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
    event_log = reduce_event_comments(comments, workstream_id=token)
    checkpoint_log = reduce_checkpoint_comments(comments, workstream_id=token)
    plan_revision = result["root"].get("plan_revision")
    projection_log = reduce_projection_comments(
        comments, workstream_id=token, expected_plan_revision=plan_revision,
        authenticated_route=authenticated_route,
        authenticated_source=authenticated_source,
    )
    events = [_event_record(event) for event in event_log.events]

    result["material_events"] = events
    result["material_event_revision"] = event_log.revision
    result.update(projection_log.snapshot)
    relations = result.get("relations") or []
    if relations and relation_target_resolver is not None:
        result["relation_targets"] = relation_target_resolver(relations)
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
    if token and identifier.upper() != extract_token(token):
        raise ResumeError("token/root mismatch")
    for field in ("url", "plan_revision", "revision"):
        if field not in root or root[field] in (None, ""):
            raise ResumeError(f"root missing {field}")
    if not isinstance(root["revision"], int) or root["revision"] < 0:
        raise ResumeError("root revision must be a non-negative integer")
    if "issue_revision" in root and (
        not isinstance(root["issue_revision"], int) or root["issue_revision"] < 0
    ):
        raise ResumeError("issue revision must be a non-negative integer")
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
    active: dict[tuple[str, str], dict[str, Any]] = {}
    if projection_events:
        if not isinstance(authenticated_route, dict) or not all(
            isinstance(authenticated_route.get(field), str) and authenticated_route[field]
            for field in ("workspace_id", "team_id", "project_id", "root_issue_id")
        ):
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
                if contract.get("exact_head") != scoped_repository["exact_head"]:
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
            if scoped_repository is None or scoped_repository.get("exact_head") != closure.get("exact_head"):
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
                "material_events",
            )
        }
        availability["child_closures"] = "available"
        availability["latest_checkpoint"] = (
            "available" if "latest_checkpoint" in snapshot else "transport_unimplemented"
        )
    except (ChoiceError, ScopeError) as error:
        raise ResumeError(str(error)) from error
    if require_projection_authority:
        if not projection_events:
            raise ResumeError("projection_authority_absent")
        if snapshot.get("authenticated_source") is None:
            raise ResumeError("projection_source_bytes_unverified")
    return {"root": root, "children": children, "decisions": snapshot.get("decisions", []),
            "choice_events": choice_events, "scope": scope,
            "relations": relations, "evidence_contracts": evidence_contracts,
            "child_closures": snapshot.get("child_closures", []),
            "surface_availability": availability,
            "provenance": snapshot.get("provenance", []),
            "material_events": material_events,
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
            "authenticated_source": snapshot.get("authenticated_source")}


def compact_context(
    snapshot: dict[str, Any], token: str, max_bytes: int = 16 * 1024,
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

    def history_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
        encoded = json.dumps(
            events, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()
        latest = events[-1] if events else None
        return {
            "count": len(events),
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "latest": ({
                key: latest[key]
                for key in ("event_id", "kind", "key", "created_at")
                if latest.get(key) is not None
            } if latest else None),
        }

    children = []
    child_material_item_count = 0
    for child in clean["children"]:
        if _is_terminal(child):
            continue
        compact_child = dict(child) if include_history else _compact_child(child)
        if "material_events" in child:
            checkpoint_revision = (
                child["latest_checkpoint"]["root_revision"]
                if child.get("latest_checkpoint") is not None else 0
            )
            obligations = _uncheckpointed_material_obligations(
                child["material_events"], checkpoint_revision,
            )
            compact_child["latest_checkpoint"] = (
                child.get("latest_checkpoint") if include_history
                else _compact_checkpoint(child.get("latest_checkpoint"))
            )
            compact_child["uncheckpointed_material_obligations"] = obligations
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
                clean["provenance"], clean["projection_events"], clean["scope"],
            )
        ),
        "material_event_revision": clean["material_event_revision"],
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
    }
    if include_history:
        context["material_events"] = clean["material_events"]
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
            context.get("projection_events", []),
            context.get("projection_history", []),
            context.get("projection_quarantined", []),
            context.get("projection_unresolved_quarantine", []),
        )
    ) + len(clean["provenance"])
    if max_items < 0 or item_count > max_items:
        raise ResumeError(f"resume_context_over_item_budget:{item_count}>{max_items}")
    encoded = json.dumps(context, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
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
    parser.add_argument("--max-bytes", type=int, default=16 * 1024)
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
                token, include_child_comments=True,
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
            child_comments = live_graph_snapshot.pop("child_comments", None)
            live_graph_snapshot = add_child_material_history(
                live_graph_snapshot, child_comments,
                authenticated_route=route,
            )
            # This first join discovers the projected plan source before its
            # bytes can be authenticated. Lifecycle validation is necessarily
            # provisional here; the full-authority join below repeats the same
            # live inputs with authenticated_source and enforces it strictly.
            snapshot = add_material_history(
                live_graph_snapshot, comments, token, authenticated_route=route,
                permit_stale_lifecycle_for_reconcile=not args.inspection_only,
                relation_target_resolver=lambda relations: read_relation_targets(
                    client, relations,
                ),
            )
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
    json.dump(output, sys.stdout, ensure_ascii=False, sort_keys=True, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
