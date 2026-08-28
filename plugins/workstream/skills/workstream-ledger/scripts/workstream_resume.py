#!/usr/bin/env python3
"""Validate and compact a Linear-backed workstream snapshot for recovery.

The transport that obtains the snapshot may be Linear MCP, a future CLI, or a
repository-specific adapter. This command is deliberately transport-neutral:
it validates the durable join and refuses ambiguous/stale/incomplete input
before an agent edits anything.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_checkpoint import CheckpointError, recover_latest
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
from workstream_choices import ChoiceError, reduce_choices
from workstream_evidence import evidence_errors
from workstream_scope import repository_key, ScopeError, validate_relations, validate_scope


TOKEN = re.compile(r"\b[A-Z][A-Z0-9]*-\d+\b", re.I)
TERMINAL = {"done", "cancelled", "canceled", "superseded"}
MATERIAL_OBLIGATION_TERMS = ("requirement", "blocker", "blocked", "followup", "decision")
MATERIAL_OBLIGATION_KEYS = {
    "requirement", "requirements", "blocker", "blockers",
    "followup", "followups", "decision", "decisions",
}


class ResumeError(ValueError):
    pass


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
            "workstream_id", "checkpoint_event_id", "root_revision", "plan_revision",
            "status", "exact_head", "blocker", "next_action", "worktree",
            "acknowledgement",
        )
    }
    result["evidence"] = {
        "count": len(evidence),
        "items": evidence,
        "sha256": hashlib.sha256(json.dumps(
            evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
    }
    result["provenance"] = {
        "count": len(provenance),
        "latest": provenance[-1],
        "sha256": hashlib.sha256(json.dumps(
            provenance, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest(),
    }
    return result


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


def add_material_history(
    snapshot: dict[str, Any], comments: list[dict[str, Any]], token: str,
    *, authenticated_route: dict[str, str] | None = None,
    authenticated_source: dict[str, Any] | None = None,
    permit_stale_lifecycle_for_reconcile: bool = False,
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
    result["authenticated_route"] = dict(authenticated_route) if authenticated_route else None
    result["authenticated_source"] = (
        dict(authenticated_source) if authenticated_source else None
    )
    result["root"]["issue_revision"] = result["root"].get("revision", 0)
    result["root"]["revision"] = event_log.revision
    result["latest_checkpoint"] = None
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
        result["latest_checkpoint"] = recover_latest(
            current_checkpoints, token,
            expected_plan_revision=result["root"].get("plan_revision"),
        )
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
) -> dict[str, Any]:
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
    if str(root.get("status", "")).lower() not in TERMINAL and not root_next_action:
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
    if str(root.get("status", "")).lower() not in TERMINAL and not root.get("next_action"):
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
        if str(child.get("status", "")).lower() not in TERMINAL and not child.get("next_action"):
            raise ResumeError(f"nonterminal child missing next_action:{key}")
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
        active: dict[tuple[str, str], dict[str, Any]] = {}
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
) -> dict[str, Any]:
    normalized_token = extract_token(token)
    clean = validate_snapshot(
        snapshot, normalized_token,
        require_projection_authority=require_projection_authority,
    )
    root = clean["root"]
    children = []
    for child in clean["children"]:
        if str(child.get("status", "")).lower() in TERMINAL:
            continue
        children.append(dict(child) if include_history else {
            key: value for key, value in child.items()
            if key not in {"parent", "project", "team"}
        })

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
        "workstream_id": root["identifier"].upper(),
        "context_url": root["url"],
        "plan_revision": root["plan_revision"],
        "root_revision": root["revision"],
        "issue_revision": root.get("issue_revision"),
        "status": root.get("status"),
        "next_action": root.get("next_action"),
        "children": children,
        "decisions": clean["decisions"],
        "choice_events": clean["choice_events"],
        "scope": clean["scope"] if include_history else _compact_scope(clean["scope"]),
        "relations": clean["relations"],
        "evidence_contracts": clean["evidence_contracts"],
        "surface_availability": clean["surface_availability"],
        "provenance": clean["provenance"],
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
            "full" if require_projection_authority else "inspection_only"
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
    item_count = sum(
        len(value) for value in (
            context["children"], context["decisions"], context["choice_events"],
            context["relations"], context["provenance"],
            context["evidence_contracts"],
            context["uncheckpointed_material_obligations"],
            context.get("material_events", []),
            context.get("projection_events", []),
            context.get("projection_history", []),
            context.get("projection_quarantined", []),
            context.get("projection_unresolved_quarantine", []),
            (context["latest_checkpoint"] or {}).get("provenance_chain", []),
        )
    )
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
            live_graph_snapshot = transport.snapshot_for_root(token)
            complete_route = route if all(
                route.get(field) for field in ("workspace_id", "team_id", "project_id")
            ) else {}
            comments = LinearCommentEventAdapter(
                client, issue_id=token,
                team_id=complete_route.get("team_id"),
                workspace_id=complete_route.get("workspace_id"),
                project_id=complete_route.get("project_id"),
            ).comments()
            # This first join discovers the projected plan source before its
            # bytes can be authenticated. Lifecycle validation is necessarily
            # provisional here; the full-authority join below repeats the same
            # live inputs with authenticated_source and enforces it strictly.
            snapshot = add_material_history(
                live_graph_snapshot, comments, token, authenticated_route=route,
                permit_stale_lifecycle_for_reconcile=not args.inspection_only,
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
