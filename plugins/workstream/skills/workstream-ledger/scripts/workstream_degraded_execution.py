#!/usr/bin/env python3
"""Same-process continuation during a narrow transient Linear outage.

This cannot recover or mint authority. A trusted runtime issues an opaque,
current-turn grant only after its authenticated resume validator returns full
execution authority. Productive delivery may continue while tracking and
lifecycle mutations remain fenced.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import json
import os
import re
import time
import uuid
from typing import Any, Callable

from workstream_delta import DeltaJournal, MutationAdapter, MutationReceipt

HEX64 = re.compile(r"[0-9a-f]{64}")
HEAD = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")
RETRIABLE_FAILURES = frozenset({
    "linear_http_429", "linear_http_502", "linear_http_503",
    "linear_http_504", "linear_read_timeout", "linear_network_unreachable",
})
FORBIDDEN_CHANGE_KINDS = frozenset({
    "scope", "source", "generation", "root_transition", "child_ownership",
    "attach", "successor", "closure", "semantic_closure",
})
MAX_GRANT_SECONDS = 3600.0
_PROCESS_INCARNATION = f"{os.getpid()}:{uuid.uuid4().hex}"
_LIVE_GRANTS: dict[str, tuple[int, dict[str, Any]]] = {}


class DegradedExecutionError(RuntimeError):
    """The degraded-continuation contract was not satisfied."""


def _mapping(value: Any, keys: set[str], error: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise DegradedExecutionError(error)
    return value


def _validate_full_context(value: dict[str, Any]) -> dict[str, Any]:
    root = _mapping(value, {
        "resume_authority", "authority_scope", "workstream_id", "owner",
        "authenticated_route", "source", "generation", "frontiers",
        "worktree", "repository", "shipyard", "snapshot_sha256",
        "authenticated_at", "snapshot_created_at",
    }, "live_full_context_shape_invalid")
    owner = _mapping(root["owner"], {
        "agent", "provider", "session_id", "machine", "session_incarnation",
    }, "live_owner_invalid")
    route = _mapping(root["authenticated_route"], {
        "workspace_uuid", "team_uuid", "project_uuid", "root_issue_uuid",
    }, "live_route_invalid")
    source = _mapping(root["source"], {"identity", "sha256"}, "live_source_invalid")
    generation = _mapping(root["generation"], {
        "plan_revision", "activation_epoch", "transition_tip_event_id",
    }, "live_generation_invalid")
    frontiers = _mapping(root["frontiers"], {
        "material_revision", "projection_revision", "checkpoint_event_id",
        "graph_frontier_sha256",
    }, "live_frontiers_invalid")
    worktree = _mapping(root["worktree"], {
        "path", "branch", "head", "state",
    }, "live_worktree_invalid")
    repository = _mapping(root["repository"], {
        "repository_key", "exact_head",
    }, "live_repository_invalid")
    shipyard = _mapping(root["shipyard"], {
        "run_id", "repository_key", "exact_head", "ownership_state",
    }, "live_shipyard_invalid")
    strings = [*owner.values(), *route.values(), source["identity"],
               generation["transition_tip_event_id"],
               frontiers["checkpoint_event_id"], worktree["path"], worktree["branch"],
               repository["repository_key"], shipyard["run_id"],
               root["authenticated_at"], root["snapshot_created_at"]]
    if (
        root["resume_authority"] != "full"
        or root["authority_scope"] != "executable_current"
        or not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(root["workstream_id"]))
        or not all(isinstance(item, str) and item for item in strings)
        or not HEX64.fullmatch(str(source["sha256"]))
        or generation["plan_revision"] != source["sha256"]
        or isinstance(generation["activation_epoch"], bool)
        or not isinstance(generation["activation_epoch"], int)
        or generation["activation_epoch"] < 0
        or any(isinstance(frontiers[key], bool)
               or not isinstance(frontiers[key], int) or frontiers[key] < 0
               for key in ("material_revision", "projection_revision"))
        or not HEX64.fullmatch(str(frontiers["graph_frontier_sha256"]))
        or worktree["state"] != "safe"
        or not HEAD.fullmatch(str(worktree["head"]))
        or repository["exact_head"] != worktree["head"]
        or shipyard["repository_key"] != repository["repository_key"]
        or shipyard["exact_head"] != repository["exact_head"]
        or shipyard["ownership_state"] not in {"accepted", "monitoring", "landed"}
        or not HEX64.fullmatch(str(root["snapshot_sha256"]))
    ):
        raise DegradedExecutionError("live_full_context_invalid")
    return deepcopy(root)


@dataclass(frozen=True)
class LiveContinuationGrant:
    """Opaque capability; a serialized or reconstructed copy is invalid."""
    token: str
    process_incarnation: str
    turn_id: str
    expires_monotonic: float


class AuthenticatedGrantIssuer:
    """Bridge to the runtime's authoritative authenticated resume validator."""

    def __init__(self, validator: Callable[[dict[str, Any]], bool]):
        self._validator = validator

    def _authenticated(self, context: dict[str, Any]) -> dict[str, Any]:
        if self._validator(context) is not True:
            raise DegradedExecutionError("authenticated_full_grant_required")
        return _validate_full_context(context)

    def issue(self, context: dict[str, Any], *, turn_id: str,
              lifetime_seconds: float = 300.0,
              monotonic_now: float | None = None) -> LiveContinuationGrant:
        checked = self._authenticated(context)
        if not isinstance(turn_id, str) or not turn_id:
            raise DegradedExecutionError("turn_id_required")
        if not 0 < lifetime_seconds <= MAX_GRANT_SECONDS:
            raise DegradedExecutionError("grant_lifetime_invalid")
        now = time.monotonic() if monotonic_now is None else monotonic_now
        grant = LiveContinuationGrant(uuid.uuid4().hex, _PROCESS_INCARNATION,
                                      turn_id, now + lifetime_seconds)
        _LIVE_GRANTS[grant.token] = (id(grant), checked)
        return grant

    def validate_reconciliation(self, context: dict[str, Any]) -> dict[str, Any]:
        return self._authenticated(context)


def _grant_context(grant: LiveContinuationGrant, *, requester: dict[str, str],
                   turn_id: str, monotonic_now: float | None) -> dict[str, Any]:
    registered = _LIVE_GRANTS.get(getattr(grant, "token", ""))
    now = time.monotonic() if monotonic_now is None else monotonic_now
    if (not isinstance(grant, LiveContinuationGrant) or registered is None
            or registered[0] != id(grant)
            or grant.process_incarnation != _PROCESS_INCARNATION
            or grant.turn_id != turn_id or now >= grant.expires_monotonic):
        raise DegradedExecutionError("live_same_process_current_turn_grant_required")
    context = registered[1]
    if requester != context["owner"]:
        raise DegradedExecutionError("fresh_session_requires_independent_full_authority")
    return deepcopy(context)


def authorize_active_owner(grant: LiveContinuationGrant, *,
                           requester: dict[str, str], turn_id: str,
                           tracking_failure: str, write_outcome: str,
                           monotonic_now: float | None = None) -> dict[str, Any]:
    context = _grant_context(grant, requester=requester, turn_id=turn_id,
                             monotonic_now=monotonic_now)
    if tracking_failure not in RETRIABLE_FAILURES:
        raise DegradedExecutionError("tracking_failure_not_retriable")
    if write_outcome not in {"no_request_sent", "read_only_failed"}:
        raise DegradedExecutionError("ambiguous_postwrite_result_requires_reconciliation")
    return {
        "resume_authority": "degraded_continuation",
        **{key: deepcopy(context[key]) for key in (
            "workstream_id", "owner", "authenticated_route", "source",
            "generation", "frontiers", "worktree", "repository", "shipyard")},
        "snapshot_sha256": context["snapshot_sha256"],
        "authenticated_at": context["authenticated_at"],
        "snapshot_created_at": context["snapshot_created_at"],
        "tracking_failure": tracking_failure,
        "linear_mutation_allowed": False,
        "scope_expansion_allowed": False,
        "root_or_generation_transition_allowed": False,
        "semantic_closure_allowed": False,
        "linear_resume_or_handoff_certification_allowed": False,
        "provider_or_local_implementation_allowed": True,
        "exact_head_shipyard_handoff_or_landing_allowed": True,
    }


class DegradedExecutionOutbox:
    """Durable local material-delta buffer for a live continuation grant."""

    def __init__(self, journal: DeltaJournal, grant: LiveContinuationGrant,
                 issuer: AuthenticatedGrantIssuer):
        self.journal, self.grant, self.issuer = journal, grant, issuer

    def record_boundary(self, *, requester: dict[str, str], turn_id: str,
                        tracking_failure: str, write_outcome: str,
                        boundary_id: str, changes: list[dict[str, Any]],
                        monotonic_now: float | None = None) -> dict[str, Any]:
        permit = authorize_active_owner(
            self.grant, requester=requester, turn_id=turn_id,
            tracking_failure=tracking_failure, write_outcome=write_outcome,
            monotonic_now=monotonic_now)
        if not isinstance(changes, list) or not changes:
            raise DegradedExecutionError("material_changes_required")
        forbidden = [str(change.get("kind")) for change in changes
                     if not isinstance(change, dict)
                     or change.get("kind") in FORBIDDEN_CHANGE_KINDS]
        if forbidden:
            raise DegradedExecutionError("degraded_change_forbidden:" + ",".join(sorted(forbidden)))
        pending = [row for row in self.journal.pending()
                   if row.workstream_id == permit["workstream_id"]]
        expected_revision = permit["frontiers"]["material_revision"] + len(pending)
        checkpoint = {"mode": "tracking_degraded", **{
            key: deepcopy(permit[key]) for key in (
                "owner", "authenticated_route", "source", "generation",
                "frontiers", "worktree", "repository", "shipyard",
                "snapshot_sha256", "authenticated_at", "snapshot_created_at")}}
        expected_payload = {"boundary_id": boundary_id, "changes": changes,
                            "checkpoint": checkpoint}
        matches = []
        for event_id, payload, revision in self.journal.db.execute(
                "SELECT event_id,payload,expected_revision FROM material_deltas "
                "WHERE workstream_id=? AND kind='material_boundary'",
                (permit["workstream_id"],)).fetchall():
            decoded = json.loads(payload)
            if decoded.get("boundary_id") == boundary_id:
                matches.append((event_id, decoded, revision))
        if matches:
            if len(matches) != 1 or matches[0][1] != expected_payload:
                raise DegradedExecutionError(f"degraded_boundary_conflict:{boundary_id}")
            return {**permit, "event_id": matches[0][0],
                    "expected_revision": matches[0][2],
                    "durable_local_outbox": True, "replay": True}
        event_id = self.journal.append_boundary(
            permit["workstream_id"], boundary_id, changes, expected_revision,
            source="agent_discovery", checkpoint=checkpoint)
        return {**permit, "event_id": event_id,
                "expected_revision": expected_revision,
                "durable_local_outbox": True, "replay": False}

    def certification_blockers(self) -> list[str]:
        registered = _LIVE_GRANTS.get(self.grant.token)
        workstream_id = registered[1]["workstream_id"] if registered else None
        if any(row.workstream_id == workstream_id for row in self.journal.pending()):
            return ["tracking_reconciliation_required", "semantic_closure_blocked",
                    "linear_resume_handoff_certification_blocked"]
        return []

    def reconcile(self, adapter: MutationAdapter, *,
                  live_context: dict[str, Any]) -> list[MutationReceipt]:
        current = self.issuer.validate_reconciliation(live_context)
        registered = _LIVE_GRANTS.get(self.grant.token)
        if registered is None:
            raise DegradedExecutionError("original_grant_unavailable")
        original = registered[1]
        for key in (
            "workstream_id", "owner", "authenticated_route", "source",
            "generation", "frontiers", "worktree", "repository", "shipyard",
            "snapshot_sha256", "authenticated_at", "snapshot_created_at",
        ):
            if current[key] != original[key]:
                raise DegradedExecutionError(f"reconciliation_binding_changed:{key}")
        receipts = self.journal.apply_with_rebase(adapter)
        if self.certification_blockers():
            raise DegradedExecutionError("tracking_reconciliation_incomplete")
        return receipts
