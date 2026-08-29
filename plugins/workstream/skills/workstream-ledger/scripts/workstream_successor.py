#!/usr/bin/env python3
"""Choose a safe attach or versioned successor worktree from durable state.

This module is deliberately model-free and side-effect free.  A caller may
execute the returned git command only after recording the disposition in the
workstream; local paths never determine the workstream identity.
"""

from __future__ import annotations

import re
from typing import Any


TOKEN = re.compile(r"^[A-Z][A-Z0-9]*-\d+$", re.IGNORECASE)
GIT_OID = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$", re.IGNORECASE)


class SuccessorError(ValueError):
    pass


def _reconcile_recorded_disposition(
    snapshot: dict[str, Any], decision: dict[str, Any]
) -> dict[str, Any]:
    recorded = snapshot.get("disposition")
    if recorded is None:
        return {**decision, "durable_disposition": None,
                "durable_projection_required": True}
    if not isinstance(recorded, dict):
        raise SuccessorError("recorded disposition is malformed")
    expected = {
        "disposition": decision["disposition"],
        "remote_head": decision.get("head") or decision.get("predecessor", {}).get("head"),
        "recovered_from_checkpoint": decision.get("recovered_from_checkpoint"),
    }
    # A successor is created from the verified remote head, not the stale
    # predecessor head.
    if decision["disposition"] == "create_successor":
        expected["remote_head"] = decision.get("verified_remote_head")
    for field, value in expected.items():
        if recorded.get(field) != value:
            raise SuccessorError(f"recorded_disposition_conflict:{field}")
    return {**decision, "durable_disposition": recorded,
            "durable_projection_required": False}


def choose_disposition(snapshot: dict[str, Any], *, remote_head: str | None = None) -> dict[str, Any]:
    root = snapshot.get("root") or {}
    token = str(root.get("identifier") or snapshot.get("workstream_id") or "").upper()
    if not TOKEN.fullmatch(token):
        raise SuccessorError("durable root identifier must be a Linear issue token")
    checkpoint = snapshot.get("latest_checkpoint") or {}
    provenance = snapshot.get("provenance") or {}
    if checkpoint:
        worktree = checkpoint.get("worktree") or {}
        recovered_from = checkpoint.get("checkpoint_event_id")
    elif isinstance(provenance, list):
        candidates = [item for item in provenance if isinstance(item, dict) and item.get("worktree")]
        if len(candidates) > 1:
            raise SuccessorError("multiple projected worktree authorities")
        worktree = (candidates[0] if candidates else {}).get("worktree") or {}
        recovered_from = None
    elif isinstance(provenance, dict):
        compact_latest = provenance.get("latest")
        is_compact = (
            (snapshot.get("context_schema") or {}).get("representation")
            == "compact_validated"
        )
        if is_compact and (
            provenance.get("worktree_authority_ambiguous") is True
            or provenance.get("worktree_authority_count", 0) > 1
        ):
            raise SuccessorError("multiple projected worktree authorities")
        if is_compact and provenance.get("worktree_authority_count") == 1:
            if (
                not isinstance(compact_latest, dict)
                or not isinstance(provenance.get("latest_projection_head"), dict)
            ):
                raise SuccessorError("compact worktree authority is incomplete")
            worktree = compact_latest.get("worktree") or {}
        else:
            worktree = provenance.get("worktree") or {}
        recovered_from = None
    else:
        worktree = {}
        recovered_from = None
    state = str(worktree.get("state", "unavailable")).lower()
    local_head = worktree.get("head")
    if state == "safe" and remote_head and local_head == remote_head:
        return _reconcile_recorded_disposition(snapshot, {
            "disposition": "attach", "workstream": token, "head": remote_head,
            "reason": "worktree is current and matches remote truth",
            "recovered_from_checkpoint": recovered_from,
        })
    if state == "safe" and not remote_head:
        reason = "current remote head is unavailable"
    elif state == "safe":
        reason = "worktree does not match current remote head"
    else:
        reason = {
            "missing": "worktree is unavailable",
            "unavailable": "source machine/worktree is unavailable",
            "dirty": "worktree has uncommitted local state",
            "stale": "worktree does not match current remote head",
            "superseded": "worktree is superseded",
            "merged": "worktree is merged and not an attach target",
            "archived": "worktree is archived",
        }.get(state, "worktree safety is unknown")
    return _reconcile_recorded_disposition(snapshot, {
        "disposition": "create_successor", "workstream": token, "reason": reason,
        "predecessor": {"path": worktree.get("path"), "head": local_head, "state": state},
        "verified_remote_head": remote_head,
        "recovered_from_checkpoint": recovered_from,
    })


def successor_command(snapshot: dict[str, Any], *, remote_repo: str, remote_ref: str,
                      successor_path: str, branch: str,
                      remote_head: str | None = None) -> dict[str, Any]:
    if not remote_head or not GIT_OID.fullmatch(remote_head):
        raise SuccessorError("verified full remote head is required for successor creation")
    decision = choose_disposition(
        snapshot,
        remote_head=remote_head,
    )
    if decision["disposition"] != "create_successor":
        raise SuccessorError("successor command requested for attachable worktree")
    if not remote_repo or not remote_ref or not successor_path or not branch:
        raise SuccessorError("remote repo/ref, successor path, and branch are required")
    return {**decision, "remote_repo": remote_repo, "remote_ref": remote_ref,
            "verified_remote": {"ref": remote_ref, "head": remote_head},
            "successor_path": successor_path, "branch": branch,
            "command": ["git", "-C", remote_repo, "worktree", "add", "-b", branch,
                         successor_path, remote_head]}
