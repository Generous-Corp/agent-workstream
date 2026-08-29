#!/usr/bin/env python3
"""Durable material-boundary checkpoints for short-token recovery.

This module defines and validates the payload that a remote workstream adapter
must acknowledge before an agent treats a material boundary as recoverable. It
is transport-neutral and does not pretend that a local SQLite row is available
after its source machine disappears.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import re
from typing import Any


SCHEMA_VERSION = 1
TOKEN = re.compile(r"^[A-Z][A-Z0-9]*-\d+$")
WORKTREE_STATES = {
    "safe", "stale", "dirty", "unavailable", "missing", "superseded",
    "merged", "archived", "unknown",
}


class CheckpointError(ValueError):
    pass


def _immutable_payload(checkpoint: dict[str, Any]) -> dict[str, Any]:
    return {
        key: deepcopy(value)
        for key, value in checkpoint.items()
        if key not in {"event_id", "acknowledgement"}
    }


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _event_id(checkpoint: dict[str, Any]) -> str:
    return "wsc_" + hashlib.sha256(_canonical(_immutable_payload(checkpoint))).hexdigest()[:32]


def build_checkpoint(
    *,
    workstream_id: str,
    boundary_id: str,
    root_revision: int,
    plan_revision: str,
    before_status: str,
    after_status: str,
    execution: dict[str, Any],
    exact_head: str | None,
    evidence: list[dict[str, Any]],
    blocker: dict[str, Any] | None,
    next_action: str,
    predecessor_event_id: str | None = None,
) -> dict[str, Any]:
    """Build one pending checkpoint without performing a remote mutation."""
    checkpoint = {
        "schema_version": SCHEMA_VERSION,
        "workstream_id": workstream_id.upper(),
        "boundary_id": boundary_id,
        "root_revision": root_revision,
        "plan_revision": plan_revision,
        "status": {"before": before_status, "after": after_status},
        "execution": deepcopy(execution),
        "exact_head": exact_head,
        "evidence": deepcopy(evidence),
        "blocker": deepcopy(blocker),
        "next_action": next_action,
        "predecessor_event_id": predecessor_event_id,
        "acknowledgement": {
            "state": "pending",
            "remote_id": None,
            "applied_revision": None,
        },
    }
    checkpoint["event_id"] = _event_id(checkpoint)
    validate_checkpoint(checkpoint)
    return checkpoint


def validate_checkpoint(checkpoint: dict[str, Any]) -> None:
    if checkpoint.get("schema_version") != SCHEMA_VERSION:
        raise CheckpointError("unsupported_checkpoint_schema")
    token = checkpoint.get("workstream_id")
    if not isinstance(token, str) or not TOKEN.fullmatch(token):
        raise CheckpointError("invalid_workstream_id")
    for field in ("boundary_id", "plan_revision", "next_action"):
        if not isinstance(checkpoint.get(field), str) or not checkpoint[field].strip():
            raise CheckpointError(f"checkpoint_missing:{field}")
    revision = checkpoint.get("root_revision")
    if not isinstance(revision, int) or revision < 0:
        raise CheckpointError("invalid_root_revision")
    status = checkpoint.get("status")
    if not isinstance(status, dict) or not status.get("before") or not status.get("after"):
        raise CheckpointError("checkpoint_missing:before_after_status")
    execution = checkpoint.get("execution")
    if not isinstance(execution, dict):
        raise CheckpointError("checkpoint_missing:execution")
    for field in ("agent", "provider", "session_id", "machine"):
        if not isinstance(execution.get(field), str) or not execution[field].strip():
            raise CheckpointError(f"checkpoint_missing:execution.{field}")
    worktree = execution.get("worktree")
    if not isinstance(worktree, dict) or worktree.get("state") not in WORKTREE_STATES:
        raise CheckpointError("checkpoint_missing:execution.worktree")
    if worktree["state"] not in {"unavailable", "missing", "unknown"}:
        for field in ("path", "branch", "head"):
            if not isinstance(worktree.get(field), str) or not worktree[field].strip():
                raise CheckpointError(f"checkpoint_missing:execution.worktree.{field}")
    if worktree["state"] == "safe" and checkpoint.get("exact_head") != worktree.get("head"):
        raise CheckpointError("safe_worktree_head_mismatch")
    if checkpoint.get("exact_head") is not None and not isinstance(checkpoint["exact_head"], str):
        raise CheckpointError("invalid_exact_head")
    if not isinstance(checkpoint.get("evidence"), list):
        raise CheckpointError("checkpoint_missing:evidence")
    if checkpoint.get("blocker") is not None and not isinstance(checkpoint["blocker"], dict):
        raise CheckpointError("invalid_blocker")
    predecessor = checkpoint.get("predecessor_event_id")
    if predecessor is not None and not isinstance(predecessor, str):
        raise CheckpointError("invalid_predecessor_event_id")
    acknowledgement = checkpoint.get("acknowledgement")
    if not isinstance(acknowledgement, dict) or acknowledgement.get("state") not in {
        "pending", "remote_acknowledged",
    }:
        raise CheckpointError("invalid_checkpoint_acknowledgement")
    if acknowledgement["state"] == "remote_acknowledged":
        if not acknowledgement.get("remote_id"):
            raise CheckpointError("checkpoint_missing:acknowledgement.remote_id")
        applied = acknowledgement.get("applied_revision")
        if not isinstance(applied, int) or applied < revision:
            raise CheckpointError("invalid_checkpoint_applied_revision")
    if checkpoint.get("event_id") != _event_id(checkpoint):
        raise CheckpointError("checkpoint_event_id_mismatch")


def acknowledge_checkpoint(
    checkpoint: dict[str, Any], remote_id: str, applied_revision: int
) -> dict[str, Any]:
    """Return a remotely acknowledged copy, preserving immutable event identity."""
    validate_checkpoint(checkpoint)
    if not remote_id:
        raise CheckpointError("checkpoint_missing:acknowledgement.remote_id")
    if applied_revision < checkpoint["root_revision"]:
        raise CheckpointError("invalid_checkpoint_applied_revision")
    current = checkpoint["acknowledgement"]
    if current["state"] == "remote_acknowledged":
        if current["remote_id"] != remote_id or current["applied_revision"] != applied_revision:
            raise CheckpointError("checkpoint_acknowledgement_conflict")
        return deepcopy(checkpoint)
    result = deepcopy(checkpoint)
    result["acknowledgement"] = {
        "state": "remote_acknowledged",
        "remote_id": remote_id,
        "applied_revision": applied_revision,
    }
    validate_checkpoint(result)
    return result


def recover_generations(
    checkpoints: list[dict[str, Any]], workstream_id: str,
) -> dict[str, dict[str, Any]]:
    """Validate every immutable plan generation and return each chain tip."""
    token = workstream_id.upper()
    if not TOKEN.fullmatch(token):
        raise CheckpointError("invalid_workstream_id")
    unique: dict[str, dict[str, Any]] = {}
    immutable: dict[str, bytes] = {}
    for checkpoint in checkpoints:
        if checkpoint.get("workstream_id") != token:
            continue
        event_id = checkpoint.get("event_id")
        if not isinstance(event_id, str):
            raise CheckpointError("checkpoint_missing:event_id")
        material = _canonical(_immutable_payload(checkpoint))
        if event_id in immutable and immutable[event_id] != material:
            raise CheckpointError("checkpoint_event_collision")
        immutable[event_id] = material
        existing = unique.get(event_id)
        if existing is None or checkpoint.get("acknowledgement", {}).get("state") == "remote_acknowledged":
            unique[event_id] = checkpoint
    if not unique:
        raise CheckpointError("checkpoint_not_found")

    generations: dict[str, list[dict[str, Any]]] = {}
    for checkpoint in unique.values():
        validate_checkpoint(checkpoint)
        if checkpoint["acknowledgement"]["state"] != "remote_acknowledged":
            raise CheckpointError("checkpoint_not_remote_acknowledged")
        generations.setdefault(checkpoint["plan_revision"], []).append(checkpoint)

    recovered: dict[str, dict[str, Any]] = {}
    for plan_revision, generation in generations.items():
        records = sorted(
            generation,
            key=lambda item: (item["root_revision"], item["event_id"]),
        )
        if records[0].get("predecessor_event_id") is not None:
            raise CheckpointError("checkpoint_chain_truncated")
        previous: dict[str, Any] | None = None
        provenance: list[dict[str, Any]] = []
        for checkpoint in records:
            if previous is not None:
                if checkpoint["root_revision"] <= previous["root_revision"]:
                    raise CheckpointError("checkpoint_revision_not_monotonic")
                if checkpoint.get("predecessor_event_id") != previous["event_id"]:
                    raise CheckpointError("checkpoint_chain_broken")
            execution = checkpoint["execution"]
            provenance.append(
                {
                    "event_id": checkpoint["event_id"],
                    "agent": execution["agent"],
                    "provider": execution["provider"],
                    "session_id": execution["session_id"],
                    "machine": execution["machine"],
                    "worktree": deepcopy(execution["worktree"]),
                }
            )
            previous = checkpoint
        assert previous is not None
        execution = previous["execution"]
        recovered[plan_revision] = {
            "workstream_id": token,
            "checkpoint_event_id": previous["event_id"],
            "root_revision": previous["root_revision"],
            "plan_revision": previous["plan_revision"],
            "status": deepcopy(previous["status"]),
            "exact_head": previous["exact_head"],
            "evidence": deepcopy(previous["evidence"]),
            "blocker": deepcopy(previous["blocker"]),
            "next_action": previous["next_action"],
            "worktree": deepcopy(execution["worktree"]),
            "acknowledgement": deepcopy(previous["acknowledgement"]),
            "provenance_chain": provenance,
        }
    return recovered


def recover_latest(
    checkpoints: list[dict[str, Any]],
    workstream_id: str,
    *,
    expected_plan_revision: str,
) -> dict[str, Any]:
    """Validate all plan generations and return the exact expected chain tip."""
    recovered = recover_generations(checkpoints, workstream_id)
    try:
        return deepcopy(recovered[expected_plan_revision])
    except KeyError as error:
        # Checkpoints exist, but none belong to the caller's expected plan.
        raise CheckpointError("plan_sync_required") from error
