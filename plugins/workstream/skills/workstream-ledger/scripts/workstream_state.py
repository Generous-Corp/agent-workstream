#!/usr/bin/env python3
"""Deterministic root-state reducer and adversarial closure checks."""

from __future__ import annotations

from copy import deepcopy
from typing import Any


class StateConflict(RuntimeError):
    """A stale writer attempted to replace a newer root revision."""


def apply_delta(root: dict[str, Any], expected_revision: int, delta: dict[str, Any]) -> dict[str, Any]:
    if root.get("revision") != expected_revision:
        raise StateConflict(f"expected revision {expected_revision}, current {root.get('revision')}")
    next_root = deepcopy(root)
    next_root["revision"] = expected_revision + 1
    history = list(next_root.get("history", []))
    history.append({"revision": next_root["revision"], "delta": deepcopy(delta)})
    next_root["history"] = history
    for key, value in delta.items():
        if key != "children":
            next_root[key] = deepcopy(value)
    return next_root


def reconcile_external(root: dict[str, Any], live: dict[str, Any]) -> dict[str, Any]:
    """Project live execution truth without closing semantic acceptance."""
    result = deepcopy(root)
    contradictions = list(result.get("contradictions", []))
    recorded_head = result.get("pr_head")
    live_head = live.get("pr_head")
    if live.get("merged") and (not recorded_head or not live_head):
        contradictions.append({
            "kind": "exact_head_unavailable",
            "recorded": recorded_head,
            "live": live_head,
        })
        result["receipt_valid"] = False
        result["contradictions"] = contradictions
        return result
    if recorded_head and live_head and recorded_head != live_head:
        contradictions.append({"kind": "head_drift", "recorded": recorded_head, "live": live_head})
        result["receipt_valid"] = False
        result["contradictions"] = contradictions
        return result
    if live.get("merged") and not live.get("merge_sha"):
        contradictions.append({"kind": "merge_sha_unavailable"})
        result["receipt_valid"] = False
        result["contradictions"] = contradictions
        return result
    if live.get("merged"):
        result["status"] = "Landed — acceptance review required"
        result["merge_sha"] = live.get("merge_sha")
    if live_head and (not recorded_head or live_head == recorded_head):
        result["pr_head"] = live_head
    result["contradictions"] = contradictions
    return result


def closure_errors(
    root: dict[str, Any], *, expected_plan_revision: str,
    required_child_keys: set[str], live: dict[str, Any] | None = None,
) -> list[str]:
    """Return deterministic blockers; empty is necessary, not sufficient, for Done."""
    errors: list[str] = []
    if root.get("plan_revision") != expected_plan_revision:
        errors.append("plan_sync_required")
    children = root.get("children", [])
    keys = [child.get("key") for child in children]
    if len(keys) != len(set(keys)):
        errors.append("duplicate_child")
    errors.extend(f"missing_child:{key}" for key in sorted(required_child_keys - set(keys)))
    terminal = {"done", "cancelled", "canceled", "superseded"}
    for child in children:
        key = child.get("key")
        status = str(child.get("status", "")).lower()
        if key in required_child_keys and status not in terminal:
            errors.append(f"required_child_open:{key}")
        if status not in terminal:
            if not child.get("owner"):
                errors.append(f"unowned_nonterminal:{key}")
            if status == "blocked" and not child.get("next_action"):
                errors.append(f"blocked_without_next_action:{key}")
            if status == "blocked" and not child.get("review_condition"):
                errors.append(f"blocked_without_review_condition:{key}")
    root_status = str(root.get("status", "")).lower()
    if root_status == "done" and any(
        str(child.get("status", "")).lower() not in terminal for child in children
    ):
        errors.append("done_with_open_children")
    if live:
        if live.get("pr_head") and root.get("pr_head") and live["pr_head"] != root["pr_head"]:
            errors.append("stale_head_or_receipt")
        if live.get("merged") and root_status == "done" and not root.get("closure_receipt"):
            errors.append("semantic_done_without_closure_receipt")
    return sorted(set(errors))
