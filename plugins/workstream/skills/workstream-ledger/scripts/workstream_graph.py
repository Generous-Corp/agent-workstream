#!/usr/bin/env python3
"""Build deterministic, idempotent root/child operations for a Linear adapter."""

from __future__ import annotations

from typing import Any


class GraphReviewRequired(ValueError):
    pass


def build_operations(
    plan: dict[str, Any],
    *,
    existing_root: dict[str, Any] | None = None,
    existing_children: list[dict[str, Any]] | None = None,
    accepted_keys: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Return stable operations; a transport applies them with its own CAS.

    Markdown candidates are never silently treated as actionable. Callers must
    pass the reviewed stable-key set, and repeated calls against the same
    existing graph become updates rather than duplicate creates.
    """
    if plan.get("graph_review_required") and accepted_keys is None:
        raise GraphReviewRequired("review candidate graph before creating children")
    candidates = {str(child["key"]): child for child in plan.get("children", [])}
    accepted = set(candidates if accepted_keys is None else accepted_keys)
    unknown = accepted - candidates.keys()
    if unknown:
        raise GraphReviewRequired("accepted key is not present in plan: " + ",".join(sorted(unknown)))
    root_data = plan.get("root") or {}
    root_key = root_data.get("stable_key")
    if not root_key or not root_data.get("plan_revision"):
        raise ValueError("plan root needs stable_key and plan_revision")
    root_action = "update_root" if existing_root else "create_root"
    operations = [{
        "action": root_action,
        "stable_key": root_key,
        "title": root_data.get("title", "Untitled workstream"),
        "plan_revision": root_data["plan_revision"],
        "next_action": root_data.get("next_action"),
        "existing_identifier": (existing_root or {}).get("identifier"),
    }]
    by_key = {str(child.get("stable_key")): child for child in (existing_children or [])}
    for key in sorted(accepted):
        candidate = candidates[key]
        old = by_key.get(key)
        operations.append({
            "action": "update_child" if old else "create_child",
            "stable_key": key,
            "title": candidate["title"],
            "root_stable_key": root_key,
            "existing_identifier": (old or {}).get("identifier"),
            "plan_line": candidate.get("line"),
            "next_action": candidate.get("next_action"),
        })
    return operations
