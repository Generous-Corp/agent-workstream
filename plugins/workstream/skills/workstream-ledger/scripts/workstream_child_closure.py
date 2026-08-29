#!/usr/bin/env python3
"""Canonical terminal-child readback and evidence digests."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any


class ChildClosureError(ValueError):
    pass


CHILD_READBACK_FIELDS = {
    "child_identifier", "child_issue_id", "parent_issue_id", "workspace_id",
    "team_id", "project_id", "assignee_id", "state_id", "state_name",
    "state_type",
}


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def terminal_child_readback(child: dict[str, Any]) -> dict[str, Any]:
    """Return the exact live identity/state surface eligible for repair."""
    state_type = str(child.get("status_type") or "").lower()
    if "assignee" not in child:
        raise ChildClosureError("terminal_child_readback_missing:assignee")
    assignee = child["assignee"]
    if assignee is None:
        assignee_id = None
    elif isinstance(assignee, dict):
        assignee_id = assignee.get("id")
        if not isinstance(assignee_id, str) or not assignee_id:
            raise ChildClosureError("terminal_child_readback_invalid:assignee_id")
    else:
        raise ChildClosureError("terminal_child_readback_invalid:assignee")
    value = {
        "child_identifier": str(child.get("identifier") or "").upper(),
        "child_issue_id": child.get("id"),
        "parent_issue_id": (child.get("parent") or {}).get("id"),
        "workspace_id": ((child.get("team") or {}).get("organization") or {}).get("id"),
        "team_id": (child.get("team") or {}).get("id"),
        "project_id": (child.get("project") or {}).get("id"),
        "assignee_id": assignee_id,
        "state_id": child.get("state_id"),
        "state_name": child.get("status"),
        "state_type": state_type,
    }
    if set(value) != CHILD_READBACK_FIELDS:
        raise ChildClosureError("invalid_terminal_child_readback")
    for field in CHILD_READBACK_FIELDS - {"assignee_id"}:
        if not isinstance(value[field], str) or not value[field]:
            raise ChildClosureError(f"terminal_child_readback_missing:{field}")
    if value["assignee_id"] is not None and (
        not isinstance(value["assignee_id"], str) or not value["assignee_id"]
    ):
        raise ChildClosureError("terminal_child_readback_invalid:assignee_id")
    if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", value["child_identifier"]):
        raise ChildClosureError("terminal_child_readback_invalid:child_identifier")
    if state_type != "completed":
        raise ChildClosureError("terminal_child_not_completed")
    return value


def child_readback_sha256(child: dict[str, Any]) -> str:
    return canonical_digest(terminal_child_readback(child))


def evidence_receipts_sha256(contracts: list[dict[str, Any]]) -> str:
    receipts: list[dict[str, Any]] = []
    for contract in contracts:
        layers = contract.get("layers") or {}
        for name in sorted(layers):
            layer = layers[name]
            for receipt in (layer.get("receipts") or []):
                receipts.append({"layer": name, "receipt": receipt})
    receipts.sort(key=lambda item: json.dumps(
        item, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ))
    return canonical_digest(receipts)
