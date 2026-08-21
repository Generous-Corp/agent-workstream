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


def choose_disposition(snapshot: dict[str, Any], *, remote_head: str | None = None) -> dict[str, Any]:
    root = snapshot.get("root") or {}
    token = str(root.get("identifier", "")).upper()
    if not TOKEN.fullmatch(token):
        raise SuccessorError("durable root identifier must be a Linear issue token")
    provenance = snapshot.get("provenance") or {}
    worktree = provenance.get("worktree") or {}
    state = str(worktree.get("state", "unavailable")).lower()
    local_head = worktree.get("head")
    if state == "safe" and remote_head and local_head == remote_head:
        return {"disposition": "attach", "workstream": token, "head": remote_head,
                "reason": "worktree is current and matches remote truth"}
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
    return {"disposition": "create_successor", "workstream": token, "reason": reason,
            "predecessor": {"path": worktree.get("path"), "head": local_head, "state": state}}


def successor_command(snapshot: dict[str, Any], *, remote_repo: str, remote_ref: str,
                      successor_path: str, branch: str,
                      remote_head: str | None = None) -> dict[str, Any]:
    if not remote_head or not GIT_OID.fullmatch(remote_head):
        raise SuccessorError("verified full remote head is required for successor creation")
    worktree = (snapshot.get("provenance") or {}).get("worktree") or {}
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
