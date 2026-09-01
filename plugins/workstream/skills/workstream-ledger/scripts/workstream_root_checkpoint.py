#!/usr/bin/env python3
"""Preview or persist one authenticated root material-boundary checkpoint."""

from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Callable

from workstream_checkpoint import build_checkpoint, CheckpointError
from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_linear import (
    bootstrap_linear_route, HttpGraphQLClient, LinearGraphQLTransport,
    LinearTransportError,
)
from workstream_linear_events import (
    ledger_boundary_slot_id, ledger_serialization_frontier, material_frontier,
    reduce_event_comments,
)
from workstream_linear_checkpoints import LinearCheckpointAdapter
from workstream_linear_projection import (
    build_projection_event, LinearProjectionAdapter,
    reduce_projection_comments, select_plan_generation,
)
from workstream_root_transition import _validate_authority


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _json_argument(value: str, *, field: str, expected: type) -> Any:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise ValueError(f"{field} must be valid JSON") from error
    if not isinstance(parsed, expected):
        raise ValueError(f"{field} must be a JSON {expected.__name__}")
    return parsed


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("token")
    value.add_argument("--config")
    value.add_argument("--linear-workspace-id")
    value.add_argument("--linear-team-id")
    value.add_argument("--linear-project-id")
    value.add_argument(
        "--linear-endpoint", default="https://api.linear.app/graphql",
    )
    value.add_argument("--boundary-id")
    value.add_argument("--created-at", required=True)
    value.add_argument("--agent", required=True)
    value.add_argument("--provider", required=True)
    value.add_argument("--session-id", required=True)
    value.add_argument("--machine", required=True)
    value.add_argument("--worktree-state", required=True)
    value.add_argument("--worktree-path")
    value.add_argument("--worktree-branch")
    value.add_argument("--worktree-head")
    value.add_argument("--exact-head")
    value.add_argument("--before-status", required=True)
    value.add_argument("--after-status", required=True)
    value.add_argument("--evidence-json", default="[]")
    value.add_argument("--blocker-json")
    value.add_argument("--next-action", required=True)
    value.add_argument("--expected-material-revision", type=int)
    value.add_argument("--expected-preview-sha256")
    value.add_argument("--apply", action="store_true")
    return value


def _client_and_route(args: argparse.Namespace, factory: Callable[..., Any]):
    configured, _ = resolve_linear_route(
        config_path=args.config, workspace_id=args.linear_workspace_id,
        team_id=args.linear_team_id, project_id=args.linear_project_id,
    )
    token = load_linear_api_key()
    if not token:
        raise LinearTransportError("linear_auth_unavailable")
    client = factory(token, args.linear_endpoint)
    route = bootstrap_linear_route(client, args.token.upper())
    if configured and any(
        configured.get(key) != route.get(key)
        for key in ("workspace_id", "team_id", "project_id")
    ):
        raise LinearTransportError("checkpoint_route_mismatch")
    return client, route


def _prepare(args: argparse.Namespace, client: Any, route: dict[str, str]):
    workstream_id = args.token.upper()
    linear = LinearGraphQLTransport(
        client, workspace_id=route["workspace_id"], team_id=route["team_id"],
        project_id=route["project_id"],
    )
    graph_before = linear.snapshot_for_root(
        workstream_id, include_description=True, include_child_comments=True,
    )
    adapter = LinearCheckpointAdapter(
        client, issue_id=workstream_id, workstream_id=workstream_id,
        issue_uuid=route["root_issue_id"], **{
            key: route[key] for key in (
                "workspace_id", "team_id", "project_id",
            )
        },
    )
    comments = adapter._comments()
    graph_after = linear.snapshot_for_root(
        workstream_id, include_description=True, include_child_comments=True,
    )
    if graph_before != graph_after:
        raise LinearTransportError("checkpoint_graph_changed_during_read")
    root = graph_after["root"]
    _validate_authority(graph_after, workstream_id, route)
    if root.get("status") != args.before_status:
        raise LinearTransportError("checkpoint_before_status_readback_mismatch")
    selected = select_plan_generation(
        comments, workstream_id=workstream_id,
        description_plan_revision=root.get("plan_revision"),
        authenticated_route=route,
    )
    plan_revision = selected["plan_revision"]
    projection = reduce_projection_comments(
        comments, workstream_id=workstream_id,
        expected_plan_revision=plan_revision, authenticated_route=route,
    )
    source = projection.snapshot.get("source") or {}
    source = {
        "identity": source.get("identity") or source.get("url"),
        "sha256": source.get("sha256"),
    }
    if source["sha256"] != plan_revision or not source["identity"]:
        raise LinearTransportError("checkpoint_active_source_incomplete")
    material = reduce_event_comments(comments, workstream_id=workstream_id)
    state = adapter._reduce(comments)
    generations = adapter._recover_checkpoint_generations(state)
    latest = generations.get(plan_revision)
    latest_record = next((
        item for item in state.checkpoints
        if latest is not None
        and item["event_id"] == latest["checkpoint_event_id"]
    ), None)
    predecessor = latest["checkpoint_event_id"] if latest else None
    worktree = {"state": args.worktree_state}
    for key, value in (
        ("path", args.worktree_path), ("branch", args.worktree_branch),
        ("head", args.worktree_head),
    ):
        if value is not None:
            worktree[key] = value
    evidence = _json_argument(
        args.evidence_json, field="evidence-json", expected=list,
    )
    blocker = (
        None if args.blocker_json is None else _json_argument(
            args.blocker_json, field="blocker-json", expected=dict,
        )
    )

    boundary_id = f"material-{material.revision}"
    if args.boundary_id is not None and args.boundary_id != boundary_id:
        raise LinearTransportError(
            f"checkpoint_boundary_id_mismatch:expected={boundary_id}"
        )

    def build(predecessor_event_id: str | None):
        return build_checkpoint(
            workstream_id=workstream_id, boundary_id=boundary_id,
            root_revision=material.revision, plan_revision=plan_revision,
            before_status=args.before_status, after_status=args.after_status,
            execution={
                "agent": args.agent, "provider": args.provider,
                "session_id": args.session_id, "machine": args.machine,
                "worktree": worktree,
            },
            exact_head=args.exact_head, evidence=evidence, blocker=blocker,
            next_action=args.next_action,
            predecessor_event_id=predecessor_event_id,
        )

    checkpoint = build(predecessor)
    frontier_before = sorted(item["event_id"] for item in state.checkpoints)
    if latest_record and latest_record["boundary_id"] == boundary_id:
        replay = build(latest_record.get("predecessor_event_id"))
        if replay["event_id"] != latest_record["event_id"]:
            raise LinearTransportError("checkpoint_boundary_id_conflict")
        checkpoint = replay
        frontier_before = [
            event_id for event_id in frontier_before
            if event_id != latest_record["event_id"]
        ]
    disposition = projection.snapshot.get("disposition") or {}
    disposition_head = disposition.get("remote_head")
    if args.exact_head is not None and disposition_head not in {
        None, args.exact_head,
    }:
        raise LinearTransportError("checkpoint_disposition_head_mismatch")
    desired_disposition = {
        "disposition": disposition.get("disposition") or "attach",
        "remote_head": args.exact_head or disposition_head,
        "recovered_from_checkpoint": checkpoint["event_id"],
    }
    current_disposition = next((
        event for event in reversed(projection.events)
        if event["kind"] == "disposition" and event["key"] == "root"
    ), None)
    projection_replay = (
        latest is not None
        and disposition == desired_disposition
        and current_disposition is not None
        and projection.events[-1] == current_disposition
        and current_disposition["created_at"] == args.created_at
    )
    if latest is not None and disposition == desired_disposition and not projection_replay:
        raise LinearTransportError("checkpoint_projection_replay_superseded")
    candidate = (
        deepcopy(current_disposition) if projection_replay else build_projection_event(
            workstream_id=workstream_id, kind="disposition", key="root",
            value=desired_disposition, plan_revision=plan_revision,
            expected_revision=projection.revision,
            created_at=args.created_at,
            supersedes_event_id=(
                current_disposition["event_id"] if current_disposition else None
            ),
            authority=route,
        )
    )
    checkpoint_ids = sorted(item["event_id"] for item in state.checkpoints)
    if projection_replay:
        checkpoint_ids = [
            event_id for event_id in checkpoint_ids
            if event_id != checkpoint["event_id"]
        ]
    serialization_comments = comments
    if projection_replay:
        checkpoint_remote_id = state.remote_ids.get(checkpoint["event_id"])
        serialization_comments = [
            comment for comment in comments
            if comment.get("id") != checkpoint_remote_id
        ]
    serialization = ledger_serialization_frontier(
        checkpoint_ids, serialization_comments, workstream_id=workstream_id,
        authenticated_route=route, current_plan_revision=plan_revision,
        material_revision=material.revision,
    )
    contract = {
        "schema_version": 1, "command": "checkpoint", "apply": False,
        "writes_performed": 0, "workstream_id": workstream_id,
        "authenticated_route": route, "source": source,
        "description_plan_revision": root.get("plan_revision"),
        "active_generation": {
            key: selected.get(key) for key in (
                "plan_revision", "activation_epoch",
                "transition_tip_event_id", "authority_origin",
            )
        },
        "material_revision": material.revision,
        "material_event_ids": sorted(event.event_id for event in material.events),
        "material_frontier": material_frontier(material),
        "checkpoint_frontier_before": frontier_before,
        "ledger_serialization_frontier": serialization,
        "deterministic_slot_id": ledger_boundary_slot_id(
            workstream_id, material.revision, serialization, route,
        ),
        "graph_sha256": _digest(graph_after),
        "root_state": {
            "id": root.get("id"), "identifier": root.get("identifier"),
            "status": root.get("status"), "status_type": root.get("status_type"),
        },
        "checkpoint": checkpoint,
        "projection_candidate": candidate,
    }
    return (
        {**contract, "preview_sha256": _digest(contract)}, adapter,
        projection_replay,
    )


def _root_surface(client: Any, route: dict[str, str], token: str) -> dict[str, Any]:
    graph = LinearGraphQLTransport(
        client, workspace_id=route["workspace_id"], team_id=route["team_id"],
        project_id=route["project_id"],
    ).snapshot_for_root(token, include_description=True)
    _validate_authority(graph, token, route)
    root = graph["root"]
    return {
        "id": root.get("id"), "identifier": root.get("identifier"),
        "parent": root.get("parent"), "archivedAt": root.get("archivedAt"),
        "status": root.get("status"), "status_type": root.get("status_type"),
        "description_plan_revision": root.get("plan_revision"),
    }


def _ordinary_resume(token: str) -> dict[str, Any]:
    result = subprocess.run(
        [
            sys.executable, str(Path(__file__).with_name("workstream_resume.py")),
            token,
        ],
        check=False, capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        raise LinearTransportError(
            "checkpoint_ordinary_resume_refused:" + result.stderr.strip()
        )
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise LinearTransportError("checkpoint_ordinary_resume_invalid_json") from error
    if (
        value.get("resume_authority") != "full"
        or value.get("executable") is not True
        or len(result.stdout.encode()) > 24 * 1024
        or value.get("dependency_graph") is None
    ):
        raise LinearTransportError("checkpoint_ordinary_resume_not_bounded_full")
    return value


def run(
    argv: list[str], *, client_factory: Callable[..., Any] = HttpGraphQLClient,
) -> dict[str, Any]:
    args = parser().parse_args(argv)
    if args.apply and (
        args.expected_material_revision is None
        or not args.expected_preview_sha256
    ):
        raise ValueError(
            "checkpoint apply requires expected material revision and preview digest"
        )
    client, route = _client_and_route(args, client_factory)
    preview, adapter, projection_replay = _prepare(args, client, route)
    if not args.apply:
        return preview
    if args.expected_material_revision != preview["material_revision"]:
        raise LinearTransportError("checkpoint_material_revision_fence_mismatch")
    if args.expected_preview_sha256 != preview["preview_sha256"]:
        raise LinearTransportError("checkpoint_preview_digest_mismatch")
    # A second complete read is the immediate pre-write CAS fence.
    fenced, adapter, projection_replay = _prepare(args, client, route)
    if fenced != preview:
        raise LinearTransportError("checkpoint_preview_stale_reload_required")
    native_before = _root_surface(client, route, preview["workstream_id"])
    if native_before != {
        **preview["root_state"], "parent": None, "archivedAt": None,
        "description_plan_revision": preview["description_plan_revision"],
    }:
        raise LinearTransportError("checkpoint_native_root_prewrite_drift")
    before_persist = adapter._state()
    already_persisted = any(
        item["event_id"] == preview["checkpoint"]["event_id"]
        for item in before_persist.checkpoints
    )
    def validate_immediate_prewrite() -> None:
        if _root_surface(client, route, preview["workstream_id"]) != native_before:
            raise LinearTransportError("checkpoint_native_root_prewrite_drift")

    try:
        receipt = adapter.persist(
            preview["checkpoint"], prewrite_validator=validate_immediate_prewrite,
        )
    except (LinearTransportError, OSError, TimeoutError) as error:
        if str(error) == "checkpoint_native_root_prewrite_drift":
            raise
        try:
            observed = adapter._state()
            receipt = next(
                item for item in observed.checkpoints
                if item["event_id"] == preview["checkpoint"]["event_id"]
            )
        except (LinearTransportError, OSError, StopIteration, TimeoutError):
            raise LinearTransportError(
                "checkpoint_apply_unknown_replay_required"
            ) from error
    if not projection_replay:
        projection_adapter = LinearProjectionAdapter(
            client, issue_id=preview["workstream_id"],
            workstream_id=preview["workstream_id"],
            plan_revision=preview["source"]["sha256"], **route,
        )
        try:
            projection_adapter.append(
                preview["projection_candidate"],
                expected_material_revision=preview["material_revision"],
            )
        except (LinearTransportError, OSError, TimeoutError) as error:
            try:
                state = projection_adapter.state()
                if not any(
                    event == preview["projection_candidate"]
                    for event in state.events
                ):
                    raise StopIteration
            except (LinearTransportError, OSError, StopIteration, TimeoutError):
                raise LinearTransportError(
                    "checkpoint_projection_apply_unknown_replay_required"
                ) from error
    if _root_surface(client, route, preview["workstream_id"]) != native_before:
        raise LinearTransportError(
            "checkpoint_postwrite_native_root_drift_reconcile_required"
        )
    try:
        resume = _ordinary_resume(preview["workstream_id"])
    except (LinearTransportError, OSError, TimeoutError) as error:
        raise LinearTransportError(
            "checkpoint_applied_but_ordinary_resume_refused"
        ) from error
    latest_checkpoint = resume.get("latest_checkpoint") or {}
    if (
        resume.get("plan_revision") != preview["source"]["sha256"]
        or latest_checkpoint.get("checkpoint_event_id") != receipt["event_id"]
        or latest_checkpoint.get("root_revision") != preview["material_revision"]
        or (latest_checkpoint.get("acknowledgement") or {}).get("remote_id")
        != receipt["acknowledgement"]["remote_id"]
    ):
        raise LinearTransportError(
            "checkpoint_applied_but_ordinary_resume_receipt_mismatch"
        )
    return {
        **preview, "apply": True,
        "writes_performed": (
            (0 if already_persisted else 1) + (0 if projection_replay else 1)
        ),
        "receipt": receipt, "resume_authority": resume["resume_authority"],
    }


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(sys.argv[1:] if argv is None else argv)
    except (
        CheckpointError, LinearTransportError, OSError, TimeoutError, ValueError,
    ) as error:
        print(f"workstream checkpoint refused: {error}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
