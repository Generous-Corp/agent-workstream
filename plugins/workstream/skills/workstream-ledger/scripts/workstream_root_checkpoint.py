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

from workstream_checkpoint import (
    build_checkpoint, CheckpointError, canonical_authority_tip,
    recover_latest,
)
from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_linear import (
    bootstrap_linear_route, HttpGraphQLClient, LinearGraphQLTransport,
    LinearTransportError,
)
from workstream_linear_events import (
    canonical_authenticated_source, LinearCommentEventAdapter,
    ledger_boundary_slot_id, ledger_serialization_frontier, material_frontier,
    reduce_event_comments,
)
from workstream_linear_checkpoints import LinearCheckpointAdapter, LinearCheckpointError
from workstream_linear_checkpoints import encode_checkpoint_comment
from workstream_linear_projection import (
    build_projection_event, LinearProjectionAdapter,
    encode_projection_comment, reduce_projection_comments,
    LinearProjectionError, projection_slot_id, select_plan_generation,
)
from workstream_root_transition import _validate_authority
from workstream_child_dependencies import LinearChildDependencyAdapter


class CheckpointPartialApplyError(LinearTransportError):
    """A checkpoint may be durable; expose exact replay authority."""

    def __init__(self, payload: dict[str, Any]):
        self.payload = payload
        super().__init__(str(payload["reason"]))


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _valid_fixed_frontier_shape(value: dict[str, Any]) -> bool:
    """Require the producer's executable frontier and hydration contract."""
    scope = value.get("authority_scope")
    frontier = value.get("execution_frontier")
    deferred = value.get("deferred_audit_detail")
    required_frontier = {
        "root", "children", "obligations", "decisions", "choices",
        "dependencies", "child_dependency_graph", "columns", "checkpoint",
        "disposition",
    }
    required_deferred = {
        "state", "hydration_required_before_action", "algorithm", "fields",
        "fields_sha256", "full_context_sha256", "hydration_selectors",
        "obligation_selector_rules", "hydration_recipe",
        "original_context_bytes", "audit_route", "full_history_route",
    }
    return (
        isinstance(scope, dict)
        and set(scope) == {
            "history_validation", "execution_frontier", "item_count",
            "omitted_items_claimed_executable", "truncated_cell_count",
            "truncated_cell_marker", "truncated_cell_rule",
        }
        and scope.get("history_validation") == "complete_authenticated"
        and scope.get("execution_frontier")
        == "complete_digest_bound_excerpts"
        and scope.get("omitted_items_claimed_executable") is False
        and type(scope.get("item_count")) is int
        and 0 <= scope["item_count"] <= 100
        and isinstance(frontier, dict)
        and set(frontier) == required_frontier
        and isinstance(frontier.get("root"), dict)
        and set(frontier["root"]) == {"status", "next", "blocker"}
        and all(isinstance(frontier.get(key), list) for key in (
            "children", "obligations", "decisions", "choices", "dependencies",
        ))
        and isinstance(frontier.get("child_dependency_graph"), dict)
        and set(frontier["child_dependency_graph"]) == {"authority", "relations"}
        and isinstance(frontier["child_dependency_graph"]["relations"], list)
        and isinstance(frontier.get("columns"), dict)
        and set(frontier["columns"]) == {
            "children", "obligations", "decisions", "choices",
            "dependencies", "child_dependency_graph.relations",
        }
        and scope["item_count"] == (
            1 + sum(len(frontier[key]) for key in (
                "children", "obligations", "decisions", "choices",
                "dependencies",
            )) + len(frontier["child_dependency_graph"]["relations"])
        )
        and isinstance(deferred, dict)
        and set(deferred) == required_deferred
        and deferred.get("state") == "fixed_frontier_authority_envelope"
        and deferred.get("hydration_required_before_action") is True
        and deferred.get("algorithm") == "fixed-six-slot-frontier-v1"
        and isinstance(deferred.get("fields"), list)
        and isinstance(deferred.get("hydration_selectors"), dict)
        and isinstance(deferred.get("obligation_selector_rules"), dict)
        and isinstance(deferred.get("hydration_recipe"), str)
        and isinstance(deferred.get("original_context_bytes"), int)
        and deferred["original_context_bytes"] > 24 * 1024
        and isinstance(deferred.get("audit_route"), dict)
        and isinstance(deferred.get("full_history_route"), dict)
    )


def _normalized_tip(
    checkpoints: list[dict[str, Any]], token: str, plan_revision: str,
) -> dict[str, Any]:
    try:
        return canonical_authority_tip(recover_latest(
            checkpoints, token,
            expected_plan_revision=plan_revision,
        ))
    except (CheckpointError, KeyError, IndexError) as error:
        raise LinearTransportError("checkpoint_normalized_tip_unavailable") from error


def _root_authority_graph(graph: dict[str, Any]) -> dict[str, Any]:
    """Exclude only Linear's self-mutating root comment timestamp.

    ``commentCreate`` advances the parent issue's ``updatedAt`` even though it
    does not change any native workstream authority.  The exact timestamp is
    still fenced immediately before a write by ``_root_surface``; this
    normalized graph is used only to compare authority after our own append
    and to make a checkpoint-only replay deterministic.
    """
    normalized = deepcopy(graph)
    normalized["root"].pop("updatedAt", None)
    return normalized


def _validate_proposed_full_authority(
    *, client: Any, graph: dict[str, Any], comments: list[dict[str, Any]],
    checkpoint: dict[str, Any], checkpoint_remote_id: str,
    projection_candidate: dict[str, Any], checkpoint_replay: bool,
    projection_replay: bool, workstream_id: str, route: dict[str, str],
    source: dict[str, str], selected_generation: dict[str, Any],
) -> None:
    """Run resume's authority join against the exact proposed durable state."""
    from workstream_linear_projection import (
        bind_active_plan_generation, child_mutation_authorizations_from_comments,
    )
    from workstream_generation import (
        assert_generation_write_authority,
        assert_no_pending_generation_reservation,
    )
    from workstream_plan import plan_payload
    from workstream_relation_readback import read_relation_targets
    from workstream_resume import (
        add_live_child_material_history, add_material_history,
        ResumeError, compact_context, validate_snapshot,
    )

    proposed_comments = deepcopy(comments)
    if not checkpoint_replay:
        proposed_comments.append({
            "id": checkpoint_remote_id,
            "body": encode_checkpoint_comment(checkpoint),
            "createdAt": checkpoint["execution"]["session_id"],
            "updatedAt": checkpoint["execution"]["session_id"],
        })
    if not projection_replay:
        proposed_comments.append({
            "id": projection_slot_id(
                workstream_id, projection_candidate["plan_revision"],
                projection_candidate["expected_revision"], route,
            ),
            "body": encode_projection_comment(projection_candidate),
            "createdAt": projection_candidate["created_at"],
            "updatedAt": projection_candidate["created_at"],
        })
    try:
        authenticated_source = plan_payload(
            source["identity"], source["identity"],
        )["source"]
        proposed_graph = bind_active_plan_generation(
            deepcopy(graph), proposed_comments, workstream_id=workstream_id,
            selected=selected_generation, authenticated_route=route,
        )
        assert_no_pending_generation_reservation(
            proposed_comments, workstream_id=workstream_id,
            authenticated_route=route,
        )
        assert_generation_write_authority(
            proposed_comments, workstream_id=workstream_id,
            plan_revision=selected_generation["plan_revision"],
            authenticated_route=route,
        )
        mutation_authorizations = child_mutation_authorizations_from_comments(
            proposed_comments, workstream_id=workstream_id,
            description_plan_revision=selected_generation[
                "description_plan_revision"
            ], authenticated_route=route,
        )
        if mutation_authorizations:
            proposed_graph = LinearGraphQLTransport(
                client, workspace_id=route["workspace_id"],
                team_id=route["team_id"], project_id=route["project_id"],
            ).recover_authorized_children(
                proposed_graph, mutation_authorizations,
            )
        proposed_graph = add_live_child_material_history(
            proposed_graph, authenticated_route=route,
            root_comments=proposed_comments,
        )
        # Re-run the production native dependency join against the proposed
        # durable comments.  The producer snapshot is only a candidate: the
        # root/child graph and relation edges must come from an exact Linear
        # readback, with material/projection frontiers authenticated before any
        # checkpoint or disposition append.
        dependency_adapter = LinearChildDependencyAdapter(
            client, workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
            root_issue_id=route["root_issue_id"], root_identifier=workstream_id,
            plan_revision=selected_generation["plan_revision"],
        )
        baseline_comments = {
            item.get("id"): item for item in comments
        }

        def dependency_reread() -> tuple[dict[str, Any], list[dict[str, Any]]]:
            fresh_graph = LinearGraphQLTransport(
                client, workspace_id=route["workspace_id"],
                team_id=route["team_id"], project_id=route["project_id"],
            ).snapshot_for_root(
                workstream_id, include_description=True,
                include_child_comments=True,
            )
            fresh_comments = LinearCommentEventAdapter(
                client, issue_id=workstream_id,
                workspace_id=route["workspace_id"], team_id=route["team_id"],
                project_id=route["project_id"],
            ).comments()
            fresh_by_id = {item.get("id"): item for item in fresh_comments}
            # The proposed checkpoint/projection comments are not remote yet;
            # every pre-existing comment must nevertheless be byte-identical
            # to this fresh read.  Never let a stale in-memory proposal mask a
            # concurrent Linear frontier change.
            if any(fresh_by_id.get(key) != value for key, value in baseline_comments.items()):
                raise LinearTransportError(
                    "checkpoint_dependency_frontier_changed_during_read"
                )
            merged = list(fresh_comments)
            for item in proposed_comments:
                if item.get("id") not in fresh_by_id:
                    merged.append(item)
            return fresh_graph, merged

        proposed_graph["dependency_graph"] = (
            dependency_adapter.read_authorized_graph_for_snapshot(
                proposed_graph, proposed_comments,
                generation_selector_plan_revision=(
                    proposed_graph["root"].get("plan_revision")
                ),
                reread=dependency_reread,
            )
        )
        joined = add_material_history(
            proposed_graph, proposed_comments, workstream_id,
            authenticated_route=route, authenticated_source=authenticated_source,
            relation_target_resolver=lambda relations: read_relation_targets(
                client, relations,
            ),
        )
        validate_snapshot(
            joined, workstream_id, require_projection_authority=True,
            # Legacy roots with no dependency declarations have no graph to
            # authenticate.  Whenever the snapshot carries dependency
            # authority, require the same strict graph join as production
            # resume (including relation-target readback).
            require_dependency_graph=bool(
                joined.get("dependencies") or joined.get("dependency_graph")
            ),
        )
        # Exercise the exact producer-side compact envelope and limits used by
        # ordinary resume.  This catches an over-budget or over-item proposal
        # before either append; the post-write command still performs its own
        # authenticated reread.
        if joined.get("dependency_graph") is not None:
            compact = compact_context(
                joined, workstream_id, max_bytes=24 * 1024, max_items=100,
                require_projection_authority=True,
                require_dependency_graph=True,
            )
            if len(json.dumps(compact, ensure_ascii=False,
                              sort_keys=True, separators=(",", ":")).encode()) > 24 * 1024:
                raise ResumeError("resume_context_over_budget")
    except (LinearProjectionError, ResumeError, LinearTransportError) as error:
        raise LinearTransportError(
            "checkpoint_proposed_resume_refused:" + str(error)
        ) from error


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
    checkpoint_replay = False
    frontier_before = sorted(item["event_id"] for item in state.checkpoints)
    if latest_record and latest_record["boundary_id"] == boundary_id:
        replay = build(latest_record.get("predecessor_event_id"))
        if replay["event_id"] != latest_record["event_id"]:
            raise LinearTransportError("checkpoint_boundary_id_conflict")
        checkpoint = replay
        checkpoint_replay = True
        frontier_before = [
            event_id for event_id in frontier_before
            if event_id != latest_record["event_id"]
        ]
    elif latest is not None and latest["root_revision"] >= material.revision:
        # The selected activation already covers this frontier.  Refuse during
        # preview; persist() would otherwise discover the same non-monotonic
        # condition only after its second read.
        raise LinearTransportError("checkpoint_successor_revision_not_monotonic")
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
    unresolved_quarantine = projection.snapshot.get(
        "projection_unresolved_quarantine"
    ) or []
    if unresolved_quarantine:
        raise LinearTransportError(
            "checkpoint_projection_unresolved_quarantine_refused"
        )
    projection_quarantine = projection.snapshot.get(
        "projection_quarantined"
    ) or []
    projection_frontier = {
        "revision": projection.revision,
        "event_ids": [event["event_id"] for event in projection.events],
        "remote_ids": {
            event["event_id"]: projection.remote_ids.get(event["event_id"])
            for event in projection.events
        },
        "events_sha256": _digest(list(projection.events)),
        "quarantined_sha256": _digest(projection_quarantine),
    }
    checkpoint_ids = sorted(item["event_id"] for item in state.checkpoints)
    if checkpoint_replay:
        checkpoint_ids = [
            event_id for event_id in checkpoint_ids
            if event_id != checkpoint["event_id"]
        ]
    serialization_comments = comments
    if checkpoint_replay:
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
        "graph_sha256": _digest(_root_authority_graph(graph_after)),
        "root_updated_at": root.get("updatedAt"),
        "root_state": {
            "id": root.get("id"), "identifier": root.get("identifier"),
            "status": root.get("status"), "status_type": root.get("status_type"),
        },
        "checkpoint": checkpoint,
        "projection_candidate": candidate,
        "projection_frontier": projection_frontier,
        "projection_quarantine": projection_quarantine,
    }
    _validate_proposed_full_authority(
        client=client, graph=graph_after, comments=comments,
        checkpoint=checkpoint,
        checkpoint_remote_id=contract["deterministic_slot_id"],
        projection_candidate=candidate, checkpoint_replay=checkpoint_replay,
        projection_replay=projection_replay, workstream_id=workstream_id,
        route=route, source=source, selected_generation=selected,
    )
    return (
        {**contract, "preview_sha256": _digest(contract)}, adapter,
        projection_replay,
    )


def _root_surface(client: Any, route: dict[str, str], token: str) -> dict[str, Any]:
    graph = LinearGraphQLTransport(
        client, workspace_id=route["workspace_id"], team_id=route["team_id"],
        project_id=route["project_id"],
    ).snapshot_for_root(
        token, include_description=True, include_child_comments=True,
    )
    _validate_authority(graph, token, route)
    return {
        "graph": graph,
        "graph_sha256": _digest(graph),
        "authority_graph_sha256": _digest(_root_authority_graph(graph)),
    }


def _same_root_authority(
    observed: dict[str, Any], expected: dict[str, Any],
) -> bool:
    """Compare every native root field except its own comment timestamp."""
    return (
        observed.get("authority_graph_sha256")
        == expected.get("authority_graph_sha256")
    )


def _comments_match_owned_append(
    before: list[dict[str, Any]], after: list[dict[str, Any]], *,
    remote_id: str, wrote: bool,
) -> bool:
    """Prove a stage changed the root ledger by exactly its own receipt."""
    before_by_id = {item.get("id"): item for item in before}
    after_by_id = {item.get("id"): item for item in after}
    if len(before_by_id) != len(before) or len(after_by_id) != len(after):
        return False
    if any(after_by_id.get(key) != value for key, value in before_by_id.items()):
        return False
    expected_ids = set(before_by_id)
    if wrote:
        if remote_id in expected_ids:
            return False
        expected_ids.add(remote_id)
    return set(after_by_id) == expected_ids


def _compensate_checkpoint_projection(
    *, client: Any, preview: dict[str, Any], route: dict[str, str],
    native_before: dict[str, Any], checkpoint_receipt: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any] | None:
    """Append at most one safe disposition compensation for a checkpoint.

    Linear cannot atomically append the checkpoint and its projection.  This
    narrow repair is deliberately conservative: it is eligible only when a
    fresh read proves that all authority except ``recovered_from_checkpoint``
    is unchanged and the active disposition still names the checkpoint's exact
    head.  Any ambiguity or race returns ``None``; callers must surface their
    existing partial-apply handle rather than retrying or guessing.
    """
    checkpoint = preview["checkpoint"]
    if (checkpoint_receipt.get("event_id") != checkpoint.get("event_id")
            or (checkpoint_receipt.get("acknowledgement") or {}).get("state")
            != "remote_acknowledged"):
        return None
    # Compensation is reserved for the known strict-resume stale-pointer
    # incident.  Budget, source, dependency, lifecycle, and transport errors
    # must remain partial/refused rather than triggering a speculative write.
    # The native root and complete comment graph are the first fence.  A
    # comment append legitimately changes updatedAt, so compare the normalized
    # authority graph only.
    try:
        observed_native = _root_surface(client, route, preview["workstream_id"])
    except (LinearTransportError, OSError, TimeoutError):
        return None
    if observed_native.get("authority_graph_sha256") != native_before.get(
            "authority_graph_sha256"):
        return None
    # Re-run the complete producer oracle after the checkpoint append.  This
    # is intentionally more than a projection reread: _prepare authenticates
    # the selected generation and source bytes, native lifecycle/children,
    # child material, relations and dependency graph, quarantine, material
    # frontier, and the canonical latest checkpoint.  The only compatible
    # difference is that the checkpoint is now an exact replay and the fresh
    # disposition candidate supersedes the still-stale predecessor pointer.
    try:
        fresh, _, fresh_projection_replay = _prepare(args, client, route)
    except (LinearProjectionError, LinearTransportError, OSError, TimeoutError):
        return None
    if (
        fresh_projection_replay
        or fresh["workstream_id"] != preview["workstream_id"]
        or fresh["source"] != preview["source"]
        or fresh["active_generation"] != preview["active_generation"]
        or fresh["material_revision"] != preview["material_revision"]
        or fresh["material_event_ids"] != preview["material_event_ids"]
        or fresh["material_frontier"] != preview["material_frontier"]
        or fresh["checkpoint"] != checkpoint
        or fresh["projection_frontier"] != preview["projection_frontier"]
        or fresh["projection_candidate"] != preview["projection_candidate"]
        or fresh["projection_quarantine"] != preview["projection_quarantine"]
    ):
        return None
    projection = LinearProjectionAdapter(
        client, issue_id=preview["workstream_id"],
        workstream_id=preview["workstream_id"],
        plan_revision=preview["source"]["sha256"], **route,
    )
    try:
        state = projection.state()
        candidate = fresh["projection_candidate"]
        if state.revision != candidate.get("expected_revision"):
            return None
        current = next((event for event in reversed(state.events)
                         if event["kind"] == "disposition"
                         and event["key"] == "root"), None)
    except (LinearProjectionError, LinearTransportError, OSError, TimeoutError):
        return None
    if current is None:
        return None
    value = current.get("value")
    if not isinstance(value, dict):
        return None
    expected = preview["projection_candidate"].get("value") or {}
    # Only the recovery pointer may differ.  Preserve the live mode/head and
    # reject a head mismatch rather than silently changing repository authority.
    if set(value) - {"recovered_from_checkpoint"} != set(expected) - {
            "recovered_from_checkpoint"}:
        return None
    for key in set(value) | set(expected):
        if key == "recovered_from_checkpoint":
            continue
        if value.get(key) != expected.get(key):
            return None
    exact_head = checkpoint.get("exact_head")
    if exact_head is not None and value.get("remote_head") != exact_head:
        return None
    # A newer checkpoint or a different checkpoint already represented in the
    # disposition makes this repair ambiguous.  Exact replay is a no-write.
    recovered = value.get("recovered_from_checkpoint")
    if recovered == checkpoint["event_id"]:
        return {
            "event_id": current["event_id"],
            "remote_id": state.remote_ids.get(current["event_id"]),
            "revision": state.revision,
            "disposition": "existing",
            "checkpoint_event_id": checkpoint["event_id"],
        }
    # A correction may advance only the exact disposition event reviewed by
    # the original preview, and a non-null pointer must be the checkpoint's
    # own canonical predecessor.  The exact projection-frontier equality above
    # proves that event has not been superseded or rebased since review.
    reviewed_predecessor_event = preview["projection_candidate"].get(
        "supersedes_event_id"
    )
    checkpoint_predecessor = checkpoint.get("predecessor_event_id")
    if (
        not isinstance(reviewed_predecessor_event, str)
        or current.get("event_id") != reviewed_predecessor_event
        or (
            recovered is not None
            and recovered != checkpoint_predecessor
        )
    ):
        return None
    # The fresh producer oracle already proved the acknowledged checkpoint is
    # the canonical latest record at this exact material frontier.  Retain the
    # remote-slot check because the reviewed receipt is the caller's durable
    # proof, not merely an equivalent checkpoint body found elsewhere.
    try:
        checkpoint_adapter = LinearCheckpointAdapter(
            client, issue_id=preview["workstream_id"],
            workstream_id=preview["workstream_id"], issue_uuid=route["root_issue_id"],
            workspace_id=route["workspace_id"], team_id=route["team_id"],
            project_id=route["project_id"],
        )
        checkpoint_state = checkpoint_adapter._state()
        observed = next((item for item in checkpoint_state.checkpoints
                         if item["event_id"] == checkpoint["event_id"]), None)
        # Compensation is valid only for the canonical latest checkpoint in
        # this plan generation. A newer material boundary must never be
        # overwritten by a pointer to an older checkpoint.
        generations = checkpoint_adapter._recover_checkpoint_generations(
            checkpoint_state
        )
        latest = generations.get(checkpoint["plan_revision"])
        ack = (checkpoint_receipt.get("acknowledgement") or {})
        if (observed is None
                or latest is None
                or latest.get("checkpoint_event_id") != checkpoint["event_id"]
                or latest.get("root_revision") != preview["material_revision"]
                or not isinstance(ack.get("remote_id"), str)
                or checkpoint_state.remote_ids.get(checkpoint["event_id"])
                != ack.get("remote_id")
                or (observed.get("acknowledgement") or {}) != ack
                or checkpoint["root_revision"] != preview["material_revision"]):
            return None
        # The durable body normalizes its acknowledgement after the remote
        # receipt. Compare all producer fields separately, excluding that
        # server-assigned acknowledgement rather than comparing pending vs
        # acknowledged records wholesale.
        if {
            key: value for key, value in observed.items()
            if key != "acknowledgement"
        } != {
            key: value for key, value in checkpoint.items()
            if key != "acknowledgement"
        }:
            return None
    except (LinearCheckpointError, LinearTransportError, OSError, TimeoutError):
        return None
    candidate = fresh["projection_candidate"]
    quarantined = state.snapshot.get("projection_quarantined") or []
    if (
        state.snapshot.get("projection_unresolved_quarantine")
        or quarantined != (preview.get("projection_quarantine") or [])
        or quarantined != (fresh.get("projection_quarantine") or [])
    ):
        return None
    try:
        receipt = projection.append(
            candidate, expected_material_revision=preview["material_revision"],
            expected_quarantine_count=len(quarantined),
            expected_quarantine_sha256=_digest(quarantined),
        )
    except (LinearProjectionError, LinearTransportError, OSError, TimeoutError):
        return None
    return {
        **receipt,
        "acknowledgement": {
            "state": "remote_acknowledged",
            "remote_id": receipt["remote_id"],
            "applied_revision": receipt["revision"],
        },
    }


def _partial_apply_error(
    reason: str, preview: dict[str, Any], *,
    checkpoint_receipt: dict[str, Any] | None = None,
    projection_receipt: dict[str, Any] | None = None,
    failure: dict[str, str] | None = None,
) -> CheckpointPartialApplyError:
    checkpoint = preview["checkpoint"]
    candidate = preview["projection_candidate"]
    payload = {
        "schema_version": 1,
        "status": "applied_or_unknown_replay_required",
        "reason": reason,
        "workstream_id": preview["workstream_id"],
        "reviewed_preview_sha256": preview["preview_sha256"],
        "checkpoint": {
            "event_id": checkpoint["event_id"],
            "expected_remote_id": preview["deterministic_slot_id"],
            "receipt": checkpoint_receipt,
        },
        "projection": {
            "event_id": candidate["event_id"],
            "expected_revision": candidate["expected_revision"],
            "receipt": projection_receipt,
        },
        "replay_guidance": (
            "Rerun the zero-write checkpoint preview against the exact durable "
            "receipts, review its fresh digest, then apply with the same material "
            "revision, timestamp, execution inputs, and authenticated route/source."
        ),
    }
    if failure is not None:
        payload["failure"] = failure
    return CheckpointPartialApplyError(payload)


def _ordinary_resume(
    token: str, *, args: argparse.Namespace, route: dict[str, str],
    source: dict[str, str], expected_checkpoint: dict[str, Any] | None = None,
    expected_remote_id: str | None = None,
) -> dict[str, Any]:
    command = [
        sys.executable, str(Path(__file__).with_name("workstream_resume.py")),
        token, "--linear-workspace-id", route["workspace_id"],
        "--linear-team-id", route["team_id"],
        "--linear-project-id", route["project_id"],
        "--linear-endpoint", args.linear_endpoint,
        "--plan-source", source["identity"],
        "--plan-identity", source["identity"],
        "--max-bytes", str(24 * 1024), "--max-items", "100",
    ]
    if args.config:
        command.extend(["--config", args.config])
    result = subprocess.run(
        command,
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
    if not isinstance(value, dict):
        raise LinearTransportError("checkpoint_ordinary_resume_invalid_json")
    source = canonical_authenticated_source(source)
    if (
        value.get("resume_authority") != "full"
        or value.get("workstream_id") != token
        or value.get("plan_revision") != source["sha256"]
        or len(result.stdout.encode()) > 24 * 1024
        or value.get("plan_generation_pending") is not None
    ):
        raise LinearTransportError("checkpoint_ordinary_resume_not_bounded_full")
    dependency_graph = value.get("dependency_graph")
    if isinstance(dependency_graph, dict):
        if (value.get("source") != source
                or value.get("authenticated_route") != route
                or dependency_graph.get("route") != route
                or dependency_graph.get("plan_revision") != source["sha256"]):
            raise LinearTransportError("checkpoint_ordinary_resume_not_bounded_full")
    else:
        schema = value.get("context_schema")
        frontier = value.get("execution_frontier")
        checkpoint = frontier.get("checkpoint") if isinstance(frontier, dict) else None
        binding = value.get("authority_binding")
        if (not isinstance(schema, dict)
                or set(schema) != {"name", "version", "representation", "envelope"}
                or schema.get("name") != "agent-workstream.resume-context"
                or schema.get("version") != 2
                or schema.get("representation") != "compact_validated"
                or schema.get("envelope") != "fixed_frontier_authority_v1"
                or not isinstance(binding, dict)
                or set(binding) != {"route_sha256", "source_sha256", "checkpoint_sha256"}
                or binding.get("route_sha256") != _digest(route)
                or binding.get("source_sha256") != _digest(source)
                or not _valid_fixed_frontier_shape(value)
                or not isinstance(frontier, dict)
                or not (
                    isinstance(checkpoint, str)
                    and len(checkpoint.encode("utf-8")) <= 24
                    and __import__("re").fullmatch(r".{0,15}~#[0-9a-f]{8}", checkpoint)
                )):
            raise LinearTransportError("checkpoint_ordinary_resume_not_bounded_full")
        if expected_checkpoint is not None:
            try:
                expected_tip = canonical_authority_tip(expected_checkpoint)
            except CheckpointError as error:
                raise LinearTransportError("checkpoint_expected_tip_invalid") from error
            if binding.get("checkpoint_sha256") != _digest(expected_tip):
                raise LinearTransportError("checkpoint_ordinary_resume_checkpoint_mismatch")
    return value


def _is_exact_stale_checkpoint_resume_refusal(error: BaseException) -> bool:
    """Accept only the two canonical local/production error envelopes."""
    return str(error) in {
        (
            "checkpoint_ordinary_resume_refused:"
            "disposition_checkpoint_stale_reconcile_required"
        ),
        (
            "checkpoint_ordinary_resume_refused:workstream resume refused: "
            "disposition_checkpoint_stale_reconcile_required"
        ),
    }


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
    if (
        native_before["authority_graph_sha256"] != preview["graph_sha256"]
        or native_before["graph"]["root"].get("updatedAt")
        != preview["root_updated_at"]
    ):
        raise LinearTransportError("checkpoint_native_root_prewrite_drift")
    before_persist = adapter._state()
    comments_before_checkpoint = adapter._comments()
    already_persisted = any(
        item["event_id"] == preview["checkpoint"]["event_id"]
        for item in before_persist.checkpoints
    )
    def validate_immediate_prewrite() -> None:
        latest_preview, _, _ = _prepare(args, client, route)
        if latest_preview != preview:
            raise LinearTransportError("checkpoint_preview_stale_reload_required")
        if _root_surface(client, route, preview["workstream_id"]) != native_before:
            raise LinearTransportError("checkpoint_native_root_prewrite_drift")

    try:
        receipt = adapter.persist(
            preview["checkpoint"], prewrite_validator=validate_immediate_prewrite,
        )
    except (LinearTransportError, OSError, TimeoutError) as error:
        if str(error) in {
            "checkpoint_native_root_prewrite_drift",
            "checkpoint_preview_stale_reload_required",
        }:
            raise
        try:
            observed = adapter._state()
            receipt = next(
                item for item in observed.checkpoints
                if item["event_id"] == preview["checkpoint"]["event_id"]
            )
        except (LinearTransportError, OSError, StopIteration, TimeoutError):
            raise _partial_apply_error(
                "checkpoint_apply_unknown_replay_required", preview,
            ) from error
    if (
        (receipt.get("acknowledgement") or {}).get("remote_id")
        != preview["deterministic_slot_id"]
    ):
        raise _partial_apply_error(
            "checkpoint_receipt_remote_slot_mismatch", preview,
            checkpoint_receipt=receipt,
        )
    after_checkpoint_surface = _root_surface(
        client, route, preview["workstream_id"],
    )
    comments_after_checkpoint = adapter._comments()
    checkpoint_remote_id = receipt["acknowledgement"]["remote_id"]
    checkpoint_surface_matches = (
        _same_root_authority(after_checkpoint_surface, native_before)
        if not already_persisted else after_checkpoint_surface == native_before
    )
    if (
        not checkpoint_surface_matches
        or not _comments_match_owned_append(
            comments_before_checkpoint, comments_after_checkpoint,
            remote_id=checkpoint_remote_id, wrote=not already_persisted,
        )
    ):
        raise _partial_apply_error(
            "checkpoint_applied_but_native_root_drift", preview,
            checkpoint_receipt=receipt,
        )
    projection_receipt = None
    projection_apply_unconfirmed = False
    projection_apply_error: Exception | None = None
    comments_before_projection = comments_after_checkpoint
    if not projection_replay:
        projection_adapter = LinearProjectionAdapter(
            client, issue_id=preview["workstream_id"],
            workstream_id=preview["workstream_id"],
            plan_revision=preview["source"]["sha256"], **route,
        )
        try:
            quarantine = preview.get("projection_quarantine") or []
            projection_receipt = projection_adapter.append(
                preview["projection_candidate"],
                expected_material_revision=preview["material_revision"],
                expected_quarantine_count=len(quarantine),
                expected_quarantine_sha256=_digest(quarantine),
            )
        except (LinearTransportError, OSError, TimeoutError) as error:
            try:
                state = projection_adapter.state()
                recovered = next((
                    event for event in state.events
                    if event == preview["projection_candidate"]
                ), None)
                remote_id = state.remote_ids.get(
                    preview["projection_candidate"]["event_id"],
                )
                if recovered is None or not isinstance(remote_id, str):
                    raise StopIteration
                projection_receipt = {
                    "event_id": recovered["event_id"],
                    "remote_id": remote_id,
                    "revision": state.revision,
                    "recovered_after_transport_failure": True,
                }
            except (LinearTransportError, OSError, StopIteration, TimeoutError):
                # The checkpoint is durable but Linear did not acknowledge the
                # paired projection and a fresh read cannot find it.  Preserve
                # this exact non-atomic state long enough to ask ordinary
                # resume what is wrong.  Only its precise stale-pointer refusal
                # is eligible for the bounded compensation below; every other
                # result remains a partial apply.
                projection_apply_unconfirmed = True
                projection_apply_error = error
    else:
        projection_receipt = {
            "event_id": preview["projection_candidate"]["event_id"],
            "disposition": "existing",
        }
    after_projection_surface = _root_surface(
        client, route, preview["workstream_id"],
    )
    comments_after_projection = adapter._comments()
    projection_remote_id = (
        projection_receipt.get("remote_id") if projection_receipt else ""
    ) or ""
    projection_surface_matches = (
        after_projection_surface == after_checkpoint_surface
        if projection_replay else _same_root_authority(
            after_projection_surface, after_checkpoint_surface,
        )
    )
    unconfirmed_surface_is_unchanged = (
        projection_apply_unconfirmed
        and after_projection_surface == after_checkpoint_surface
        and comments_after_projection == comments_before_projection
    )
    if not unconfirmed_surface_is_unchanged and (
        not projection_surface_matches
        or (not projection_replay and not projection_remote_id)
        or not _comments_match_owned_append(
            comments_before_projection, comments_after_projection,
            remote_id=projection_remote_id, wrote=not projection_replay,
        )
    ):
        raise _partial_apply_error(
            "checkpoint_postwrite_native_root_drift_reconcile_required",
            preview, checkpoint_receipt=receipt,
            projection_receipt=projection_receipt,
        )
    # The producer and consumer bind one shape only: the acknowledged chain
    # tip recovered from every immutable checkpoint, including provenance.
    try:
        acknowledged_tip = _normalized_tip(
            adapter._state().checkpoints,
            preview["workstream_id"], preview["source"]["sha256"],
        )
    except (LinearTransportError, OSError, TimeoutError) as error:
        raise _partial_apply_error(
            "checkpoint_acknowledged_tip_unavailable", preview,
            checkpoint_receipt=receipt, projection_receipt=projection_receipt,
        ) from error
    try:
        resume = _ordinary_resume(
            preview["workstream_id"], args=args, route=route,
            source=preview["source"], expected_checkpoint=acknowledged_tip,
            expected_remote_id=receipt["acknowledgement"]["remote_id"],
        )
    except (LinearTransportError, OSError, TimeoutError) as error:
        # One bounded compensation is allowed for the only non-atomic gap:
        # the checkpoint was acknowledged but its recovery pointer was lost.
        # The helper rereads every authority surface and uses projection CAS;
        # if it cannot prove compatibility it returns None and we preserve the
        # precise partial-apply handle below.
        compensation = None
        if (projection_apply_unconfirmed
                and _is_exact_stale_checkpoint_resume_refusal(error)):
            compensation = _compensate_checkpoint_projection(
                client=client, preview=preview, route=route,
                native_before=native_before, checkpoint_receipt=receipt, args=args,
            )
        if compensation is not None:
            # The correction is already durable and acknowledged.  Preserve
            # its exact receipt before any subsequent read can fail so the
            # partial handle never regresses to the original unknown append.
            projection_receipt = compensation
            try:
                after_projection_surface = _root_surface(
                    client, route, preview["workstream_id"],
                )
                comments_after_projection = adapter._comments()
                resume = _ordinary_resume(
                    preview["workstream_id"], args=args, route=route,
                    source=preview["source"],
                    expected_checkpoint=acknowledged_tip,
                    expected_remote_id=receipt["acknowledgement"]["remote_id"],
                )
            except (LinearTransportError, OSError, TimeoutError) as second_error:
                raise _partial_apply_error(
                    "checkpoint_compensation_applied_but_resume_refused",
                    preview, checkpoint_receipt=receipt,
                    projection_receipt=projection_receipt,
                    failure={
                        "stage": "post_compensation_ordinary_resume",
                        "reason": str(second_error),
                    },
                ) from second_error
        if "resume" not in locals():
            raise _partial_apply_error(
                (
                    "checkpoint_projection_apply_unknown_replay_required"
                    if projection_apply_unconfirmed
                    else "checkpoint_applied_but_ordinary_resume_refused"
                ), preview,
                checkpoint_receipt=receipt,
                projection_receipt=projection_receipt,
            ) from (projection_apply_error or error)
    if _root_surface(
        client, route, preview["workstream_id"],
    ) != after_projection_surface or adapter._comments() != comments_after_projection:
        raise _partial_apply_error(
            "checkpoint_applied_but_resume_native_root_drift", preview,
            checkpoint_receipt=receipt,
            projection_receipt=projection_receipt,
        )
    latest_checkpoint = resume.get("latest_checkpoint") or {}
    compact_checkpoint_ok = (
        isinstance(resume.get("authority_binding"), dict)
        and resume["authority_binding"].get("checkpoint_sha256")
        == _digest(acknowledged_tip)
    )
    legacy_checkpoint_ok = (
        latest_checkpoint.get("checkpoint_event_id") == receipt["event_id"]
        and latest_checkpoint.get("root_revision") == preview["material_revision"]
        and (latest_checkpoint.get("acknowledgement") or {}).get("remote_id")
        == receipt["acknowledgement"]["remote_id"]
    )
    if (
        resume.get("plan_revision") != preview["source"]["sha256"]
        or not (legacy_checkpoint_ok or compact_checkpoint_ok)
    ):
        raise _partial_apply_error(
            "checkpoint_applied_but_ordinary_resume_receipt_mismatch", preview,
            checkpoint_receipt=receipt,
            projection_receipt=projection_receipt,
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
    except CheckpointPartialApplyError as error:
        json.dump(error.payload, sys.stderr, ensure_ascii=False, sort_keys=True)
        sys.stderr.write("\n")
        return 3
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
