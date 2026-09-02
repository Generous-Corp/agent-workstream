#!/usr/bin/env python3
"""Fenced native child completion plus its exact closure finalizer."""

from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
import re
import subprocess
import sys
from typing import Any, Protocol

from workstream_child_completion_prepare import prepare_child_completion
from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_linear import (
    HttpGraphQLClient, LinearGraphQLTransport, resolve_authenticated_issue_route,
)
from workstream_linear_events import (
    COMMENT_CREATE_CAPABILITY_QUERY, COMMENT_CREATE_MUTATION,
    LinearCommentEventAdapter, ledger_boundary_slot_id,
    encode_ledger_reservation, ledger_serialization_frontier,
    pending_ledger_reservations, reduce_event_comments,
)
from workstream_linear_checkpoints import reduce_checkpoint_comments
from workstream_child_dependencies import LinearChildDependencyAdapter
from workstream_linear_projection import (
    LinearProjectionAdapter, build_projection_event, reduce_projection_comments,
)
from workstream_plan import plan_payload
from workstream_projection import (
    _active_heads, bind_projection_plan_generation, stable_live_readback,
)
from workstream_relation_readback import read_relation_targets
from workstream_resume import (
    add_live_child_material_history, add_material_history, extract_token,
)


MINIMUM_WRITER_VERSION = "0.4.82"
SANCTIONED_FLEET_MACHINE_IDS = frozenset({"M1", "M3", "M5"})


class ChildCompletionError(RuntimeError):
    pass


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()


def _native_child(child: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": child.get("id"), "identifier": child.get("identifier"),
        "title": child.get("title"), "description": child.get("description"),
        "url": child.get("url"), "archivedAt": child.get("archivedAt"),
        "updatedAt": child.get("updatedAt"),
        "parent": deepcopy(child.get("parent")),
        "team": deepcopy(child.get("team")),
        "project": deepcopy(child.get("project")),
        "assignee": deepcopy(child.get("assignee")),
        "state": deepcopy(child.get("state")),
        "state_id": child.get("state_id"), "status": child.get("status"),
        "status_type": child.get("status_type"),
    }


def _state_type(issue: dict[str, Any]) -> str:
    return str(issue.get("status_type") or (issue.get("state") or {}).get("type")
               or "").lower()


def _post_child_valid(
    observed: dict[str, Any], before: dict[str, Any],
    completed_state: dict[str, str], returned: dict[str, Any] | None = None,
) -> bool:
    mutable = {"state", "state_id", "status", "status_type", "updatedAt"}
    preserved = {key: value for key, value in before.items() if key not in mutable}
    observed_preserved = {
        key: value for key, value in observed.items() if key not in mutable
    }
    expected_state = {
        key: completed_state[key] for key in ("id", "name", "type")
    }
    return bool(
        observed_preserved == preserved
        and observed.get("state") == expected_state
        and observed.get("state_id") == completed_state["id"]
        and observed.get("status") == completed_state["name"]
        and str(observed.get("status_type", "")).lower() == "completed"
        and isinstance(observed.get("updatedAt"), str)
        and observed["updatedAt"]
        and observed["updatedAt"] != before.get("updatedAt")
        and (returned is None or observed == returned)
    )


def _validate_pending_finalizer(
    reservation: dict[str, Any], *, snapshot: dict[str, Any], state: Any,
    comments: list[dict[str, Any]], child_comments: list[dict[str, Any]],
    root_token: str,
) -> None:
    """Re-prove every saved fence before finalizing a post-native crash."""
    fences = reservation.get("intent_fences")
    frontiers = (fences or {}).get("frontiers")
    if not isinstance(fences, dict) or not isinstance(frontiers, dict):
        raise ChildCompletionError("child_completion_recovery_fences_missing")
    if _digest(frontiers) != fences.get("frontiers_sha256"):
        raise ChildCompletionError("child_completion_recovery_fences_tampered")
    root = snapshot.get("root") or {}
    observed_root = _native_child(root)
    expected_root = (fences.get("native_root_before")
                     or fences.get("native_root") or {})
    immutable_root = {
        key: value for key, value in observed_root.items() if key != "updatedAt"
    }
    expected_immutable_root = {
        key: value for key, value in expected_root.items() if key != "updatedAt"
    }
    if (immutable_root != expected_immutable_root
            or not isinstance(observed_root.get("updatedAt"), str)
            or not observed_root["updatedAt"]
            or observed_root.get("updatedAt") == expected_root.get("updatedAt")):
        raise ChildCompletionError("child_completion_recovery_root_drift")
    child_id = fences.get("child_issue_id")
    child = next((item for item in snapshot.get("children", [])
                  if item.get("id") == child_id), None)
    if child is None or not _post_child_valid(
        _native_child(child), fences.get("native_child_before") or {},
        fences.get("completed_state") or {},
    ):
        raise ChildCompletionError("child_completion_recovery_child_drift")
    slot = ledger_boundary_slot_id(
        root_token, reservation["material_revision"],
        reservation["frontier_ids"], reservation["authority"],
    )
    root_comments = [item for item in comments if item.get("id") != slot]
    expected = frontiers.get("root") or {}
    if (_digest(root_comments) != expected.get("comments_sha256")
            or _digest(child_comments) != (frontiers.get("child") or {}).get(
                "comments_sha256")):
        raise ChildCompletionError("child_completion_recovery_comment_drift")
    root_events = reduce_event_comments(comments, workstream_id=root_token)
    child_events = reduce_event_comments(
        child_comments, workstream_id=str(child.get("identifier", "")).upper(),
    )
    if root_events.revision != frontiers["root"].get("material_revision") \
            or child_events.revision != frontiers["child"].get("material_revision"):
        raise ChildCompletionError("child_completion_recovery_material_drift")
    root_checkpoints = reduce_checkpoint_comments(comments, workstream_id=root_token)
    child_checkpoints = reduce_checkpoint_comments(
        child_comments, workstream_id=str(child.get("identifier", "")).upper(),
    )
    if [item["event_id"] for item in root_checkpoints.checkpoints] \
            != frontiers["root"].get("checkpoint_event_ids") \
            or [item["event_id"] for item in child_checkpoints.checkpoints] \
            != frontiers["child"].get("checkpoint_event_ids"):
        raise ChildCompletionError("child_completion_recovery_checkpoint_drift")
    remote_ids = [((getattr(state, "remote_ids", {}) or {}).get(item["event_id"]))
                  for item in state.events]
    if remote_ids[:len(reservation.get("projection_frontier_ids", []))] \
            != reservation.get("projection_frontier_ids"):
        raise ChildCompletionError("child_completion_recovery_projection_drift")
    if any(event["event_id"] == reservation["intent_event"]["event_id"]
           for event in state.events):
        raise ChildCompletionError("child_completion_recovery_already_finalized")
    if _eligible_fleet_gate(
        state, plan_revision=reservation["plan_revision"]
    ) != frontiers.get("fleet_gate"):
        raise ChildCompletionError("child_completion_recovery_fleet_gate_drift")
    if _digest(snapshot.get("dependency_graph")) \
            != frontiers.get("dependency_graph_sha256"):
        raise ChildCompletionError("child_completion_recovery_dependency_drift")
    if _digest({
        "relations": snapshot.get("relations"),
        "relation_targets": snapshot.get("relation_targets"),
    }) != frontiers.get("resolved_relations_sha256"):
        raise ChildCompletionError("child_completion_recovery_relation_drift")


def _eligible_fleet_gate(
    state: Any, *, plan_revision: str,
) -> dict[str, Any]:
    event = _active_heads(state).get(("writer_fleet_gate", "root"))
    value = (event or {}).get("value")
    if (
        not isinstance(value, dict)
        or not re.fullmatch(r"\d+\.\d+\.\d+", str(
            value.get("minimum_writer_version", "")
        ))
        or tuple(map(int, value["minimum_writer_version"].split(".")))
        < tuple(map(int, MINIMUM_WRITER_VERSION.split(".")))
        or value.get("legacy_writer_count") != 0
        or value.get("plan_revision") != plan_revision
        or not isinstance(value.get("writers"), list) or not value["writers"]
        or {writer.get("machine_id") for writer in value["writers"]}
        != SANCTIONED_FLEET_MACHINE_IDS
        or len(value["writers"]) != len(SANCTIONED_FLEET_MACHINE_IDS)
        or len({writer.get("writer_id") for writer in value["writers"]})
        != len(value["writers"])
    ):
        raise ChildCompletionError("current_zero_legacy_writer_fleet_gate_required")
    return {"event_id": event["event_id"], "value_sha256": _digest(value)}


def build_child_completion_transaction(
    snapshot: dict[str, Any], state: Any, *, root_token: str,
    child_token: str, evidence_contract: dict[str, Any],
    authenticated_source: dict[str, str], authenticated_route: dict[str, str],
    completed_state: dict[str, str], created_at: str,
    root_material_revision: int, root_checkpoint_event_ids: list[str],
    root_serialization_frontier: list[str],
    root_comment_frontier: list[dict[str, Any]],
    child_material_revision: int, child_checkpoint_event_ids: list[str],
    child_comment_frontier: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build the one prospective closure and bind every mutable frontier."""
    if str(completed_state.get("type", "")).lower() != "completed":
        raise ChildCompletionError("authenticated_completed_state_required")
    fleet = _eligible_fleet_gate(
        state, plan_revision=authenticated_source["sha256"],
    )
    children = [item for item in snapshot.get("children", [])
                if str(item.get("identifier", "")).upper() == child_token]
    if len(children) != 1:
        raise ChildCompletionError("child_identity_not_unique")
    child = children[0]
    root = snapshot.get("root") or {}
    if root.get("archivedAt") is not None or child.get("archivedAt") is not None:
        raise ChildCompletionError("active_unarchived_root_and_child_required")
    if _state_type(root) != "started" or _state_type(child) != "started":
        raise ChildCompletionError("started_native_root_and_child_required")
    root_route = {
        "workspace_id": (((root.get("team") or {}).get("organization") or {}).get("id")),
        "team_id": (root.get("team") or {}).get("id"),
        "project_id": (root.get("project") or {}).get("id"),
        "root_issue_id": root.get("id"),
    }
    if (
        root_route != authenticated_route
        or str(root.get("identifier", "")).upper() != root_token
        or root.get("parent") is not None
        or (child.get("state") or {}).get("id") != child.get("state_id")
        or (child.get("state") or {}).get("name") != child.get("status")
        or str((child.get("state") or {}).get("type", "")).lower()
        != str(child.get("status_type", "")).lower()
    ):
        raise ChildCompletionError("native_root_or_child_readback_mismatch")
    if str(child.get("status_type", "")).lower() == "completed":
        raise ChildCompletionError("child_already_completed_use_replay")
    prospective = deepcopy(snapshot)
    target = next(item for item in prospective["children"]
                  if str(item.get("identifier", "")).upper() == child_token)
    target["state_id"] = completed_state["id"]
    target["status"] = completed_state["name"]
    target["status_type"] = completed_state["type"]
    target["state"] = {key: completed_state[key] for key in ("id", "name", "type")}
    prepared = prepare_child_completion(
        prospective, state, root_token=root_token, child_token=child_token,
        evidence_contract=evidence_contract,
        authenticated_source=authenticated_source,
        authenticated_route=authenticated_route,
    )
    if prepared["operation_status"] != "closure_projection_required":
        raise ChildCompletionError("active_reviewed_evidence_contract_required")
    closure_item = next(item for item in prepared["projection_manifest"]["projection"]
                        if item["kind"] == "child_closure" and item["key"] == child_token)
    event = build_projection_event(
        workstream_id=root_token, kind="child_closure", key=child_token,
        value=closure_item["value"], plan_revision=authenticated_source["sha256"],
        expected_revision=state.revision, created_at=created_at,
        authority=authenticated_route,
    )
    projection_remote_ids = [
        (getattr(state, "remote_ids", {}) or {}).get(item["event_id"])
        for item in state.events
    ]
    if not all(isinstance(item, str) and item for item in projection_remote_ids):
        raise ChildCompletionError("projection_remote_frontier_incomplete")
    frontiers = {
        "root": {
            "material_revision": root_material_revision,
            "checkpoint_event_ids": root_checkpoint_event_ids,
            "comments_sha256": _digest(root_comment_frontier),
        },
        "child": {
            "material_revision": child_material_revision,
            "checkpoint_event_ids": child_checkpoint_event_ids,
            "comments_sha256": _digest(child_comment_frontier),
            "native_state_sha256": _digest(_native_child(child)),
        },
        "projection_revision": state.revision,
        "projection_event_ids": [item["event_id"] for item in state.events],
        "fleet_gate": fleet,
        "native_root_sha256": _digest(_native_child(root)),
        "dependency_graph_sha256": _digest(snapshot.get("dependency_graph")),
        "resolved_relations_sha256": _digest({
            "relations": snapshot.get("relations"),
            "relation_targets": snapshot.get("relation_targets"),
        }),
    }
    intent_sha = _digest(event)
    reservation = {
        "schema_version": 1, "workstream_id": root_token,
        "material_revision": root_material_revision,
        "intent_kind": "child_completion_projection",
        "plan_revision": authenticated_source["sha256"],
        "projection_revision": state.revision,
        "projection_frontier_ids": projection_remote_ids,
        "frontier_ids": root_serialization_frontier,
        "authority": authenticated_route, "intent_event": event,
        "intent_sha256": intent_sha,
        "intent_fences": {
            "frontiers": frontiers, "frontiers_sha256": _digest(frontiers),
            "child_issue_id": child["id"],
            "completed_state": completed_state,
            "native_root_before": _native_child(root),
            "native_child_before": _native_child(child),
            "native_child_after": _native_child(target),
        },
    }
    encode_ledger_reservation(reservation)
    return {
        "schema_version": 1, "operation_status": "ready",
        "child_issue_id": child["id"], "completed_state": completed_state,
        "native_child_before": _native_child(child),
        "native_child_after": _native_child(target),
        "native_root": _native_child(root),
        "prospective_child_closure": deepcopy(event["value"]),
        "intent_event": event, "intent_sha256": intent_sha,
        "frontiers": frontiers, "frontiers_sha256": _digest(frontiers),
        "reservation": reservation,
        "reservation_root_updated_at": root.get("updatedAt"),
        "reservation_slot_id": ledger_boundary_slot_id(
            root_token, root_material_revision, root_serialization_frontier,
            authenticated_route,
        ),
    }


class CompletionAdapter(Protocol):
    def surface(self) -> dict[str, Any]: ...
    def reserve(self, transaction: dict[str, Any]) -> str: ...
    def update_child_state(self, child_id: str, state_id: str) -> None: ...
    def append_closure(self, event: dict[str, Any]) -> None: ...
    def full_resume(self) -> dict[str, Any]: ...


def apply_child_completion(
    adapter: CompletionAdapter, transaction: dict[str, Any], *, apply: bool,
) -> dict[str, Any]:
    """Execute an exact transaction; the adapter owns authenticated rereads."""
    if not apply:
        return {**deepcopy(transaction), "apply": False}
    before = adapter.surface()
    if before["frontiers_sha256"] != transaction["frontiers_sha256"]:
        raise ChildCompletionError("child_completion_frontier_drift")
    phase = before["phase"]
    if phase == "complete":
        if before.get("closure") != transaction["prospective_child_closure"]:
            raise ChildCompletionError("child_completion_closure_contradiction")
    elif phase in {"open", "reserved_open"}:
        if phase == "open":
            receipt = adapter.reserve(transaction)
            if receipt not in {"created", "exact_replay"}:
                raise ChildCompletionError("child_completion_reservation_not_owned")
        immediate = adapter.surface()
        if immediate["phase"] != "reserved_open" or immediate[
            "frontiers_sha256"
        ] != transaction["frontiers_sha256"]:
            raise ChildCompletionError("child_completion_prewrite_drift")
        adapter.update_child_state(
            transaction["child_issue_id"], transaction["completed_state"]["id"],
        )
        phase = "reserved_completed"
    elif phase != "reserved_completed":
        raise ChildCompletionError("child_completion_replay_contradiction")
    if phase == "reserved_completed":
        post = adapter.surface()
        if post["phase"] != "reserved_completed" or post[
            "frontiers_sha256"
        ] != transaction["frontiers_sha256"]:
            raise ChildCompletionError("child_completion_postwrite_drift")
        adapter.append_closure(transaction["intent_event"])
    resumed = adapter.full_resume()
    if resumed.get("resume_authority") != "full":
        raise ChildCompletionError("child_completion_full_resume_required")
    return {"apply": True, "operation_status": "complete", "resume": resumed}


COMPLETED_STATE_QUERY = """
query WorkstreamCompletedState($teamId: String!, $stateId: String!) {
  team(id: $teamId) { id organization { id } }
  workflowState(id: $stateId) { id name type team { id } }
}
"""
CHILD_UPDATE = """
mutation WorkstreamChildCompletion($issueId: String!, $input: IssueUpdateInput!) {
  issueUpdate(id: $issueId, input: $input) {
    success
    issue {
      id identifier title description url archivedAt updatedAt
      parent { id identifier }
      project { id }
      team { id organization { id } }
      assignee { id }
      state { id name type }
    }
  }
}
"""


def _completed_state(client: Any, route: dict[str, str], state_id: str) -> dict[str, str]:
    result = client.execute(COMPLETED_STATE_QUERY, {
        "teamId": route["team_id"], "stateId": state_id,
    })
    state = result.get("workflowState") or {}
    team = result.get("team") or {}
    if (
        state.get("id") != state_id
        or (state.get("team") or {}).get("id") != route["team_id"]
        or team.get("id") != route["team_id"]
        or (team.get("organization") or {}).get("id") != route["workspace_id"]
        or str(state.get("type", "")).lower() != "completed"
        or not isinstance(state.get("name"), str) or not state["name"]
    ):
        raise ChildCompletionError("authenticated_completed_state_required")
    return {"id": state_id, "name": state["name"], "type": state["type"]}


class LiveCompletionAdapter:
    def __init__(self, client: Any, *, transaction: dict[str, Any], token: str,
                 route: dict[str, str], projection: LinearProjectionAdapter,
                 graph: LinearGraphQLTransport, comments: LinearCommentEventAdapter,
                 resume_args: list[str], source: dict[str, str]):
        self.client, self.transaction, self.token = client, transaction, token
        self.route, self.projection, self.graph, self.comments = route, projection, graph, comments
        self.resume_args = resume_args
        self.source = source
        self.post_update_native: dict[str, Any] | None = None

    def surface(self) -> dict[str, Any]:
        graph, comments = stable_live_readback(
            self.graph, self.comments, self.token, include_description=True,
            include_child_comments=True,
        )
        state = reduce_projection_comments(
            comments, workstream_id=self.token,
            expected_plan_revision=self.transaction["reservation"]["plan_revision"],
            authenticated_route=self.route,
        )
        bound_graph = bind_projection_plan_generation(
            graph, comments, workstream_id=self.token,
            requested_plan_revision=self.source["sha256"],
            authenticated_route=self.route,
        )
        bound_graph = add_live_child_material_history(
            bound_graph, authenticated_route=self.route, root_comments=comments,
            proposal_plan_revision=self.source["sha256"],
        )
        dependencies = LinearChildDependencyAdapter(
            self.client, workspace_id=self.route["workspace_id"],
            team_id=self.route["team_id"], project_id=self.route["project_id"],
            root_issue_id=self.route["root_issue_id"],
            root_identifier=self.token, plan_revision=self.source["sha256"],
        ).read_authorized_graph_for_snapshot(
            bound_graph, comments,
            generation_selector_plan_revision=bound_graph["root"].get(
                "description_plan_revision", bound_graph["root"].get("plan_revision")
            ),
            reread=lambda: stable_live_readback(
                self.graph, self.comments, self.token, include_description=True,
                include_child_comments=True,
            ),
        )
        bound_graph["dependency_graph"] = dependencies
        live_snapshot = add_material_history(
            bound_graph, comments, self.token, authenticated_route=self.route,
            authenticated_source=self.source,
            relation_target_resolver=lambda relations: read_relation_targets(
                self.client, relations,
            ),
        )
        active = _active_heads(state)
        closure = active.get(("child_closure", self.transaction["intent_event"]["key"]))
        child = next((item for item in graph.get("children", [])
                      if item.get("id") == self.transaction["child_issue_id"]), None)
        if child is None:
            raise ChildCompletionError("child_identity_drift")
        reservation_present = any(
            item.get("id") == self.transaction["reservation_slot_id"]
            and item.get("body") == encode_ledger_reservation(
                self.transaction["reservation"]
            ) for item in comments
        )
        completed = str(child.get("status_type", "")).lower() == "completed"
        base_comments = [item for item in comments
                         if item.get("id") != self.transaction["reservation_slot_id"]]
        child_comments = (graph.get("child_comments") or {}).get(
            self.transaction["intent_event"]["key"], []
        )
        native = _native_child(child)
        expected_native = (
            self.transaction["native_child_after"] if completed
            else self.transaction["native_child_before"]
        )
        original_projection_ids = self.transaction["frontiers"][
            "projection_event_ids"
        ]
        root_native = _native_child(graph.get("root") or {})
        expected_root = self.transaction["native_root"]
        root_preserved = {
            key: value for key, value in root_native.items() if key != "updatedAt"
        } == {
            key: value for key, value in expected_root.items() if key != "updatedAt"
        }
        reservation_receipts = [item for item in comments
                                if item.get("id") == self.transaction[
                                    "reservation_slot_id"
                                ]]
        root_clock_valid = (
            isinstance(root_native.get("updatedAt"), str)
            and root_native.get("updatedAt")
            and (not reservation_present or root_native.get("updatedAt")
                 != expected_root.get("updatedAt"))
        )
        native_valid = (
            native == expected_native if not completed else _post_child_valid(
                native, self.transaction["native_child_before"],
                self.transaction["completed_state"], self.post_update_native,
            )
        )
        if (
            _digest(base_comments) != self.transaction["frontiers"]["root"][
                "comments_sha256"
            ]
            or _digest(child_comments) != self.transaction["frontiers"]["child"][
                "comments_sha256"
            ]
            or not native_valid
            or not root_preserved or not root_clock_valid
            or _digest(live_snapshot.get("dependency_graph"))
            != self.transaction["frontiers"]["dependency_graph_sha256"]
            or _digest({
                "relations": live_snapshot.get("relations"),
                "relation_targets": live_snapshot.get("relation_targets"),
            }) != self.transaction["frontiers"]["resolved_relations_sha256"]
            or [item["event_id"] for item in state.events][
                :len(original_projection_ids)
            ] != original_projection_ids
            or (closure is None and len(state.events) != len(original_projection_ids))
            or (closure is not None and closure != self.transaction["intent_event"])
            or _eligible_fleet_gate(
                state, plan_revision=self.transaction["reservation"]["plan_revision"]
            ) != self.transaction["frontiers"]["fleet_gate"]
        ):
            raise ChildCompletionError("child_completion_frontier_drift")
        phase = ("complete" if closure else
                 "reserved_completed" if reservation_present and completed else
                 "reserved_open" if reservation_present else "open")
        return {
            "phase": phase,
            "frontiers_sha256": self.transaction["frontiers_sha256"],
            "closure": (closure or {}).get("value"),
        }

    def reserve(self, transaction: dict[str, Any]) -> str:
        capability = self.client.execute(COMMENT_CREATE_CAPABILITY_QUERY, {})
        fields = {item.get("name") for item in
                  ((capability.get("__type") or {}).get("inputFields") or [])}
        if "id" not in fields:
            raise ChildCompletionError("comment_id_capability_unavailable")
        slot, body = transaction["reservation_slot_id"], encode_ledger_reservation(
            transaction["reservation"]
        )
        existing = [item for item in self.comments.comments() if item.get("id") == slot]
        if existing:
            if len(existing) == 1 and existing[0].get("body") == body:
                return "exact_replay"
            raise ChildCompletionError("child_completion_reservation_conflict")
        result = self.client.execute(COMMENT_CREATE_MUTATION, {"input": {
            "id": slot, "issueId": self.route["root_issue_id"], "body": body,
        }}).get("commentCreate")
        if not isinstance(result, dict) or result.get("success") is not True:
            raise ChildCompletionError("child_completion_reservation_unproven")
        return "created"

    def update_child_state(self, child_id: str, state_id: str) -> None:
        result = self.client.execute(CHILD_UPDATE, {
            "issueId": child_id, "input": {"stateId": state_id},
        }).get("issueUpdate")
        if not isinstance(result, dict) or result.get("success") is not True:
            raise ChildCompletionError("child_completion_update_unproven")
        issue = result.get("issue")
        if not isinstance(issue, dict):
            raise ChildCompletionError("child_completion_update_readback_missing")
        normalized = deepcopy(issue)
        state = normalized.get("state") or {}
        normalized["state_id"] = state.get("id")
        normalized["status"] = state.get("name")
        normalized["status_type"] = state.get("type")
        self.post_update_native = _native_child(normalized)

    def append_closure(self, event: dict[str, Any]) -> None:
        self.projection.append(
            event, expected_material_revision=self.transaction["reservation"][
                "material_revision"
            ],
        )

    def full_resume(self) -> dict[str, Any]:
        result = subprocess.run(
            [sys.executable, str(__file__).replace(
                "workstream_child_completion.py", "workstream_resume.py"
            ), *self.resume_args], capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise ChildCompletionError("child_completion_full_resume_required")
        return json.loads(result.stdout)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("root")
    value.add_argument("--child", required=True)
    value.add_argument("--evidence-contract", required=True)
    value.add_argument("--plan-source", required=True)
    value.add_argument("--plan-identity", required=True)
    value.add_argument("--completed-state-id", required=True)
    value.add_argument("--created-at", required=True)
    value.add_argument("--expected-preview-sha256")
    value.add_argument("--apply", action="store_true")
    value.add_argument("--config")
    value.add_argument("--workspace-id")
    value.add_argument("--team-id")
    value.add_argument("--project-id")
    value.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    return value


def run(argv: list[str]) -> dict[str, Any]:
    args = parser().parse_args(argv)
    token, child_token = extract_token(args.root), extract_token(args.child)
    source = plan_payload(args.plan_source, args.plan_identity)["source"]
    api_key = load_linear_api_key()
    if not api_key:
        raise ChildCompletionError("linear_auth_unavailable")
    client = HttpGraphQLClient(api_key, args.linear_endpoint)
    declared, _ = resolve_linear_route(
        config_path=args.config, workspace_id=args.workspace_id,
        team_id=args.team_id, project_id=args.project_id,
    )
    route = resolve_authenticated_issue_route(client, token, declared)
    graph_adapter = LinearGraphQLTransport(
        client, workspace_id=route["workspace_id"], team_id=route["team_id"],
        project_id=route["project_id"],
    )
    comments_adapter = LinearCommentEventAdapter(
        client, issue_id=token, **route,
    )
    projection = LinearProjectionAdapter(
        client, issue_id=token, workstream_id=token,
        plan_revision=source["sha256"], **route,
    )
    graph, comments = stable_live_readback(
        graph_adapter, comments_adapter, token, include_description=True,
        include_child_comments=True,
    )
    graph = bind_projection_plan_generation(
        graph, comments, workstream_id=token,
        requested_plan_revision=source["sha256"], authenticated_route=route,
    )
    graph = add_live_child_material_history(
        graph, authenticated_route=route, root_comments=comments,
        proposal_plan_revision=source["sha256"],
    )
    dependency_adapter = LinearChildDependencyAdapter(
        client, workspace_id=route["workspace_id"], team_id=route["team_id"],
        project_id=route["project_id"], root_issue_id=route["root_issue_id"],
        root_identifier=token, plan_revision=source["sha256"],
    )
    graph["dependency_graph"] = dependency_adapter.read_authorized_graph_for_snapshot(
        graph, comments,
        generation_selector_plan_revision=graph["root"].get(
            "description_plan_revision", graph["root"].get("plan_revision")
        ),
        reread=lambda: stable_live_readback(
            graph_adapter, comments_adapter, token, include_description=True,
            include_child_comments=True,
        ),
    )
    snapshot = add_material_history(
        graph, comments, token, authenticated_route=route,
        authenticated_source=source,
        relation_target_resolver=lambda relations: read_relation_targets(
            client, relations,
        ),
    )
    state = reduce_projection_comments(
        comments, workstream_id=token,
        expected_plan_revision=source["sha256"], authenticated_route=route,
    )
    evidence = json.loads(open(args.evidence_contract, encoding="utf-8").read())
    completed_state = _completed_state(client, route, args.completed_state_id)
    current = prepare_child_completion(
        snapshot, state, root_token=token, child_token=child_token,
        evidence_contract=evidence, authenticated_source=source,
        authenticated_route=route,
    )
    resume_args = [
        token, "--plan-source", args.plan_source,
        "--plan-identity", args.plan_identity,
        "--linear-workspace-id", route["workspace_id"],
        "--linear-team-id", route["team_id"],
        "--linear-project-id", route["project_id"],
    ]
    if current["operation_status"] == "complete":
        if args.apply:
            result = subprocess.run([
                sys.executable, str(__file__).replace(
                    "workstream_child_completion.py", "workstream_resume.py"
                ), *resume_args,
            ], capture_output=True, text=True)
            if result.returncode != 0 or json.loads(result.stdout).get(
                "resume_authority"
            ) != "full":
                raise ChildCompletionError("child_completion_full_resume_required")
        return {"apply": args.apply, "operation_status": "complete_noop"}
    if current["operation_status"] == "closure_projection_required":
        _eligible_fleet_gate(state, plan_revision=source["sha256"])
        desired = next(item for item in current["projection_manifest"]["projection"]
                       if item["kind"] == "child_closure" and item["key"] == child_token)
        pending = [item for item in pending_ledger_reservations(
            comments, workstream_id=token, authenticated_route=route,
            current_plan_revision=source["sha256"],
        ) if item["intent_kind"] == "child_completion_projection"
             and item["intent_event"]["key"] == child_token]
        if len(pending) != 1 or pending[0]["intent_event"]["value"] != desired["value"]:
            raise ChildCompletionError("post_native_completion_reservation_required")
        _validate_pending_finalizer(
            pending[0], snapshot=snapshot, state=state, comments=comments,
            child_comments=(graph.get("child_comments") or {}).get(child_token, []),
            root_token=token,
        )
        recovery = {
            "schema_version": 1, "operation_status": "post_native_finalization_ready",
            "intent_event": pending[0]["intent_event"],
            "reservation_sha256": _digest(pending[0]),
        }
        preview_sha = _digest(recovery)
        if not args.apply:
            return {**recovery, "apply": False, "preview_sha256": preview_sha}
        if args.expected_preview_sha256 != preview_sha:
            raise ChildCompletionError("matching_reviewed_preview_required")
        projection.append(
            pending[0]["intent_event"],
            expected_material_revision=pending[0]["material_revision"],
        )
        result = subprocess.run([
            sys.executable, str(__file__).replace(
                "workstream_child_completion.py", "workstream_resume.py"
            ), *resume_args,
        ], capture_output=True, text=True)
        resumed = json.loads(result.stdout) if result.returncode == 0 else {}
        if resumed.get("resume_authority") != "full":
            raise ChildCompletionError("child_completion_full_resume_required")
        return {"apply": True, "operation_status": "complete", "resume": resumed}
    child_comments = (graph.get("child_comments") or {}).get(child_token, [])
    root_raw = reduce_event_comments(comments, workstream_id=token)
    root_checkpoints = reduce_checkpoint_comments(comments, workstream_id=token)
    child_raw = reduce_event_comments(child_comments, workstream_id=child_token)
    child_checkpoints = reduce_checkpoint_comments(
        child_comments, workstream_id=child_token,
    )
    root_checkpoint_ids = [
        item["event_id"] for item in root_checkpoints.checkpoints
    ]
    serialization_frontier = ledger_serialization_frontier(
        root_checkpoint_ids, comments, workstream_id=token,
        authenticated_route=route, current_plan_revision=source["sha256"],
        material_revision=root_raw.revision,
    )
    open_pending = [item for item in pending_ledger_reservations(
        comments, workstream_id=token, authenticated_route=route,
        current_plan_revision=source["sha256"],
    ) if item["intent_kind"] == "child_completion_projection"
         and item["intent_event"]["key"] == child_token]
    comments_for_build = comments
    created_at = args.created_at
    if open_pending:
        if len(open_pending) != 1:
            raise ChildCompletionError("child_completion_reservation_ambiguous")
        reservation = open_pending[0]
        slot = ledger_boundary_slot_id(
            token, reservation["material_revision"],
            reservation["frontier_ids"], route,
        )
        comments_for_build = [item for item in comments if item.get("id") != slot]
        serialization_frontier = reservation["frontier_ids"]
        created_at = reservation["intent_event"]["created_at"]
    transaction = build_child_completion_transaction(
        snapshot, state, root_token=token, child_token=child_token,
        evidence_contract=evidence, authenticated_source=source,
        authenticated_route=route, completed_state=completed_state,
        created_at=created_at, root_material_revision=root_raw.revision,
        root_checkpoint_event_ids=root_checkpoint_ids,
        root_serialization_frontier=serialization_frontier,
        root_comment_frontier=comments_for_build,
        child_material_revision=child_raw.revision,
        child_checkpoint_event_ids=[item["event_id"] for item in child_checkpoints.checkpoints],
        child_comment_frontier=child_comments,
    )
    if open_pending and transaction["reservation"] != open_pending[0]:
        raise ChildCompletionError("child_completion_replay_contradiction")
    preview_sha = _digest(transaction)
    result = {**transaction, "preview_sha256": preview_sha}
    if not args.apply:
        return {**result, "apply": False}
    if args.expected_preview_sha256 != preview_sha:
        raise ChildCompletionError("matching_reviewed_preview_required")
    adapter = LiveCompletionAdapter(
        client, transaction=transaction, token=token, route=route,
        projection=projection, graph=graph_adapter, comments=comments_adapter,
        resume_args=resume_args, source=source,
    )
    return apply_child_completion(adapter, transaction, apply=True)


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(sys.argv[1:] if argv is None else argv)
    except (ChildCompletionError, OSError, ValueError) as error:
        print(f"workstream child completion refused: {error}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
