#!/usr/bin/env python3
"""Fenced, same-root Linear description and native-state transitions.

Linear does not expose conditional ``issueUpdate``.  This command therefore
uses the strongest available protocol: a deterministic append-only reservation
for one reviewed snapshot, followed by immediate prewrite and postwrite
readback.  It never claims the mutable update itself is atomic.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import re
import sys
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_linear import (
    bootstrap_linear_route, canonical_native_uuid, HttpGraphQLClient,
    LinearGraphQLTransport, LinearTransportError,
)
from workstream_linear_events import (
    COMMENT_CREATE_CAPABILITY_QUERY, COMMENT_CREATE_MUTATION,
    LinearCommentEventAdapter,
)
from workstream_linear_projection import (
    reduce_projection_comments, select_plan_generation, TOMBSTONE,
)
from workstream_plan import CANONICAL_PLAN_LINE, HTTPS_URL, canonical_plan_url, same_plan_document
from workstream_plan import plan_payload


PREFIX = "<!-- workstream-root-transition:v1:"
PATTERN = re.compile(r"<!-- workstream-root-transition:v1:([A-Za-z0-9_-]+) -->")
HEX64 = re.compile(r"[0-9a-f]{64}")
IMMUTABLE_GITHUB_BLOB = re.compile(
    r"https://github\.com/[^/]+/[^/]+/blob/[0-9a-f]{40}/.+"
)
TERMINAL_TYPES = {"completed", "cancelled", "canceled"}

ISSUE_UPDATE = """
mutation WorkstreamRootTransition($issueId: String!, $input: IssueUpdateInput!) {
  issueUpdate(id: $issueId, input: $input) {
    success
    issue {
      id identifier title description url updatedAt archivedAt
      project { id }
      team { id organization { id } }
      assignee { id }
      state { id name type }
    }
  }
}
"""

STATE_QUERY = """
query WorkstreamRootTransitionState($teamId: String!, $stateId: String!) {
  team(id: $teamId) { id organization { id } }
  workflowState(id: $stateId) { id name type team { id } }
}
"""


class RootTransitionError(LinearTransportError):
    """A native root transition cannot be proven safe."""


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _root_view(snapshot: dict[str, Any]) -> dict[str, Any]:
    root = snapshot.get("root") or {}
    return {
        "id": root.get("id"), "identifier": root.get("identifier"),
        "title": root.get("title"), "description": root.get("description"),
        "url": root.get("url"), "updated_at": root.get("updatedAt"),
        "archived_at": root.get("archivedAt"), "parent": root.get("parent"),
        "project": root.get("project"), "team": root.get("team"),
        "assignee": root.get("assignee"), "state": root.get("state"),
    }


def snapshot_sha256(snapshot: dict[str, Any]) -> str:
    return _digest(_root_view(snapshot))


def _root_without_updated_at(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Return the material root fields unaffected by comment creation."""
    value = _root_view(snapshot)
    value.pop("updated_at", None)
    return value


def _preserved_root(snapshot: dict[str, Any], operation: str) -> dict[str, Any]:
    value = _root_without_updated_at(snapshot)
    value.pop(
        "description"
        if operation in {"plan-url", "reconcile-plan-url"} else "state",
        None,
    )
    return value


def _decode(body: str) -> dict[str, Any] | None:
    matches = PATTERN.findall(body)
    if not matches:
        return None
    if len(matches) != 1 or body.count(PREFIX) != 1:
        raise RootTransitionError("malformed_root_transition_reservation")
    try:
        raw = base64.urlsafe_b64decode(matches[0] + "=" * (-len(matches[0]) % 4))
        envelope = json.loads(raw)
        value = envelope["reservation"]
        if set(envelope) != {"reservation", "sha256"} or not hmac.compare_digest(
            str(envelope["sha256"]), _digest(value)
        ):
            raise ValueError("digest mismatch")
        return value
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RootTransitionError("malformed_root_transition_reservation") from error


def _encode(value: dict[str, Any]) -> str:
    encoded = base64.urlsafe_b64encode(_canonical({
        "reservation": value, "sha256": _digest(value),
    })).decode("ascii").rstrip("=")
    return f"{PREFIX}{encoded} -->"


def _slot_id(authority: dict[str, str], snapshot: str, frontier: str) -> str:
    material = _canonical([
        "workstream-root-transition-slot-v1", authority["workspace_id"],
        authority["root_issue_id"], snapshot, frontier,
    ])
    raw = bytearray(hashlib.sha256(material).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def comment_frontier_sha256(
    comments: list[dict[str, Any]], *, exclude_id: str | None = None,
) -> str:
    values = [{
        "id": item.get("id"), "body": item.get("body"),
        "created_at": item.get("createdAt"), "updated_at": item.get("updatedAt"),
    } for item in comments if item.get("id") != exclude_id]
    return _digest(sorted(values, key=lambda item: str(item["id"])))


def canonical_plan_url_spans(
    description: str, current: str,
) -> list[tuple[int, int]]:
    """Locate one logical canonical URL, including its Markdown duplicate."""
    lines = list(CANONICAL_PLAN_LINE.finditer(description))
    if len(lines) != 1:
        raise RootTransitionError(
            "canonical_plan_url_occurrence_ambiguous:keep exactly one labeled URL"
        )
    line = lines[0]
    matches = [
        match for match in HTTPS_URL.finditer(line.group(1))
        if match.group(0) == current
    ]
    if len(matches) == 1:
        pass
    elif len(matches) == 2 and re.fullmatch(
        rf"\[{re.escape(current)}\]"
        rf"\((?:{re.escape(current)}|<{re.escape(current)}>)\)",
        line.group(1),
    ):
        pass
    else:
        raise RootTransitionError(
            "canonical_plan_url_occurrence_ambiguous:keep exactly one labeled URL"
        )
    offset = line.start(1)
    return [(offset + match.start(), offset + match.end()) for match in matches]


def replace_spans(
    value: str, spans: list[tuple[int, int]], replacement: str,
) -> str:
    for start, end in reversed(spans):
        value = value[:start] + replacement + value[end:]
    return value


def replace_canonical_plan_url(description: str, target: str) -> tuple[str, str]:
    current = canonical_plan_url(description)
    if not re.fullmatch(r"https://github\.com/[^/]+/[^/]+/blob/[0-9a-f]{40}/.+", current):
        raise RootTransitionError("canonical_plan_url_not_pinned_immutable_github_blob")
    if not re.fullmatch(r"https://github\.com/[^/]+/[^/]+/blob/main/.+", target):
        raise RootTransitionError("target_plan_url_must_be_canonical_github_blob_main")
    if not same_plan_document(current, target):
        raise RootTransitionError("canonical_plan_url_different_document")
    spans = canonical_plan_url_spans(description, current)
    return replace_spans(description, spans, target), current


def reconcile_canonical_plan_url(
    description: str, target: str,
) -> tuple[str, str]:
    current = canonical_plan_url(description)
    if not IMMUTABLE_GITHUB_BLOB.fullmatch(target):
        raise RootTransitionError(
            "locator_reconcile_target_must_be_immutable_github_blob"
        )
    if not same_plan_document(current, target):
        raise RootTransitionError("locator_reconcile_different_plan_document")
    spans = canonical_plan_url_spans(description, current)
    return replace_spans(description, spans, target), current


def _reservation_matches_request(
    reservation: dict[str, Any], *, operation: str, target: str,
) -> bool:
    if operation in {"plan-url", "reconcile-plan-url"}:
        return (
            reservation.get("after") == {"canonical_plan_url": target}
            and isinstance((reservation.get("update") or {}).get("description"), str)
            and canonical_plan_url(reservation["update"]["description"]) == target
        )
    if operation == "reopen":
        return reservation.get("update") == {"stateId": target}
    return False


def validate_active_locator_authorization(
    *, source: dict[str, str], token: str, authority: dict[str, str],
    comments: list[dict[str, Any]], graph: dict[str, Any],
) -> dict[str, Any]:
    """Authorize only a locator repair to one exact structured active source."""
    if (
        not isinstance(source, dict)
        or set(source) != {"identity", "sha256"}
        or not IMMUTABLE_GITHUB_BLOB.fullmatch(str(source.get("identity", "")))
        or not HEX64.fullmatch(str(source.get("sha256", "")))
    ):
        raise RootTransitionError("locator_reconcile_authenticated_source_invalid")
    try:
        selected = select_plan_generation(
            comments, workstream_id=token,
            description_plan_revision=(graph.get("root") or {}).get(
                "plan_revision"
            ),
            authenticated_route=authority,
        )
    except LinearTransportError as error:
        raise RootTransitionError(str(error)) from error
    if (
        selected.get("authority_origin")
        not in {"generation_genesis", "generation_transition"}
        or not isinstance(selected.get("transition_tip_event_id"), str)
        or not selected["transition_tip_event_id"]
        or not isinstance(selected.get("activation_epoch"), int)
        or isinstance(selected.get("activation_epoch"), bool)
        or selected["activation_epoch"] < 0
    ):
        raise RootTransitionError(
            "locator_reconcile_structured_active_generation_required"
        )
    if selected.get("plan_revision") != source["sha256"]:
        raise RootTransitionError("locator_reconcile_target_not_active_source")
    try:
        from workstream_generation import assert_no_pending_generation_reservation

        assert_no_pending_generation_reservation(
            comments, workstream_id=token, authenticated_route=authority,
        )
        state = reduce_projection_comments(
            comments, workstream_id=token,
            expected_plan_revision=selected["plan_revision"],
            authenticated_route=authority, authenticated_source=source,
        )
    except LinearTransportError as error:
        raise RootTransitionError(str(error)) from error
    active: dict[tuple[str, str], dict[str, Any]] = {}
    for event in state.events:
        identity = (event["kind"], event["key"])
        if event["value"] == TOMBSTONE:
            active.pop(identity, None)
        else:
            active[identity] = event
    source_event = active.get(("source", "root"))
    if (
        source_event is None
        or source_event.get("value") != source
        or state.snapshot.get("source") != source
        or not state.events
    ):
        raise RootTransitionError("locator_reconcile_active_source_mismatch")
    return {
        "schema_version": 1,
        "authorization_kind": "active_generation_plan_locator",
        "source": deepcopy(source),
        "generation": deepcopy(selected),
        "projection": {
            "revision": state.revision,
            "frontier_event_id": state.events[-1]["event_id"],
            "events_sha256": _digest(list(state.events)),
            "source_event_id": source_event["event_id"],
            "source_value_sha256": _digest(source_event["value"]),
        },
    }


def _validate_authority(
    snapshot: dict[str, Any], token: str, authority: dict[str, str],
) -> None:
    root = snapshot.get("root") or {}
    observed = {
        "workspace_id": (((root.get("team") or {}).get("organization") or {}).get("id")),
        "team_id": (root.get("team") or {}).get("id"),
        "project_id": (root.get("project") or {}).get("id"),
        "root_issue_id": root.get("id"),
    }
    if observed != authority or str(root.get("identifier", "")).upper() != token:
        raise RootTransitionError("root_transition_route_or_identity_mismatch")
    if root.get("parent") is not None or root.get("archivedAt") is not None:
        raise RootTransitionError("root_transition_requires_active_root_issue")


def validate_operator_contract(
    contract: dict[str, Any], *, source: dict[str, str], token: str,
    authority: dict[str, str], comments: list[dict[str, Any]],
    graph: dict[str, Any], started_state: dict[str, str],
    description_plan_revision: str | None,
) -> dict[str, Any]:
    """Authenticate the prepared candidate and predecessor quiescence contract."""
    from workstream_generation import (
        prepare_generation_operator_contract,
    )

    if not isinstance(contract, dict):
        raise RootTransitionError("root_transition_operator_contract_invalid")
    if (
        contract.get("schema_version") != 1
        or contract.get("workstream_id") != token
        or contract.get("authenticated_route") != authority
        or contract.get("source") != source
        or not isinstance(contract.get("created_at"), str)
        or not isinstance(contract.get("remote_head"), str)
    ):
        raise RootTransitionError("root_transition_operator_contract_invalid")
    try:
        expected = prepare_generation_operator_contract(
            comments=comments, graph=graph, workstream_id=token,
            authority=authority,
            description_plan_revision=description_plan_revision,
            target_source=source, created_at=contract["created_at"],
            remote_head=contract["remote_head"],
            started_state=started_state,
        )
    except LinearTransportError as error:
        raise RootTransitionError(str(error)) from error
    if not hmac.compare_digest(_canonical(expected), _canonical(contract)):
        raise RootTransitionError(
            "root_transition_operator_contract_not_exact_live_prepare_output"
        )
    if expected["projection_preview"]["phase"] != "activation_ready":
        raise RootTransitionError(
            "root_transition_operator_candidate_projection_incomplete"
        )
    generation = expected["generation"]
    native_transition = expected["native_transition"]
    retirement = expected["retirement_proof"]
    frontiers = expected["frontiers"]
    return {
        "schema_version": 1,
        "contract_sha256": expected["contract_sha256"],
        "source": deepcopy(source),
        "generation": deepcopy(generation),
        "native_transition": deepcopy(native_transition),
        "retirement_sha256": retirement["declaration_sha256"],
        "frontiers_sha256": _digest(frontiers),
    }


def authenticated_started_state(
    client: Any, *, authority: dict[str, str], state_id: str,
) -> dict[str, str]:
    try:
        state_id = canonical_native_uuid(state_id, kind="state")
    except ValueError as error:
        raise RootTransitionError("reviewed_started_state_id_invalid") from error
    result = client.execute(STATE_QUERY, {
        "teamId": authority["team_id"], "stateId": state_id,
    })
    state = result.get("workflowState")
    team = result.get("team") or {}
    if (
        team.get("id") != authority["team_id"]
        or (team.get("organization") or {}).get("id") != authority["workspace_id"]
        or not isinstance(state, dict) or state.get("id") != state_id
        or (state.get("team") or {}).get("id") != authority["team_id"]
        or str(state.get("type", "")).lower() != "started"
        or not isinstance(state.get("name"), str) or not state["name"]
    ):
        raise RootTransitionError("reviewed_started_state_readback_mismatch")
    return {
        "id": state["id"], "name": state["name"], "type": state["type"],
        "team_id": (state.get("team") or {}).get("id"),
    }


class RootTransitionTransport:
    def __init__(
        self, client: Any, *, token: str, authority: dict[str, str],
        operator_authorization: dict[str, Any] | None = None,
        operator_validator: Callable[
            [dict[str, Any], list[dict[str, Any]]], dict[str, Any]
        ] | None = None,
        after_reservation_created: Callable[[], None] | None = None,
    ):
        self.client = client
        self.token = token.upper()
        self.authority = deepcopy(authority)
        required_operator = {
            "schema_version", "contract_sha256", "source", "generation",
            "native_transition", "retirement_sha256", "frontiers_sha256",
        }
        required_locator = {
            "schema_version", "authorization_kind", "source", "generation",
            "projection",
        }
        if (operator_authorization is None) == (operator_validator is None):
            raise RootTransitionError("root_transition_operator_authorization_required")
        self._required_operator = required_operator
        self._required_locator = required_locator
        self.operator_authorization = (
            self._validated_authorization(operator_authorization)
            if operator_authorization is not None else None
        )
        self.operator_validator = operator_validator
        self.after_reservation_created = after_reservation_created
        self.graph = LinearGraphQLTransport(
            client, team_id=authority["team_id"],
            workspace_id=authority["workspace_id"],
            project_id=authority["project_id"],
        )
        self.comments = LinearCommentEventAdapter(
            client, issue_id=self.token, **authority,
        )

    def _read(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        first = self.graph.snapshot_for_root(
            self.token, include_description=True, include_child_comments=True,
        )
        comments = self.comments.comments()
        second = self.graph.snapshot_for_root(
            self.token, include_description=True, include_child_comments=True,
        )
        final_comments = self.comments.comments()
        final = self.graph.snapshot_for_root(
            self.token, include_description=True, include_child_comments=True,
        )
        if first != second or second != final or comments != final_comments:
            raise RootTransitionError("root_transition_snapshot_changed_during_read")
        _validate_authority(final, self.token, self.authority)
        return final, final_comments

    def _validated_authorization(
        self, authorization: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not isinstance(authorization, dict):
            raise RootTransitionError("root_transition_operator_authorization_required")
        if authorization.get("authorization_kind") == (
            "active_generation_plan_locator"
        ):
            generation = authorization.get("generation")
            projection = authorization.get("projection")
            source = authorization.get("source")
            valid = (
                set(authorization) == self._required_locator
                and authorization.get("schema_version") == 1
                and isinstance(source, dict)
                and set(source) == {"identity", "sha256"}
                and IMMUTABLE_GITHUB_BLOB.fullmatch(
                    str(source.get("identity", ""))
                )
                and HEX64.fullmatch(str(source.get("sha256", "")))
                and isinstance(generation, dict)
                and set(generation) == {
                    "plan_revision", "description_plan_revision",
                    "transition_tip_event_id", "activation_epoch",
                    "authority_origin",
                }
                and generation.get("plan_revision") == source.get("sha256")
                and generation.get("authority_origin")
                in {"generation_genesis", "generation_transition"}
                and re.fullmatch(
                    r"wsp_[0-9a-f]{32}",
                    str(generation.get("transition_tip_event_id", "")),
                )
                and isinstance(generation.get("activation_epoch"), int)
                and not isinstance(generation.get("activation_epoch"), bool)
                and generation["activation_epoch"] >= 0
                and (
                    generation.get("description_plan_revision") is None
                    or isinstance(
                        generation.get("description_plan_revision"), str,
                    )
                )
                and isinstance(projection, dict)
                and set(projection) == {
                    "revision", "frontier_event_id", "events_sha256",
                    "source_event_id", "source_value_sha256",
                }
                and isinstance(projection.get("revision"), int)
                and not isinstance(projection.get("revision"), bool)
                and projection["revision"] > 0
                and all(
                    re.fullmatch(r"wsp_[0-9a-f]{32}", str(projection.get(field, "")))
                    for field in ("frontier_event_id", "source_event_id")
                )
                and all(
                    HEX64.fullmatch(str(projection.get(field, "")))
                    for field in ("events_sha256", "source_value_sha256")
                )
            )
            if not valid:
                raise RootTransitionError(
                    "root_transition_locator_authorization_required"
                )
            return deepcopy(authorization)
        if (
            set(authorization) != self._required_operator
            or authorization.get("schema_version") != 1
            or not all(HEX64.fullmatch(str(authorization.get(field, "")))
                       for field in ("contract_sha256", "retirement_sha256",
                                     "frontiers_sha256"))
        ):
            raise RootTransitionError("root_transition_operator_authorization_required")
        return deepcopy(authorization)

    def _authorize(
        self, snapshot: dict[str, Any], comments: list[dict[str, Any]],
    ) -> dict[str, Any]:
        authorization = (
            self.operator_validator(snapshot, comments)
            if self.operator_validator is not None
            else self.operator_authorization
        )
        return self._validated_authorization(authorization)

    def _started_state(self, state_id: str) -> dict[str, Any]:
        return authenticated_started_state(
            self.client, authority=self.authority, state_id=state_id,
        )

    @staticmethod
    def _reservation_receipt(
        comments: list[dict[str, Any]], *, slot: str, body: str,
    ) -> dict[str, str]:
        matches = [item for item in comments if item.get("id") == slot]
        if len(matches) != 1 or matches[0].get("body") != body:
            raise RootTransitionError("root_transition_reservation_changed_or_missing")
        receipt = {
            field: matches[0].get(field)
            for field in ("id", "body", "createdAt", "updatedAt")
        }
        if (
            not all(isinstance(value, str) and value for value in receipt.values())
        ):
            raise RootTransitionError("root_transition_reservation_receipt_invalid")
        return receipt  # type: ignore[return-value]

    def _reserve(
        self, reservation: dict[str, Any], slot: str,
    ) -> tuple[str, str, dict[str, str]]:
        capability = self.client.execute(COMMENT_CREATE_CAPABILITY_QUERY, {})
        fields = {
            item.get("name") for item in
            (((capability.get("__type") or {}).get("inputFields")) or [])
        }
        if "id" not in fields:
            raise RootTransitionError("root_transition_comment_id_capability_unavailable")
        body = _encode(reservation)
        try:
            response = self.client.execute(COMMENT_CREATE_MUTATION, {
                "input": {"id": slot, "issueId": self.authority["root_issue_id"], "body": body},
            })
        except (LinearTransportError, OSError):
            observed = self.comments.comments()
            matches = [item for item in observed if item.get("id") == slot]
            if len(matches) == 1 and matches[0].get("body") == body:
                return (
                    "existing_or_unknown", body,
                    self._reservation_receipt(observed, slot=slot, body=body),
                )
            raise RootTransitionError("root_transition_reservation_slot_conflict")
        created = response.get("commentCreate")
        returned = (created or {}).get("comment") if isinstance(created, dict) else None
        if (
            not isinstance(created, dict) or created.get("success") is not True
            or not isinstance(returned, dict) or returned.get("id") != slot
            or returned.get("body") != body
            or not isinstance(returned.get("createdAt"), str)
            or not returned.get("createdAt")
            or not isinstance(returned.get("updatedAt"), str)
            or not returned.get("updatedAt")
        ):
            raise RootTransitionError("root_transition_reservation_create_unproven")
        observed = self._reservation_receipt(
            self.comments.comments(), slot=slot, body=body,
        )
        returned_receipt = {
            field: returned[field]
            for field in ("id", "body", "createdAt", "updatedAt")
        }
        if observed != returned_receipt:
            raise RootTransitionError("root_transition_reservation_receipt_changed")
        return "created", body, returned_receipt

    def preview(self, *, operation: str, target: str) -> dict[str, Any]:
        snapshot, comments = self._read()
        operator_authorization = self._authorize(snapshot, comments)
        root = snapshot["root"]
        if operation == "plan-url":
            if operator_authorization.get("authorization_kind") is not None:
                raise RootTransitionError(
                    "root_transition_candidate_authorization_required"
                )
            if target != (operator_authorization.get("source") or {}).get("identity"):
                raise RootTransitionError(
                    "root_transition_plan_url_not_authorized_by_candidate"
                )
            desired_description, current = replace_canonical_plan_url(
                root.get("description") or "", target,
            )
            update = {"description": desired_description}
            before = {"canonical_plan_url": current}
            after = {"canonical_plan_url": target}
        elif operation == "reconcile-plan-url":
            if operator_authorization.get("authorization_kind") != (
                "active_generation_plan_locator"
            ):
                raise RootTransitionError(
                    "root_transition_locator_authorization_required"
                )
            if target != (operator_authorization.get("source") or {}).get(
                "identity"
            ):
                raise RootTransitionError(
                    "locator_reconcile_target_not_active_source"
                )
            desired_description, current = reconcile_canonical_plan_url(
                root.get("description") or "", target,
            )
            update = {"description": desired_description}
            before = {"canonical_plan_url": current}
            after = {"canonical_plan_url": target}
        elif operation == "reopen":
            if operator_authorization.get("authorization_kind") is not None:
                raise RootTransitionError(
                    "root_transition_candidate_authorization_required"
                )
            authorized_state = (
                (operator_authorization.get("native_transition") or {})
                .get("target_state") or {}
            )
            if target != authorized_state.get("id"):
                raise RootTransitionError(
                    "root_transition_reopen_state_not_authorized_by_candidate"
                )
            state = self._started_state(target)
            if state != authorized_state:
                raise RootTransitionError(
                    "root_transition_reopen_state_readback_changed"
                )
            native = root.get("state") or {}
            if str(native.get("type", "")).lower() not in TERMINAL_TYPES:
                raise RootTransitionError("root_reopen_requires_terminal_root")
            update = {"stateId": target}
            before = {"state": native}
            after = {"state": state}
        else:
            raise RootTransitionError("unknown_root_transition")
        snapshot_digest = snapshot_sha256(snapshot)
        frontier = comment_frontier_sha256(comments)
        intent = {
            "schema_version": 1, "operation": operation,
            "workstream_id": self.token, "authority": self.authority,
            "expected_snapshot_sha256": snapshot_digest,
            "expected_frontier_sha256": frontier,
            "before": before, "after": after, "update": update,
            "preserved_root": _preserved_root(snapshot, operation),
            "operator_authorization": deepcopy(operator_authorization),
        }
        return {
            "apply": False, "conditional_update_available": False,
            "linear_update_limit": (
                "issueUpdate is not conditional; deterministic reservation and immediate "
                "prewrite/postwrite readback are the strongest supported fence"
            ),
            "expected_snapshot_sha256": snapshot_digest,
            "expected_frontier_sha256": frontier,
            "reservation_slot_id": _slot_id(
                self.authority, snapshot_digest, frontier,
            ),
            "intent_sha256": _digest(intent), "intent": intent,
        }

    def apply(
        self, *, operation: str, target: str,
        expected_snapshot_sha256: str, expected_frontier_sha256: str,
        expected_intent_sha256: str,
    ) -> dict[str, Any]:
        if (
            not HEX64.fullmatch(expected_snapshot_sha256)
            or not HEX64.fullmatch(expected_frontier_sha256)
            or not HEX64.fullmatch(expected_intent_sha256)
        ):
            raise RootTransitionError("root_transition_expected_fence_invalid")
        snapshot, comments = self._read()
        current_authorization = self._authorize(snapshot, comments)
        reviewed_state = self._started_state(target) if operation == "reopen" else None
        slot = _slot_id(self.authority, expected_snapshot_sha256, expected_frontier_sha256)
        reserved = [item for item in comments if item.get("id") == slot]
        # Rebuild the original reviewed intent only while the original snapshot is present.
        replayed = snapshot_sha256(snapshot) != expected_snapshot_sha256
        if replayed and len(reserved) == 1:
            reservation_body = str(reserved[0].get("body") or "")
            pending = _decode(reservation_body)
            if isinstance(pending, dict):
                unsigned = {
                    key: deepcopy(value) for key, value in pending.items()
                    if key != "intent_sha256"
                }
                receipt = self._reservation_receipt(
                    comments, slot=slot, body=reservation_body,
                )
                root = snapshot.get("root") or {}
                update = pending.get("update") or {}
                target_applied = (
                    root.get("description") == update.get("description")
                    if operation in {"plan-url", "reconcile-plan-url"}
                    else (root.get("state") or {}).get("id") == target
                )
                reservation_only = (
                    pending.get("operation") == operation
                    and pending.get("expected_snapshot_sha256")
                    == expected_snapshot_sha256
                    and pending.get("expected_frontier_sha256")
                    == expected_frontier_sha256
                    and pending.get("authority") == self.authority
                    and pending.get("intent_sha256") == expected_intent_sha256
                    and pending.get("intent_sha256") == _digest(unsigned)
                    and pending.get("operator_authorization")
                    == current_authorization
                    and _reservation_matches_request(
                        pending, operation=operation, target=target,
                    )
                    and root.get("updatedAt") == receipt["updatedAt"]
                    and _preserved_root(snapshot, operation)
                    == pending.get("preserved_root")
                    and comment_frontier_sha256(comments, exclude_id=slot)
                    == expected_frontier_sha256
                    and not target_applied
                )
                if reservation_only:
                    raise RootTransitionError(
                        "root_transition_reservation_pending_review_new_preview_required"
                    )
        if (
            operation == "reconcile-plan-url"
            and not replayed
            and not reserved
            and canonical_plan_url(
                (snapshot.get("root") or {}).get("description") or ""
            ) == target
        ):
            preview = self.preview(operation=operation, target=target)
            if preview["expected_frontier_sha256"] != expected_frontier_sha256:
                raise RootTransitionError("root_transition_frontier_drift")
            if preview["intent_sha256"] != expected_intent_sha256:
                raise RootTransitionError("root_transition_intent_mismatch")
            return {
                "apply": True, "result": "already_current_noop",
                "conditional_update_available": False,
                "reservation_slot_id": None,
                "expected_snapshot_sha256": expected_snapshot_sha256,
                "expected_frontier_sha256": expected_frontier_sha256,
                "final_frontier_sha256": expected_frontier_sha256,
                "post_read_status": "reviewed_frontier_match",
                "final_snapshot_sha256": expected_snapshot_sha256,
                "authenticated_route": self.authority,
                "final_root": _root_view(snapshot),
            }
        if not replayed:
            if reserved:
                if len(reserved) != 1:
                    raise RootTransitionError("root_transition_reservation_slot_conflict")
                existing = _decode(str(reserved[0].get("body") or ""))
                if (
                    not isinstance(existing, dict)
                    or existing.get("intent_sha256") != expected_intent_sha256
                    or existing.get("operation") != operation
                    or existing.get("authority") != self.authority
                    or not _reservation_matches_request(
                        existing, operation=operation, target=target,
                    )
                ):
                    raise RootTransitionError("root_transition_reservation_slot_conflict")
                raise RootTransitionError(
                    "root_transition_reservation_pending_review_new_preview_required"
                )
            preview = self.preview(operation=operation, target=target)
            if preview["expected_frontier_sha256"] != expected_frontier_sha256:
                raise RootTransitionError("root_transition_frontier_drift")
            if preview["intent_sha256"] != expected_intent_sha256:
                raise RootTransitionError("root_transition_intent_mismatch")
            reservation = {**preview["intent"], "intent_sha256": preview["intent_sha256"]}
            reservation_result, reservation_body, reservation_receipt = self._reserve(
                reservation, slot,
            )
            if reservation_result != "created":
                raise RootTransitionError(
                    "root_transition_reservation_not_owned_by_this_process"
                )
            if self.after_reservation_created is not None:
                self.after_reservation_created()
            immediate, immediate_comments = self._read()
            immediate_authorization = self._authorize(
                immediate, immediate_comments,
            )
            immediate_receipt = self._reservation_receipt(
                immediate_comments, slot=slot, body=reservation_body,
            )
            if (
                immediate_receipt != reservation_receipt
                or (immediate.get("root") or {}).get("updatedAt")
                != reservation_receipt["updatedAt"]
                or immediate_authorization
                != reservation.get("operator_authorization")
                or current_authorization
                != reservation.get("operator_authorization")
                # Linear advances the root issue's updatedAt when a comment is
                # created.  Compare every material root field to the exact
                # pre-reservation read, but do not let our own reservation
                # invalidate its reviewed snapshot solely through that clock.
                or _root_without_updated_at(immediate)
                != _root_without_updated_at(snapshot)
                or comment_frontier_sha256(immediate_comments, exclude_id=slot)
                != expected_frontier_sha256
            ):
                raise RootTransitionError("root_transition_prewrite_drift")
            response = self.client.execute(ISSUE_UPDATE, {
                "issueId": self.authority["root_issue_id"],
                "input": preview["intent"]["update"],
            }).get("issueUpdate")
            if not isinstance(response, dict) or response.get("success") is not True:
                raise RootTransitionError("root_transition_update_not_accepted")
        else:
            if len(reserved) != 1:
                raise RootTransitionError("root_transition_snapshot_drift")
            reservation = _decode(str(reserved[0].get("body") or ""))
            if not isinstance(reservation, dict):
                raise RootTransitionError("root_transition_reservation_missing")
            if (
                reservation.get("operation") != operation
                or reservation.get("expected_snapshot_sha256") != expected_snapshot_sha256
                or reservation.get("expected_frontier_sha256") != expected_frontier_sha256
                or reservation.get("authority") != self.authority
                or reservation.get("intent_sha256") != expected_intent_sha256
                or reservation.get("operator_authorization")
                != current_authorization
                or not _reservation_matches_request(
                    reservation, operation=operation, target=target,
                )
            ):
                raise RootTransitionError("root_transition_replay_intent_mismatch")
            unsigned = {key: deepcopy(value) for key, value in reservation.items()
                        if key != "intent_sha256"}
            if reservation.get("intent_sha256") != _digest(unsigned):
                raise RootTransitionError("root_transition_replay_intent_mismatch")
            if operation in {"plan-url", "reconcile-plan-url"}:
                update = reservation.get("update") or {}
                after = reservation.get("after") or {}
                if (
                    set(update) != {"description"}
                    or not isinstance(update["description"], str)
                    or after != {"canonical_plan_url": target}
                    or canonical_plan_url(update["description"]) != target
                ):
                    raise RootTransitionError("root_transition_replay_intent_mismatch")
            elif (
                reservation.get("update") != {"stateId": target}
                or reservation.get("after") != {"state": reviewed_state}
            ):
                raise RootTransitionError("root_transition_replay_intent_mismatch")
        final, final_comments = self._read()
        final_authorization = self._authorize(final, final_comments)
        reservation_body = _encode(reservation)
        self._reservation_receipt(
            final_comments, slot=slot, body=reservation_body,
        )
        if final_authorization != reservation.get("operator_authorization"):
            raise RootTransitionError("root_transition_postwrite_operator_drift")
        final_root = final["root"]
        if _preserved_root(final, operation) != reservation.get("preserved_root"):
            raise RootTransitionError("root_transition_postwrite_unrelated_root_drift")
        if operation in {"plan-url", "reconcile-plan-url"}:
            if canonical_plan_url(final_root.get("description") or "") != target:
                raise RootTransitionError("root_transition_postwrite_description_mismatch")
            # Exact desired text from the reservation proves unrelated prose was preserved.
            if final_root.get("description") != reservation["update"]["description"]:
                raise RootTransitionError("root_transition_postwrite_description_drift")
        else:
            state = final_root.get("state") or {}
            observed_state = {
                "id": state.get("id"), "name": state.get("name"),
                "type": state.get("type"),
                "team_id": (final_root.get("team") or {}).get("id"),
            }
            if observed_state != reviewed_state:
                raise RootTransitionError("root_transition_postwrite_state_mismatch")
        observed_frontier = comment_frontier_sha256(final_comments, exclude_id=slot)
        if not replayed and observed_frontier != expected_frontier_sha256:
            raise RootTransitionError("root_transition_postwrite_frontier_drift")
        return {
            "apply": True, "result": "applied_or_exact_replay",
            "conditional_update_available": False,
            "reservation_slot_id": slot,
            "expected_snapshot_sha256": expected_snapshot_sha256,
            "expected_frontier_sha256": expected_frontier_sha256,
            "final_frontier_sha256": observed_frontier,
            "post_read_status": (
                "exact_replay_frontier_advanced" if replayed and observed_frontier
                != expected_frontier_sha256 else "reviewed_frontier_match"
            ),
            "final_snapshot_sha256": snapshot_sha256(final),
            "authenticated_route": self.authority,
            "final_root": _root_view(final),
        }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--config")
    value.add_argument("--linear-workspace-id")
    value.add_argument("--linear-team-id")
    value.add_argument("--linear-project-id")
    value.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    commands = value.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan-url")
    plan.add_argument("token")
    plan.add_argument("--to", required=True)
    reconcile = commands.add_parser("reconcile-plan-url")
    reconcile.add_argument("token")
    reconcile.add_argument("--to", required=True)
    reopen = commands.add_parser("reopen")
    reopen.add_argument("token")
    reopen.add_argument("--state-id", required=True)
    for command in (plan, reopen):
        command.add_argument("--operator-contract", required=True)
        command.add_argument("--plan-source", required=True)
        command.add_argument("--plan-identity")
    for command in (plan, reconcile, reopen):
        command.add_argument("--expected-snapshot-sha256")
        command.add_argument("--expected-frontier-sha256")
        command.add_argument("--expected-intent-sha256")
        command.add_argument("--apply", action="store_true")
    return value


def main() -> int:
    args = parser().parse_args()
    try:
        fences = (
            args.expected_snapshot_sha256,
            args.expected_frontier_sha256,
            args.expected_intent_sha256,
        )
        if args.apply and not all(
            isinstance(item, str) and HEX64.fullmatch(item) for item in fences
        ):
            raise RootTransitionError("root_transition_expected_fence_invalid")
        if not args.apply and any(fences):
            raise RootTransitionError("root_transition_preview_rejects_apply_fences")
        route, _ = resolve_linear_route(
            config_path=args.config, workspace_id=args.linear_workspace_id,
            team_id=args.linear_team_id, project_id=args.linear_project_id,
        )
        api_key = load_linear_api_key()
        if not api_key:
            raise RootTransitionError("linear_auth_unavailable")
        client = HttpGraphQLClient(api_key, args.linear_endpoint)
        authority = bootstrap_linear_route(client, args.token.upper())
        if route and any(route.get(key) != authority.get(key) for key in (
            "workspace_id", "team_id", "project_id",
        )):
            raise RootTransitionError("root_transition_configured_route_mismatch")
        if args.command == "reconcile-plan-url":
            if not IMMUTABLE_GITHUB_BLOB.fullmatch(args.to):
                raise RootTransitionError(
                    "locator_reconcile_target_must_be_immutable_github_blob"
                )
            payload_source = plan_payload(args.to, args.to)["source"]
            source = {
                "identity": payload_source["identity"],
                "sha256": payload_source["sha256"],
            }

            def operator_validator(
                snapshot: dict[str, Any], comments: list[dict[str, Any]],
            ) -> dict[str, Any]:
                return validate_active_locator_authorization(
                    source=source, token=args.token.upper(),
                    authority=authority, comments=comments, graph=snapshot,
                )
        else:
            payload_source = plan_payload(
                args.plan_source, args.plan_identity or args.plan_source,
            )["source"]
            source = {
                "identity": payload_source["identity"],
                "sha256": payload_source["sha256"],
            }
            with Path(args.operator_contract).open(encoding="utf-8") as handle:
                contract = json.load(handle)
            target_state_id = (
                ((contract.get("native_transition") or {}).get("target_state") or {})
                .get("id")
            ) if isinstance(contract, dict) else None
            if not isinstance(target_state_id, str):
                raise RootTransitionError(
                    "root_transition_operator_native_state_invalid"
                )

            def operator_validator(
                snapshot: dict[str, Any], comments: list[dict[str, Any]],
            ) -> dict[str, Any]:
                started_state = authenticated_started_state(
                    client, authority=authority, state_id=target_state_id,
                )
                return validate_operator_contract(
                    contract, source=source, token=args.token.upper(),
                    authority=authority, comments=comments,
                    graph=snapshot, started_state=started_state,
                    description_plan_revision=snapshot["root"].get(
                        "plan_revision"
                    ),
                )

        transport = RootTransitionTransport(
            client, token=args.token.upper(), authority=authority,
            operator_validator=operator_validator,
        )
        operation = args.command
        target = (
            args.to
            if operation in {"plan-url", "reconcile-plan-url"}
            else args.state_id
        )
        if operation == "plan-url" and target != source["identity"]:
            raise RootTransitionError(
                "root_transition_plan_url_must_equal_authenticated_target_source"
            )
        if args.apply:
            if (
                not args.expected_snapshot_sha256
                or not args.expected_frontier_sha256
                or not args.expected_intent_sha256
            ):
                raise RootTransitionError("root_transition_apply_requires_reviewed_fences")
            output = transport.apply(
                operation=operation, target=target,
                expected_snapshot_sha256=args.expected_snapshot_sha256,
                expected_frontier_sha256=args.expected_frontier_sha256,
                expected_intent_sha256=args.expected_intent_sha256,
            )
        else:
            output = transport.preview(operation=operation, target=target)
        json.dump(output, sys.stdout, ensure_ascii=False, sort_keys=True, indent=2)
        sys.stdout.write("\n")
        return 0
    except (OSError, ValueError, LinearTransportError) as error:
        print(f"workstream root transition refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
