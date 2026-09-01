#!/usr/bin/env python3
"""Derive historical evidence authority from immutable closure event order."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from workstream_evidence import evidence_errors
from workstream_scope import repository_key, ScopeError, validate_scope


TOMBSTONE = {"_projection_tombstone": True}


class ProjectionHistoryError(ValueError):
    pass


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()


def _active(events: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for event in events:
        identity = (event["kind"], event["key"])
        if event["value"] == TOMBSTONE:
            result.pop(identity, None)
        else:
            result[identity] = event
    return result


def _generation(
    projection_history: list[dict[str, Any]], plan_revision: str,
) -> list[dict[str, Any]]:
    return sorted(
        [
            event for event in projection_history
            if event.get("plan_revision") == plan_revision
        ],
        key=lambda event: (
            event.get("expected_revision"), event.get("created_at"),
            event.get("event_id"),
        ),
    )


def _bound_predecessor_generation(
    event: dict[str, Any], projection_history: list[dict[str, Any]],
    authority: dict[str, Any],
    selected_transition_tip_event_id: str | None,
    authorized_prepared_transition_event_id: str | None,
) -> list[dict[str, Any]]:
    """Recover the exact reviewed prefix across its final activation append."""
    predecessor = _generation(
        projection_history, authority["predecessor_plan_revision"],
    )
    revision = authority["predecessor_projection_revision"]
    if _digest(projection_history) == authority["projection_history_sha256"]:
        return predecessor

    # Activation changes authority by appending one generation transition to
    # the predecessor's deterministic final CAS slot. Carried evidence was
    # reviewed before that append, so accept only the exact terminal control
    # already validated by generation selection, then replay the bound digest.
    if len(predecessor) != revision + 1:
        raise ProjectionHistoryError("carried_evidence_authority_invalid")
    transition = predecessor[-1]
    bound_history = [
        item for item in projection_history
        if item.get("event_id") != transition.get("event_id")
    ]
    value = transition.get("value")
    from_frontier = value.get("from") if isinstance(value, dict) else None
    to_frontier = value.get("to") if isinstance(value, dict) else None
    if (
        transition.get("event_id") not in {
            selected_transition_tip_event_id,
            authorized_prepared_transition_event_id,
        }
        or transition.get("kind") != "generation_transition"
        or transition.get("workstream_id") != event.get("workstream_id")
        or transition.get("plan_revision")
        != authority["predecessor_plan_revision"]
        or transition.get("expected_revision") != revision
        or not isinstance(from_frontier, dict)
        or not isinstance(to_frontier, dict)
        or from_frontier.get("plan_revision")
        != authority["predecessor_plan_revision"]
        or from_frontier.get("projection_revision") != revision
        or from_frontier.get("projection_events_sha256")
        != authority["predecessor_projection_events_sha256"]
        or from_frontier.get("projection_frontier_event_id")
        != authority["predecessor_projection_frontier_event_id"]
        or to_frontier.get("plan_revision") != event.get("plan_revision")
        or _digest(bound_history) != authority["projection_history_sha256"]
    ):
        raise ProjectionHistoryError("carried_evidence_authority_invalid")
    return predecessor[:revision]


def _carried_predecessor_authority(
    event: dict[str, Any], projection_history: list[dict[str, Any]],
    current_scope: dict[str, Any],
    selected_transition_tip_event_id: str | None = None,
    authorized_prepared_transition_event_id: str | None = None,
) -> dict[str, Any] | None:
    contract = event.get("value")
    authority = (
        contract.get("predecessor_closure_authority")
        if isinstance(contract, dict) else None
    )
    if authority is None:
        return None
    required = {
        "schema_version", "predecessor_plan_revision",
        "predecessor_projection_revision",
        "predecessor_projection_events_sha256", "projection_history_sha256",
        "predecessor_projection_frontier_event_id",
        "predecessor_projection_frontier_sha256",
        "material_revision", "material_events_sha256",
        "checkpoint_event_id", "checkpoint_events_sha256",
        "input_frontier_sha256", "predecessor_evidence_event_id",
        "predecessor_evidence_value_sha256", "predecessor_closure_event_id",
        "predecessor_closure_value_sha256",
    }
    if (
        not isinstance(authority, dict)
        or set(authority) != required
        or authority.get("schema_version") != 1
        or not re.fullmatch(
            r"[0-9a-f]{64}",
            str(authority.get("predecessor_plan_revision", "")),
        )
        or not isinstance(authority.get("predecessor_projection_revision"), int)
        or isinstance(authority.get("predecessor_projection_revision"), bool)
        or authority["predecessor_projection_revision"] <= 0
        or not isinstance(authority.get("material_revision"), int)
        or isinstance(authority.get("material_revision"), bool)
        or authority["material_revision"] < 0
        or not (
            authority.get("checkpoint_event_id") is None
            or isinstance(authority.get("checkpoint_event_id"), str)
            and bool(authority["checkpoint_event_id"])
        )
        or not all(
            re.fullmatch(r"[0-9a-f]{64}", str(authority.get(field, "")))
            for field in (
                "predecessor_projection_events_sha256",
                "predecessor_projection_frontier_sha256",
                "projection_history_sha256", "material_events_sha256",
                "checkpoint_events_sha256", "input_frontier_sha256",
                "predecessor_evidence_value_sha256",
                "predecessor_closure_value_sha256",
            )
        )
        or not all(
            isinstance(authority.get(field), str) and authority[field]
            for field in (
                "predecessor_evidence_event_id",
                "predecessor_closure_event_id",
                "predecessor_projection_frontier_event_id",
            )
        )
    ):
        raise ProjectionHistoryError("carried_evidence_authority_invalid")
    predecessor = _bound_predecessor_generation(
        event, projection_history, authority,
        selected_transition_tip_event_id,
        authorized_prepared_transition_event_id,
    )
    if (
        not predecessor
        or authority.get("predecessor_projection_revision") != len(predecessor)
        or authority.get("predecessor_projection_events_sha256")
        != _digest(predecessor)
        or authority.get("predecessor_projection_frontier_event_id")
        != predecessor[-1].get("event_id")
        or authority.get("predecessor_projection_frontier_sha256")
        != _digest(predecessor[-1])
    ):
        raise ProjectionHistoryError("carried_evidence_predecessor_drift")
    predecessor_active = _active(predecessor)
    old_evidence = next((
        item for item in predecessor
        if item.get("event_id")
        == authority.get("predecessor_evidence_event_id")
    ), None)
    old_closure = next((
        item for item in predecessor
        if item.get("event_id")
        == authority.get("predecessor_closure_event_id")
    ), None)
    child_id = str(contract.get("owning_child", "")).upper()
    if (
        old_evidence is None
        or old_closure is None
        or predecessor_active.get(("evidence_contract", event.get("key")))
        != old_evidence
        or predecessor_active.get(("child_closure", child_id)) != old_closure
        or _digest(old_evidence.get("value"))
        != authority.get("predecessor_evidence_value_sha256")
        or _digest(old_closure.get("value"))
        != authority.get("predecessor_closure_value_sha256")
    ):
        raise ProjectionHistoryError("carried_evidence_predecessor_drift")
    authorized = _closure_bound_single_generation(predecessor, current_scope)
    repository_key_value = old_closure["value"].get("repository_key")
    current_repository = next((
        repository for repository in current_scope.get("repositories", [])
        if repository_key(repository) == repository_key_value
    ), None)
    current_head_closure = (
        current_scope.get("child_ownership", {}).get(child_id)
        == repository_key_value
        and current_repository is not None
        and current_repository.get("exact_head")
        == old_closure["value"].get("exact_head")
    )
    if (
        old_evidence.get("event_id") not in authorized
        and not current_head_closure
    ):
        raise ProjectionHistoryError("carried_evidence_not_closure_bound")
    expected = dict(old_evidence["value"])
    expected["plan_revision"] = event.get("plan_revision")
    expected["predecessor_closure_authority"] = authority
    if contract != expected:
        raise ProjectionHistoryError("carried_evidence_contract_mutated")
    return {
        "child_identifier": child_id,
        "repository_key": old_closure["value"].get("repository_key"),
        "exact_head": old_closure["value"].get("exact_head"),
        "predecessor_closure_event_id": old_closure["event_id"],
    }


def carried_predecessor_evidence_authority(
    event: dict[str, Any], projection_history: list[dict[str, Any]],
    current_scope: dict[str, Any], *,
    selected_transition_tip_event_id: str | None = None,
    authorized_prepared_transition_event_id: str | None = None,
) -> dict[str, Any] | None:
    """Validate and return one persisted predecessor-closure carry proof."""
    return _carried_predecessor_authority(
        event, projection_history, current_scope,
        selected_transition_tip_event_id,
        authorized_prepared_transition_event_id,
    )


def closure_bound_historical_evidence(
    projection_events: list[dict[str, Any]], current_scope: dict[str, Any],
    projection_history: list[dict[str, Any]] | None = None,
    *, selected_transition_tip_event_id: str | None = None,
    authorized_prepared_transition_event_id: str | None = None,
) -> frozenset[str]:
    """Return active evidence event IDs authorized at a closed child's old head.

    Authority comes only from the scope and complete evidence set active before
    the immutable closure event. Current scope must still preserve the same
    repository identity and child ownership. Unclosed evidence gets no
    historical authority.
    """
    carried: dict[str, dict[str, Any]] = {}
    if projection_history is not None:
        if not isinstance(projection_history, list):
            raise ProjectionHistoryError("projection_history_invalid")
        for event in projection_events:
            if event.get("kind") != "evidence_contract" or event.get("value") == TOMBSTONE:
                continue
            authority = _carried_predecessor_authority(
                event, projection_history, current_scope,
                selected_transition_tip_event_id,
                authorized_prepared_transition_event_id,
            )
            if authority is not None:
                carried[event["event_id"]] = authority
    return _closure_bound_single_generation(
        projection_events, current_scope, carried=carried,
    )


def _closure_bound_single_generation(
    projection_events: list[dict[str, Any]], current_scope: dict[str, Any], *,
    carried: dict[str, dict[str, Any]] | None = None,
) -> frozenset[str]:
    carried = carried or {}
    try:
        validate_scope(
            current_scope,
            root_id=str(projection_events[-1].get("workstream_id", "")),
            child_ids=set(current_scope.get("child_ownership", {})),
        )
    except (ScopeError, IndexError) as error:
        raise ProjectionHistoryError(
            f"closure_history_current_scope_invalid:{error}"
        ) from error
    final = _active(projection_events)
    current_repositories = {
        repository_key(repository): repository
        for repository in current_scope.get("repositories", [])
    }
    event_indexes = {
        event["event_id"]: index for index, event in enumerate(projection_events)
    }
    authorized: set[str] = set()
    for (kind, child_id), closure_event in sorted(final.items()):
        if kind != "child_closure":
            continue
        closure = closure_event["value"]
        index = event_indexes.get(closure_event["event_id"])
        if index is None:
            raise ProjectionHistoryError(
                f"closure_history_event_missing:{child_id}"
            )
        before = _active(projection_events[:index])
        scope_event = before.get(("scope", "root"))
        if scope_event is None:
            raise ProjectionHistoryError(
                f"closure_history_scope_missing:{child_id}"
            )
        historical_scope = scope_event["value"]
        try:
            validate_scope(
                historical_scope,
                root_id=str(closure_event.get("workstream_id", "")),
                child_ids=set(historical_scope.get("child_ownership", {})),
            )
        except ScopeError as error:
            raise ProjectionHistoryError(
                f"closure_history_scope_invalid:{child_id}:{error}"
            ) from error
        repository_key_value = closure.get("repository_key")
        current_repository = current_repositories.get(repository_key_value)
        if (
            current_scope.get("child_ownership", {}).get(child_id)
            != repository_key_value
            or current_repository is None
        ):
            raise ProjectionHistoryError(
                f"closure_history_repository_mismatch:{child_id}"
            )
        current_head_closure = (
            current_repository.get("exact_head") == closure.get("exact_head")
        )
        historical_repository = next((
            repository for repository in historical_scope.get("repositories", [])
            if repository_key(repository) == repository_key_value
        ), None)
        before_evidence = sorted(
            [
                event for (event_kind, _key), event in before.items()
                if event_kind == "evidence_contract"
                and event["value"].get("owning_child") == child_id
            ],
            key=lambda event: (event["key"], event["event_id"]),
        )
        carried_closure = bool(before_evidence) and all(
            event["event_id"] in carried
            and carried[event["event_id"]]["child_identifier"] == child_id
            and carried[event["event_id"]]["repository_key"]
            == closure.get("repository_key")
            and carried[event["event_id"]]["exact_head"]
            == closure.get("exact_head")
            for event in before_evidence
        )
        if carried_closure:
            historical_repository = current_repository
        elif (
            (
                not current_head_closure
                and historical_scope.get("child_ownership", {}).get(child_id)
                != repository_key_value
            )
            or historical_repository is None
            or historical_repository.get("exact_head") != closure.get("exact_head")
            or historical_repository.get("slug") not in [
                current_repository.get("slug"),
                *current_repository.get("aliases", []),
            ]
        ):
            raise ProjectionHistoryError(
                f"closure_history_repository_mismatch:{child_id}"
            )
        if (
            closure.get("plan_revision") != closure_event.get("plan_revision")
            or closure.get("workspace_id")
            != historical_scope.get("linear", {}).get("workspace_id")
            or closure.get("team_id")
            != historical_scope.get("linear", {}).get("team_id")
            or closure.get("project_id")
            != historical_scope.get("linear", {}).get("project_id")
            or closure.get("parent_issue_id")
            != historical_scope.get("linear", {}).get("root_issue_id")
        ):
            raise ProjectionHistoryError(
                f"closure_history_repository_mismatch:{child_id}"
            )
        expected_heads = [
            {
                "key": event["key"], "event_id": event["event_id"],
                "value_sha256": _digest(event["value"]),
            }
            for event in before_evidence
        ]
        if expected_heads != closure.get("evidence_heads"):
            raise ProjectionHistoryError(
                f"closure_history_evidence_set_mismatch:{child_id}"
            )
        closure_evidence_keys = {
            str(head.get("key")) for head in closure.get("evidence_heads", [])
        }
        for later_event in projection_events[index + 1:]:
            if later_event.get("kind") != "evidence_contract":
                continue
            if (
                str(later_event.get("key")) in closure_evidence_keys
                or (
                    later_event.get("value") != TOMBSTONE
                    and later_event.get("value", {}).get("owning_child")
                    == child_id
                )
            ):
                raise ProjectionHistoryError(
                    "child_closure_evidence_set_mismatch:history:"
                    f"{child_id}"
                )
        final_evidence = sorted(
            [
                event for (event_kind, _key), event in final.items()
                if event_kind == "evidence_contract"
                and event["value"].get("owning_child") == child_id
            ],
            key=lambda event: (event["key"], event["event_id"]),
        )
        if not current_head_closure and [
            event["event_id"] for event in final_evidence
        ] != [
            event["event_id"] for event in before_evidence
        ]:
            raise ProjectionHistoryError(
                "child_closure_evidence_set_mismatch:history:"
                f"{child_id}"
            )
        for event in before_evidence:
            contract = event["value"]
            if (
                contract.get("owning_child") != child_id
                or contract.get("repository_key") != repository_key_value
                or contract.get("exact_head") != closure.get("exact_head")
                or contract.get("repository") not in [
                    historical_repository.get("slug"),
                    *historical_repository.get("aliases", []),
                ]
                or evidence_errors(contract)
            ):
                raise ProjectionHistoryError(
                    f"closure_history_evidence_invalid:{child_id}"
                )
            if not current_head_closure:
                authorized.add(event["event_id"])
    return frozenset(authorized)
