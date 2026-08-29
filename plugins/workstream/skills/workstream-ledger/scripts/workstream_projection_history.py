#!/usr/bin/env python3
"""Derive historical evidence authority from immutable closure event order."""

from __future__ import annotations

import hashlib
import json
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


def closure_bound_historical_evidence(
    projection_events: list[dict[str, Any]], current_scope: dict[str, Any],
) -> frozenset[str]:
    """Return active evidence event IDs authorized at a closed child's old head.

    Authority comes only from the scope and complete evidence set active before
    the immutable closure event. Current scope must still preserve the same
    repository identity and child ownership. Unclosed evidence gets no
    historical authority.
    """
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
        if (
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
            or closure.get("plan_revision")
            != closure_event.get("plan_revision")
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
        before_evidence = sorted(
            [
                event for (event_kind, _key), event in before.items()
                if event_kind == "evidence_contract"
                and event["value"].get("owning_child") == child_id
            ],
            key=lambda event: (event["key"], event["event_id"]),
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
