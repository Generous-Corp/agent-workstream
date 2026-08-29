#!/usr/bin/env python3
"""Idempotently reconcile the required append-only Linear resume projection."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_linear import (
    HttpGraphQLClient, LinearGraphQLTransport, LinearTransportError,
    resolve_authenticated_issue_route,
)
from workstream_linear_events import LinearCommentEventAdapter
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, LinearProjectionAdapter,
    LinearProjectionError, projection_slot_id, TOMBSTONE,
)
from workstream_plan import canonical_plan_url, plan_payload, same_plan_document
from workstream_relation_readback import RelationReadbackError, read_relation_targets
from workstream_resume import add_material_history, compact_context, extract_token, ResumeError
from workstream_scope import repository_key, ScopeError, validate_relation_graph
from workstream_successor import choose_disposition, SuccessorError
from workstream_evidence import evidence_errors
from workstream_child_closure import (
    canonical_digest, evidence_receipts_sha256, terminal_child_readback,
    ChildClosureError,
)


REQUIRED_KINDS = {"scope", "source", "provenance"}


def _value_digest(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _latest_historical_source(
    projection_history: list[dict[str, Any]] | None,
) -> dict[str, Any] | None:
    """Recover the last source head when the current plan generation is empty."""
    if projection_history is None:
        return None
    if not isinstance(projection_history, list):
        raise LinearProjectionError("invalid_projection_history")
    source_events = [
        event for event in projection_history
        if isinstance(event, dict) and event.get("kind") == "source"
    ]
    if not source_events:
        return None
    dated: list[tuple[datetime, dict[str, Any]]] = []
    for event in source_events:
        created_at = event.get("created_at")
        try:
            observed = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
        except ValueError as error:
            raise LinearProjectionError(
                "historical_source_order_ambiguous_requires_explicit_review"
            ) from error
        if observed.tzinfo is None:
            raise LinearProjectionError(
                "historical_source_order_ambiguous_requires_explicit_review"
            )
        dated.append((observed.astimezone(timezone.utc), event))
    latest_time = max(item[0] for item in dated)
    latest = [event for observed, event in dated if observed == latest_time]
    generations = {event.get("plan_revision") for event in latest}
    if len(generations) != 1:
        raise LinearProjectionError(
            "historical_source_order_ambiguous_requires_explicit_review"
        )
    generation = next(iter(generations))
    generation_events = sorted(
        (event for event in source_events if event.get("plan_revision") == generation),
        key=lambda event: (
            event.get("expected_revision"), event.get("created_at"),
            event.get("event_id"),
        ),
    )
    head: dict[str, Any] | None = None
    for event in generation_events:
        supersedes = event.get("supersedes_event_id")
        if (head is None and supersedes is not None) or (
            head is not None and supersedes != head.get("event_id")
        ):
            raise LinearProjectionError(
                "historical_source_chain_ambiguous_requires_explicit_review"
            )
        head = event
    if head is None or head.get("value") == TOMBSTONE:
        return None
    value = head.get("value")
    if not isinstance(value, dict):
        raise LinearProjectionError("invalid_historical_projection_source")
    return deepcopy(value)


def synchronize_manifest_source(
    manifest: dict[str, Any], description: str | None,
    authenticated_source: dict[str, Any],
    live_source: dict[str, Any] | None = None,
    projection_history: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Bind one labeled issue plan to the desired structured source."""
    canonical = canonical_plan_url(description)
    supplied_identity = authenticated_source.get("identity")
    if (
        isinstance(supplied_identity, str)
        and supplied_identity.startswith(("http://", "https://"))
        and not same_plan_document(canonical, supplied_identity)
    ):
        raise LinearProjectionError("plan_source_conflicts_canonical_issue_url")
    result = deepcopy(manifest)
    projection = result.get("projection")
    if not isinstance(projection, list):
        raise LinearProjectionError("manifest_projection_must_be_list")
    source_items = [
        item for item in projection
        if isinstance(item, dict) and item.get("kind") == "source"
    ]
    if len(source_items) > 1:
        raise LinearProjectionError("manifest_projection_multiple_sources")
    if source_items:
        current_value = source_items[0].get("value")
        if not isinstance(current_value, dict):
            raise LinearProjectionError("invalid_manifest_projection_source")
        current_identity = current_value.get("identity") or current_value.get("url")
        if current_identity and not same_plan_document(canonical, current_identity):
            raise LinearProjectionError("manifest_source_conflicts_canonical_issue_url")
    elif live_source is None:
        live_source = _latest_historical_source(projection_history)
    if live_source is not None:
        if not isinstance(live_source, dict):
            raise LinearProjectionError("invalid_live_projection_source")
        live_identity = live_source.get("identity") or live_source.get("url")
        if not isinstance(live_identity, str) or not live_identity:
            raise LinearProjectionError("invalid_live_projection_source")
        if not same_plan_document(canonical, live_identity) and not source_items:
            raise LinearProjectionError(
                "live_source_document_change_requires_explicit_review:"
                "add the canonical source to the reviewed projection manifest"
            )
    source = {
        "identity": canonical,
        "sha256": authenticated_source.get("sha256"),
    }
    item = {"kind": "source", "key": "root", "value": source}
    if source_items:
        projection[projection.index(source_items[0])] = item
    else:
        projection.append(item)
    return result, {**authenticated_source, "identity": canonical}


def validate_canonical_source_readback(
    description: str | None, authenticated_source: dict[str, Any],
) -> None:
    """Refuse success if the issue's canonical source changed during writes."""
    if canonical_plan_url(description) != authenticated_source.get("identity"):
        raise LinearProjectionError("canonical_plan_changed_during_projection")


def _active_heads(state: Any) -> dict[tuple[str, str], dict[str, Any]]:
    active: dict[tuple[str, str], dict[str, Any]] = {}
    for event in state.events:
        identity = (event["kind"], event["key"])
        if event["value"] == TOMBSTONE:
            active.pop(identity, None)
        else:
            active[identity] = event
    return active


def _latest_heads(state: Any) -> dict[tuple[str, str], dict[str, Any]]:
    """Return the latest event for every key, including tombstone heads."""
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for event in state.events:
        latest[(event["kind"], event["key"])] = event
    return latest


def projection_review_contract(state: Any) -> dict[str, Any]:
    """Return the exact remote projection surface a manifest must review."""
    legacy_events = (
        list(state.events)
        if state.events and all(event["schema_version"] == 1 for event in state.events)
        else []
    )
    quarantine = state.snapshot.get("projection_quarantined") or []
    return _contract_from_heads(
        state.revision, _active_heads(state),
        legacy_event_ids=[event["event_id"] for event in legacy_events],
        legacy_events_sha256=(
            _value_digest(legacy_events) if legacy_events else None
        ),
        quarantine_count=len(quarantine),
        quarantine_sha256=_value_digest(quarantine),
    )


def _contract_from_heads(
    revision: int, active: dict[tuple[str, str], dict[str, Any]],
    *, legacy_event_ids: list[str], legacy_events_sha256: str | None,
    quarantine_count: int, quarantine_sha256: str,
) -> dict[str, Any]:
    return {
        "expected_projection_revision": revision,
        "expected_active_heads": [
            {
                "kind": kind,
                "key": key,
                "event_id": event["event_id"],
                "value_sha256": _value_digest(event["value"]),
            }
            for (kind, key), event in sorted(active.items())
        ],
        "expected_legacy_v1_event_ids": list(legacy_event_ids),
        "expected_legacy_v1_events_sha256": legacy_events_sha256,
        "expected_projection_quarantine_count": quarantine_count,
        "expected_projection_quarantine_sha256": quarantine_sha256,
    }


def _reviewed_manifest(manifest: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    required = {
        "projection", "retirements", "expected_projection_revision",
        "expected_active_heads", "expected_legacy_v1_event_ids",
        "expected_legacy_v1_events_sha256", "expected_projection_quarantine_count",
        "expected_projection_quarantine_sha256",
    }
    allowed = required | {"terminal_child_repairs"}
    if not isinstance(manifest, dict) or frozenset(manifest) not in {
        frozenset(required), frozenset(allowed),
    }:
        raise LinearProjectionError("manifest_review_contract_required")
    revision = manifest["expected_projection_revision"]
    if not isinstance(revision, int) or revision < 0:
        raise LinearProjectionError("invalid_manifest_projection_revision")
    heads = manifest["expected_active_heads"]
    if not isinstance(heads, list):
        raise LinearProjectionError("manifest_active_heads_must_be_list")
    identities: set[tuple[str, str]] = set()
    for index, head in enumerate(heads):
        if not isinstance(head, dict) or set(head) != {
            "kind", "key", "event_id", "value_sha256",
        }:
            raise LinearProjectionError(f"invalid_manifest_active_head:{index}")
        identity = (head.get("kind"), head.get("key"))
        if not all(isinstance(value, str) and value for value in identity):
            raise LinearProjectionError(f"invalid_manifest_active_head_identity:{index}")
        if identity in identities:
            raise LinearProjectionError(
                f"duplicate_manifest_active_head:{identity[0]}:{identity[1]}"
            )
        if not isinstance(head.get("event_id"), str) or not head["event_id"]:
            raise LinearProjectionError(f"invalid_manifest_active_head_event:{index}")
        if not re.fullmatch(r"[0-9a-f]{64}", str(head.get("value_sha256", ""))):
            raise LinearProjectionError(f"invalid_manifest_active_head_digest:{index}")
        identities.add(identity)
    legacy_ids = manifest["expected_legacy_v1_event_ids"]
    legacy_digest = manifest["expected_legacy_v1_events_sha256"]
    if (
        not isinstance(legacy_ids, list)
        or legacy_ids != list(dict.fromkeys(legacy_ids))
        or not all(isinstance(event_id, str) and event_id for event_id in legacy_ids)
        or (
            legacy_ids
            and not re.fullmatch(r"[0-9a-f]{64}", str(legacy_digest or ""))
        )
        or (not legacy_ids and legacy_digest is not None)
    ):
        raise LinearProjectionError("invalid_manifest_legacy_v1_contract")
    quarantine_count = manifest["expected_projection_quarantine_count"]
    if (
        not isinstance(quarantine_count, int)
        or quarantine_count < 0
        or not re.fullmatch(
            r"[0-9a-f]{64}",
            str(manifest["expected_projection_quarantine_sha256"]),
        )
    ):
        raise LinearProjectionError("invalid_manifest_projection_quarantine_contract")
    retirements = manifest["retirements"]
    if not isinstance(retirements, list):
        raise LinearProjectionError("manifest_retirements_must_be_list")
    retired: set[tuple[str, str]] = set()
    for index, retirement in enumerate(retirements):
        if not isinstance(retirement, dict) or set(retirement) != {
            "kind", "key", "expected_event_id", "expected_value_sha256",
        }:
            raise LinearProjectionError(f"invalid_manifest_retirement:{index}")
        identity = (retirement.get("kind"), retirement.get("key"))
        if not all(isinstance(value, str) and value for value in identity):
            raise LinearProjectionError(f"invalid_manifest_retirement_identity:{index}")
        if identity in retired:
            raise LinearProjectionError(
                f"duplicate_manifest_retirement:{identity[0]}:{identity[1]}"
            )
        if not isinstance(retirement.get("expected_event_id"), str) or not retirement["expected_event_id"]:
            raise LinearProjectionError(f"invalid_manifest_retirement_event:{index}")
        if not re.fullmatch(r"[0-9a-f]{64}", str(retirement.get("expected_value_sha256", ""))):
            raise LinearProjectionError(f"invalid_manifest_retirement_digest:{index}")
        retired.add(identity)
    repairs = manifest.get("terminal_child_repairs", [])
    if not isinstance(repairs, list) or len(repairs) > 1:
        raise LinearProjectionError("invalid_manifest_terminal_child_repairs")
    for index, repair in enumerate(repairs):
        if not isinstance(repair, dict) or set(repair) != {
            "child_identifier", "child_issue_id", "expected_child_readback_sha256",
            "expected_assignee_id", "approved_evidence_heads",
        }:
            raise LinearProjectionError(f"invalid_manifest_terminal_child_repair:{index}")
        heads = repair.get("approved_evidence_heads")
        valid_heads = (
            isinstance(heads, list)
            and bool(heads)
            and all(
                isinstance(head, dict)
                and set(head) == {"key", "event_id", "value_sha256"}
                and all(isinstance(head.get(field), str) and head[field]
                        for field in ("key", "event_id"))
                and re.fullmatch(r"[0-9a-f]{64}", str(head.get("value_sha256", "")))
                for head in heads
            )
        )
        if (
            not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(repair.get("child_identifier", "")))
            or not all(isinstance(repair.get(field), str) and repair[field]
                       for field in ("child_issue_id", "expected_assignee_id"))
            or not re.fullmatch(r"[0-9a-f]{64}", str(repair.get("expected_child_readback_sha256", "")))
            or not valid_heads
            or heads != sorted(heads, key=lambda item: (item.get("key", ""), item.get("event_id", "")))
        ):
            raise LinearProjectionError(f"invalid_manifest_terminal_child_repair:{index}")
    return _desired_items(manifest), retirements


def prepare_terminal_child_repairs(
    manifest: dict[str, Any], snapshot: dict[str, Any], state: Any,
) -> dict[str, Any]:
    """Derive one terminal ownership repair from exact live/evidence truth."""
    result = deepcopy(manifest)
    _reviewed_manifest(result)
    original_contract = {
        key: deepcopy(result[key]) for key in (
            "expected_projection_revision", "expected_active_heads",
            "expected_legacy_v1_event_ids", "expected_legacy_v1_events_sha256",
            "expected_projection_quarantine_count",
            "expected_projection_quarantine_sha256",
        )
    }
    repairs = result.get("terminal_child_repairs") or []
    if not repairs:
        return result
    if result.get("retirements"):
        raise LinearProjectionError("terminal_child_repair_forbids_retirements")
    repair = repairs[0]
    child_id = repair["child_identifier"].upper()
    matching = [
        child for child in snapshot.get("children", [])
        if str(child.get("identifier", "")).upper() == child_id
    ]
    if len(matching) != 1:
        raise LinearProjectionError("terminal_child_repair_ambiguous_child")
    child = matching[0]
    try:
        readback = terminal_child_readback(child)
    except ChildClosureError as error:
        raise LinearProjectionError(str(error)) from error
    if (
        readback["child_issue_id"] != repair["child_issue_id"]
        or readback["assignee_id"] != repair["expected_assignee_id"]
        or canonical_digest(readback) != repair["expected_child_readback_sha256"]
    ):
        raise LinearProjectionError("terminal_child_readback_changed_reload_required")

    active = _active_heads(state)
    scope_event = active.get(("scope", "root"))
    if scope_event is None:
        raise LinearProjectionError("terminal_child_repair_scope_missing")
    current_scope = deepcopy(scope_event["value"])
    for field in ("workspace_id", "team_id", "project_id", "root_issue_id"):
        expected = current_scope["linear"][field]
        observed = (
            readback["parent_issue_id"] if field == "root_issue_id"
            else readback[field]
        )
        if observed != expected:
            raise LinearProjectionError(f"terminal_child_repair_route_mismatch:{field}")

    contracts: list[dict[str, Any]] = []
    approved_heads: list[dict[str, str]] = []
    active_evidence_heads = [
        {
            "key": key,
            "event_id": event["event_id"],
            "value_sha256": canonical_digest(event["value"]),
        }
        for (kind, key), event in active.items()
        if kind == "evidence_contract"
        and event["value"].get("owning_child") == child_id
    ]
    active_evidence_heads.sort(key=lambda item: (item["key"], item["event_id"]))
    if repair["approved_evidence_heads"] != active_evidence_heads:
        raise LinearProjectionError(
            "terminal_child_repair_evidence_set_changed_reload_required"
        )
    for expected in repair["approved_evidence_heads"]:
        event = active.get(("evidence_contract", expected["key"]))
        if (
            event is None
            or event["event_id"] != expected["event_id"]
            or canonical_digest(event["value"]) != expected["value_sha256"]
        ):
            raise LinearProjectionError("terminal_child_repair_evidence_changed_reload_required")
        contract = event["value"]
        errors = evidence_errors(contract)
        if errors or contract.get("owning_child") != child_id:
            raise LinearProjectionError(
                "terminal_child_repair_evidence_invalid:"
                + ",".join(errors or ["wrong_owner"])
            )
        contracts.append(contract)
        approved_heads.append(deepcopy(expected))
    owners = {(contract["repository_key"], contract["exact_head"]) for contract in contracts}
    if len(owners) != 1:
        raise LinearProjectionError("terminal_child_repair_owner_ambiguous")
    owner, exact_head = next(iter(owners))
    repository = next((
        item for item in current_scope["repositories"]
        if repository_key(item) == owner
    ), None)
    if repository is None or repository.get("exact_head") != exact_head:
        raise LinearProjectionError("terminal_child_repair_repository_head_mismatch")
    existing_owner = current_scope["child_ownership"].get(child_id)
    if existing_owner not in {None, owner}:
        raise LinearProjectionError("terminal_child_repair_ownership_conflict")
    desired_scope = deepcopy(current_scope)
    desired_scope["child_ownership"][child_id] = owner
    closure = {
        "schema_version": 1,
        **readback,
        "plan_revision": scope_event["plan_revision"],
        "repository_key": owner,
        "exact_head": exact_head,
        "evidence_heads": approved_heads,
        "evidence_receipts_sha256": evidence_receipts_sha256(contracts),
        "child_readback_sha256": canonical_digest(readback),
    }
    existing_closure = active.get(("child_closure", child_id))
    if existing_closure is not None and existing_closure["value"] != closure:
        raise LinearProjectionError("terminal_child_repair_closure_conflict")

    desired = result["projection"]
    scope_item = next(item for item in desired if item["kind"] == "scope")
    pre_repair_scope = deepcopy(current_scope)
    pre_repair_scope["child_ownership"].pop(child_id, None)
    if (
        scope_item["value"] != current_scope
        and scope_item["value"] != desired_scope
        and scope_item["value"] != pre_repair_scope
    ):
        raise LinearProjectionError("terminal_child_repair_scope_widened")
    source_item = next(item for item in desired if item["kind"] == "source")
    current_source = active.get(("source", "root"))
    if current_source is None or source_item["value"] != current_source["value"]:
        raise LinearProjectionError("terminal_child_repair_source_changed")
    for item in desired:
        identity = (item["kind"], item["key"])
        if identity == ("scope", "root") or identity == ("child_closure", child_id):
            continue
        current = active.get(identity)
        if current is None or current["value"] != item["value"]:
            raise LinearProjectionError(
                f"terminal_child_repair_unrelated_change:{identity[0]}:{identity[1]}"
            )
    scope_item["value"] = desired_scope
    desired[:] = [
        {"kind": "child_closure", "key": child_id, "value": closure},
        *[
            item for item in desired
            if (item["kind"], item["key"]) != ("child_closure", child_id)
        ],
    ]

    current_contract = projection_review_contract(state)
    if current_contract != original_contract and existing_closure is not None:
        expected_heads = {
            (head["kind"], head["key"]): head
            for head in original_contract["expected_active_heads"]
        }
        allowed_identities = set(expected_heads) | {("child_closure", child_id)}
        unchanged = all(
            identity in active
            and active[identity]["event_id"] == expected["event_id"]
            and canonical_digest(active[identity]["value"])
            == expected["value_sha256"]
            for identity, expected in expected_heads.items()
            if identity != ("scope", "root")
        )
        expected_scope = expected_heads.get(("scope", "root"))
        historical_scope = next((
            event for event in state.events
            if expected_scope is not None
            and event["event_id"] == expected_scope["event_id"]
        ), None)
        active_scope = active.get(("scope", "root"))
        scope_changed = bool(
            expected_scope is not None
            and active_scope is not None
            and active_scope["event_id"] != expected_scope["event_id"]
        )
        closure_added = ("child_closure", child_id) not in expected_heads
        expected_revision = original_contract["expected_projection_revision"]
        allowed_progress = (
            set(active) == allowed_identities
            and unchanged
            and historical_scope is not None
            and canonical_digest(historical_scope["value"])
            == expected_scope["value_sha256"]
            and active_scope is not None
            and active_scope["value"] in (current_scope, desired_scope)
            and existing_closure["value"] == closure
            and state.revision
            == expected_revision + int(closure_added) + int(scope_changed)
            and all(
                current_contract[field] == original_contract[field]
                for field in (
                    "expected_legacy_v1_event_ids",
                    "expected_legacy_v1_events_sha256",
                    "expected_projection_quarantine_count",
                    "expected_projection_quarantine_sha256",
                )
            )
        )
        if allowed_progress:
            result.update(current_contract)
    return result


def stable_live_readback(
    transport: LinearGraphQLTransport,
    comments: LinearCommentEventAdapter,
    token: str, *, include_description: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Double-collect both surfaces and refuse a mixed concurrent snapshot."""
    graph_before = transport.snapshot_for_root(
        token, include_description=include_description,
    )
    comments_before = comments.comments()
    graph_after = transport.snapshot_for_root(
        token, include_description=include_description,
    )
    comments_after = comments.comments()
    graph_fence = transport.snapshot_for_root(
        token, include_description=include_description,
    )
    if (
        graph_before != graph_after
        or graph_after != graph_fence
        or comments_before != comments_after
    ):
        raise LinearProjectionError("projection_final_readback_changed_during_read")
    return graph_fence, comments_after


def _ordered_write_items(
    items: list[dict[str, Any]],
    unresolved_relations: set[tuple[str, str]] | frozenset[tuple[str, str]],
) -> list[dict[str, Any]]:
    """Keep migration repairs first and authority-enabling scope last."""
    migration = [
        item for item in items
        if (item["kind"], item["key"]) in unresolved_relations
    ]
    remaining = [
        item for item in items
        if (item["kind"], item["key"]) not in unresolved_relations
    ]
    if not any(item["kind"] == "child_closure" for item in remaining):
        return [*migration, *remaining]
    closures = [item for item in remaining if item["kind"] == "child_closure"]
    disposition = [item for item in remaining if item["kind"] == "disposition"]
    scopes = [item for item in remaining if item["kind"] == "scope"]
    ordinary = [
        item for item in remaining
        if item["kind"] not in {"child_closure", "disposition", "scope"}
    ]
    return [*migration, *closures, *ordinary, *disposition, *scopes]


def _require_repairs_for_changed_child_closures(
    desired: list[dict[str, Any]],
    active: dict[tuple[str, str], dict[str, Any]],
    repairs: list[dict[str, Any]],
) -> None:
    """Bind every closure creation/replacement to its reviewed live fence."""
    repairs_by_child = {
        repair["child_identifier"].upper(): repair for repair in repairs
    }
    for item in desired:
        if item["kind"] != "child_closure":
            continue
        child_id = item["key"].upper()
        current = active.get(("child_closure", child_id))
        if current is not None and current["value"] == item["value"]:
            continue
        repair = repairs_by_child.get(child_id)
        closure = item["value"]
        if repair is None:
            raise LinearProjectionError(
                f"terminal_child_closure_repair_required:{child_id}"
            )
        if (
            closure.get("child_identifier") != child_id
            or closure.get("child_issue_id") != repair["child_issue_id"]
            or closure.get("assignee_id") != repair["expected_assignee_id"]
            or closure.get("child_readback_sha256")
            != repair["expected_child_readback_sha256"]
            or closure.get("evidence_heads") != repair["approved_evidence_heads"]
        ):
            raise LinearProjectionError(
                f"terminal_child_closure_repair_mismatch:{child_id}"
            )


def load_material_history_for_projection_reconcile(
    snapshot: dict[str, Any], comments: list[dict[str, Any]], token: str,
    manifest: dict[str, Any], adapter: LinearProjectionAdapter, *,
    authenticated_route: dict[str, str], authenticated_source: dict[str, Any],
    remote_head: str | None = None,
    max_bytes: int = 16 * 1024, max_items: int = 100,
    relation_target_resolver: Callable[
        [list[dict[str, Any]]], dict[str, dict[str, Any]]
    ],
) -> tuple[dict[str, Any], frozenset[tuple[str, str]]]:
    """Load strict history, except for an exactly reviewed projection repair.

    Historical relation heads can predate the peer projection contract.  They
    may be inspected only by this reconcile boundary, and only when every head
    whose authenticated peer readback is incomplete is exactly retired or
    replaced by the reviewed manifest. A newly added child or synchronized plan
    URL can also make the current scope/source fail strict resume before the
    reviewed replacement is appended. In that case, validate the exact
    candidate projection entirely in memory first. Ordinary resume never calls
    this helper and remains strict.
    """
    desired, reviewed_retirements = _reviewed_manifest(manifest)
    initial = adapter.state()
    reviewed_contract = {
        "expected_projection_revision": manifest["expected_projection_revision"],
        "expected_active_heads": sorted(
            manifest["expected_active_heads"],
            key=lambda item: (item["kind"], item["key"]),
        ),
        "expected_legacy_v1_event_ids": manifest["expected_legacy_v1_event_ids"],
        "expected_legacy_v1_events_sha256": manifest[
            "expected_legacy_v1_events_sha256"
        ],
        "expected_projection_quarantine_count": manifest[
            "expected_projection_quarantine_count"
        ],
        "expected_projection_quarantine_sha256": manifest[
            "expected_projection_quarantine_sha256"
        ],
    }
    active = _active_heads(initial)
    _require_repairs_for_changed_child_closures(
        desired, active, manifest.get("terminal_child_repairs") or [],
    )
    desired_by_identity = {
        (item["kind"], item["key"]): item["value"] for item in desired
    }
    current_scope_event = active.get(("scope", "root"))
    desired_scope = desired_by_identity.get(("scope", "root"))
    if current_scope_event is not None and isinstance(desired_scope, dict):
        current_owners = current_scope_event["value"].get("child_ownership") or {}
        desired_owners = desired_scope.get("child_ownership") or {}
        added_owners = set(desired_owners) - set(current_owners)
        terminal_children = {
            str(child.get("identifier", "")).upper()
            for child in snapshot.get("children", [])
            if str(child.get("status_type") or child.get("status") or "").lower()
            in {"done", "completed", "cancelled", "canceled", "superseded"}
        }
        repair_ids = {
            repair["child_identifier"].upper()
            for repair in manifest.get("terminal_child_repairs", [])
        }
        for child_id in sorted(added_owners & terminal_children):
            if (
                child_id not in repair_ids
                or ("child_closure", child_id) not in desired_by_identity
            ):
                raise LinearProjectionError(
                    f"terminal_child_ownership_repair_required:{child_id}"
                )
    unresolved: set[tuple[str, str]] = set()
    for identity, event in active.items():
        if identity[0] != "relation":
            continue
        try:
            relation_target_resolver([deepcopy(event["value"])])
        except RelationReadbackError:
            unresolved.add(identity)

    retirements_by_identity = {
        (item["kind"], item["key"]): item for item in reviewed_retirements
    }
    uncovered: list[str] = []
    for identity in sorted(unresolved):
        current = active[identity]
        replacement = desired_by_identity.get(identity)
        retirement = retirements_by_identity.get(identity)
        replaced = replacement is not None and replacement != current["value"]
        retired = retirement is not None and (
            retirement["expected_event_id"] == current["event_id"]
            and retirement["expected_value_sha256"]
            == _value_digest(current["value"])
        )
        if not replaced and not retired:
            uncovered.append(f"{identity[0]}:{identity[1]}")
    if uncovered:
        raise LinearProjectionError(
            "legacy_unresolved_relation_migration_required:"
            + ",".join(uncovered)
        )
    authority_sensitive_changes = any(
        identity[0] in {
            "scope", "source", "evidence_contract", "child_closure",
        }
        and (identity not in active or active[identity]["value"] != value)
        for identity, value in desired_by_identity.items()
    ) or any(
        retirement["kind"] in {"evidence_contract", "child_closure"}
        for retirement in reviewed_retirements
    )
    if authority_sensitive_changes:
        if remote_head is None:
            raise LinearProjectionError("prospective_remote_head_required")
        if projection_review_contract(initial) != reviewed_contract:
            raise LinearProjectionError("projection_review_stale_reload_required")
        latest = _latest_heads(initial)
        retirement_items: list[dict[str, Any]] = []
        for retirement in reviewed_retirements:
            identity = (retirement["kind"], retirement["key"])
            current = active.get(identity)
            if (
                current is None
                or current["event_id"] != retirement["expected_event_id"]
                or _value_digest(current["value"])
                != retirement["expected_value_sha256"]
            ):
                raise LinearProjectionError(
                    f"projection_retirement_stale:{identity[0]}:{identity[1]}"
                )
            retirement_items.append({
                "kind": identity[0], "key": identity[1], "value": TOMBSTONE,
            })

        def prospective(items: list[dict[str, Any]]) -> dict[str, Any]:
            candidate_comments = deepcopy(comments)
            candidate_active = dict(active)
            candidate_latest = dict(latest)
            expected_revision = initial.revision
            for item in items:
                identity = (item["kind"], item["key"])
                current = candidate_active.get(identity)
                if current is not None and current["value"] == item["value"]:
                    continue
                previous = candidate_latest.get(identity)
                event = build_projection_event(
                    workstream_id=adapter.workstream_id,
                    kind=item["kind"], key=item["key"], value=item["value"],
                    plan_revision=adapter.plan_revision,
                    expected_revision=expected_revision,
                    created_at="1970-01-01T00:00:00Z",
                    supersedes_event_id=(
                        previous["event_id"] if previous else None
                    ),
                    authority=adapter.authority,
                )
                candidate_comments.append({
                    "id": projection_slot_id(
                        adapter.workstream_id, adapter.plan_revision,
                        expected_revision, adapter.authority,
                    ),
                    "body": encode_projection_comment(event),
                })
                expected_revision += 1
                candidate_latest[identity] = event
                if item["value"] == TOMBSTONE:
                    candidate_active.pop(identity, None)
                else:
                    candidate_active[identity] = event
            return add_material_history(
                snapshot, candidate_comments, token,
                authenticated_route=authenticated_route,
                authenticated_source=authenticated_source,
                relation_target_resolver=relation_target_resolver,
                permit_stale_lifecycle_for_reconcile=True,
            )

        provisional = prospective(_ordered_write_items(
            [*desired, *retirement_items], unresolved,
        ))
        provisional = dict(provisional)
        provisional.pop("disposition", None)
        decision = choose_disposition(provisional, remote_head=remote_head)
        disposition = {
            "kind": "disposition", "key": "root", "value": {
                "disposition": decision["disposition"],
                "remote_head": remote_head,
                "recovered_from_checkpoint": decision.get(
                    "recovered_from_checkpoint"
                ),
            },
        }
        candidate_items = _ordered_write_items(
            [*desired, disposition, *retirement_items], unresolved,
        )
        candidate = prospective(candidate_items)
        compact_context(
            candidate, token, max_bytes=max_bytes, max_items=max_items,
            require_projection_authority=True,
        )
        return candidate, frozenset(unresolved)

    try:
        return add_material_history(
            snapshot, comments, token, authenticated_route=authenticated_route,
            authenticated_source=authenticated_source,
            relation_target_resolver=relation_target_resolver,
        ), frozenset()
    except RelationReadbackError:
        if projection_review_contract(initial) != reviewed_contract:
            raise LinearProjectionError("projection_review_stale_reload_required")

        if not unresolved:
            # Do not turn an unexpected batched-read failure into a bypass.
            raise

        return add_material_history(
            snapshot, comments, token, authenticated_route=authenticated_route,
            authenticated_source=authenticated_source,
            permit_stale_lifecycle_for_reconcile=True,
        ), frozenset(unresolved)


def _desired_items(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    items = manifest.get("projection")
    if not isinstance(items, list):
        raise LinearProjectionError("manifest_projection_must_be_list")
    seen: set[tuple[str, str]] = set()
    kinds: set[str] = set()
    result: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        if not isinstance(item, dict) or set(item) != {"kind", "key", "value"}:
            raise LinearProjectionError(f"invalid_manifest_projection_item:{index}")
        if item["kind"] == "disposition":
            raise LinearProjectionError("manifest_disposition_is_computed")
        identity = (item.get("kind"), item.get("key"))
        if not all(isinstance(value, str) and value for value in identity):
            raise LinearProjectionError(f"invalid_manifest_projection_identity:{index}")
        if identity in seen:
            raise LinearProjectionError(f"duplicate_manifest_projection_identity:{identity[0]}:{identity[1]}")
        if not isinstance(item.get("value"), dict):
            raise LinearProjectionError(f"invalid_manifest_projection_value:{index}")
        seen.add(identity)
        kinds.add(identity[0])
        result.append(deepcopy(item))
    missing = sorted(REQUIRED_KINDS - kinds)
    if missing:
        raise LinearProjectionError("manifest_projection_missing:" + ",".join(missing))
    if sum(item["kind"] == "scope" for item in result) != 1 or sum(
        item["kind"] == "source" for item in result
    ) != 1:
        raise LinearProjectionError("manifest_projection_singleton_invalid")
    return result


def reconcile_required_projection(
    adapter: LinearProjectionAdapter, snapshot: dict[str, Any],
    manifest: dict[str, Any], *, remote_head: str, created_at: str,
    authenticated_source: dict[str, Any],
    relation_target_resolver: (
        Callable[[list[dict[str, Any]]], dict[str, dict[str, Any]]] | None
    ) = None,
    terminal_child_fence: Callable[[str], str] | None = None,
    legacy_unresolved_relation_heads: frozenset[tuple[str, str]] = frozenset(),
) -> dict[str, Any]:
    """Append only missing/changed values and verify the complete current view."""
    if not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", remote_head):
        raise LinearProjectionError("verified_full_remote_head_required")
    desired, reviewed_retirements = _reviewed_manifest(manifest)
    scope_item = next(item for item in desired if item["kind"] == "scope")
    source_item = next(item for item in desired if item["kind"] == "source")
    source_identity = source_item["value"].get("identity") or source_item["value"].get("url")
    if source_identity != authenticated_source.get("identity"):
        raise LinearProjectionError("projection_source_identity_mismatch")
    if source_item["value"].get("sha256") != authenticated_source.get("sha256"):
        raise LinearProjectionError("projection_source_bytes_mismatch")
    if source_item["value"].get("sha256") != adapter.plan_revision:
        raise LinearProjectionError("root_plan_revision_source_bytes_mismatch")
    if all((adapter.workspace_id, adapter.team_id, adapter.project_id, adapter.root_issue_id)):
        linear = scope_item["value"].get("linear") or {}
        for field, expected in (
            ("workspace_id", adapter.workspace_id), ("team_id", adapter.team_id),
            ("project_id", adapter.project_id), ("root_issue_id", adapter.root_issue_id),
        ):
            if linear.get(field) != expected:
                raise LinearProjectionError(f"projection_route_mismatch:{field}")
    if manifest.get("terminal_child_repairs"):
        primary_key = scope_item["value"].get("primary_repository")
        primary = next((
            repository for repository in scope_item["value"].get("repositories", [])
            if repository_key(repository) == primary_key
        ), None)
        if primary is None or primary.get("exact_head") != remote_head:
            raise LinearProjectionError(
                "terminal_child_repair_primary_head_mismatch"
            )

    disposition_input = dict(snapshot)
    disposition_input.pop("disposition", None)
    # The reviewed provenance being persisted is part of this same durable
    # operation, so disposition must be derived from it on first creation too.
    disposition_input["provenance"] = [
        item["value"] for item in desired if item["kind"] == "provenance"
    ]
    decision = choose_disposition(disposition_input, remote_head=remote_head)
    disposition = {
        "disposition": decision["disposition"],
        "remote_head": remote_head,
        "recovered_from_checkpoint": decision.get("recovered_from_checkpoint"),
    }
    desired.append({"kind": "disposition", "key": "root", "value": disposition})
    desired_by_identity = {
        (item["kind"], item["key"]): item["value"] for item in desired
    }

    # Validate every event envelope before the first irreversible append.
    initial = adapter.state()
    observed_contract = projection_review_contract(initial)
    reviewed_contract = {
        "expected_projection_revision": manifest["expected_projection_revision"],
        "expected_active_heads": sorted(
            manifest["expected_active_heads"], key=lambda item: (item["kind"], item["key"])
        ),
        "expected_legacy_v1_event_ids": manifest["expected_legacy_v1_event_ids"],
        "expected_legacy_v1_events_sha256": manifest[
            "expected_legacy_v1_events_sha256"
        ],
        "expected_projection_quarantine_count": manifest[
            "expected_projection_quarantine_count"
        ],
        "expected_projection_quarantine_sha256": manifest[
            "expected_projection_quarantine_sha256"
        ],
    }
    if observed_contract != reviewed_contract:
        raise LinearProjectionError("projection_review_stale_reload_required")
    active_heads = _active_heads(initial)
    _require_repairs_for_changed_child_closures(
        desired, active_heads, manifest.get("terminal_child_repairs") or [],
    )
    latest_heads = _latest_heads(initial)
    retirements: list[dict[str, Any]] = []
    for retirement in reviewed_retirements:
        identity = (retirement["kind"], retirement["key"])
        if identity in desired_by_identity:
            raise LinearProjectionError(
                f"projection_retirement_still_desired:{identity[0]}:{identity[1]}"
            )
        current = active_heads.get(identity)
        if current is None:
            raise LinearProjectionError(
                f"projection_retirement_missing:{identity[0]}:{identity[1]}"
            )
        if (
            current["event_id"] != retirement["expected_event_id"]
            or _value_digest(current["value"]) != retirement["expected_value_sha256"]
        ):
            raise LinearProjectionError(
                f"projection_retirement_stale:{identity[0]}:{identity[1]}"
            )
        retirements.append({
            "kind": identity[0], "key": identity[1], "value": TOMBSTONE,
        })

    for identity in legacy_unresolved_relation_heads:
        current = active_heads.get(identity)
        replacement = desired_by_identity.get(identity)
        retired = any(
            (item["kind"], item["key"]) == identity for item in retirements
        )
        if (
            current is None
            or identity[0] != "relation"
            or (not retired and (replacement is None or replacement == current["value"]))
        ):
            raise LinearProjectionError(
                f"legacy_unresolved_relation_migration_required:{identity[0]}:{identity[1]}"
            )

    effective_relations = {
        key: deepcopy(event["value"])
        for (kind, key), event in active_heads.items()
        if kind == "relation"
    }
    effective_relations.update({
        key: deepcopy(value)
        for (kind, key), value in desired_by_identity.items()
        if kind == "relation"
    })
    for retirement in retirements:
        if retirement["kind"] == "relation":
            effective_relations.pop(retirement["key"], None)
    if effective_relations:
        if relation_target_resolver is None:
            raise LinearProjectionError("relation_target_readback_required")
        relations = [effective_relations[key] for key in sorted(effective_relations)]
        try:
            validate_relation_graph(
                relations, root_id=adapter.workstream_id,
                workspace_id=adapter.workspace_id,
                root_issue_id=adapter.root_issue_id,
                resolve_target=relation_target_resolver(relations),
            )
        except ScopeError as error:
            raise LinearProjectionError(str(error)) from error

    write_items = _ordered_write_items(
        [*desired, *retirements], legacy_unresolved_relation_heads,
    )

    for item in write_items:
        build_projection_event(
            workstream_id=adapter.workstream_id,
            kind=item["kind"], key=item["key"], value=item["value"],
            plan_revision=adapter.plan_revision, expected_revision=0,
            created_at=created_at,
            authority=adapter.authority,
        )

    # Re-read the exact reviewed surface immediately before the first append.
    # A late unrelated key is as material as a changed reviewed head: neither
    # may be silently retained or tombstoned by this reconciliation.
    if projection_review_contract(adapter.state()) != observed_contract:
        raise LinearProjectionError("projection_review_stale_reload_required")

    repair = (manifest.get("terminal_child_repairs") or [None])[0]
    if repair is not None:
        if terminal_child_fence is None:
            raise LinearProjectionError("terminal_child_readback_fence_required")
        if terminal_child_fence(repair["child_identifier"]) != repair[
            "expected_child_readback_sha256"
        ]:
            raise LinearProjectionError(
                "terminal_child_readback_changed_reload_required"
            )

    activation_receipt = None
    if initial.events and all(
        event["schema_version"] == 1 for event in initial.events
    ):
        legacy_event_ids = manifest["expected_legacy_v1_event_ids"]
        activation_receipt = adapter.activate_v2(
            created_at=created_at, expected_revision=initial.revision,
            expected_legacy_event_ids=legacy_event_ids,
            expected_legacy_events_sha256=manifest[
                "expected_legacy_v1_events_sha256"
            ],
        )
        activated = adapter.state()
        activated_contract = projection_review_contract(activated)
        if (
            activated.revision != initial.revision + 1
            or [event["event_id"] for event in activated.events[:initial.revision]]
            != legacy_event_ids
            or activated.events[-1]["kind"] != "cas_activation"
            or activated.events[-1]["value"].get("legacy_event_ids") != legacy_event_ids
            or activated_contract["expected_legacy_v1_event_ids"] != []
            or activated_contract["expected_legacy_v1_events_sha256"] is not None
            or activated_contract["expected_projection_quarantine_count"] != 0
        ):
            raise LinearProjectionError("projection_v2_activation_readback_mismatch")
        initial = activated
        observed_contract = activated_contract
        active_heads = _active_heads(initial)
        latest_heads = _latest_heads(initial)

    receipts: list[dict[str, Any]] = (
        [activation_receipt] if activation_receipt is not None else []
    )
    expected_revision = initial.revision
    expected_active_heads = dict(active_heads)
    expected_latest_heads = dict(latest_heads)
    for item in write_items:
        state = adapter.state()
        if projection_review_contract(state) != _contract_from_heads(
            expected_revision, expected_active_heads,
            legacy_event_ids=observed_contract["expected_legacy_v1_event_ids"],
            legacy_events_sha256=observed_contract[
                "expected_legacy_v1_events_sha256"
            ],
            quarantine_count=observed_contract[
                "expected_projection_quarantine_count"
            ],
            quarantine_sha256=observed_contract[
                "expected_projection_quarantine_sha256"
            ],
        ):
            raise LinearProjectionError("projection_changed_during_reconcile")
        identity = (item["kind"], item["key"])
        active_current = expected_active_heads.get(identity)
        if active_current is not None and active_current["value"] == item["value"]:
            continue
        latest_current = expected_latest_heads.get(identity)
        if repair is not None and terminal_child_fence(
            repair["child_identifier"]
        ) != repair["expected_child_readback_sha256"]:
            raise LinearProjectionError(
                "terminal_child_readback_changed_reload_required"
            )
        event = build_projection_event(
            workstream_id=adapter.workstream_id,
            kind=item["kind"], key=item["key"], value=item["value"],
            plan_revision=adapter.plan_revision,
            expected_revision=expected_revision, created_at=created_at,
            supersedes_event_id=(
                latest_current["event_id"] if latest_current else None
            ),
            authority=adapter.authority,
        )
        receipts.append(adapter.append(
            event,
            expected_quarantine_count=observed_contract[
                "expected_projection_quarantine_count"
            ],
            expected_quarantine_sha256=observed_contract[
                "expected_projection_quarantine_sha256"
            ],
        ))
        expected_revision += 1
        expected_latest_heads[identity] = event
        if item["value"] == TOMBSTONE:
            expected_active_heads.pop(identity, None)
        else:
            expected_active_heads[identity] = event
        if projection_review_contract(adapter.state()) != _contract_from_heads(
            expected_revision, expected_active_heads,
            legacy_event_ids=observed_contract["expected_legacy_v1_event_ids"],
            legacy_events_sha256=observed_contract[
                "expected_legacy_v1_events_sha256"
            ],
            quarantine_count=observed_contract[
                "expected_projection_quarantine_count"
            ],
            quarantine_sha256=observed_contract[
                "expected_projection_quarantine_sha256"
            ],
        ):
            raise LinearProjectionError("projection_changed_during_reconcile")

    final = adapter.state()
    if repair is not None and terminal_child_fence(repair["child_identifier"]) != repair[
        "expected_child_readback_sha256"
    ]:
        raise LinearProjectionError(
            "terminal_child_readback_changed_reload_required"
        )
    if projection_review_contract(final) != _contract_from_heads(
        expected_revision, expected_active_heads,
        legacy_event_ids=observed_contract["expected_legacy_v1_event_ids"],
        legacy_events_sha256=observed_contract[
            "expected_legacy_v1_events_sha256"
        ],
        quarantine_count=observed_contract[
            "expected_projection_quarantine_count"
        ],
        quarantine_sha256=observed_contract[
            "expected_projection_quarantine_sha256"
        ],
    ):
        raise LinearProjectionError("projection_final_contract_mismatch")
    active: dict[tuple[str, str], dict[str, Any]] = {}
    for event in final.events:
        identity = (event["kind"], event["key"])
        if event["value"] == TOMBSTONE:
            active.pop(identity, None)
        else:
            active[identity] = event["value"]
    expected_active = {
        identity: deepcopy(event["value"])
        for identity, event in active_heads.items()
    }
    expected_active.update(deepcopy(desired_by_identity))
    for retirement in retirements:
        expected_active.pop((retirement["kind"], retirement["key"]), None)
    if active != expected_active:
        raise LinearProjectionError("projection_readback_not_exact")
    return {
        "workstream_id": adapter.workstream_id,
        "plan_revision": adapter.plan_revision,
        "projection_revision": final.revision,
        "writes": receipts,
        "disposition": disposition,
        "readback_verified": True,
        "projection_contract": projection_review_contract(final),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("token")
    parser.add_argument("manifest", help="reviewed projection JSON path")
    parser.add_argument("--remote-head", required=True)
    parser.add_argument("--plan-source", required=True)
    parser.add_argument("--plan-identity")
    parser.add_argument(
        "--max-bytes", type=int, default=16 * 1024,
        help="maximum encoded full-resume context accepted after projection",
    )
    parser.add_argument(
        "--max-items", type=int, default=100,
        help="maximum full-resume item count accepted after projection",
    )
    parser.add_argument("--config")
    parser.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    args = parser.parse_args()
    try:
        token = extract_token(args.token)
        manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
        authenticated_source = plan_payload(args.plan_source, args.plan_identity)["source"]
        plan_revision = authenticated_source["sha256"]
        api_key = load_linear_api_key()
        if not api_key:
            raise LinearProjectionError("linear_auth_unavailable")
        client = HttpGraphQLClient(api_key, args.linear_endpoint)
        route, _ = resolve_linear_route(config_path=args.config)
        route = resolve_authenticated_issue_route(client, token, route)
        transport = LinearGraphQLTransport(
            client, team_id=route["team_id"], workspace_id=route["workspace_id"],
            project_id=route["project_id"],
        )
        graph = transport.snapshot_for_root(token, include_description=True)
        if graph["root"].get("plan_revision") != plan_revision:
            raise LinearProjectionError("root_plan_revision_source_bytes_mismatch")
        comment_adapter = LinearCommentEventAdapter(
            client, issue_id=token, workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
        )
        adapter = LinearProjectionAdapter(
            client, issue_id=token, workstream_id=token,
            plan_revision=plan_revision, workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
            root_issue_id=route["root_issue_id"],
        )
        projection_state = adapter.state()
        manifest, authenticated_source = synchronize_manifest_source(
            manifest, graph["root"].get("description"), authenticated_source,
            projection_state.snapshot.get("source"),
            projection_state.snapshot.get("projection_history"),
        )
        manifest = prepare_terminal_child_repairs(
            manifest, graph, projection_state,
        )
        graph = deepcopy(graph)
        graph["root"].pop("description", None)
        comments = comment_adapter.comments()
        resolver = lambda relations: read_relation_targets(client, relations)
        snapshot, legacy_unresolved_relation_heads = (
            load_material_history_for_projection_reconcile(
                graph, comments, token, manifest, adapter,
                authenticated_route=route,
                authenticated_source=authenticated_source,
                remote_head=args.remote_head,
                max_bytes=args.max_bytes, max_items=args.max_items,
                relation_target_resolver=resolver,
            )
        )

        def terminal_child_fence(child_identifier: str) -> str:
            """Re-read the exact terminal issue state at each mutation fence."""
            live = transport.snapshot_for_root(token)
            matches = [
                child for child in live.get("children", [])
                if child.get("identifier") == child_identifier
            ]
            if len(matches) != 1:
                raise LinearProjectionError(
                    "terminal_child_readback_ambiguous_reload_required:"
                    f"{child_identifier}"
                )
            try:
                return canonical_digest(terminal_child_readback(matches[0]))
            except ChildClosureError as error:
                raise LinearProjectionError(str(error)) from error

        result = reconcile_required_projection(
            adapter, snapshot, manifest, remote_head=args.remote_head,
            created_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            authenticated_source=authenticated_source,
            relation_target_resolver=resolver,
            terminal_child_fence=terminal_child_fence,
            legacy_unresolved_relation_heads=legacy_unresolved_relation_heads,
        )
        # Double-collect graph and comments so a concurrent root/child/checkpoint
        # mutation cannot be certified from a mixed pre/post-write snapshot.
        final_comments = LinearCommentEventAdapter(
            client, issue_id=token, workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
        )
        graph_after, comments_after = stable_live_readback(
            transport, final_comments, token, include_description=True,
        )
        validate_canonical_source_readback(
            graph_after["root"].get("description"), authenticated_source,
        )
        graph_after = deepcopy(graph_after)
        graph_after["root"].pop("description", None)
        verified = add_material_history(
            graph_after, comments_after, token, authenticated_route=route,
            authenticated_source=authenticated_source,
            relation_target_resolver=lambda relations: read_relation_targets(
                client, relations,
            ),
        )
        context = compact_context(
            verified, token, max_bytes=args.max_bytes, max_items=args.max_items,
            require_projection_authority=True,
        )
        choose_disposition(context, remote_head=args.remote_head)
        result["source_sync"] = {
            "identity": authenticated_source["identity"],
            "sha256": authenticated_source["sha256"],
            "resume_authority": context["resume_authority"],
        }
        json.dump(result, sys.stdout, sort_keys=True, indent=2)
        sys.stdout.write("\n")
        return 0
    except (OSError, json.JSONDecodeError, LinearProjectionError, LinearTransportError, ResumeError,
            SuccessorError, ValueError) as error:
        print(f"workstream projection refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
