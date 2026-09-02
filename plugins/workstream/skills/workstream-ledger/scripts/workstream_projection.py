#!/usr/bin/env python3
"""Idempotently reconcile the required append-only Linear resume projection."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sys
from types import SimpleNamespace
from typing import Any

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_linear import (
    HttpGraphQLClient, LinearGraphQLTransport, LinearTransportError,
    resolve_authenticated_issue_route,
)
from workstream_linear_events import (
    LinearCommentEventAdapter, LinearEventError, reduce_event_comments,
)
from workstream_linear_checkpoints import (
    LinearCheckpointError, reduce_checkpoint_comments,
)
from workstream_linear_projection import (
    bind_active_plan_generation, build_projection_event, encode_projection_comment,
    LinearProjectionAdapter,
    LinearProjectionError, projection_slot_id, reduce_projection_comments,
    select_plan_generation, TOMBSTONE,
)
from workstream_plan import canonical_plan_url, plan_payload, same_plan_document
from workstream_relation_readback import RelationReadbackError, read_relation_targets
from workstream_resume import (
    DEFAULT_RESUME_MAX_BYTES, ResumeError, add_live_child_material_history,
    add_material_history, compact_context, extract_token,
)
from workstream_scope import repository_key, ScopeError, validate_relation_graph
from workstream_successor import choose_disposition, SuccessorError
from workstream_checkpoint import (
    CheckpointError, recover_generations, recover_latest,
)
from workstream_evidence import evidence_errors
from workstream_child_closure import (
    canonical_digest, evidence_receipts_sha256, terminal_child_readback,
    CHILD_READBACK_FIELDS, ChildClosureError,
)
from workstream_child_dependencies import (
    ChildDependencyError, LinearChildDependencyAdapter,
    rebind_authenticated_dependency_graph,
)
from workstream_projection_history import (
    carried_predecessor_evidence_authority,
    closure_bound_historical_evidence, ProjectionHistoryError,
    validated_nonprimary_backfill_authority,
)
from workstream_github_backfill import (
    GitHubBackfillReceiptError, GitHubBackfillReceiptReader,
    github_token_from_command,
)


REQUIRED_KINDS = {"scope", "source", "provenance"}
REVIEW_CONTRACT_FIELDS = (
    "expected_projection_revision", "expected_active_heads",
    "expected_legacy_v1_event_ids", "expected_legacy_v1_events_sha256",
    "expected_projection_quarantine_count",
    "expected_projection_quarantine_sha256",
)
PREDECESSOR_SEED_BINDING_FIELDS = {
    "schema_version", "plan_revision", "projection_revision",
    "projection_events_sha256", "projection_frontier_event_id",
    "projection_frontier_sha256", "projection_history_sha256",
    "material_revision", "material_events_sha256", "checkpoint_event_id",
    "checkpoint_events_sha256", "input_frontier_sha256", "evidence_heads",
}
GEN14_SPLIT_PREFIX_SHA256 = (
    "180e178d1732b914edce564ba1d6411e229ceaaecd48cbcb5422de2736d56c28"
)
GEN14_SPLIT_STORED_FRONTIER_SHA256 = (
    "e0317b7cda88262a7baf0df28b13c4c27af8a4e171156147f304f62e104cfc23"
)
GEN14_SPLIT_RECOMPUTED_FRONTIER_SHA256 = (
    "7c53170bb0ba8434182809f23f40f893b279f3b88a14face18326fd737b41240"
)


def bind_projection_plan_generation(
    graph: dict[str, Any], comments: list[dict[str, Any]], *,
    workstream_id: str, requested_plan_revision: str,
    authenticated_route: dict[str, str],
) -> dict[str, Any]:
    """Bind projection work to an active or new candidate, never description authority."""
    result = deepcopy(graph)
    root = result.get("root") or {}
    description_revision = root.get("plan_revision")
    binding = projection_generation_source_binding(
        comments, workstream_id=workstream_id,
        description_plan_revision=description_revision,
        requested_plan_revision=requested_plan_revision,
        authenticated_route=authenticated_route,
    )
    selected = binding["selected"]
    result["root"] = dict(root)
    result["root"]["description_plan_revision"] = description_revision
    result["root"]["plan_revision"] = requested_plan_revision
    if selected is not None:
        if requested_plan_revision == selected["plan_revision"]:
            result = bind_active_plan_generation(
                graph, comments, workstream_id=workstream_id,
                selected=selected, authenticated_route=authenticated_route,
            )
        else:
            result["root"]["generation_transition_tip_event_id"] = selected[
                "transition_tip_event_id"
            ]
            result["root"]["generation_activation_epoch"] = selected[
                "activation_epoch"
            ]
            result["root"]["generation_authority_origin"] = selected[
                "authority_origin"
            ]
    return result


def projection_generation_source_binding(
    comments: list[dict[str, Any]], *, workstream_id: str,
    description_plan_revision: str | None, requested_plan_revision: str,
    authenticated_route: dict[str, str],
) -> dict[str, Any]:
    """Classify source authority without treating description prose as generation authority."""
    try:
        selected = select_plan_generation(
            comments, workstream_id=workstream_id,
            description_plan_revision=description_plan_revision,
            authenticated_route=authenticated_route,
        )
    except LinearProjectionError as error:
        if str(error) != "generation_description_plan_missing_bootstrap_required":
            raise
        selected = None
    from workstream_generation import generation_controls
    controls = generation_controls(comments)
    controlled_plans = {
        frontier["plan_revision"] for event in controls
        for frontier in (event["value"]["from"], event["value"]["to"])
    }
    if (
        selected is not None
        and requested_plan_revision != selected["plan_revision"]
        and requested_plan_revision in controlled_plans
    ):
        raise LinearProjectionError(
            f"generation_projection_plan_retired:{requested_plan_revision}"
        )
    if selected is None or requested_plan_revision != selected["plan_revision"]:
        mode = "inactive_candidate"
    elif controls:
        mode = "structured_active"
    else:
        mode = "legacy_active"
    return {
        "mode": mode, "selected": selected,
        "requested_plan_revision": requested_plan_revision,
        "controlled_plan_revisions": sorted(controlled_plans),
    }


def _value_digest(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _latest_acknowledged_checkpoint_id(snapshot: dict[str, Any]) -> str | None:
    """Return the checkpoint authority that a disposition must name."""
    checkpoint = snapshot.get("latest_checkpoint")
    if checkpoint is None:
        return None
    acknowledgement = checkpoint.get("acknowledgement")
    checkpoint_id = checkpoint.get("checkpoint_event_id")
    if (
        not isinstance(checkpoint_id, str)
        or not checkpoint_id
        or not isinstance(acknowledgement, dict)
        or acknowledgement.get("state") != "remote_acknowledged"
        or not isinstance(acknowledgement.get("remote_id"), str)
        or not acknowledgement["remote_id"]
    ):
        raise LinearProjectionError(
            "latest_checkpoint_not_remote_acknowledged"
        )
    return checkpoint_id


def projection_disposition_value(
    snapshot: dict[str, Any], desired: list[dict[str, Any]], *,
    remote_head: str, workstream_id: str | None = None,
) -> dict[str, Any]:
    """Derive the one disposition written with a reviewed projection."""
    disposition_input = deepcopy(snapshot)
    disposition_input.pop("disposition", None)
    if workstream_id is not None:
        root = deepcopy(disposition_input.get("root") or {})
        root.setdefault("identifier", workstream_id)
        disposition_input["root"] = root
    # The reviewed provenance being persisted is part of this same durable
    # operation, so disposition must be derived from it on first creation too.
    disposition_input["provenance"] = [
        deepcopy(item["value"])
        for item in desired if item["kind"] == "provenance"
    ]
    decision = choose_disposition(disposition_input, remote_head=remote_head)
    return {
        "disposition": decision["disposition"],
        "remote_head": remote_head,
        "recovered_from_checkpoint": decision.get("recovered_from_checkpoint"),
    }


def latest_acknowledged_checkpoint_id_from_comments(
    comments: list[dict[str, Any]], *, workstream_id: str,
    plan_revision: str, authenticated_route: dict[str, str],
) -> str | None:
    """Recover the selected generation's acknowledged checkpoint chain tip."""
    from workstream_generation import (
        generation_controls, selected_activation_checkpoints,
    )

    selected_checkpoints = None
    if generation_controls(comments):
        selected = select_plan_generation(
            comments, workstream_id=workstream_id,
            description_plan_revision=None,
            authenticated_route=authenticated_route,
        )
        if selected["plan_revision"] == plan_revision:
            selected_checkpoints = selected_activation_checkpoints(
                comments, workstream_id=workstream_id,
                transition_event_id=selected["transition_tip_event_id"],
                active_plan_revision=selected["plan_revision"],
                authenticated_route=authenticated_route,
            )
    checkpoint_log = reduce_checkpoint_comments(
        comments, workstream_id=workstream_id,
        selected_activation_checkpoints=selected_checkpoints,
    )
    matching = [
        checkpoint for checkpoint in checkpoint_log.checkpoints
        if checkpoint.get("plan_revision") == plan_revision
    ]
    if not matching:
        return None
    try:
        return recover_latest(
            list(checkpoint_log.checkpoints), workstream_id,
            expected_plan_revision=plan_revision,
        )["checkpoint_event_id"]
    except CheckpointError as error:
        raise LinearProjectionError(str(error)) from error


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
    *, generation_binding: dict[str, Any] | None = None,
    expected_projection_contract: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Bind one labeled issue plan to the desired structured source."""
    canonical = canonical_plan_url(description)
    supplied_identity = authenticated_source.get("identity")
    source_mode = (
        generation_binding.get("mode") if generation_binding is not None
        else "legacy_active"
    )
    if source_mode not in {
        "legacy_active", "inactive_candidate", "structured_active",
    }:
        raise LinearProjectionError("invalid_generation_source_binding")
    if (
        source_mode == "legacy_active"
        and isinstance(supplied_identity, str)
        and supplied_identity.startswith(("http://", "https://"))
        and canonical != supplied_identity
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
    if source_mode == "inactive_candidate":
        if (
            not isinstance(supplied_identity, str)
            or not same_plan_document(canonical, supplied_identity)
        ):
            raise LinearProjectionError(
                "generation_candidate_source_document_mismatch"
            )
        if len(source_items) != 1:
            raise LinearProjectionError(
                "generation_candidate_source_explicit_review_required"
            )
        if expected_projection_contract is None or any(
            manifest.get(field) != expected_projection_contract.get(field)
            for field in REVIEW_CONTRACT_FIELDS
        ):
            raise LinearProjectionError(
                "generation_candidate_projection_contract_mismatch"
            )
        if source_items[0].get("value") != {
            "identity": supplied_identity,
            "sha256": authenticated_source.get("sha256"),
        }:
            raise LinearProjectionError(
                "generation_candidate_source_review_mismatch"
            )
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
    if source_mode == "structured_active":
        if live_source != {
            "identity": supplied_identity,
            "sha256": authenticated_source.get("sha256"),
        }:
            raise LinearProjectionError("active_projection_source_mismatch")
    source_identity = (
        supplied_identity
        if source_mode in {"inactive_candidate", "structured_active"}
        else canonical
    )
    source = {
        "identity": source_identity,
        "sha256": authenticated_source.get("sha256"),
    }
    item = {"kind": "source", "key": "root", "value": source}
    if source_items:
        projection[projection.index(source_items[0])] = item
    else:
        projection.append(item)
    return result, {**authenticated_source, "identity": source_identity}


def canonical_source_diagnostic_fence(
    description: str | None,
) -> dict[str, str]:
    """Bind the immutable labeled value and the complete diagnostic prose."""
    value = description if isinstance(description, str) else ""
    return {
        "canonical_identity": canonical_plan_url(description),
        "description_sha256": hashlib.sha256(value.encode("utf-8")).hexdigest(),
    }


def validate_canonical_source_readback(
    description: str | None, expected: dict[str, Any],
) -> None:
    """Refuse success if the issue's canonical source changed during writes."""
    if "description_sha256" in expected:
        observed = canonical_source_diagnostic_fence(description)
        matches = observed == expected
    else:
        matches = canonical_plan_url(description) == expected.get("identity")
    if not matches:
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


def projection_input_frontier_sha256(
    snapshot: dict[str, Any], comments: list[dict[str, Any]],
) -> str:
    """Bind a reviewed transition to its issue/material/checkpoint frontier."""
    graph = deepcopy(snapshot)
    # Native dependency authority is independently authenticated and rebound
    # to this projection frontier.  It must not retroactively change legacy
    # projection-input bindings created before the graph surface existed.
    graph.pop("dependency_graph", None)
    root = graph.get("root")
    if isinstance(root, dict):
        root.pop("description", None)
        root.pop("updatedAt", None)
    for child in graph.get("children", []):
        if isinstance(child, dict):
            child.pop("description", None)
            child.pop("updatedAt", None)
    material_comments = sorted(
        [
            {"id": comment.get("id"), "body": comment.get("body")}
            for comment in comments
            if isinstance(comment, dict)
            and isinstance(comment.get("body"), str)
            and (
                "<!-- workstream-delta:v1:" in comment["body"]
                or "<!-- workstream-checkpoint:v1:" in comment["body"]
            )
        ],
        key=lambda item: (str(item.get("id", "")), str(item.get("body", ""))),
    )
    return canonical_digest({"graph": graph, "material_comments": material_comments})


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


def _exact_empty_review_contract(contract: dict[str, Any]) -> bool:
    """Recognize only the authenticated, never-written generation frontier."""
    return contract == _contract_from_heads(
        0, {}, legacy_event_ids=[], legacy_events_sha256=None,
        quarantine_count=0, quarantine_sha256=_value_digest([]),
    )


def _terminal_seed_bootstrap_prefix(
    contract: dict[str, Any], state: Any, desired: list[dict[str, Any]], *,
    remote_head: str | None,
) -> bool:
    """Recognize only a canonical prefix that began at the empty frontier."""
    if not state.events or projection_review_contract(state) != contract:
        return False
    evidence = [
        (item["kind"], item["key"]) for item in desired
        if item["kind"] == "evidence_contract"
    ]
    ordinary = [
        (item["kind"], item["key"]) for item in desired
        if item["kind"] not in {"evidence_contract", "scope"}
    ]
    expected = [
        *evidence, *ordinary, ("disposition", "root"), ("scope", "root"),
    ]
    observed = [(event["kind"], event["key"]) for event in state.events]
    if observed != expected[:len(observed)] or ("scope", "root") in observed:
        return False
    desired_values = {
        (item["kind"], item["key"]): item["value"] for item in desired
    }
    for event, identity in zip(state.events, observed):
        if identity == ("disposition", "root"):
            if (
                not isinstance(event["value"], dict)
                or event["value"].get("remote_head") != remote_head
                or event["value"].get("disposition")
                not in {"attach", "create_successor"}
                or set(event["value"]) != {
                    "disposition", "remote_head", "recovered_from_checkpoint",
                }
            ):
                return False
        elif event["value"] != desired_values.get(identity):
            return False
    return True


def _reviewed_manifest(manifest: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    required = {
        "projection", "retirements", "expected_projection_revision",
        "expected_active_heads", "expected_legacy_v1_event_ids",
        "expected_legacy_v1_events_sha256", "expected_projection_quarantine_count",
        "expected_projection_quarantine_sha256",
    }
    repairs_allowed = required | {"terminal_child_repairs"}
    gen14_bridge_repairs_allowed = repairs_allowed | {
        "terminal_child_repair_gen14_frontier_bridge"
    }
    seeds_allowed = required | {"terminal_child_evidence_seeds"}
    predecessor_seeds_allowed = seeds_allowed | {
        "terminal_child_evidence_seed_predecessor"
    }
    seed_transition_allowed = seeds_allowed | {
        "terminal_child_evidence_seed_head_transition"
    }
    predecessor_seed_transition_allowed = predecessor_seeds_allowed | {
        "terminal_child_evidence_seed_head_transition"
    }
    legacy_split_seed_allowed = predecessor_seeds_allowed | {
        "terminal_child_evidence_seed_legacy_split_head_repair"
    }
    nonprimary_backfill_allowed = seeds_allowed | {
        "terminal_child_evidence_seed_nonprimary_backfill"
    }
    source_transition_allowed = required | {"terminal_child_source_transition"}
    if not isinstance(manifest, dict) or frozenset(manifest) not in {
        frozenset(required), frozenset(repairs_allowed),
        frozenset(gen14_bridge_repairs_allowed),
        frozenset(seeds_allowed), frozenset(predecessor_seeds_allowed),
        frozenset(seed_transition_allowed),
        frozenset(predecessor_seed_transition_allowed),
        frozenset(legacy_split_seed_allowed),
        frozenset(nonprimary_backfill_allowed),
        frozenset(source_transition_allowed),
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
        if identity[0] == "child_closure":
            raise LinearProjectionError(
                f"terminal_child_closure_retirement_forbidden:{identity[1]}"
            )
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
    if not isinstance(repairs, list) or len(repairs) > 100:
        raise LinearProjectionError("invalid_manifest_terminal_child_repairs")
    repair_children: set[str] = set()
    repair_issue_ids: set[str] = set()
    repair_evidence_events: set[str] = set()
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
            or not isinstance(repair.get("child_issue_id"), str)
            or not repair["child_issue_id"]
            or not (
                repair.get("expected_assignee_id") is None
                or (
                    isinstance(repair.get("expected_assignee_id"), str)
                    and bool(repair["expected_assignee_id"])
                )
            )
            or not re.fullmatch(r"[0-9a-f]{64}", str(repair.get("expected_child_readback_sha256", "")))
            or not valid_heads
            or heads != sorted(heads, key=lambda item: (item.get("key", ""), item.get("event_id", "")))
        ):
            raise LinearProjectionError(f"invalid_manifest_terminal_child_repair:{index}")
        child_identifier = repair["child_identifier"].upper()
        if (
            child_identifier in repair_children
            or repair["child_issue_id"] in repair_issue_ids
        ):
            raise LinearProjectionError("duplicate_manifest_terminal_child_repair")
        repair_children.add(child_identifier)
        repair_issue_ids.add(repair["child_issue_id"])
        evidence_events = {head["event_id"] for head in heads}
        if repair_evidence_events & evidence_events:
            raise LinearProjectionError(
                "overlapping_manifest_terminal_child_evidence"
            )
        repair_evidence_events.update(evidence_events)
    if repairs != sorted(repairs, key=lambda item: item["child_identifier"].upper()):
        raise LinearProjectionError("terminal_child_repairs_not_canonical")
    seeds = manifest.get("terminal_child_evidence_seeds", [])
    if not isinstance(seeds, list) or len(seeds) > 100:
        raise LinearProjectionError("invalid_manifest_terminal_child_evidence_seeds")
    if repairs and seeds:
        raise LinearProjectionError("terminal_child_seed_and_repair_are_separate_phases")
    seed_children: set[str] = set()
    seed_issues: set[str] = set()
    seed_evidence_keys: set[str] = set()
    for index, seed in enumerate(seeds):
        if not isinstance(seed, dict) or set(seed) != {
            "child_identifier", "child_issue_id",
            "expected_child_readback_sha256", "expected_assignee_id",
            "evidence_keys",
        }:
            raise LinearProjectionError(
                f"invalid_manifest_terminal_child_evidence_seed:{index}"
            )
        child_id = str(seed.get("child_identifier", "")).upper()
        keys = seed.get("evidence_keys")
        if (
            not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", child_id)
            or not isinstance(seed.get("child_issue_id"), str)
            or not seed["child_issue_id"]
            or not (
                seed.get("expected_assignee_id") is None
                or (
                    isinstance(seed.get("expected_assignee_id"), str)
                    and bool(seed["expected_assignee_id"])
                )
            )
            or not re.fullmatch(
                r"[0-9a-f]{64}",
                str(seed.get("expected_child_readback_sha256", "")),
            )
            or not isinstance(keys, list) or not keys
            or not all(isinstance(key, str) and key for key in keys)
            or keys != sorted(set(keys))
            or child_id in seed_children
            or seed["child_issue_id"] in seed_issues
            or seed_evidence_keys.intersection(keys)
        ):
            raise LinearProjectionError(
                f"invalid_manifest_terminal_child_evidence_seed:{index}"
            )
        seed_children.add(child_id)
        seed_issues.add(seed["child_issue_id"])
        seed_evidence_keys.update(keys)
    if seeds != sorted(seeds, key=lambda item: item["child_identifier"].upper()):
        raise LinearProjectionError("terminal_child_evidence_seeds_not_canonical")
    predecessor = manifest.get("terminal_child_evidence_seed_predecessor")
    if predecessor is not None:
        evidence_heads = predecessor.get("evidence_heads") if isinstance(
            predecessor, dict
        ) else None
        valid_heads = (
            isinstance(evidence_heads, list)
            and bool(evidence_heads)
            and evidence_heads == sorted(
                evidence_heads,
                key=lambda item: (
                    str(item.get("child_identifier", "")),
                    str(item.get("key", "")),
                ),
            )
            and all(
                isinstance(item, dict)
                and set(item) == {
                    "child_identifier", "key", "evidence_event_id",
                    "evidence_value_sha256", "closure_event_id",
                    "closure_value_sha256",
                }
                and re.fullmatch(
                    r"[A-Z][A-Z0-9]*-\d+",
                    str(item.get("child_identifier", "")),
                )
                and all(
                    isinstance(item.get(field), str) and item[field]
                    for field in ("key", "evidence_event_id", "closure_event_id")
                )
                and all(
                    re.fullmatch(r"[0-9a-f]{64}", str(item.get(field, "")))
                    for field in (
                        "evidence_value_sha256", "closure_value_sha256",
                    )
                )
                for item in evidence_heads
            )
        )
        if (
            not seeds
            or not isinstance(predecessor, dict)
            or set(predecessor) != PREDECESSOR_SEED_BINDING_FIELDS
            or predecessor.get("schema_version") != 1
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(predecessor.get("plan_revision", "")),
            )
            or not isinstance(predecessor.get("projection_revision"), int)
            or isinstance(predecessor.get("projection_revision"), bool)
            or predecessor["projection_revision"] <= 0
            or not all(
                re.fullmatch(r"[0-9a-f]{64}", str(predecessor.get(field, "")))
                for field in (
                    "projection_events_sha256", "projection_frontier_sha256",
                    "projection_history_sha256", "material_events_sha256",
                    "checkpoint_events_sha256", "input_frontier_sha256",
                )
            )
            or not isinstance(predecessor.get("projection_frontier_event_id"), str)
            or not predecessor["projection_frontier_event_id"]
            or not isinstance(predecessor.get("material_revision"), int)
            or isinstance(predecessor.get("material_revision"), bool)
            or predecessor["material_revision"] < 0
            or not (
                predecessor.get("checkpoint_event_id") is None
                or isinstance(predecessor.get("checkpoint_event_id"), str)
                and bool(predecessor["checkpoint_event_id"])
            )
            or not valid_heads
        ):
            raise LinearProjectionError(
                "invalid_terminal_child_evidence_seed_predecessor"
            )
    seed_head_transition = manifest.get(
        "terminal_child_evidence_seed_head_transition"
    )
    nonprimary_backfill = manifest.get(
        "terminal_child_evidence_seed_nonprimary_backfill"
    )
    legacy_split = manifest.get(
        "terminal_child_evidence_seed_legacy_split_head_repair"
    )
    if seed_head_transition is not None and legacy_split is not None:
        raise LinearProjectionError(
            "terminal_child_evidence_seed_head_transition_ambiguous"
        )
    if nonprimary_backfill is not None:
        backfill_fields = {
            "repository_key", "from_exact_head", "to_exact_head",
            "from_scope_event_id", "from_scope_value_sha256",
            "from_disposition_event_id", "from_disposition_value_sha256",
            "input_frontier_sha256", "provider_repository_id",
            "pull_request_number", "merge_sha", "checks_sha256",
            "provider_receipt_sha256",
        }
        if (
            not seeds
            or seed_head_transition is not None
            or legacy_split is not None
            or not isinstance(nonprimary_backfill, dict)
            or set(nonprimary_backfill) != backfill_fields
            or not isinstance(nonprimary_backfill.get("repository_key"), str)
            or not nonprimary_backfill["repository_key"]
            or any(
                not re.fullmatch(
                    r"[0-9a-f]{40}(?:[0-9a-f]{24})?",
                    str(nonprimary_backfill.get(field, "")),
                )
                for field in ("from_exact_head", "to_exact_head")
            )
            or not re.fullmatch(
                r"[0-9a-f]{40}", str(nonprimary_backfill.get("merge_sha", ""))
            )
            or nonprimary_backfill["from_exact_head"]
            == nonprimary_backfill["to_exact_head"]
            or any(
                not isinstance(nonprimary_backfill.get(field), str)
                or not nonprimary_backfill[field]
                for field in (
                    "from_scope_event_id", "from_disposition_event_id",
                    "provider_repository_id",
                )
            )
            or any(
                not re.fullmatch(r"[0-9a-f]{64}", str(nonprimary_backfill.get(field, "")))
                for field in (
                    "from_scope_value_sha256", "from_disposition_value_sha256",
                    "input_frontier_sha256", "checks_sha256",
                    "provider_receipt_sha256",
                )
            )
            or not isinstance(nonprimary_backfill.get("pull_request_number"), int)
            or isinstance(nonprimary_backfill.get("pull_request_number"), bool)
            or nonprimary_backfill["pull_request_number"] <= 0
        ):
            raise LinearProjectionError(
                "invalid_terminal_child_evidence_seed_nonprimary_backfill"
            )
    if seed_head_transition is not None:
        disposition = seed_head_transition.get("disposition") if isinstance(
            seed_head_transition, dict
        ) else None
        head_transition_fields = {
            "repository_key", "from_exact_head", "to_exact_head",
            "from_scope_event_id", "from_scope_value_sha256",
            "from_disposition_event_id", "from_disposition_value_sha256",
            "disposition", "input_frontier_sha256",
        }
        if (
            not seeds
            or not isinstance(seed_head_transition, dict)
            or set(seed_head_transition) not in (
                head_transition_fields,
                head_transition_fields | {"created_at"},
            )
            or not isinstance(seed_head_transition.get("repository_key"), str)
            or not seed_head_transition["repository_key"]
            or not re.fullmatch(
                r"[0-9a-f]{40}(?:[0-9a-f]{24})?",
                str(seed_head_transition.get("from_exact_head", "")),
            )
            or not re.fullmatch(
                r"[0-9a-f]{40}(?:[0-9a-f]{24})?",
                str(seed_head_transition.get("to_exact_head", "")),
            )
            or seed_head_transition["from_exact_head"]
            == seed_head_transition["to_exact_head"]
            or not isinstance(seed_head_transition.get("from_scope_event_id"), str)
            or not seed_head_transition["from_scope_event_id"]
            or not re.fullmatch(
                r"[0-9a-f]{64}",
                str(seed_head_transition.get("from_scope_value_sha256", "")),
            )
            or not isinstance(
                seed_head_transition.get("from_disposition_event_id"), str,
            )
            or not seed_head_transition["from_disposition_event_id"]
            or not re.fullmatch(
                r"[0-9a-f]{64}",
                str(seed_head_transition.get(
                    "from_disposition_value_sha256", "",
                )),
            )
            or not re.fullmatch(
                r"[0-9a-f]{64}",
                str(seed_head_transition.get("input_frontier_sha256", "")),
            )
            or (
                "created_at" in seed_head_transition
                and (
                    not isinstance(seed_head_transition["created_at"], str)
                    or not seed_head_transition["created_at"]
                )
            )
            or not isinstance(disposition, dict)
            or set(disposition) != {
                "disposition", "remote_head", "recovered_from_checkpoint",
            }
            or disposition.get("remote_head")
            != seed_head_transition["to_exact_head"]
        ):
            raise LinearProjectionError(
                "invalid_terminal_child_evidence_seed_head_transition"
            )
    if legacy_split is not None:
        disposition = legacy_split.get("disposition") if isinstance(
            legacy_split, dict
        ) else None
        if (
            not seeds
            or predecessor is None
            or not isinstance(legacy_split, dict)
            or set(legacy_split) != {
                "repository_key", "from_exact_head", "to_exact_head",
                "from_scope_event_id", "from_scope_value_sha256",
                "from_disposition_event_id",
                "from_disposition_value_sha256",
                "from_disposition_exact_head", "disposition",
                "input_frontier_sha256", "created_at",
            }
            or not isinstance(legacy_split.get("repository_key"), str)
            or not legacy_split["repository_key"]
            or any(
                not re.fullmatch(
                    r"[0-9a-f]{40}(?:[0-9a-f]{24})?",
                    str(legacy_split.get(field, "")),
                )
                for field in (
                    "from_exact_head", "from_disposition_exact_head",
                    "to_exact_head",
                )
            )
            or len({
                legacy_split["from_exact_head"],
                legacy_split["from_disposition_exact_head"],
                legacy_split["to_exact_head"],
            }) != 3
            or any(
                not isinstance(legacy_split.get(field), str)
                or not legacy_split[field]
                for field in (
                    "from_scope_event_id", "from_disposition_event_id",
                )
            )
            or any(
                not re.fullmatch(
                    r"[0-9a-f]{64}", str(legacy_split.get(field, "")),
                )
                for field in (
                    "from_scope_value_sha256",
                    "from_disposition_value_sha256",
                    "input_frontier_sha256",
                )
            )
            or disposition != {
                "disposition": "create_successor",
                "remote_head": legacy_split.get("to_exact_head"),
                "recovered_from_checkpoint": None,
            }
            or not isinstance(legacy_split.get("created_at"), str)
            or not legacy_split["created_at"]
        ):
            raise LinearProjectionError(
                "invalid_terminal_child_evidence_seed_legacy_split_head_repair"
            )
    if (
        predecessor is not None
        and (seed_head_transition is not None or legacy_split is not None)
        and predecessor["input_frontier_sha256"]
        != (seed_head_transition or legacy_split)["input_frontier_sha256"]
    ):
        raise LinearProjectionError(
            "terminal_child_evidence_seed_input_frontier_mismatch"
        )
    transition = manifest.get("terminal_child_source_transition")
    if transition is not None:
        if not isinstance(transition, dict) or set(transition) != {
            "from_identity", "to_identity", "sha256", "created_at",
            "expected_revision", "from_event_id", "from_value_sha256",
            "pending_children",
        }:
            raise LinearProjectionError("invalid_terminal_child_source_transition")
        pending = transition.get("pending_children")
        if (
            not all(isinstance(transition.get(field), str) and transition[field]
                    for field in (
                        "from_identity", "to_identity", "created_at",
                        "from_event_id",
                    ))
            or not re.fullmatch(r"[0-9a-f]{64}", str(transition.get("sha256", "")))
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(transition.get("from_value_sha256", "")),
            )
            or not isinstance(transition.get("expected_revision"), int)
            or transition["expected_revision"] < 0
            or not isinstance(pending, list) or not pending
        ):
            raise LinearProjectionError("invalid_terminal_child_source_transition")
        seen_children: set[str] = set()
        seen_issues: set[str] = set()
        for index, child in enumerate(pending):
            child_id = str(child.get("child_identifier", "")).upper() if isinstance(child, dict) else ""
            if (
                not isinstance(child, dict)
                or set(child) != {"child_identifier", "child_issue_id",
                                  "expected_child_readback_sha256", "expected_assignee_id"}
                or not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", child_id)
                or not isinstance(child.get("child_issue_id"), str)
                or not child["child_issue_id"]
                or child.get("expected_assignee_id") is not None
                and (not isinstance(child.get("expected_assignee_id"), str)
                     or not child["expected_assignee_id"])
                or not re.fullmatch(r"[0-9a-f]{64}", str(child.get("expected_child_readback_sha256", "")))
                or child_id in seen_children
                or child["child_issue_id"] in seen_issues
            ):
                raise LinearProjectionError(
                    f"invalid_terminal_child_source_transition_child:{index}"
                )
            seen_children.add(child_id)
            seen_issues.add(child["child_issue_id"])
        if pending != sorted(pending, key=lambda child: child["child_identifier"].upper()):
            raise LinearProjectionError("terminal_child_source_transition_not_canonical")
    bridge = manifest.get("terminal_child_repair_gen14_frontier_bridge")
    if bridge is not None:
        if (
            not isinstance(bridge, dict)
            or set(bridge) != {
                "prefix_sha256", "stored_input_frontier_sha256",
                "recomputed_input_frontier_sha256", "source_event_id",
                "source_value_sha256", "created_at", "child_identifiers",
            }
            or bridge["prefix_sha256"] != GEN14_SPLIT_PREFIX_SHA256
            or bridge["stored_input_frontier_sha256"]
            != GEN14_SPLIT_STORED_FRONTIER_SHA256
            or bridge["recomputed_input_frontier_sha256"]
            != GEN14_SPLIT_RECOMPUTED_FRONTIER_SHA256
            or not isinstance(bridge["source_event_id"], str)
            or not bridge["source_event_id"]
            or not re.fullmatch(
                r"[0-9a-f]{64}", str(bridge["source_value_sha256"]),
            )
            or not isinstance(bridge["created_at"], str)
            or not bridge["created_at"]
            or not isinstance(bridge["child_identifiers"], list)
            or not 1 <= len(bridge["child_identifiers"]) <= 2
            or bridge["child_identifiers"] != sorted(
                bridge["child_identifiers"]
            )
            or len(set(bridge["child_identifiers"]))
            != len(bridge["child_identifiers"])
            or any(
                not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(child_id))
                for child_id in bridge["child_identifiers"]
            )
        ):
            raise LinearProjectionError(
                "invalid_terminal_child_repair_gen14_frontier_bridge"
            )
    return _desired_items(manifest), retirements


def _completed_owned_missing_closures(
    snapshot: dict[str, Any], scope_value: dict[str, Any],
    active: dict[tuple[str, str], dict[str, Any]],
) -> set[str]:
    return {
        str(child.get("identifier", "")).upper()
        for child in snapshot.get("children", [])
        if str(child.get("status_type", "")).lower() == "completed"
        and str(child.get("identifier", "")).upper()
        in scope_value.get("child_ownership", {})
        and (
            "child_closure", str(child.get("identifier", "")).upper(),
        ) not in active
    }


def _valid_reviewed_source_transition(first: str, second: str) -> bool:
    pattern = re.compile(
        r"https://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.+)"
    )
    left = pattern.fullmatch(first)
    right = pattern.fullmatch(second)
    return bool(
        left and right
        and same_plan_document(first, second)
        and (
            left.group(3) == "main"
            or re.fullmatch(r"[0-9a-f]{40}", left.group(3))
        )
        and re.fullmatch(r"[0-9a-f]{40}", right.group(3))
        and left.group(3) != right.group(3)
        and (left.group(1).lower(), left.group(2).lower(), left.group(4))
        == (right.group(1).lower(), right.group(2).lower(), right.group(4))
    )


def prepare_terminal_child_source_transition(
    manifest: dict[str, Any], snapshot: dict[str, Any], state: Any,
) -> dict[str, Any]:
    """Validate or replay one exact same-document source-only transition."""
    result = deepcopy(manifest)
    _reviewed_manifest(result)
    transition = result.get("terminal_child_source_transition")
    if transition is None:
        return result
    if result.get("retirements"):
        raise LinearProjectionError("terminal_child_source_transition_forbids_retirements")
    active = _active_heads(state)
    source_head = active.get(("source", "root"))
    scope_head = active.get(("scope", "root"))
    if source_head is None or scope_head is None:
        raise LinearProjectionError("terminal_child_source_transition_surface_missing")
    old_source = source_head["value"]
    expected_source = {
        "identity": transition["to_identity"], "sha256": transition["sha256"],
    }
    if (
        old_source not in ({"identity": transition["from_identity"],
                            "sha256": transition["sha256"]}, expected_source)
        or not _valid_reviewed_source_transition(
            transition["from_identity"], transition["to_identity"],
        )
    ):
        raise LinearProjectionError("terminal_child_source_transition_invalid_route")
    original_contract = {field: deepcopy(result[field])
                         for field in REVIEW_CONTRACT_FIELDS}
    predecessor = next((
        event for event in state.events
        if event["event_id"] == transition["from_event_id"]
    ), None)
    if (
        predecessor is None
        or (predecessor["kind"], predecessor["key"]) != ("source", "root")
        or predecessor["value"] != {
            "identity": transition["from_identity"],
            "sha256": transition["sha256"],
        }
        or _value_digest(predecessor["value"])
        != transition["from_value_sha256"]
    ):
        raise LinearProjectionError(
            "terminal_child_source_transition_predecessor_mismatch"
        )
    if predecessor.get("schema_version") != 2 or not isinstance(
        predecessor.get("authority"), dict,
    ):
        raise LinearProjectionError(
            "terminal_child_source_transition_requires_v2_source_predecessor"
        )
    expected_event = build_projection_event(
        workstream_id=predecessor["workstream_id"], kind="source", key="root",
        value=expected_source, plan_revision=transition["sha256"],
        expected_revision=transition["expected_revision"],
        created_at=transition["created_at"],
        supersedes_event_id=predecessor["event_id"],
        authority=predecessor["authority"],
    )
    reviewed_source = next((
        head for head in original_contract["expected_active_heads"]
        if (head["kind"], head["key"]) == ("source", "root")
    ), None)
    if old_source == expected_source:
        if source_head != expected_event:
            raise LinearProjectionError(
                "terminal_child_source_transition_replay_event_mismatch"
            )
    elif reviewed_source != {
        "kind": "source", "key": "root",
        "event_id": transition["from_event_id"],
        "value_sha256": transition["from_value_sha256"],
    }:
        raise LinearProjectionError(
            "terminal_child_source_transition_predecessor_mismatch"
        )
    elif original_contract["expected_projection_revision"] != transition[
        "expected_revision"
    ]:
        raise LinearProjectionError(
            "terminal_child_source_transition_predecessor_mismatch"
        )
    desired = result["projection"]
    source_item = next((item for item in desired if item["kind"] == "source"), None)
    if source_item is None or source_item["value"] != expected_source:
        raise LinearProjectionError("terminal_child_source_transition_source_mismatch")
    pending_ids = {child["child_identifier"].upper()
                   for child in transition["pending_children"]}
    actual_pending = _completed_owned_missing_closures(
        snapshot, scope_head["value"], active,
    )
    if actual_pending != pending_ids:
        raise LinearProjectionError(
            "terminal_child_source_transition_pending_set_mismatch:"
            + ",".join(sorted(actual_pending ^ pending_ids))
        )
    children = {str(child.get("identifier", "")).upper(): child
                for child in snapshot.get("children", [])}
    for expected in transition["pending_children"]:
        child_id = expected["child_identifier"].upper()
        child = children.get(child_id)
        if child is None:
            raise LinearProjectionError(
                f"terminal_child_source_transition_child_missing:{child_id}"
            )
        try:
            readback = terminal_child_readback(child)
        except ChildClosureError as error:
            raise LinearProjectionError(f"{child_id}:{error}") from error
        if (
            readback["child_issue_id"] != expected["child_issue_id"]
            or readback["assignee_id"] != expected["expected_assignee_id"]
            or canonical_digest(readback)
            != expected["expected_child_readback_sha256"]
        ):
            raise LinearProjectionError(
                f"terminal_child_readback_changed_reload_required:{child_id}"
            )
        linear = scope_head["value"]["linear"]
        if any(readback[field] != linear[field]
               for field in ("workspace_id", "team_id", "project_id")) \
                or readback["parent_issue_id"] != linear["root_issue_id"]:
            raise LinearProjectionError(
                f"terminal_child_source_transition_route_mismatch:{child_id}"
            )
    for item in desired:
        identity = (item["kind"], item["key"])
        if identity == ("source", "root"):
            continue
        current = active.get(identity)
        if current is None or current["value"] != item["value"]:
            raise LinearProjectionError(
                f"terminal_child_source_transition_unrelated_change:"
                f"{identity[0]}:{identity[1]}"
            )
    current_contract = projection_review_contract(state)
    if current_contract != original_contract:
        expected_revision = original_contract["expected_projection_revision"]
        progress = state.events[expected_revision:]
        if not (
            len(progress) == 1
            and progress[0] == expected_event
            and all(
                field in {"expected_projection_revision", "expected_active_heads"}
                or current_contract[field] == original_contract[field]
                for field in REVIEW_CONTRACT_FIELDS
            )
        ):
            raise LinearProjectionError("projection_review_stale_reload_required")
        result.update(current_contract)
    return result


def _with_validation_only_seed_closures(
    snapshot: dict[str, Any], seeds: list[dict[str, Any]],
    adapter: LinearProjectionAdapter,
) -> dict[str, Any]:
    """Close only the reviewed seed gap in-memory so all later gates run."""
    candidate = deepcopy(snapshot)
    events = candidate.get("projection_events")
    if not isinstance(events, list):
        raise LinearProjectionError("terminal_child_seed_projection_events_missing")
    active: dict[tuple[str, str], dict[str, Any]] = {}
    for event in events:
        identity = (event["kind"], event["key"])
        if event["value"] == TOMBSTONE:
            active.pop(identity, None)
        else:
            active[identity] = event
    scope_event = active.get(("scope", "root"))
    if scope_event is None:
        raise LinearProjectionError("terminal_child_evidence_seed_scope_missing")
    children = {
        str(child.get("identifier", "")).upper(): child
        for child in candidate.get("children", [])
    }
    closures = list(candidate.get("child_closures") or [])
    for seed in seeds:
        child_id = seed["child_identifier"].upper()
        child = children.get(child_id)
        if child is None:
            raise LinearProjectionError(
                f"terminal_child_evidence_seed_ambiguous_child:{child_id}"
            )
        try:
            readback = terminal_child_readback(child)
        except ChildClosureError as error:
            raise LinearProjectionError(f"{child_id}:{error}") from error
        contracts_with_events = sorted(
            [
                event for (kind, _key), event in active.items()
                if kind == "evidence_contract"
                and event["value"].get("owning_child") == child_id
            ],
            key=lambda event: (event["key"], event["event_id"]),
        )
        contracts = [event["value"] for event in contracts_with_events]
        owner = scope_event["value"]["child_ownership"][child_id]
        contract_heads = {
            (contract.get("repository_key"), contract.get("exact_head"))
            for contract in contracts
        }
        if len(contract_heads) != 1 or next(iter(contract_heads))[0] != owner:
            raise LinearProjectionError(
                f"terminal_child_evidence_seed_owner_ambiguous:{child_id}"
            )
        _contract_owner, contract_head = next(iter(contract_heads))
        closure = {
            "schema_version": 2,
            **readback,
            "plan_revision": adapter.plan_revision,
            "repository_key": owner,
            "exact_head": contract_head,
            "evidence_heads": [
                {
                    "key": event["key"], "event_id": event["event_id"],
                    "value_sha256": canonical_digest(event["value"]),
                }
                for event in contracts_with_events
            ],
            "evidence_receipts_sha256": evidence_receipts_sha256(contracts),
            "child_readback_sha256": canonical_digest(readback),
        }
        event = build_projection_event(
            workstream_id=adapter.workstream_id,
            kind="child_closure", key=child_id, value=closure,
            plan_revision=adapter.plan_revision,
            expected_revision=len(events),
            created_at="1970-01-01T00:00:00Z",
            authority=adapter.authority,
        )
        events.append(event)
        active[("child_closure", child_id)] = event
        closures.append(closure)
    closures.sort(key=lambda value: json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ))
    candidate["child_closures"] = closures
    candidate["projection_revision"] = len(events)
    dependency_graph = candidate.get("dependency_graph")
    if isinstance(dependency_graph, dict):
        dependency_graph["observed_frontier"]["projection_revision"] = len(events)
    return candidate


def terminal_child_evidence_seed_predecessor_contract(
    snapshot: dict[str, Any], state: Any, comments: list[dict[str, Any]], *,
    workstream_id: str, predecessor_plan_revision: str,
    desired_scope: dict[str, Any], seeds: list[dict[str, Any]],
    desired_contracts: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Derive the exact predecessor authority for carried terminal evidence."""
    history = state.snapshot.get("projection_history") or []
    if not isinstance(history, list):
        raise LinearProjectionError("terminal_seed_predecessor_history_invalid")
    linear = desired_scope.get("linear") or {}
    predecessor_route = {
        field: linear.get(field)
        for field in (
            "workspace_id", "team_id", "project_id", "root_issue_id",
        )
    }
    try:
        predecessor_state = reduce_projection_comments(
            comments, workstream_id=workstream_id,
            expected_plan_revision=predecessor_plan_revision,
            authenticated_route=predecessor_route,
        )
    except LinearProjectionError as error:
        raise LinearProjectionError(
            "terminal_seed_predecessor_projection_invalid"
        ) from error
    generation = list(predecessor_state.events)
    activations = [
        (index, event) for index, event in enumerate(generation)
        if event.get("kind") == "cas_activation"
    ]
    has_legacy = any(event.get("schema_version") == 1 for event in generation)
    mixed_history_invalid = has_legacy and (
        len(activations) != 1
        or any(
            event.get("schema_version") != 1
            for event in generation[:activations[0][0]]
        )
        or any(
            event.get("schema_version") != 2
            for event in generation[activations[0][0]:]
        )
        or activations[0][1]["value"].get("legacy_event_ids") != [
            event["event_id"] for event in generation[:activations[0][0]]
        ]
    )
    raw_generation = sorted(
        [
            event for event in history
            if event.get("plan_revision") == predecessor_plan_revision
        ],
        key=lambda event: (
            event.get("expected_revision"), event.get("created_at"),
            event.get("event_id"),
        ),
    )
    if (
        not generation
        or predecessor_state.snapshot.get("projection_quarantined")
        or generation != raw_generation
        or mixed_history_invalid
        or not has_legacy and any(
            event.get("schema_version") != 2 for event in generation
        )
    ):
        raise LinearProjectionError(
            "terminal_seed_predecessor_projection_invalid"
        )
    heads: dict[tuple[str, str], dict[str, Any]] = {}
    active: dict[tuple[str, str], dict[str, Any]] = {}
    for event in generation:
        identity = (event["kind"], event["key"])
        previous = heads.get(identity)
        if event.get("supersedes_event_id") != (
            previous.get("event_id") if previous else None
        ):
            raise LinearProjectionError(
                "terminal_seed_predecessor_projection_invalid"
            )
        heads[identity] = event
        if event["value"] == TOMBSTONE:
            active.pop(identity, None)
        else:
            active[identity] = event
    try:
        authorized = closure_bound_historical_evidence(
            generation, desired_scope,
        )
        authorized = authorized | _current_head_predecessor_evidence(
            snapshot, active, desired_scope, seeds,
            predecessor_plan_revision=predecessor_plan_revision,
        )
        material = reduce_event_comments(comments, workstream_id=workstream_id)
        checkpoints = reduce_checkpoint_comments(
            comments, workstream_id=workstream_id,
        )
        recovered = recover_generations(
            list(checkpoints.checkpoints), workstream_id,
        )
    except (
        ProjectionHistoryError, LinearEventError, LinearCheckpointError,
        CheckpointError,
    ) as error:
        raise LinearProjectionError(str(error)) from error
    material_values = [
        {
            "event_id": event.event_id, "workstream_id": event.workstream_id,
            "kind": event.kind, "source": event.source,
            "payload": event.payload,
            "expected_revision": event.expected_revision,
            "created_at": event.created_at,
        }
        for event in material.events
    ]
    predecessor_checkpoints = [
        checkpoint for checkpoint in checkpoints.checkpoints
        if checkpoint.get("plan_revision") == predecessor_plan_revision
    ]
    checkpoint = recovered.get(predecessor_plan_revision)
    reviewed_heads: list[dict[str, Any]] = []
    authorities: dict[str, dict[str, Any]] = {}
    expected_keys = {
        key for seed in seeds for key in seed["evidence_keys"]
    }
    if set(desired_contracts) != expected_keys:
        raise LinearProjectionError(
            "terminal_seed_predecessor_contract_set_incomplete"
        )
    for seed in seeds:
        child_id = seed["child_identifier"].upper()
        closure = active.get(("child_closure", child_id))
        if closure is None:
            raise LinearProjectionError(
                f"terminal_seed_predecessor_closure_missing:{child_id}"
            )
        for key in seed["evidence_keys"]:
            evidence = active.get(("evidence_contract", key))
            desired = desired_contracts[key]
            if evidence is None or evidence["event_id"] not in authorized:
                raise LinearProjectionError(
                    f"terminal_seed_predecessor_evidence_not_authorized:"
                    f"{child_id}:{key}"
                )
            expected = deepcopy(evidence["value"])
            expected["plan_revision"] = desired.get("plan_revision")
            expected.pop("predecessor_closure_authority", None)
            candidate = deepcopy(desired)
            candidate.pop("predecessor_closure_authority", None)
            if (
                evidence["value"].get("plan_revision")
                != predecessor_plan_revision
                or candidate != expected
                or closure["value"].get("repository_key")
                != desired.get("repository_key")
                or closure["value"].get("exact_head")
                != desired.get("exact_head")
            ):
                raise LinearProjectionError(
                    f"terminal_seed_predecessor_contract_mutated:{child_id}:{key}"
                )
            reviewed = {
                "child_identifier": child_id, "key": key,
                "evidence_event_id": evidence["event_id"],
                "evidence_value_sha256": canonical_digest(evidence["value"]),
                "closure_event_id": closure["event_id"],
                "closure_value_sha256": canonical_digest(closure["value"]),
            }
            reviewed_heads.append(reviewed)
            authorities[key] = reviewed
    reviewed_heads.sort(key=lambda item: (item["child_identifier"], item["key"]))
    frontier = generation[-1]
    return {
        "schema_version": 1,
        "plan_revision": predecessor_plan_revision,
        "projection_revision": len(generation),
        "projection_events_sha256": canonical_digest(generation),
        "projection_frontier_event_id": frontier["event_id"],
        "projection_frontier_sha256": canonical_digest(frontier),
        "projection_history_sha256": canonical_digest(history),
        "material_revision": material.revision,
        "material_events_sha256": canonical_digest(material_values),
        "checkpoint_event_id": (
            checkpoint.get("checkpoint_event_id") if checkpoint else None
        ),
        "checkpoint_events_sha256": canonical_digest(predecessor_checkpoints),
        "input_frontier_sha256": projection_input_frontier_sha256(
            snapshot, comments,
        ),
        "evidence_heads": reviewed_heads,
    }, authorities


def _current_head_predecessor_evidence(
    snapshot: dict[str, Any], active: dict[tuple[str, str], dict[str, Any]],
    current_scope: dict[str, Any], seeds: list[dict[str, Any]], *,
    predecessor_plan_revision: str,
) -> frozenset[str]:
    """Authorize carried evidence whose closed repository head is unchanged.

    Historical-head authority comes from immutable projection order. When the
    repository has not advanced, bind the same closure to the exact current
    terminal child readback instead; merely sharing a head is insufficient.
    """
    seeds_by_child: dict[str, dict[str, Any]] = {}
    for seed in seeds:
        child_id = str(seed.get("child_identifier", "")).upper()
        if child_id in seeds_by_child:
            raise LinearProjectionError(
                f"terminal_seed_predecessor_child_ambiguous:{child_id}"
            )
        seeds_by_child[child_id] = seed
    repositories = {
        repository_key(repository): repository
        for repository in current_scope.get("repositories", [])
    }
    authorized: set[str] = set()
    for child_id, seed in seeds_by_child.items():
        closure = active.get(("child_closure", child_id))
        if closure is None:
            continue
        closure_value = closure["value"]
        repository_key_value = closure_value.get("repository_key")
        repository = repositories.get(repository_key_value)
        if (
            current_scope.get("child_ownership", {}).get(child_id)
            != repository_key_value
            or repository is None
            or repository.get("exact_head") != closure_value.get("exact_head")
        ):
            continue
        matches = [
            child for child in snapshot.get("children", [])
            if str(child.get("identifier", "")).upper() == child_id
        ]
        if len(matches) != 1:
            raise LinearProjectionError(
                f"terminal_seed_predecessor_child_ambiguous:{child_id}"
            )
        try:
            readback = terminal_child_readback(matches[0])
        except ChildClosureError as error:
            raise LinearProjectionError(f"{child_id}:{error}") from error
        readback_sha256 = canonical_digest(readback)
        if (
            readback["child_issue_id"] != seed.get("child_issue_id")
            or readback["assignee_id"] != seed.get("expected_assignee_id")
            or readback_sha256 != seed.get("expected_child_readback_sha256")
            or closure_value.get("child_readback_sha256") != readback_sha256
            or {
                field: closure_value.get(field) for field in CHILD_READBACK_FIELDS
            } != readback
            or closure_value.get("plan_revision")
            != predecessor_plan_revision
        ):
            continue
        evidence = sorted(
            [
                event for (kind, _key), event in active.items()
                if kind == "evidence_contract"
                and event["value"].get("owning_child") == child_id
            ],
            key=lambda event: (event["key"], event["event_id"]),
        )
        expected_heads = [
            {
                "key": event["key"], "event_id": event["event_id"],
                "value_sha256": canonical_digest(event["value"]),
            }
            for event in evidence
        ]
        if (
            not evidence
            or seed.get("evidence_keys") != [
                event["key"] for event in evidence
            ]
            or expected_heads != closure_value.get("evidence_heads")
            or evidence_receipts_sha256([
                event["value"] for event in evidence
            ]) != closure_value.get("evidence_receipts_sha256")
            or any(
                event["value"].get("plan_revision")
                != predecessor_plan_revision
                or event["value"].get("repository_key")
                != repository_key_value
                or event["value"].get("exact_head")
                != closure_value.get("exact_head")
                or evidence_errors(event["value"])
                for event in evidence
            )
        ):
            continue
        authorized.update(event["event_id"] for event in evidence)
    return frozenset(authorized)


def _fence_predecessor_projection_history(
    state: Any, binding: dict[str, Any],
) -> None:
    """Refuse any predecessor projection append after the reviewed carry."""
    history = state.snapshot.get("projection_history") or []
    if not isinstance(history, list):
        raise LinearProjectionError(
            "terminal_seed_predecessor_history_changed_reload_required"
        )
    generation = sorted(
        [
            event for event in history
            if event.get("plan_revision") == binding["plan_revision"]
        ],
        key=lambda event: (
            event.get("expected_revision"), event.get("created_at"),
            event.get("event_id"),
        ),
    )
    frontier = generation[-1] if generation else None
    if (
        canonical_digest(history) != binding["projection_history_sha256"]
        or len(generation) != binding["projection_revision"]
        or canonical_digest(generation) != binding[
            "projection_events_sha256"
        ]
        or frontier is None
        or frontier.get("event_id") != binding[
            "projection_frontier_event_id"
        ]
        or canonical_digest(frontier) != binding[
            "projection_frontier_sha256"
        ]
    ):
        raise LinearProjectionError(
            "terminal_seed_predecessor_history_changed_reload_required"
        )


def _predecessor_binding_from_carried_authority(
    authority: dict[str, Any],
) -> dict[str, Any]:
    return {
        "plan_revision": authority["predecessor_plan_revision"],
        "projection_revision": authority[
            "predecessor_projection_revision"
        ],
        "projection_events_sha256": authority[
            "predecessor_projection_events_sha256"
        ],
        "projection_frontier_event_id": authority[
            "predecessor_projection_frontier_event_id"
        ],
        "projection_frontier_sha256": authority[
            "predecessor_projection_frontier_sha256"
        ],
        "projection_history_sha256": authority[
            "projection_history_sha256"
        ],
    }


def _validate_gen14_legacy_split_repair_prefix(
    manifest: dict[str, Any], state: Any, desired_scope: dict[str, Any],
) -> None:
    """Accept only the captured prefix and its deterministic D6/S7 tail."""
    transition = manifest[
        "terminal_child_evidence_seed_legacy_split_head_repair"
    ]
    prefix = list(state.events[:6])
    prefix_created_at = prefix[0].get("created_at") if prefix else None
    plan_revision = prefix[0].get("plan_revision") if prefix else None
    authority = prefix[0].get("authority") if prefix else None
    base_state = SimpleNamespace(
        revision=6, events=tuple(prefix), snapshot=deepcopy(state.snapshot),
    )
    reviewed_contract = {
        field: deepcopy(manifest[field]) for field in REVIEW_CONTRACT_FIELDS
    }
    base_contract = projection_review_contract(base_state)
    current_contract = projection_review_contract(state)
    reviewed_revision = manifest.get("expected_projection_revision")
    progress_contract = None
    if isinstance(reviewed_revision, int) and 6 <= reviewed_revision <= len(
        state.events
    ):
        progress_contract = projection_review_contract(SimpleNamespace(
            revision=reviewed_revision,
            events=tuple(state.events[:reviewed_revision]),
            snapshot=deepcopy(state.snapshot),
        ))
    scope = prefix[5].get("value", {}) if len(prefix) == 6 else {}
    primary_key = scope.get("primary_repository")
    primary = [
        repository for repository in scope.get("repositories", [])
        if repository_key(repository) == primary_key
    ]
    disposition = prefix[4].get("value", {}) if len(prefix) == 6 else {}
    frontiers = {
        event.get("value", {}).get("predecessor_closure_authority", {}).get(
            "input_frontier_sha256"
        )
        for event in prefix[:2]
    }
    if len(prefix) != 6 or not hmac.compare_digest(
        canonical_digest(prefix), GEN14_SPLIT_PREFIX_SHA256,
    ):
        raise LinearProjectionError("legacy_split_head_repair_prefix_mismatch")
    if frontiers != {GEN14_SPLIT_STORED_FRONTIER_SHA256}:
        raise LinearProjectionError(
            "legacy_split_head_repair_stored_frontier_mismatch"
        )
    if not hmac.compare_digest(
        transition["input_frontier_sha256"],
        GEN14_SPLIT_RECOMPUTED_FRONTIER_SHA256,
    ):
        raise LinearProjectionError(
            "legacy_split_head_repair_recomputed_frontier_mismatch"
        )
    if (
        len(prefix) != 6
        or not hmac.compare_digest(
            canonical_digest(prefix), GEN14_SPLIT_PREFIX_SHA256,
        )
        or len(state.events) not in {6, 7, 8}
        or len(primary) != 1 or len(frontiers) != 1
        or any(
            event.get("schema_version") != 2
            or event.get("expected_revision") != index
            or event.get("workstream_id") != "GEN-14"
            or event.get("plan_revision") != plan_revision
            or event.get("authority") != authority
            or event.get("created_at") != prefix_created_at
            or event.get("supersedes_event_id") is not None
            for index, event in enumerate(prefix)
        )
        or not isinstance(prefix_created_at, str) or not prefix_created_at
        or (
            reviewed_contract != base_contract
            and reviewed_contract != current_contract
            and reviewed_contract != progress_contract
        )
        or transition["from_exact_head"] != primary[0].get("exact_head")
        or transition["from_disposition_exact_head"]
        != disposition.get("remote_head")
        or transition["to_exact_head"] in {
            transition["from_exact_head"],
            transition["from_disposition_exact_head"],
        }
        or transition["from_scope_event_id"] != prefix[5].get("event_id")
        or transition["from_disposition_event_id"] != prefix[4].get("event_id")
        or transition["from_scope_value_sha256"]
        != canonical_digest(prefix[5].get("value"))
        or transition["from_disposition_value_sha256"]
        != canonical_digest(prefix[4].get("value"))
    ):
        raise LinearProjectionError("legacy_split_head_repair_prefix_mismatch")
    expected_tail = [
        build_projection_event(
            workstream_id="GEN-14", kind="disposition", key="root",
            value=transition["disposition"], plan_revision=plan_revision,
            expected_revision=6, created_at=transition["created_at"],
            supersedes_event_id=prefix[4]["event_id"], authority=authority,
        ),
        build_projection_event(
            workstream_id="GEN-14", kind="scope", key="root",
            value=desired_scope, plan_revision=plan_revision,
            expected_revision=7, created_at=transition["created_at"],
            supersedes_event_id=prefix[5]["event_id"], authority=authority,
        ),
    ]
    if list(state.events[6:]) != expected_tail[:len(state.events) - 6]:
        raise LinearProjectionError(
            "legacy_split_head_repair_noncanonical_progress"
        )


def _gen14_completed_split_migration(state: Any) -> bool:
    """Recognize only the authenticated D6/S7 completion of the captured prefix."""
    if len(state.events) != 8:
        return False
    prefix = list(state.events[:6])
    if not hmac.compare_digest(
        canonical_digest(prefix), GEN14_SPLIT_PREFIX_SHA256,
    ):
        return False
    frontiers = {
        event.get("value", {}).get("predecessor_closure_authority", {}).get(
            "input_frontier_sha256"
        )
        for event in prefix[:2]
    }
    disposition, scope = state.events[6:8]
    primary_key = prefix[5].get("value", {}).get("primary_repository")
    desired_scope = deepcopy(prefix[5].get("value", {}))
    primary = next((
        repository for repository in desired_scope.get("repositories", [])
        if repository_key(repository) == primary_key
    ), None)
    remote_head = disposition.get("value", {}).get("remote_head")
    if (
        frontiers != {GEN14_SPLIT_STORED_FRONTIER_SHA256}
        or primary is None
        or not isinstance(remote_head, str)
        or remote_head in {
            primary.get("exact_head"),
            prefix[4].get("value", {}).get("remote_head"),
        }
        or disposition.get("created_at") != scope.get("created_at")
    ):
        return False
    primary["exact_head"] = remote_head
    expected = [
        build_projection_event(
            workstream_id="GEN-14", kind="disposition", key="root",
            value={
                "disposition": "create_successor", "remote_head": remote_head,
                "recovered_from_checkpoint": None,
            },
            plan_revision=prefix[0]["plan_revision"], expected_revision=6,
            created_at=disposition["created_at"],
            supersedes_event_id=prefix[4]["event_id"],
            authority=prefix[0]["authority"],
        ),
        build_projection_event(
            workstream_id="GEN-14", kind="scope", key="root",
            value=desired_scope,
            plan_revision=prefix[0]["plan_revision"], expected_revision=7,
            created_at=disposition["created_at"],
            supersedes_event_id=prefix[5]["event_id"],
            authority=prefix[0]["authority"],
        ),
    ]
    return list(state.events[6:8]) == expected


def _gen14_completed_split_stable_source_prefix(state: Any) -> bool:
    """Recognize exact D6/S7 plus at most its reviewed source-only event."""
    if len(state.events) not in {8, 9}:
        return False
    base = SimpleNamespace(
        revision=8, events=tuple(state.events[:8]),
        snapshot=deepcopy(state.snapshot),
    )
    if not _gen14_completed_split_migration(base):
        return False
    if len(state.events) == 8:
        return True
    prefix_source = state.events[3]
    source = state.events[8]
    value = source.get("value", {})
    return (
        isinstance(value, dict)
        and value.get("sha256") == prefix_source.get("value", {}).get("sha256")
        and _valid_reviewed_source_transition(
            str(prefix_source.get("value", {}).get("identity", "")),
            str(value.get("identity", "")),
        )
        and source == build_projection_event(
            workstream_id="GEN-14", kind="source", key="root", value=value,
            plan_revision=prefix_source["plan_revision"], expected_revision=8,
            created_at=source.get("created_at", ""),
            supersedes_event_id=prefix_source["event_id"],
            authority=prefix_source["authority"],
        )
    )


def _gen14_stable_repair_descendant(
    state: Any, desired: list[dict[str, Any]], bridge: dict[str, Any],
) -> bool:
    """Accept only the exact sorted closure prefix after reviewed source."""
    if len(state.events) < 9 or not _gen14_completed_split_stable_source_prefix(
        SimpleNamespace(
            revision=9, events=tuple(state.events[:9]),
            snapshot=deepcopy(state.snapshot),
        )
    ):
        return False
    source = state.events[8]
    desired_sources = [
        item for item in desired
        if (item.get("kind"), item.get("key")) == ("source", "root")
    ]
    if (
        source["event_id"] != bridge.get("source_event_id")
        or canonical_digest(source["value"])
        != bridge.get("source_value_sha256")
        or len(desired_sources) != 1
        or desired_sources[0].get("value") != source.get("value")
    ):
        return False
    closures = sorted(
        [item for item in desired if item.get("kind") == "child_closure"],
        key=lambda item: item.get("key", ""),
    )
    closure_ids = [item.get("key") for item in closures]
    if (
        not 1 <= len(closures) <= 2
        or closure_ids != bridge.get("child_identifiers")
        or len(set(closure_ids)) != len(closure_ids)
        or any(
            not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(child_id or ""))
            for child_id in closure_ids
        )
    ):
        return False
    expected = [
        build_projection_event(
            workstream_id="GEN-14", kind="child_closure", key=item["key"],
            value=item["value"], plan_revision=state.events[0]["plan_revision"],
            expected_revision=9 + index, created_at=bridge["created_at"],
            authority=state.events[0]["authority"],
        )
        for index, item in enumerate(closures)
    ]
    tail = list(state.events[9:])
    return len(tail) <= len(expected) and tail == expected[:len(tail)]


def _gen14_frontier_only_evidence_normalization(
    current: dict[str, Any], desired: dict[str, Any], *, enabled: bool,
) -> bool:
    if not enabled:
        return False
    migrated = deepcopy(current)
    authority = migrated.get("predecessor_closure_authority")
    if (
        not isinstance(authority, dict)
        or authority.get("input_frontier_sha256")
        != GEN14_SPLIT_STORED_FRONTIER_SHA256
    ):
        return False
    authority["input_frontier_sha256"] = (
        GEN14_SPLIT_RECOMPUTED_FRONTIER_SHA256
    )
    return migrated == desired


def _gen14_completed_normalization_replay(
    state: Any, manifest: dict[str, Any], desired: list[dict[str, Any]], *,
    created_at: str,
) -> bool:
    """Accept only the deterministic ordinary tail following exact D6/S7."""
    ordinary_transition = manifest.get(
        "terminal_child_evidence_seed_head_transition"
    )
    if (
        len(state.events) < 8
        or not isinstance(ordinary_transition, dict)
        or ordinary_transition.get("created_at") != created_at
        or manifest.get("terminal_child_evidence_seed_legacy_split_head_repair")
        is not None
    ):
        return False
    base = SimpleNamespace(
        revision=8, events=tuple(state.events[:8]), snapshot=deepcopy(state.snapshot),
    )
    if not _gen14_completed_split_migration(base):
        return False
    active = _active_heads(base)
    seeds = manifest.get("terminal_child_evidence_seeds") or []
    seed_keys = {key for seed in seeds for key in seed.get("evidence_keys", [])}
    candidates = [
        item for item in desired
        if (
            (item["kind"] == "evidence_contract" and item["key"] in seed_keys)
            or (item["kind"], item["key"])
            in {("disposition", "root"), ("scope", "root")}
        )
        and (
            active.get((item["kind"], item["key"])) is None
            or active[(item["kind"], item["key"])]["value"] != item["value"]
        )
    ]
    ordered = sorted(candidates, key=lambda item: (
        0 if item["kind"] == "evidence_contract"
        else 1 if item["kind"] == "disposition" else 2,
        item["key"],
    ))
    expected = [build_projection_event(
        workstream_id="GEN-14", kind=item["kind"], key=item["key"],
        value=item["value"], plan_revision=state.events[0]["plan_revision"],
        expected_revision=8 + index, created_at=created_at,
        supersedes_event_id=(
            active.get((item["kind"], item["key"])) or {}
        ).get("event_id"), authority=state.events[0]["authority"],
    ) for index, item in enumerate(ordered)]
    tail = list(state.events[8:])
    return len(tail) <= len(expected) and tail == expected[:len(tail)]


def prepare_terminal_child_evidence_seeds(
    manifest: dict[str, Any], snapshot: dict[str, Any], state: Any, *,
    remote_head: str | None = None,
    comments: list[dict[str, Any]] | None = None,
    trusted_nonprimary_backfill_receipt: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate an add-only evidence prefix before terminal closure repair."""
    result = deepcopy(manifest)
    _reviewed_manifest(result)
    seeds = result.get("terminal_child_evidence_seeds") or []
    if not seeds:
        return result
    if result.get("retirements"):
        raise LinearProjectionError("terminal_child_evidence_seed_forbids_retirements")
    original_contract = {
        field: deepcopy(result[field]) for field in REVIEW_CONTRACT_FIELDS
    }
    active = _active_heads(state)
    scope_event = active.get(("scope", "root"))
    bootstrap = _exact_empty_review_contract(original_contract)
    desired = result["projection"]
    desired_by_identity = {
        (item["kind"], item["key"]): item for item in desired
    }
    desired_scope_item = desired_by_identity.get(("scope", "root"))
    if desired_scope_item is None:
        raise LinearProjectionError("terminal_child_evidence_seed_scope_missing")
    desired_scope = desired_scope_item["value"]
    legacy_split = result.get(
        "terminal_child_evidence_seed_legacy_split_head_repair"
    )
    nonprimary_backfill = result.get(
        "terminal_child_evidence_seed_nonprimary_backfill"
    )
    if legacy_split is not None:
        _validate_gen14_legacy_split_repair_prefix(
            result, state, desired_scope,
        )
    predecessor_binding = result.get(
        "terminal_child_evidence_seed_predecessor"
    )
    completed_split_normalization = False
    predecessor_authorities: dict[str, dict[str, Any]] = {}
    if predecessor_binding is not None:
        if legacy_split is not None:
            carried = [
                event["value"] for (kind, _key), event in active.items()
                if kind == "evidence_contract"
                and isinstance(
                    event["value"].get("predecessor_closure_authority"), dict,
                )
            ]
            authorities = [
                contract["predecessor_closure_authority"]
                for contract in carried
            ]
            if not authorities:
                raise LinearProjectionError(
                    "legacy_split_head_repair_predecessor_missing"
                )
            first = authorities[0]
            observed_binding = {
                "schema_version": 1,
                "plan_revision": first["predecessor_plan_revision"],
                "projection_revision": first["predecessor_projection_revision"],
                "projection_events_sha256": first[
                    "predecessor_projection_events_sha256"
                ],
                "projection_frontier_event_id": first[
                    "predecessor_projection_frontier_event_id"
                ],
                "projection_frontier_sha256": first[
                    "predecessor_projection_frontier_sha256"
                ],
                "projection_history_sha256": first["projection_history_sha256"],
                "material_revision": first["material_revision"],
                "material_events_sha256": first["material_events_sha256"],
                "checkpoint_event_id": first["checkpoint_event_id"],
                "checkpoint_events_sha256": first["checkpoint_events_sha256"],
                "input_frontier_sha256": first["input_frontier_sha256"],
                "evidence_heads": sorted([{
                    "child_identifier": contract["owning_child"],
                    "key": contract["slice_id"],
                    "evidence_event_id": authority[
                        "predecessor_evidence_event_id"
                    ],
                    "evidence_value_sha256": authority[
                        "predecessor_evidence_value_sha256"
                    ],
                    "closure_event_id": authority[
                        "predecessor_closure_event_id"
                    ],
                    "closure_value_sha256": authority[
                        "predecessor_closure_value_sha256"
                    ],
                } for contract, authority in zip(carried, authorities)],
                    key=lambda item: (
                        item["child_identifier"], item["key"],
                    )),
            }
            common = {
                key: value for key, value in first.items()
                if not key.startswith("predecessor_evidence_")
                and not key.startswith("predecessor_closure_")
            }
            migrated_observed_binding = deepcopy(observed_binding)
            captured_frontier_migration = (
                observed_binding["input_frontier_sha256"]
                == GEN14_SPLIT_STORED_FRONTIER_SHA256
                and predecessor_binding.get("input_frontier_sha256")
                == GEN14_SPLIT_RECOMPUTED_FRONTIER_SHA256
            )
            if captured_frontier_migration:
                migrated_observed_binding["input_frontier_sha256"] = (
                    GEN14_SPLIT_RECOMPUTED_FRONTIER_SHA256
                )
            if (
                migrated_observed_binding != predecessor_binding
                or any({
                    key: value for key, value in authority.items()
                    if not key.startswith("predecessor_evidence_")
                    and not key.startswith("predecessor_closure_")
                } != common for authority in authorities)
            ):
                raise LinearProjectionError(
                    "legacy_split_head_repair_predecessor_changed"
                )
        elif comments is None:
            raise LinearProjectionError(
                "terminal_seed_predecessor_comments_required"
            )
        if legacy_split is None:
            desired_contracts = {
            item["key"]: item["value"] for item in desired
            if item["kind"] == "evidence_contract"
            }
            observed_binding, predecessor_authorities = (
                terminal_child_evidence_seed_predecessor_contract(
                    snapshot, state, comments,
                    workstream_id=str(snapshot.get("root", {}).get("identifier", "")),
                    predecessor_plan_revision=predecessor_binding["plan_revision"],
                    desired_scope=desired_scope, seeds=seeds,
                    desired_contracts=desired_contracts,
                )
            )
        if legacy_split is None and observed_binding != predecessor_binding:
            raise LinearProjectionError(
                "terminal_seed_predecessor_binding_changed_reload_required"
            )
        common_authority = {
            "schema_version": 1,
            "predecessor_plan_revision": predecessor_binding["plan_revision"],
            "predecessor_projection_revision": predecessor_binding[
                "projection_revision"
            ],
            "predecessor_projection_events_sha256": predecessor_binding[
                "projection_events_sha256"
            ],
            "predecessor_projection_frontier_event_id": predecessor_binding[
                "projection_frontier_event_id"
            ],
            "predecessor_projection_frontier_sha256": predecessor_binding[
                "projection_frontier_sha256"
            ],
            "projection_history_sha256": predecessor_binding[
                "projection_history_sha256"
            ],
            "material_revision": predecessor_binding["material_revision"],
            "material_events_sha256": predecessor_binding[
                "material_events_sha256"
            ],
            "checkpoint_event_id": predecessor_binding["checkpoint_event_id"],
            "checkpoint_events_sha256": predecessor_binding[
                "checkpoint_events_sha256"
            ],
            "input_frontier_sha256": predecessor_binding[
                "input_frontier_sha256"
            ],
        }
        ordinary_transition = result.get(
            "terminal_child_evidence_seed_head_transition"
        )
        retain_stable_completed_split_evidence = (
            legacy_split is None
            and ordinary_transition is None
            and _gen14_completed_split_stable_source_prefix(state)
        )
        for item in desired if legacy_split is None else []:
            if item["kind"] != "evidence_contract":
                continue
            if retain_stable_completed_split_evidence:
                current = active.get((item["kind"], item["key"]))
                if current is None:
                    raise LinearProjectionError(
                        "gen14_completed_split_evidence_missing"
                    )
                # The content-addressed prefix already authenticated these
                # evidence values. D6/S7 advances target history, so a freshly
                # derived predecessor-authority envelope is not the stored
                # value and must not be written as an evidence replacement.
                # This no-transition path is the stable repaired-head case;
                # changed-head normalization retains its existing contract.
                item["value"] = deepcopy(current["value"])
                continue
            reviewed = predecessor_authorities[item["key"]]
            item["value"]["predecessor_closure_authority"] = {
                **common_authority,
                "predecessor_evidence_event_id": reviewed[
                    "evidence_event_id"
                ],
                "predecessor_evidence_value_sha256": reviewed[
                    "evidence_value_sha256"
                ],
                "predecessor_closure_event_id": reviewed["closure_event_id"],
                "predecessor_closure_value_sha256": reviewed[
                    "closure_value_sha256"
                ],
            }
        completed_split_normalization = False
        if legacy_split is None and ordinary_transition is not None:
            replay_desired = [*deepcopy(desired), {
                "kind": "disposition", "key": "root",
                "value": deepcopy(ordinary_transition["disposition"]),
            }]
            ordinary_created_at = ordinary_transition.get("created_at")
            completed_split_normalization = (
                isinstance(ordinary_created_at, str)
                and bool(ordinary_created_at)
                and (
                    _gen14_completed_split_migration(state)
                    or _gen14_completed_normalization_replay(
                        state, result, replay_desired,
                        created_at=ordinary_created_at,
                    )
                )
            )
    bootstrap = bootstrap or _terminal_seed_bootstrap_prefix(
        original_contract, state, desired, remote_head=remote_head,
    )
    if (
        bootstrap
        and state.snapshot.get("projection_history")
        and predecessor_binding is None
    ):
        raise LinearProjectionError(
            "terminal_seed_predecessor_binding_required"
        )
    if scope_event is None and not bootstrap:
        raise LinearProjectionError("terminal_child_evidence_seed_scope_missing")
    scope_value = (
        scope_event["value"] if scope_event is not None else desired_scope
    )
    seed_key_set = {
        key for seed in seeds for key in seed["evidence_keys"]
    }
    if bootstrap:
        allowed = {
            ("scope", "root"), ("source", "root"),
            *[("evidence_contract", key) for key in seed_key_set],
            *[
                (item["kind"], item["key"])
                for item in desired if item["kind"] == "provenance"
            ],
        }
        desired_identities = set(desired_by_identity)
        if (
            result.get("retirements")
            or result.get("terminal_child_evidence_seed_head_transition")
            or result.get(
                "terminal_child_evidence_seed_legacy_split_head_repair"
            )
            or desired_identities != allowed
            or any(item["kind"] == "relation" for item in desired)
        ):
            raise LinearProjectionError(
                "terminal_child_evidence_seed_bootstrap_unrelated_change"
            )
    primary_key = scope_value.get("primary_repository")
    current_primary = next((
        repository for repository in scope_value.get("repositories", [])
        if repository_key(repository) == primary_key
    ), None)
    desired_primary = next((
        repository for repository in desired_scope.get("repositories", [])
        if repository_key(repository) == primary_key
    ), None)
    transition = (
        result.get("terminal_child_evidence_seed_head_transition")
        or result.get("terminal_child_evidence_seed_legacy_split_head_repair")
    )
    if nonprimary_backfill is not None:
        reviewed_scope_event = next((
            event for event in state.events
            if event.get("event_id") == nonprimary_backfill["from_scope_event_id"]
            and (event.get("kind"), event.get("key")) == ("scope", "root")
        ), None)
        reviewed_disposition_event = next((
            event for event in state.events
            if event.get("event_id") == nonprimary_backfill["from_disposition_event_id"]
            and (event.get("kind"), event.get("key")) == ("disposition", "root")
        ), None)
        current_disposition = active.get(("disposition", "root"), {}).get("value", {})
        if (
            reviewed_scope_event is None
            or scope_event is None
            or reviewed_scope_event["event_id"] != scope_event["event_id"]
            or reviewed_scope_event["value"] != scope_value
            or canonical_digest(reviewed_scope_event["value"])
            != nonprimary_backfill["from_scope_value_sha256"]
            or reviewed_disposition_event is None
            or active.get(("disposition", "root"), {}).get("event_id")
            != reviewed_disposition_event["event_id"]
            or canonical_digest(reviewed_disposition_event["value"])
            != nonprimary_backfill["from_disposition_value_sha256"]
            or current_disposition != reviewed_disposition_event["value"]
            or remote_head != current_disposition.get("remote_head")
            or nonprimary_backfill["repository_key"] == primary_key
        ):
            raise LinearProjectionError(
                "terminal_child_evidence_seed_nonprimary_backfill_frontier_invalid"
            )
    if transition is not None:
        reviewed_scope_event = next((
            event for event in state.events
            if event.get("event_id") == transition["from_scope_event_id"]
            and (event.get("kind"), event.get("key")) == ("scope", "root")
        ), None)
        reviewed_primary = next((
            repository
            for repository in (reviewed_scope_event or {"value": {}})[
                "value"
            ].get("repositories", [])
            if repository_key(repository) == transition["repository_key"]
        ), None)
        reviewed_disposition_event = next((
            event for event in state.events
            if event.get("event_id")
            == transition["from_disposition_event_id"]
            and (event.get("kind"), event.get("key"))
            == ("disposition", "root")
        ), None)
        reviewed_disposition = (
            reviewed_disposition_event.get("value", {})
            if reviewed_disposition_event is not None else {}
        )
        current_disposition_event = active.get(("disposition", "root"))
        disposition_progress_valid = (
            current_disposition_event is not None
            and (
                current_disposition_event["event_id"]
                == transition["from_disposition_event_id"]
                and current_disposition_event["value"] == reviewed_disposition
                or current_disposition_event["value"]
                == transition["disposition"]
                and current_disposition_event.get("supersedes_event_id")
                == transition["from_disposition_event_id"]
            )
        )
        if (
            reviewed_scope_event is None
            or canonical_digest(reviewed_scope_event["value"])
            != transition["from_scope_value_sha256"]
            or reviewed_primary is None
            or reviewed_primary.get("exact_head")
            != transition["from_exact_head"]
            or reviewed_disposition_event is None
            or canonical_digest(reviewed_disposition)
            != transition["from_disposition_value_sha256"]
            or set(reviewed_disposition) != {
                "disposition", "remote_head", "recovered_from_checkpoint",
            }
            or reviewed_disposition.get("remote_head")
            != transition.get(
                "from_disposition_exact_head", transition["from_exact_head"]
            )
            or reviewed_disposition.get("disposition")
            not in {"attach", "create_successor"}
            or not disposition_progress_valid
        ):
            raise LinearProjectionError(
                "terminal_child_evidence_seed_reviewed_predecessor_missing"
            )
    scope_head_transition = scope_event is not None and desired_scope != scope_value
    if scope_head_transition:
        if (
            transition is None
            or remote_head is None
            or not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", remote_head)
            or current_primary is None
            or desired_primary is None
            or desired_scope.get("primary_repository") != primary_key
            or desired_primary.get("exact_head") != remote_head
            or transition.get("repository_key") != primary_key
            or transition.get("from_exact_head")
            != current_primary.get("exact_head")
            or transition.get("to_exact_head") != remote_head
        ):
            raise LinearProjectionError(
                "terminal_child_evidence_seed_primary_head_transition_invalid"
            )
        exact_scope = deepcopy(scope_value)
        exact_primary = next(
            repository for repository in exact_scope["repositories"]
            if repository_key(repository) == primary_key
        )
        exact_primary["exact_head"] = remote_head
        if desired_scope != exact_scope:
            raise LinearProjectionError(
                "terminal_child_evidence_seed_scope_head_only_required"
            )
    elif transition is not None and (
        desired_primary is None
        or desired_primary.get("exact_head") != transition.get("to_exact_head")
        or transition.get("repository_key") != primary_key
        or remote_head != transition.get("to_exact_head")
    ):
        raise LinearProjectionError(
            "terminal_child_evidence_seed_disposition_changed"
        )
    seed_keys: list[tuple[str, str]] = []
    for seed in seeds:
        child_id = seed["child_identifier"].upper()
        matching = [
            child for child in snapshot.get("children", [])
            if str(child.get("identifier", "")).upper() == child_id
        ]
        if len(matching) != 1:
            raise LinearProjectionError(
                f"terminal_child_evidence_seed_ambiguous_child:{child_id}"
            )
        try:
            readback = terminal_child_readback(matching[0])
        except ChildClosureError as error:
            raise LinearProjectionError(f"{child_id}:{error}") from error
        if (
            readback["child_issue_id"] != seed["child_issue_id"]
            or readback["assignee_id"] != seed["expected_assignee_id"]
            or canonical_digest(readback)
            != seed["expected_child_readback_sha256"]
        ):
            raise LinearProjectionError(
                f"terminal_child_readback_changed_reload_required:{child_id}"
            )
        for field in ("workspace_id", "team_id", "project_id", "root_issue_id"):
            expected = scope_value["linear"][field]
            observed = (
                readback["parent_issue_id"]
                if field == "root_issue_id" else readback[field]
            )
            if observed != expected:
                raise LinearProjectionError(
                    f"terminal_child_evidence_seed_route_mismatch:{field}:"
                    f"{child_id}"
                )
        if ("child_closure", child_id) in active:
            raise LinearProjectionError(
                f"terminal_child_evidence_seed_closure_already_exists:{child_id}"
            )
        owner = scope_value.get("child_ownership", {}).get(child_id)
        if not owner:
            raise LinearProjectionError(
                f"terminal_child_evidence_seed_owner_missing:{child_id}"
            )
        repositories = [
            item for item in desired_scope.get("repositories", [])
            if repository_key(item) == owner
        ]
        if len(repositories) != 1:
            raise LinearProjectionError(
                f"terminal_child_evidence_seed_repository_ambiguous:{child_id}"
            )
        repository = repositories[0]
        nonprimary_transition = (
            (transition is not None or nonprimary_backfill is not None)
            and owner != primary_key
        )
        if nonprimary_transition:
            current_repositories = [
                item for item in scope_value.get("repositories", [])
                if repository_key(item) == owner
            ]
            if (
                desired_scope.get("child_ownership", {}).get(child_id) != owner
                or len(current_repositories) != 1
                or current_repositories[0] != repository
            ):
                raise LinearProjectionError(
                    f"terminal_child_evidence_seed_nonprimary_owner_changed:"
                    f"{child_id}"
                )
        for key in seed["evidence_keys"]:
            identity = ("evidence_contract", key)
            item = desired_by_identity.get(identity)
            if item is None:
                raise LinearProjectionError(
                    f"terminal_child_evidence_seed_contract_missing:{child_id}:{key}"
                )
            contract = item["value"]
            if nonprimary_backfill is not None:
                contract["nonprimary_backfill_authority"] = deepcopy(
                    nonprimary_backfill
                )
                if validated_nonprimary_backfill_authority(
                    {"value": contract}, scope_value, list(state.events),
                    list(state.events),
                    trusted_receipt=trusted_nonprimary_backfill_receipt,
                ) is None:
                    raise LinearProjectionError(
                        f"terminal_child_evidence_seed_nonprimary_backfill_contract_invalid:{child_id}:{key}"
                    )
            historical_authority = contract.get(
                "predecessor_closure_authority"
            )
            if (
                contract.get("owning_child") != child_id
                or contract.get("repository_key") != owner
                or (
                    nonprimary_transition
                    and nonprimary_backfill is None
                    and contract.get("exact_head") != repository.get("exact_head")
                )
                or (
                    nonprimary_backfill is None
                    and
                    contract.get("exact_head") != repository.get("exact_head")
                    and historical_authority is None
                )
                or evidence_errors(contract)
            ):
                raise LinearProjectionError(
                    f"terminal_child_evidence_seed_contract_invalid:{child_id}:{key}"
                )
            if nonprimary_transition and nonprimary_backfill is None and (
                predecessor_binding is None
                or not isinstance(historical_authority, dict)
            ):
                raise LinearProjectionError(
                    f"terminal_child_evidence_seed_nonprimary_predecessor_required:"
                    f"{child_id}:{key}"
                )
            if nonprimary_backfill is not None and (
                owner != nonprimary_backfill["repository_key"]
                or repository.get("provider_repository_id")
                != nonprimary_backfill["provider_repository_id"]
                or repository.get("exact_head")
                != nonprimary_backfill["from_exact_head"]
                or contract.get("exact_head") != nonprimary_backfill["to_exact_head"]
            ):
                raise LinearProjectionError(
                    f"terminal_child_evidence_seed_nonprimary_backfill_contract_invalid:{child_id}:{key}"
                )
            current = active.get(identity)
            if (
                current is not None
                and current["value"] != contract
                and not _gen14_frontier_only_evidence_normalization(
                    current["value"], contract,
                    enabled=completed_split_normalization,
                )
            ):
                raise LinearProjectionError(
                    f"terminal_child_evidence_seed_replacement_forbidden:{child_id}:{key}"
                )
            seed_keys.append(identity)
    allowed_changes = set(seed_keys)
    if scope_head_transition:
        allowed_changes.add(("scope", "root"))
    if bootstrap:
        allowed_changes.update(desired_by_identity)
    for item in desired:
        identity = (item["kind"], item["key"])
        current = active.get(identity)
        if identity in allowed_changes:
            continue
        if current is None or current["value"] != item["value"]:
            raise LinearProjectionError(
                f"terminal_child_evidence_seed_unrelated_change:{identity[0]}:{identity[1]}"
            )
    changed_order = [
        (item["kind"], item["key"]) for item in desired
        if (item["kind"], item["key"]) in set(seed_keys)
        and (item["kind"], item["key"]) not in active
    ]
    canonical_missing = [identity for identity in seed_keys if identity not in active]
    if changed_order != canonical_missing:
        raise LinearProjectionError("terminal_child_evidence_seed_projection_not_canonical")
    current_contract = projection_review_contract(state)
    if current_contract != original_contract:
        ordinary_transition = result.get(
            "terminal_child_evidence_seed_head_transition"
        )
        replay_desired = [*deepcopy(desired)]
        if ordinary_transition is not None:
            replay_desired.append({
                "kind": "disposition", "key": "root",
                "value": deepcopy(ordinary_transition["disposition"]),
            })
        if ordinary_transition is not None and _gen14_completed_normalization_replay(
            state, result, replay_desired,
            created_at=ordinary_transition.get("created_at", ""),
        ):
            result.update(current_contract)
            return result
        if legacy_split is not None:
            # The exact legacy validator above has already proven that the
            # only intervening events are the deterministic D6/S7 repair
            # tail.  Refresh only the review contract so a lost response can
            # replay the retained manifest without weakening ordinary CAS.
            result.update(current_contract)
            return result
        if bootstrap:
            evidence_progress = [
                (item["kind"], item["key"]) for item in desired
                if item["kind"] == "evidence_contract"
            ]
            ordinary_progress = [
                (item["kind"], item["key"]) for item in desired
                if item["kind"] not in {"evidence_contract", "scope"}
            ]
            expected_progress = [
                *evidence_progress, *ordinary_progress,
                ("disposition", "root"), ("scope", "root"),
            ]
            progress_events = list(state.events)
            progress = [
                (event["kind"], event["key"]) for event in progress_events
            ]
            desired_values = {
                identity: item["value"]
                for identity, item in desired_by_identity.items()
            }

            def valid_progress_event(
                event: dict[str, Any], identity: tuple[str, str],
            ) -> bool:
                if identity == ("disposition", "root"):
                    return (
                        isinstance(event["value"], dict)
                        and event["value"].get("remote_head") == remote_head
                        and event["value"].get("disposition")
                        in {"attach", "create_successor"}
                        and set(event["value"]) == {
                            "disposition", "remote_head",
                            "recovered_from_checkpoint",
                        }
                    )
                return (
                    identity in desired_values
                    and event["value"] == desired_values[identity]
                )

            valid_values = all(
                valid_progress_event(event, identity)
                for event, identity in zip(progress_events, progress)
            )
            if (
                progress != expected_progress[:len(progress)]
                or len(active) != len(progress)
                or set(active) != set(progress)
                or not valid_values
                or any(
                    current_contract[field] != original_contract[field]
                    for field in (
                        "expected_legacy_v1_event_ids",
                        "expected_legacy_v1_events_sha256",
                        "expected_projection_quarantine_count",
                        "expected_projection_quarantine_sha256",
                    )
                )
            ):
                raise LinearProjectionError(
                    "projection_review_stale_reload_required"
                )
            result.update(current_contract)
            return result
        expected_heads = {
            (head["kind"], head["key"]): head
            for head in original_contract["expected_active_heads"]
        }
        originally_missing = [
            identity for identity in seed_keys if identity not in expected_heads
        ]
        reviewed_scope_transition = (
            expected_heads.get(("scope", "root"), {}).get("value_sha256")
            != canonical_digest(desired_scope)
        )
        allowed_progress = [*originally_missing]
        if transition is not None:
            allowed_progress.append(("disposition", "root"))
        if reviewed_scope_transition:
            allowed_progress.append(("scope", "root"))
        progress = [
            (event["kind"], event["key"])
            for event in state.events[
                original_contract["expected_projection_revision"]:
            ]
        ]
        added = [identity for identity in originally_missing if identity in active]
        expected_identities = set(expected_heads)
        expected_identities.update(added)
        disposition = active.get(("disposition", "root"))
        valid_disposition = (
            disposition is not None
            and transition is not None
            and disposition["value"] == transition["disposition"]
        )
        allowed = (
            progress == allowed_progress[:len(progress)]
            and set(active) == expected_identities
            and all(
                identity in active and (
                    identity == ("scope", "root")
                    and reviewed_scope_transition
                    and active[identity]["value"] == desired_scope
                    or identity == ("disposition", "root")
                    and identity in progress
                    and valid_disposition
                    or active[identity]["event_id"] == head["event_id"]
                    and canonical_digest(active[identity]["value"])
                    == head["value_sha256"]
                )
                for identity, head in expected_heads.items()
            )
            and all(
                active[identity]["value"]
                == desired_by_identity[identity]["value"]
                for identity in originally_missing
                if identity in active
            )
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
        if allowed:
            result.update(current_contract)
        else:
            raise LinearProjectionError("projection_review_stale_reload_required")
    return result


def prepare_terminal_child_repairs(
    manifest: dict[str, Any], snapshot: dict[str, Any], state: Any,
) -> dict[str, Any]:
    """Derive one atomic terminal-repair batch from one stable truth surface."""
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
    active = _active_heads(state)
    bridge = result.get("terminal_child_repair_gen14_frontier_bridge")
    if bridge is not None:
        carried_frontiers = {
            event["value"].get("predecessor_closure_authority", {}).get(
                "input_frontier_sha256"
            )
            for (kind, _key), event in active.items()
            if kind == "evidence_contract"
        }
        if (
            carried_frontiers != {
                bridge["stored_input_frontier_sha256"]
            }
        ):
            raise LinearProjectionError(
                "terminal_child_repair_gen14_frontier_bridge_mismatch"
            )
    scope_event = active.get(("scope", "root"))
    if scope_event is None:
        raise LinearProjectionError("terminal_child_repair_scope_missing")
    current_scope = deepcopy(scope_event["value"])
    desired_scope = deepcopy(current_scope)
    closures: dict[str, dict[str, Any]] = {}
    existing_closures: dict[str, dict[str, Any] | None] = {}
    for repair in repairs:
        child_id = repair["child_identifier"].upper()
        matching = [
            child for child in snapshot.get("children", [])
            if str(child.get("identifier", "")).upper() == child_id
        ]
        if len(matching) != 1:
            raise LinearProjectionError(
                f"terminal_child_repair_ambiguous_child:{child_id}"
            )
        try:
            readback = terminal_child_readback(matching[0])
        except ChildClosureError as error:
            raise LinearProjectionError(f"{child_id}:{error}") from error
        if (
            readback["child_issue_id"] != repair["child_issue_id"]
            or readback["assignee_id"] != repair["expected_assignee_id"]
            or canonical_digest(readback)
            != repair["expected_child_readback_sha256"]
        ):
            raise LinearProjectionError(
                f"terminal_child_readback_changed_reload_required:{child_id}"
            )
        for field in ("workspace_id", "team_id", "project_id", "root_issue_id"):
            expected = current_scope["linear"][field]
            observed = (
                readback["parent_issue_id"]
                if field == "root_issue_id" else readback[field]
            )
            if observed != expected:
                raise LinearProjectionError(
                    f"terminal_child_repair_route_mismatch:{field}:{child_id}"
                )
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
        active_evidence_heads.sort(
            key=lambda item: (item["key"], item["event_id"]),
        )
        if repair["approved_evidence_heads"] != active_evidence_heads:
            raise LinearProjectionError(
                f"terminal_child_repair_evidence_set_changed_reload_required:"
                f"{child_id}"
            )
        contracts: list[dict[str, Any]] = []
        carried_authorities: list[dict[str, Any] | None] = []
        for expected in repair["approved_evidence_heads"]:
            event = active.get(("evidence_contract", expected["key"]))
            if (
                event is None
                or event["event_id"] != expected["event_id"]
                or canonical_digest(event["value"]) != expected["value_sha256"]
            ):
                raise LinearProjectionError(
                    f"terminal_child_repair_evidence_changed_reload_required:"
                    f"{child_id}"
                )
            contract = event["value"]
            errors = evidence_errors(contract)
            if errors or contract.get("owning_child") != child_id:
                raise LinearProjectionError(
                    f"terminal_child_repair_evidence_invalid:{child_id}:"
                    + ",".join(errors or ["wrong_owner"])
                )
            contracts.append(contract)
            try:
                carried_authorities.append(
                    carried_predecessor_evidence_authority(
                        event,
                        state.snapshot.get("projection_history") or [],
                        current_scope,
                    )
                )
            except ProjectionHistoryError as error:
                raise LinearProjectionError(str(error)) from error
            if carried_authorities[-1] is None:
                carried_authorities[-1] = (
                    validated_nonprimary_backfill_authority(
                        event, current_scope, list(state.events),
                        state.snapshot.get("projection_history") or [],
                    )
                )
        owners = {
            (contract["repository_key"], contract["exact_head"])
            for contract in contracts
        }
        if len(owners) != 1:
            raise LinearProjectionError(
                f"terminal_child_repair_owner_ambiguous:{child_id}"
            )
        owner, exact_head = next(iter(owners))
        repository = next((
            item for item in current_scope["repositories"]
            if repository_key(item) == owner
        ), None)
        historical_head_authorized = bool(carried_authorities) and all(
            authority is not None
            and authority["child_identifier"] == child_id
            and authority["repository_key"] == owner
            and authority["exact_head"] == exact_head
            for authority in carried_authorities
        )
        if repository is None or (
            repository.get("exact_head") != exact_head
            and not historical_head_authorized
        ):
            raise LinearProjectionError(
                f"terminal_child_repair_repository_head_mismatch:{child_id}"
            )
        existing_owner = current_scope["child_ownership"].get(child_id)
        if existing_owner not in {None, owner}:
            raise LinearProjectionError(
                f"terminal_child_repair_ownership_conflict:{child_id}"
            )
        desired_scope["child_ownership"][child_id] = owner
        closure = {
            "schema_version": 2,
            **readback,
            "plan_revision": scope_event["plan_revision"],
            "repository_key": owner,
            "exact_head": exact_head,
            "evidence_heads": deepcopy(repair["approved_evidence_heads"]),
            "evidence_receipts_sha256": evidence_receipts_sha256(contracts),
            "child_readback_sha256": canonical_digest(readback),
        }
        existing = active.get(("child_closure", child_id))
        if existing is not None and existing["value"] != closure:
            raise LinearProjectionError(
                f"terminal_child_repair_closure_conflict:{child_id}"
            )
        closures[child_id] = closure
        existing_closures[child_id] = existing

    desired = result["projection"]
    scope_item = next(item for item in desired if item["kind"] == "scope")
    candidate_scope = scope_item["value"]
    repair_ids = set(closures)
    candidate_base = deepcopy(candidate_scope)
    current_base = deepcopy(current_scope)
    for child_id in repair_ids:
        candidate_owner = candidate_base["child_ownership"].pop(child_id, None)
        current_base["child_ownership"].pop(child_id, None)
        if candidate_owner not in {None, closures[child_id]["repository_key"]}:
            raise LinearProjectionError("terminal_child_repair_scope_widened")
    if candidate_base != current_base:
        raise LinearProjectionError("terminal_child_repair_scope_widened")
    source_item = next(item for item in desired if item["kind"] == "source")
    current_source = active.get(("source", "root"))
    if current_source is None or source_item["value"] != current_source["value"]:
        raise LinearProjectionError("terminal_child_repair_source_changed")
    for item in desired:
        identity = (item["kind"], item["key"])
        if identity == ("scope", "root") or (
            identity[0] == "child_closure" and identity[1] in repair_ids
        ):
            continue
        current = active.get(identity)
        if current is None or current["value"] != item["value"]:
            raise LinearProjectionError(
                f"terminal_child_repair_unrelated_change:{identity[0]}:{identity[1]}"
            )
    scope_item["value"] = desired_scope
    desired[:] = [
        *[
            {"kind": "child_closure", "key": child_id,
             "value": closures[child_id]}
            for child_id in sorted(closures)
        ],
        *[
            item for item in desired
            if not (
                item["kind"] == "child_closure" and item["key"] in repair_ids
            )
        ],
    ]
    if bridge is not None and not _gen14_stable_repair_descendant(
        state, desired, bridge,
    ):
        raise LinearProjectionError(
            "terminal_child_repair_gen14_frontier_bridge_mismatch"
        )

    current_contract = projection_review_contract(state)
    if current_contract != original_contract:
        expected_heads = {
            (head["kind"], head["key"]): head
            for head in original_contract["expected_active_heads"]
        }
        added_closure_ids = {
            child_id for child_id in repair_ids
            if ("child_closure", child_id) not in expected_heads
            and ("child_closure", child_id) in active
        }
        originally_missing = [
            repair["child_identifier"].upper() for repair in repairs
            if ("child_closure", repair["child_identifier"].upper())
            not in expected_heads
        ]
        allowed_identities = set(expected_heads) | {
            ("child_closure", child_id) for child_id in added_closure_ids
        }
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
        expected_revision = original_contract["expected_projection_revision"]
        progress_events = state.events[expected_revision:]
        added_closures_match = all(
            active[("child_closure", child_id)]["value"] == closures[child_id]
            for child_id in added_closure_ids
        )
        all_closures_active = all(
            ("child_closure", child_id) in active
            and active[("child_closure", child_id)]["value"]
            == closures[child_id]
            for child_id in repair_ids
        )
        historical_scope_value = (
            historical_scope["value"] if historical_scope is not None else None
        )
        allowed_progress = (
            [
                (event["kind"], event["key"])
                for event in progress_events
            ]
            == [
                ("child_closure", child_id)
                for child_id in originally_missing[:len(added_closure_ids)]
            ] + ([("scope", "root")] if scope_changed else [])
            and added_closure_ids
            == set(originally_missing[:len(added_closure_ids)])
            and set(active) == allowed_identities
            and unchanged
            and historical_scope is not None
            and canonical_digest(historical_scope_value)
            == expected_scope["value_sha256"]
            and active_scope is not None
            and active_scope["value"] in (historical_scope_value, desired_scope)
            and (not scope_changed or all_closures_active)
            and added_closures_match
            and state.revision
            == expected_revision + len(added_closure_ids) + int(scope_changed)
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
        else:
            raise LinearProjectionError(
                "projection_review_stale_reload_required"
            )
    return result


def stable_live_readback(
    transport: LinearGraphQLTransport,
    comments: LinearCommentEventAdapter,
    token: str, *, include_description: bool = False,
    include_child_comments: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Double-collect both surfaces and refuse a mixed concurrent snapshot."""
    graph_before = transport.snapshot_for_root(
        token, include_description=include_description,
        include_child_comments=include_child_comments,
    )
    comments_before = comments.comments()
    graph_after = transport.snapshot_for_root(
        token, include_description=include_description,
        include_child_comments=include_child_comments,
    )
    comments_after = comments.comments()
    graph_fence = transport.snapshot_for_root(
        token, include_description=include_description,
        include_child_comments=include_child_comments,
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
    *, terminal_seed_head_transition: bool = False,
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
    if terminal_seed_head_transition:
        evidence = [
            item for item in remaining if item["kind"] == "evidence_contract"
        ]
        scopes = [item for item in remaining if item["kind"] == "scope"]
        disposition = [
            item for item in remaining if item["kind"] == "disposition"
        ]
        ordinary = [
            item for item in remaining
            if item["kind"] not in {"evidence_contract", "scope", "disposition"}
        ]
        return [*migration, *evidence, *ordinary, *disposition, *scopes]
    if not any(item["kind"] == "child_closure" for item in remaining):
        return [*migration, *remaining]
    closures = sorted(
        [item for item in remaining if item["kind"] == "child_closure"],
        key=lambda item: item["key"],
    )
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
    projection_history: list[dict[str, Any]],
) -> None:
    """Bind every closure creation/replacement to its reviewed live fence."""
    repairs_by_child = {
        repair["child_identifier"].upper(): repair for repair in repairs
    }
    desired_scope = next(
        (item["value"] for item in desired if item["kind"] == "scope"), None,
    )
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
        readback = {
            field: closure.get(field) for field in CHILD_READBACK_FIELDS
        }
        if canonical_digest(readback) != closure.get("child_readback_sha256"):
            raise LinearProjectionError(
                f"terminal_child_closure_readback_digest_mismatch:{child_id}"
            )
        if not isinstance(desired_scope, dict):
            raise LinearProjectionError(
                f"terminal_child_closure_scope_missing:{child_id}"
            )
        linear = desired_scope.get("linear") or {}
        if any(
            closure.get(field) != linear.get(field)
            for field in ("workspace_id", "team_id", "project_id")
        ) or closure.get("parent_issue_id") != linear.get("root_issue_id"):
            raise LinearProjectionError(
                f"terminal_child_closure_route_mismatch:{child_id}"
            )
        if desired_scope.get("child_ownership", {}).get(child_id) != closure.get(
            "repository_key"
        ):
            raise LinearProjectionError(
                f"terminal_child_closure_ownership_mismatch:{child_id}"
            )
        repository = next((
            item for item in desired_scope.get("repositories", [])
            if repository_key(item) == closure.get("repository_key")
        ), None)
        carried_head_authorized = False
        if repository is not None and repository.get("exact_head") != closure.get(
            "exact_head"
        ):
            carried_head_authorized = True
            for (kind, _key), event in active.items():
                if (
                    kind != "evidence_contract"
                    or event["value"].get("owning_child") != child_id
                ):
                    continue
                try:
                    authority = carried_predecessor_evidence_authority(
                        event, projection_history, desired_scope,
                    )
                except ProjectionHistoryError as error:
                    raise LinearProjectionError(str(error)) from error
                if authority is None:
                    authority = validated_nonprimary_backfill_authority(
                        event, desired_scope, list(active.values()),
                        projection_history,
                    )
                if (
                    authority is None
                    or authority["repository_key"] != closure.get("repository_key")
                    or authority["exact_head"] != closure.get("exact_head")
                ):
                    carried_head_authorized = False
                    break
        if repository is None or (
            repository.get("exact_head") != closure.get("exact_head")
            and not carried_head_authorized
        ):
            raise LinearProjectionError(
                f"terminal_child_closure_repository_mismatch:{child_id}"
            )
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
        active_evidence_heads.sort(
            key=lambda head: (head["key"], head["event_id"]),
        )
        if active_evidence_heads != repair["approved_evidence_heads"]:
            raise LinearProjectionError(
                f"terminal_child_closure_evidence_set_mismatch:{child_id}"
            )
        contracts: list[dict[str, Any]] = []
        for head in active_evidence_heads:
            contract = active[("evidence_contract", head["key"])]["value"]
            if (
                evidence_errors(contract)
                or contract.get("repository_key") != closure.get("repository_key")
                or contract.get("exact_head") != closure.get("exact_head")
            ):
                raise LinearProjectionError(
                    f"terminal_child_closure_evidence_invalid:{child_id}"
                )
            contracts.append(contract)
        if evidence_receipts_sha256(contracts) != closure.get(
            "evidence_receipts_sha256"
        ):
            raise LinearProjectionError(
                f"terminal_child_closure_receipts_mismatch:{child_id}"
            )


def load_material_history_for_projection_reconcile(
    snapshot: dict[str, Any], comments: list[dict[str, Any]], token: str,
    manifest: dict[str, Any], adapter: LinearProjectionAdapter, *,
    authenticated_route: dict[str, str], authenticated_source: dict[str, Any],
    remote_head: str | None = None,
    max_bytes: int = DEFAULT_RESUME_MAX_BYTES, max_items: int = 100,
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
        list(initial.snapshot.get("projection_history") or []),
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
    reviewed_changes = any(
        identity not in active or active[identity]["value"] != value
        for identity, value in desired_by_identity.items()
    ) or bool(reviewed_retirements)
    if (
        reviewed_changes
        and projection_review_contract(initial) != reviewed_contract
    ):
        raise LinearProjectionError("projection_review_stale_reload_required")
    if reviewed_changes and remote_head is None:
        raise LinearProjectionError("prospective_remote_head_required")
    if remote_head is not None:
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
            candidate = add_material_history(
                snapshot, candidate_comments, token,
                authenticated_route=authenticated_route,
                authenticated_source=authenticated_source,
                relation_target_resolver=relation_target_resolver,
                permit_stale_lifecycle_for_reconcile=(
                    authority_sensitive_changes or bool(unresolved)
                ),
            )
            if snapshot.get("dependency_graph") is not None:
                candidate["dependency_graph"] = rebind_authenticated_dependency_graph(
                    candidate, candidate_comments, snapshot["dependency_graph"],
                    authority={**authenticated_route, "root_identifier": token},
                    plan_revision=adapter.plan_revision,
                )
            return candidate

        seeds = manifest.get("terminal_child_evidence_seeds") or []
        seed_scope_transition = bool(seeds) and (
            current_scope_event is None
            or current_scope_event["value"] != desired_scope
        )
        provisional = prospective(_ordered_write_items(
            [*desired, *retirement_items], unresolved,
            terminal_seed_head_transition=seed_scope_transition,
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
            terminal_seed_head_transition=seed_scope_transition,
        )
        candidate = prospective(candidate_items)
        source_transition = manifest.get("terminal_child_source_transition")
        validation_candidate = (
            _with_validation_only_seed_closures(candidate, seeds, adapter)
            if seeds else candidate
        )
        expected_missing = frozenset(
            child["child_identifier"].upper()
            for child in (source_transition or {}).get("pending_children", [])
        )
        compact_context(
            validation_candidate, token, max_bytes=max_bytes,
            max_items=max_items, require_projection_authority=True,
            require_dependency_graph=False,
            expected_missing_terminal_closures=expected_missing,
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
    terminal_child_fence: Callable[[list[str]], dict[str, str]] | None = None,
    projection_input_fence: Callable[[], str] | None = None,
    checkpoint_fence: Callable[[], str | None] | None = None,
    projection_comments: list[dict[str, Any]] | None = None,
    projection_input_snapshot: dict[str, Any] | None = None,
    expected_projection_input_frontier: str | None = None,
    legacy_unresolved_relation_heads: frozenset[tuple[str, str]] = frozenset(),
) -> dict[str, Any]:
    """Append only missing/changed values and verify the complete current view."""
    if not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", remote_head):
        raise LinearProjectionError("verified_full_remote_head_required")
    if (
        expected_projection_input_frontier is not None
        and not re.fullmatch(
            r"[0-9a-f]{64}", expected_projection_input_frontier,
        )
    ):
        raise LinearProjectionError("invalid_projection_input_frontier")
    expected_checkpoint_id = _latest_acknowledged_checkpoint_id(snapshot)
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

    disposition = projection_disposition_value(
        snapshot, desired, remote_head=remote_head,
        workstream_id=adapter.workstream_id,
    )
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
    legacy_split = manifest.get(
        "terminal_child_evidence_seed_legacy_split_head_repair"
    )
    if observed_contract != reviewed_contract:
        if legacy_split is None:
            if not _gen14_completed_normalization_replay(
                initial, manifest, desired, created_at=created_at,
            ):
                raise LinearProjectionError("projection_review_stale_reload_required")
        else:
            _validate_gen14_legacy_split_repair_prefix(
                manifest, initial, scope_item["value"],
            )
    repair_frontier_bridge = manifest.get(
        "terminal_child_repair_gen14_frontier_bridge"
    )
    if repair_frontier_bridge is not None:
        # The bridge is an exception for the live input frontier only. Rebuild
        # every closure from the fenced child snapshot and carried evidence so
        # an already-active caller-supplied value cannot evade the ordinary
        # changed-closure check merely by matching the manifest.
        authoritative_repair = prepare_terminal_child_repairs(
            manifest, snapshot, initial,
        )
        authoritative_desired, _ = _reviewed_manifest(authoritative_repair)
        submitted_desired = [
            item for item in desired if item["kind"] != "disposition"
        ]
        if submitted_desired != authoritative_desired:
            raise LinearProjectionError(
                "terminal_child_repair_gen14_frontier_bridge_mismatch"
            )
    active_heads = _active_heads(initial)
    _require_repairs_for_changed_child_closures(
        desired, active_heads, manifest.get("terminal_child_repairs") or [],
        list(initial.snapshot.get("projection_history") or []),
    )
    repairs = manifest.get("terminal_child_repairs") or []
    seeds = manifest.get("terminal_child_evidence_seeds") or []
    seed_head_transition = (
        manifest.get("terminal_child_evidence_seed_head_transition")
        or manifest.get(
            "terminal_child_evidence_seed_legacy_split_head_repair"
        )
    )
    seed_nonprimary_backfill = manifest.get(
        "terminal_child_evidence_seed_nonprimary_backfill"
    )
    completed_split_normalization = (
        legacy_split is None
        and isinstance(
            manifest.get("terminal_child_evidence_seed_head_transition"), dict,
        )
        and manifest["terminal_child_evidence_seed_head_transition"].get(
            "created_at"
        ) == created_at
        and (
            _gen14_completed_split_migration(initial)
            or _gen14_completed_normalization_replay(
                initial, manifest, desired, created_at=created_at,
            )
        )
    )
    if legacy_split is not None and (
        created_at != legacy_split["created_at"]
        or adapter.workstream_id != "GEN-14"
        or len(initial.events) < 6
        or adapter.plan_revision != initial.events[0].get("plan_revision")
        or adapter.authority != initial.events[0].get("authority")
    ):
        raise LinearProjectionError(
            "legacy_split_head_repair_execution_contract_mismatch"
        )
    source_transition = manifest.get("terminal_child_source_transition")
    seed_predecessor = manifest.get(
        "terminal_child_evidence_seed_predecessor"
    )
    repaired_child_ids = {
        repair["child_identifier"].upper() for repair in repairs
    }
    repair_predecessor_frontiers = {
        event["value"]["predecessor_closure_authority"][
            "input_frontier_sha256"
        ]
        for (kind, _key), event in active_heads.items()
        if kind == "evidence_contract"
        and event["value"].get("owning_child") in repaired_child_ids
        and isinstance(
            event["value"].get("predecessor_closure_authority"), dict,
        )
    }
    if len(repair_predecessor_frontiers) > 1:
        raise LinearProjectionError(
            "terminal_child_repair_predecessor_frontier_ambiguous"
        )
    repair_predecessor_frontier = next(
        iter(repair_predecessor_frontiers), None,
    )
    if repair_frontier_bridge is not None and (
        adapter.workstream_id != "GEN-14"
        or adapter.plan_revision != initial.events[0].get("plan_revision")
        or adapter.authority != initial.events[0].get("authority")
        or created_at != repair_frontier_bridge["created_at"]
        or sorted(repaired_child_ids)
        != repair_frontier_bridge["child_identifiers"]
        or repair_predecessor_frontier
        != repair_frontier_bridge["stored_input_frontier_sha256"]
        or not _gen14_stable_repair_descendant(
            initial, desired, repair_frontier_bridge,
        )
    ):
        raise LinearProjectionError(
            "terminal_child_repair_gen14_frontier_bridge_mismatch"
        )
    repair_predecessor_projection_bindings = {
        canonical_digest(
            _predecessor_binding_from_carried_authority(
                event["value"]["predecessor_closure_authority"]
            )
        ): _predecessor_binding_from_carried_authority(
            event["value"]["predecessor_closure_authority"]
        )
        for (kind, _key), event in active_heads.items()
        if kind == "evidence_contract"
        and event["value"].get("owning_child") in repaired_child_ids
        and isinstance(
            event["value"].get("predecessor_closure_authority"), dict,
        )
    }
    if len(repair_predecessor_projection_bindings) > 1:
        raise LinearProjectionError(
            "terminal_child_repair_predecessor_history_ambiguous"
        )
    repair_predecessor_projection_binding = next(
        iter(repair_predecessor_projection_bindings.values()), None,
    )
    if source_transition:
        if initial.events and all(
            event["schema_version"] == 1 for event in initial.events
        ):
            raise LinearProjectionError(
                "terminal_child_source_transition_requires_v2_projection"
            )
        if prepare_terminal_child_source_transition(
            manifest, snapshot, initial,
        ) != manifest:
            raise LinearProjectionError(
                "terminal_child_source_transition_review_stale_reload_required"
            )
        if created_at != source_transition["created_at"]:
            raise LinearProjectionError(
                "terminal_child_source_transition_execution_contract_mismatch"
            )
        for item in desired:
            identity = (item["kind"], item["key"])
            current = active_heads.get(identity)
            if identity == ("source", "root"):
                continue
            if current is None or current["value"] != item["value"]:
                raise LinearProjectionError(
                    f"terminal_child_source_transition_unrelated_change:"
                    f"{identity[0]}:{identity[1]}"
                )
    if seeds:
        seed_bootstrap = (
            not active_heads
            and _exact_empty_review_contract(reviewed_contract)
        ) or _terminal_seed_bootstrap_prefix(
            reviewed_contract, initial, desired, remote_head=remote_head,
        )
        prepared_seed_manifest = prepare_terminal_child_evidence_seeds(
            manifest, projection_input_snapshot or snapshot, initial,
            remote_head=remote_head,
            comments=projection_comments,
        )
        if prepared_seed_manifest != manifest:
            prepared_body = deepcopy(prepared_seed_manifest)
            reviewed_body = deepcopy(manifest)
            for field in REVIEW_CONTRACT_FIELDS:
                prepared_body.pop(field, None)
                reviewed_body.pop(field, None)
            if (
                (legacy_split is None and not completed_split_normalization)
                or prepared_body != reviewed_body
            ):
                raise LinearProjectionError(
                    "terminal_child_evidence_seed_review_stale_reload_required"
                )
        if reviewed_retirements:
            raise LinearProjectionError(
                "terminal_child_evidence_seed_forbids_retirements"
            )
        seed_input_frontier = (
            seed_head_transition or seed_predecessor
            or seed_nonprimary_backfill or {}
        ).get("input_frontier_sha256") or repair_predecessor_frontier
        if seed_input_frontier is not None:
            if projection_input_fence is None:
                raise LinearProjectionError(
                    "terminal_child_evidence_seed_input_fence_required"
                )
            if projection_input_fence() != seed_input_frontier:
                raise LinearProjectionError(
                    "terminal_child_evidence_seed_input_frontier_changed"
                )
        seed_ids = {
            seed["child_identifier"].upper() for seed in seeds
        }
        missing = _completed_owned_missing_closures(
            snapshot, scope_item["value"], active_heads,
        )
        if missing != seed_ids:
            raise LinearProjectionError(
                "terminal_child_evidence_seed_set_incomplete:"
                + ",".join(sorted(missing ^ seed_ids))
            )
        seed_keys = {
            key for seed in seeds for key in seed["evidence_keys"]
        }
        desired_seed_keys = {
            item["key"] for item in desired
            if item["kind"] == "evidence_contract"
            and item["key"] in seed_keys
        }
        if desired_seed_keys != seed_keys:
            raise LinearProjectionError(
                "terminal_child_evidence_seed_contract_set_incomplete"
            )
        for item in desired:
            identity = (item["kind"], item["key"])
            if identity[0] == "disposition":
                current = active_heads.get(identity)
                if current is not None and current["value"] == item["value"]:
                    continue
                if seed_bootstrap:
                    continue
                current_scope = active_heads.get(("scope", "root"))
                desired_scope = scope_item["value"]
                current_primary_key = (
                    current_scope or {"value": {}}
                )["value"].get("primary_repository")
                desired_primary = next((
                    repository
                    for repository in desired_scope.get("repositories", [])
                    if repository_key(repository) == current_primary_key
                ), None)
                if (
                    seed_head_transition is None
                    or item["value"] != seed_head_transition["disposition"]
                    or desired_scope.get("primary_repository")
                    != current_primary_key
                    or desired_primary is None
                    or desired_primary.get("exact_head") != remote_head
                ):
                    raise LinearProjectionError(
                        "terminal_child_evidence_seed_disposition_changed"
                    )
                continue
            current = active_heads.get(identity)
            if identity[0] == "evidence_contract" and identity[1] in seed_keys:
                if (
                    current is not None
                    and current["value"] != item["value"]
                    and not _gen14_frontier_only_evidence_normalization(
                        current["value"], item["value"],
                        enabled=completed_split_normalization,
                    )
                ):
                    raise LinearProjectionError(
                        f"terminal_child_evidence_seed_replacement_forbidden:"
                        f"{identity[1]}"
                    )
                continue
            if identity == ("scope", "root"):
                if current is not None and current["value"] == item["value"]:
                    continue
                if seed_bootstrap:
                    continue
                exact_scope = deepcopy((current or {"value": {}})["value"])
                primary_key = exact_scope.get("primary_repository")
                primary = next((
                    repository for repository in exact_scope.get("repositories", [])
                    if repository_key(repository) == primary_key
                ), None)
                if primary is None:
                    raise LinearProjectionError(
                        "terminal_child_evidence_seed_scope_missing"
                    )
                primary["exact_head"] = remote_head
                if item["value"] != exact_scope:
                    raise LinearProjectionError(
                        "terminal_child_evidence_seed_scope_head_only_required"
                    )
                continue
            if seed_bootstrap and identity[0] in {"source", "provenance"}:
                continue
            if current is None or current["value"] != item["value"]:
                raise LinearProjectionError(
                    f"terminal_child_evidence_seed_unrelated_change:"
                    f"{identity[0]}:{identity[1]}"
                )
    if repairs:
        repaired_children = {
            repair["child_identifier"].upper() for repair in repairs
        }
        desired_scope = scope_item["value"]
        missing_terminal_children = _completed_owned_missing_closures(
            snapshot, desired_scope, active_heads,
        )
        uncovered_terminal_children = (
            missing_terminal_children - repaired_children
        )
        if uncovered_terminal_children:
            raise LinearProjectionError(
                "terminal_child_repairs_incomplete:"
                + ",".join(sorted(uncovered_terminal_children))
            )
        if reviewed_retirements:
            raise LinearProjectionError("terminal_child_repair_forbids_retirements")
        current_scope_head = active_heads.get(("scope", "root"))
        desired_scope_item = next(
            (item for item in desired if item["kind"] == "scope"), None,
        )
        if (
            current_scope_head is None
            or desired_scope_item is None
        ):
            raise LinearProjectionError("terminal_child_repair_scope_missing")
        exact_scope = deepcopy(current_scope_head["value"])
        for repair in repairs:
            child_id = repair["child_identifier"].upper()
            desired_closure_item = next((
                item for item in desired
                if (item["kind"], item["key"])
                == ("child_closure", child_id)
            ), None)
            if desired_closure_item is None:
                raise LinearProjectionError(
                    f"terminal_child_repair_closure_missing:{child_id}"
                )
            exact_scope["child_ownership"][child_id] = desired_closure_item[
                "value"
            ]["repository_key"]
        if desired_scope_item["value"] != exact_scope:
            raise LinearProjectionError("terminal_child_repair_scope_widened")
        for item in desired:
            identity = (item["kind"], item["key"])
            if item["kind"] in {"scope", "child_closure", "disposition"}:
                continue
            current = active_heads.get(identity)
            if current is None or current["value"] != item["value"]:
                raise LinearProjectionError(
                    f"terminal_child_repair_unrelated_change:"
                    f"{identity[0]}:{identity[1]}"
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

    seed_scope_transition = bool(seeds) and (
        active_heads.get(("scope", "root"), {}).get("value")
        != scope_item["value"]
    )
    write_items = _ordered_write_items(
        [*desired, *retirements], legacy_unresolved_relation_heads,
        terminal_seed_head_transition=seed_scope_transition,
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

    def fence_terminal_repairs() -> None:
        fenced_children = (
            repairs or seeds
            or ((source_transition or {}).get("pending_children") or [])
        )
        if not fenced_children:
            return
        if terminal_child_fence is None:
            raise LinearProjectionError("terminal_child_readback_fence_required")
        child_ids = [
            item["child_identifier"].upper() for item in fenced_children
        ]
        observed = terminal_child_fence(child_ids)
        if not isinstance(observed, dict) or set(observed) != set(child_ids):
            raise LinearProjectionError(
                "terminal_child_readback_fence_incomplete_reload_required"
            )
        for item in fenced_children:
            child_id = item["child_identifier"].upper()
            if observed[child_id] != item[
                "expected_child_readback_sha256"
            ]:
                raise LinearProjectionError(
                    f"terminal_child_readback_changed_reload_required:{child_id}"
                )

    def fence_projection_inputs() -> None:
        transition_frontier = (
            seed_head_transition or seed_predecessor or {}
        ).get("input_frontier_sha256") or repair_predecessor_frontier
        if repair_frontier_bridge is not None:
            transition_frontier = repair_frontier_bridge[
                "recomputed_input_frontier_sha256"
            ]
        if (
            transition_frontier is not None
            and expected_projection_input_frontier is not None
            and transition_frontier != expected_projection_input_frontier
        ):
            raise LinearProjectionError(
                "projection_input_frontier_contract_mismatch"
            )
        expected_frontier = (
            transition_frontier or expected_projection_input_frontier
        )
        if expected_frontier is None:
            return
        if projection_input_fence is None:
            raise LinearProjectionError("projection_input_fence_required")
        if projection_input_fence() != expected_frontier:
            if transition_frontier is None:
                raise LinearProjectionError(
                    "projection_input_frontier_changed_reload_required"
                )
            raise LinearProjectionError(
                "terminal_child_evidence_seed_input_frontier_changed"
            )

    def fence_predecessor_projection_history() -> None:
        binding = seed_predecessor or repair_predecessor_projection_binding
        if binding is not None:
            _fence_predecessor_projection_history(adapter.state(), binding)

    def fence_checkpoint_authority() -> None:
        if checkpoint_fence is None:
            if expected_checkpoint_id is not None:
                raise LinearProjectionError(
                    "checkpoint_authority_fence_required"
                )
            return
        if checkpoint_fence() != expected_checkpoint_id:
            raise LinearProjectionError(
                "checkpoint_authority_changed_reload_required"
            )

    fence_terminal_repairs()
    fence_projection_inputs()
    fence_predecessor_projection_history()
    fence_checkpoint_authority()

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
        if expected_projection_input_frontier is not None:
            activation_receipt = {
                **activation_receipt,
                "reviewed_projection_input_frontier_sha256": (
                    expected_projection_input_frontier
                ),
            }
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
        fence_projection_inputs()
        fence_predecessor_projection_history()
        fence_checkpoint_authority()
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
        fence_terminal_repairs()
        fence_projection_inputs()
        fence_predecessor_projection_history()
        fence_checkpoint_authority()
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
        receipt = adapter.append(
            event,
            expected_quarantine_count=observed_contract[
                "expected_projection_quarantine_count"
            ],
            expected_quarantine_sha256=observed_contract[
                "expected_projection_quarantine_sha256"
            ],
        )
        if expected_projection_input_frontier is not None:
            receipt = {
                **receipt,
                "reviewed_projection_input_frontier_sha256": (
                    expected_projection_input_frontier
                ),
            }
        receipts.append(receipt)
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
        fence_projection_inputs()
        fence_predecessor_projection_history()
        fence_checkpoint_authority()

    fence_terminal_repairs()
    fence_projection_inputs()
    fence_predecessor_projection_history()
    fence_checkpoint_authority()
    final = adapter.state()
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
    fence_projection_inputs()
    fence_predecessor_projection_history()
    fence_checkpoint_authority()
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
    result = {
        "workstream_id": adapter.workstream_id,
        "plan_revision": adapter.plan_revision,
        "projection_revision": final.revision,
        "writes": receipts,
        "disposition": disposition,
        "readback_verified": True,
        "resume_authority_verified": not bool(seeds or source_transition),
        "projection_contract": projection_review_contract(final),
    }
    if expected_projection_input_frontier is not None:
        result["projection_input_frontier"] = {
            "sha256": expected_projection_input_frontier,
            "prewrite_verified": True,
            # Linear exposes no transaction spanning child comments and the
            # root projection comment. Final product readback must therefore
            # reject any interleaving the last prewrite observation missed.
            "atomic_with_projection_append": False,
            "postwrite_verification_required": True,
        }
    return result


class ProjectionPreviewAdapter:
    """In-memory append adapter used to prove an exact projection batch.

    It starts from one authenticated live comment snapshot and implements only
    the projection methods used by ``reconcile_required_projection``.  No
    GraphQL client is retained, so preview cannot accidentally mutate Linear.
    """

    def __init__(
        self, adapter: LinearProjectionAdapter,
        comments: list[dict[str, Any]],
    ) -> None:
        self.workstream_id = adapter.workstream_id
        self.plan_revision = adapter.plan_revision
        self.workspace_id = adapter.workspace_id
        self.team_id = adapter.team_id
        self.project_id = adapter.project_id
        self.root_issue_id = adapter.root_issue_id
        self._authority = deepcopy(adapter.authority)
        self._comments = deepcopy(comments)
        self.receipts: list[dict[str, Any]] = []

    @property
    def authority(self) -> dict[str, str]:
        return deepcopy(self._authority)

    def state(self) -> Any:
        return reduce_projection_comments(
            self._comments, workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route=self.authority,
        )

    def append(self, event: dict[str, Any], **_fences: Any) -> dict[str, Any]:
        before = self.state()
        if event["expected_revision"] != before.revision:
            raise LinearProjectionError("projection_preview_revision_mismatch")
        remote_id = projection_slot_id(
            self.workstream_id, self.plan_revision, before.revision,
            self.authority,
        )
        body = encode_projection_comment(event)
        self._comments.append({
            "id": remote_id, "body": body,
            "createdAt": event["created_at"], "updatedAt": event["created_at"],
        })
        receipt = {
            "preview": True, "remote_id": remote_id,
            "event_id": event["event_id"], "event": deepcopy(event),
            "event_sha256": _value_digest(event),
        }
        self.receipts.append(receipt)
        return receipt

    def activate_v2(
        self, *, created_at: str, expected_revision: int | None = None,
        expected_legacy_event_ids: list[str] | None = None,
        expected_legacy_events_sha256: str | None = None,
    ) -> dict[str, Any] | None:
        before = self.state()
        if any(event["schema_version"] == 2 for event in before.events) \
                or not before.events:
            return None
        legacy_ids = [event["event_id"] for event in before.events]
        legacy_sha256 = _value_digest(list(before.events))
        if (
            (expected_revision is not None and before.revision != expected_revision)
            or (
                expected_legacy_event_ids is not None
                and legacy_ids != expected_legacy_event_ids
            )
            or (
                expected_legacy_events_sha256 is not None
                and legacy_sha256 != expected_legacy_events_sha256
            )
        ):
            raise LinearProjectionError(
                "projection_v2_activation_stale_reload_required"
            )
        event = build_projection_event(
            workstream_id=self.workstream_id, kind="cas_activation", key="root",
            value={
                "legacy_digest_kind": "canonical-full-events-v1",
                "legacy_event_ids": legacy_ids,
                "legacy_events_sha256": legacy_sha256,
            },
            plan_revision=self.plan_revision,
            expected_revision=before.revision, created_at=created_at,
            authority=self.authority,
        )
        return self.append(event)


def projection_preview_sha256(result: dict[str, Any]) -> str:
    """Bind the exact deterministic zero-write result reviewed before apply."""
    return _value_digest(result)


def require_matching_projection_preview(
    *, created_at: str | None, expected_sha256: str | None,
    observed_sha256: str,
) -> None:
    if (
        not isinstance(created_at, str) or not created_at
        or expected_sha256 != observed_sha256
    ):
        raise LinearProjectionError(
            "projection_apply_requires_matching_reviewed_preview"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("token")
    parser.add_argument("manifest", help="reviewed projection JSON path")
    parser.add_argument("--remote-head", required=True)
    parser.add_argument("--plan-source", required=True)
    parser.add_argument("--plan-identity")
    parser.add_argument(
        "--max-bytes", type=int, default=DEFAULT_RESUME_MAX_BYTES,
        help="maximum encoded full-resume context accepted after projection",
    )
    parser.add_argument(
        "--max-items", type=int, default=100,
        help="maximum full-resume item count accepted after projection",
    )
    parser.add_argument("--config")
    parser.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    github_auth = parser.add_mutually_exclusive_group()
    github_auth.add_argument("--github-token-command")
    github_auth.add_argument(
        "--github-token-env", choices=("GITHUB_TOKEN", "GH_TOKEN"),
    )
    parser.add_argument("--github-token-arg", action="append", default=[])
    parser.add_argument("--github-token-timeout", type=float, default=10.0)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--preview", action="store_true",
        help="validate and simulate the exact batch without any Linear write",
    )
    mode.add_argument(
        "--apply", action="store_true",
        help="apply a batch previously reviewed with --preview",
    )
    parser.add_argument(
        "--created-at",
        help="exact reviewed UTC timestamp; required for safe --apply",
    )
    parser.add_argument(
        "--expected-preview-sha256",
        help="exact preview digest required for safe --apply",
    )
    args = parser.parse_args()
    try:
        if args.apply and (not args.created_at or not args.expected_preview_sha256):
            raise LinearProjectionError(
                "projection_apply_requires_matching_reviewed_preview"
            )
        if args.github_token_arg and not args.github_token_command:
            raise LinearProjectionError("github_token_args_require_command")
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
        graph, comments = stable_live_readback(
            transport, comment_adapter, token, include_description=True,
            include_child_comments=True,
        )
        description = graph["root"].get("description")
        description_fence = canonical_source_diagnostic_fence(description)
        generation_selector_plan_revision = graph["root"].get("plan_revision")

        def dependency_reread(comment_source: Any) -> tuple[
            dict[str, Any], list[dict[str, Any]],
        ]:
            reread_graph = transport.snapshot_for_root(
                token, include_description=True, include_child_comments=True,
            )
            validate_canonical_source_readback(
                reread_graph["root"].get("description"), description_fence,
            )
            return reread_graph, comment_source.comments()

        generation_binding = projection_generation_source_binding(
            comments, workstream_id=token,
            description_plan_revision=generation_selector_plan_revision,
            requested_plan_revision=plan_revision,
            authenticated_route=route,
        )
        graph = bind_projection_plan_generation(
            graph, comments, workstream_id=token,
            requested_plan_revision=plan_revision,
            authenticated_route=route,
        )
        graph = add_live_child_material_history(
            graph, authenticated_route=route, root_comments=comments,
            proposal_plan_revision=(
                generation_binding["selected"] or {
                    "plan_revision": plan_revision,
                }
            )["plan_revision"],
        )
        projection_state = adapter.state()
        dependency_adapter = LinearChildDependencyAdapter(
            client, workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
            root_issue_id=route["root_issue_id"], root_identifier=token,
            plan_revision=plan_revision,
        )
        graph["dependency_graph"] = dependency_adapter.read_authorized_graph_for_snapshot(
            graph, comments,
            generation_selector_plan_revision=generation_selector_plan_revision,
            reread=lambda: dependency_reread(comment_adapter),
        )
        trusted_nonprimary_backfill_receipt = None
        nonprimary_backfill = manifest.get(
            "terminal_child_evidence_seed_nonprimary_backfill"
        )
        if nonprimary_backfill is not None:
            if not isinstance(nonprimary_backfill, dict):
                raise LinearProjectionError(
                    "invalid_terminal_child_evidence_seed_nonprimary_backfill"
                )
            if not args.github_token_command and not args.github_token_env:
                raise LinearProjectionError(
                    "terminal_child_evidence_seed_nonprimary_backfill_github_auth_required"
                )
            active_scope_event = _active_heads(projection_state).get(
                ("scope", "root")
            )
            repositories = (
                active_scope_event or {"value": {}}
            )["value"].get("repositories", [])
            matches = [
                repository for repository in repositories
                if repository_key(repository)
                == nonprimary_backfill.get("repository_key")
            ]
            if len(matches) != 1:
                raise LinearProjectionError(
                    "terminal_child_evidence_seed_nonprimary_backfill_repository_ambiguous"
                )
            repository = matches[0]
            coordinate = str(repository.get("slug", ""))
            coordinate = coordinate.removeprefix("https://").removeprefix(
                "github.com/"
            )
            try:
                github_token = (
                    github_token_from_command(
                        [args.github_token_command, *args.github_token_arg],
                        timeout=args.github_token_timeout,
                    )
                    if args.github_token_command
                    else os.environ.get(args.github_token_env or "", "")
                )
                trusted_nonprimary_backfill_receipt = (
                    GitHubBackfillReceiptReader(github_token).read(
                        repository=coordinate,
                        provider_repository_id=str(
                            nonprimary_backfill.get("provider_repository_id", "")
                        ),
                        pull_request_number=nonprimary_backfill.get(
                            "pull_request_number"
                        ),
                        expected_head=str(
                            nonprimary_backfill.get("to_exact_head", "")
                        ),
                        expected_merge_sha=str(
                            nonprimary_backfill.get("merge_sha", "")
                        ),
                    )
                )
            except GitHubBackfillReceiptError as error:
                raise LinearProjectionError(
                    "terminal_child_evidence_seed_nonprimary_backfill_"
                    f"{error.code}"
                ) from error
            for field in ("checks_sha256", "provider_receipt_sha256"):
                observed = nonprimary_backfill.get(field)
                authenticated = trusted_nonprimary_backfill_receipt[field]
                if observed is not None and observed != authenticated:
                    raise LinearProjectionError(
                        "terminal_child_evidence_seed_nonprimary_backfill_"
                        f"{field}_mismatch"
                    )
                nonprimary_backfill[field] = authenticated
        # A seed batch may have committed a canonical prefix before the client
        # died or lost its response. Normalize the reviewed contract through
        # the seed prefix validator before generic inactive-source sync compares
        # it with the live target frontier. Divergent prefixes still refuse.
        if manifest.get("terminal_child_evidence_seeds"):
            manifest = prepare_terminal_child_evidence_seeds(
                manifest, graph, projection_state, remote_head=args.remote_head,
                comments=comments,
                trusted_nonprimary_backfill_receipt=(
                    trusted_nonprimary_backfill_receipt
                ),
            )
        # A closure batch has the same crash-recovery requirement as an
        # evidence seed batch: its canonical prefix (including a completed
        # batch whose final response was lost) must advance the reviewed
        # contract before inactive-source synchronization compares it with the
        # live target frontier. The repair validator admits only the exact
        # ordered closure/scope prefix and rejects every unrelated drift.
        if manifest.get("terminal_child_repairs"):
            manifest = prepare_terminal_child_repairs(
                manifest, graph, projection_state,
            )
        # A source-only batch may likewise have committed its sole exact event
        # before the caller received the response.  Advance that retained
        # manifest through the full-envelope replay validator before inactive
        # candidate source synchronization compares the reviewed contract with
        # the now N+1 target frontier.  The post-sync call below remains the
        # final authenticated-source validation.
        if manifest.get("terminal_child_source_transition"):
            manifest = prepare_terminal_child_source_transition(
                manifest, graph, projection_state,
            )
        manifest, authenticated_source = synchronize_manifest_source(
            manifest, description, authenticated_source,
            projection_state.snapshot.get("source"),
            projection_state.snapshot.get("projection_history"),
            generation_binding=generation_binding,
            expected_projection_contract=projection_review_contract(
                projection_state,
            ),
        )
        manifest = prepare_terminal_child_source_transition(
            manifest, graph, projection_state,
        )
        manifest = prepare_terminal_child_evidence_seeds(
            manifest, graph, projection_state, remote_head=args.remote_head,
            comments=comments,
            trusted_nonprimary_backfill_receipt=(
                trusted_nonprimary_backfill_receipt
            ),
        )
        manifest = prepare_terminal_child_repairs(
            manifest, graph, projection_state,
        )
        projection_input_graph = deepcopy(graph)
        projection_input_graph.pop("dependency_graph", None)
        projection_input_graph["root"].pop("description", None)
        expected_projection_input_frontier = (
            projection_input_frontier_sha256(projection_input_graph, comments)
        )
        seed_head_transition = (
            manifest.get("terminal_child_evidence_seed_head_transition")
            or manifest.get(
                "terminal_child_evidence_seed_legacy_split_head_repair"
            )
        )
        seed_input_frontier = (
            seed_head_transition
            or manifest.get("terminal_child_evidence_seed_predecessor")
            or manifest.get("terminal_child_evidence_seed_nonprimary_backfill")
            or {}
        ).get("input_frontier_sha256")
        if seed_input_frontier is not None and (
            expected_projection_input_frontier != seed_input_frontier
        ):
            raise LinearProjectionError(
                "terminal_child_evidence_seed_input_frontier_changed"
            )
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

        def terminal_child_fence(
            child_identifiers: list[str],
        ) -> dict[str, str]:
            """Re-read every repaired child from one root snapshot per fence."""
            live = transport.snapshot_for_root(token)
            result: dict[str, str] = {}
            for child_identifier in child_identifiers:
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
                    result[child_identifier] = canonical_digest(
                        terminal_child_readback(matches[0])
                    )
                except ChildClosureError as error:
                    raise LinearProjectionError(str(error)) from error
            return result

        def projection_input_fence() -> str:
            live_graph, live_comments = stable_live_readback(
                transport, comment_adapter, token,
                include_child_comments=True,
            )
            live_generation_binding = projection_generation_source_binding(
                live_comments, workstream_id=token,
                description_plan_revision=live_graph["root"].get("plan_revision"),
                requested_plan_revision=plan_revision,
                authenticated_route=route,
            )
            live_graph = bind_projection_plan_generation(
                live_graph, live_comments, workstream_id=token,
                requested_plan_revision=plan_revision,
                authenticated_route=route,
            )
            live_graph = add_live_child_material_history(
                live_graph, authenticated_route=route,
                root_comments=live_comments,
                proposal_plan_revision=(
                    live_generation_binding["selected"] or {
                        "plan_revision": plan_revision,
                    }
                )["plan_revision"],
            )
            return projection_input_frontier_sha256(
                live_graph, live_comments,
            )

        def checkpoint_fence() -> str | None:
            comments_before = comment_adapter.comments()
            comments_after = comment_adapter.comments()
            if comments_before != comments_after:
                raise LinearProjectionError(
                    "checkpoint_authority_changed_during_read"
                )
            return latest_acknowledged_checkpoint_id_from_comments(
                comments_after, workstream_id=token,
                plan_revision=plan_revision,
                authenticated_route=route,
            )

        created_at = args.created_at or datetime.now(
            timezone.utc
        ).isoformat().replace("+00:00", "Z")
        preview_adapter = ProjectionPreviewAdapter(adapter, comments)
        preview_result = reconcile_required_projection(
            preview_adapter, snapshot, manifest, remote_head=args.remote_head,
            created_at=created_at,
            authenticated_source=authenticated_source,
            relation_target_resolver=resolver,
            terminal_child_fence=terminal_child_fence,
            projection_input_fence=projection_input_fence,
            checkpoint_fence=checkpoint_fence,
            projection_comments=comments,
            projection_input_snapshot=projection_input_graph,
            expected_projection_input_frontier=(
                expected_projection_input_frontier
            ),
            legacy_unresolved_relation_heads=legacy_unresolved_relation_heads,
        )
        preview = {
            "apply": False, "writes_performed": 0,
            "created_at": created_at,
            "simulated_result": preview_result,
        }
        preview_digest = projection_preview_sha256(preview)
        preview["preview_sha256"] = preview_digest
        if args.preview:
            json.dump(preview, sys.stdout, ensure_ascii=False, sort_keys=True, indent=2)
            sys.stdout.write("\n")
            return 0
        require_matching_projection_preview(
            created_at=args.created_at,
            expected_sha256=args.expected_preview_sha256,
            observed_sha256=preview_digest,
        )

        description_before_write = transport.snapshot_for_root(
            token, include_description=True,
        )["root"].get("description")
        validate_canonical_source_readback(
            description_before_write, description_fence,
        )
        result = reconcile_required_projection(
            adapter, snapshot, manifest, remote_head=args.remote_head,
            created_at=created_at,
            authenticated_source=authenticated_source,
            relation_target_resolver=resolver,
            terminal_child_fence=terminal_child_fence,
            projection_input_fence=projection_input_fence,
            checkpoint_fence=checkpoint_fence,
            projection_comments=comments,
            projection_input_snapshot=projection_input_graph,
            expected_projection_input_frontier=(
                expected_projection_input_frontier
            ),
            legacy_unresolved_relation_heads=legacy_unresolved_relation_heads,
        )
        result["canonical_description_fence"] = description_fence
        result["reviewed_preview_sha256"] = preview_digest
        # Double-collect graph and comments so a concurrent root/child/checkpoint
        # mutation cannot be certified from a mixed pre/post-write snapshot.
        final_comments = LinearCommentEventAdapter(
            client, issue_id=token, workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
        )
        graph_after, comments_after = stable_live_readback(
            transport, final_comments, token, include_description=True,
            include_child_comments=True,
        )
        validate_canonical_source_readback(
            graph_after["root"].get("description"), description_fence,
        )
        dependency_graph_after = dependency_adapter.read_authorized_graph_for_snapshot(
            graph_after, comments_after,
            generation_selector_plan_revision=generation_selector_plan_revision,
            reread=lambda: dependency_reread(final_comments),
        )
        final_generation_binding = projection_generation_source_binding(
            comments_after, workstream_id=token,
            description_plan_revision=graph_after["root"].get("plan_revision"),
            requested_plan_revision=plan_revision,
            authenticated_route=route,
        )
        graph_after = bind_projection_plan_generation(
            graph_after, comments_after, workstream_id=token,
            requested_plan_revision=plan_revision,
            authenticated_route=route,
        )
        graph_after = add_live_child_material_history(
            graph_after, authenticated_route=route,
            root_comments=comments_after,
            proposal_plan_revision=(
                final_generation_binding["selected"] or {
                    "plan_revision": plan_revision,
                }
            )["plan_revision"],
        )
        graph_after["dependency_graph"] = dependency_graph_after
        final_projection_input_graph = deepcopy(graph_after)
        final_projection_input_graph.pop("dependency_graph", None)
        final_projection_input_graph["root"].pop("description", None)
        if (
            projection_input_frontier_sha256(
                final_projection_input_graph, comments_after,
            )
            != expected_projection_input_frontier
        ):
            raise LinearProjectionError(
                "projection_input_frontier_changed_after_projection"
            )
        result["projection_input_frontier"]["postwrite_verified"] = True
        verified = add_material_history(
            graph_after, comments_after, token, authenticated_route=route,
            authenticated_source=authenticated_source,
            relation_target_resolver=lambda relations: read_relation_targets(
                client, relations,
            ),
        )
        seeds = manifest.get("terminal_child_evidence_seeds") or []
        source_transition = manifest.get("terminal_child_source_transition")
        if source_transition:
            final_transition = prepare_terminal_child_source_transition(
                manifest, graph_after, adapter.state(),
            )
            if final_transition["expected_projection_revision"] != adapter.state().revision:
                raise LinearProjectionError(
                    "terminal_child_source_transition_final_contract_mismatch"
                )
            expected_pending = frozenset(
                child["child_identifier"].upper()
                for child in source_transition["pending_children"]
            )
            context = compact_context(
                verified, token, max_bytes=args.max_bytes,
                max_items=args.max_items, require_projection_authority=True,
                require_dependency_graph=True,
                expected_missing_terminal_closures=expected_pending,
            )
            # The bounded resume gate may return a fixed-schema authority
            # envelope. Disposition uses the same already-validated snapshot,
            # not a presentation representation of it.
            choose_disposition(verified, remote_head=args.remote_head)
            result.update({
                "operation_status": "partial",
                "resume_authority": "partial_terminal_closure_required",
                "resume_authority_verified": False,
                "pending_terminal_closure": sorted(expected_pending),
                "source_transition": {
                    "from_identity": source_transition["from_identity"],
                    "to_identity": source_transition["to_identity"],
                    "sha256": source_transition["sha256"],
                    "verified": True,
                },
            })
            result["source_sync"] = {
                "identity": authenticated_source["identity"],
                "sha256": authenticated_source["sha256"],
                "resume_authority": "partial_terminal_closure_required",
            }
        elif seeds:
            expected_pending = {
                seed["child_identifier"].upper() for seed in seeds
            }
            scope_after = verified.get("scope") or {}
            closure_ids = {
                closure.get("child_identifier")
                for closure in verified.get("child_closures", [])
            }
            actual_pending = {
                str(child.get("identifier", "")).upper()
                for child in graph_after.get("children", [])
                if str(child.get("status_type", "")).lower() == "completed"
                and str(child.get("identifier", "")).upper()
                in scope_after.get("child_ownership", {})
                and str(child.get("identifier", "")).upper() not in closure_ids
            }
            if actual_pending != expected_pending:
                raise LinearProjectionError(
                    "terminal_child_evidence_seed_pending_set_mismatch:"
                    + ",".join(sorted(actual_pending ^ expected_pending))
                )
            validation_snapshot = _with_validation_only_seed_closures(
                verified, seeds, adapter,
            )
            validated_context = compact_context(
                validation_snapshot, token, max_bytes=args.max_bytes,
                max_items=args.max_items,
                require_projection_authority=True,
                require_dependency_graph=True,
            )
            choose_disposition(validation_snapshot, remote_head=args.remote_head)
            result["pending_terminal_closure"] = sorted(expected_pending)
            result["operation_status"] = "partial"
            result["resume_authority"] = "partial_terminal_closure_required"
            result["resume_authority_verified"] = False
            result["source_sync"] = {
                "identity": authenticated_source["identity"],
                "sha256": authenticated_source["sha256"],
                "resume_authority": "partial_terminal_closure_required",
            }
        else:
            context = compact_context(
                verified, token, max_bytes=args.max_bytes,
                max_items=args.max_items,
                require_projection_authority=True,
                require_dependency_graph=True,
            )
            choose_disposition(verified, remote_head=args.remote_head)
            result["source_sync"] = {
                "identity": authenticated_source["identity"],
                "sha256": authenticated_source["sha256"],
                "resume_authority": context["resume_authority"],
            }
            result["operation_status"] = "complete"
            result["resume_authority"] = "full"
            result["resume_authority_verified"] = True
        json.dump(result, sys.stdout, sort_keys=True, indent=2)
        sys.stdout.write("\n")
        return 0
    except (OSError, json.JSONDecodeError, ChildDependencyError,
            LinearProjectionError, LinearTransportError, ResumeError,
            SuccessorError, ValueError) as error:
        print(f"workstream projection refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
