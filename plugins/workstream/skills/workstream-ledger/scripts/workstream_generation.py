#!/usr/bin/env python3
"""Bootstrap or advance append-only workstream plan-generation authority.

The command never updates a Linear issue. It reserves the shared material /
checkpoint boundary, seals and strictly revalidates a candidate, prepares an
inert transition, then changes authority with a separate deterministic
finalization. A lost response is recovered by complete readback; exact
historical replay is always zero-write.
"""

from __future__ import annotations

import argparse
import base64
from copy import deepcopy
import hashlib
import hmac
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import uuid
from typing import Any, Callable

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_linear import (
    bootstrap_linear_route, HttpGraphQLClient, LinearGraphQLTransport,
    LinearTransportError,
)
from workstream_linear_events import (
    COMMENT_CREATE_CAPABILITY_QUERY, COMMENT_CREATE_MUTATION,
    assert_no_pending_ledger_reservation, ledger_boundary_slot_id,
    ledger_serialization_frontier, reduce_event_comments,
)
from workstream_linear_checkpoints import reduce_checkpoint_comments
from workstream_linear_checkpoints import encode_checkpoint_comment
from workstream_linear_projection import (
    _canonical, _generation_frontier, build_projection_event,
    encode_projection_comment, LinearProjectionAdapter, PROJECTION_PREFIX,
    PROJECTION_RE, projection_slot_id, reduce_projection_comments,
    select_plan_generation, decode_projection_receipt,
)
from workstream_plan import plan_payload
from workstream_resume import (
    DEFAULT_RESUME_MAX_BYTES, add_child_material_history, add_material_history,
    apply_generation_execution_status, compact_context,
    read_relation_targets,
)
from workstream_checkpoint import (
    acknowledge_checkpoint, recover_latest, validate_checkpoint,
)
from workstream_successor import choose_disposition
from workstream_child_dependencies import (
    LinearChildDependencyAdapter, rebind_authenticated_dependency_graph,
)
from workstream_child_closure import canonical_digest, terminal_child_readback
from workstream_scope import repository_key


RESERVATION_PREFIX = "<!-- workstream-generation-reservation:v2:"
RESERVATION_RE = re.compile(
    r"<!-- workstream-generation-reservation:v2:([A-Za-z0-9_-]+) -->"
)
HEX64 = re.compile(r"[0-9a-f]{64}")
EVENT_ID = re.compile(r"wsp_[0-9a-f]{32}")
RESERVATION_ID = re.compile(r"wsgr_[0-9a-f]{32}")
FINALIZATION_PREFIX = "<!-- workstream-generation-finalization:v1:"
FINALIZATION_RE = re.compile(
    r"<!-- workstream-generation-finalization:v1:([A-Za-z0-9_-]+) -->"
)
FINALIZATION_ID = re.compile(r"wsgf_[0-9a-f]{32}")
CHECKPOINT_CUSTODY_PREFIX = "<!-- workstream-generation-checkpoint-custody:v1:"
CHECKPOINT_CUSTODY_RE = re.compile(
    r"<!-- workstream-generation-checkpoint-custody:v1:([A-Za-z0-9_-]+) -->"
)
CLOCK_CUSTODY_PREFIX = "<!-- workstream-generation-graph-clock-custody:v1:"
CLOCK_CUSTODY_RE = re.compile(
    r"<!-- workstream-generation-graph-clock-custody:v1:([A-Za-z0-9_-]+) -->"
)
PREPARE_STARTED_STATE_QUERY = """
query WorkstreamGenerationPrepareState($teamId: String!, $stateId: String!) {
  team(id: $teamId) { id organization { id } }
  workflowState(id: $stateId) { id name type team { id } }
}
"""

# Agent Workstream 0.4.51 wrote this one inactive GEN-14 candidate with the
# disposition and scope taken from different authenticated Shipyard heads.
# Keep the escape hatch content-addressed to that exact six-event prefix; this
# is a migration receipt, not permission to reconcile arbitrary split heads.
GEN14_LEGACY_SPLIT_PREFIX_SHA256 = (
    "180e178d1732b914edce564ba1d6411e229ceaaecd48cbcb5422de2736d56c28"
)
GEN14_LEGACY_SPLIT_STORED_FRONTIER_SHA256 = (
    "e0317b7cda88262a7baf0df28b13c4c27af8a4e171156147f304f62e104cfc23"
)
GEN14_LEGACY_SPLIT_RECOMPUTED_FRONTIER_SHA256 = (
    "7c53170bb0ba8434182809f23f40f893b279f3b88a14face18326fd737b41240"
)


class WorkstreamGenerationError(LinearTransportError):
    """Generation authority cannot be changed without guessing."""


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _gen14_split_prefix_context(
    target: Any, *, workstream_id: str, target_plan: str,
    input_frontier_sha256: str,
) -> dict[str, Any] | None:
    """Authenticate the one private prefix through one public opaque digest."""
    if workstream_id != "GEN-14" or target.revision < 6:
        return None
    prefix = list(target.events[:6])
    if len(prefix) != 6 or not hmac.compare_digest(
        canonical_digest(prefix), GEN14_LEGACY_SPLIT_PREFIX_SHA256,
    ):
        return None
    plan = prefix[0].get("plan_revision")
    authority = prefix[0].get("authority")
    created_at = prefix[0].get("created_at")
    identities = [(event.get("kind"), event.get("key")) for event in prefix]
    frontiers = {
        event.get("value", {}).get("predecessor_closure_authority", {}).get(
            "input_frontier_sha256"
        )
        for event in prefix[:2]
    }
    scope = prefix[5].get("value", {})
    primary_key = scope.get("primary_repository")
    primary = [
        repository for repository in scope.get("repositories", [])
        if repository_key(repository) == primary_key
    ]
    disposition = prefix[4].get("value", {})
    captured_frontier_migration = (
        frontiers == {GEN14_LEGACY_SPLIT_STORED_FRONTIER_SHA256}
        and hmac.compare_digest(
            input_frontier_sha256,
            GEN14_LEGACY_SPLIT_RECOMPUTED_FRONTIER_SHA256,
        )
    )
    if (
        target_plan != plan
        or identities[:4] != [
            ("evidence_contract", identities[0][1]),
            ("evidence_contract", identities[1][1]),
            ("provenance", identities[2][1]), ("source", "root"),
        ]
        or identities[4:] != [("disposition", "root"), ("scope", "root")]
        or not isinstance(created_at, str) or not created_at
        or len(primary) != 1 or len(frontiers) != 1
        or not captured_frontier_migration
        or disposition != {
            "disposition": "create_successor",
            "remote_head": disposition.get("remote_head"),
            "recovered_from_checkpoint": None,
        }
        or primary[0].get("exact_head") == disposition.get("remote_head")
        or not all(
            event.get("schema_version") == 2
            and event.get("expected_revision") == index
            and event.get("workstream_id") == workstream_id
            and event.get("plan_revision") == plan
            and event.get("authority") == authority
            and event.get("created_at") == created_at
            and event.get("supersedes_event_id") is None
            for index, event in enumerate(prefix)
        )
    ):
        return None
    return {
        "prefix": prefix, "plan_revision": plan, "authority": authority,
        "scope_head": primary[0]["exact_head"],
        "disposition_head": disposition["remote_head"],
    }


def _gen14_legacy_split_head_prefix(
    target: Any, *, workstream_id: str, target_plan: str,
    input_frontier_sha256: str, remote_head: str,
) -> bool:
    context = _gen14_split_prefix_context(
        target, workstream_id=workstream_id, target_plan=target_plan,
        input_frontier_sha256=input_frontier_sha256,
    )
    if context is None or remote_head in {
        context["scope_head"], context["disposition_head"],
    }:
        return False
    if target.revision == 6:
        return True
    prefix = context["prefix"]
    repair_created_at = target.events[6].get("created_at")
    desired_scope = deepcopy(prefix[5]["value"])
    next(repository for repository in desired_scope["repositories"]
         if repository_key(repository) == desired_scope["primary_repository"]
         )["exact_head"] = remote_head
    expected_tail = [
        build_projection_event(
            workstream_id=workstream_id, kind="disposition", key="root",
            value={"disposition": "create_successor", "remote_head": remote_head,
                   "recovered_from_checkpoint": None},
            plan_revision=target_plan, expected_revision=6,
            created_at=repair_created_at, supersedes_event_id=prefix[4]["event_id"],
            authority=context["authority"],
        ),
        build_projection_event(
            workstream_id=workstream_id, kind="scope", key="root",
            value=desired_scope, plan_revision=target_plan, expected_revision=7,
            created_at=repair_created_at, supersedes_event_id=prefix[5]["event_id"],
            authority=context["authority"],
        ),
    ]
    required_tail = min(target.revision - 6, 2)
    return list(target.events[6:6 + required_tail]) == expected_tail[:required_tail]


def _gen14_recorded_repair_head(
    target: Any, requested_head: str, *, workstream_id: str,
    target_plan: str, input_frontier_sha256: str,
) -> str:
    """Finish exact D6 at recorded C before considering a newer main."""
    if target.revision not in {7, 8}:
        return requested_head
    recorded = target.events[6].get("value", {}).get("remote_head")
    if not isinstance(recorded, str) or not _gen14_legacy_split_head_prefix(
        target, workstream_id=workstream_id, target_plan=target_plan,
        input_frontier_sha256=input_frontier_sha256, remote_head=recorded,
    ):
        return requested_head
    return recorded


def _envelope(prefix: str, payload_name: str, payload: dict[str, Any]) -> str:
    material = _canonical(payload)
    encoded = base64.urlsafe_b64encode(_canonical({
        payload_name: payload, "sha256": hashlib.sha256(material).hexdigest(),
    })).decode("ascii").rstrip("=")
    return f"{prefix}{encoded} -->"


def _decode_envelope(
    body: str, *, prefix: str, pattern: re.Pattern[str], payload_name: str,
) -> dict[str, Any]:
    matches = pattern.findall(body)
    if len(matches) != 1 or body.count(prefix) != 1:
        raise WorkstreamGenerationError(f"malformed_generation_{payload_name}")
    try:
        encoded = matches[0]
        envelope = json.loads(base64.urlsafe_b64decode(
            encoded + "=" * (-len(encoded) % 4)
        ))
        if set(envelope) != {payload_name, "sha256"}:
            raise ValueError("unexpected envelope")
        payload = envelope[payload_name]
        if not isinstance(payload, dict) or not hmac.compare_digest(
            str(envelope["sha256"]), _digest(payload),
        ):
            raise ValueError("digest mismatch")
        return payload
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise WorkstreamGenerationError(
            f"malformed_generation_{payload_name}"
        ) from error


def _validate_retirement(value: dict[str, Any], predecessor: str, epoch: int) -> None:
    fields = {
        "predecessor_plan_revision", "retired_at", "retired_writer_epoch",
        "provenance_event_ids", "checkpoint_event_ids", "declaration_sha256",
    }
    schema_version = value.get("schema_version") if isinstance(value, dict) else None
    if schema_version == 2:
        fields.update({"schema_version", "authenticated_quiescence"})
    if not isinstance(value, dict) or set(value) != fields:
        raise WorkstreamGenerationError("invalid_generation_retirement_proof")
    unsigned = {key: deepcopy(item) for key, item in value.items()
                if key != "declaration_sha256"}
    if (
        value["predecessor_plan_revision"] != predecessor
        or value["retired_writer_epoch"] != epoch
        or not isinstance(value["retired_at"], str) or not value["retired_at"]
        or any(
            not isinstance(value[field], list)
            or value[field] != sorted(set(value[field]))
            or not all(isinstance(item, str) and item for item in value[field])
            for field in ("provenance_event_ids", "checkpoint_event_ids")
        )
        or value["declaration_sha256"] != _digest(unsigned)
        or (
            schema_version == 2
            and (
                not isinstance(value.get("authenticated_quiescence"), dict)
                or value["retired_at"]
                != value["authenticated_quiescence"].get("observed_at")
            )
        )
    ):
        raise WorkstreamGenerationError("invalid_generation_retirement_proof")


def build_retirement_proof(
    *, predecessor_plan_revision: str, retired_at: str, retired_writer_epoch: int,
    provenance_event_ids: list[str], checkpoint_event_ids: list[str],
) -> dict[str, Any]:
    proof = {
        "predecessor_plan_revision": predecessor_plan_revision,
        "retired_at": retired_at,
        "retired_writer_epoch": retired_writer_epoch,
        "provenance_event_ids": sorted(set(provenance_event_ids)),
        "checkpoint_event_ids": sorted(set(checkpoint_event_ids)),
    }
    return {**proof, "declaration_sha256": _digest(proof)}


def build_authenticated_retirement_proof(
    *, predecessor_plan_revision: str, observed_at: str,
    retired_writer_epoch: int, provenance_event_ids: list[str],
    checkpoint_event_ids: list[str], authority: dict[str, str],
    selected_generation: dict[str, Any], material: Any,
    predecessor_projection_contract: dict[str, Any],
) -> dict[str, Any]:
    """Bind retirement to authenticated frontiers later serialized by activation."""
    quiescence = {
        "schema_version": 1,
        "observed_at": observed_at,
        "authenticated_route": deepcopy(authority),
        "selected_generation": {
            "plan_revision": selected_generation["plan_revision"],
            "activation_epoch": selected_generation["activation_epoch"],
            "transition_tip_event_id": selected_generation[
                "transition_tip_event_id"
            ],
        },
        "material_revision": material.revision,
        "material_event_ids": sorted(event.event_id for event in material.events),
        "checkpoint_event_ids": sorted(checkpoint_event_ids),
        "predecessor_projection": deepcopy(predecessor_projection_contract),
        "ordering": (
            "activation_reservation_must_follow_exact_frontiers_and_blocks_"
            "upgraded_predecessor_writers"
        ),
    }
    proof = {
        "schema_version": 2,
        "predecessor_plan_revision": predecessor_plan_revision,
        "retired_at": observed_at,
        "retired_writer_epoch": retired_writer_epoch,
        "provenance_event_ids": sorted(set(provenance_event_ids)),
        "checkpoint_event_ids": sorted(set(checkpoint_event_ids)),
        "authenticated_quiescence": quiescence,
    }
    return {**proof, "declaration_sha256": _digest(proof)}


def prepare_generation_operator_contract(
    *, comments: list[dict[str, Any]], graph: dict[str, Any],
    workstream_id: str, authority: dict[str, str],
    description_plan_revision: str | None, target_source: dict[str, str],
    created_at: str, remote_head: str, started_state: dict[str, str],
) -> dict[str, Any]:
    """Build the complete zero-write review contract for a new generation.

    This intentionally produces a first projection phase rather than pretending
    that predecessor terminal closures can simply be copied into an inactive
    generation.  Every predecessor head is classified as carried, staged, or
    computed; an unclassified key is a protocol error.
    """
    from workstream_projection import (
        _active_heads, _contract_from_heads,
        _gen14_stable_repair_descendant, _reviewed_manifest,
        _value_digest, prepare_terminal_child_evidence_seeds,
        prepare_terminal_child_repairs,
        prepare_terminal_child_source_transition, projection_review_contract,
        LinearProjectionError,
        terminal_child_evidence_seed_predecessor_contract,
        bind_projection_plan_generation, projection_disposition_value,
    )

    if (
        not isinstance(created_at, str) or not created_at
        or set(target_source) != {"identity", "sha256"}
        or not isinstance(target_source["identity"], str)
        or not target_source["identity"]
        or not HEX64.fullmatch(str(target_source["sha256"]))
        or not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", remote_head)
        or set(started_state) != {"id", "name", "type", "team_id"}
        or not isinstance(started_state["id"], str) or not started_state["id"]
        or not isinstance(started_state["name"], str) or not started_state["name"]
        or str(started_state["type"]).lower() != "started"
        or started_state["team_id"] != authority.get("team_id")
    ):
        raise WorkstreamGenerationError(
            "generation_prepare_exact_source_and_timestamps_required"
        )
    selected = select_plan_generation(
        comments, workstream_id=workstream_id,
        description_plan_revision=description_plan_revision,
        authenticated_route=authority,
    )
    predecessor_plan = selected["plan_revision"]
    target_plan = target_source["sha256"]
    if predecessor_plan == target_plan:
        raise WorkstreamGenerationError("generation_target_already_active")
    epoch = (
        selected["activation_epoch"]
        if selected["activation_epoch"] is not None else -1
    ) + 1
    predecessor = reduce_projection_comments(
        comments, workstream_id=workstream_id,
        expected_plan_revision=predecessor_plan,
        authenticated_route=authority,
    )
    target = reduce_projection_comments(
        comments, workstream_id=workstream_id,
        expected_plan_revision=target_plan,
        authenticated_route=authority,
    )
    # Generation controls belong to the authority chain, not the portable
    # workstream projection.  Their exact history is already bound by the
    # selected generation and projection frontier; copying a predecessor seal
    # into a new plan would both forge its embedded plan binding and make a
    # second reviewed migration impossible.
    generation_control_kinds = {
        "generation_genesis", "generation_candidate_seal",
        "generation_transition", "generation_abort",
    }
    predecessor_heads = {
        identity: event for identity, event in _active_heads(predecessor).items()
        if identity[0] not in generation_control_kinds
    }
    target_heads = {
        identity: event for identity, event in _active_heads(target).items()
        if identity[0] not in generation_control_kinds
    }
    target_disposition_event = target_heads.get(("disposition", "root"))
    target_disposition = (
        target_disposition_event.get("value", {})
        if target_disposition_event is not None else {}
    )
    required = {("scope", "root"), ("source", "root")}
    missing = sorted(required - set(predecessor_heads))
    if missing:
        raise WorkstreamGenerationError(
            "generation_prepare_predecessor_projection_incomplete:"
            + ",".join(f"{kind}:{key}" for kind, key in missing)
        )

    checkpoints = reduce_generation_checkpoint_comments(
        comments, workstream_id=workstream_id,
        authenticated_route=authority,
    )
    provenance_ids = sorted(
        event["event_id"] for (kind, _key), event in predecessor_heads.items()
        if kind == "provenance"
    )
    checkpoint_ids = sorted(
        item["event_id"] for item in checkpoints.checkpoints
        if item["plan_revision"] == predecessor_plan
    )
    material = reduce_event_comments(comments, workstream_id=workstream_id)
    retirement = build_authenticated_retirement_proof(
        predecessor_plan_revision=predecessor_plan,
        observed_at=created_at, retired_writer_epoch=epoch,
        provenance_event_ids=provenance_ids,
        checkpoint_event_ids=checkpoint_ids,
        authority=authority, selected_generation=selected, material=material,
        predecessor_projection_contract=projection_review_contract(predecessor),
    )

    closure_heads = {
        key: event for (kind, key), event in predecessor_heads.items()
        if kind == "child_closure"
    }
    terminal_evidence: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for (kind, key), event in predecessor_heads.items():
        if kind != "evidence_contract":
            continue
        owner = str(event["value"].get("owning_child", "")).upper()
        if owner in closure_heads:
            terminal_evidence.setdefault(owner, []).append((key, event))
    if set(closure_heads) != set(terminal_evidence):
        incomplete = sorted(set(closure_heads) - set(terminal_evidence))
        raise WorkstreamGenerationError(
            "generation_prepare_terminal_evidence_incomplete:"
            + ",".join(incomplete)
        )

    complete_items: list[dict[str, Any]] = []
    carried: list[dict[str, str]] = []
    staged: list[dict[str, Any]] = []
    computed: list[dict[str, str]] = []
    terminal_evidence_keys = {
        key for values in terminal_evidence.values() for key, _event in values
    }
    for (kind, key), event in sorted(predecessor_heads.items()):
        identity = {"kind": kind, "key": key, "event_id": event["event_id"]}
        if (kind, key) == ("scope", "root"):
            value = deepcopy(event["value"])
            primary_key = value.get("primary_repository")
            primary_repositories = [
                repository for repository in value.get("repositories", [])
                if repository_key(repository) == primary_key
            ]
            if len(primary_repositories) != 1:
                raise WorkstreamGenerationError(
                    "generation_prepare_primary_repository_ambiguous"
                )
            predecessor_head = primary_repositories[0].get("exact_head")
            primary_repositories[0]["exact_head"] = remote_head
            complete_items.append({"kind": kind, "key": key, "value": value})
            carried.append({
                **identity,
                "mode": (
                    "exact_value_copy"
                    if predecessor_head == remote_head
                    else "primary_head_rebound_to_verified_remote_head"
                ),
            })
        elif (kind, key) == ("source", "root"):
            complete_items.append({"kind": kind, "key": key,
                                   "value": deepcopy(target_source)})
            carried.append({**identity, "mode": "replaced_by_exact_target_source"})
        elif kind == "disposition":
            computed.append({**identity, "mode": "computed_from_verified_remote_head"})
        elif kind == "child_closure":
            staged.append({**identity, "phase": "terminal_child_closure_repair"})
        elif kind == "evidence_contract":
            value = deepcopy(event["value"])
            value["plan_revision"] = target_plan
            value.pop("predecessor_closure_authority", None)
            complete_items.append({"kind": kind, "key": key, "value": value})
            carried.append({
                **identity,
                "mode": (
                    "predecessor_terminal_evidence_seed"
                    if key in terminal_evidence_keys else "plan_rebound_exact_value_copy"
                ),
            })
        else:
            complete_items.append({"kind": kind, "key": key,
                                   "value": deepcopy(event["value"])})
            carried.append({**identity, "mode": "exact_value_copy"})

    def target_disposition_matches(
        projection: list[dict[str, Any]],
    ) -> bool:
        """Derive disposition only once the target projection can activate.

        Earlier phases may intentionally expose multiple predecessor worktree
        authorities.  They do not need a target disposition yet, and asking
        the disposition reducer to choose one would turn a valid seed preview
        into an unrelated ambiguity refusal.
        """
        return target_disposition == projection_disposition_value(
            graph, projection, remote_head=remote_head,
            workstream_id=workstream_id,
        )

    terminal_stage: dict[str, Any] | None = None
    phase = "complete_projection"
    effective_remote_head = remote_head
    manifest: dict[str, Any]

    def finish(
        reviewed_manifest: dict[str, Any], reviewed_phase: str,
        reviewed_terminal_stage: dict[str, Any] | None,
        *, reviewed_remote_head: str = remote_head,
    ) -> dict[str, Any]:
        _reviewed_manifest(reviewed_manifest)
        phase_source = next(
            item["value"] for item in reviewed_manifest["projection"]
            if (item["kind"], item["key"]) == ("source", "root")
        )
        classified = {
            (item["kind"], item["key"])
            for item in [*carried, *staged, *computed]
        }
        if classified != set(predecessor_heads):
            omitted = sorted(set(predecessor_heads) - classified)
            extra = sorted(classified - set(predecessor_heads))
            raise WorkstreamGenerationError(
                "generation_prepare_active_key_classification_incomplete:"
                f"omitted={omitted}:extra={extra}"
            )
        contract = {
            "schema_version": 1,
            "workstream_id": workstream_id,
            "created_at": created_at,
            "authenticated_route": deepcopy(authority),
            "source": deepcopy(target_source),
            "native_transition": {
                "operation": "reopen",
                "target_state": deepcopy(started_state),
            },
            "remote_head": remote_head,
            "generation": {
                "from_plan_revision": predecessor_plan,
                "target_plan_revision": target_plan,
                "activation_epoch": epoch,
                "previous_control_event_id": selected["transition_tip_event_id"],
            },
            "frontiers": {
                "material_revision": material.revision,
                "predecessor_projection": projection_review_contract(predecessor),
                "target_projection": projection_review_contract(target),
                "predecessor_checkpoint_event_ids": checkpoint_ids,
            },
            "retirement_proof": retirement,
            "projection_preview": {
                "apply": False,
                "writes_performed": 0,
                "invocation": {
                    "remote_head": reviewed_remote_head,
                    "created_at": reviewed_manifest.get(
                        "terminal_child_evidence_seed_legacy_split_head_repair",
                        {},
                    ).get("created_at", created_at),
                    "source": deepcopy(phase_source),
                },
                "manifest": reviewed_manifest,
                "active_key_accounting": {
                    "carried": carried, "staged": staged, "computed": computed,
                },
                "terminal_child_stage": reviewed_terminal_stage,
                "phase": reviewed_phase,
                "next_gate": {
                    "terminal_source_transition": (
                        "review_and_apply_terminal_source_transition"
                    ),
                    "terminal_evidence_seed": (
                        "review_and_apply_terminal_evidence_seed"
                    ),
                    "terminal_closure_repair": (
                        "review_and_apply_terminal_closure_repair"
                    ),
                    "complete_projection": "review_and_apply_target_projection",
                    "activation_ready": "preview_generation_activation",
                }[reviewed_phase],
            },
        }
        return {**contract, "contract_sha256": _digest(contract)}

    if closure_heads:
        # Generation prepare and projection preview must bind terminal carry to
        # the same canonical candidate graph.  The transport snapshot still
        # names the description-selected predecessor and contains raw child
        # comment collections; projection preview first binds the requested
        # generation and reduces those child logs.  Computing the predecessor
        # binding from the raw snapshot therefore made a deterministic prepare
        # output impossible for its consumer to accept.
        if "child_comments" in graph:
            graph = bind_projection_plan_generation(
                graph, comments, workstream_id=workstream_id,
                requested_plan_revision=target_plan,
                authenticated_route=authority,
            )
            graph = add_child_material_history(
                graph, graph["child_comments"],
                authenticated_route=authority, root_comments=comments,
                proposal_plan_revision=predecessor_plan,
            )
        seed_items = [
            deepcopy(item) for item in complete_items
            if item["kind"] in {"scope", "source", "provenance"}
            or (
                item["kind"] == "evidence_contract"
                and item["key"] in terminal_evidence_keys
            )
        ]
        scope = next(
            item["value"] for item in seed_items
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        seeds: list[dict[str, Any]] = []
        for child_id in sorted(closure_heads):
            matches = [
                child for child in graph.get("children", [])
                if str(child.get("identifier", "")).upper() == child_id
            ]
            if len(matches) != 1:
                raise WorkstreamGenerationError(
                    f"generation_prepare_terminal_child_ambiguous:{child_id}"
                )
            readback = terminal_child_readback(matches[0])
            seeds.append({
                "child_identifier": child_id,
                "child_issue_id": readback["child_issue_id"],
                "expected_child_readback_sha256": canonical_digest(readback),
                "expected_assignee_id": readback["assignee_id"],
                "evidence_keys": sorted(
                    key for key, _event in terminal_evidence[child_id]
                ),
            })
        current_source = target_heads.get(("source", "root"))

        def source_transition_projection() -> list[dict[str, Any]]:
            if current_source is None:
                raise WorkstreamGenerationError(
                    "generation_prepare_target_source_missing"
                )
            projection = [
                {
                    "kind": kind, "key": key,
                    "value": deepcopy(event["value"]),
                }
                for (kind, key), event in sorted(target_heads.items())
                if kind != "disposition"
            ]
            source_item = next((
                item for item in projection
                if (item["kind"], item["key"]) == ("source", "root")
            ), None)
            if source_item is None:
                raise WorkstreamGenerationError(
                    "generation_prepare_target_source_missing"
                )
            source_item["value"] = deepcopy(target_source)
            return projection

        def stage_source_transition() -> dict[str, Any]:
            source_projection = source_transition_projection()
            source_manifest = {
                **projection_review_contract(target),
                "projection": source_projection,
                "retirements": [],
                "terminal_child_source_transition": {
                    "from_identity": current_source["value"].get("identity"),
                    "to_identity": target_source["identity"],
                    "sha256": target_plan,
                    "created_at": created_at,
                    "expected_revision": target.revision,
                    "from_event_id": current_source["event_id"],
                    "from_value_sha256": canonical_digest(
                        current_source["value"]
                    ),
                    "pending_children": [
                        {
                            key: deepcopy(value)
                            for key, value in seed.items()
                            if key != "evidence_keys"
                        }
                        for seed in seeds
                    ],
                },
            }
            try:
                source_manifest = prepare_terminal_child_source_transition(
                    source_manifest, graph, target,
                )
            except LinearProjectionError as error:
                raise WorkstreamGenerationError(str(error)) from error
            return finish(
                source_manifest, "terminal_source_transition", {
                    "state": "terminal_source_transition_required",
                    "children": [
                        seed["child_identifier"] for seed in seeds
                    ],
                },
            )

        source_transition_required = (
            current_source is not None
            and current_source["value"] != target_source
        )
        if source_transition_required:
            # Validate the current candidate as an accepted generation prefix
            # before authorizing any durable source-only phase.  Seed validation
            # must therefore review the currently active same-digest locator;
            # only the separately fenced source-transition manifest may propose
            # the authenticated replacement locator.
            next(
                item for item in seed_items
                if (item["kind"], item["key"]) == ("source", "root")
            )["value"] = deepcopy(current_source["value"])
        desired_contracts = {
            item["key"]: item["value"] for item in seed_items
            if item["kind"] == "evidence_contract"
        }
        binding, _authorities = terminal_child_evidence_seed_predecessor_contract(
            graph, target, comments, workstream_id=workstream_id,
            predecessor_plan_revision=predecessor_plan,
            desired_scope=scope, seeds=seeds,
            desired_contracts=desired_contracts,
        )
        recorded_repair_head = _gen14_recorded_repair_head(
            target, remote_head, workstream_id=workstream_id,
            target_plan=target_plan,
            input_frontier_sha256=binding["input_frontier_sha256"],
        )
        if recorded_repair_head == remote_head:
            recorded_repair_head = None
        seed_remote_head = (
            recorded_repair_head
            if target.revision == 7 and isinstance(recorded_repair_head, str)
            else remote_head
        )
        if seed_remote_head != remote_head:
            effective_remote_head = seed_remote_head
            seed_primary = next(
                repository for repository in scope["repositories"]
                if repository_key(repository) == scope["primary_repository"]
            )
            seed_primary["exact_head"] = seed_remote_head
        empty_contract = _contract_from_heads(
            0, {}, legacy_event_ids=[], legacy_events_sha256=None,
            quarantine_count=0, quarantine_sha256=_value_digest([]),
        )
        seed_identities = {
            (item["kind"], item["key"]) for item in seed_items
        } | {("disposition", "root")}
        seed_events = tuple(
            event for event in target.events
            if (event["kind"], event["key"]) in seed_identities
        )
        seed_target = SimpleNamespace(
            revision=len(seed_events), events=seed_events,
            remote_ids=getattr(target, "remote_ids", {}),
            snapshot=deepcopy(target.snapshot),
        )
        current_seed_scope = target_heads.get(("scope", "root"))
        seed_contract = (
            projection_review_contract(seed_target)
            if current_seed_scope is not None else empty_contract
        )
        seed_manifest = {
            **seed_contract, "projection": seed_items, "retirements": [],
            "terminal_child_evidence_seeds": seeds,
            "terminal_child_evidence_seed_predecessor": binding,
        }
        legacy_candidate = (
            workstream_id == "GEN-14" and target.revision >= 6
            and hmac.compare_digest(
                canonical_digest(list(target.events[:6])),
                GEN14_LEGACY_SPLIT_PREFIX_SHA256,
            )
        )
        durable_repair_head = (
            target.events[6].get("value", {}).get("remote_head")
            if target.revision >= 7 else remote_head
        )
        legacy_prefix = _gen14_legacy_split_head_prefix(
            target, workstream_id=workstream_id, target_plan=target_plan,
            input_frontier_sha256=binding["input_frontier_sha256"],
            remote_head=durable_repair_head,
        )
        if legacy_prefix:
            # The one content-addressed legacy prefix already contains the
            # reviewed predecessor authority on its evidence contracts.  Keep
            # those exact active values; the ordinary seed path starts from
            # the predecessor and injects equivalent authority later, which
            # would make this replay look like an evidence replacement.
            for item in seed_items:
                identity = (item["kind"], item["key"])
                if item["kind"] != "evidence_contract":
                    continue
                current = target_heads.get(identity)
                if current is None:
                    raise WorkstreamGenerationError(
                        "generation_prepare_legacy_split_head_evidence_missing"
                    )
                item["value"] = deepcopy(current["value"])
        if legacy_candidate and not legacy_prefix:
            captured_frontiers = {
                event.get("value", {}).get(
                    "predecessor_closure_authority", {}
                ).get("input_frontier_sha256")
                for event in target.events[:2]
            }
            if captured_frontiers != {
                GEN14_LEGACY_SPLIT_STORED_FRONTIER_SHA256
            }:
                raise WorkstreamGenerationError(
                    "generation_prepare_legacy_split_head_stored_frontier_changed"
                )
            if binding["input_frontier_sha256"] != (
                GEN14_LEGACY_SPLIT_RECOMPUTED_FRONTIER_SHA256
            ):
                raise WorkstreamGenerationError(
                    "generation_prepare_legacy_split_head_recomputed_frontier_changed"
                )
            raise WorkstreamGenerationError(
                "generation_prepare_legacy_split_head_prefix_changed"
            )
        if (
            current_seed_scope is not None
            and current_seed_scope["value"] != scope
        ):
            desired_seed_disposition = projection_disposition_value(
                graph, seed_items, remote_head=seed_remote_head,
                workstream_id=workstream_id,
            )
            current_scope = current_seed_scope["value"]
            primary_key = scope["primary_repository"]
            current_primary = [
                repository
                for repository in current_scope.get("repositories", [])
                if repository_key(repository) == primary_key
            ]
            desired_primary = [
                repository for repository in scope.get("repositories", [])
                if repository_key(repository) == primary_key
            ]
            expected_scope = deepcopy(current_scope)
            expected_primary = [
                repository
                for repository in expected_scope.get("repositories", [])
                if repository_key(repository) == primary_key
            ]
            if (
                len(current_primary) != 1
                or len(desired_primary) != 1
                or len(expected_primary) != 1
                or current_scope.get("primary_repository") != primary_key
                or desired_primary[0].get("exact_head") != seed_remote_head
                or not re.fullmatch(
                    r"[0-9a-f]{40}(?:[0-9a-f]{24})?",
                    str(current_primary[0].get("exact_head", "")),
                )
            ):
                raise WorkstreamGenerationError(
                    "generation_prepare_target_primary_head_transition_invalid"
                )
            expected_primary[0]["exact_head"] = seed_remote_head
            if expected_scope != scope:
                raise WorkstreamGenerationError(
                    "generation_prepare_target_scope_drift"
                )
            reviewed_disposition_event = target_disposition_event
            if target_disposition == desired_seed_disposition:
                supersedes = (
                    reviewed_disposition_event or {}
                ).get("supersedes_event_id")
                reviewed_disposition_event = next((
                    event for event in target.events
                    if event.get("event_id") == supersedes
                    and (event.get("kind"), event.get("key"))
                    == ("disposition", "root")
                ), None)
            reviewed_disposition = (
                reviewed_disposition_event.get("value", {})
                if reviewed_disposition_event is not None else {}
            )
            ordinary_transition = (
                isinstance(reviewed_disposition, dict)
                and set(reviewed_disposition) == {
                    "disposition", "remote_head", "recovered_from_checkpoint",
                }
                and reviewed_disposition.get("remote_head")
                == current_primary[0]["exact_head"]
                and reviewed_disposition.get("disposition")
                in {"attach", "create_successor"}
                and (
                    target_disposition == reviewed_disposition
                    or target_disposition == desired_seed_disposition
                )
            )
            legacy_split = (
                not ordinary_transition
                and reviewed_disposition_event is not None
                and legacy_prefix
                and reviewed_disposition.get("remote_head")
                == target.events[4]["value"]["remote_head"]
                and target_disposition in (
                    reviewed_disposition, desired_seed_disposition,
                )
                and seed_remote_head not in {
                    current_primary[0]["exact_head"],
                    reviewed_disposition.get("remote_head"),
                }
                and desired_seed_disposition == {
                    "disposition": "create_successor",
                    "remote_head": seed_remote_head,
                    "recovered_from_checkpoint": None,
                }
            )
            if not ordinary_transition and not legacy_split:
                raise WorkstreamGenerationError(
                    "generation_prepare_target_disposition_missing_for_head_transition"
                )
            transition_name = (
                "terminal_child_evidence_seed_legacy_split_head_repair"
                if legacy_split
                else "terminal_child_evidence_seed_head_transition"
            )
            transition = {
                "repository_key": primary_key,
                "from_exact_head": current_primary[0]["exact_head"],
                "to_exact_head": seed_remote_head,
                "from_scope_event_id": current_seed_scope["event_id"],
                "from_scope_value_sha256": canonical_digest(current_scope),
                "from_disposition_event_id": reviewed_disposition_event["event_id"],
                "from_disposition_value_sha256": canonical_digest(
                    reviewed_disposition
                ),
                "disposition": deepcopy(desired_seed_disposition),
                "input_frontier_sha256": binding["input_frontier_sha256"],
                "created_at": created_at,
            }
            if legacy_split:
                transition["from_disposition_exact_head"] = (
                    reviewed_disposition["remote_head"]
                )
                transition["created_at"] = (
                    target.events[6]["created_at"]
                    if target.revision >= 7 else created_at
                )
            seed_manifest[transition_name] = transition
        normalized_seed = prepare_terminal_child_evidence_seeds(
            seed_manifest, graph, seed_target, remote_head=seed_remote_head,
            comments=comments,
        )
        seed_values = {
            (item["kind"], item["key"]): item["value"]
            for item in normalized_seed["projection"]
        }
        # The seed consumer attaches the authenticated predecessor closure
        # authority to carried terminal evidence.  That value is durable target
        # authority, not a transient seed-only wrapper; retain it in the final
        # generation projection so later closure/activation preparation cannot
        # propose stripping the proof it just required.
        for item in complete_items:
            identity = (item["kind"], item["key"])
            if item["kind"] == "evidence_contract" and identity in seed_values:
                item["value"] = deepcopy(seed_values[identity])
        allowed_seed = (
            set(seed_values)
            | {(item["kind"], item["key"]) for item in complete_items}
            | {("child_closure", child_id) for child_id in closure_heads}
            | {("disposition", "root")}
        )
        if not set(target_heads).issubset(allowed_seed):
            unexpected = sorted(set(target_heads) - allowed_seed)
            raise WorkstreamGenerationError(
                "generation_prepare_noncanonical_target_prefix:"
                + ",".join(f"{kind}:{key}" for kind, key in unexpected)
            )
        seed_satisfied = all(
            identity in target_heads
            and target_heads[identity]["value"] == value
            for identity, value in seed_values.items()
        ) and ("disposition", "root") in target_heads
        if not seed_satisfied:
            phase = "terminal_evidence_seed"
            manifest = normalized_seed
            terminal_stage = {
                "state": "terminal_evidence_seed_required",
                "children": [seed["child_identifier"] for seed in seeds],
            }
        else:
            repairs: list[dict[str, Any]] = []
            for seed in seeds:
                child_id = seed["child_identifier"]
                approved = sorted([
                    {
                        "key": key, "event_id": event["event_id"],
                        "value_sha256": canonical_digest(event["value"]),
                    }
                    for (kind, key), event in target_heads.items()
                    if kind == "evidence_contract"
                    and event["value"].get("owning_child") == child_id
                ], key=lambda item: (item["key"], item["event_id"]))
                if [item["key"] for item in approved] != seed["evidence_keys"]:
                    raise WorkstreamGenerationError(
                        f"generation_prepare_target_evidence_changed:{child_id}"
                    )
                repairs.append({
                    "child_identifier": child_id,
                    "child_issue_id": seed["child_issue_id"],
                    "expected_child_readback_sha256": seed[
                        "expected_child_readback_sha256"
                    ],
                    "expected_assignee_id": seed["expected_assignee_id"],
                    "approved_evidence_heads": approved,
                })
            repair_manifest = {
                **projection_review_contract(target),
                "projection": [
                    {"kind": kind, "key": key,
                     "value": deepcopy(event["value"])}
                    for (kind, key), event in sorted(target_heads.items())
                    if kind != "disposition"
                ],
                "retirements": [], "terminal_child_repairs": repairs,
            }
            normalized_repair = prepare_terminal_child_repairs(
                repair_manifest, graph, target,
            )
            source_head = target_heads.get(("source", "root"))
            bridge = {
                "prefix_sha256": GEN14_LEGACY_SPLIT_PREFIX_SHA256,
                "stored_input_frontier_sha256": (
                    GEN14_LEGACY_SPLIT_STORED_FRONTIER_SHA256
                ),
                "recomputed_input_frontier_sha256": (
                    GEN14_LEGACY_SPLIT_RECOMPUTED_FRONTIER_SHA256
                ),
                "source_event_id": (
                    source_head.get("event_id", "")
                    if isinstance(source_head, dict) else ""
                ),
                "source_value_sha256": (
                    canonical_digest(source_head.get("value"))
                    if isinstance(source_head, dict) else ""
                ),
                "created_at": created_at,
                "child_identifiers": sorted(
                    repair["child_identifier"].upper() for repair in repairs
                ),
            }
            if _gen14_stable_repair_descendant(
                target, normalized_repair["projection"], bridge,
            ):
                repair_manifest[
                    "terminal_child_repair_gen14_frontier_bridge"
                ] = bridge
                normalized_repair = prepare_terminal_child_repairs(
                    repair_manifest, graph, target,
                )
            repair_values = {
                (item["kind"], item["key"]): item["value"]
                for item in normalized_repair["projection"]
            }
            repair_satisfied = all(
                identity in target_heads
                and target_heads[identity]["value"] == value
                for identity, value in repair_values.items()
            )
            if not repair_satisfied:
                phase = "terminal_closure_repair"
                manifest = normalized_repair
                terminal_stage = {
                    "state": "terminal_closure_repair_required",
                    "children": [seed["child_identifier"] for seed in seeds],
                }
            else:
                terminal_stage = {
                    "state": "terminal_children_carried",
                    "children": [seed["child_identifier"] for seed in seeds],
                }
                current_values = {
                    (kind, key): event["value"]
                    for (kind, key), event in target_heads.items()
                    if kind != "disposition"
                }
                desired_values = {
                    (item["kind"], item["key"]): item["value"]
                    for item in complete_items
                }
                # Target closures are derived from authenticated live state;
                # they replace predecessor closure values in the final set.
                desired_values.update({
                    identity: value for identity, value in current_values.items()
                    if identity[0] == "child_closure"
                })
                allowed_final = set(desired_values) | {("disposition", "root")}
                if not set(target_heads).issubset(allowed_final):
                    unexpected = sorted(set(target_heads) - allowed_final)
                    raise WorkstreamGenerationError(
                        "generation_prepare_noncanonical_target_prefix:"
                        + ",".join(f"{kind}:{key}" for kind, key in unexpected)
                    )
                manifest = {
                    **projection_review_contract(target),
                    "projection": [
                        {"kind": kind, "key": key, "value": deepcopy(value)}
                        for (kind, key), value in sorted(desired_values.items())
                    ],
                    "retirements": [],
                }
                if all(
                    identity in current_values and current_values[identity] == value
                    for identity, value in desired_values.items()
                ) and target_disposition_matches(manifest["projection"]):
                    phase = "activation_ready"
        if source_transition_required and not (
            legacy_prefix and phase == "terminal_evidence_seed"
        ):
            # The phase-specific validators above have now admitted the exact
            # candidate as a canonical eventual-generation prefix.  The one
            # content-addressed GEN-14 lineage must finish its strict
            # evidence/disposition/scope tail before source; every other
            # admitted prefix may stage the source-only transition now.
            if phase == "terminal_evidence_seed":
                phase_allowed = set(seed_values) | {("disposition", "root")}
            elif phase == "terminal_closure_repair":
                phase_allowed = (
                    set(seed_values)
                    | {
                        ("child_closure", seed["child_identifier"])
                        for seed in seeds
                    }
                    | {("disposition", "root")}
                )
            else:
                phase_allowed = set(desired_values) | {("disposition", "root")}
                if any(
                    identity not in {("source", "root"), ("disposition", "root")}
                    and target_heads[identity]["value"] != desired_values[identity]
                    for identity in set(target_heads) & set(desired_values)
                ):
                    raise WorkstreamGenerationError(
                        "generation_prepare_noncanonical_target_prefix"
                    )
            if not set(target_heads).issubset(phase_allowed):
                unexpected = sorted(set(target_heads) - phase_allowed)
                raise WorkstreamGenerationError(
                    "generation_prepare_noncanonical_target_prefix:"
                    + ",".join(f"{kind}:{key}" for kind, key in unexpected)
                )
            if target_disposition_matches(source_transition_projection()):
                return stage_source_transition()
    else:
        desired_values = {
            (item["kind"], item["key"]): item["value"]
            for item in complete_items
        }
        allowed = set(desired_values) | {("disposition", "root")}
        if not set(target_heads).issubset(allowed) or any(
            identity in target_heads
            and target_heads[identity]["value"] != desired_values[identity]
            for identity in set(target_heads) & set(desired_values)
        ):
            raise WorkstreamGenerationError(
                "generation_prepare_noncanonical_target_prefix"
            )
        manifest = {
            **projection_review_contract(target),
            "projection": complete_items, "retirements": [],
        }
        if all(
            identity in target_heads and target_heads[identity]["value"] == value
            for identity, value in desired_values.items()
        ) and target_disposition_matches(complete_items):
            phase = "activation_ready"
    return finish(
        manifest, phase, terminal_stage,
        reviewed_remote_head=effective_remote_head,
    )


def validate_activation_operator_contract(
    contract: dict[str, Any], *, source: dict[str, str], workstream_id: str,
    authority: dict[str, str], comments: list[dict[str, Any]],
    graph: dict[str, Any], description_plan_revision: str | None,
    created_at: str, remote_head: str | None,
) -> dict[str, Any]:
    """Require the exact live activation-ready prepare output at CLI activation."""
    from workstream_root_transition import (
        reopen_transition_witness_context, validate_operator_contract,
    )

    if not isinstance(contract, dict):
        raise WorkstreamGenerationError(
            "generation_operator_contract_invalid"
        )
    native = (graph.get("root") or {}).get("state") or {}
    started_state = {
        "id": native.get("id"), "name": native.get("name"),
        "type": native.get("type"), "team_id": authority["team_id"],
    }
    target_state = (
        ((contract.get("native_transition") or {}).get("target_state") or {})
        if isinstance(contract, dict) else {}
    )
    if (
        str(native.get("type", "")).lower() != "started"
        or started_state != target_state
        or created_at != contract.get("created_at")
        or (remote_head is not None and remote_head != contract.get("remote_head"))
    ):
        raise WorkstreamGenerationError(
            "generation_operator_contract_native_or_invocation_mismatch"
        )
    witness = reopen_transition_witness_context(
        comments=comments, graph=graph, token=workstream_id,
        authority=authority,
        contract_sha256=str(contract.get("contract_sha256", "")),
        target_state=started_state,
        operator_contract_sha256=_digest(contract),
    )
    validation_graph = witness["graph"] if witness is not None else graph
    authorization = validate_operator_contract(
        contract, source=source, token=workstream_id,
        authority=authority, comments=comments, graph=validation_graph,
        started_state=started_state,
        description_plan_revision=description_plan_revision,
    )
    if witness is not None and authorization != witness["reservation"][
        "operator_authorization"
    ]:
        raise WorkstreamGenerationError(
            "generation_root_transition_witness_authorization_mismatch"
        )
    result = {
        "authorization": authorization,
        "retirement_proof": deepcopy(contract["retirement_proof"]),
        "remote_head": contract["remote_head"],
    }
    if witness is not None:
        result["root_transition_recovery_receipt"] = witness["receipt"]
    return result


def _validate_candidate_receipt(
    receipt: dict[str, Any], *, plan_revision: str, authority: dict[str, str],
    source: dict[str, str], material_revision: int,
    checkpoint_event_ids: list[str], projection_revision: int,
) -> None:
    required = {
        "resume_authority", "plan_revision", "authenticated_route", "source",
        "material_revision", "checkpoint_event_ids", "projection_revision",
        "graph_frontier_sha256", "snapshot_sha256",
        "quarantined_legacy_writes",
    }
    if (
        not isinstance(receipt, dict) or set(receipt) != required
        or receipt["resume_authority"] != "full"
        or receipt["plan_revision"] != plan_revision
        or receipt["authenticated_route"] != authority
        or receipt["source"] != source
        or receipt["material_revision"] != material_revision
        or receipt["checkpoint_event_ids"] != checkpoint_event_ids
        or receipt["projection_revision"] != projection_revision
        or not HEX64.fullmatch(str(receipt["graph_frontier_sha256"]))
        or not HEX64.fullmatch(str(receipt["snapshot_sha256"]))
        or not isinstance(receipt["quarantined_legacy_writes"], dict)
        or set(receipt["quarantined_legacy_writes"]) != {"count", "sha256"}
        or not isinstance(receipt["quarantined_legacy_writes"]["count"], int)
        or not HEX64.fullmatch(str(
            receipt["quarantined_legacy_writes"]["sha256"]
        ))
    ):
        raise WorkstreamGenerationError(
            "generation_candidate_not_strict_full_authority"
        )


def _prospective_activation_checkpoint(
    checkpoint: dict[str, Any], *, workstream_id: str,
    target_plan_revision: str, material_revision: int,
    target_state: Any, remote_head: str | None, created_at: str,
    authority: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Validate and model the checkpoint-bound target projection in memory."""
    validate_checkpoint(checkpoint)
    if (
        checkpoint["workstream_id"] != workstream_id
        or checkpoint["plan_revision"] != target_plan_revision
        or checkpoint["root_revision"] != material_revision
        or checkpoint["acknowledgement"] != {
            "state": "pending", "remote_id": None,
            "applied_revision": None,
        }
        or not isinstance(remote_head, str)
        or not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", remote_head)
    ):
        raise WorkstreamGenerationError(
            "generation_activation_checkpoint_mismatch"
        )
    source = target_state.snapshot.get("source") or {}
    if source.get("sha256") != target_plan_revision:
        raise WorkstreamGenerationError(
            "generation_activation_checkpoint_source_incomplete"
        )
    synthetic = acknowledge_checkpoint(
        checkpoint, remote_id="00000000-0000-4000-8000-000000000000",
        applied_revision=material_revision,
    )
    recovered = recover_latest(
        [synthetic], workstream_id,
        expected_plan_revision=target_plan_revision,
    )
    decision = choose_disposition({
        "root": {"identifier": workstream_id},
        "latest_checkpoint": recovered,
    }, remote_head=remote_head)
    desired = {
        "disposition": decision["disposition"],
        "remote_head": remote_head,
        "recovered_from_checkpoint": checkpoint["event_id"],
    }
    disposition_head = next((
        event for event in reversed(target_state.events)
        if event["kind"] == "disposition" and event["key"] == "root"
    ), None)
    if disposition_head is not None and disposition_head["value"] == desired:
        return desired, None
    return desired, build_projection_event(
        workstream_id=workstream_id, kind="disposition", key="root",
        value=desired, plan_revision=target_plan_revision,
        expected_revision=target_state.revision, created_at=created_at,
        supersedes_event_id=(
            disposition_head["event_id"] if disposition_head else None
        ), authority=authority,
    )


def _validate_checkpoint_custody(value: dict[str, Any]) -> None:
    required = {
        "schema_version", "workstream_id", "authority", "target_plan_revision",
        "created_at", "remote_head", "operator_contract_sha256",
        "native_root_sha256", "native_root_material_sha256",
        "root_transition_receipt_ref", "root_transition_receipt_sha256",
        "activation_checkpoint_sha256", "retirement_sha256",
        "prospective_event_id", "prospective_event_sha256", "source",
    }
    if (
        not isinstance(value, dict) or set(value) != required
        or value.get("schema_version") != 1
        or not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(value.get("workstream_id", "")))
        or not isinstance(value.get("authority"), dict)
        or not all(isinstance(item, str) and item for item in value["authority"].values())
        or not all(HEX64.fullmatch(str(value.get(field, ""))) for field in (
            "target_plan_revision", "operator_contract_sha256",
            "native_root_sha256", "native_root_material_sha256",
            "root_transition_receipt_sha256", "activation_checkpoint_sha256",
            "retirement_sha256", "prospective_event_sha256",
        ))
        or not isinstance(value.get("created_at"), str) or not value["created_at"]
        or not isinstance(value.get("remote_head"), str)
        or not EVENT_ID.fullmatch(str(value.get("prospective_event_id", "")))
        or not re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
            r"[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            str(value.get("root_transition_receipt_ref", "")),
        )
        or not isinstance(value.get("source"), dict)
        or set(value["source"]) != {"identity", "sha256"}
        or not isinstance(value["source"].get("identity"), str)
        or not value["source"]["identity"]
        or not HEX64.fullmatch(str(value["source"].get("sha256", "")))
    ):
        raise WorkstreamGenerationError("invalid_generation_checkpoint_custody")


def encode_generation_checkpoint_custody(value: dict[str, Any]) -> str:
    _validate_checkpoint_custody(value)
    return _envelope(
        CHECKPOINT_CUSTODY_PREFIX, "checkpoint_custody", value,
    )


def checkpoint_custody_slot_id(value: dict[str, Any]) -> str:
    _validate_checkpoint_custody(value)
    return str(uuid.UUID(hex=_digest(value)[:32], version=4))


def reduce_generation_checkpoint_custodies(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str],
) -> list[dict[str, Any]]:
    result = []
    observed = set()
    for comment in comments:
        body = str(comment.get("body") or "")
        if CHECKPOINT_CUSTODY_PREFIX not in body:
            continue
        value = _decode_envelope(
            body, prefix=CHECKPOINT_CUSTODY_PREFIX,
            pattern=CHECKPOINT_CUSTODY_RE,
            payload_name="checkpoint_custody",
        )
        _validate_checkpoint_custody(value)
        slot = checkpoint_custody_slot_id(value)
        if (
            value["workstream_id"] != workstream_id
            or value["authority"] != authenticated_route
            or comment.get("id") != slot
            or slot in observed
        ):
            raise WorkstreamGenerationError(
                "generation_checkpoint_custody_mismatch"
            )
        observed.add(slot)
        result.append({**value, "remote_id": slot})
    return sorted(result, key=lambda item: item["remote_id"])


def _validate_reservation(value: dict[str, Any]) -> None:
    if not isinstance(value, dict):
        raise WorkstreamGenerationError("invalid_generation_reservation")
    fields = {
        "schema_version", "reservation_id", "workstream_id", "authority",
        "mode", "from_plan_revision", "to_plan_revision", "activation_epoch",
        "previous_control_event_id", "source", "material_revision",
        "checkpoint_event_ids", "ledger_frontier", "from_projection_revision",
        "to_projection_revision", "graph_frontier_sha256",
        "candidate_resume_sha256", "retirement", "created_at",
    }
    schema_version = value.get("schema_version")
    if schema_version in {3, 4, 5, 6}:
        fields.add("native_root_sha256")
    if schema_version in {4, 5, 6}:
        fields.update({"activation_checkpoint", "remote_head"})
    if schema_version in {5, 6}:
        fields.add("operator_contract_sha256")
    if schema_version == 6:
        fields.update({
            "native_root_material_sha256", "root_transition_receipt_ref",
            "root_transition_receipt_sha256",
        })
    if (
        set(value) != fields
        or schema_version not in {2, 3, 4, 5, 6}
        or not RESERVATION_ID.fullmatch(str(value.get("reservation_id", "")))
        or value.get("mode") not in {"bootstrap", "activate"}
        or not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(value.get("workstream_id", "")))
        or not isinstance(value.get("authority"), dict)
        or set(value["authority"]) != {
            "workspace_id", "team_id", "project_id", "root_issue_id",
        }
        or not all(isinstance(item, str) and item for item in value["authority"].values())
        or not all(HEX64.fullmatch(str(value.get(field, ""))) for field in (
            "from_plan_revision", "to_plan_revision", "graph_frontier_sha256",
            "candidate_resume_sha256",
        ))
        or not isinstance(value.get("source"), dict)
        or set(value["source"]) != {"identity", "sha256"}
        or value["source"].get("sha256") != value["to_plan_revision"]
        or not isinstance(value["source"].get("identity"), str)
        or not value["source"]["identity"]
        or any(not isinstance(value.get(field), int)
               or isinstance(value.get(field), bool) or value[field] < 0
               for field in (
                   "activation_epoch", "material_revision",
                   "from_projection_revision", "to_projection_revision",
               ))
        or any(
            not isinstance(value.get(field), list)
            or value[field] != sorted(set(value[field]))
            or not all(isinstance(item, str) and item for item in value[field])
            for field in ("checkpoint_event_ids", "ledger_frontier")
        )
        or not isinstance(value.get("created_at"), str) or not value["created_at"]
        or (
            schema_version in {3, 4, 5, 6}
            and not HEX64.fullmatch(str(value.get("native_root_sha256", "")))
        )
        or (
            schema_version in {4, 5, 6}
            and (
                (value.get("activation_checkpoint") is not None
                 and not isinstance(value.get("activation_checkpoint"), dict))
                or (value.get("remote_head") is not None
                    and not re.fullmatch(
                        r"[0-9a-f]{40}(?:[0-9a-f]{24})?",
                        str(value.get("remote_head", "")),
                    ))
                or ((value.get("activation_checkpoint") is None)
                    != (value.get("remote_head") is None))
            )
        )
        or (
            schema_version in {5, 6}
            and not HEX64.fullmatch(str(value.get("operator_contract_sha256", "")))
        )
        or (
            schema_version == 6
            and (
                not HEX64.fullmatch(str(value.get(
                    "native_root_material_sha256", "",
                )))
                or not re.fullmatch(
                    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
                    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                    str(value.get("root_transition_receipt_ref", "")),
                )
                or not HEX64.fullmatch(str(value.get(
                    "root_transition_receipt_sha256", "",
                )))
            )
        )
    ):
        raise WorkstreamGenerationError("invalid_generation_reservation")
    previous = value["previous_control_event_id"]
    if previous is not None and not EVENT_ID.fullmatch(str(previous)):
        raise WorkstreamGenerationError("invalid_generation_reservation")
    _validate_retirement(
        value["retirement"], value["from_plan_revision"], value["activation_epoch"],
    )
    if schema_version in {4, 5, 6} and value["activation_checkpoint"] is not None:
        try:
            validate_checkpoint(value["activation_checkpoint"])
        except (TypeError, ValueError) as error:
            raise WorkstreamGenerationError(
                "invalid_generation_reservation"
            ) from error
        if (
            value["activation_checkpoint"]["workstream_id"] != value["workstream_id"]
            or value["activation_checkpoint"]["plan_revision"]
            != value["to_plan_revision"]
        ):
            raise WorkstreamGenerationError("invalid_generation_reservation")
    unsigned = {key: deepcopy(item) for key, item in value.items()
                if key != "reservation_id"}
    if value["reservation_id"] != "wsgr_" + _digest(unsigned)[:32]:
        raise WorkstreamGenerationError("invalid_generation_reservation")


def encode_generation_reservation(value: dict[str, Any]) -> str:
    _validate_reservation(value)
    return _envelope(RESERVATION_PREFIX, "reservation", value)


def _validate_graph_clock_custody(value: dict[str, Any]) -> None:
    fields = {
        "schema_version", "workstream_id", "authority", "reservation_id",
        "reservation_sha256", "source", "candidate_seal_event_id",
        "candidate_seal_sha256", "graph_frontier_sha256",
        "native_root_sha256", "native_root_material_sha256",
        "root_transition_receipt_ref", "root_transition_receipt_sha256",
        "historical_root_updated_at", "observed_root_updated_at",
        "observed_graph_frontier_sha256", "observed_native_root_sha256",
    }
    if (
        not isinstance(value, dict) or set(value) != fields
        or value.get("schema_version") != 1
        or not re.fullmatch(
            r"[A-Z][A-Z0-9]*-\d+", str(value.get("workstream_id", ""))
        )
        or not isinstance(value.get("authority"), dict)
        or set(value["authority"]) != {
            "workspace_id", "team_id", "project_id", "root_issue_id",
        }
        or not all(
            isinstance(item, str) and item
            for item in value["authority"].values()
        )
        or not RESERVATION_ID.fullmatch(str(value.get("reservation_id", "")))
        or not EVENT_ID.fullmatch(str(value.get("candidate_seal_event_id", "")))
        or not all(HEX64.fullmatch(str(value.get(field, ""))) for field in (
            "reservation_sha256", "candidate_seal_sha256",
            "graph_frontier_sha256", "native_root_sha256",
            "native_root_material_sha256", "root_transition_receipt_sha256",
            "observed_graph_frontier_sha256", "observed_native_root_sha256",
        ))
        or not isinstance(value.get("source"), dict)
        or set(value["source"]) != {"identity", "sha256"}
        or not isinstance(value["source"].get("identity"), str)
        or not value["source"]["identity"]
        or not HEX64.fullmatch(str(value["source"].get("sha256", "")))
        or not re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
            r"[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            str(value.get("root_transition_receipt_ref", "")),
        )
        or not isinstance(value.get("historical_root_updated_at"), str)
        or not value["historical_root_updated_at"]
        or not isinstance(value.get("observed_root_updated_at"), str)
        or not value["observed_root_updated_at"]
    ):
        raise WorkstreamGenerationError("invalid_generation_graph_clock_custody")


def encode_generation_graph_clock_custody(value: dict[str, Any]) -> str:
    _validate_graph_clock_custody(value)
    return _envelope(CLOCK_CUSTODY_PREFIX, "graph_clock_custody", value)


def graph_clock_custody_slot_id(value: dict[str, Any]) -> str:
    _validate_graph_clock_custody(value)
    return str(uuid.UUID(hex=_digest(value)[:32], version=4))


def reduce_generation_graph_clock_custodies(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    observed: set[str] = set()
    for comment in comments:
        body = str(comment.get("body") or "")
        if CLOCK_CUSTODY_PREFIX not in body:
            continue
        value = _decode_envelope(
            body, prefix=CLOCK_CUSTODY_PREFIX, pattern=CLOCK_CUSTODY_RE,
            payload_name="graph_clock_custody",
        )
        _validate_graph_clock_custody(value)
        slot = graph_clock_custody_slot_id(value)
        if (
            value["workstream_id"] != workstream_id
            or value["authority"] != authenticated_route
            or comment.get("id") != slot
            or slot in observed
        ):
            raise WorkstreamGenerationError(
                "generation_graph_clock_custody_mismatch"
            )
        observed.add(slot)
        result.append({**value, "remote_id": slot})
    return sorted(result, key=lambda item: item["remote_id"])


def _validate_finalization(value: dict[str, Any]) -> None:
    fields = {
        "schema_version", "finalization_id", "workstream_id", "authority",
        "reservation_id", "reservation_sha256", "transition_event_id",
        "transition_sha256", "native_root_sha256", "source",
        "execution_status", "created_at",
    }
    if (
        not isinstance(value, dict) or set(value) != fields
        or value.get("schema_version") != 1
        or not FINALIZATION_ID.fullmatch(str(value.get("finalization_id", "")))
        or not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(value.get("workstream_id", "")))
        or not isinstance(value.get("authority"), dict)
        or set(value["authority"]) != {
            "workspace_id", "team_id", "project_id", "root_issue_id",
        }
        or not all(isinstance(item, str) and item for item in value["authority"].values())
        or not RESERVATION_ID.fullmatch(str(value.get("reservation_id", "")))
        or not EVENT_ID.fullmatch(str(value.get("transition_event_id", "")))
        or not all(HEX64.fullmatch(str(value.get(field, ""))) for field in (
            "reservation_sha256", "transition_sha256", "native_root_sha256",
        ))
        or not isinstance(value.get("source"), dict)
        or set(value["source"]) != {"identity", "sha256"}
        or not isinstance(value["source"].get("identity"), str)
        or not value["source"]["identity"]
        or not HEX64.fullmatch(str(value["source"].get("sha256", "")))
        or value.get("execution_status") != {
            "authority": "generation_local",
            "name": "In Progress",
            "type": "started",
        }
        or not isinstance(value.get("created_at"), str) or not value["created_at"]
    ):
        raise WorkstreamGenerationError("invalid_generation_finalization")
    unsigned = {key: deepcopy(item) for key, item in value.items()
                if key != "finalization_id"}
    if value["finalization_id"] != "wsgf_" + _digest(unsigned)[:32]:
        raise WorkstreamGenerationError("invalid_generation_finalization")


def encode_generation_finalization(value: dict[str, Any]) -> str:
    _validate_finalization(value)
    return _envelope(FINALIZATION_PREFIX, "finalization", value)


def generation_finalization_slot_id(value: dict[str, Any]) -> str:
    _validate_finalization(value)
    digest = hashlib.sha256(_canonical(value)).hexdigest()
    return str(uuid.UUID(hex=digest[:32], version=4))


def reduce_generation_finalizations(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str] | None,
) -> list[dict[str, Any]]:
    from workstream_linear_projection import (
        PROJECTION_PREFIX, PROJECTION_RE,
    )
    transitions: dict[str, dict[str, Any]] = {}
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str) or PROJECTION_PREFIX not in body:
            continue
        encoded = PROJECTION_RE.findall(body)
        if len(encoded) != 1 or body.count(PROJECTION_PREFIX) != 1:
            raise WorkstreamGenerationError("malformed_projection_marker")
        event = decode_projection_receipt(comment, encoded[0])
        if event["kind"] == "generation_transition":
            transitions[event["event_id"]] = event
    result: list[dict[str, Any]] = []
    observed: set[str] = set()
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str):
            raise WorkstreamGenerationError("malformed_generation_finalization")
        if FINALIZATION_PREFIX not in body:
            continue
        value = _decode_envelope(
            body, prefix=FINALIZATION_PREFIX, pattern=FINALIZATION_RE,
            payload_name="finalization",
        )
        _validate_finalization(value)
        transition = transitions.get(value["transition_event_id"])
        if (
            value["workstream_id"] != workstream_id
            or (authenticated_route is not None
                and value["authority"] != authenticated_route)
            or transition is None
            or transition["workstream_id"] != workstream_id
            or transition["authority"] != value["authority"]
            or transition["value"].get("schema_version") != 4
            or transition["value"]["reservation_id"] != value["reservation_id"]
            or transition["value"]["reservation_sha256"] != value["reservation_sha256"]
            or transition["value"]["native_root_sha256"] != value["native_root_sha256"]
            or transition["value"]["source"] != value["source"]
            or _digest(transition) != value["transition_sha256"]
            or comment.get("id") != generation_finalization_slot_id(value)
        ):
            raise WorkstreamGenerationError("generation_finalization_binding_mismatch")
        if value["finalization_id"] in observed:
            raise WorkstreamGenerationError("duplicate_generation_finalization")
        observed.add(value["finalization_id"])
        result.append({**value, "remote_id": comment["id"]})
    return sorted(result, key=lambda item: item["finalization_id"])


def finalized_generation_transition_ids(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str] | None,
) -> set[str]:
    return {
        item["transition_event_id"] for item in reduce_generation_finalizations(
            comments, workstream_id=workstream_id,
            authenticated_route=authenticated_route,
        )
    }


def selected_generation_execution_status(
    comments: list[dict[str, Any]], *, workstream_id: str,
    transition_event_id: str | None,
    authenticated_route: dict[str, str] | None,
) -> dict[str, str] | None:
    """Return the append-only status selected with a schema-v4 generation.

    Native Linear status is a mutable display cache and cannot be atomically
    changed with an append-only generation transition.  The finalization is
    therefore the generation-local execution authority.  A terminal native
    status observed after preparation cannot make the newly activated plan
    appear Done; resume authoritatively prefers this status while retaining the
    native observation for reconciliation and closure review.
    """
    if transition_event_id is None:
        return None
    matches = [
        item for item in reduce_generation_finalizations(
            comments, workstream_id=workstream_id,
            authenticated_route=authenticated_route,
        )
        if item["transition_event_id"] == transition_event_id
    ]
    if len(matches) > 1:
        raise WorkstreamGenerationError("duplicate_generation_finalization")
    return deepcopy(matches[0]["execution_status"]) if matches else None


def reduce_generation_reservations(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str],
) -> list[dict[str, Any]]:
    checkpoints = reduce_generation_checkpoint_comments(
        comments, workstream_id=workstream_id,
        authenticated_route=authenticated_route,
    )
    material = reduce_event_comments(comments, workstream_id=workstream_id)
    observed: dict[str, dict[str, Any]] = {}
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str):
            raise WorkstreamGenerationError("malformed_generation_comment")
        if RESERVATION_PREFIX not in body:
            continue
        value = _decode_envelope(
            body, prefix=RESERVATION_PREFIX, pattern=RESERVATION_RE,
            payload_name="reservation",
        )
        _validate_reservation(value)
        if value["workstream_id"] != workstream_id or value["authority"] != authenticated_route:
            raise WorkstreamGenerationError("generation_reservation_route_mismatch")
        if value["reservation_id"] in observed:
            raise WorkstreamGenerationError("duplicate_generation_reservation")
        slot = ledger_boundary_slot_id(
            workstream_id, value["material_revision"], value["ledger_frontier"],
            authenticated_route,
        )
        if comment.get("id") != slot:
            raise WorkstreamGenerationError("generation_reservation_slot_mismatch")
        if value["material_revision"] > material.revision or not set(
            value["checkpoint_event_ids"]
        ).issubset({item["event_id"] for item in checkpoints.checkpoints}):
            raise WorkstreamGenerationError("generation_reservation_frontier_impossible")
        observed[value["reservation_id"]] = {
            **value, "remote_id": slot, "reservation_sha256": _digest(value),
        }
    return sorted(observed.values(), key=lambda item: item["reservation_id"])


def generation_abort_slot_id(
    reservation: dict[str, Any], projection_revision: int | None = None,
) -> str:
    """Use a predecessor CAS slot so abort and activation cannot both win."""
    return projection_slot_id(
        reservation["workstream_id"], reservation["from_plan_revision"],
        (reservation["from_projection_revision"] if projection_revision is None
         else projection_revision), reservation["authority"],
    )


def generation_ledger_frontier_tokens(
    comments: list[dict[str, Any]], *, workstream_id: str,
) -> list[str]:
    """Separate upgraded active writers from quarantined legacy successors."""
    from workstream_linear_projection import (
        PROJECTION_PREFIX, PROJECTION_RE,
    )

    tokens: set[str] = set()
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str) or PROJECTION_PREFIX not in body:
            continue
        matches = PROJECTION_RE.findall(body)
        if len(matches) != 1 or body.count(PROJECTION_PREFIX) != 1:
            continue
        try:
            event = decode_projection_receipt(comment, matches[0])
        except LinearTransportError:
            continue
        if (
            event["workstream_id"] == workstream_id
            and event["kind"] in {"generation_genesis", "generation_transition"}
            and comment.get("id") == projection_slot_id(
                workstream_id, event["plan_revision"],
                event["expected_revision"], event["authority"],
            )
        ):
            tokens.add(
                f"generation:{event['value']['reservation_id']}:"
                f"{event['value']['reservation_sha256']}"
            )
    return sorted(tokens)


def _generation_abort_ids(
    comments: list[dict[str, Any]], reservations: list[dict[str, Any]],
) -> set[str]:
    by_token = {
        f"{item['reservation_id']}:{item['reservation_sha256']}": item
        for item in reservations
    }
    result: set[str] = set()
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str) or PROJECTION_PREFIX not in body:
            continue
        matches = PROJECTION_RE.findall(body)
        if len(matches) != 1 or body.count(PROJECTION_PREFIX) != 1:
            continue
        event = decode_projection_receipt(comment, matches[0])
        if event["kind"] != "generation_abort":
            continue
        value = event["value"]
        token = f"{value['reservation_id']}:{value['reservation_sha256']}"
        if token in result:
            raise WorkstreamGenerationError("duplicate_generation_abort")
        reservation = by_token.get(token)
        if reservation is None:
            raise WorkstreamGenerationError("generation_abort_slot_mismatch")
        state = reduce_projection_comments(
            comments, workstream_id=reservation["workstream_id"],
            expected_plan_revision=reservation["from_plan_revision"],
            authenticated_route=reservation["authority"],
        )
        original_revision = reservation["from_projection_revision"]
        abort_revision = event["expected_revision"]
        intervening = list(state.events[original_revision:abort_revision])
        expected_ids = [item["event_id"] for item in intervening]
        original_occupant = expected_ids[0] if expected_ids else None
        if (
            event["plan_revision"] != reservation["from_plan_revision"]
            or event["authority"] != reservation["authority"]
            or value["original_projection_revision"] != original_revision
            or abort_revision != original_revision + len(intervening)
            or len(state.events) <= abort_revision
            or state.events[abort_revision] != event
            or value["intervening_event_ids"] != expected_ids
            or value["intervening_events_sha256"] != _digest(intervening)
            or value["original_occupant_event_id"] != original_occupant
            or any(item["kind"].startswith("generation_") for item in intervening)
            or comment.get("id") != generation_abort_slot_id(
                reservation, abort_revision,
            )
        ):
            raise WorkstreamGenerationError("generation_abort_frontier_mismatch")
        result.add(token)
    return result


def generation_quarantined_comment_ids(
    comments: list[dict[str, Any]], *, workstream_id: str,
) -> set[str]:
    """Quarantine legacy ledger writers which route around a generation fence.

    Old runtimes treat an occupied shared-ledger slot as an unrelated collision
    and walk to a successor.  A live generation reservation instead owns that
    entire deterministic collision chain.  Reducers ignore ledger records in
    those successor slots, so an old writer cannot advance either frontier.
    """
    reservations: list[dict[str, Any]] = []
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str) or RESERVATION_PREFIX not in body:
            continue
        try:
            value = _decode_envelope(
                body, prefix=RESERVATION_PREFIX, pattern=RESERVATION_RE,
                payload_name="reservation",
            )
            _validate_reservation(value)
            if (
                value["workstream_id"] != workstream_id
                or comment.get("id") != ledger_boundary_slot_id(
                    workstream_id, value["material_revision"],
                    value["ledger_frontier"], value["authority"],
                )
            ):
                continue
        except (LinearTransportError, KeyError, TypeError, ValueError):
            continue
        reservations.append({**value, "reservation_sha256": _digest(value)})
    if not reservations:
        return set()
    aborted = _generation_abort_ids(comments, reservations)
    by_id = {
        item.get("id"): item for item in comments
        if isinstance(item.get("id"), str)
    }
    quarantined: set[str] = set()
    for reservation in reservations:
        token = (
            f"{reservation['reservation_id']}:"
            f"{reservation['reservation_sha256']}"
        )
        if token in aborted:
            continue
        frontier = list(reservation["ledger_frontier"])
        occupant = by_id.get(ledger_boundary_slot_id(
            workstream_id, reservation["material_revision"], frontier,
            reservation["authority"],
        ))
        for _attempt in range(32):
            if occupant is None:
                break
            collision = "collision:" + hashlib.sha256(json.dumps(
                [occupant.get("id"), occupant.get("body")], ensure_ascii=False,
                sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")).hexdigest()
            frontier = sorted([*frontier, collision])
            occupant = by_id.get(ledger_boundary_slot_id(
                workstream_id, reservation["material_revision"], frontier,
                reservation["authority"],
            ))
            if occupant is not None:
                quarantined.add(occupant["id"])
    return quarantined


def generation_quarantine_metadata(
    comments: list[dict[str, Any]], *, workstream_id: str,
) -> dict[str, Any]:
    """Return stable evidence for ignored old-runtime ledger writes."""
    from workstream_linear_checkpoints import CHECKPOINT_PREFIX
    from workstream_linear_events import EVENT_PREFIX

    quarantined = generation_quarantined_comment_ids(
        comments, workstream_id=workstream_id,
    )
    records = sorted(
        ({
            "remote_id": item["id"],
            "body_sha256": hashlib.sha256(item["body"].encode("utf-8")).hexdigest(),
        } for item in comments if item.get("id") in quarantined
         and isinstance(item.get("body"), str)
         and (EVENT_PREFIX in item["body"] or CHECKPOINT_PREFIX in item["body"])),
        key=lambda item: item["remote_id"],
    )
    return {"count": len(records), "sha256": _digest(records)}


def pending_generation_reservations(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str],
) -> list[dict[str, Any]]:
    reservations = reduce_generation_reservations(
        comments, workstream_id=workstream_id,
        authenticated_route=authenticated_route,
    )
    aborted = _generation_abort_ids(comments, reservations)
    finalized_tokens = {
        f"{item['reservation_id']}:{item['reservation_sha256']}"
        for item in reduce_generation_finalizations(
            comments, workstream_id=workstream_id,
            authenticated_route=authenticated_route,
        )
    }
    completed: set[str] = set()
    for plan_revision in {
        item["from_plan_revision"] for item in reservations
    } | {item["to_plan_revision"] for item in reservations}:
        state = reduce_projection_comments(
            comments, workstream_id=workstream_id,
            expected_plan_revision=plan_revision,
            authenticated_route=authenticated_route,
        )
        for event in state.events:
            if event["kind"] in {"generation_genesis", "generation_transition"}:
                token = (
                    f"{event['value']['reservation_id']}:"
                    f"{event['value']['reservation_sha256']}"
                )
                reservation = next((
                    item for item in reservations
                    if f"{item['reservation_id']}:{item['reservation_sha256']}" == token
                ), None)
                if (
                    reservation is not None
                    and (reservation.get("schema_version") not in {4, 5, 6}
                         or reservation.get("mode") == "bootstrap"
                         or token in finalized_tokens)
                ):
                    completed.add(token)
    return [item for item in reservations if (
        f"{item['reservation_id']}:{item['reservation_sha256']}" not in completed
        and f"{item['reservation_id']}:{item['reservation_sha256']}" not in aborted
    )]


def assert_no_pending_generation_reservation(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str], allowed_reservation_id: str | None = None,
) -> None:
    pending = pending_generation_reservations(
        comments, workstream_id=workstream_id,
        authenticated_route=authenticated_route,
    )
    blocked = [item for item in pending
               if item["reservation_id"] != allowed_reservation_id]
    if blocked:
        raise WorkstreamGenerationError(
            f"generation_boundary_reserved:{blocked[0]['reservation_id']}"
        )


def generation_controls(comments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from workstream_linear_projection import (
        PROJECTION_PREFIX, PROJECTION_RE, decode_projection_receipt,
    )
    result = []
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str) or PROJECTION_PREFIX not in body:
            continue
        matches = PROJECTION_RE.findall(body)
        if len(matches) != 1 or body.count(PROJECTION_PREFIX) != 1:
            raise WorkstreamGenerationError("malformed_projection_marker")
        event = decode_projection_receipt(comment, matches[0])
        if event["kind"] in {"generation_genesis", "generation_transition"}:
            result.append(event)
    finalized = finalized_generation_transition_ids(
        comments, workstream_id=(
            result[0]["workstream_id"] if result else "GEN-0"
        ), authenticated_route=None,
    ) if result else set()
    return [
        event for event in result
        if event["kind"] != "generation_transition"
        or event["value"].get("schema_version") != 4
        or event["event_id"] in finalized
    ]


def selected_activation_checkpoint(
    comments: list[dict[str, Any]], *, workstream_id: str,
    transition_event_id: str | None, target_plan_revision: str,
    authenticated_route: dict[str, str],
) -> tuple[dict[str, Any], str] | None:
    """Return only the checkpoint carried by the authenticated selected tip.

    The caller must pass the transition identity returned by
    ``select_plan_generation``.  Merely finding a syntactically valid
    transition in the comment stream is never sufficient.
    """
    if transition_event_id is None:
        return None
    matches: list[tuple[dict[str, Any], str]] = []
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str) or PROJECTION_PREFIX not in body:
            continue
        encoded = PROJECTION_RE.findall(body)
        if len(encoded) != 1 or body.count(PROJECTION_PREFIX) != 1:
            raise WorkstreamGenerationError("malformed_projection_marker")
        event = decode_projection_receipt(comment, encoded[0])
        if event["event_id"] != transition_event_id:
            continue
        remote_id = comment.get("id")
        if not isinstance(remote_id, str) or not remote_id:
            raise WorkstreamGenerationError(
                "generation_activation_checkpoint_remote_id_missing"
            )
        matches.append((event, remote_id))
    if len(matches) != 1:
        raise WorkstreamGenerationError(
            "generation_selected_transition_readback_ambiguous"
        )
    event, remote_id = matches[0]
    checkpoint = event.get("value", {}).get("activation_checkpoint")
    if checkpoint is None:
        return None
    if (
        event.get("kind") != "generation_transition"
        or event.get("workstream_id") != workstream_id
        or event.get("authority") != authenticated_route
        or event.get("value", {}).get("to", {}).get("plan_revision")
        != target_plan_revision
        or checkpoint.get("plan_revision") != target_plan_revision
    ):
        raise WorkstreamGenerationError(
            "generation_selected_activation_checkpoint_mismatch"
        )
    return deepcopy(checkpoint), remote_id


def selected_activation_checkpoints(
    comments: list[dict[str, Any]], *, workstream_id: str,
    transition_event_id: str | None, active_plan_revision: str,
    authenticated_route: dict[str, str],
) -> list[tuple[dict[str, Any], str]]:
    """Return carried checkpoints only after the complete control chain wins."""
    if transition_event_id is None:
        return []
    selected = select_plan_generation(
        comments, workstream_id=workstream_id,
        description_plan_revision=None,
        authenticated_route=authenticated_route,
    )
    if (
        selected["transition_tip_event_id"] != transition_event_id
        or selected["plan_revision"] != active_plan_revision
    ):
        raise WorkstreamGenerationError(
            "generation_selected_transition_changed"
        )
    result: list[tuple[dict[str, Any], str]] = []
    controls = sorted(
        generation_controls(comments),
        key=lambda event: event["value"]["activation_epoch"],
    )
    for event in controls:
        checkpoint = event.get("value", {}).get("activation_checkpoint")
        if checkpoint is None:
            continue
        selected_checkpoint = selected_activation_checkpoint(
            comments, workstream_id=workstream_id,
            transition_event_id=event["event_id"],
            target_plan_revision=event["value"]["to"]["plan_revision"],
            authenticated_route=authenticated_route,
        )
        if selected_checkpoint is not None:
            result.append(selected_checkpoint)
    return result


def reduce_generation_checkpoint_comments(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str],
):
    """Reduce checkpoints authorized by the selected generation chain only."""
    carried = None
    if generation_controls(comments):
        selected = select_plan_generation(
            comments, workstream_id=workstream_id,
            description_plan_revision=None,
            authenticated_route=authenticated_route,
        )
        carried = selected_activation_checkpoints(
            comments, workstream_id=workstream_id,
            transition_event_id=selected["transition_tip_event_id"],
            active_plan_revision=selected["plan_revision"],
            authenticated_route=authenticated_route,
        )
    return reduce_checkpoint_comments(
        comments, workstream_id=workstream_id,
        selected_activation_checkpoints=carried,
    )


def assert_generation_write_authority(
    comments: list[dict[str, Any]], *, workstream_id: str,
    plan_revision: str | None, authenticated_route: dict[str, str],
    allow_unactivated_candidate_projection: bool = False,
) -> None:
    controls = generation_controls(comments)
    if not controls:
        return
    if plan_revision is None:
        raise WorkstreamGenerationError("generation_writer_epoch_required")
    selected = select_plan_generation(
        comments, workstream_id=workstream_id,
        description_plan_revision=plan_revision,
        authenticated_route=authenticated_route,
    )
    if selected["plan_revision"] == plan_revision:
        return
    controlled_plans = {
        frontier["plan_revision"] for event in controls
        for frontier in (event["value"]["from"], event["value"]["to"])
    }
    if allow_unactivated_candidate_projection and plan_revision not in controlled_plans:
        return
    raise WorkstreamGenerationError(
        f"generation_writer_retired:{plan_revision}:{selected['activation_epoch']}"
    )


CandidateLoader = Callable[[str], dict[str, Any]]
NativeRootLoader = Callable[[], dict[str, Any]]
SourceLoader = Callable[[], dict[str, str]]
OperatorValidator = Callable[[], dict[str, Any]]
OperatorSnapshotValidator = Callable[
    [list[dict[str, Any]], dict[str, Any]], dict[str, Any]
]


def _activation_native_root_snapshot(
    transport: Any, workstream_id: str,
) -> dict[str, Any]:
    """Load every root field bound by durable native-transition custody."""
    return transport.snapshot_for_root(
        workstream_id, include_description=True, include_child_comments=True,
    )


def native_root_activation_proof(
    snapshot: dict[str, Any], *, workstream_id: str,
    issue_id: str, authority: dict[str, str],
) -> dict[str, Any]:
    """Bind a reviewed nonterminal native root readback to one activation."""
    root = snapshot.get("root") or {}
    project = root.get("project") or {}
    team = root.get("team") or {}
    organization = team.get("organization") or {}
    status = root.get("status")
    status_type = root.get("status_type")
    terminal = {str(status).lower(), str(status_type).lower()} & {
        "done", "completed", "cancelled", "canceled", "superseded",
    }
    if (
        root.get("id") != authority.get("root_issue_id")
        or str(root.get("identifier", "")).upper() != workstream_id.upper()
        or project.get("id") != authority.get("project_id")
        or team.get("id") != authority.get("team_id")
        or organization.get("id") != authority.get("workspace_id")
        or not isinstance(root.get("state_id"), str) or not root["state_id"]
        or not isinstance(status, str) or not status
        or not isinstance(status_type, str) or not status_type
    ):
        raise WorkstreamGenerationError("generation_native_root_readback_mismatch")
    if terminal:
        raise WorkstreamGenerationError(
            "generation_activation_requires_reviewed_nonterminal_root"
        )
    if status_type.lower() != "started":
        raise WorkstreamGenerationError(
            "generation_activation_requires_reviewed_in_progress_root"
        )
    value = {
        "schema_version": 1,
        "root_issue_id": root["id"],
        "workstream_id": str(root["identifier"]).upper(),
        "workspace_id": organization["id"],
        "team_id": team["id"],
        "project_id": project["id"],
        "state_id": root["state_id"],
        "status": status,
        "status_type": status_type,
        "updated_at": root.get("updatedAt"),
    }
    material = {
        key: deepcopy(item) for key, item in value.items()
        if key != "updated_at"
    }
    return {
        **value, "sha256": _digest(value),
        "material_sha256": _digest(material),
    }


class GenerationTransport:
    def __init__(
        self, client: Any, *, issue_id: str, workstream_id: str,
        authority: dict[str, str], candidate_loader: CandidateLoader,
        legacy_description_plan_revision: str | None = None,
        native_root_loader: NativeRootLoader | None = None,
        source_loader: SourceLoader | None = None,
        operator_validator: OperatorValidator | None = None,
        operator_snapshot_validator: OperatorSnapshotValidator | None = None,
        operator_contract_sha256: str | None = None,
        operator_remote_head: str | None = None,
    ):
        self.client = client
        self.issue_id = issue_id
        self.workstream_id = workstream_id.upper()
        self.authority = dict(authority)
        self.candidate_loader = candidate_loader
        self.legacy_description_plan_revision = legacy_description_plan_revision
        self.native_root_loader = native_root_loader
        self.source_loader = source_loader
        self.operator_validator = operator_validator
        self.operator_snapshot_validator = operator_snapshot_validator
        if operator_contract_sha256 is not None and not HEX64.fullmatch(
            operator_contract_sha256
        ):
            raise WorkstreamGenerationError("generation_operator_contract_digest_invalid")
        self.operator_contract_sha256 = operator_contract_sha256
        if operator_remote_head is not None and not re.fullmatch(
            r"[0-9a-f]{40}(?:[0-9a-f]{24})?", operator_remote_head,
        ):
            raise WorkstreamGenerationError(
                "generation_operator_remote_head_invalid"
            )
        self.operator_remote_head = operator_remote_head
        self._required_reservation: tuple[str, str] | None = None
        self._capability_checked = False

    def _activation_protocol_remote_head(
        self, activation_checkpoint: dict[str, Any] | None,
        remote_head: str | None,
    ) -> str | None:
        if (
            self.operator_remote_head is not None
            and remote_head is not None
            and remote_head != self.operator_remote_head
        ):
            raise WorkstreamGenerationError(
                "generation_operator_remote_head_mismatch"
            )
        return remote_head if activation_checkpoint is not None else None

    def _checkpoint_custody(
        self, *, target_plan_revision: str, created_at: str,
        remote_head: str, activation_checkpoint: dict[str, Any],
        retirement: dict[str, Any], event: dict[str, Any],
        native_root: dict[str, Any], operator_validation: dict[str, Any],
        source: dict[str, str],
    ) -> dict[str, Any]:
        receipt = operator_validation.get("root_transition_recovery_receipt")
        if (
            not isinstance(receipt, dict)
            or self.operator_contract_sha256 is None
        ):
            raise WorkstreamGenerationError(
                "generation_checkpoint_custody_root_receipt_missing"
            )
        return {
            "schema_version": 1, "workstream_id": self.workstream_id,
            "authority": deepcopy(self.authority),
            "target_plan_revision": target_plan_revision,
            "created_at": created_at, "remote_head": remote_head,
            "operator_contract_sha256": self.operator_contract_sha256,
            "native_root_sha256": native_root["sha256"],
            "native_root_material_sha256": native_root["material_sha256"],
            "root_transition_receipt_ref": receipt["reservation_slot_id"],
            "root_transition_receipt_sha256": receipt["sha256"],
            "activation_checkpoint_sha256": _digest(activation_checkpoint),
            "retirement_sha256": _digest(retirement),
            "prospective_event_id": event["event_id"],
            "prospective_event_sha256": _digest(event),
            "source": deepcopy(source),
        }

    def _append_checkpoint_custody(
        self, value: dict[str, Any],
    ) -> dict[str, Any]:
        _validate_checkpoint_custody(value)
        comments = self._comments()
        existing = reduce_generation_checkpoint_custodies(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        )
        matches = [item for item in existing if {
            key: item[key] for key in value
        } == value]
        if matches:
            if len(matches) != 1:
                raise WorkstreamGenerationError(
                    "generation_checkpoint_custody_ambiguous"
                )
            return matches[0]
        slot = checkpoint_custody_slot_id(value)
        self._capability()
        body = encode_generation_checkpoint_custody(value)
        try:
            self.client.execute(COMMENT_CREATE_MUTATION, {"input": {
                "id": slot, "issueId": self.issue_id, "body": body,
            }})
        except (LinearTransportError, OSError, TimeoutError):
            after = reduce_generation_checkpoint_custodies(
                self._comments(), workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            )
            match = next((item for item in after if item["remote_id"] == slot), None)
            if match is not None and {key: match[key] for key in value} == value:
                return match
            raise WorkstreamGenerationError(
                "generation_checkpoint_custody_slot_lost_reload_required"
            )
        after = reduce_generation_checkpoint_custodies(
            self._comments(), workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        )
        match = next((item for item in after if item["remote_id"] == slot), None)
        if match is None or {key: match[key] for key in value} != value:
            raise WorkstreamGenerationError(
                "generation_checkpoint_custody_not_observed"
            )
        return match

    def _checkpoint_pre_reservation_replay(
        self, comments: list[dict[str, Any]], *, target_plan_revision: str,
        retirement: dict[str, Any], created_at: str,
        activation_checkpoint: dict[str, Any] | None,
        remote_head: str | None, expected_native_root_sha256: str | None,
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        """Authorize the one exact prospective checkpoint append after a crash."""
        if (
            activation_checkpoint is None
            or self.operator_snapshot_validator is None
            or self.native_root_loader is None
            or not isinstance(expected_native_root_sha256, str)
            or not HEX64.fullmatch(expected_native_root_sha256)
        ):
            return None
        custodies = [item for item in reduce_generation_checkpoint_custodies(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        ) if (
            item["target_plan_revision"] == target_plan_revision
            and item["created_at"] == created_at
            and item["remote_head"] == remote_head
            and item["operator_contract_sha256"]
            == self.operator_contract_sha256
            and item["activation_checkpoint_sha256"]
            == _digest(activation_checkpoint)
            and item["retirement_sha256"] == _digest(retirement)
        )]
        if not custodies:
            return None
        if len(custodies) != 1:
            raise WorkstreamGenerationError(
                "generation_checkpoint_custody_ambiguous"
            )
        custody = custodies[0]
        if expected_native_root_sha256 != custody["native_root_sha256"]:
            raise WorkstreamGenerationError(
                "generation_checkpoint_custody_native_root_mismatch"
            )
        target = self._states(comments, target_plan_revision)[0]
        observed_matches = [
            event for event in target.events
            if event.get("event_id") == custody["prospective_event_id"]
        ]
        if len(observed_matches) > 1:
            raise WorkstreamGenerationError(
                "generation_checkpoint_pre_reservation_event_mismatch"
            )
        observed = observed_matches[0] if observed_matches else None
        if observed is not None:
            if target.events[-1] != observed:
                raise WorkstreamGenerationError(
                    "generation_checkpoint_pre_reservation_event_mismatch"
                )
            prior_events = list(target.events[:-1])
            prior_disposition = next((
                event for event in reversed(prior_events)
                if (event.get("kind"), event.get("key"))
                == ("disposition", "root")
            ), None)
            prior_snapshot = deepcopy(target.snapshot)
            if prior_disposition is None:
                prior_snapshot.pop("disposition", None)
            else:
                prior_snapshot["disposition"] = deepcopy(
                    prior_disposition["value"]
                )
            prior = SimpleNamespace(
                revision=target.revision - 1, events=prior_events,
                snapshot=prior_snapshot,
            )
        else:
            prior = target
        material = reduce_event_comments(
            comments, workstream_id=self.workstream_id,
        )
        _desired, expected = _prospective_activation_checkpoint(
            activation_checkpoint, workstream_id=self.workstream_id,
            target_plan_revision=target_plan_revision,
            material_revision=material.revision, target_state=prior,
            remote_head=remote_head, created_at=created_at,
            authority=self.authority,
        )
        if (
            expected is None
            or expected["event_id"] != custody["prospective_event_id"]
            or _digest(expected) != custody["prospective_event_sha256"]
            or observed is not None and observed != expected
        ):
            raise WorkstreamGenerationError(
                "generation_checkpoint_pre_reservation_event_mismatch"
            )
        matches = []
        if observed is not None:
            for comment in comments:
                body = str(comment.get("body") or "")
                if PROJECTION_PREFIX not in body:
                    continue
                encoded = PROJECTION_RE.findall(body)
                if len(encoded) != 1 or body.count(PROJECTION_PREFIX) != 1:
                    raise WorkstreamGenerationError("malformed_projection_marker")
                event = decode_projection_receipt(comment, encoded[0])
                if event.get("event_id") == observed["event_id"]:
                    matches.append(comment)
        if observed is not None and (
            len(matches) != 1 or matches[0].get("id") != projection_slot_id(
                self.workstream_id, target_plan_revision,
                observed["expected_revision"], self.authority,
            )
        ):
            raise WorkstreamGenerationError(
                "generation_checkpoint_pre_reservation_event_mismatch"
            )
        excluded_ids = {custody["remote_id"]}
        if matches:
            excluded_ids.add(matches[0]["id"])
        reviewed_comments = [
            item for item in comments if item.get("id") not in excluded_ids
        ]
        graph = self.native_root_loader()
        validation = self.operator_snapshot_validator(
            deepcopy(reviewed_comments), deepcopy(graph),
        )
        if (
            not isinstance(validation, dict)
            or validation.get("retirement_proof") != retirement
            or not isinstance(
                validation.get("root_transition_recovery_receipt"), dict,
            )
            or validation["root_transition_recovery_receipt"].get("sha256")
            != custody["root_transition_receipt_sha256"]
            or validation["root_transition_recovery_receipt"].get(
                "reservation_slot_id"
            ) != custody["root_transition_receipt_ref"]
        ):
            raise WorkstreamGenerationError(
                "generation_checkpoint_pre_reservation_operator_mismatch"
            )
        native = native_root_activation_proof(
            graph, workstream_id=self.workstream_id,
            issue_id=self.issue_id, authority=self.authority,
        )
        if native["material_sha256"] != custody["native_root_material_sha256"]:
            raise WorkstreamGenerationError(
                "generation_checkpoint_custody_native_root_material_mismatch"
            )
        native["sha256"] = custody["native_root_sha256"]
        return validation, native

    def _validate_operator(
        self, retirement: dict[str, Any],
    ) -> dict[str, Any] | None:
        if self.operator_validator is None:
            return None
        authorization = self.operator_validator()
        if (
            not isinstance(authorization, dict)
            or authorization.get("retirement_proof") != retirement
        ):
            raise WorkstreamGenerationError(
                "generation_operator_contract_live_state_drift"
            )
        return authorization

    def _pending_operator_replay(
        self, comments: list[dict[str, Any]], *, target_plan_revision: str,
        retirement: dict[str, Any], created_at: str,
    ) -> bool:
        """Recognize custody reserved by this exact reviewed contract.

        The first operator validation happens before any activation-side write.
        Once its schema-v5 reservation is durably read back with the exact
        contract digest, replay must use that fenced custody: a prospective
        activation checkpoint may already have advanced the inert target
        projection, so recomputing the pre-write prepare contract would be both
        stale and impossible by construction.
        """
        if self.operator_contract_sha256 is None:
            return False
        selected = select_plan_generation(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=self.legacy_description_plan_revision,
            authenticated_route=self.authority,
        )
        from_plan = selected["plan_revision"]
        if from_plan == target_plan_revision:
            return False
        epoch = (
            selected["activation_epoch"]
            if selected["activation_epoch"] is not None else -1
        ) + 1
        reservation = self._matching_reservation(
            comments, mode="activate", from_plan=from_plan,
            to_plan=target_plan_revision, epoch=epoch,
            previous_control=selected["transition_tip_event_id"],
            retirement=retirement, created_at=created_at,
        )
        if reservation is None:
            return False
        if (
            reservation.get("schema_version") not in {5, 6}
            or reservation.get("operator_contract_sha256")
            != self.operator_contract_sha256
        ):
            raise WorkstreamGenerationError(
                "generation_pending_operator_contract_mismatch"
            )
        if reservation.get("schema_version") == 6:
            from workstream_root_transition import (
                _decode as decode_root_transition,
                reopen_transition_witness_context,
            )
            if self.native_root_loader is None:
                raise WorkstreamGenerationError(
                    "generation_pending_root_transition_witness_missing"
                )
            root_snapshot = self.native_root_loader()
            native = native_root_activation_proof(
                root_snapshot, workstream_id=self.workstream_id,
                issue_id=self.issue_id, authority=self.authority,
            )
            if native["material_sha256"] != reservation.get(
                "native_root_material_sha256"
            ):
                raise WorkstreamGenerationError(
                    "generation_pending_native_root_material_mismatch"
                )
            ref = reservation.get("root_transition_receipt_ref")
            matches = [item for item in comments if item.get("id") == ref]
            if len(matches) != 1:
                raise WorkstreamGenerationError(
                    "generation_pending_root_transition_witness_missing"
                )
            root_reservation = decode_root_transition(
                str(matches[0].get("body") or "")
            )
            target_state = (root_reservation.get("after") or {}).get("state")
            contract_sha = (root_reservation.get(
                "operator_authorization"
            ) or {}).get("contract_sha256")
            context = reopen_transition_witness_context(
                comments=comments, graph=root_snapshot,
                token=self.workstream_id, authority=self.authority,
                contract_sha256=str(contract_sha or ""),
                target_state=target_state, expected_slot=ref,
                require_original_frontier=False,
                operator_contract_sha256=self.operator_contract_sha256,
            )
            if (
                context is None
                or context["receipt"]["sha256"] != reservation.get(
                    "root_transition_receipt_sha256"
                )
            ):
                raise WorkstreamGenerationError(
                    "generation_pending_root_transition_witness_mismatch"
                )
        target_state = self._states(comments, target_plan_revision)[0]
        seals = [
            event for event in target_state.events
            if event["kind"] == "generation_candidate_seal"
            and event["key"] == reservation["reservation_id"]
        ]
        if not seals:
            if reservation.get("schema_version") == 6:
                # The exact reservation already carries the recovered native
                # transition receipt and material-root proof.  That durable
                # custody is sufficient before the candidate seal exists;
                # recomputing the pre-transition operator contract here would
                # inspect the protocol's own reservation comment as drift.
                return True
            # Only checkpoint-bound activation intentionally advances the
            # inert target between the first review and the seal. Ordinary
            # activation can and must still recompute the live operator
            # contract here so an injected target drift is diagnosed before
            # the seal attempt.
            return reservation.get("activation_checkpoint") is not None
        if len(seals) != 1:
            raise WorkstreamGenerationError("generation_pending_candidate_seal_ambiguous")
        value = seals[0]["value"]
        if (
            value.get("reservation_sha256") != reservation["reservation_sha256"]
            or value.get("retirement") != retirement
            or value.get("source") != reservation["source"]
            or value.get("previous_control_event_id")
            != reservation["previous_control_event_id"]
            or value.get("activation_epoch") != reservation["activation_epoch"]
            or (value.get("from") or {}).get("plan_revision") != from_plan
            or (value.get("to") or {}).get("plan_revision")
            != target_plan_revision
        ):
            raise WorkstreamGenerationError(
                "generation_pending_candidate_seal_mismatch"
            )
        return True

    def _native_root_proof(
        self, expected_sha256: str | None, *, require_reviewed: bool,
        expected_material_sha256: str | None = None,
    ) -> dict[str, Any] | None:
        if self.native_root_loader is None:
            return None
        proof = native_root_activation_proof(
            self.native_root_loader(), workstream_id=self.workstream_id,
            issue_id=self.issue_id, authority=self.authority,
        )
        if require_reviewed and (
            not isinstance(expected_sha256, str)
            or not hmac.compare_digest(proof["sha256"], expected_sha256)
        ):
            raise WorkstreamGenerationError(
                "generation_native_root_review_proof_mismatch:"
                f"expected {proof['sha256']} from preview"
            )
        if (
            expected_material_sha256 is not None
            and proof["material_sha256"] != expected_material_sha256
        ):
            raise WorkstreamGenerationError(
                "generation_native_root_material_proof_mismatch"
            )
        return proof

    def _assert_source_current(
        self, expected: dict[str, str] | None,
    ) -> dict[str, str] | None:
        if self.source_loader is None:
            return None
        observed = self.source_loader()
        if (
            not isinstance(observed, dict)
            or set(observed) < {"identity", "sha256"}
            or not isinstance(observed["identity"], str)
            or not observed["identity"]
            or not HEX64.fullmatch(str(observed["sha256"]))
        ):
            raise WorkstreamGenerationError(
                "generation_canonical_source_readback_invalid"
            )
        normalized = {
            "identity": observed["identity"], "sha256": observed["sha256"],
        }
        if expected is not None and normalized != expected:
            raise WorkstreamGenerationError(
                "generation_canonical_source_changed_during_activation"
            )
        return normalized

    def _post_protocol_native_root_proof(
        self, reservation: dict[str, Any], expected_sha256: str | None,
    ) -> dict[str, Any] | None:
        if reservation.get("schema_version") == 6:
            return self._native_root_proof(
                None, require_reviewed=False,
                expected_material_sha256=reservation[
                    "native_root_material_sha256"
                ],
            )
        return self._native_root_proof(
            expected_sha256, require_reviewed=True,
        )

    def _comments(self) -> list[dict[str, Any]]:
        adapter = LinearProjectionAdapter(
            self.client, issue_id=self.issue_id, workstream_id=self.workstream_id,
            plan_revision="0" * 64, **self.authority,
        )
        return adapter._comments()

    def _assert_required_reservation_present(
        self, comments: list[dict[str, Any]],
    ) -> None:
        if self._required_reservation is None:
            return
        reservation_id, reservation_sha256 = self._required_reservation
        if not any(
            item["reservation_id"] == reservation_id
            and item["reservation_sha256"] == reservation_sha256
            for item in reduce_generation_reservations(
                comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            )
        ):
            raise WorkstreamGenerationError(
                "generation_continue_reservation_lost"
            )

    def _capability(self) -> None:
        if self._capability_checked:
            return
        result = self.client.execute(COMMENT_CREATE_CAPABILITY_QUERY, {})
        fields = ((result.get("__type") or {}).get("inputFields") or [])
        if "id" not in {item.get("name") for item in fields if isinstance(item, dict)}:
            raise WorkstreamGenerationError("linear_comment_create_id_capability_unavailable")
        self._capability_checked = True

    def _states(self, comments: list[dict[str, Any]], *plans: str):
        return [reduce_projection_comments(
            comments, workstream_id=self.workstream_id,
            expected_plan_revision=plan, authenticated_route=self.authority,
        ) for plan in plans]

    def _candidate(
        self, plan: str, comments: list[dict[str, Any]], *,
        activation_checkpoint: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        state = self._states(comments, plan)[0]
        source = state.snapshot.get("source") or {}
        source = {"identity": source.get("identity") or source.get("url"),
                  "sha256": source.get("sha256")}
        material = reduce_event_comments(comments, workstream_id=self.workstream_id)
        checkpoint_ids = sorted(
            item["event_id"] for item in reduce_generation_checkpoint_comments(
                comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            ).checkpoints if item["plan_revision"] == plan
        )
        if activation_checkpoint is not None:
            if activation_checkpoint.get("plan_revision") != plan:
                raise WorkstreamGenerationError(
                    "generation_activation_checkpoint_mismatch"
                )
            checkpoint_ids = sorted(set([
                *checkpoint_ids, activation_checkpoint["event_id"],
            ]))
        receipt = self.candidate_loader(plan)
        _validate_candidate_receipt(
            receipt, plan_revision=plan, authority=self.authority, source=source,
            material_revision=material.revision,
            checkpoint_event_ids=checkpoint_ids, projection_revision=state.revision,
        )
        return {"state": state, "source": source, "receipt": receipt,
                "material": material, "checkpoint_ids": checkpoint_ids}

    def _validate_retirement_frontier(
        self, comments: list[dict[str, Any]], *, from_plan: str,
        retirement: dict[str, Any], from_state: Any | None = None,
        checkpoints: Any | None = None,
    ) -> dict[str, list[str]]:
        """Fence the exact active predecessor writers before any append."""
        if from_state is None:
            from_state = self._states(comments, from_plan)[0]
        if checkpoints is None:
            checkpoints = reduce_generation_checkpoint_comments(
                comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            )
        provenance_heads: dict[str, dict[str, Any]] = {}
        for event in from_state.events:
            if event["kind"] == "provenance":
                provenance_heads[event["key"]] = event
        expected_provenance_ids = sorted(
            event["event_id"] for event in provenance_heads.values()
            if event["value"] != {"_projection_tombstone": True}
        )
        expected_predecessor_checkpoints = sorted(
            item["event_id"] for item in checkpoints.checkpoints
            if item["plan_revision"] == from_plan
        )
        if (
            retirement["provenance_event_ids"] != expected_provenance_ids
            or retirement["checkpoint_event_ids"]
            != expected_predecessor_checkpoints
        ):
            raise WorkstreamGenerationError(
                "generation_retirement_frontier_mismatch"
            )
        if retirement.get("schema_version") == 2:
            from workstream_projection import projection_review_contract

            quiescence = retirement.get("authenticated_quiescence") or {}
            selected = select_plan_generation(
                comments, workstream_id=self.workstream_id,
                description_plan_revision=self.legacy_description_plan_revision,
                authenticated_route=self.authority,
            )
            material = reduce_event_comments(
                comments, workstream_id=self.workstream_id,
            )
            expected_quiescence = {
                "schema_version": 1,
                "observed_at": retirement["retired_at"],
                "authenticated_route": self.authority,
                "selected_generation": {
                    "plan_revision": selected["plan_revision"],
                    "activation_epoch": selected["activation_epoch"],
                    "transition_tip_event_id": selected[
                        "transition_tip_event_id"
                    ],
                },
                "material_revision": material.revision,
                "material_event_ids": sorted(
                    event.event_id for event in material.events
                ),
                "checkpoint_event_ids": expected_predecessor_checkpoints,
                "predecessor_projection": projection_review_contract(from_state),
                "ordering": (
                    "activation_reservation_must_follow_exact_frontiers_and_blocks_"
                    "upgraded_predecessor_writers"
                ),
            }
            if quiescence != expected_quiescence:
                raise WorkstreamGenerationError(
                    "generation_retirement_quiescence_frontier_mismatch"
                )
        return {
            "provenance_event_ids": expected_provenance_ids,
            "checkpoint_event_ids": expected_predecessor_checkpoints,
        }

    def _prepared_activation_checkpoint_id(
        self, comments: list[dict[str, Any]], *, target_plan_revision: str,
        target_state: Any,
        activation_checkpoint: dict[str, Any] | None,
    ) -> str | None:
        target_disposition = target_state.snapshot.get("disposition")
        prepared = (
            target_disposition.get("recovered_from_checkpoint")
            if isinstance(target_disposition, dict) else None
        )
        target_checkpoint_ids = {
            item["event_id"]
            for item in reduce_generation_checkpoint_comments(
                comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            ).checkpoints
            if item["plan_revision"] == target_plan_revision
        }
        if (
            isinstance(prepared, str)
            and prepared not in target_checkpoint_ids
            and (
                activation_checkpoint is None
                or activation_checkpoint.get("event_id") != prepared
                or activation_checkpoint.get("plan_revision")
                != target_plan_revision
            )
        ):
            raise WorkstreamGenerationError(
                "generation_prepared_activation_checkpoint_required"
            )
        return prepared

    def _reservation(
        self, *, comments: list[dict[str, Any]], mode: str, from_plan: str,
        to_plan: str, epoch: int, previous_control: str | None,
        candidate: dict[str, Any], retirement: dict[str, Any], created_at: str,
        native_root_sha256: str | None = None,
        activation_checkpoint: dict[str, Any] | None = None,
        remote_head: str | None = None,
        operator_contract_sha256: str | None = None,
        root_transition_receipt: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._required_reservation is not None:
            raise WorkstreamGenerationError(
                "generation_continue_new_reservation_forbidden"
            )
        checkpoints = reduce_generation_checkpoint_comments(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        )
        all_checkpoint_ids = sorted(
            item["event_id"] for item in checkpoints.checkpoints
        )
        assert_no_pending_ledger_reservation(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
            current_plan_revision=from_plan,
        )
        assert_no_pending_generation_reservation(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        )
        frontier = ledger_serialization_frontier(
            all_checkpoint_ids, comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
            current_plan_revision=from_plan,
            material_revision=candidate["material"].revision,
        )
        from_state, to_state = self._states(comments, from_plan, to_plan)
        self._validate_retirement_frontier(
            comments, from_plan=from_plan, retirement=retirement,
            from_state=from_state, checkpoints=checkpoints,
        )
        unsigned = {
            "schema_version": (
                6 if root_transition_receipt is not None
                else (5 if operator_contract_sha256 is not None
                else (4 if native_root_sha256 is not None else 2)
                )
            ),
            "workstream_id": self.workstream_id,
            "authority": self.authority, "mode": mode,
            "from_plan_revision": from_plan, "to_plan_revision": to_plan,
            "activation_epoch": epoch,
            "previous_control_event_id": previous_control,
            "source": candidate["source"],
            "material_revision": candidate["material"].revision,
            "checkpoint_event_ids": all_checkpoint_ids,
            "ledger_frontier": frontier,
            "from_projection_revision": from_state.revision,
            "to_projection_revision": to_state.revision,
            "graph_frontier_sha256": candidate["receipt"]["graph_frontier_sha256"],
            "candidate_resume_sha256": candidate["receipt"]["snapshot_sha256"],
            "retirement": retirement, "created_at": created_at,
        }
        if native_root_sha256 is not None:
            if not HEX64.fullmatch(native_root_sha256):
                raise WorkstreamGenerationError(
                    "generation_native_root_review_proof_mismatch"
                )
            unsigned["native_root_sha256"] = native_root_sha256
            unsigned["activation_checkpoint"] = deepcopy(activation_checkpoint)
            unsigned["remote_head"] = remote_head
        if operator_contract_sha256 is not None:
            unsigned["operator_contract_sha256"] = operator_contract_sha256
        if root_transition_receipt is not None:
            if native_root_sha256 is None:
                raise WorkstreamGenerationError(
                    "generation_root_transition_receipt_native_root_required"
                )
            unsigned.update({
                "native_root_material_sha256": root_transition_receipt[
                    "native_root_material_sha256"
                ],
                "root_transition_receipt_ref": root_transition_receipt[
                    "reservation_slot_id"
                ],
                "root_transition_receipt_sha256": root_transition_receipt[
                    "sha256"
                ],
            })
        value = {**unsigned, "reservation_id": "wsgr_" + _digest(unsigned)[:32]}
        _validate_reservation(value)
        return value

    def _append_reservation(self, reservation: dict[str, Any]) -> dict[str, Any]:
        comments = self._comments()
        existing = next((item for item in reduce_generation_reservations(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        ) if item["reservation_id"] == reservation["reservation_id"]), None)
        if existing:
            if {key: existing[key] for key in reservation} != reservation:
                raise WorkstreamGenerationError("generation_reservation_replay_mismatch")
            return existing
        if self._required_reservation is not None:
            raise WorkstreamGenerationError(
                "generation_continue_reservation_lost_before_append"
            )
        slot = ledger_boundary_slot_id(
            self.workstream_id, reservation["material_revision"],
            reservation["ledger_frontier"], self.authority,
        )
        self._capability()
        try:
            response = self.client.execute(COMMENT_CREATE_MUTATION, {"input": {
                "id": slot, "issueId": self.issue_id,
                "body": encode_generation_reservation(reservation),
            }})
        except (LinearTransportError, OSError, TimeoutError):
            after = self._comments()
            existing = next((item for item in reduce_generation_reservations(
                after, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            ) if item["reservation_id"] == reservation["reservation_id"]), None)
            if existing:
                return existing
            raise WorkstreamGenerationError("generation_reservation_slot_lost_reload_required")
        created = response.get("commentCreate") or {}
        if created.get("success") is not True or (created.get("comment") or {}).get("id") != slot:
            raise WorkstreamGenerationError("generation_reservation_unconfirmed")
        after = self._comments()
        existing = next((item for item in reduce_generation_reservations(
            after, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        ) if item["reservation_id"] == reservation["reservation_id"]), None)
        if not existing:
            raise WorkstreamGenerationError("generation_reservation_not_observed")
        return existing

    def _append_finalization(
        self, *, reservation: dict[str, Any], transition: dict[str, Any],
        native_root: dict[str, Any], created_at: str,
    ) -> dict[str, Any]:
        unsigned = {
            "schema_version": 1, "workstream_id": self.workstream_id,
            "authority": self.authority,
            "reservation_id": reservation["reservation_id"],
            "reservation_sha256": reservation["reservation_sha256"],
            "transition_event_id": transition["event_id"],
            "transition_sha256": _digest(transition),
            "native_root_sha256": (
                reservation["native_root_sha256"]
                if reservation.get("schema_version") == 6
                else native_root["sha256"]
            ),
            "source": deepcopy(reservation["source"]),
            "execution_status": {
                "authority": "generation_local",
                "name": "In Progress",
                "type": "started",
            },
            "created_at": created_at,
        }
        value = {
            **unsigned, "finalization_id": "wsgf_" + _digest(unsigned)[:32],
        }
        _validate_finalization(value)
        comments = self._comments()
        existing = next((item for item in reduce_generation_finalizations(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        ) if item["finalization_id"] == value["finalization_id"]), None)
        if existing is not None:
            if {key: existing[key] for key in value} != value:
                raise WorkstreamGenerationError(
                    "generation_finalization_replay_mismatch"
                )
            return existing
        slot = generation_finalization_slot_id(value)
        self._capability()
        body = encode_generation_finalization(value)
        try:
            response = self.client.execute(COMMENT_CREATE_MUTATION, {"input": {
                "id": slot, "issueId": self.issue_id, "body": body,
            }})
        except (LinearTransportError, OSError, TimeoutError):
            after = self._comments()
            existing = next((item for item in reduce_generation_finalizations(
                after, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            ) if item["finalization_id"] == value["finalization_id"]), None)
            if existing is not None:
                return existing
            raise WorkstreamGenerationError(
                "generation_finalization_slot_lost_reload_required"
            )
        created = response.get("commentCreate") or {}
        if (
            created.get("success") is not True
            or (created.get("comment") or {}).get("id") != slot
        ):
            raise WorkstreamGenerationError("generation_finalization_unconfirmed")
        after = self._comments()
        existing = next((item for item in reduce_generation_finalizations(
            after, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        ) if item["finalization_id"] == value["finalization_id"]), None)
        if existing is None:
            raise WorkstreamGenerationError("generation_finalization_not_observed")
        return existing

    def _matching_reservation(
        self, comments: list[dict[str, Any]], *, mode: str, from_plan: str,
        to_plan: str, epoch: int, previous_control: str | None,
        retirement: dict[str, Any], created_at: str,
    ) -> dict[str, Any] | None:
        """Discover an exact crashed operation before applying pending guards."""
        reservations = reduce_generation_reservations(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        )
        matches = [
            item for item in reservations
            if item["mode"] == mode
            and item["from_plan_revision"] == from_plan
            and item["to_plan_revision"] == to_plan
            and item["activation_epoch"] == epoch
            and item["previous_control_event_id"] == previous_control
            and item["retirement"] == retirement
            and item["created_at"] == created_at
        ]
        if len(matches) > 1:
            raise WorkstreamGenerationError("generation_operation_replay_ambiguous")
        if self._required_reservation is not None:
            required_id, required_sha256 = self._required_reservation
            exact = [
                item for item in reservations
                if item["reservation_id"] == required_id
                and item["reservation_sha256"] == required_sha256
            ]
            if len(exact) != 1:
                raise WorkstreamGenerationError(
                    "generation_continue_reservation_lost"
                )
            if len(matches) != 1 or matches[0] != exact[0]:
                raise WorkstreamGenerationError(
                    "generation_continue_reservation_inputs_mismatch"
                )
        return matches[0] if matches else None

    def _assert_reservation_live(
        self, comments: list[dict[str, Any]], reservation: dict[str, Any],
    ) -> None:
        live = pending_generation_reservations(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        )
        if not any(
            item["reservation_id"] == reservation["reservation_id"]
            and item["reservation_sha256"] == reservation["reservation_sha256"]
            for item in live
        ):
            if self._required_reservation is not None:
                all_reservations = reduce_generation_reservations(
                    comments, workstream_id=self.workstream_id,
                    authenticated_route=self.authority,
                )
                token = (
                    f"{reservation['reservation_id']}:"
                    f"{reservation['reservation_sha256']}"
                )
                if token in _generation_abort_ids(comments, all_reservations):
                    raise WorkstreamGenerationError(
                        "generation_continue_reservation_aborted"
                    )
                raise WorkstreamGenerationError(
                    "generation_continue_reservation_completed_during_execution_retry"
                )
            raise WorkstreamGenerationError("generation_reservation_aborted_or_completed")

    def _historical_replay(
        self, comments: list[dict[str, Any]], *, from_plan: str, to_plan: str,
        expected_retirement: dict[str, Any] | None = None,
        expected_created_at: str | None = None,
        validate_activation_inputs: bool = False,
        expected_activation_checkpoint: dict[str, Any] | None = None,
        expected_remote_head: str | None = None,
    ) -> dict[str, Any] | None:
        for state in self._states(comments, from_plan):
            matching = [event for event in state.events
                        if event["kind"] in {"generation_genesis", "generation_transition"}
                        and event["value"]["from"]["plan_revision"] == from_plan
                        and event["value"]["to"]["plan_revision"] == to_plan
                        and (expected_retirement is None
                             or event["value"]["retirement"] == expected_retirement)
                        and (expected_created_at is None
                             or event["created_at"] == expected_created_at)]
            if self._required_reservation is not None and matching:
                required_id, required_sha256 = self._required_reservation
                if any(
                    event["value"].get("reservation_id") != required_id
                    or event["value"].get("reservation_sha256")
                    != required_sha256
                    for event in matching
                ):
                    raise WorkstreamGenerationError(
                        "generation_continue_historical_reservation_mismatch"
                    )
            if len(matching) > 1:
                raise WorkstreamGenerationError("generation_historical_replay_ambiguous")
            if matching:
                select_plan_generation(
                    comments, workstream_id=self.workstream_id,
                    description_plan_revision=from_plan,
                    authenticated_route=self.authority,
                )
                event = matching[0]
                reservation_matches = [
                    item for item in reduce_generation_reservations(
                        comments, workstream_id=self.workstream_id,
                        authenticated_route=self.authority,
                    )
                    if item["reservation_id"]
                    == event["value"]["reservation_id"]
                    and item["reservation_sha256"]
                    == event["value"]["reservation_sha256"]
                ]
                if len(reservation_matches) > 1:
                    raise WorkstreamGenerationError(
                        "generation_historical_reservation_ambiguous"
                    )
                if reservation_matches and (
                    reservation_matches[0].get("schema_version") in {5, 6}
                    and reservation_matches[0].get("operator_contract_sha256")
                    != self.operator_contract_sha256
                ):
                    raise WorkstreamGenerationError(
                        "generation_historical_operator_contract_mismatch"
                    )
                if validate_activation_inputs:
                    carried = event["value"].get("activation_checkpoint")
                    if carried != expected_activation_checkpoint:
                        raise WorkstreamGenerationError(
                            "generation_historical_replay_checkpoint_mismatch"
                        )
                    if carried is None:
                        if expected_remote_head is not None:
                            raise WorkstreamGenerationError(
                                "generation_historical_replay_remote_head_mismatch"
                            )
                    else:
                        target = self._states(comments, to_plan)[0]
                        bound_revision = event["value"]["to"][
                            "projection_revision"
                        ]
                        disposition_event = next((
                            item for item in reversed(
                                target.events[:bound_revision]
                            )
                            if item["kind"] == "disposition"
                            and item["key"] == "root"
                        ), None)
                        disposition = (
                            disposition_event["value"]
                            if disposition_event is not None else None
                        )
                        if (
                            not isinstance(disposition, dict)
                            or disposition.get("recovered_from_checkpoint")
                            != carried["event_id"]
                            or disposition.get("remote_head") != expected_remote_head
                        ):
                            raise WorkstreamGenerationError(
                                "generation_historical_replay_remote_head_mismatch"
                            )
                return {"event_id": event["event_id"],
                        "remote_id": state.remote_ids[event["event_id"]],
                        "revision": event["expected_revision"] + 1,
                        "activated_plan_revision": event["value"]["to"]["plan_revision"],
                        "bound_graph_frontier_sha256": event["value"]["graph_frontier_sha256"],
                        "bound_candidate_resume_sha256": event["value"]["candidate_resume_sha256"],
                        "replay": True,
                        **({"prepared_schema_version": 4}
                           if event["value"].get("schema_version") == 4 else {})}
        return None

    def continue_reservation(
        self, *, reservation_id: str, reservation_sha256: str,
    ) -> dict[str, Any]:
        """Finish one exact durable schema-v6 activation without local files."""
        if not RESERVATION_ID.fullmatch(str(reservation_id)):
            raise WorkstreamGenerationError(
                "generation_continue_reservation_id_invalid"
            )
        if not HEX64.fullmatch(str(reservation_sha256)):
            raise WorkstreamGenerationError(
                "generation_continue_reservation_sha256_invalid"
            )
        comments = self._comments()
        reservations = reduce_generation_reservations(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        )
        by_id = [
            item for item in reservations
            if item["reservation_id"] == reservation_id
        ]
        if not by_id:
            raise WorkstreamGenerationError(
                "generation_continue_reservation_id_not_found"
            )
        exact = [
            item for item in by_id
            if hmac.compare_digest(
                item["reservation_sha256"], reservation_sha256,
            )
        ]
        if not exact:
            raise WorkstreamGenerationError(
                "generation_continue_reservation_sha256_mismatch"
            )
        if len(exact) != 1:
            raise WorkstreamGenerationError(
                "generation_continue_reservation_ambiguous"
            )
        reservation = exact[0]
        schema_version = reservation.get("schema_version")
        if schema_version != 6:
            raise WorkstreamGenerationError(
                "generation_continue_schema_unavailable:"
                f"schema{schema_version}_requires_exact_reviewed_inputs;"
                "only_schema6_is_self_contained"
            )
        if reservation.get("mode") != "activate":
            raise WorkstreamGenerationError(
                "generation_continue_schema6_activation_required"
            )
        self._assert_source_current(reservation["source"])
        token = f"{reservation_id}:{reservation_sha256}"
        if token in _generation_abort_ids(comments, reservations):
            raise WorkstreamGenerationError(
                "generation_continue_reservation_aborted"
            )
        pending = pending_generation_reservations(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        )
        finalized = reduce_generation_finalizations(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        )
        is_pending = any(
            item["reservation_id"] == reservation_id
            and item["reservation_sha256"] == reservation_sha256
            for item in pending
        )
        is_finalized = any(
            item["reservation_id"] == reservation_id
            and item["reservation_sha256"] == reservation_sha256
            for item in finalized
        )
        if not is_pending and not is_finalized:
            raise WorkstreamGenerationError(
                "generation_continue_reservation_not_pending_or_finalized"
            )
        self._required_reservation = (reservation_id, reservation_sha256)
        try:
            if is_finalized:
                replay = self._historical_replay(
                    comments,
                    from_plan=reservation["from_plan_revision"],
                    to_plan=reservation["to_plan_revision"],
                    expected_retirement=reservation["retirement"],
                    expected_created_at=reservation["created_at"],
                    validate_activation_inputs=True,
                    expected_activation_checkpoint=reservation[
                        "activation_checkpoint"
                    ],
                    expected_remote_head=reservation["remote_head"],
                )
                if replay is None:
                    raise WorkstreamGenerationError(
                        "generation_continue_finalized_transition_missing"
                    )
                return replay
            return self.activate(
                target_plan_revision=reservation["to_plan_revision"],
                created_at=reservation["created_at"],
                retirement=deepcopy(reservation["retirement"]),
                activation_checkpoint=deepcopy(
                    reservation["activation_checkpoint"]
                ),
                remote_head=reservation["remote_head"],
                expected_native_root_sha256=reservation["native_root_sha256"],
            )
        finally:
            self._required_reservation = None

    def abort(
        self, *, reservation_id: str, reservation_sha256: str,
        reason: str, created_at: str,
    ) -> dict[str, Any]:
        """Durably release one exact incomplete reservation without changing authority."""
        comments = self._comments()
        reservations = reduce_generation_reservations(
            comments, workstream_id=self.workstream_id,
            authenticated_route=self.authority,
        )
        matching = [item for item in reservations
                    if item["reservation_id"] == reservation_id
                    and item["reservation_sha256"] == reservation_sha256]
        if len(matching) != 1:
            raise WorkstreamGenerationError("generation_abort_reservation_mismatch")
        if (
            not isinstance(reason, str) or not reason
            or not isinstance(created_at, str) or not created_at
        ):
            raise WorkstreamGenerationError("invalid_generation_abort")
        reservation = matching[0]
        token = f"{reservation_id}:{reservation_sha256}"
        aborted = _generation_abort_ids(comments, reservations)
        if token in aborted:
            state = self._states(comments, reservation["from_plan_revision"])[0]
            event = next(item for item in state.events if (
                item["kind"] == "generation_abort"
                and item["value"]["reservation_id"] == reservation_id
                and item["value"]["reservation_sha256"] == reservation_sha256
            ))
            if event["value"]["reason"] != reason or event["created_at"] != created_at:
                raise WorkstreamGenerationError("generation_abort_replay_mismatch")
            return {
                "reservation_id": reservation_id,
                "remote_id": state.remote_ids[event["event_id"]], "replay": True,
            }
        pending = any(
            item["reservation_id"] == reservation_id
            and item["reservation_sha256"] == reservation_sha256
            for item in pending_generation_reservations(
                comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            )
        )
        if not pending:
            raise WorkstreamGenerationError("generation_abort_after_activation")
        prepared_token = (
            f"generation:{reservation_id}:{reservation_sha256}"
        )
        if prepared_token in generation_ledger_frontier_tokens(
            comments, workstream_id=self.workstream_id,
        ):
            raise WorkstreamGenerationError(
                "generation_abort_after_preparation_replay_required"
            )
        self._capability()
        for _attempt in range(8):
            state = self._states(comments, reservation["from_plan_revision"])[0]
            original_revision = reservation["from_projection_revision"]
            if state.revision < original_revision:
                raise WorkstreamGenerationError("generation_abort_frontier_regressed")
            intervening = list(state.events[original_revision:])
            if any(item["kind"].startswith("generation_") for item in intervening):
                raise WorkstreamGenerationError("generation_abort_after_activation")
            value = {
                "schema_version": 2, "reservation_id": reservation_id,
                "reservation_sha256": reservation_sha256, "reason": reason,
                "original_projection_revision": original_revision,
                "intervening_event_ids": [item["event_id"] for item in intervening],
                "intervening_events_sha256": _digest(intervening),
                "original_occupant_event_id": (
                    intervening[0]["event_id"] if intervening else None
                ),
            }
            event = build_projection_event(
                workstream_id=self.workstream_id, kind="generation_abort",
                key=reservation_id, value=value,
                plan_revision=reservation["from_plan_revision"],
                expected_revision=state.revision,
                created_at=created_at, authority=self.authority,
            )
            body = encode_projection_comment(event)
            slot = generation_abort_slot_id(reservation, state.revision)
            try:
                response = self.client.execute(COMMENT_CREATE_MUTATION, {"input": {
                    "id": slot, "issueId": self.issue_id, "body": body,
                }})
            except (LinearTransportError, OSError, TimeoutError):
                after = self._comments()
                observed = next(
                    (item for item in after if item.get("id") == slot), None,
                )
                if observed is not None and observed.get("body") == body:
                    comments = after
                    break
                # A valid predecessor winner moves the rebased abort CAS. A
                # completed activation is detected before the next attempt.
                if not any(item["reservation_id"] == reservation_id
                           and item["reservation_sha256"] == reservation_sha256
                           for item in pending_generation_reservations(
                               after, workstream_id=self.workstream_id,
                               authenticated_route=self.authority,
                           )):
                    raise WorkstreamGenerationError("generation_abort_after_activation")
                comments = after
                continue
            created = response.get("commentCreate") or {}
            if (created.get("success") is not True
                    or (created.get("comment") or {}).get("id") != slot):
                raise WorkstreamGenerationError("generation_abort_unconfirmed")
            comments = self._comments()
            break
        else:
            raise WorkstreamGenerationError("generation_abort_rebase_limit")
        if any(item["reservation_id"] == reservation_id
               and item["reservation_sha256"] == reservation_sha256
               for item in pending_generation_reservations(
                   comments, workstream_id=self.workstream_id,
                   authenticated_route=self.authority,
               )):
            raise WorkstreamGenerationError("generation_abort_not_observed")
        return {"reservation_id": reservation_id, "remote_id": slot, "replay": False}

    def bootstrap(self, *, target_plan_revision: str, created_at: str) -> dict[str, Any]:
        comments = self._comments()
        if generation_controls(comments):
            selected = select_plan_generation(
                comments, workstream_id=self.workstream_id,
                description_plan_revision=None, authenticated_route=self.authority,
            )
            if selected["authority_origin"] == "generation_genesis" and selected[
                "plan_revision"] == target_plan_revision:
                replay = self._historical_replay(
                    comments, from_plan=target_plan_revision, to_plan=target_plan_revision,
                )
                if replay:
                    return replay
            raise WorkstreamGenerationError("generation_already_bootstrapped")
        if self.legacy_description_plan_revision is not None:
            raise WorkstreamGenerationError(
                "generation_bootstrap_requires_descriptionless_legacy_root"
            )
        retirement = build_retirement_proof(
            predecessor_plan_revision=target_plan_revision, retired_at=created_at,
            retired_writer_epoch=0,
            provenance_event_ids=[event["event_id"] for event in self._states(
                comments, target_plan_revision,
            )[0].events
                                  if event["kind"] == "provenance"],
            checkpoint_event_ids=sorted(
                item["event_id"] for item in reduce_generation_checkpoint_comments(
                    comments, workstream_id=self.workstream_id,
                    authenticated_route=self.authority,
                ).checkpoints if item["plan_revision"] == target_plan_revision
            ),
        )
        reservation = self._matching_reservation(
            comments, mode="bootstrap", from_plan=target_plan_revision,
            to_plan=target_plan_revision, epoch=0, previous_control=None,
            retirement=retirement, created_at=created_at,
        )
        if reservation is None:
            candidate = self._candidate(target_plan_revision, comments)
            reservation = self._reservation(
                comments=comments, mode="bootstrap", from_plan=target_plan_revision,
                to_plan=target_plan_revision, epoch=0, previous_control=None,
                candidate=candidate, retirement=retirement, created_at=created_at,
            )
            stored = self._append_reservation(reservation)
        else:
            stored = reservation
        reservation = stored
        comments = self._comments()
        self._assert_reservation_live(comments, reservation)
        candidate = self._candidate(target_plan_revision, comments)
        if (
            candidate["receipt"]["graph_frontier_sha256"]
            != reservation["graph_frontier_sha256"]
            or candidate["receipt"]["snapshot_sha256"]
            != reservation["candidate_resume_sha256"]
        ):
            raise WorkstreamGenerationError("generation_candidate_changed_after_reservation")
        frontier = _generation_frontier(
            candidate["state"], comments, plan_revision=target_plan_revision,
            material_revision=candidate["material"].revision,
        )
        value = {
            "schema_version": 2, "reservation_id": reservation["reservation_id"],
            "reservation_sha256": stored["reservation_sha256"],
            "from": frontier, "to": frontier, "source": candidate["source"],
            "graph_frontier_sha256": reservation["graph_frontier_sha256"],
            "candidate_resume_sha256": reservation["candidate_resume_sha256"],
            "retirement": retirement, "previous_control_event_id": None,
            "activation_epoch": 0, "candidate_seal_event_id": None,
            "candidate_seal_sha256": None,
        }
        event = build_projection_event(
            workstream_id=self.workstream_id, kind="generation_genesis", key="root",
            value=value, plan_revision=target_plan_revision,
            expected_revision=candidate["state"].revision, created_at=created_at,
            authority=self.authority,
        )
        receipt = LinearProjectionAdapter(
            self.client, issue_id=self.issue_id, workstream_id=self.workstream_id,
            plan_revision=target_plan_revision, **self.authority,
        ).append(event, expected_material_revision=candidate["material"].revision,
                 allowed_generation_reservation_id=reservation["reservation_id"])
        after = self._comments()
        selected = select_plan_generation(
            after, workstream_id=self.workstream_id,
            description_plan_revision=None, authenticated_route=self.authority,
        )
        if selected["plan_revision"] != target_plan_revision:
            raise WorkstreamGenerationError("generation_genesis_not_observed")
        return {
            **receipt,
            "activated_plan_revision": target_plan_revision,
            "bound_graph_frontier_sha256": value["graph_frontier_sha256"],
            "bound_candidate_resume_sha256": value["candidate_resume_sha256"],
            "quarantined_legacy_writes": candidate["receipt"][
                "quarantined_legacy_writes"
            ],
            "replay": False,
        }

    def preview_activate(
        self, *, target_plan_revision: str, created_at: str,
        retirement: dict[str, Any],
        activation_checkpoint: dict[str, Any] | None = None,
        remote_head: str | None = None,
    ) -> dict[str, Any]:
        """Validate activation inputs without creating a remote artifact."""
        protocol_remote_head = self._activation_protocol_remote_head(
            activation_checkpoint, remote_head,
        )
        native_root = self._native_root_proof(None, require_reviewed=False)
        comments = self._comments()
        replay_from = (
            retirement.get("predecessor_plan_revision")
            if isinstance(retirement, dict) else None
        )
        if isinstance(replay_from, str):
            replay = self._historical_replay(
                comments, from_plan=replay_from,
                to_plan=target_plan_revision,
                expected_retirement=retirement,
                expected_created_at=created_at,
                validate_activation_inputs=True,
                expected_activation_checkpoint=activation_checkpoint,
                expected_remote_head=protocol_remote_head,
            )
            if replay:
                return replay
        pending_operator_replay = self._pending_operator_replay(
            comments, target_plan_revision=target_plan_revision,
            retirement=retirement, created_at=created_at,
        )
        operator_validation = None
        if not pending_operator_replay:
            operator_validation = self._validate_operator(retirement)
        selected = select_plan_generation(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=self.legacy_description_plan_revision,
            authenticated_route=self.authority,
        )
        from_plan = selected["plan_revision"]
        if from_plan == target_plan_revision:
            raise WorkstreamGenerationError("generation_target_already_active")
        epoch = (
            selected["activation_epoch"]
            if selected["activation_epoch"] is not None else -1
        ) + 1
        _validate_retirement(retirement, from_plan, epoch)
        retirement_frontier = self._validate_retirement_frontier(
            comments, from_plan=from_plan, retirement=retirement,
        )
        material = reduce_event_comments(
            comments, workstream_id=self.workstream_id,
        )
        target_state = self._states(comments, target_plan_revision)[0]
        target_source = target_state.snapshot.get("source") or {}
        target_source = {
            "identity": target_source.get("identity") or target_source.get("url"),
            "sha256": target_source.get("sha256"),
        }
        self._assert_source_current(target_source)
        self._prepared_activation_checkpoint_id(
            comments, target_plan_revision=target_plan_revision,
            target_state=target_state,
            activation_checkpoint=activation_checkpoint,
        )
        prospective_disposition = None
        prospective_event = None
        if activation_checkpoint is not None:
            prospective_disposition, prospective_event = (
                _prospective_activation_checkpoint(
                    activation_checkpoint, workstream_id=self.workstream_id,
                    target_plan_revision=target_plan_revision,
                    material_revision=material.revision,
                    target_state=target_state, remote_head=remote_head,
                    created_at=created_at, authority=self.authority,
                )
            )
        checkpoint_ids = sorted(
            item["event_id"]
            for item in reduce_generation_checkpoint_comments(
                comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            ).checkpoints
            if item["plan_revision"] == target_plan_revision
        )
        if activation_checkpoint is not None:
            checkpoint_ids = sorted(set([
                *checkpoint_ids, activation_checkpoint["event_id"],
            ]))
        source = target_state.snapshot.get("source") or {}
        source = {
            "identity": source.get("identity") or source.get("url"),
            "sha256": source.get("sha256"),
        }
        receipt = self.candidate_loader(target_plan_revision)
        _validate_candidate_receipt(
            receipt, plan_revision=target_plan_revision,
            authority=self.authority, source=source,
            material_revision=material.revision,
            checkpoint_event_ids=checkpoint_ids,
            projection_revision=(
                target_state.revision + int(prospective_event is not None)
            ),
        )
        previous_control = selected["transition_tip_event_id"]
        matching = self._matching_reservation(
            comments, mode="activate", from_plan=from_plan,
            to_plan=target_plan_revision, epoch=epoch,
            previous_control=previous_control, retirement=retirement,
            created_at=created_at,
        )
        if matching is not None:
            self._assert_reservation_live(comments, matching)
        else:
            assert_no_pending_ledger_reservation(
                comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
                current_plan_revision=from_plan,
            )
            assert_no_pending_generation_reservation(
                comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            )
        return {
            "apply": False,
            "command": "activate",
            "from_plan_revision": from_plan,
            "target_plan_revision": target_plan_revision,
            "activation_epoch": epoch,
            "retirement_frontier": retirement_frontier,
            "prospective_target_disposition": prospective_disposition,
            "prospective_target_disposition_event": deepcopy(
                prospective_event
            ),
            "candidate": receipt,
            "native_root_activation_proof": native_root,
        }

    def activate(
        self, *, target_plan_revision: str, created_at: str,
        retirement: dict[str, Any], activation_checkpoint: dict[str, Any] | None = None,
        remote_head: str | None = None,
        expected_native_root_sha256: str | None = None,
    ) -> dict[str, Any]:
        protocol_remote_head = self._activation_protocol_remote_head(
            activation_checkpoint, remote_head,
        )
        comments = self._comments()
        self._assert_required_reservation_present(comments)
        # Before the first activation write, bind the complete reviewed native
        # observation including updatedAt. On a schema-v6 crash replay,
        # protocol-owned comments may have advanced only that clock; exact
        # custody instead revalidates the witness and stored material digest.
        selected_for_native = select_plan_generation(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=self.legacy_description_plan_revision,
            authenticated_route=self.authority,
        )
        native_matches = [
            item for item in reduce_generation_reservations(
                comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            )
            if item.get("schema_version") == 6
            and item.get("mode") == "activate"
            and item.get("to_plan_revision") == target_plan_revision
            and item.get("retirement") == retirement
            and item.get("created_at") == created_at
            and item.get("operator_contract_sha256")
            == self.operator_contract_sha256
        ]
        if len(native_matches) > 1:
            raise WorkstreamGenerationError(
                "generation_native_root_schema6_custody_ambiguous"
            )
        native_reservation = native_matches[0] if native_matches else None
        checkpoint_replay = None
        if native_reservation is None:
            checkpoint_replay = self._checkpoint_pre_reservation_replay(
                comments, target_plan_revision=target_plan_revision,
                retirement=retirement, created_at=created_at,
                activation_checkpoint=activation_checkpoint,
                remote_head=remote_head,
                expected_native_root_sha256=expected_native_root_sha256,
            )
        if (
            native_reservation is not None
            and native_reservation.get("schema_version") == 6
        ):
            if selected_for_native["plan_revision"] != target_plan_revision:
                self._pending_operator_replay(
                    comments, target_plan_revision=target_plan_revision,
                    retirement=retirement, created_at=created_at,
                )
            native_root = self._native_root_proof(
                None, require_reviewed=False,
                expected_material_sha256=native_reservation[
                    "native_root_material_sha256"
                ],
            )
        elif checkpoint_replay is not None:
            operator_validation, native_root = checkpoint_replay
        else:
            native_root = self._native_root_proof(
                expected_native_root_sha256, require_reviewed=True,
            )
        replay_from = (
            retirement.get("predecessor_plan_revision")
            if isinstance(retirement, dict) else None
        )
        if isinstance(replay_from, str):
            replay = self._historical_replay(
                comments, from_plan=replay_from, to_plan=target_plan_revision,
                expected_retirement=retirement, expected_created_at=created_at,
                validate_activation_inputs=True,
                expected_activation_checkpoint=activation_checkpoint,
                expected_remote_head=protocol_remote_head,
            )
            if replay:
                finalized = finalized_generation_transition_ids(
                    comments, workstream_id=self.workstream_id,
                    authenticated_route=self.authority,
                )
                if (
                    replay.get("prepared_schema_version") != 4
                    or replay["event_id"] in finalized
                ):
                    final_native = (
                        self._post_protocol_native_root_proof(
                            native_reservation, expected_native_root_sha256,
                        )
                        if native_reservation is not None else
                        self._native_root_proof(
                            expected_native_root_sha256, require_reviewed=True,
                        )
                    )
                    result = (
                        {**replay, "native_root_activation_proof": final_native}
                        if final_native is not None else replay
                    )
                    if replay.get("prepared_schema_version") == 4:
                        finalization = next(
                            item for item in reduce_generation_finalizations(
                                comments, workstream_id=self.workstream_id,
                                authenticated_route=self.authority,
                            ) if item["transition_event_id"] == replay["event_id"]
                        )
                        result.update({
                            "two_phase_finalization": finalization,
                            "final_candidate": self.candidate_loader(
                                target_plan_revision
                            ),
                        })
                    return result
                transition = next(
                    event for state in self._states(comments, replay_from)
                    for event in state.events
                    if event["event_id"] == replay["event_id"]
                )
                reservation = next((
                    item for item in pending_generation_reservations(
                        comments, workstream_id=self.workstream_id,
                        authenticated_route=self.authority,
                    )
                    if item["reservation_id"]
                    == transition["value"]["reservation_id"]
                    and item["reservation_sha256"]
                    == transition["value"]["reservation_sha256"]
                ), None)
                if reservation is None:
                    raise WorkstreamGenerationError(
                        "generation_prepared_reservation_missing"
                    )
                if (
                    reservation.get("source") != transition["value"].get("source")
                    or reservation.get("retirement") != retirement
                    or reservation.get("activation_checkpoint")
                    != activation_checkpoint
                    or reservation.get("remote_head") != protocol_remote_head
                    or reservation.get("native_root_sha256")
                    != expected_native_root_sha256
                ):
                    raise WorkstreamGenerationError(
                        "generation_reservation_replay_inputs_mismatch"
                    )
                prepared_post = self._candidate(
                    target_plan_revision, comments,
                    activation_checkpoint=activation_checkpoint,
                )
                if (
                    prepared_post["receipt"]["graph_frontier_sha256"]
                    != transition["value"]["graph_frontier_sha256"]
                    or prepared_post["receipt"]["snapshot_sha256"]
                    != transition["value"]["candidate_resume_sha256"]
                ):
                    raise WorkstreamGenerationError(
                        "generation_prepared_candidate_changed"
                    )
                final_native = self._post_protocol_native_root_proof(
                    reservation, expected_native_root_sha256,
                )
                self._assert_source_current(reservation["source"])
                finalization = self._append_finalization(
                    reservation=reservation, transition=transition,
                    native_root=final_native, created_at=created_at,
                )
                after = self._comments()
                self._assert_source_current(reservation["source"])
                final = select_plan_generation(
                    after, workstream_id=self.workstream_id,
                    description_plan_revision=replay_from,
                    authenticated_route=self.authority,
                )
                if final["plan_revision"] != target_plan_revision:
                    raise WorkstreamGenerationError(
                        "generation_activation_not_observed"
                    )
                return {
                    **replay,
                    "native_root_activation_proof": final_native,
                    "two_phase_finalization": finalization,
                    "final_candidate": prepared_post["receipt"],
                }
        pending_operator_replay = self._pending_operator_replay(
            comments, target_plan_revision=target_plan_revision,
            retirement=retirement, created_at=created_at,
        )
        operator_validation = (
            checkpoint_replay[0] if checkpoint_replay is not None else None
        )
        if not pending_operator_replay:
            if operator_validation is None:
                operator_validation = self._validate_operator(retirement)
        selected = select_plan_generation(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=self.legacy_description_plan_revision,
            authenticated_route=self.authority,
        )
        from_plan = selected["plan_revision"]
        if from_plan == target_plan_revision:
            raise WorkstreamGenerationError("generation_target_already_active")
        epoch = (selected["activation_epoch"] if selected["activation_epoch"] is not None else -1) + 1
        _validate_retirement(retirement, from_plan, epoch)
        # The retirement declaration gates every activation-side append,
        # including a prepared target disposition. Validate it against active
        # predecessor heads and root checkpoints before the first write.
        self._validate_retirement_frontier(
            comments, from_plan=from_plan, retirement=retirement,
        )
        target_state = self._states(comments, target_plan_revision)[0]
        target_source = target_state.snapshot.get("source") or {}
        target_source = {
            "identity": target_source.get("identity") or target_source.get("url"),
            "sha256": target_source.get("sha256"),
        }
        self._assert_source_current(target_source)
        self._prepared_activation_checkpoint_id(
            comments, target_plan_revision=target_plan_revision,
            target_state=target_state,
            activation_checkpoint=activation_checkpoint,
        )
        previous_control = selected["transition_tip_event_id"]
        if activation_checkpoint is not None:
            material = reduce_event_comments(comments, workstream_id=self.workstream_id)
            _desired_disposition, event = _prospective_activation_checkpoint(
                activation_checkpoint, workstream_id=self.workstream_id,
                target_plan_revision=target_plan_revision,
                material_revision=material.revision,
                target_state=target_state, remote_head=remote_head,
                created_at=created_at, authority=self.authority,
            )
            if event is not None:
                broad_match = self._matching_reservation(
                    comments, mode="activate", from_plan=from_plan,
                    to_plan=target_plan_revision, epoch=epoch,
                    previous_control=previous_control,
                    retirement=retirement, created_at=created_at,
                )
                if broad_match is not None:
                    self._assert_reservation_live(comments, broad_match)
                # A canonical replay can only have a reservation after this
                # disposition exists. Refuse unrelated boundary custody before
                # creating the prospective target event.
                assert_no_pending_ledger_reservation(
                    comments, workstream_id=self.workstream_id,
                    authenticated_route=self.authority,
                    current_plan_revision=from_plan,
                )
                assert_no_pending_generation_reservation(
                    comments, workstream_id=self.workstream_id,
                    authenticated_route=self.authority,
                )
                recovery_receipt = (
                    operator_validation.get("root_transition_recovery_receipt")
                    if isinstance(operator_validation, dict) else None
                )
                if checkpoint_replay is None and isinstance(
                    recovery_receipt, dict,
                ):
                    if native_root is None or not isinstance(remote_head, str):
                        raise WorkstreamGenerationError(
                            "generation_checkpoint_custody_authorization_missing"
                        )
                    custody = self._checkpoint_custody(
                        target_plan_revision=target_plan_revision,
                        created_at=created_at, remote_head=remote_head,
                        activation_checkpoint=activation_checkpoint,
                        retirement=retirement, event=event,
                        native_root=native_root,
                        operator_validation=operator_validation,
                        source=target_source,
                    )
                    self._append_checkpoint_custody(custody)
                    comments = self._comments()
                    checkpoint_replay = (
                        self._checkpoint_pre_reservation_replay(
                            comments,
                            target_plan_revision=target_plan_revision,
                            retirement=retirement, created_at=created_at,
                            activation_checkpoint=activation_checkpoint,
                            remote_head=remote_head,
                            expected_native_root_sha256=native_root["sha256"],
                        )
                    )
                    if checkpoint_replay is None:
                        raise WorkstreamGenerationError(
                            "generation_checkpoint_custody_not_observed"
                        )
                LinearProjectionAdapter(
                    self.client, issue_id=self.issue_id,
                    workstream_id=self.workstream_id,
                    plan_revision=target_plan_revision, **self.authority,
                ).append(event, expected_material_revision=material.revision)
                comments = self._comments()
        candidate = self._candidate(
            target_plan_revision, comments,
            activation_checkpoint=activation_checkpoint,
        )
        reservation = self._matching_reservation(
            comments, mode="activate", from_plan=from_plan,
            to_plan=target_plan_revision, epoch=epoch,
            previous_control=previous_control, retirement=retirement,
            created_at=created_at,
        )
        if reservation is None:
            root_transition_receipt = (
                operator_validation.get("root_transition_recovery_receipt")
                if isinstance(operator_validation, dict) else None
            )
            if root_transition_receipt is not None:
                root_transition_receipt = {
                    **deepcopy(root_transition_receipt),
                    "native_root_material_sha256": native_root[
                        "material_sha256"
                    ],
                }
            reservation = self._reservation(
                comments=comments, mode="activate", from_plan=from_plan,
                to_plan=target_plan_revision, epoch=epoch,
                previous_control=previous_control, candidate=candidate,
                retirement=retirement, created_at=created_at,
                native_root_sha256=(
                    native_root["sha256"] if native_root is not None else None
                ),
                activation_checkpoint=activation_checkpoint,
                remote_head=protocol_remote_head,
                operator_contract_sha256=self.operator_contract_sha256,
                root_transition_receipt=root_transition_receipt,
            )
            stored = self._append_reservation(reservation)
        else:
            stored = reservation
        reservation = stored
        if (
            native_root is not None
            and (
                reservation.get("native_root_material_sha256")
                != native_root["material_sha256"]
                if reservation.get("schema_version") == 6 else
                reservation.get("native_root_sha256") != native_root["sha256"]
            )
        ):
            raise WorkstreamGenerationError(
                "generation_reservation_native_root_proof_mismatch"
            )
        if reservation.get("schema_version") in {4, 5, 6} and (
            reservation.get("activation_checkpoint") != activation_checkpoint
            or reservation.get("remote_head") != protocol_remote_head
            or reservation.get("source") != target_source
            or reservation.get("retirement") != retirement
        ):
            raise WorkstreamGenerationError(
                "generation_reservation_replay_inputs_mismatch"
            )
        comments = self._comments()
        self._assert_reservation_live(comments, reservation)
        if not self._pending_operator_replay(
            comments, target_plan_revision=target_plan_revision,
            retirement=retirement, created_at=created_at,
        ):
            self._validate_operator(retirement)
        from_state, to_state = self._states(comments, from_plan, target_plan_revision)
        selected_before_seal = select_plan_generation(
            comments, workstream_id=self.workstream_id,
            description_plan_revision=from_plan,
            authenticated_route=self.authority,
        )
        if selected_before_seal["transition_tip_event_id"] != previous_control:
            raise WorkstreamGenerationError(
                "generation_predecessor_changed_before_activation"
            )
        authorized_activation_event_ids = frozenset(
            event["event_id"] for event in generation_controls(comments)
            if event.get("value", {}).get("activation_checkpoint") is not None
        )
        from_frontier = _generation_frontier(
            from_state, comments, plan_revision=from_plan,
            material_revision=reservation["material_revision"],
            authorized_activation_event_ids=authorized_activation_event_ids,
        )
        to_frontier_before = _generation_frontier(
            to_state, comments, plan_revision=target_plan_revision,
            projection_revision=reservation["to_projection_revision"],
            material_revision=reservation["material_revision"],
            authorized_activation_event_ids=authorized_activation_event_ids,
        )
        if activation_checkpoint is not None:
            to_frontier_before["checkpoint_event_ids"] = candidate["receipt"][
                "checkpoint_event_ids"
            ]
            to_frontier_before["checkpoint_events_sha256"] = _digest(
                to_frontier_before["checkpoint_event_ids"]
            )
        seal_value = {
            "schema_version": 2, "reservation_id": reservation["reservation_id"],
            "reservation_sha256": stored["reservation_sha256"],
            "from": from_frontier, "to": to_frontier_before,
            "source": candidate["source"],
            "graph_frontier_sha256": reservation["graph_frontier_sha256"],
            "candidate_resume_sha256": reservation["candidate_resume_sha256"],
            "retirement": retirement,
            "previous_control_event_id": previous_control,
            "activation_epoch": epoch,
        }
        seal = build_projection_event(
            workstream_id=self.workstream_id, kind="generation_candidate_seal",
            key=reservation["reservation_id"], value=seal_value,
            plan_revision=target_plan_revision,
            expected_revision=reservation["to_projection_revision"],
            created_at=created_at, authority=self.authority,
        )
        LinearProjectionAdapter(
            self.client, issue_id=self.issue_id, workstream_id=self.workstream_id,
            plan_revision=target_plan_revision, **self.authority,
        ).append(seal, expected_material_revision=reservation["material_revision"],
                 allowed_generation_reservation_id=reservation["reservation_id"])
        comments = self._comments()
        self._assert_reservation_live(comments, reservation)
        post = self._candidate(
            target_plan_revision, comments,
            activation_checkpoint=activation_checkpoint,
        )
        if post["receipt"]["graph_frontier_sha256"] != reservation["graph_frontier_sha256"]:
            raise WorkstreamGenerationError("generation_graph_changed_after_reservation")
        from_state, to_state = self._states(comments, from_plan, target_plan_revision)
        if from_state.revision != reservation["from_projection_revision"]:
            raise WorkstreamGenerationError("generation_predecessor_changed_before_activation")
        if to_state.revision != reservation["to_projection_revision"] + 1:
            raise WorkstreamGenerationError("generation_candidate_changed_after_seal")
        to_frontier = _generation_frontier(
            to_state, comments, plan_revision=target_plan_revision,
            material_revision=reservation["material_revision"],
            authorized_activation_event_ids=authorized_activation_event_ids,
        )
        if activation_checkpoint is not None:
            to_frontier["checkpoint_event_ids"] = post["receipt"][
                "checkpoint_event_ids"
            ]
            to_frontier["checkpoint_events_sha256"] = _digest(
                to_frontier["checkpoint_event_ids"]
            )
        value = {
            **seal_value, "to": to_frontier,
            "candidate_resume_sha256": post["receipt"]["snapshot_sha256"],
            "candidate_seal_event_id": seal["event_id"],
            "candidate_seal_sha256": _digest(seal),
        }
        if activation_checkpoint is not None:
            value.update({
                "schema_version": 3,
                "activation_checkpoint": deepcopy(activation_checkpoint),
                "activation_checkpoint_sha256": _digest(activation_checkpoint),
            })
        if native_root is not None:
            value.update({
                "schema_version": 4,
                "activation_checkpoint": deepcopy(activation_checkpoint),
                "activation_checkpoint_sha256": (
                    _digest(activation_checkpoint)
                    if activation_checkpoint is not None else None
                ),
                "native_root_sha256": (
                    reservation["native_root_sha256"]
                    if reservation.get("schema_version") == 6
                    else native_root["sha256"]
                ),
            })
        activation = build_projection_event(
            workstream_id=self.workstream_id, kind="generation_transition", key="root",
            value=value, plan_revision=from_plan,
            expected_revision=from_state.revision, created_at=created_at,
            authority=self.authority,
        )
        # This is the final complete-read fence before the recoverable
        # preparation. Legacy ledger collision successors remain quarantined.
        final_comments = self._comments()
        self._assert_reservation_live(final_comments, reservation)
        final_post = self._candidate(
            target_plan_revision, final_comments,
            activation_checkpoint=activation_checkpoint,
        )
        final_from, final_to = self._states(
            final_comments, from_plan, target_plan_revision,
        )
        if (
            final_post["receipt"]["graph_frontier_sha256"]
            != reservation["graph_frontier_sha256"]
            or final_post["material"].revision != reservation["material_revision"]
            or final_from.revision != reservation["from_projection_revision"]
            or final_to.revision != reservation["to_projection_revision"] + 1
        ):
            raise WorkstreamGenerationError("generation_final_fence_changed")
        # Schema-v4 writes a recoverable preparation first. Authority changes
        # only when its separately authenticated finalization is appended.
        final_native = self._post_protocol_native_root_proof(
            reservation, expected_native_root_sha256,
        )
        self._assert_source_current(reservation["source"])
        receipt = LinearProjectionAdapter(
            self.client, issue_id=self.issue_id, workstream_id=self.workstream_id,
            plan_revision=from_plan, **self.authority,
        ).append(activation, expected_material_revision=reservation["material_revision"],
                 allowed_generation_reservation_id=reservation["reservation_id"],
                 allow_retired_generation_control=True)
        after = self._comments()
        if reservation.get("schema_version") in {4, 5, 6}:
            self._assert_reservation_live(after, reservation)
            prepared = select_plan_generation(
                after, workstream_id=self.workstream_id,
                description_plan_revision=from_plan,
                authenticated_route=self.authority,
            )
            if prepared["plan_revision"] != from_plan:
                raise WorkstreamGenerationError(
                    "generation_preparation_became_authoritative"
                )
            # Revalidate the complete candidate after preparation and before
            # finalization; status/source races therefore leave an exact live
            # reservation and old execution authority.
            prepared_post = self._candidate(
                target_plan_revision, after,
                activation_checkpoint=activation_checkpoint,
            )
            if (
                prepared_post["receipt"]["graph_frontier_sha256"]
                != reservation["graph_frontier_sha256"]
                or prepared_post["receipt"]["snapshot_sha256"]
                != value["candidate_resume_sha256"]
            ):
                raise WorkstreamGenerationError(
                    "generation_prepared_candidate_changed"
                )
            final_native = self._post_protocol_native_root_proof(
                reservation, expected_native_root_sha256,
            )
            self._assert_source_current(reservation["source"])
            finalization = self._append_finalization(
                reservation=reservation, transition=activation,
                native_root=final_native, created_at=created_at,
            )
            after = self._comments()
            # This cannot make mutable Git refs atomic with Linear, but it does
            # guarantee the command never reports success if canonical bytes
            # changed while the finalization write was in flight. Ordinary
            # resume then detects the active/live digest mismatch and remains
            # non-executable until a subsequent generation is reviewed.
            self._assert_source_current(reservation["source"])
        else:
            finalization = None
        final = select_plan_generation(
            after, workstream_id=self.workstream_id,
            description_plan_revision=from_plan, authenticated_route=self.authority,
        )
        if final["plan_revision"] != target_plan_revision or final["activation_epoch"] != epoch:
            raise WorkstreamGenerationError("generation_activation_not_observed")
        return {
            **receipt,
            "activated_plan_revision": target_plan_revision,
            "bound_graph_frontier_sha256": value["graph_frontier_sha256"],
            "bound_candidate_resume_sha256": value["candidate_resume_sha256"],
            "quarantined_legacy_writes": final_post["receipt"][
                "quarantined_legacy_writes"
            ],
            "replay": False,
            "native_root_activation_proof": final_native,
            **({
                "two_phase_finalization": finalization,
                "final_candidate": final_post["receipt"],
            } if finalization is not None else {}),
        }


def strict_candidate_loader(
    client: Any, *, token: str, authority: dict[str, str],
    plan_source: str, plan_identity: str | None,
    max_bytes: int = DEFAULT_RESUME_MAX_BYTES, max_items: int = 100,
    activation_checkpoint: dict[str, Any] | None = None,
    activation_remote_head: str | None = None,
    activation_created_at: str | None = None,
    root_updated_at_override: str | None = None,
) -> CandidateLoader:
    authenticated_source = plan_payload(
        plan_source, plan_identity or plan_source,
    )["source"]

    def load(plan_revision: str) -> dict[str, Any]:
        if authenticated_source["sha256"] != plan_revision:
            raise WorkstreamGenerationError("generation_source_bytes_mismatch")
        transport = LinearGraphQLTransport(
            client, team_id=authority["team_id"],
            workspace_id=authority["workspace_id"], project_id=authority["project_id"],
        )
        if root_updated_at_override is not None:
            if not isinstance(root_updated_at_override, str) or not root_updated_at_override:
                raise WorkstreamGenerationError(
                    "generation_graph_clock_historical_timestamp_invalid"
                )

        def snapshot_for_candidate() -> dict[str, Any]:
            snapshot = transport.snapshot_for_root(
                token, include_description=True, include_child_comments=True,
            )
            if root_updated_at_override is not None:
                snapshot["root"]["updatedAt"] = root_updated_at_override
            return snapshot

        graph = snapshot_for_candidate()
        description_plan_revision = (
            graph["root"].get("plan_revision") or plan_revision
        )
        comments = LinearProjectionAdapter(
            client, issue_id=token, workstream_id=token,
            plan_revision=plan_revision, **authority,
        )._comments()
        dependency_adapter = LinearChildDependencyAdapter(
            client, workspace_id=authority["workspace_id"],
            team_id=authority["team_id"], project_id=authority["project_id"],
            root_issue_id=authority["root_issue_id"], root_identifier=token,
            plan_revision=plan_revision,
        )
        base_dependency_graph = dependency_adapter.read_authorized_graph_for_snapshot(
            graph, comments,
            generation_selector_plan_revision=description_plan_revision,
            reread=lambda: (
                snapshot_for_candidate(),
                LinearProjectionAdapter(
                    client, issue_id=token, workstream_id=token,
                    plan_revision=plan_revision, **authority,
                )._comments(),
            ),
        )
        selected = select_plan_generation(
            comments, workstream_id=token,
            description_plan_revision=description_plan_revision,
            authenticated_route=authority,
        )
        from workstream_linear_projection import (
            child_mutation_authorizations_from_comments,
        )
        mutation_authorizations = child_mutation_authorizations_from_comments(
            comments, workstream_id=token,
            description_plan_revision=description_plan_revision,
            authenticated_route=authority,
        )
        if mutation_authorizations:
            graph = transport.recover_authorized_children(
                graph, mutation_authorizations,
            )
        child_comments = graph.pop("child_comments", None)
        # A predecessor proposal which has not won its root activation is a
        # real recovery obligation. Activating another generation would make
        # that proposal ineligible forever, so refuse before constructing or
        # reserving the candidate. This loader is rerun at every generation
        # fence, which also catches proposals appearing during preparation.
        from workstream_child_proposal import pending_proposal_obligations

        pending_predecessor_proposals: list[dict[str, Any]] = []
        if not isinstance(child_comments, dict):
            raise WorkstreamGenerationError(
                "generation_child_comment_collection_missing"
            )
        for child in graph.get("children", []):
            token_value = str(child.get("identifier", "")).upper()
            comments_for_child = child_comments.get(token_value)
            if comments_for_child is None:
                continue
            pending_predecessor_proposals.extend(
                pending_proposal_obligations(
                    comments_for_child, mutation_authorizations,
                    child_workstream_id=token_value,
                    child_issue_id=child.get("id"),
                    plan_revision=selected["plan_revision"],
                )
            )
        if pending_predecessor_proposals:
            raise WorkstreamGenerationError(
                "generation_predecessor_child_proposals_pending:"
                + ",".join(sorted(
                    item["proposal_id"] for item in pending_predecessor_proposals
                ))
            )
        graph["root"]["plan_revision"] = plan_revision
        # Candidate validation may target an inactive generation. Preserve the
        # actual description-backed predecessor selector so child proposal
        # authorizations from that generation remain verifiable while the
        # candidate graph is evaluated under the target plan.
        graph["root"]["description_plan_revision"] = description_plan_revision
        selected_checkpoints = None
        if selected["plan_revision"] == plan_revision:
            graph["root"].update({
                "generation_transition_tip_event_id": selected[
                    "transition_tip_event_id"
                ],
                "generation_activation_epoch": selected["activation_epoch"],
                "generation_authority_origin": selected["authority_origin"],
                "description_plan_revision": description_plan_revision,
            })
            apply_generation_execution_status(
                graph["root"], selected_generation_execution_status(
                    comments, workstream_id=token,
                    transition_event_id=selected["transition_tip_event_id"],
                    authenticated_route=authority,
                ),
            )
            selected_checkpoints = selected_activation_checkpoints(
                comments, workstream_id=token,
                transition_event_id=selected["transition_tip_event_id"],
                active_plan_revision=plan_revision,
                authenticated_route=authority,
            )
        graph = add_child_material_history(
            graph, child_comments, authenticated_route=authority,
            root_comments=comments,
        )
        if activation_checkpoint is not None:
            validate_checkpoint(activation_checkpoint)
            if (
                activation_checkpoint["workstream_id"] != token
                or activation_checkpoint["plan_revision"] != plan_revision
            ):
                raise WorkstreamGenerationError(
                    "generation_activation_checkpoint_mismatch"
                )
            checkpoint_log = reduce_checkpoint_comments(
                comments, workstream_id=token,
                selected_activation_checkpoints=selected_checkpoints,
            )
            existing = next((
                item for item in checkpoint_log.checkpoints
                if item["event_id"] == activation_checkpoint["event_id"]
            ), None)
            if existing is None:
                comments = [*comments, {
                    "id": "00000000-0000-4000-8000-000000000000",
                    "body": encode_checkpoint_comment(activation_checkpoint),
                }]
            elif any(
                existing.get(field) != activation_checkpoint.get(field)
                for field in activation_checkpoint if field != "acknowledgement"
            ):
                raise WorkstreamGenerationError(
                    "generation_activation_checkpoint_conflict"
                )
            if not isinstance(activation_created_at, str) or not activation_created_at:
                raise WorkstreamGenerationError(
                    "generation_activation_checkpoint_mismatch"
                )
            material = reduce_event_comments(
                comments, workstream_id=token,
            )
            target_state = reduce_projection_comments(
                comments, workstream_id=token,
                expected_plan_revision=plan_revision,
                authenticated_route=authority,
            )
            _desired, prospective = _prospective_activation_checkpoint(
                activation_checkpoint, workstream_id=token,
                target_plan_revision=plan_revision,
                material_revision=material.revision,
                target_state=target_state,
                remote_head=activation_remote_head,
                created_at=activation_created_at, authority=authority,
            )
            if prospective is not None:
                comments = [*comments, {
                    "id": projection_slot_id(
                        token, plan_revision,
                        prospective["expected_revision"], authority,
                    ),
                    "body": encode_projection_comment(prospective),
                    "createdAt": activation_created_at,
                    "updatedAt": activation_created_at,
                }]
        material = reduce_event_comments(comments, workstream_id=token)
        joined = add_material_history(
            graph, comments, token, authenticated_route=authority,
            authenticated_source=authenticated_source,
            relation_target_resolver=lambda relations: read_relation_targets(client, relations),
        )
        joined["dependency_graph"] = rebind_authenticated_dependency_graph(
            joined, comments, base_dependency_graph,
            authority={**authority, "root_identifier": token},
            plan_revision=plan_revision,
        )
        context = compact_context(
            joined, token, max_bytes=max_bytes, max_items=max_items,
            require_projection_authority=True, require_dependency_graph=True,
            include_history=False,
        )
        if context.get("resume_authority") != "full":
            raise WorkstreamGenerationError("generation_candidate_not_strict_full_authority")
        projection = reduce_projection_comments(
            comments, workstream_id=token, expected_plan_revision=plan_revision,
            authenticated_route=authority, authenticated_source=authenticated_source,
        )
        checkpoints = reduce_checkpoint_comments(
            comments, workstream_id=token,
            selected_activation_checkpoints=selected_checkpoints,
        )
        graph_root = dict(graph["root"])
        for field in (
            "description_plan_revision", "generation_transition_tip_event_id",
            "generation_activation_epoch", "generation_authority_origin",
            "issue_status", "issue_status_type", "generation_execution_status",
        ):
            graph_root.pop(field, None)
        graph_surface = {
            "root": graph_root, "children": graph.get("children", []),
            "decisions": graph.get("decisions", []),
        }
        quarantined_legacy_writes = joined["root"].get(
            "quarantined_legacy_writes", {
                "count": 0,
                "sha256": hashlib.sha256(b"[]").hexdigest(),
            },
        )
        candidate_resume_surface = {
            "resume_authority": context["resume_authority"],
            "plan_revision": plan_revision,
            "material_revision": material.revision,
            "material_event_ids": [
                event.event_id for event in material.events
            ],
            "checkpoint_event_ids": sorted(
                item["event_id"] for item in checkpoints.checkpoints
                if item["plan_revision"] == plan_revision
            ),
            "projection_events": [
                event for event in projection.events
                if event["kind"] not in {
                    "generation_genesis", "generation_transition",
                }
            ],
            "quarantined_legacy_writes": quarantined_legacy_writes,
        }
        return {
            "resume_authority": "full", "plan_revision": plan_revision,
            "authenticated_route": authority,
            "source": {"identity": authenticated_source["identity"],
                       "sha256": authenticated_source["sha256"]},
            "material_revision": material.revision,
            "checkpoint_event_ids": sorted(
                item["event_id"] for item in checkpoints.checkpoints
                if item["plan_revision"] == plan_revision
            ),
            "projection_revision": projection.revision,
            "graph_frontier_sha256": _digest(graph_surface),
            "snapshot_sha256": _digest(candidate_resume_surface),
            "quarantined_legacy_writes": quarantined_legacy_writes,
        }
    return load


def generation_graph_clock_custody(
    client: Any, *, token: str, authority: dict[str, str],
    reservation_id: str, reservation_sha256: str,
    historical_root_updated_at: str, apply: bool,
) -> dict[str, Any]:
    """Bind the one protocol-owned root-clock advance on a sealed schema-v6 retry.

    The supplied historical timestamp is never trusted by itself. It is usable
    only when substituting that single field through the production strict
    candidate loader reproduces both reservation digests exactly.
    """
    token = token.upper()
    if (
        not RESERVATION_ID.fullmatch(str(reservation_id))
        or not HEX64.fullmatch(str(reservation_sha256))
        or not isinstance(historical_root_updated_at, str)
        or not historical_root_updated_at
    ):
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_arguments_invalid"
        )
    adapter = LinearProjectionAdapter(
        client, issue_id=token, workstream_id=token,
        plan_revision="0" * 64, **authority,
    )
    comments = adapter._comments()
    reservations = reduce_generation_reservations(
        comments, workstream_id=token, authenticated_route=authority,
    )
    exact = [item for item in reservations if (
        item["reservation_id"] == reservation_id
        and hmac.compare_digest(item["reservation_sha256"], reservation_sha256)
    )]
    if len(exact) != 1:
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_reservation_mismatch"
        )
    reservation = exact[0]
    if reservation.get("schema_version") != 6 or reservation.get("mode") != "activate":
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_schema6_activation_required"
        )
    pending = pending_generation_reservations(
        comments, workstream_id=token, authenticated_route=authority,
    )
    if len(pending) != 1 or any(
        item["reservation_id"] != reservation_id
        or item["reservation_sha256"] != reservation_sha256
        for item in pending
    ):
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_pending_reservation_ambiguous"
        )
    if f"{reservation_id}:{reservation_sha256}" in _generation_abort_ids(
        comments, reservations,
    ):
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_reservation_aborted"
        )
    if any(
        event["kind"] == "generation_transition"
        and event.get("value", {}).get("reservation_id") == reservation_id
        for event in generation_controls(comments)
    ) or any(
        item["reservation_id"] == reservation_id
        for item in reduce_generation_finalizations(
            comments, workstream_id=token, authenticated_route=authority,
        )
    ):
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_generation_already_advanced"
        )
    selected = select_plan_generation(
        comments, workstream_id=token,
        description_plan_revision=reservation["from_plan_revision"],
        authenticated_route=authority,
    )
    if (
        selected["plan_revision"] != reservation["from_plan_revision"]
        or selected["transition_tip_event_id"]
        != reservation["previous_control_event_id"]
    ):
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_predecessor_changed"
        )
    from_state = reduce_projection_comments(
        comments, workstream_id=token,
        expected_plan_revision=reservation["from_plan_revision"],
        authenticated_route=authority,
    )
    target_state = reduce_projection_comments(
        comments, workstream_id=token,
        expected_plan_revision=reservation["to_plan_revision"],
        authenticated_route=authority,
    )
    seals = [event for event in target_state.events if (
        event["kind"] == "generation_candidate_seal"
        and event["key"] == reservation_id
    )]
    if len(seals) != 1:
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_candidate_seal_ambiguous"
        )
    seal = seals[0]
    seal_value = seal.get("value") or {}
    if (
        seal_value.get("reservation_sha256") != reservation_sha256
        or seal_value.get("graph_frontier_sha256")
        != reservation["graph_frontier_sha256"]
        or seal_value.get("source") != reservation["source"]
        or seal_value.get("retirement") != reservation["retirement"]
        or seal_value.get("previous_control_event_id")
        != reservation["previous_control_event_id"]
        or from_state.revision != reservation["from_projection_revision"]
        or target_state.revision != reservation["to_projection_revision"] + 1
    ):
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_candidate_seal_mismatch"
        )
    existing = [item for item in reduce_generation_graph_clock_custodies(
        comments, workstream_id=token, authenticated_route=authority,
    ) if item["reservation_id"] == reservation_id]
    if len(existing) > 1:
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_ambiguous"
        )
    if existing and (
        existing[0]["reservation_sha256"] != reservation_sha256
        or existing[0]["historical_root_updated_at"]
        != historical_root_updated_at
    ):
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_replay_mismatch"
        )

    source_identity = reservation["source"]["identity"]
    current_loader = strict_candidate_loader(
        client, token=token, authority=authority,
        plan_source=source_identity, plan_identity=source_identity,
        activation_checkpoint=reservation["activation_checkpoint"],
        activation_remote_head=reservation["remote_head"],
        activation_created_at=reservation["created_at"],
    )
    historical_loader = strict_candidate_loader(
        client, token=token, authority=authority,
        plan_source=source_identity, plan_identity=source_identity,
        activation_checkpoint=reservation["activation_checkpoint"],
        activation_remote_head=reservation["remote_head"],
        activation_created_at=reservation["created_at"],
        root_updated_at_override=historical_root_updated_at,
    )
    current_candidate = current_loader(reservation["to_plan_revision"])
    historical_candidate = historical_loader(reservation["to_plan_revision"])
    # A reservation's checkpoint list is the global serialization frontier;
    # it deliberately includes predecessor checkpoints.  A strict candidate
    # receipt is scoped to the target generation, whose exact checkpoint
    # frontier was already bound by the reservation-backed candidate seal.
    expected_checkpoint_ids = seal_value["to"]["checkpoint_event_ids"]
    if (
        current_candidate["source"] != reservation["source"]
        or current_candidate["material_revision"] != reservation["material_revision"]
        or current_candidate["checkpoint_event_ids"] != expected_checkpoint_ids
        or historical_candidate["graph_frontier_sha256"]
        != reservation["graph_frontier_sha256"]
        or any(
            historical_candidate[field] != current_candidate[field]
            for field in (
                "source", "material_revision", "checkpoint_event_ids",
                "projection_revision", "snapshot_sha256",
            )
        )
    ):
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_strict_candidate_mismatch"
        )
    linear = LinearGraphQLTransport(
        client, team_id=authority["team_id"],
        workspace_id=authority["workspace_id"],
        project_id=authority["project_id"],
    )
    native_snapshot = _activation_native_root_snapshot(linear, token)
    root = native_snapshot.get("root") or {}
    observed_updated_at = root.get("updatedAt")
    if not isinstance(observed_updated_at, str) or not observed_updated_at:
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_current_timestamp_missing"
        )
    current_native = native_root_activation_proof(
        native_snapshot, workstream_id=token, issue_id=token,
        authority=authority,
    )
    historical_snapshot = deepcopy(native_snapshot)
    historical_snapshot["root"]["updatedAt"] = historical_root_updated_at
    historical_native = native_root_activation_proof(
        historical_snapshot, workstream_id=token, issue_id=token,
        authority=authority,
    )
    if (
        current_native["material_sha256"]
        != reservation["native_root_material_sha256"]
        or historical_native["sha256"] != reservation["native_root_sha256"]
    ):
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_native_root_mismatch"
        )
    root_receipts = [
        item for item in comments
        if item.get("id") == reservation["root_transition_receipt_ref"]
    ]
    if len(root_receipts) != 1:
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_root_receipt_missing"
        )
    from workstream_root_transition import (
        _decode as decode_root_transition, reopen_transition_witness_context,
    )
    root_receipt = decode_root_transition(
        str(root_receipts[0].get("body") or "")
    )
    target_state_id = (root_receipt.get("after") or {}).get("state")
    contract_sha = (root_receipt.get("operator_authorization") or {}).get(
        "contract_sha256"
    )
    root_context = reopen_transition_witness_context(
        comments=comments, graph=native_snapshot, token=token,
        authority=authority, contract_sha256=str(contract_sha or ""),
        target_state=target_state_id,
        expected_slot=reservation["root_transition_receipt_ref"],
        require_original_frontier=False,
        operator_contract_sha256=reservation["operator_contract_sha256"],
    )
    if (
        root_context is None
        or root_context["receipt"]["sha256"]
        != reservation["root_transition_receipt_sha256"]
    ):
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_root_receipt_mismatch"
        )
    value = {
        "schema_version": 1, "workstream_id": token,
        "authority": deepcopy(authority),
        "reservation_id": reservation_id,
        "reservation_sha256": reservation_sha256,
        "source": deepcopy(reservation["source"]),
        "candidate_seal_event_id": seal["event_id"],
        "candidate_seal_sha256": _digest(seal),
        "graph_frontier_sha256": reservation["graph_frontier_sha256"],
        "native_root_sha256": reservation["native_root_sha256"],
        "native_root_material_sha256": reservation[
            "native_root_material_sha256"
        ],
        "root_transition_receipt_ref": reservation[
            "root_transition_receipt_ref"
        ],
        "root_transition_receipt_sha256": reservation[
            "root_transition_receipt_sha256"
        ],
        "historical_root_updated_at": historical_root_updated_at,
        "observed_root_updated_at": observed_updated_at,
        "observed_graph_frontier_sha256": current_candidate[
            "graph_frontier_sha256"
        ],
        "observed_native_root_sha256": current_native["sha256"],
    }
    if existing:
        stored = {key: existing[0][key] for key in value}
        if stored != value:
            # Reconstruct the pre-append observation recorded by the receipt;
            # only the root clock may have advanced since it was appended.
            observed_loader = strict_candidate_loader(
                client, token=token, authority=authority,
                plan_source=source_identity, plan_identity=source_identity,
                activation_checkpoint=reservation["activation_checkpoint"],
                activation_remote_head=reservation["remote_head"],
                activation_created_at=reservation["created_at"],
                root_updated_at_override=existing[0]["observed_root_updated_at"],
            )
            observed_candidate = observed_loader(reservation["to_plan_revision"])
            observed_snapshot = deepcopy(native_snapshot)
            observed_snapshot["root"]["updatedAt"] = existing[0][
                "observed_root_updated_at"
            ]
            observed_native = native_root_activation_proof(
                observed_snapshot, workstream_id=token, issue_id=token,
                authority=authority,
            )
            stable = {
                **value,
                "observed_root_updated_at": existing[0]["observed_root_updated_at"],
                "observed_graph_frontier_sha256": observed_candidate[
                    "graph_frontier_sha256"
                ],
                "observed_native_root_sha256": observed_native["sha256"],
            }
            if stored != stable:
                raise WorkstreamGenerationError(
                    "generation_graph_clock_custody_replay_mismatch"
                )
        return {**existing[0], "apply": apply, "replay": True}
    result = {**value, "apply": apply, "replay": False}
    if not apply:
        return result

    # Final zero-write race fence. The append itself is the first mutation.
    if adapter._comments() != comments:
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_comments_changed_before_append"
        )
    if _activation_native_root_snapshot(linear, token) != native_snapshot:
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_root_changed_before_append"
        )
    if current_loader(reservation["to_plan_revision"]) != current_candidate:
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_candidate_changed_before_append"
        )
    slot = graph_clock_custody_slot_id(value)
    body = encode_generation_graph_clock_custody(value)
    try:
        client.execute(COMMENT_CREATE_MUTATION, {"input": {
            "id": slot, "issueId": token, "body": body,
        }})
    except (LinearTransportError, OSError, TimeoutError):
        after = reduce_generation_graph_clock_custodies(
            adapter._comments(), workstream_id=token,
            authenticated_route=authority,
        )
        if not any(
            item["remote_id"] == slot
            and {key: item[key] for key in value} == value
            for item in after
        ):
            raise WorkstreamGenerationError(
                "generation_graph_clock_custody_slot_lost_reload_required"
            )
    after = reduce_generation_graph_clock_custodies(
        adapter._comments(), workstream_id=token,
        authenticated_route=authority,
    )
    matches = [item for item in after if item["remote_id"] == slot]
    if len(matches) != 1 or {key: matches[0][key] for key in value} != value:
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_not_observed"
        )
    after_comments = adapter._comments()
    after_selected = select_plan_generation(
        after_comments, workstream_id=token,
        description_plan_revision=reservation["from_plan_revision"],
        authenticated_route=authority,
    )
    after_from = reduce_projection_comments(
        after_comments, workstream_id=token,
        expected_plan_revision=reservation["from_plan_revision"],
        authenticated_route=authority,
    )
    after_target = reduce_projection_comments(
        after_comments, workstream_id=token,
        expected_plan_revision=reservation["to_plan_revision"],
        authenticated_route=authority,
    )
    after_native = native_root_activation_proof(
        _activation_native_root_snapshot(linear, token),
        workstream_id=token, issue_id=token, authority=authority,
    )
    if (
        after_selected != selected
        or after_from.revision != from_state.revision
        or after_target.revision != target_state.revision
        or reduce_event_comments(
            after_comments, workstream_id=token,
        ).revision != reservation["material_revision"]
        or historical_loader(reservation["to_plan_revision"])
        != historical_candidate
        or after_native["material_sha256"]
        != reservation["native_root_material_sha256"]
    ):
        raise WorkstreamGenerationError(
            "generation_graph_clock_custody_authority_changed_after_append"
        )
    return {**matches[0], "apply": True, "replay": False}


def strict_active_generation_receipt(
    client: Any, *, token: str, authority: dict[str, str],
    description_plan_revision: str | None, requested_plan_revision: str,
    requested_loader: CandidateLoader, max_bytes: int, max_items: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Strictly resume the live authority tip, including historical retries."""
    comments = LinearProjectionAdapter(
        client, issue_id=token, workstream_id=token,
        plan_revision=requested_plan_revision, **authority,
    )._comments()
    selected = select_plan_generation(
        comments, workstream_id=token,
        description_plan_revision=description_plan_revision,
        authenticated_route=authority,
    )
    active_plan = selected["plan_revision"]
    if active_plan == requested_plan_revision:
        return selected, requested_loader(active_plan)
    active_state = reduce_projection_comments(
        comments, workstream_id=token, expected_plan_revision=active_plan,
        authenticated_route=authority,
    )
    source = active_state.snapshot.get("source") or {}
    identity = source.get("identity") or source.get("url")
    if (
        not isinstance(identity, str) or not identity
        or source.get("sha256") != active_plan
    ):
        raise WorkstreamGenerationError("generation_active_source_incomplete")
    loader = strict_candidate_loader(
        client, token=token, authority=authority,
        plan_source=identity, plan_identity=identity,
        max_bytes=max_bytes, max_items=max_items,
    )
    return selected, loader(active_plan)


def _route_and_client(args: argparse.Namespace) -> tuple[Any, dict[str, str]]:
    route, _ = resolve_linear_route(
        config_path=args.config, workspace_id=args.linear_workspace_id,
        team_id=args.linear_team_id, project_id=args.linear_project_id,
    )
    token = load_linear_api_key()
    if not token:
        raise WorkstreamGenerationError("linear_auth_unavailable")
    client = HttpGraphQLClient(token, args.linear_endpoint)
    authenticated = bootstrap_linear_route(client, args.token)
    if route and any(route.get(key) != authenticated.get(key)
                     for key in ("workspace_id", "team_id", "project_id")):
        raise WorkstreamGenerationError("generation_route_mismatch")
    return client, authenticated


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--config")
    value.add_argument("--linear-workspace-id")
    value.add_argument("--linear-team-id")
    value.add_argument("--linear-project-id")
    value.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    commands = value.add_subparsers(dest="command", required=True)
    for name in ("bootstrap", "activate", "prepare"):
        command = commands.add_parser(name)
        command.add_argument("token")
        command.add_argument(
            "--plan-source", required=(name in {"bootstrap", "prepare"})
        )
        command.add_argument("--plan-identity")
        command.add_argument("--created-at", required=True)
        command.add_argument("--max-bytes", type=int, default=DEFAULT_RESUME_MAX_BYTES)
        command.add_argument("--max-items", type=int, default=100)
        if name != "prepare":
            command.add_argument("--apply", action="store_true")
        else:
            command.add_argument(
                "--remote-head", required=True,
                help="authenticated exact repository head for target disposition",
            )
            command.add_argument(
                "--started-state-id", required=True,
                help="reviewed Linear started-state UUID for native root reopen",
            )
            command.add_argument(
                "--manifest-output",
                help=(
                    "atomically write the exact nested projection manifest for "
                    "the next projection preview/apply"
                ),
            )
        if name == "activate":
            command.add_argument(
                "--operator-contract",
                help="exact activation_ready JSON emitted by generation prepare",
            )
            command.add_argument("--retirement-proof",
                                 help=argparse.SUPPRESS)
            command.add_argument("--abort-reservation-id")
            command.add_argument("--abort-reservation-sha256")
            command.add_argument("--abort-reason")
            command.add_argument(
                "--activation-checkpoint",
                help="reviewed pending root checkpoint JSON carried by activation",
            )
            command.add_argument(
                "--remote-head",
                help="authenticated remote head used for checkpoint-bound disposition",
            )
            command.add_argument(
                "--expected-native-root-sha256",
                help=(
                    "exact nonterminal root readback digest emitted by reviewed preview; "
                    "required with activate --apply"
                ),
            )
    continuation = commands.add_parser("continue")
    continuation.add_argument("token")
    continuation.add_argument("--reservation-id", required=True)
    continuation.add_argument("--reservation-sha256", required=True)
    continuation.add_argument("--apply", action="store_true")
    custody = commands.add_parser("clock-custody")
    custody.add_argument("token")
    custody.add_argument("--reservation-id", required=True)
    custody.add_argument("--reservation-sha256", required=True)
    custody.add_argument("--historical-root-updated-at", required=True)
    custody.add_argument("--apply", action="store_true")
    return value


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "continue" and not args.apply:
            raise WorkstreamGenerationError(
                "generation_continue_requires_apply"
            )
        if args.command == "continue" and not RESERVATION_ID.fullmatch(
            str(args.reservation_id)
        ):
            raise WorkstreamGenerationError(
                "generation_continue_reservation_id_invalid"
            )
        if args.command == "continue" and not HEX64.fullmatch(
            str(args.reservation_sha256)
        ):
            raise WorkstreamGenerationError(
                "generation_continue_reservation_sha256_invalid"
            )
        if args.command == "activate" and args.retirement_proof:
            raise WorkstreamGenerationError(
                "generation_legacy_retirement_proof_cannot_authorize_operator"
            )
        aborting = args.command == "activate" and args.abort_reservation_id
        if args.command not in {"continue", "clock-custody"} and not aborting and (
            not args.plan_source
            or (args.command == "activate" and not args.operator_contract)
        ):
            raise WorkstreamGenerationError(
                "generation_candidate_cli_arguments_incomplete"
            )
        if (
            args.command == "activate" and args.apply and not aborting
            and not HEX64.fullmatch(str(args.expected_native_root_sha256 or ""))
        ):
            raise WorkstreamGenerationError(
                "generation_activate_apply_requires_reviewed_native_root_proof"
            )
        client, authority = _route_and_client(args)
        if args.command == "clock-custody":
            output = generation_graph_clock_custody(
                client, token=args.token, authority=authority,
                reservation_id=args.reservation_id,
                reservation_sha256=args.reservation_sha256,
                historical_root_updated_at=args.historical_root_updated_at,
                apply=args.apply,
            )
            output["command"] = "clock-custody"
            json.dump(
                output, sys.stdout, ensure_ascii=False,
                sort_keys=True, indent=2,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "continue":
            token = args.token.upper()
            comments = LinearProjectionAdapter(
                client, issue_id=token, workstream_id=token,
                plan_revision="0" * 64, **authority,
            )._comments()
            reservations = reduce_generation_reservations(
                comments, workstream_id=token,
                authenticated_route=authority,
            )
            by_id = [
                item for item in reservations
                if item["reservation_id"] == args.reservation_id
            ]
            if not by_id:
                raise WorkstreamGenerationError(
                    "generation_continue_reservation_id_not_found"
                )
            exact = [
                item for item in by_id
                if hmac.compare_digest(
                    item["reservation_sha256"], args.reservation_sha256,
                )
            ]
            if not exact:
                raise WorkstreamGenerationError(
                    "generation_continue_reservation_sha256_mismatch"
                )
            if len(exact) != 1:
                raise WorkstreamGenerationError(
                    "generation_continue_reservation_ambiguous"
                )
            reservation = exact[0]
            if reservation.get("schema_version") != 6:
                raise WorkstreamGenerationError(
                    "generation_continue_schema_unavailable:"
                    f"schema{reservation.get('schema_version')}_requires_exact_"
                    "reviewed_inputs;only_schema6_is_self_contained"
                )
            payload_source = plan_payload(
                reservation["source"]["identity"],
                reservation["source"]["identity"],
            )["source"]
            source = {
                "identity": payload_source.get("identity")
                or payload_source.get("url"),
                "sha256": payload_source.get("sha256"),
            }
            if source != reservation["source"]:
                raise WorkstreamGenerationError(
                    "generation_continue_authenticated_source_mismatch"
                )
            clock_custodies = [
                item for item in reduce_generation_graph_clock_custodies(
                    comments, workstream_id=token,
                    authenticated_route=authority,
                )
                if item["reservation_id"] == args.reservation_id
                and item["reservation_sha256"] == args.reservation_sha256
            ]
            if len(clock_custodies) > 1:
                raise WorkstreamGenerationError(
                    "generation_graph_clock_custody_ambiguous"
                )
            root_updated_at_override = None
            if clock_custodies:
                pending_tokens = {
                    (item["reservation_id"], item["reservation_sha256"])
                    for item in pending_generation_reservations(
                        comments, workstream_id=token,
                        authenticated_route=authority,
                    )
                }
                if (args.reservation_id, args.reservation_sha256) in pending_tokens:
                    verified_custody = generation_graph_clock_custody(
                        client, token=token, authority=authority,
                        reservation_id=args.reservation_id,
                        reservation_sha256=args.reservation_sha256,
                        historical_root_updated_at=clock_custodies[0][
                            "historical_root_updated_at"
                        ],
                        apply=False,
                    )
                else:
                    # Finalized replay is independently fenced by the exact
                    # transition/finalization reducers below. The deterministic
                    # custody envelope remains the historical clock authority;
                    # rerunning its pre-transition proof would incorrectly
                    # require the predecessor to still be active.
                    verified_custody = clock_custodies[0]
                root_updated_at_override = verified_custody[
                    "historical_root_updated_at"
                ]
            loader = strict_candidate_loader(
                client, token=token, authority=authority,
                plan_source=reservation["source"]["identity"],
                plan_identity=reservation["source"]["identity"],
                activation_checkpoint=reservation["activation_checkpoint"],
                activation_remote_head=reservation["remote_head"],
                activation_created_at=reservation["created_at"],
                root_updated_at_override=root_updated_at_override,
            )
            linear_transport = LinearGraphQLTransport(
                client, team_id=authority["team_id"],
                workspace_id=authority["workspace_id"],
                project_id=authority["project_id"],
            )
            initial_root_snapshot = linear_transport.snapshot_for_root(
                token, include_description=True, include_child_comments=True,
            )
            description_plan_revision = initial_root_snapshot["root"].get(
                "plan_revision"
            )
            transport = GenerationTransport(
                client, issue_id=token, workstream_id=token,
                authority=authority, candidate_loader=loader,
                legacy_description_plan_revision=description_plan_revision,
                native_root_loader=lambda: _activation_native_root_snapshot(
                    linear_transport, token,
                ),
                source_loader=lambda: plan_payload(
                    reservation["source"]["identity"],
                    reservation["source"]["identity"],
                )["source"],
                operator_contract_sha256=reservation[
                    "operator_contract_sha256"
                ],
                operator_remote_head=reservation["remote_head"],
            )
            output = transport.continue_reservation(
                reservation_id=args.reservation_id,
                reservation_sha256=args.reservation_sha256,
            )
            selected, final_candidate = strict_active_generation_receipt(
                client, token=token, authority=authority,
                description_plan_revision=description_plan_revision,
                requested_plan_revision=reservation["to_plan_revision"],
                requested_loader=loader,
                max_bytes=DEFAULT_RESUME_MAX_BYTES, max_items=100,
            )
            output.update({
                "command": "continue", "apply": True,
                "reservation_id": args.reservation_id,
                "reservation_sha256": args.reservation_sha256,
                "final_active_plan_revision": selected["plan_revision"],
                "final_candidate": final_candidate,
            })
            if selected["plan_revision"] != output["activated_plan_revision"]:
                output["post_read_status"] = (
                    "historical_replay_active_generation_advanced"
                )
            elif (
                final_candidate["graph_frontier_sha256"]
                != output["bound_graph_frontier_sha256"]
                or final_candidate["snapshot_sha256"]
                != output["bound_candidate_resume_sha256"]
            ):
                raise WorkstreamGenerationError(
                    "authority_changed_with_post_read_drift"
                )
            else:
                output["post_read_status"] = (
                    "authority_bound_two_phase_finalization_match"
                )
            json.dump(
                output, sys.stdout, ensure_ascii=False,
                sort_keys=True, indent=2,
            )
            sys.stdout.write("\n")
            return 0
        if args.command == "activate" and args.abort_reservation_id:
            if (
                not args.apply or not args.abort_reservation_sha256
                or not args.abort_reason or args.plan_source
                or args.retirement_proof or args.operator_contract
            ):
                raise WorkstreamGenerationError("invalid_generation_abort_cli")
            transport = GenerationTransport(
                client, issue_id=args.token.upper(), workstream_id=args.token.upper(),
                authority=authority, candidate_loader=lambda _plan: {},
            )
            output = transport.abort(
                reservation_id=args.abort_reservation_id,
                reservation_sha256=args.abort_reservation_sha256,
                reason=args.abort_reason, created_at=args.created_at,
            )
            json.dump(output, sys.stdout, ensure_ascii=False, sort_keys=True, indent=2)
            sys.stdout.write("\n")
            return 0
        source = plan_payload(args.plan_source, args.plan_identity or args.plan_source)["source"]
        if args.command == "prepare":
            state_result = client.execute(PREPARE_STARTED_STATE_QUERY, {
                "teamId": authority["team_id"], "stateId": args.started_state_id,
            })
            state = state_result.get("workflowState") or {}
            team = state_result.get("team") or {}
            if (
                team.get("id") != authority["team_id"]
                or (team.get("organization") or {}).get("id")
                != authority["workspace_id"]
                or state.get("id") != args.started_state_id
                or (state.get("team") or {}).get("id") != authority["team_id"]
                or str(state.get("type", "")).lower() != "started"
                or not isinstance(state.get("name"), str) or not state["name"]
            ):
                raise WorkstreamGenerationError(
                    "generation_prepare_started_state_readback_mismatch"
                )
            started_state = {
                "id": state["id"], "name": state["name"],
                "type": state["type"], "team_id": authority["team_id"],
            }
            linear_transport = LinearGraphQLTransport(
                client, team_id=authority["team_id"],
                workspace_id=authority["workspace_id"],
                project_id=authority["project_id"],
            )
            graph = linear_transport.snapshot_for_root(
                args.token.upper(), include_description=True,
                include_child_comments=True,
            )
            comments = LinearProjectionAdapter(
                client, issue_id=args.token.upper(),
                workstream_id=args.token.upper(),
                plan_revision=source["sha256"], **authority,
            )._comments()
            output = prepare_generation_operator_contract(
                comments=comments, graph=graph,
                workstream_id=args.token.upper(), authority=authority,
                description_plan_revision=graph["root"].get("plan_revision"),
                target_source={
                    "identity": source["identity"], "sha256": source["sha256"],
                },
                created_at=args.created_at,
                remote_head=args.remote_head,
                started_state=started_state,
            )
            if args.manifest_output:
                destination = Path(args.manifest_output)
                destination.parent.mkdir(parents=True, exist_ok=True)
                encoded = json.dumps(
                    output["projection_preview"]["manifest"],
                    ensure_ascii=False, sort_keys=True, indent=2,
                ) + "\n"
                descriptor, temporary = tempfile.mkstemp(
                    prefix=f".{destination.name}.", dir=destination.parent,
                )
                try:
                    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                        handle.write(encoded)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(temporary, destination)
                except BaseException:
                    try:
                        os.unlink(temporary)
                    except FileNotFoundError:
                        pass
                    raise
            json.dump(output, sys.stdout, ensure_ascii=False, sort_keys=True, indent=2)
            sys.stdout.write("\n")
            return 0
        activation_checkpoint = None
        if args.command == "activate" and args.activation_checkpoint:
            if (
                args.max_bytes != DEFAULT_RESUME_MAX_BYTES
                or args.max_items != 100
                or not args.remote_head
            ):
                raise WorkstreamGenerationError(
                    "generation_activation_checkpoint_requires_default_resume_budget"
                )
            with open(args.activation_checkpoint, encoding="utf-8") as handle:
                activation_checkpoint = json.load(handle)
            validate_checkpoint(activation_checkpoint)
        activation_protocol_remote_head = (
            getattr(args, "remote_head", None)
            if activation_checkpoint is not None else None
        )
        loader = strict_candidate_loader(
            client, token=args.token.upper(), authority=authority,
            plan_source=args.plan_source, plan_identity=args.plan_identity,
            max_bytes=args.max_bytes, max_items=args.max_items,
            activation_checkpoint=activation_checkpoint,
            activation_remote_head=activation_protocol_remote_head,
            activation_created_at=args.created_at,
        )
        linear_transport = LinearGraphQLTransport(
            client, team_id=authority["team_id"],
            workspace_id=authority["workspace_id"],
            project_id=authority["project_id"],
        )
        initial_root_snapshot = linear_transport.snapshot_for_root(
            args.token.upper(), include_description=True,
            include_child_comments=(args.command == "activate"),
        )
        description_plan_revision = initial_root_snapshot["root"].get("plan_revision")
        operator_contract = None
        activation_operator_validator = None
        activation_operator_snapshot_validator = None
        if args.command == "activate":
            with open(args.operator_contract, encoding="utf-8") as handle:
                operator_contract = json.load(handle)

            def activation_operator_snapshot_validator(
                comments_snapshot: list[dict[str, Any]],
                graph_snapshot: dict[str, Any],
            ) -> dict[str, Any]:
                return validate_activation_operator_contract(
                    operator_contract, source={
                        "identity": source["identity"],
                        "sha256": source["sha256"],
                    }, workstream_id=args.token.upper(), authority=authority,
                    comments=comments_snapshot, graph=graph_snapshot,
                    description_plan_revision=graph_snapshot["root"].get(
                        "plan_revision"
                    ), created_at=args.created_at,
                    remote_head=args.remote_head,
                )

            def activation_operator_validator() -> dict[str, Any]:
                graph_before = linear_transport.snapshot_for_root(
                    args.token.upper(), include_description=True,
                    include_child_comments=True,
                )
                adapter = LinearProjectionAdapter(
                    client, issue_id=args.token.upper(),
                    workstream_id=args.token.upper(),
                    plan_revision=source["sha256"], **authority,
                )
                comments_before = adapter._comments()
                graph_after = linear_transport.snapshot_for_root(
                    args.token.upper(), include_description=True,
                    include_child_comments=True,
                )
                comments_after = adapter._comments()
                graph_fence = linear_transport.snapshot_for_root(
                    args.token.upper(), include_description=True,
                    include_child_comments=True,
                )
                if (
                    graph_before != graph_after or graph_after != graph_fence
                    or comments_before != comments_after
                ):
                    raise WorkstreamGenerationError(
                        "generation_operator_snapshot_changed_during_read"
                    )
                return activation_operator_snapshot_validator(
                    comments_after, graph_fence,
                )

        transport = GenerationTransport(
            client, issue_id=args.token.upper(), workstream_id=args.token.upper(),
            authority=authority, candidate_loader=loader,
            legacy_description_plan_revision=description_plan_revision,
            native_root_loader=(
                (lambda: _activation_native_root_snapshot(
                    linear_transport, args.token.upper(),
                ))
                if args.command == "activate" else None
            ),
            source_loader=(
                (lambda: plan_payload(
                    args.plan_source, args.plan_identity or args.plan_source,
                )["source"])
                if args.command == "activate" else None
            ),
            operator_validator=activation_operator_validator,
            operator_snapshot_validator=activation_operator_snapshot_validator,
            operator_contract_sha256=(
                _digest(operator_contract)
                if isinstance(operator_contract, dict) else None
            ),
            operator_remote_head=(
                operator_contract.get("remote_head")
                if isinstance(operator_contract, dict) else None
            ),
        )
        retirement = None
        if args.command == "activate":
            retirement = (
                operator_contract.get("retirement_proof")
                if isinstance(operator_contract, dict) else None
            )
        if not args.apply and args.command == "activate":
            output = transport.preview_activate(
                target_plan_revision=source["sha256"],
                created_at=args.created_at, retirement=retirement,
                activation_checkpoint=activation_checkpoint,
                remote_head=args.remote_head,
            )
        elif not args.apply:
            receipt = loader(source["sha256"])
            output = {"apply": False, "command": args.command, "candidate": receipt}
        elif args.command == "bootstrap":
            output = transport.bootstrap(
                target_plan_revision=source["sha256"], created_at=args.created_at,
            )
        else:
            if not args.expected_native_root_sha256:
                raise WorkstreamGenerationError(
                    "generation_activate_apply_requires_reviewed_native_root_proof"
                )
            output = transport.activate(
                target_plan_revision=source["sha256"], created_at=args.created_at,
                retirement=retirement,
                activation_checkpoint=activation_checkpoint,
                remote_head=args.remote_head,
                expected_native_root_sha256=args.expected_native_root_sha256,
            )
        if args.apply:
            if output.get("two_phase_finalization") is not None:
                selected, final_candidate = strict_active_generation_receipt(
                    client, token=args.token.upper(), authority=authority,
                    description_plan_revision=description_plan_revision,
                    requested_plan_revision=source["sha256"],
                    requested_loader=loader, max_bytes=args.max_bytes,
                    max_items=args.max_items,
                )
                output["final_active_plan_revision"] = selected["plan_revision"]
                output["final_candidate"] = final_candidate
                if selected["plan_revision"] != output["activated_plan_revision"]:
                    output["post_read_status"] = (
                        "historical_replay_active_generation_advanced"
                    )
                elif (
                    final_candidate["graph_frontier_sha256"]
                    != output["bound_graph_frontier_sha256"]
                    or final_candidate["snapshot_sha256"]
                    != output["bound_candidate_resume_sha256"]
                ):
                    raise WorkstreamGenerationError(
                        "authority_changed_with_post_read_drift"
                    )
                else:
                    output["post_read_status"] = (
                        "authority_bound_two_phase_finalization_match"
                    )
                output["final_native_root"] = output[
                    "native_root_activation_proof"
                ]
            else:
                selected, final_candidate = strict_active_generation_receipt(
                    client, token=args.token.upper(), authority=authority,
                    description_plan_revision=description_plan_revision,
                    requested_plan_revision=source["sha256"],
                    requested_loader=loader, max_bytes=args.max_bytes,
                    max_items=args.max_items,
                )
                output = {
                    **output,
                    "final_active_plan_revision": selected["plan_revision"],
                    "final_candidate": final_candidate,
                }
                if selected["plan_revision"] != output["activated_plan_revision"]:
                    output["post_read_status"] = (
                        "historical_replay_active_generation_advanced"
                    )
                elif (
                    final_candidate["graph_frontier_sha256"]
                    != output["bound_graph_frontier_sha256"]
                    or final_candidate["snapshot_sha256"]
                    != output["bound_candidate_resume_sha256"]
                ):
                    raise WorkstreamGenerationError(
                        "authority_changed_with_post_read_drift"
                    )
                else:
                    output["post_read_status"] = "authority_bound_post_read_match"
        json.dump(output, sys.stdout, ensure_ascii=False, sort_keys=True, indent=2)
        sys.stdout.write("\n")
        return 0
    except (OSError, ValueError, json.JSONDecodeError, LinearTransportError) as error:
        print(f"workstream generation refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
