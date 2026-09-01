#!/usr/bin/env python3
"""Bootstrap or advance append-only workstream plan-generation authority.

The command never updates a Linear issue.  It reserves the shared material /
checkpoint boundary, seals and strictly revalidates a candidate, then changes
authority with one final deterministic comment append.  A lost response is
recovered by complete readback; exact historical replay is always zero-write.
"""

from __future__ import annotations

import argparse
import base64
from copy import deepcopy
import hashlib
import hmac
import json
import re
import sys
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
    select_plan_generation, _decode_projection,
)
from workstream_plan import plan_payload
from workstream_resume import (
    DEFAULT_RESUME_MAX_BYTES, add_child_material_history, add_material_history,
    compact_context,
    read_relation_targets,
)
from workstream_checkpoint import (
    acknowledge_checkpoint, recover_latest, validate_checkpoint,
)
from workstream_successor import choose_disposition


RESERVATION_PREFIX = "<!-- workstream-generation-reservation:v2:"
RESERVATION_RE = re.compile(
    r"<!-- workstream-generation-reservation:v2:([A-Za-z0-9_-]+) -->"
)
HEX64 = re.compile(r"[0-9a-f]{64}")
EVENT_ID = re.compile(r"wsp_[0-9a-f]{32}")
RESERVATION_ID = re.compile(r"wsgr_[0-9a-f]{32}")


class WorkstreamGenerationError(LinearTransportError):
    """Generation authority cannot be changed without guessing."""


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


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


def _validate_reservation(value: dict[str, Any]) -> None:
    fields = {
        "schema_version", "reservation_id", "workstream_id", "authority",
        "mode", "from_plan_revision", "to_plan_revision", "activation_epoch",
        "previous_control_event_id", "source", "material_revision",
        "checkpoint_event_ids", "ledger_frontier", "from_projection_revision",
        "to_projection_revision", "graph_frontier_sha256",
        "candidate_resume_sha256", "retirement", "created_at",
    }
    if (
        not isinstance(value, dict) or set(value) != fields
        or value.get("schema_version") != 2
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
    ):
        raise WorkstreamGenerationError("invalid_generation_reservation")
    previous = value["previous_control_event_id"]
    if previous is not None and not EVENT_ID.fullmatch(str(previous)):
        raise WorkstreamGenerationError("invalid_generation_reservation")
    _validate_retirement(
        value["retirement"], value["from_plan_revision"], value["activation_epoch"],
    )
    unsigned = {key: deepcopy(item) for key, item in value.items()
                if key != "reservation_id"}
    if value["reservation_id"] != "wsgr_" + _digest(unsigned)[:32]:
        raise WorkstreamGenerationError("invalid_generation_reservation")


def encode_generation_reservation(value: dict[str, Any]) -> str:
    _validate_reservation(value)
    return _envelope(RESERVATION_PREFIX, "reservation", value)


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
        _decode_projection, PROJECTION_PREFIX, PROJECTION_RE,
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
            event = _decode_projection(matches[0])
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
        event = _decode_projection(matches[0])
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
                completed.add(
                    f"{event['value']['reservation_id']}:"
                    f"{event['value']['reservation_sha256']}"
                )
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
    from workstream_linear_projection import PROJECTION_PREFIX, PROJECTION_RE, _decode_projection
    result = []
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str) or PROJECTION_PREFIX not in body:
            continue
        matches = PROJECTION_RE.findall(body)
        if len(matches) != 1 or body.count(PROJECTION_PREFIX) != 1:
            raise WorkstreamGenerationError("malformed_projection_marker")
        event = _decode_projection(matches[0])
        if event["kind"] in {"generation_genesis", "generation_transition"}:
            result.append(event)
    return result


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
        event = _decode_projection(encoded[0])
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


class GenerationTransport:
    def __init__(
        self, client: Any, *, issue_id: str, workstream_id: str,
        authority: dict[str, str], candidate_loader: CandidateLoader,
        legacy_description_plan_revision: str | None = None,
    ):
        self.client = client
        self.issue_id = issue_id
        self.workstream_id = workstream_id.upper()
        self.authority = dict(authority)
        self.candidate_loader = candidate_loader
        self.legacy_description_plan_revision = legacy_description_plan_revision
        self._capability_checked = False

    def _comments(self) -> list[dict[str, Any]]:
        adapter = LinearProjectionAdapter(
            self.client, issue_id=self.issue_id, workstream_id=self.workstream_id,
            plan_revision="0" * 64, **self.authority,
        )
        return adapter._comments()

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
    ) -> dict[str, Any]:
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
            "schema_version": 2, "workstream_id": self.workstream_id,
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

    def _matching_reservation(
        self, comments: list[dict[str, Any]], *, mode: str, from_plan: str,
        to_plan: str, epoch: int, previous_control: str | None,
        retirement: dict[str, Any], created_at: str,
    ) -> dict[str, Any] | None:
        """Discover an exact crashed operation before applying pending guards."""
        matches = [
            item for item in reduce_generation_reservations(
                comments, workstream_id=self.workstream_id,
                authenticated_route=self.authority,
            )
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
            if len(matching) > 1:
                raise WorkstreamGenerationError("generation_historical_replay_ambiguous")
            if matching:
                select_plan_generation(
                    comments, workstream_id=self.workstream_id,
                    description_plan_revision=from_plan,
                    authenticated_route=self.authority,
                )
                event = matching[0]
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
                        "replay": True}
        return None

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
        if not any(item["reservation_id"] == reservation_id
                   and item["reservation_sha256"] == reservation_sha256
                   for item in pending_generation_reservations(
                       comments, workstream_id=self.workstream_id,
                       authenticated_route=self.authority,
                   )):
            raise WorkstreamGenerationError("generation_abort_after_activation")
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
                expected_remote_head=remote_head,
            )
            if replay:
                return replay
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
        }

    def activate(
        self, *, target_plan_revision: str, created_at: str,
        retirement: dict[str, Any], activation_checkpoint: dict[str, Any] | None = None,
        remote_head: str | None = None,
    ) -> dict[str, Any]:
        comments = self._comments()
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
                expected_remote_head=remote_head,
            )
            if replay:
                return replay
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
            reservation = self._reservation(
                comments=comments, mode="activate", from_plan=from_plan,
                to_plan=target_plan_revision, epoch=epoch,
                previous_control=previous_control, candidate=candidate,
                retirement=retirement, created_at=created_at,
            )
            stored = self._append_reservation(reservation)
        else:
            stored = reservation
        reservation = stored
        comments = self._comments()
        self._assert_reservation_live(comments, reservation)
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
        activation = build_projection_event(
            workstream_id=self.workstream_id, kind="generation_transition", key="root",
            value=value, plan_revision=from_plan,
            expected_revision=from_state.revision, created_at=created_at,
            authority=self.authority,
        )
        # This is the final complete-read fence. Abort uses this same remote CAS
        # slot, while legacy ledger collision successors remain quarantined.
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
        # Authority changes here and only here.  No append follows this call.
        receipt = LinearProjectionAdapter(
            self.client, issue_id=self.issue_id, workstream_id=self.workstream_id,
            plan_revision=from_plan, **self.authority,
        ).append(activation, expected_material_revision=reservation["material_revision"],
                 allowed_generation_reservation_id=reservation["reservation_id"],
                 allow_retired_generation_control=True)
        after = self._comments()
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
        }


def strict_candidate_loader(
    client: Any, *, token: str, authority: dict[str, str],
    plan_source: str, plan_identity: str | None,
    max_bytes: int = DEFAULT_RESUME_MAX_BYTES, max_items: int = 100,
    activation_checkpoint: dict[str, Any] | None = None,
    activation_remote_head: str | None = None,
    activation_created_at: str | None = None,
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
        graph = transport.snapshot_for_root(
            token, include_description=True, include_child_comments=True,
        )
        description_plan_revision = (
            graph["root"].get("plan_revision") or plan_revision
        )
        comments = LinearProjectionAdapter(
            client, issue_id=token, workstream_id=token,
            plan_revision=plan_revision, **authority,
        )._comments()
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
        context = compact_context(
            joined, token, max_bytes=max_bytes, max_items=max_items,
            require_projection_authority=True, include_history=False,
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
    for name in ("bootstrap", "activate"):
        command = commands.add_parser(name)
        command.add_argument("token")
        command.add_argument("--plan-source", required=(name == "bootstrap"))
        command.add_argument("--plan-identity")
        command.add_argument("--created-at", required=True)
        command.add_argument("--max-bytes", type=int, default=DEFAULT_RESUME_MAX_BYTES)
        command.add_argument("--max-items", type=int, default=100)
        command.add_argument("--apply", action="store_true")
        if name == "activate":
            command.add_argument("--retirement-proof",
                                 help="reviewed JSON file containing the durable retirement proof")
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
    return value


def main() -> int:
    args = parser().parse_args()
    try:
        client, authority = _route_and_client(args)
        if args.command == "activate" and args.abort_reservation_id:
            if (
                not args.apply or not args.abort_reservation_sha256
                or not args.abort_reason or args.plan_source or args.retirement_proof
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
        if not args.plan_source or (args.command == "activate" and not args.retirement_proof):
            raise WorkstreamGenerationError("generation_candidate_cli_arguments_incomplete")
        source = plan_payload(args.plan_source, args.plan_identity or args.plan_source)["source"]
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
        loader = strict_candidate_loader(
            client, token=args.token.upper(), authority=authority,
            plan_source=args.plan_source, plan_identity=args.plan_identity,
            max_bytes=args.max_bytes, max_items=args.max_items,
            activation_checkpoint=activation_checkpoint,
            activation_remote_head=getattr(args, "remote_head", None),
            activation_created_at=args.created_at,
        )
        description_plan_revision = LinearGraphQLTransport(
            client, team_id=authority["team_id"],
            workspace_id=authority["workspace_id"],
            project_id=authority["project_id"],
        ).snapshot_for_root(args.token.upper())["root"].get("plan_revision")
        transport = GenerationTransport(
            client, issue_id=args.token.upper(), workstream_id=args.token.upper(),
            authority=authority, candidate_loader=loader,
            legacy_description_plan_revision=description_plan_revision,
        )
        retirement = None
        if args.command == "activate":
            with open(args.retirement_proof, encoding="utf-8") as handle:
                retirement = json.load(handle)
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
            output = transport.activate(
                target_plan_revision=source["sha256"], created_at=args.created_at,
                retirement=retirement,
                activation_checkpoint=activation_checkpoint,
                remote_head=args.remote_head,
            )
        if args.apply:
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
