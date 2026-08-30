#!/usr/bin/env python3
"""Authenticated append-only Linear transport for material-delta events.

Linear issue updates are not conditional. This adapter therefore never
rewrites issue state: each delta is one issue comment and the live revision is
the number of unique, valid event comments. A route-scoped deterministic
comment ID is the exclusive remote slot for each revision.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import uuid
from dataclasses import dataclass
from typing import Any

from workstream_checkpoint import CheckpointError, recover_generations
from workstream_delta import (
    MATERIAL_REPAIR_KIND, Delta, MutationReceipt, RevisionConflict,
    canonical_sha256, validate_material_event_semantics,
    validate_reviewed_repair_event_shape,
)
from workstream_linear import (
    GraphQLClient, HttpGraphQLClient, LinearTransportError, validate_issue_route,
)


EVENT_PREFIX = "<!-- workstream-delta:v1:"
EVENT_RE = re.compile(r"<!-- workstream-delta:v1:([A-Za-z0-9_-]+) -->")
SERIALIZATION_PREFIX = "<!-- workstream-ledger-reservation:v1:"
SERIALIZATION_RE = re.compile(
    r"<!-- workstream-ledger-reservation:v1:([A-Za-z0-9_-]+) -->"
)
MAX_LEDGER_RESERVATION_BYTES = 64 * 1024
MAX_WORKSTREAM_ID_BYTES = 64


COMMENTS_QUERY = """
query WorkstreamDeltaComments($issueId: String!, $after: String) {
  issue(id: $issueId) {
    id
    identifier
    team { id organization { id } }
    project { id }
    comments(first: 250, after: $after) {
      nodes { id body createdAt updatedAt }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

COMMENT_CREATE_MUTATION = """
mutation WorkstreamDeltaCommentCreate($input: CommentCreateInput!) {
  commentCreate(input: $input) {
    success
    comment { id body createdAt updatedAt }
  }
}
"""

COMMENT_CREATE_CAPABILITY_QUERY = """
query WorkstreamEventCommentCreateCapability {
  __type(name: "CommentCreateInput") { inputFields { name } }
}
"""


class LinearEventError(LinearTransportError):
    """The remote event journal cannot be reduced without guessing."""


class PinnedRepairPreconditionError(LinearEventError):
    """A repair's final internal read no longer matches its pinned manifest."""


@dataclass(frozen=True)
class ReducedEventLog:
    workstream_id: str
    revision: int
    events: tuple[Delta, ...]
    remote_ids: dict[str, str]
    raw_events: tuple[Delta, ...] = ()
    repair_bindings: tuple[dict[str, Any], ...] = ()


def ledger_boundary_slot_id(
    workstream_id: str,
    material_revision: int,
    checkpoint_event_ids: list[str],
    authority: dict[str, str],
) -> str:
    """Return the shared remote slot for one combined ledger frontier."""
    if (
        not isinstance(workstream_id, str)
        or len(workstream_id.encode("utf-8")) > MAX_WORKSTREAM_ID_BYTES
        or not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", workstream_id.upper())
    ):
        raise LinearEventError("invalid_comment_slot_workstream")
    if (
        not isinstance(material_revision, int)
        or isinstance(material_revision, bool)
        or material_revision < 0
    ):
        raise LinearEventError("invalid_comment_slot_material_revision")
    if (
        not isinstance(checkpoint_event_ids, list)
        or not all(isinstance(item, str) and item for item in checkpoint_event_ids)
        or checkpoint_event_ids != sorted(set(checkpoint_event_ids))
    ):
        raise LinearEventError("invalid_comment_slot_checkpoint_frontier")
    required = {"workspace_id", "team_id", "project_id", "root_issue_id"}
    if set(authority) != required or not all(
        isinstance(authority[field], str) and authority[field] for field in required
    ):
        raise LinearEventError("comment_slot_authority_incomplete")
    stable_authority = {
        "workspace_id": authority["workspace_id"],
        "root_issue_id": authority["root_issue_id"],
    }
    material = json.dumps(
        [
            "workstream-ledger-boundary-slot-v1", stable_authority,
            material_revision,
            hashlib.sha256(
                json.dumps(
                    checkpoint_event_ids, separators=(",", ":")
                ).encode("utf-8")
            ).hexdigest(),
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    raw = bytearray(hashlib.sha256(material).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def encode_ledger_reservation(reservation: dict[str, Any]) -> str:
    required = {
        "schema_version", "workstream_id", "material_revision", "intent_kind",
        "plan_revision", "projection_revision", "projection_frontier_ids",
        "frontier_ids", "authority", "intent_event", "intent_sha256",
    }
    if (
        not isinstance(reservation, dict)
        or set(reservation) != required
        or not isinstance(reservation.get("schema_version"), int)
        or isinstance(reservation.get("schema_version"), bool)
        or reservation["schema_version"] != 1
        or not isinstance(reservation.get("material_revision"), int)
        or isinstance(reservation.get("material_revision"), bool)
        or reservation["material_revision"] < 0
        or not isinstance(reservation.get("projection_revision"), int)
        or isinstance(reservation.get("projection_revision"), bool)
        or reservation["projection_revision"] < 0
        or not all(
            isinstance(reservation.get(field), str) and reservation[field]
            for field in ("workstream_id", "intent_kind", "plan_revision")
        )
        or len(reservation["workstream_id"].encode("utf-8")) > MAX_WORKSTREAM_ID_BYTES
        or not re.fullmatch(
            r"[A-Z][A-Z0-9]*-\d+", reservation["workstream_id"]
        )
        or reservation["intent_kind"] not in {
            "repository_identity_projection", "repository_identity_history_seal",
        }
        or not re.fullmatch(
            r"wsp_[0-9a-f]{32}", str(
                (reservation.get("intent_event") or {}).get("event_id", "")
            )
        )
        or not re.fullmatch(r"[0-9a-f]{64}", reservation["plan_revision"])
        or not re.fullmatch(r"[0-9a-f]{64}", str(reservation.get("intent_sha256", "")))
        or not isinstance(reservation.get("intent_event"), dict)
        or not isinstance(reservation.get("authority"), dict)
        or not isinstance(reservation.get("frontier_ids"), list)
        or reservation["frontier_ids"] != sorted(set(reservation["frontier_ids"]))
        or not all(isinstance(item, str) and item for item in reservation["frontier_ids"])
        or not isinstance(reservation.get("projection_frontier_ids"), list)
        or len(reservation["projection_frontier_ids"])
        != reservation["projection_revision"]
        or not all(
            isinstance(item, str) and item
            for item in reservation["projection_frontier_ids"]
        )
    ):
        raise LinearEventError("invalid_ledger_reservation")
    from workstream_linear_projection import validate_projection_event

    try:
        validate_projection_event(reservation["intent_event"])
    except LinearTransportError as error:
        raise LinearEventError("invalid_ledger_reservation") from error
    intent = reservation["intent_event"]
    if (
        intent["workstream_id"] != reservation["workstream_id"]
        or intent["plan_revision"] != reservation["plan_revision"]
        or intent["expected_revision"] != reservation["projection_revision"]
        or intent["authority"] != reservation["authority"]
        or (
            reservation["intent_kind"] == "repository_identity_projection"
            and (intent["kind"] != "scope" or intent["key"] != "root")
        )
        or (
            reservation["intent_kind"] == "repository_identity_history_seal"
            and (
                intent["kind"] != "identity_history_seal"
                or intent["key"] != intent["value"].get("sealed_scope_event_id")
            )
        )
        or hashlib.sha256(json.dumps(
            intent, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest() != reservation["intent_sha256"]
    ):
        raise LinearEventError("invalid_ledger_reservation")
    material = json.dumps(
        reservation, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    envelope = {
        "reservation": reservation,
        "sha256": hashlib.sha256(material).hexdigest(),
    }
    encoded = base64.urlsafe_b64encode(json.dumps(
        envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).decode("ascii").rstrip("=")
    body = f"{SERIALIZATION_PREFIX}{encoded} -->"
    if len(body.encode("utf-8")) > MAX_LEDGER_RESERVATION_BYTES:
        raise LinearEventError("ledger_reservation_too_large")
    return body


def reduce_ledger_reservations(
    comments: list[dict[str, Any]], *, workstream_id: str,
) -> list[tuple[dict[str, Any], str]]:
    """Return proven-shaped reservations, quarantining malformed/duplicate intent."""
    observed: dict[str, tuple[dict[str, Any], str]] = {}
    conflicted: set[str] = set()
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str):
            continue
        if SERIALIZATION_PREFIX not in body:
            continue
        matches = SERIALIZATION_RE.findall(body)
        if len(matches) != 1 or body.count(SERIALIZATION_PREFIX) != 1:
            continue
        try:
            encoded = matches[0]
            envelope = json.loads(base64.urlsafe_b64decode(
                encoded + "=" * (-len(encoded) % 4)
            ))
            reservation = envelope["reservation"]
            digest = envelope["sha256"]
            if set(envelope) != {"reservation", "sha256"}:
                raise ValueError("unexpected reservation envelope")
            canonical = json.dumps(
                reservation, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            if not isinstance(digest, str) or not hmac.compare_digest(
                digest, hashlib.sha256(canonical).hexdigest()
            ):
                raise ValueError("reservation digest mismatch")
            if encode_ledger_reservation(reservation) != body:
                raise ValueError("noncanonical reservation body")
        except (
            binascii.Error, KeyError, TypeError, ValueError, json.JSONDecodeError,
            LinearEventError,
        ):
            # Reservation-shaped arbitrary comments are quarantined. They are
            # never durable scheduling authority.
            continue
        if reservation["workstream_id"] != workstream_id:
            continue
        event_id = reservation["intent_event"]["event_id"]
        remote_id = comment.get("id")
        expected_remote_id = ledger_boundary_slot_id(
            workstream_id, reservation["material_revision"],
            reservation["frontier_ids"], reservation["authority"],
        )
        if not isinstance(remote_id, str) or remote_id != expected_remote_id:
            continue
        if event_id in observed or event_id in conflicted:
            observed.pop(event_id, None)
            conflicted.add(event_id)
            continue
        observed[event_id] = (reservation, remote_id)
    return sorted(observed.values(), key=lambda item: (
        item[0]["material_revision"], item[0]["intent_event"]["event_id"],
    ))


def ledger_serialization_frontier(
    checkpoint_event_ids: list[str], comments: list[dict[str, Any]], *,
    workstream_id: str, authenticated_route: dict[str, str],
    current_plan_revision: str | None = None, material_revision: int,
) -> list[str]:
    reservations = _proven_ledger_reservations(
        comments, workstream_id=workstream_id,
        authenticated_route=authenticated_route,
        current_plan_revision=current_plan_revision,
    )
    from workstream_generation import generation_ledger_frontier_tokens

    frontier = sorted([
        *checkpoint_event_ids,
        *(f"reservation:{item['intent_event']['event_id']}:{item['intent_sha256']}"
          for item, _remote_id in reservations),
        *generation_ledger_frontier_tokens(
            comments, workstream_id=workstream_id,
        ),
    ])
    by_id = {
        comment.get("id"): comment for comment in comments
        if isinstance(comment.get("id"), str)
    }
    for _attempt in range(32):
        slot_id = ledger_boundary_slot_id(
            workstream_id, material_revision, frontier, authenticated_route,
        )
        occupant = by_id.get(slot_id)
        if occupant is None:
            return frontier
        collision = "collision:" + hashlib.sha256(json.dumps(
            [occupant.get("id"), occupant.get("body")], ensure_ascii=False,
            sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        if collision in frontier:
            raise LinearEventError("ledger_boundary_collision_cycle")
        frontier = sorted([*frontier, collision])
    raise LinearEventError("ledger_boundary_collision_limit")


def _proven_ledger_reservations(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str], current_plan_revision: str | None,
) -> list[tuple[dict[str, Any], str]]:
    from workstream_linear_projection import reduce_projection_comments

    if current_plan_revision is None:
        return []
    from workstream_linear_checkpoints import reduce_checkpoint_comments
    reduced = reduce_ledger_reservations(
        comments, workstream_id=workstream_id,
    )
    checkpoint_ids = {
        event["event_id"] for event in reduce_checkpoint_comments(
            comments, workstream_id=workstream_id,
        ).checkpoints
    }
    reservation_tokens = {
        f"reservation:{item['intent_event']['event_id']}:{item['intent_sha256']}"
        for item, _remote_id in reduced
    }
    by_id = {comment.get("id"): comment for comment in comments}
    proven: list[tuple[dict[str, Any], str]] = []
    for item, remote_id in reduced:
        if item["authority"] != authenticated_route:
            continue
        try:
            state = reduce_projection_comments(
                comments, workstream_id=workstream_id,
                expected_plan_revision=item["plan_revision"],
                authenticated_route=authenticated_route,
            )
        except LinearTransportError:
            continue
        prefix_ids = [
            state.remote_ids[event["event_id"]]
            for event in state.events[:item["projection_revision"]]
        ]
        base = [value for value in item["frontier_ids"]
                if not value.startswith("collision:")]
        collisions = {value for value in item["frontier_ids"]
                      if value.startswith("collision:")}
        stored_checkpoints = {value for value in base if value in checkpoint_ids}
        completed = any(event == item["intent_event"] for event in state.events)
        if (
            len(state.events) < item["projection_revision"]
            or prefix_ids != item["projection_frontier_ids"]
            or item["plan_revision"] != current_plan_revision
            or (not completed and stored_checkpoints != checkpoint_ids)
            or (completed and not stored_checkpoints.issubset(checkpoint_ids))
            or any(value not in checkpoint_ids and value not in reservation_tokens
                   for value in base)
        ):
            continue
        frontier = sorted(base)
        valid_chain = True
        while collisions:
            slot_id = ledger_boundary_slot_id(
                workstream_id, item["material_revision"], frontier,
                authenticated_route,
            )
            occupant = by_id.get(slot_id)
            if not isinstance(occupant, dict):
                valid_chain = False
                break
            token = "collision:" + hashlib.sha256(json.dumps(
                [occupant.get("id"), occupant.get("body")], ensure_ascii=False,
                sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")).hexdigest()
            if token not in collisions:
                valid_chain = False
                break
            collisions.remove(token)
            frontier = sorted([*frontier, token])
        if not valid_chain or ledger_boundary_slot_id(
            workstream_id, item["material_revision"], frontier,
            authenticated_route,
        ) != remote_id:
            continue
        proven.append((item, remote_id))
    return proven


def pending_ledger_reservations(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str], current_plan_revision: str | None = None,
) -> list[dict[str, Any]]:
    from workstream_linear_projection import reduce_projection_comments

    raw_reservations = [
        item for item, _remote_id in reduce_ledger_reservations(
            comments, workstream_id=workstream_id,
        ) if item["authority"] == authenticated_route
    ]
    if raw_reservations and current_plan_revision is None:
        raise LinearEventError("ledger_reservation_plan_authority_required")
    reservations = _proven_ledger_reservations(
        comments, workstream_id=workstream_id,
        authenticated_route=authenticated_route,
        current_plan_revision=current_plan_revision,
    )
    if not reservations:
        return []
    # A reservation is released only by its exact event or an authenticated
    # CAS successor. Its collision proof is independent of comment chronology.
    pending: list[dict[str, Any]] = []
    for item, _remote_id in reservations:
        state = reduce_projection_comments(
            comments, workstream_id=workstream_id,
            expected_plan_revision=item["plan_revision"],
            authenticated_route=authenticated_route,
        )
        if (
            not any(
                event == item["intent_event"] for event in state.events
            )
            and state.revision <= item["projection_revision"]
        ):
            pending.append(item)
    return pending


def assert_no_pending_ledger_reservation(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str], current_plan_revision: str | None = None,
) -> None:
    pending = pending_ledger_reservations(
        comments, workstream_id=workstream_id,
        authenticated_route=authenticated_route,
        current_plan_revision=current_plan_revision,
    )
    if pending:
        raise LinearEventError(
            f"ledger_boundary_reserved:{pending[0]['intent_event']['event_id']}"
        )


def _canonical_event(delta: Delta) -> dict[str, Any]:
    immutable = {
        "created_at": delta.created_at,
        "event_id": delta.event_id,
        "expected_revision": delta.expected_revision,
        "kind": delta.kind,
        "payload": delta.payload,
        "source": delta.source,
        "workstream_id": delta.workstream_id,
    }
    material = json.dumps(
        immutable, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return {
        **immutable,
        "sha256": hashlib.sha256(material).hexdigest(),
    }


def _rebase_compatible_replay(existing: Delta, requested: Delta) -> bool:
    """Accept only the same event durably rebased to a later revision."""
    if existing.expected_revision < requested.expected_revision:
        return False
    return (
        existing.event_id == requested.event_id
        and existing.workstream_id == requested.workstream_id
        and existing.kind == requested.kind
        and existing.source == requested.source
        and existing.payload == requested.payload
        and existing.created_at == requested.created_at
    )


def encode_event_comment(delta: Delta) -> str:
    # Historical decode is intentionally envelope-only.  Encoding is the
    # strict new-write boundary and must reject before a remote call is made.
    validate_material_event_semantics(delta)
    encoded = base64.urlsafe_b64encode(
        json.dumps(
            _canonical_event(delta),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).decode("ascii").rstrip("=")
    return f"{EVENT_PREFIX}{encoded} -->"


def encode_reviewed_repair_comment(delta: Delta) -> str:
    """Encode only for the fully validated dedicated repair workflow."""
    validate_reviewed_repair_event_shape(delta)
    encoded = base64.urlsafe_b64encode(
        json.dumps(
            _canonical_event(delta), ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).decode("ascii").rstrip("=")
    return f"{EVENT_PREFIX}{encoded} -->"


def assert_exact_pinned_repair_comment(
    comments: list[dict[str, Any]], delta: Delta, *, remote_slot_id: str,
    comment_body_sha256: str,
) -> None:
    """Authenticate the complete immutable repair comment, not only its marker."""
    expected_body = encode_reviewed_repair_comment(delta)
    if (
        not isinstance(comment_body_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", comment_body_sha256) is None
        or hashlib.sha256(expected_body.encode()).hexdigest()
        != comment_body_sha256
    ):
        raise LinearEventError("material_repair_manifest_comment_body_digest_mismatch")
    matches = [comment for comment in comments if comment.get("id") == remote_slot_id]
    if len(matches) != 1 or not isinstance(matches[0].get("body"), str):
        raise LinearEventError("material_repair_pinned_comment_cardinality_mismatch")
    body = matches[0]["body"]
    if (
        body != expected_body
        or hashlib.sha256(body.encode()).hexdigest() != comment_body_sha256
    ):
        raise LinearEventError("material_repair_pinned_comment_body_mismatch")
    encoded = EVENT_RE.findall(body)
    if len(encoded) != 1:
        raise LinearEventError("material_repair_pinned_comment_marker_mismatch")
    try:
        observed = _decode_event(encoded[0])
    except ValueError as error:
        raise LinearEventError(
            "material_repair_pinned_comment_event_mismatch"
        ) from error
    if _canonical_event(observed) != _canonical_event(delta):
        raise LinearEventError("material_repair_pinned_comment_event_mismatch")


def _decode_event(encoded: str) -> Delta:
    try:
        padding = "=" * (-len(encoded) % 4)
        value = json.loads(base64.urlsafe_b64decode(encoded + padding))
        digest = value.pop("sha256")
        material = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if not isinstance(digest, str) or not hmac.compare_digest(
            digest, hashlib.sha256(material).hexdigest()
        ):
            raise ValueError("digest mismatch")
        required = {
            "created_at", "event_id", "expected_revision", "kind", "payload",
            "source", "workstream_id",
        }
        if set(value) != required:
            raise ValueError("unexpected event fields")
        if not all(
            isinstance(value[name], str) and value[name]
            for name in ("created_at", "event_id", "kind", "source", "workstream_id")
        ):
            raise ValueError("empty event identity")
        if not isinstance(value["expected_revision"], int) or value["expected_revision"] < 0:
            raise ValueError("invalid expected revision")
        if not isinstance(value["payload"], dict):
            raise ValueError("invalid payload")
        return Delta(
            value["event_id"], value["workstream_id"], value["kind"],
            value["source"], value["payload"], value["expected_revision"],
            value["created_at"],
        )
    except (binascii.Error, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise LinearEventError("malformed_event_marker") from exc


def reduce_event_comments(
    comments: list[dict[str, Any]], *, workstream_id: str
) -> ReducedEventLog:
    """Reduce a complete comment snapshot, failing closed on ambiguity."""
    from workstream_generation import generation_quarantined_comment_ids

    quarantined_ids = generation_quarantined_comment_ids(
        comments, workstream_id=workstream_id,
    )
    observed: dict[str, tuple[Delta, str, str]] = {}
    for comment in comments:
        body = comment.get("body")
        if body is None:
            body = ""
        if not isinstance(body, str):
            raise LinearEventError("malformed_event_marker")
        if EVENT_PREFIX not in body:
            continue
        if comment.get("id") in quarantined_ids:
            continue
        matches = EVENT_RE.findall(body)
        if len(matches) != 1 or body.count(EVENT_PREFIX) != 1:
            raise LinearEventError("malformed_event_marker")
        delta = _decode_event(matches[0])
        if delta.workstream_id != workstream_id:
            raise LinearEventError("workstream_id_mismatch")
        signature = json.dumps(
            _canonical_event(delta), sort_keys=True, separators=(",", ":")
        )
        if delta.event_id in observed:
            previous = observed[delta.event_id]
            reason = "duplicate_event_id" if previous[2] == signature else "conflicting_event_id"
            raise LinearEventError(f"{reason}:{delta.event_id}")
        remote_id = comment.get("id")
        if not isinstance(remote_id, str) or not remote_id:
            raise LinearEventError("event_comment_missing_remote_id")
        observed[delta.event_id] = (delta, remote_id, signature)

    ordered = sorted(
        (item[0] for item in observed.values()),
        key=lambda event: (event.expected_revision, event.created_at, event.event_id),
    )
    for index, event in enumerate(ordered):
        if event.expected_revision > index:
            raise LinearEventError(
                f"event_revision_gap:{event.event_id}:{event.expected_revision}:{index}"
            )
    return ReducedEventLog(
        workstream_id=workstream_id,
        revision=len(ordered),
        events=tuple(ordered),
        remote_ids={event_id: item[1] for event_id, item in observed.items()},
        raw_events=tuple(ordered),
    )


def _body_by_remote_id(comments: list[dict[str, Any]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for comment in comments:
        remote_id, body = comment.get("id"), comment.get("body")
        if isinstance(remote_id, str) and remote_id and isinstance(body, str):
            if remote_id in result:
                raise LinearEventError(f"duplicate_remote_comment_id:{remote_id}")
            result[remote_id] = body
    return result


def validate_review_artifact_identity(artifact: Any) -> None:
    """Require one canonical immutable GitHub commit:path artifact identity."""
    if not isinstance(artifact, dict) or set(artifact) != {
        "identity", "repository", "commit", "path", "sha256", "reviewed_at",
    }:
        raise ValueError("material_repair_review_artifact_identity_mismatch")
    repository = artifact.get("repository")
    owner = r"[a-z0-9](?:[a-z0-9-]{0,37}[a-z0-9])?"
    repo = r"[a-z0-9](?:[a-z0-9._-]{0,98}[a-z0-9])?"
    artifact_path = artifact.get("path")
    path_is_canonical = (
        isinstance(artifact_path, str)
        and bool(artifact_path)
        and "\\" not in artifact_path
        and all(
            segment not in {"", ".", ".."}
            and re.fullmatch(r"[A-Za-z0-9._-]+", segment) is not None
            for segment in artifact_path.split("/")
        )
    )
    if (
        not isinstance(repository, str)
        or re.fullmatch(rf"github\.com/{owner}/{repo}", repository) is None
        or re.fullmatch(r"[0-9a-f]{40}", str(artifact.get("commit", ""))) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(artifact.get("sha256", ""))) is None
        or not path_is_canonical
        or artifact.get("identity") != (
            f"https://{repository}/blob/{artifact.get('commit')}/{artifact_path}"
        )
        or re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z",
            str(artifact.get("reviewed_at", "")),
        ) is None
    ):
        raise ValueError("material_repair_review_artifact_identity_mismatch")


def material_frontier(log: ReducedEventLog) -> dict[str, Any]:
    records = [_canonical_event(event) for event in log.raw_events or log.events]
    ids = [event["event_id"] for event in records]
    remote_map = {event_id: log.remote_ids[event_id] for event_id in ids}
    return {
        "algorithm": "raw-reducer-order-v1",
        "revision": len(records),
        "event_ids_reducer_order_sha256": canonical_sha256(ids),
        "events_sha256": canonical_sha256(records),
        "remote_map_sha256": canonical_sha256(remote_map),
    }


def apply_material_semantic_repairs(
    raw: ReducedEventLog, comments: list[dict[str, Any]], *,
    checkpoint_frontier: dict[str, Any], projection_frontier: dict[str, Any],
    generation: dict[str, Any], authenticated_route: dict[str, str],
    authenticated_source: dict[str, Any], issue_graph_frontier: dict[str, Any],
    ledger_serialization_frontier_value: list[str], validate_live_fences: bool = True,
) -> ReducedEventLog:
    """Validate repair controls, then overlay replacements at raw positions."""
    controls = [event for event in raw.events if event.kind == MATERIAL_REPAIR_KIND]
    business = [event for event in raw.events if event.kind != MATERIAL_REPAIR_KIND]
    malformed: list[Delta] = []
    for event in business:
        try:
            validate_material_event_semantics(event)
        except ValueError:
            malformed.append(event)
    if not controls:
        if malformed:
            raise LinearEventError(
                f"malformed_material_boundary_unrepaired:{malformed[0].event_id}"
            )
        return raw
    if len(controls) != 1:
        raise LinearEventError("duplicate_material_semantic_repair")
    control = controls[0]
    if not malformed:
        raise LinearEventError("material_semantic_repair_targets_valid_history")
    if control.expected_revision != raw.events.index(control):
        raise LinearEventError("material_semantic_repair_not_at_raw_frontier")
    payload = control.payload
    required = {
        "schema_version", "workstream_id", "target_bindings", "raw_frontier",
        "checkpoint_frontier", "projection_frontier", "generation",
        "authenticated_route", "authenticated_source", "issue_graph_frontier",
        "ledger_serialization_frontier", "postwrite_oracle", "review_artifact",
    }
    if set(payload) != required or payload.get("schema_version") != 1:
        raise LinearEventError("malformed_material_semantic_repair")
    if payload["workstream_id"] != raw.workstream_id:
        raise LinearEventError("material_semantic_repair_workstream_mismatch")
    for field in (
        "raw_frontier", "checkpoint_frontier", "projection_frontier",
        "generation", "authenticated_route", "authenticated_source",
        "issue_graph_frontier", "postwrite_oracle", "review_artifact",
    ):
        if not isinstance(payload[field], dict):
            raise LinearEventError(f"malformed_material_semantic_repair_{field}")
    prefix = ReducedEventLog(
        raw.workstream_id, control.expected_revision,
        tuple(raw.events[:control.expected_revision]),
        {event.event_id: raw.remote_ids[event.event_id]
         for event in raw.events[:control.expected_revision]},
        tuple(raw.events[:control.expected_revision]),
    )
    if payload["raw_frontier"] != material_frontier(prefix):
        raise LinearEventError("material_semantic_repair_material_frontier_drift")
    ledger_frontier = payload["ledger_serialization_frontier"]
    if (
        not isinstance(ledger_frontier, list)
        or ledger_frontier != sorted(set(ledger_frontier))
        or not all(isinstance(item, str) and item for item in ledger_frontier)
    ):
        raise LinearEventError("malformed_material_semantic_repair_ledger_frontier")
    if validate_live_fences and not set(ledger_frontier).issubset(
        ledger_serialization_frontier_value
    ):
        raise LinearEventError("material_semantic_repair_ledger_frontier_drift")
    # The bound serialization frontier is historical. Later checkpoints may
    # extend the live frontier, but cannot change the deterministic base slot
    # occupied by this control. A pre-write preview requires equality with the
    # current complete surface; historical resume instead trusts the immutable
    # control envelope plus its occupied slot and separately bound comment
    # proofs, allowing authorized successor surfaces to advance.
    expected_control_slot = ledger_boundary_slot_id(
        raw.workstream_id, control.expected_revision, ledger_frontier,
        payload["authenticated_route"],
    )
    if raw.remote_ids.get(control.event_id) != expected_control_slot:
        raise LinearEventError("material_semantic_repair_non_base_slot")
    historical_route = payload["authenticated_route"]
    if (
        not isinstance(historical_route, dict)
        or historical_route.get("workspace_id") != authenticated_route.get("workspace_id")
        or historical_route.get("root_issue_id") != authenticated_route.get("root_issue_id")
    ):
        raise LinearEventError("material_semantic_repair_root_authority_drift")
    if validate_live_fences:
        for name, actual in (
            ("checkpoint_frontier", checkpoint_frontier),
            ("projection_frontier", projection_frontier),
            ("generation", generation),
            ("authenticated_route", authenticated_route),
            ("authenticated_source", authenticated_source),
            ("issue_graph_frontier", issue_graph_frontier),
        ):
            if payload[name] != actual:
                raise LinearEventError(f"material_semantic_repair_{name}_drift")
    artifact = payload["review_artifact"]
    try:
        validate_review_artifact_identity(artifact)
    except ValueError:
        raise LinearEventError("malformed_material_semantic_repair_artifact")
    bindings = payload["target_bindings"]
    if not isinstance(bindings, list) or not bindings:
        raise LinearEventError("malformed_material_semantic_repair_targets")
    oracle = payload["postwrite_oracle"]
    oracle_fields = {
        "schema_version", "target_binding_count", "target_bindings_sha256",
        "strict_target_candidate_sha256", "source_identity", "source_sha256",
        "source_event_id", "source_remote_comment_id",
        "source_comment_body_sha256", "source_event_sha256",
        "projection_seal_event_id", "projection_seal_remote_comment_id",
        "projection_seal_comment_body_sha256", "projection_seal_event_sha256",
        "generation_tip_event_id", "fences_sha256",
    }
    bodies = _body_by_remote_id(comments)
    fence_values = {
        key: payload[key] for key in (
            "checkpoint_frontier", "projection_frontier", "generation",
            "authenticated_route", "authenticated_source", "issue_graph_frontier",
        )
    }
    if (
        set(oracle) != oracle_fields
        or oracle.get("schema_version") != 1
        or oracle.get("target_binding_count") != 2
        or len(bindings) != 2
        or oracle.get("target_bindings_sha256") != canonical_sha256(bindings)
        or re.fullmatch(r"[0-9a-f]{64}", str(
            oracle.get("strict_target_candidate_sha256", "")
        )) is None
    ):
        raise LinearEventError("malformed_material_semantic_repair_postwrite_oracle")
    if oracle["source_identity"] != payload["authenticated_source"].get("identity"):
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_source_identity_drift"
        )
    if oracle["source_sha256"] != payload["authenticated_source"].get("sha256"):
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_source_sha256_drift"
        )
    source_body = bodies.get(oracle["source_remote_comment_id"])
    if source_body is None:
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_source_remote_comment_id_drift"
        )
    if hashlib.sha256(source_body.encode()).hexdigest() != oracle[
        "source_comment_body_sha256"
    ]:
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_source_comment_body_sha256_drift"
        )
    seal_body = bodies.get(oracle["projection_seal_remote_comment_id"])
    if seal_body is None:
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_projection_seal_remote_comment_id_drift"
        )
    if hashlib.sha256(seal_body.encode()).hexdigest() != oracle[
        "projection_seal_comment_body_sha256"
    ]:
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_projection_seal_comment_body_sha256_drift"
        )
    try:
        from workstream_linear_projection import (
            PROJECTION_RE, _decode_projection,
        )
        source_matches = PROJECTION_RE.findall(source_body or "")
        seal_matches = PROJECTION_RE.findall(seal_body or "")
        if len(source_matches) != 1 or len(seal_matches) != 1:
            raise ValueError("projection proof marker count")
        source_event = _decode_projection(source_matches[0])
        seal_event = _decode_projection(seal_matches[0])
    except Exception:
        source_event = seal_event = None
    if not isinstance(source_event, dict) or source_event.get("kind") != "source":
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_source_event_malformed"
        )
    if source_event.get("event_id") != oracle["source_event_id"]:
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_source_event_id_drift"
        )
    if source_event.get("value") != payload["authenticated_source"]:
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_source_event_value_drift"
        )
    if canonical_sha256(source_event) != oracle["source_event_sha256"]:
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_source_event_sha256_drift"
        )
    if oracle["projection_seal_event_id"] != payload[
        "projection_frontier"
    ].get("frontier_event_id"):
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_projection_seal_event_id_drift"
        )
    if not isinstance(seal_event, dict):
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_projection_seal_event_malformed"
        )
    if seal_event.get("event_id") != oracle["projection_seal_event_id"]:
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_projection_seal_event_id_drift"
        )
    if canonical_sha256(seal_event) != oracle["projection_seal_event_sha256"]:
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_projection_seal_event_sha256_drift"
        )
    if oracle["generation_tip_event_id"] != payload["generation"].get(
        "transition_tip_event_id"
    ):
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_generation_tip_event_id_drift"
        )
    if oracle["fences_sha256"] != canonical_sha256(fence_values):
        raise LinearEventError(
            "material_semantic_repair_postwrite_oracle_fences_sha256_drift"
        )
    malformed_ids = {event.event_id for event in malformed}
    bound: dict[str, dict[str, Any]] = {}
    positions = {event.event_id: index for index, event in enumerate(raw.events)}
    events = {event.event_id: event for event in raw.events}
    binding_fields = {
        "event_id", "remote_comment_id", "comment_body_sha256",
        "canonical_event_sha256", "payload_sha256", "original_expected_revision",
        "original_index_zero_based", "original_applied_revision", "replacement",
    }
    for binding in bindings:
        if not isinstance(binding, dict) or set(binding) != binding_fields:
            raise LinearEventError("malformed_material_semantic_repair_binding")
        event_id = binding.get("event_id")
        if not isinstance(event_id, str) or event_id in bound:
            raise LinearEventError("duplicate_material_semantic_repair_target")
        event = events.get(event_id)
        if event is None or positions[event_id] >= positions[control.event_id]:
            raise LinearEventError("material_semantic_repair_forward_or_unknown_target")
        if event_id not in malformed_ids:
            raise LinearEventError("material_semantic_repair_valid_target")
        remote_id = raw.remote_ids[event_id]
        body = bodies.get(remote_id)
        if (
            binding["remote_comment_id"] != remote_id
            or body is None
            or binding["comment_body_sha256"] != hashlib.sha256(body.encode()).hexdigest()
            or binding["canonical_event_sha256"] != _canonical_event(event)["sha256"]
            or binding["payload_sha256"] != canonical_sha256(event.payload)
            or binding["original_expected_revision"] != event.expected_revision
            or binding["original_index_zero_based"] != positions[event_id]
            or binding["original_applied_revision"] != positions[event_id] + 1
        ):
            raise LinearEventError("material_semantic_repair_target_drift")
        if binding["replacement"] != {
            "boundary_id": f"repair:{event.event_id}",
            "changes": [{"kind": "progress", "payload": event.payload}],
        }:
            raise LinearEventError(
                "material_semantic_repair_non_lossless_replacement"
            )
        replacement = Delta(
            event.event_id, event.workstream_id, "material_boundary", event.source,
            binding["replacement"], event.expected_revision, event.created_at,
        )
        try:
            validate_material_event_semantics(replacement)
        except ValueError as error:
            raise LinearEventError("malformed_material_semantic_repair_replacement") from error
        bound[event_id] = {**binding, "replacement_event": replacement}
    if set(bound) != malformed_ids:
        raise LinearEventError("material_semantic_repair_incomplete_target_set")
    effective = tuple(
        bound[event.event_id]["replacement_event"] if event.event_id in bound else event
        for event in raw.events
    )
    return ReducedEventLog(
        raw.workstream_id, raw.revision, effective, dict(raw.remote_ids),
        tuple(raw.events), tuple({k: v for k, v in item.items()
                                 if k != "replacement_event"} for item in bound.values()),
    )


def _event_applied_revision(state: ReducedEventLog, event_id: str) -> int:
    """Return the stable canonical position of one event, never the log tip."""
    for index, event in enumerate(state.events, start=1):
        if event.event_id == event_id:
            return index
    raise LinearEventError(f"event_not_observed:{event_id}")


class LinearCommentEventAdapter:
    """Lossless material-delta adapter backed by Linear issue comments."""

    supports_atomic_cas = False
    supports_append_only_events = True

    def __init__(
        self,
        client: GraphQLClient,
        *,
        issue_id: str,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
        root_issue_id: str | None = None,
        plan_revision: str | None = None,
    ):
        if not issue_id:
            raise ValueError("Linear issue ID is required")
        self.client = client
        self.issue_id = issue_id
        self.workspace_id = workspace_id
        self.team_id = team_id
        self.project_id = project_id
        self.root_issue_id = root_issue_id
        if plan_revision is not None and re.fullmatch(r"[0-9a-f]{64}", plan_revision) is None:
            raise ValueError("invalid plan revision")
        self.plan_revision = plan_revision
        self._observed_authority: dict[str, str] | None = None
        self._comment_id_capability_verified = False
        if any((workspace_id, team_id, project_id)) and not all((workspace_id, team_id, project_id)):
            raise ValueError("Linear workspace, team, and project IDs must be supplied together")
        if root_issue_id and not all((workspace_id, team_id, project_id)):
            raise ValueError("Linear root issue ID requires workspace, team, and project IDs")

    @classmethod
    def from_env(
        cls,
        *,
        issue_id: str,
        env: dict[str, str] | None = None,
        config_path: str | None = None,
        plan_revision: str | None = None,
    ) -> "LinearCommentEventAdapter":
        values = os.environ if env is None else env
        from workstream_config import load_linear_api_key, resolve_linear_route

        token = load_linear_api_key(env=values)
        if not token:
            raise LinearEventError("linear_auth_unavailable")

        route, _resolved = resolve_linear_route(config_path=config_path, env=values)
        route = route or {}
        return cls(
            HttpGraphQLClient(token), issue_id=issue_id,
            workspace_id=route.get("workspace_id"), team_id=route.get("team_id"),
            project_id=route.get("project_id"),
            plan_revision=plan_revision,
        )

    def _comments(self) -> list[dict[str, Any]]:
        comments: list[dict[str, Any]] = []
        after: str | None = None
        seen_cursors: set[str] = set()
        while True:
            result = self.client.execute(
                COMMENTS_QUERY, {"issueId": self.issue_id, "after": after}
            )
            issue = result.get("issue")
            if not issue:
                raise LinearEventError("Linear workstream issue not found")
            if issue.get("identifier") != self.issue_id:
                raise LinearEventError("workstream_id_mismatch")
            try:
                validate_issue_route(
                    issue, workspace_id=self.workspace_id, team_id=self.team_id,
                    project_id=self.project_id,
                )
            except LinearTransportError as error:
                raise LinearEventError(str(error)) from error
            team = issue.get("team") or {}
            project = issue.get("project") or {}
            authority = {
                "workspace_id": (team.get("organization") or {}).get("id"),
                "team_id": team.get("id"),
                "project_id": project.get("id"),
                "root_issue_id": issue.get("id"),
            }
            if not all(isinstance(value, str) and value for value in authority.values()):
                raise LinearEventError("comment_slot_authority_incomplete")
            if self.root_issue_id and authority["root_issue_id"] != self.root_issue_id:
                raise LinearEventError("root_issue_id_mismatch")
            if self._observed_authority is not None and self._observed_authority != authority:
                raise LinearEventError("comment_slot_authority_changed")
            self._observed_authority = authority  # type: ignore[assignment]
            connection = issue.get("comments") or {}
            comments.extend(connection.get("nodes") or [])
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                return comments
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after or after in seen_cursors:
                raise LinearEventError("invalid Linear comment pagination cursor")
            seen_cursors.add(after)

    def comments(self) -> list[dict[str, Any]]:
        """Return the complete route-validated comment snapshot."""
        return self._comments()

    def _state(self, workstream_id: str) -> ReducedEventLog:
        return reduce_event_comments(self._comments(), workstream_id=workstream_id)

    def _combined_state(self) -> tuple[ReducedEventLog, Any, list[dict[str, Any]]]:
        comments = self._comments()
        events = reduce_event_comments(comments, workstream_id=self.issue_id)
        # Local import avoids a module cycle: checkpoint transport shares this
        # event transport's query, capability, and boundary-slot helpers.
        from workstream_linear_checkpoints import reduce_checkpoint_comments

        checkpoints = reduce_checkpoint_comments(
            comments, workstream_id=self.issue_id
        )
        return events, checkpoints, comments

    @staticmethod
    def _checkpoint_frontier(checkpoints: Any) -> list[str]:
        return sorted(item["event_id"] for item in checkpoints.checkpoints)

    @staticmethod
    def _validate_checkpoint_prefix(events: ReducedEventLog, checkpoints: Any) -> None:
        records = list(checkpoints.checkpoints)
        if any(item["root_revision"] > events.revision for item in records):
            raise LinearEventError("checkpoint_material_history_incomplete")
        if not records:
            return
        try:
            recover_generations(records, checkpoints.workstream_id)
        except CheckpointError as error:
            raise LinearEventError(str(error)) from error

    def current_revision(self, workstream_id: str) -> int:
        if workstream_id != self.issue_id:
            raise LinearEventError("workstream_id_mismatch")
        return self._state(workstream_id).revision

    def _assert_comment_id_capability(self) -> None:
        if self._comment_id_capability_verified:
            return
        result = self.client.execute(COMMENT_CREATE_CAPABILITY_QUERY, {})
        fields = ((result.get("__type") or {}).get("inputFields") or [])
        if not isinstance(fields, list) or "id" not in {
            field.get("name") for field in fields if isinstance(field, dict)
        }:
            raise LinearEventError("linear_comment_create_id_capability_unavailable")
        self._comment_id_capability_verified = True

    def apply_pinned_repair(
        self, delta: Delta, *, expected_remote_slot: str,
        expected_serialization_frontier: list[str],
        expected_comment_body_sha256: str,
    ) -> MutationReceipt:
        """Append one repair only at its reviewed slot after one final read."""
        if delta.workstream_id != self.issue_id or delta.kind != MATERIAL_REPAIR_KIND:
            raise PinnedRepairPreconditionError("pinned_repair_event_required")
        if (
            not isinstance(expected_remote_slot, str) or not expected_remote_slot
            or not isinstance(expected_serialization_frontier, list)
            or expected_serialization_frontier
            != sorted(set(expected_serialization_frontier))
            or not all(
                isinstance(item, str) and item
                for item in expected_serialization_frontier
            )
        ):
            raise PinnedRepairPreconditionError("invalid_pinned_repair_frontier")
        try:
            before, checkpoints, comments = self._combined_state()
        except (OSError, LinearTransportError) as error:
            raise PinnedRepairPreconditionError(
                "pinned_repair_prewrite_unavailable"
            ) from error
        try:
            self._validate_checkpoint_prefix(before, checkpoints)
        except (OSError, LinearTransportError) as error:
            raise PinnedRepairPreconditionError(
                "pinned_repair_prewrite_unavailable"
            ) from error
        existing_id = before.remote_ids.get(delta.event_id)
        if existing_id:
            existing = next(
                event for event in before.events if event.event_id == delta.event_id
            )
            if (
                existing_id != expected_remote_slot
                or not _rebase_compatible_replay(existing, delta)
            ):
                raise PinnedRepairPreconditionError(
                    f"conflicting_pinned_repair:{delta.event_id}"
                )
            assert_exact_pinned_repair_comment(
                comments, delta, remote_slot_id=expected_remote_slot,
                comment_body_sha256=expected_comment_body_sha256,
            )
            return MutationReceipt(
                delta.event_id, _event_applied_revision(before, delta.event_id),
                existing_id,
            )
        try:
            validate_reviewed_repair_event_shape(delta)
        except ValueError as error:
            raise PinnedRepairPreconditionError(str(error)) from error
        if delta.expected_revision != before.revision:
            raise PinnedRepairPreconditionError(
                "pinned_repair_material_revision_drift"
            )
        if self._observed_authority is None:
            raise PinnedRepairPreconditionError(
                "comment_slot_authority_incomplete"
            )
        from workstream_generation import (
            assert_generation_write_authority,
            assert_no_pending_generation_reservation,
        )
        try:
            assert_no_pending_ledger_reservation(
                comments, workstream_id=self.issue_id,
                authenticated_route=self._observed_authority,
                current_plan_revision=self.plan_revision,
            )
            assert_no_pending_generation_reservation(
                comments, workstream_id=self.issue_id,
                authenticated_route=self._observed_authority,
            )
            assert_generation_write_authority(
                comments, workstream_id=self.issue_id,
                plan_revision=self.plan_revision,
                authenticated_route=self._observed_authority,
            )
        except LinearTransportError as error:
            raise PinnedRepairPreconditionError(
                "pinned_repair_serialization_authority_drift"
            ) from error
        try:
            actual_frontier = ledger_serialization_frontier(
                self._checkpoint_frontier(checkpoints), comments,
                workstream_id=self.issue_id,
                authenticated_route=self._observed_authority,
                current_plan_revision=self.plan_revision,
                material_revision=before.revision,
            )
        except (OSError, LinearTransportError) as error:
            raise PinnedRepairPreconditionError(
                "pinned_repair_prewrite_unavailable"
            ) from error
        if actual_frontier != expected_serialization_frontier:
            raise PinnedRepairPreconditionError(
                "pinned_repair_serialization_frontier_drift"
            )
        if ledger_boundary_slot_id(
            delta.workstream_id, before.revision, actual_frontier,
            self._observed_authority,
        ) != expected_remote_slot:
            raise PinnedRepairPreconditionError("pinned_repair_remote_slot_drift")
        candidate_body = encode_reviewed_repair_comment(delta)
        synthetic_comments = [*comments, {
            "id": expected_remote_slot, "body": candidate_body,
            "createdAt": delta.created_at, "updatedAt": delta.created_at,
        }]
        try:
            synthetic_raw = reduce_event_comments(
                synthetic_comments, workstream_id=self.issue_id,
            )
            apply_material_semantic_repairs(
                synthetic_raw, synthetic_comments,
                checkpoint_frontier=delta.payload["checkpoint_frontier"],
                projection_frontier=delta.payload["projection_frontier"],
                generation=delta.payload["generation"],
                authenticated_route=self._observed_authority,
                authenticated_source=delta.payload["authenticated_source"],
                issue_graph_frontier=delta.payload["issue_graph_frontier"],
                ledger_serialization_frontier_value=actual_frontier,
                validate_live_fences=False,
            )
        except (KeyError, TypeError, ValueError, LinearEventError) as error:
            raise PinnedRepairPreconditionError(
                "pinned_repair_full_validation_failed"
            ) from error
        try:
            self._assert_comment_id_capability()
        except (OSError, LinearTransportError) as error:
            raise PinnedRepairPreconditionError(
                "pinned_repair_prewrite_unavailable"
            ) from error
        try:
            response = self.client.execute(
                COMMENT_CREATE_MUTATION,
                {"input": {
                    "id": expected_remote_slot,
                    "issueId": self.issue_id,
                    "body": candidate_body,
                }},
            )
        except LinearTransportError:
            after, after_checkpoints, after_comments = self._combined_state()
            self._validate_checkpoint_prefix(after, after_checkpoints)
            existing_id = after.remote_ids.get(delta.event_id)
            existing = next(
                (event for event in after.events if event.event_id == delta.event_id),
                None,
            )
            if (
                existing_id == expected_remote_slot
                and existing is not None
                and _canonical_event(existing) == _canonical_event(delta)
            ):
                assert_exact_pinned_repair_comment(
                    after_comments, delta, remote_slot_id=expected_remote_slot,
                    comment_body_sha256=expected_comment_body_sha256,
                )
                return MutationReceipt(
                    delta.event_id,
                    _event_applied_revision(after, delta.event_id),
                    expected_remote_slot,
                )
            raise
        created = response.get("commentCreate") or {}
        comment = created.get("comment")
        if (
            created.get("success") is not True or not comment
            or comment.get("id") != expected_remote_slot
        ):
            raise LinearEventError(
                "Linear comment creation returned no durable receipt"
            )
        after, after_checkpoints, after_comments = self._combined_state()
        self._validate_checkpoint_prefix(after, after_checkpoints)
        if after.remote_ids.get(delta.event_id) != expected_remote_slot:
            raise LinearEventError("event_append_not_observed")
        assert_exact_pinned_repair_comment(
            after_comments, delta, remote_slot_id=expected_remote_slot,
            comment_body_sha256=expected_comment_body_sha256,
        )
        return MutationReceipt(
            delta.event_id, _event_applied_revision(after, delta.event_id),
            expected_remote_slot,
        )

    def apply(self, delta: Delta) -> MutationReceipt:
        if delta.workstream_id != self.issue_id:
            raise LinearEventError("workstream_id_mismatch")
        if delta.kind == MATERIAL_REPAIR_KIND:
            raise LinearEventError("material_semantic_repair_reserved")
        for _attempt in range(8):
            before, checkpoints, comments = self._combined_state()
            self._validate_checkpoint_prefix(before, checkpoints)
            existing_id = before.remote_ids.get(delta.event_id)
            if existing_id:
                existing = next(
                    event for event in before.events
                    if event.event_id == delta.event_id
                )
                if not _rebase_compatible_replay(existing, delta):
                    raise LinearEventError(f"conflicting_event_id:{delta.event_id}")
                return MutationReceipt(
                    delta.event_id,
                    _event_applied_revision(before, delta.event_id),
                    existing_id,
                )
            try:
                validate_material_event_semantics(delta)
            except ValueError as error:
                raise LinearEventError(str(error)) from error
            if delta.expected_revision != before.revision:
                raise RevisionConflict(
                    f"expected revision {delta.expected_revision}, "
                    f"live revision {before.revision}"
                )
            assert_no_pending_ledger_reservation(
                comments, workstream_id=self.issue_id,
                authenticated_route=self._observed_authority,
                current_plan_revision=self.plan_revision,
            )
            if self._observed_authority is None:
                raise LinearEventError("comment_slot_authority_incomplete")
            from workstream_generation import (
                assert_generation_write_authority,
                assert_no_pending_generation_reservation,
            )
            try:
                assert_no_pending_generation_reservation(
                    comments, workstream_id=self.issue_id,
                    authenticated_route=self._observed_authority,
                )
                assert_generation_write_authority(
                    comments, workstream_id=self.issue_id,
                    plan_revision=self.plan_revision,
                    authenticated_route=self._observed_authority,
                )
            except LinearTransportError as error:
                raise LinearEventError(str(error)) from error
            frontier = ledger_serialization_frontier(
                self._checkpoint_frontier(checkpoints), comments,
                workstream_id=self.issue_id,
                authenticated_route=self._observed_authority,
                current_plan_revision=self.plan_revision,
                material_revision=before.revision,
            )
            slot_id = ledger_boundary_slot_id(
                delta.workstream_id, before.revision, frontier,
                self._observed_authority,
            )
            self._assert_comment_id_capability()
            try:
                response = self.client.execute(
                    COMMENT_CREATE_MUTATION,
                    {"input": {
                        "id": slot_id,
                        "issueId": self.issue_id,
                        "body": encode_event_comment(delta),
                    }},
                )
            except LinearTransportError:
                after_events, after_checkpoints, _ = self._combined_state()
                self._validate_checkpoint_prefix(after_events, after_checkpoints)
                winner = next(
                    (
                        event for event in after_events.events
                        if after_events.remote_ids.get(event.event_id) == slot_id
                    ),
                    None,
                )
                if winner is not None and _canonical_event(winner) == _canonical_event(delta):
                    return MutationReceipt(
                        delta.event_id,
                        _event_applied_revision(after_events, delta.event_id),
                        slot_id,
                    )
                if winner is not None:
                    raise RevisionConflict(
                        f"expected revision {delta.expected_revision}, "
                        f"live revision {after_events.revision}"
                    )
                checkpoint_winner = next(
                    (
                        item for item in after_checkpoints.checkpoints
                        if after_checkpoints.remote_ids.get(item["event_id"])
                        == slot_id
                    ),
                    None,
                )
                if checkpoint_winner is not None:
                    if after_events.revision != delta.expected_revision:
                        raise RevisionConflict(
                            f"expected revision {delta.expected_revision}, "
                            f"live revision {after_events.revision}"
                        )
                    continue
                raise
            created = response.get("commentCreate") or {}
            comment = created.get("comment")
            if (
                created.get("success") is not True
                or not comment
                or comment.get("id") != slot_id
            ):
                raise LinearEventError(
                    "Linear comment creation returned no durable receipt"
                )
            after, after_checkpoints, _ = self._combined_state()
            self._validate_checkpoint_prefix(after, after_checkpoints)
            remote_id = after.remote_ids.get(delta.event_id)
            if remote_id != comment["id"]:
                raise LinearEventError("event_append_not_observed")
            return MutationReceipt(
                delta.event_id,
                _event_applied_revision(after, delta.event_id),
                remote_id,
            )
        raise LinearEventError("ledger_boundary_coordination_retry_exhausted")
