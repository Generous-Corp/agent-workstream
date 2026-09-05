#!/usr/bin/env python3
"""Inert child records activated only by an exact root projection grant."""

from __future__ import annotations

import base64
import binascii
from copy import deepcopy
import hashlib
import hmac
import json
import re
import uuid
from typing import Any

from workstream_checkpoint import validate_checkpoint
from workstream_delta import (
    Delta, material_semantic_field, validate_material_event_semantics,
)
from workstream_linear import LinearTransportError
from workstream_linear_events import (
    COMMENT_CREATE_CAPABILITY_QUERY, COMMENT_CREATE_MUTATION, COMMENTS_QUERY,
    encode_event_comment, EVENT_PREFIX,
)
from workstream_linear_checkpoints import encode_checkpoint_comment, CHECKPOINT_PREFIX


PREFIX = "<!-- workstream-child-proposal:v1:"
PATTERN = re.compile(r"<!-- workstream-child-proposal:v1:([A-Za-z0-9_-]+) -->")


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode()


def proposal_id(kind: str, record: Any) -> str:
    return "wscp_" + hashlib.sha256(_canonical([kind, record])).hexdigest()[:32]


def proposal_slot_id(child_issue_id: str, proposal: str) -> str:
    material = hashlib.sha256(_canonical([
        "workstream-child-proposal-slot-v1", child_issue_id, proposal,
    ])).digest()[:16]
    raw = bytearray(material)
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def _validate_proposal_record(kind: str, record: Any, *,
                              strict_semantics: bool = False) -> None:
    """Validate a proposal record's shape, and on writes also its semantics.

    ``strict_semantics`` is the write boundary.  Decoding stored history stays
    shape-only on purpose: a record that was accepted by an older writer must
    remain readable, or one bad row makes the whole workstream unrecoverable.
    """
    if not isinstance(record, dict):
        raise ValueError("invalid child proposal record")
    if kind == "event":
        required = {
            "created_at", "event_id", "expected_revision", "kind", "payload",
            "source", "workstream_id",
        }
        if (
            set(record) != required
            or not all(
                isinstance(record.get(field), str) and record[field]
                for field in (
                    "created_at", "event_id", "kind", "source", "workstream_id",
                )
            )
            or not isinstance(record.get("expected_revision"), int)
            or isinstance(record.get("expected_revision"), bool)
            or record["expected_revision"] < 0
            or not isinstance(record.get("payload"), dict)
        ):
            raise ValueError("invalid child event proposal record")
        delta = Delta(**record)
        # On a write, semantics are checked before the proposal exists and so
        # before any remote publish, because the encoder that enforces them runs
        # again every time authorized history is replayed.  A record that cannot
        # be re-encoded cannot be read back, so admitting one would strand every
        # later read of this workstream.  The checkpoint branch below has always
        # validated at this boundary; events did not, and that asymmetry is what
        # let a malformed payload reach the ledger.
        if strict_semantics:
            try:
                validate_material_event_semantics(delta)
            except ValueError as error:
                raise ValueError(
                    "invalid_caller_material_event:"
                    f"{delta.event_id}:{material_semantic_field(error) or error}"
                ) from error
        return
    if kind == "checkpoint":
        validate_checkpoint(record)
        return
    raise ValueError("invalid child proposal kind")


def build_proposal(kind: str, record: Any, *, child_workstream_id: str,
                   child_issue_id: str, plan_revision: str) -> dict[str, Any]:
    _validate_proposal_record(kind, record, strict_semantics=True)
    value = {
        "schema_version": 1, "kind": kind,
        "child_workstream_id": child_workstream_id,
        "child_issue_id": child_issue_id, "plan_revision": plan_revision,
        "record": deepcopy(record),
        "record_sha256": hashlib.sha256(_canonical(record)).hexdigest(),
    }
    _validate_proposal_identity(value)
    value["proposal_id"] = proposal_id(kind, record)
    return value


def _validate_proposal_identity(value: dict[str, Any]) -> None:
    """Bind the wrapper route to the business record before either is stored."""
    child_workstream_id = value.get("child_workstream_id")
    child_issue_id = value.get("child_issue_id")
    plan_revision = value.get("plan_revision")
    record = value.get("record")
    try:
        parsed_child_issue_id = uuid.UUID(str(child_issue_id))
        canonical_child_issue_id = str(parsed_child_issue_id) == child_issue_id
    except ValueError:
        canonical_child_issue_id = False
    if (
        not isinstance(child_workstream_id, str)
        or not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", child_workstream_id)
        or not canonical_child_issue_id
        or not isinstance(plan_revision, str)
        or not re.fullmatch(r"[0-9a-f]{64}", plan_revision)
        or not isinstance(record, dict)
        or record.get("workstream_id") != child_workstream_id
        or (
            value.get("kind") == "checkpoint"
            and record.get("plan_revision") != plan_revision
        )
    ):
        raise ValueError("child proposal identity mismatch")


def encode_proposal(value: dict[str, Any], *,
                    strict_semantics: bool = True) -> str:
    expected = {
        "schema_version", "proposal_id", "kind", "child_workstream_id",
        "child_issue_id", "plan_revision", "record", "record_sha256",
    }
    if set(value) != expected or value["schema_version"] != 1:
        raise ValueError("invalid child proposal")
    _validate_proposal_record(
        value.get("kind"), value.get("record"),
        strict_semantics=strict_semantics,
    )
    _validate_proposal_identity(value)
    if value["proposal_id"] != proposal_id(value["kind"], value["record"]):
        raise ValueError("invalid child proposal ID")
    if value["record_sha256"] != hashlib.sha256(
        _canonical(value["record"])
    ).hexdigest():
        raise ValueError("invalid child proposal digest")
    envelope = {"proposal": value, "sha256": hashlib.sha256(_canonical(value)).hexdigest()}
    encoded = base64.urlsafe_b64encode(_canonical(envelope)).decode().rstrip("=")
    return f"{PREFIX}{encoded} -->"


def decode_proposal(body: str) -> dict[str, Any] | None:
    if PREFIX not in body:
        return None
    matches = PATTERN.findall(body)
    if len(matches) != 1 or body.count(PREFIX) != 1:
        raise LinearTransportError("malformed_child_proposal")
    try:
        envelope = json.loads(base64.urlsafe_b64decode(
            matches[0] + "=" * (-len(matches[0]) % 4)
        ))
        value = envelope["proposal"]
        if set(envelope) != {"proposal", "sha256"} or not hmac.compare_digest(
            envelope["sha256"], hashlib.sha256(_canonical(value)).hexdigest()
        ):
            raise ValueError
        # Byte-integrity round trip only: decoding stored history must not
        # apply the write-time semantic gate, or a record an older writer
        # accepted becomes permanently unreadable.
        if encode_proposal(value, strict_semantics=False) != body:
            raise ValueError
        return value
    except (binascii.Error, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise LinearTransportError("malformed_child_proposal") from error


def append_proposal(
    client: Any, value: dict[str, Any], *, reservation: dict[str, Any],
) -> dict[str, Any]:
    """Publish only while the matching root serialization intent is live."""
    from workstream_linear_events import (
        encode_ledger_reservation, pending_ledger_reservations,
    )

    try:
        encode_ledger_reservation(reservation)
        event = reservation["intent_event"]
        event_value = event["value"]
        expected_remote = proposal_slot_id(
            value["child_issue_id"], value["proposal_id"],
        )
        if (
            reservation["intent_kind"] != "child_mutation_projection"
            or event["kind"] != "child_mutation_authorization"
            or event_value["proposal_id"] != value["proposal_id"]
            or event_value["proposal_remote_id"] != expected_remote
            or event_value["record_sha256"] != value["record_sha256"]
            or event_value["mutation_kind"] != value["kind"]
            or event_value["child_workstream_id"] != value["child_workstream_id"]
            or event_value["child_issue_id"] != value["child_issue_id"]
            or event_value["plan_revision"] != value["plan_revision"]
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError) as error:
        raise LinearTransportError("child_proposal_reservation_mismatch") from error
    root_comments = _comments(client, reservation["workstream_id"])
    from workstream_generation import assert_no_pending_generation_reservation

    assert_no_pending_generation_reservation(
        root_comments, workstream_id=reservation["workstream_id"],
        authenticated_route=reservation["authority"],
    )
    pending = pending_ledger_reservations(
        root_comments, workstream_id=reservation["workstream_id"],
        authenticated_route=reservation["authority"],
        current_plan_revision=reservation["plan_revision"],
    )
    if [item for item in pending if item == reservation] != [reservation]:
        from workstream_linear_events import (
            reduce_event_comments, semantic_ledger_reservations,
        )
        from workstream_linear_projection import reduce_projection_comments

        state = reduce_projection_comments(
            root_comments, workstream_id=reservation["workstream_id"],
            expected_plan_revision=reservation["plan_revision"],
            authenticated_route=reservation["authority"],
        )
        material = reduce_event_comments(
            root_comments, workstream_id=reservation["workstream_id"],
        )
        semantic = semantic_ledger_reservations(
            root_comments, workstream_id=reservation["workstream_id"],
            authenticated_route=reservation["authority"],
            current_plan_revision=reservation["plan_revision"],
            intent_event=event,
            expected_material_revision=material.revision,
            expected_projection_revision=state.revision,
            expected_projection_frontier_ids=[
                state.remote_ids[item["event_id"]] for item in state.events
            ],
        )
        if not any(item == reservation for item, _remote_id in semantic):
            raise LinearTransportError("child_proposal_reservation_not_live")
    return _append_proposal(client, value)


def _append_proposal(client: Any, value: dict[str, Any]) -> dict[str, Any]:
    """Low-level deterministic child write; callers must hold root intent."""
    remote_id = proposal_slot_id(value["child_issue_id"], value["proposal_id"])
    comments = _comments(client, value["child_workstream_id"])
    indexed = proposal_index(comments)
    existing_pair = indexed.get(value["proposal_id"])
    existing = existing_pair[1] if existing_pair is not None else None
    body = encode_proposal(value)
    if existing is not None:
        if existing.get("body") != body or existing.get("id") != remote_id:
            raise LinearTransportError("child_proposal_slot_conflict")
        return {"proposal": value, "remote_id": remote_id, "disposition": "existing"}
    fields = ((client.execute(COMMENT_CREATE_CAPABILITY_QUERY, {}).get("__type") or {})
              .get("inputFields") or [])
    if "id" not in {item.get("name") for item in fields if isinstance(item, dict)}:
        raise LinearTransportError("linear_comment_create_id_capability_unavailable")
    try:
        client.execute(COMMENT_CREATE_MUTATION, {"input": {
            "id": remote_id, "issueId": value["child_workstream_id"], "body": body,
        }})
    except LinearTransportError:
        pass
    comments = _comments(client, value["child_workstream_id"])
    observed = next((item for item in comments if item.get("id") == remote_id), None)
    if not isinstance(observed, dict) or observed.get("body") != body:
        raise LinearTransportError("child_proposal_not_observed")
    return {"proposal": value, "remote_id": remote_id, "disposition": "created"}


def _comments(client: Any, issue_id: str) -> list[dict[str, Any]]:
    comments: list[dict[str, Any]] = []
    after = None
    seen: set[str] = set()
    while True:
        result = client.execute(COMMENTS_QUERY, {"issueId": issue_id, "after": after})
        connection = ((result.get("issue") or {}).get("comments") or {})
        nodes = connection.get("nodes")
        page = connection.get("pageInfo")
        if not isinstance(nodes, list) or not isinstance(page, dict):
            raise LinearTransportError("invalid child proposal comment connection")
        comments.extend(nodes)
        if not page.get("hasNextPage"):
            return comments
        after = page.get("endCursor")
        if not isinstance(after, str) or not after or after in seen:
            raise LinearTransportError("invalid child proposal pagination cursor")
        seen.add(after)


def activated_comments(comments: list[dict[str, Any]],
                       authorizations: list[dict[str, Any]], *,
                       child_workstream_id: str, child_issue_id: str,
                       quarantine: list[dict[str, Any]] | None = None,
                       ) -> list[dict[str, Any]]:
    """Materialize authorized proposals, quarantining any that cannot re-encode.

    Replaying an authorized proposal runs it back through the strict new-write
    encoder.  A record that fails there is undecodable history, not a defect in
    the caller's request, so it is skipped and reported rather than raised: one
    bad record must not make the whole workstream unreadable.  Callers pass
    ``quarantine`` to surface the named verdicts.
    """
    by_id = proposal_index(comments)
    synthetic: list[dict[str, Any]] = []
    for event in authorizations:
        value = event["value"]
        if (value.get("child_workstream_id") != child_workstream_id
                or value.get("child_issue_id") != child_issue_id):
            continue
        found = by_id.get(value["proposal_id"])
        if found is None:
            raise LinearTransportError("activated_child_proposal_missing")
        proposal, comment = found
        if (proposal["record_sha256"] != value["record_sha256"]
                or comment.get("id") != value["proposal_remote_id"]
                or proposal["kind"] != value["mutation_kind"]
                or proposal["child_workstream_id"] != child_workstream_id
                or proposal["child_issue_id"] != child_issue_id
                or proposal["plan_revision"] != value["plan_revision"]
                or _proposal_authorization_frontier(proposal) != (
                    value.get("expected_material_revision"),
                    value.get("predecessor_event_id"),
                )):
            raise LinearTransportError("activated_child_proposal_mismatch")
        try:
            body = (encode_event_comment(Delta(**proposal["record"]))
                    if proposal["kind"] == "event"
                    else encode_checkpoint_comment(proposal["record"]))
        except (ValueError, TypeError) as error:
            record = proposal["record"]
            verdict = {
                "verdict": "quarantined_undecodable_record",
                "origin": "preexisting_record",
                "child_workstream_id": child_workstream_id,
                "child_issue_id": child_issue_id,
                "proposal_id": proposal["proposal_id"],
                "mutation_kind": proposal["kind"],
                "event_id": record.get("event_id"),
                "field": material_semantic_field(error),
                "reason": str(error),
            }
            if quarantine is not None:
                quarantine.append(verdict)
            continue
        synthetic.append({**comment, "body": body})
    return [*comments, *synthetic]


def authorized_child_comments(
    comments: list[dict[str, Any]], authorizations: list[dict[str, Any]],
    origin_repairs: list[dict[str, Any]], *, child_workstream_id: str,
    child_issue_id: str, quarantine: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Admit a sealed legacy prefix plus root-authorized synthetic proposals."""
    repairs = [
        event for event in origin_repairs
        if event["value"].get("child_workstream_id") == child_workstream_id
        and event["value"].get("child_issue_id") == child_issue_id
    ]
    if len(repairs) > 1:
        raise LinearTransportError("child_origin_repair_ambiguous")
    if not repairs:
        return activated_comments(
            comments, authorizations,
            child_workstream_id=child_workstream_id,
            child_issue_id=child_issue_id, quarantine=quarantine,
        )
    history = repairs[0]["value"]["child_history"]
    receipts = [
        *history["material_receipts"], *history["checkpoint_receipts"],
    ]
    sealed_ids = [item["remote_id"] for item in receipts]
    if len(sealed_ids) != len(set(sealed_ids)):
        raise LinearTransportError("child_origin_sealed_receipt_ambiguous")
    by_remote_id: dict[str, list[dict[str, Any]]] = {}
    for comment in comments:
        remote_id = comment.get("id")
        if isinstance(remote_id, str):
            by_remote_id.setdefault(remote_id, []).append(comment)
    for receipt in receipts:
        found = by_remote_id.get(receipt["remote_id"], [])
        if len(found) != 1:
            raise LinearTransportError("child_origin_sealed_receipt_missing")
        body = found[0].get("body")
        if (
            not isinstance(body, str)
            or hashlib.sha256(body.encode("utf-8")).hexdigest()
            != receipt["body_sha256"]
        ):
            raise LinearTransportError("child_origin_sealed_receipt_changed")
    sealed = set(sealed_ids)
    for comment in comments:
        body = comment.get("body")
        if (
            isinstance(body, str)
            and (EVENT_PREFIX in body or CHECKPOINT_PREFIX in body)
            and comment.get("id") not in sealed
        ):
            raise LinearTransportError("child_legacy_write_after_origin_seal")
    from workstream_linear_projection import child_origin_history_frontier

    sealed_comments = [
        comment for comment in comments if comment.get("id") in sealed
    ]
    if child_origin_history_frontier(
        sealed_comments, workstream_id=child_workstream_id,
    ) != history:
        raise LinearTransportError("child_origin_sealed_prefix_invalid")
    return activated_comments(
        comments, authorizations,
        child_workstream_id=child_workstream_id,
        child_issue_id=child_issue_id, quarantine=quarantine,
    )


def pending_proposal_obligations(
    comments: list[dict[str, Any]], authorizations: list[dict[str, Any]], *,
    child_workstream_id: str, child_issue_id: str, plan_revision: str,
) -> list[dict[str, Any]]:
    """Expose inert recovery handles without treating proposal payload as state."""
    activated = {
        (
            event["value"]["proposal_id"],
            event["value"]["proposal_remote_id"],
            event["value"]["mutation_kind"],
            event["value"]["record_sha256"],
            event["value"]["plan_revision"],
            event["value"]["child_workstream_id"],
            event["value"]["child_issue_id"],
            event["value"].get("expected_material_revision"),
            event["value"].get("predecessor_event_id"),
        )
        for event in authorizations
        if event["value"].get("child_workstream_id") == child_workstream_id
        and event["value"].get("child_issue_id") == child_issue_id
    }
    result = []
    for proposal, comment in proposal_index(comments).values():
        expected_remote = proposal_slot_id(child_issue_id, proposal["proposal_id"])
        if comment.get("id") != expected_remote:
            raise LinearTransportError("child_proposal_slot_mismatch")
        activation_identity = (
            proposal["proposal_id"], expected_remote, proposal["kind"],
            proposal["record_sha256"], proposal["plan_revision"],
            proposal["child_workstream_id"], proposal["child_issue_id"],
            *_proposal_authorization_frontier(proposal),
        )
        if activation_identity in activated:
            continue
        if (
            proposal["child_workstream_id"] != child_workstream_id
            or proposal["child_issue_id"] != child_issue_id
            or proposal["plan_revision"] != plan_revision
        ):
            raise LinearTransportError("foreign_child_proposal")
        result.append({
            "proposal_id": proposal["proposal_id"],
            "proposal_remote_id": expected_remote,
            "kind": proposal["kind"],
            "record_sha256": proposal["record_sha256"],
            "child_workstream_id": child_workstream_id,
            "child_issue_id": child_issue_id,
        })
    return sorted(result, key=lambda item: item["proposal_id"])


def _proposal_authorization_frontier(
    proposal: dict[str, Any],
) -> tuple[int, str | None]:
    record = proposal["record"]
    if proposal["kind"] == "event":
        return record["expected_revision"], None
    return record["root_revision"], record.get("predecessor_event_id")


def proposal_index(
    comments: list[dict[str, Any]],
) -> dict[str, tuple[dict[str, Any], dict[str, Any]]]:
    by_id: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for comment in comments:
        body = comment.get("body") or ""
        proposal = decode_proposal(body) if isinstance(body, str) else None
        if proposal is not None:
            previous = by_id.get(proposal["proposal_id"])
            if previous is not None:
                reason = (
                    "duplicate_child_proposal" if previous[0] == proposal
                    else "conflicting_child_proposal"
                )
                raise LinearTransportError(f"{reason}:{proposal['proposal_id']}")
            by_id[proposal["proposal_id"]] = (proposal, comment)
    return by_id
