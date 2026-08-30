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
from workstream_delta import Delta
from workstream_linear import LinearTransportError
from workstream_linear_events import (
    COMMENT_CREATE_CAPABILITY_QUERY, COMMENT_CREATE_MUTATION, COMMENTS_QUERY,
    encode_event_comment,
)
from workstream_linear_checkpoints import encode_checkpoint_comment


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


def _validate_proposal_record(kind: str, record: Any) -> None:
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
        Delta(**record)
        return
    if kind == "checkpoint":
        validate_checkpoint(record)
        return
    raise ValueError("invalid child proposal kind")


def build_proposal(kind: str, record: Any, *, child_workstream_id: str,
                   child_issue_id: str, plan_revision: str) -> dict[str, Any]:
    _validate_proposal_record(kind, record)
    value = {
        "schema_version": 1, "kind": kind,
        "child_workstream_id": child_workstream_id,
        "child_issue_id": child_issue_id, "plan_revision": plan_revision,
        "record": deepcopy(record),
        "record_sha256": hashlib.sha256(_canonical(record)).hexdigest(),
    }
    value["proposal_id"] = proposal_id(kind, record)
    return value


def encode_proposal(value: dict[str, Any]) -> str:
    expected = {
        "schema_version", "proposal_id", "kind", "child_workstream_id",
        "child_issue_id", "plan_revision", "record", "record_sha256",
    }
    if set(value) != expected or value["schema_version"] != 1:
        raise ValueError("invalid child proposal")
    _validate_proposal_record(value.get("kind"), value.get("record"))
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
        if encode_proposal(value) != body:
            raise ValueError
        return value
    except (binascii.Error, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise LinearTransportError("malformed_child_proposal") from error


def append_proposal(client: Any, value: dict[str, Any]) -> dict[str, Any]:
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
                       child_workstream_id: str, child_issue_id: str) -> list[dict[str, Any]]:
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
                or proposal["plan_revision"] != value["plan_revision"]):
            raise LinearTransportError("activated_child_proposal_mismatch")
        body = (encode_event_comment(Delta(**proposal["record"]))
                if proposal["kind"] == "event"
                else encode_checkpoint_comment(proposal["record"]))
        synthetic.append({**comment, "body": body})
    return [*comments, *synthetic]


def pending_proposal_obligations(
    comments: list[dict[str, Any]], authorizations: list[dict[str, Any]], *,
    child_workstream_id: str, child_issue_id: str, plan_revision: str,
) -> list[dict[str, Any]]:
    """Expose inert recovery handles without treating proposal payload as state."""
    activated = {
        event["value"]["proposal_id"] for event in authorizations
        if event["value"].get("child_workstream_id") == child_workstream_id
        and event["value"].get("child_issue_id") == child_issue_id
    }
    result = []
    for proposal, comment in proposal_index(comments).values():
        expected_remote = proposal_slot_id(child_issue_id, proposal["proposal_id"])
        if comment.get("id") != expected_remote:
            raise LinearTransportError("child_proposal_slot_mismatch")
        if proposal["proposal_id"] in activated:
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
