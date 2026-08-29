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
from workstream_delta import Delta, MutationReceipt, RevisionConflict
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


@dataclass(frozen=True)
class ReducedEventLog:
    workstream_id: str
    revision: int
    events: tuple[Delta, ...]
    remote_ids: dict[str, str]


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
        or reservation["intent_kind"] != "repository_identity_projection"
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
        or intent["kind"] != "scope"
        or intent["key"] != "root"
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
    current_plan_revision: str | None = None,
) -> list[str]:
    reservations = _proven_ledger_reservations(
        comments, workstream_id=workstream_id,
        authenticated_route=authenticated_route,
        current_plan_revision=current_plan_revision,
    )
    proven_remote_ids = {remote_id for _item, remote_id in reservations}
    quarantine_hashes = sorted(
        hashlib.sha256(json.dumps(
            [comment.get("id"), comment.get("body")], ensure_ascii=False,
            sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        for comment in comments
        if isinstance(comment.get("body"), str)
        and SERIALIZATION_PREFIX in comment["body"]
        and comment.get("id") not in proven_remote_ids
    )
    quarantine = (
        ["quarantine:" + hashlib.sha256(
            "".join(quarantine_hashes).encode("ascii")
        ).hexdigest()]
        if quarantine_hashes else []
    )
    return sorted([
        *checkpoint_event_ids,
        *(f"reservation:{item['intent_event']['event_id']}:{item['intent_sha256']}"
          for item, _remote_id in reservations),
        *quarantine,
    ])


def _proven_ledger_reservations(
    comments: list[dict[str, Any]], *, workstream_id: str,
    authenticated_route: dict[str, str], current_plan_revision: str | None,
) -> list[tuple[dict[str, Any], str]]:
    from workstream_linear_projection import reduce_projection_comments

    if current_plan_revision is None:
        return []
    proven: list[tuple[dict[str, Any], str]] = []
    for item, remote_id in reduce_ledger_reservations(
        comments, workstream_id=workstream_id,
    ):
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
        if (
            len(state.events) < item["projection_revision"]
            or prefix_ids != item["projection_frontier_ids"]
            or item["plan_revision"] != current_plan_revision
        ):
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
    # Runtime import avoids the event/projection module cycle. A reservation is
    # released only by its exact event or an authenticated CAS successor.
    pending: list[dict[str, Any]] = []
    from workstream_linear_checkpoints import reduce_checkpoint_comments
    for item, remote_id in reservations:
        reservation_comment = next(
            (comment for comment in comments if comment.get("id") == remote_id),
            None,
        )
        if not isinstance(reservation_comment, dict):
            continue
        reservation_created_at = reservation_comment.get("createdAt")
        if not isinstance(reservation_created_at, str) or not reservation_created_at:
            continue
        reservation_order = (reservation_created_at, remote_id)
        prior_comments = [
            comment for comment in comments
            if isinstance(comment.get("createdAt"), str)
            and isinstance(comment.get("id"), str)
            and (comment["createdAt"], comment["id"]) < reservation_order
        ]
        prior_checkpoints = reduce_checkpoint_comments(
            prior_comments, workstream_id=workstream_id,
        )
        historical_frontier = ledger_serialization_frontier(
            sorted(event["event_id"] for event in prior_checkpoints.checkpoints),
            prior_comments, workstream_id=workstream_id,
            authenticated_route=authenticated_route,
            current_plan_revision=item["plan_revision"],
        )
        if item["frontier_ids"] != historical_frontier:
            continue
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
    encoded = base64.urlsafe_b64encode(
        json.dumps(
            _canonical_event(delta),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).decode("ascii").rstrip("=")
    return f"{EVENT_PREFIX}{encoded} -->"


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
    observed: dict[str, tuple[Delta, str, str]] = {}
    for comment in comments:
        body = comment.get("body")
        if body is None:
            body = ""
        if not isinstance(body, str):
            raise LinearEventError("malformed_event_marker")
        if EVENT_PREFIX not in body:
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

    def apply(self, delta: Delta) -> MutationReceipt:
        if delta.workstream_id != self.issue_id:
            raise LinearEventError("workstream_id_mismatch")
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
            frontier = ledger_serialization_frontier(
                self._checkpoint_frontier(checkpoints), comments,
                workstream_id=self.issue_id,
                authenticated_route=self._observed_authority,
                current_plan_revision=self.plan_revision,
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
