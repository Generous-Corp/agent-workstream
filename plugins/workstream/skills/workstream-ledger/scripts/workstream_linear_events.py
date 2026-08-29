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

from workstream_delta import Delta, MutationReceipt, RevisionConflict
from workstream_linear import (
    GraphQLClient, HttpGraphQLClient, LinearTransportError, validate_issue_route,
)


EVENT_PREFIX = "<!-- workstream-delta:v1:"
EVENT_RE = re.compile(r"<!-- workstream-delta:v1:([A-Za-z0-9_-]+) -->")


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


def deterministic_comment_slot_id(
    slot_kind: str,
    workstream_id: str,
    fence: Any,
    authority: dict[str, str],
) -> str:
    """Return one UUIDv4-shaped remote create slot for a fenced append."""
    if slot_kind not in {"material-event", "checkpoint"}:
        raise LinearEventError("invalid_comment_slot_kind")
    if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", workstream_id.upper()):
        raise LinearEventError("invalid_comment_slot_workstream")
    required = {"workspace_id", "team_id", "project_id", "root_issue_id"}
    if set(authority) != required or not all(
        isinstance(authority[field], str) and authority[field] for field in required
    ):
        raise LinearEventError("comment_slot_authority_incomplete")
    material = json.dumps(
        [f"workstream-{slot_kind}-slot-v1", authority, workstream_id.upper(), fence],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    raw = bytearray(hashlib.sha256(material).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


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
    ):
        if not issue_id:
            raise ValueError("Linear issue ID is required")
        self.client = client
        self.issue_id = issue_id
        self.workspace_id = workspace_id
        self.team_id = team_id
        self.project_id = project_id
        self._observed_authority: dict[str, str] | None = None
        self._comment_id_capability_verified = False
        if any((workspace_id, team_id, project_id)) and not all((workspace_id, team_id, project_id)):
            raise ValueError("Linear workspace, team, and project IDs must be supplied together")

    @classmethod
    def from_env(
        cls,
        *,
        issue_id: str,
        env: dict[str, str] | None = None,
        config_path: str | None = None,
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
        before = self._state(delta.workstream_id)
        existing_id = before.remote_ids.get(delta.event_id)
        if existing_id:
            existing = next(event for event in before.events if event.event_id == delta.event_id)
            if _canonical_event(existing) != _canonical_event(delta):
                raise LinearEventError(f"conflicting_event_id:{delta.event_id}")
            return MutationReceipt(delta.event_id, before.revision, existing_id)
        if delta.expected_revision != before.revision:
            raise RevisionConflict(
                f"expected revision {delta.expected_revision}, live revision {before.revision}"
            )

        if self._observed_authority is None:
            raise LinearEventError("comment_slot_authority_incomplete")
        slot_id = deterministic_comment_slot_id(
            "material-event", delta.workstream_id, delta.expected_revision,
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
            after_error = self._state(delta.workstream_id)
            winner = next(
                (
                    event for event in after_error.events
                    if after_error.remote_ids.get(event.event_id) == slot_id
                ),
                None,
            )
            if winner is not None and _canonical_event(winner) == _canonical_event(delta):
                return MutationReceipt(delta.event_id, after_error.revision, slot_id)
            if winner is not None:
                raise LinearEventError("event_slot_lost_reload_required")
            raise
        created = response.get("commentCreate") or {}
        comment = created.get("comment")
        if (
            created.get("success") is not True
            or not comment
            or comment.get("id") != slot_id
        ):
            raise LinearEventError("Linear comment creation returned no durable receipt")

        after = self._state(delta.workstream_id)
        remote_id = after.remote_ids.get(delta.event_id)
        if remote_id != comment["id"]:
            raise LinearEventError("event_append_not_observed")
        return MutationReceipt(delta.event_id, after.revision, remote_id)
