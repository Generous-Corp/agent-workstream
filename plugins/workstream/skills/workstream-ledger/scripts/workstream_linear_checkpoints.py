#!/usr/bin/env python3
"""Authenticated Linear comment transport for material-boundary checkpoints.

Checkpoint comments use the same documented, append-only ``commentCreate``
boundary as material-delta events, but have their own marker and reducer.  A
remote acknowledgement is derived only after the newly-created comment is
visible in a complete, paginated reread of the root issue's comments.
"""

from __future__ import annotations

import base64
import binascii
from copy import deepcopy
import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass
from typing import Any

from workstream_checkpoint import (
    CheckpointError,
    acknowledge_checkpoint,
    recover_latest,
    validate_checkpoint,
)
from workstream_linear import (
    GraphQLClient, HttpGraphQLClient, LinearTransportError, validate_issue_route,
)
from workstream_linear_events import (
    COMMENT_CREATE_CAPABILITY_QUERY,
    COMMENT_CREATE_MUTATION,
    COMMENTS_QUERY,
    deterministic_comment_slot_id,
    reduce_event_comments,
)


CHECKPOINT_PREFIX = "<!-- workstream-checkpoint:v1:"
CHECKPOINT_RE = re.compile(r"<!-- workstream-checkpoint:v1:([A-Za-z0-9_-]+) -->")


class LinearCheckpointError(LinearTransportError):
    """The remote checkpoint log cannot be persisted or reduced safely."""


@dataclass(frozen=True)
class ReducedCheckpointLog:
    workstream_id: str
    checkpoints: tuple[dict[str, Any], ...]
    remote_ids: dict[str, str]


def _pending_record(checkpoint: dict[str, Any]) -> dict[str, Any]:
    """Return the immutable remote representation of a valid checkpoint."""
    validate_checkpoint(checkpoint)
    record = deepcopy(checkpoint)
    record.pop("acknowledgement")
    return record


def _canonical_record(checkpoint: dict[str, Any]) -> bytes:
    return json.dumps(
        _pending_record(checkpoint),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def encode_checkpoint_comment(checkpoint: dict[str, Any]) -> str:
    record = _pending_record(checkpoint)
    material = json.dumps(
        record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    envelope = {
        "checkpoint": record,
        "sha256": hashlib.sha256(material).hexdigest(),
    }
    encoded = base64.urlsafe_b64encode(
        json.dumps(
            envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).decode("ascii").rstrip("=")
    return f"{CHECKPOINT_PREFIX}{encoded} -->"


def _decode_checkpoint(encoded: str) -> dict[str, Any]:
    try:
        padding = "=" * (-len(encoded) % 4)
        envelope = json.loads(base64.urlsafe_b64decode(encoded + padding))
        if set(envelope) != {"checkpoint", "sha256"}:
            raise ValueError("unexpected checkpoint envelope fields")
        record = envelope["checkpoint"]
        digest = envelope["sha256"]
        if not isinstance(record, dict) or not isinstance(digest, str):
            raise ValueError("invalid checkpoint envelope")
        material = json.dumps(
            record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if not hmac.compare_digest(digest, hashlib.sha256(material).hexdigest()):
            raise ValueError("digest mismatch")
        if "acknowledgement" in record:
            raise ValueError("remote checkpoint contains acknowledgement")
        checkpoint = deepcopy(record)
        checkpoint["acknowledgement"] = {
            "state": "pending",
            "remote_id": None,
            "applied_revision": None,
        }
        validate_checkpoint(checkpoint)
        return checkpoint
    except (
        binascii.Error,
        CheckpointError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        raise LinearCheckpointError("malformed_checkpoint_marker") from exc


def reduce_checkpoint_comments(
    comments: list[dict[str, Any]], *, workstream_id: str
) -> ReducedCheckpointLog:
    """Reduce a complete comment snapshot and derive remote acknowledgements."""
    observed: dict[str, tuple[dict[str, Any], str, bytes]] = {}
    for comment in comments:
        body = comment.get("body")
        if body is None:
            body = ""
        if not isinstance(body, str):
            raise LinearCheckpointError("malformed_checkpoint_marker")
        if CHECKPOINT_PREFIX not in body:
            continue
        matches = CHECKPOINT_RE.findall(body)
        if len(matches) != 1 or body.count(CHECKPOINT_PREFIX) != 1:
            raise LinearCheckpointError("malformed_checkpoint_marker")
        checkpoint = _decode_checkpoint(matches[0])
        if checkpoint["workstream_id"] != workstream_id:
            raise LinearCheckpointError("workstream_id_mismatch")
        event_id = checkpoint["event_id"]
        signature = _canonical_record(checkpoint)
        if event_id in observed:
            previous = observed[event_id]
            reason = (
                "duplicate_checkpoint_event_id"
                if previous[2] == signature
                else "conflicting_checkpoint_event_id"
            )
            raise LinearCheckpointError(f"{reason}:{event_id}")
        remote_id = comment.get("id")
        if not isinstance(remote_id, str) or not remote_id:
            raise LinearCheckpointError("checkpoint_comment_missing_remote_id")
        acknowledged = acknowledge_checkpoint(
            checkpoint, remote_id=remote_id, applied_revision=checkpoint["root_revision"]
        )
        observed[event_id] = (acknowledged, remote_id, signature)

    checkpoints = sorted(
        (item[0] for item in observed.values()),
        key=lambda item: (item["root_revision"], item["event_id"]),
    )
    return ReducedCheckpointLog(
        workstream_id=workstream_id,
        checkpoints=tuple(checkpoints),
        remote_ids={event_id: item[1] for event_id, item in observed.items()},
    )


class LinearCheckpointAdapter:
    """Append-only checkpoint persistence and recovery on a Linear root issue."""

    supports_atomic_cas = False
    supports_append_only_events = True

    def __init__(
        self,
        client: GraphQLClient,
        *,
        issue_id: str,
        workstream_id: str,
        workspace_id: str | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
    ):
        if not issue_id:
            raise ValueError("Linear issue ID is required")
        if not workstream_id:
            raise ValueError("workstream ID is required")
        self.client = client
        self.issue_id = issue_id
        self.workstream_id = workstream_id.upper()
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
        workstream_id: str,
        env: dict[str, str] | None = None,
        config_path: str | None = None,
    ) -> "LinearCheckpointAdapter":
        values = os.environ if env is None else env
        from workstream_config import load_linear_api_key, resolve_linear_route

        token = load_linear_api_key(env=values)
        if not token:
            raise LinearCheckpointError("linear_auth_unavailable")

        route, _resolved = resolve_linear_route(config_path=config_path, env=values)
        route = route or {}
        return cls(
            HttpGraphQLClient(token),
            issue_id=issue_id,
            workstream_id=workstream_id,
            workspace_id=route.get("workspace_id"),
            team_id=route.get("team_id"),
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
                raise LinearCheckpointError("Linear workstream issue not found")
            if issue.get("identifier") != self.workstream_id:
                raise LinearCheckpointError("workstream_id_mismatch")
            try:
                validate_issue_route(
                    issue, workspace_id=self.workspace_id, team_id=self.team_id,
                    project_id=self.project_id,
                )
            except LinearTransportError as error:
                raise LinearCheckpointError(str(error)) from error
            team = issue.get("team") or {}
            project = issue.get("project") or {}
            authority = {
                "workspace_id": (team.get("organization") or {}).get("id"),
                "team_id": team.get("id"),
                "project_id": project.get("id"),
                "root_issue_id": issue.get("id"),
            }
            if not all(isinstance(value, str) and value for value in authority.values()):
                raise LinearCheckpointError("comment_slot_authority_incomplete")
            if self._observed_authority is not None and self._observed_authority != authority:
                raise LinearCheckpointError("comment_slot_authority_changed")
            self._observed_authority = authority  # type: ignore[assignment]
            connection = issue.get("comments")
            if not isinstance(connection, dict):
                raise LinearCheckpointError("invalid Linear comment connection")
            nodes = connection.get("nodes")
            page_info = connection.get("pageInfo")
            if not isinstance(nodes, list) or not isinstance(page_info, dict):
                raise LinearCheckpointError("invalid Linear comment connection")
            comments.extend(nodes)
            if not page_info.get("hasNextPage"):
                return comments
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after or after in seen_cursors:
                raise LinearCheckpointError("invalid Linear comment pagination cursor")
            seen_cursors.add(after)

    def _state(self) -> ReducedCheckpointLog:
        return reduce_checkpoint_comments(
            self._comments(), workstream_id=self.workstream_id
        )

    def _assert_comment_id_capability(self) -> None:
        if self._comment_id_capability_verified:
            return
        result = self.client.execute(COMMENT_CREATE_CAPABILITY_QUERY, {})
        fields = ((result.get("__type") or {}).get("inputFields") or [])
        if not isinstance(fields, list) or "id" not in {
            field.get("name") for field in fields if isinstance(field, dict)
        }:
            raise LinearCheckpointError(
                "linear_comment_create_id_capability_unavailable"
            )
        self._comment_id_capability_verified = True

    def persist(self, checkpoint: dict[str, Any]) -> dict[str, Any]:
        """Persist once and return only a read-after-write remote acknowledgement."""
        validate_checkpoint(checkpoint)
        if checkpoint["workstream_id"] != self.workstream_id:
            raise LinearCheckpointError("workstream_id_mismatch")
        comments = self._comments()
        before = reduce_checkpoint_comments(
            comments, workstream_id=self.workstream_id
        )
        existing_id = before.remote_ids.get(checkpoint["event_id"])
        if existing_id:
            existing = next(
                item
                for item in before.checkpoints
                if item["event_id"] == checkpoint["event_id"]
            )
            if _canonical_record(existing) != _canonical_record(checkpoint):
                raise LinearCheckpointError(
                    f"conflicting_checkpoint_event_id:{checkpoint['event_id']}"
                )
            supplied_ack = checkpoint["acknowledgement"]
            if supplied_ack["state"] == "remote_acknowledged" and (
                supplied_ack != existing["acknowledgement"]
            ):
                raise LinearCheckpointError("checkpoint_acknowledgement_conflict")
            return deepcopy(existing)

        try:
            current = recover_latest(
                list(before.checkpoints), self.workstream_id,
                expected_plan_revision=checkpoint["plan_revision"],
            )
        except CheckpointError as error:
            if str(error) != "checkpoint_not_found":
                raise LinearCheckpointError(str(error)) from error
            current = None
        expected_predecessor = (
            current["checkpoint_event_id"] if current is not None else None
        )
        if checkpoint.get("predecessor_event_id") != expected_predecessor:
            raise LinearCheckpointError(
                "checkpoint_predecessor_stale_reload_required"
            )
        material = reduce_event_comments(comments, workstream_id=self.workstream_id)
        if checkpoint["root_revision"] != material.revision:
            raise LinearCheckpointError(
                "checkpoint_root_revision_stale_reload_required"
            )
        if self._observed_authority is None:
            raise LinearCheckpointError("comment_slot_authority_incomplete")
        slot_id = deterministic_comment_slot_id(
            "checkpoint", self.workstream_id,
            expected_predecessor or "<genesis>", self._observed_authority,
        )
        self._assert_comment_id_capability()

        try:
            response = self.client.execute(
                COMMENT_CREATE_MUTATION,
                {
                    "input": {
                        "id": slot_id,
                        "issueId": self.issue_id,
                        "body": encode_checkpoint_comment(checkpoint),
                    }
                },
            )
        except LinearTransportError:
            after_error = self._state()
            winner = next(
                (
                    item for item in after_error.checkpoints
                    if after_error.remote_ids.get(item["event_id"]) == slot_id
                ),
                None,
            )
            if winner is not None and _canonical_record(winner) == _canonical_record(checkpoint):
                return deepcopy(winner)
            if winner is not None:
                raise LinearCheckpointError(
                    "checkpoint_slot_lost_reload_required"
                )
            raise
        created = response.get("commentCreate") or {}
        comment = created.get("comment")
        if (
            created.get("success") is not True
            or not comment
            or comment.get("id") != slot_id
        ):
            raise LinearCheckpointError(
                "Linear comment creation returned no durable receipt"
            )

        after = self._state()
        remote_id = after.remote_ids.get(checkpoint["event_id"])
        if remote_id != comment["id"]:
            raise LinearCheckpointError("checkpoint_append_not_observed")
        return deepcopy(
            next(
                item
                for item in after.checkpoints
                if item["event_id"] == checkpoint["event_id"]
            )
        )

    def recover(self, *, expected_plan_revision: str) -> dict[str, Any]:
        state = self._state()
        return recover_latest(
            list(state.checkpoints),
            self.workstream_id,
            expected_plan_revision=expected_plan_revision,
        )
