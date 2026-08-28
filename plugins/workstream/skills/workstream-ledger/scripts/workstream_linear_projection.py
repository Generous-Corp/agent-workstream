#!/usr/bin/env python3
"""Append-only Linear projection for the complete workstream resume surface.

Each projection change is an immutable Linear comment.  Mutable current views
are derived by reducing the complete paginated comment stream; replacement of
a keyed value must name the exact event it supersedes.  This keeps scope,
relations, choices, evidence, provenance, and continuation disposition out of
unfenced issue-description overwrites.
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

from workstream_linear import (
    bootstrap_linear_route, GraphQLClient, HttpGraphQLClient, LinearTransportError,
    validate_issue_route,
)
from workstream_linear_events import COMMENT_CREATE_MUTATION, COMMENTS_QUERY


PROJECTION_PREFIX = "<!-- workstream-projection:v1:"
PROJECTION_RE = re.compile(r"<!-- workstream-projection:v1:([A-Za-z0-9_-]+) -->")
KINDS = {
    "scope", "relation", "choice", "evidence_contract", "source",
    "provenance", "disposition",
}
SINGLETON_KINDS = {"scope", "source", "disposition"}
TOMBSTONE = {"_projection_tombstone": True}


class LinearProjectionError(LinearTransportError):
    """The remote projection cannot be persisted or reduced without guessing."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _immutable(event: dict[str, Any]) -> dict[str, Any]:
    return {key: deepcopy(value) for key, value in event.items() if key != "event_id"}


def _event_id(event: dict[str, Any]) -> str:
    return "wsp_" + hashlib.sha256(_canonical(_immutable(event))).hexdigest()[:32]


def build_projection_event(
    *, workstream_id: str, kind: str, key: str, value: dict[str, Any],
    plan_revision: str, expected_revision: int, created_at: str,
    supersedes_event_id: str | None = None,
) -> dict[str, Any]:
    event = {
        "schema_version": 1,
        "workstream_id": workstream_id.upper(),
        "kind": kind,
        "key": key,
        "value": deepcopy(value),
        "plan_revision": plan_revision,
        "expected_revision": expected_revision,
        "created_at": created_at,
        "supersedes_event_id": supersedes_event_id,
    }
    event["event_id"] = _event_id(event)
    validate_projection_event(event)
    return event


def validate_projection_event(event: dict[str, Any]) -> None:
    required = {
        "schema_version", "event_id", "workstream_id", "kind", "key",
        "value", "plan_revision", "expected_revision", "created_at",
        "supersedes_event_id",
    }
    if set(event) != required or event.get("schema_version") != 1:
        raise LinearProjectionError("invalid_projection_event_fields")
    if event.get("kind") not in KINDS:
        raise LinearProjectionError("invalid_projection_kind")
    for field in ("event_id", "workstream_id", "key", "plan_revision", "created_at"):
        if not isinstance(event.get(field), str) or not event[field].strip():
            raise LinearProjectionError(f"projection_missing:{field}")
    if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", event["workstream_id"]):
        raise LinearProjectionError("invalid_projection_workstream")
    if not isinstance(event.get("value"), dict):
        raise LinearProjectionError("invalid_projection_value")
    value = event["value"]
    tombstone = value == TOMBSTONE
    if event["kind"] == "source" and not tombstone:
        if not re.fullmatch(r"[0-9a-f]{64}", str(value.get("sha256", ""))):
            raise LinearProjectionError("invalid_projection_source_digest")
        if not any(isinstance(value.get(field), str) and value[field].strip()
                   for field in ("url", "identity")):
            raise LinearProjectionError("invalid_projection_source_identity")
    if event["kind"] == "provenance" and not tombstone:
        for field in ("agent", "machine", "session_id"):
            if not isinstance(value.get(field), str) or not value[field].strip():
                raise LinearProjectionError(f"invalid_projection_provenance:{field}")
    if event["kind"] == "disposition" and not tombstone:
        if value.get("disposition") not in {"attach", "create_successor"}:
            raise LinearProjectionError("invalid_projection_disposition")
        if not isinstance(value.get("remote_head"), str) or not re.fullmatch(
            r"[0-9a-f]{40}(?:[0-9a-f]{24})?", value["remote_head"]
        ):
            raise LinearProjectionError("invalid_projection_disposition_head")
        if "recovered_from_checkpoint" not in value or (
            value["recovered_from_checkpoint"] is not None
            and not isinstance(value["recovered_from_checkpoint"], str)
        ):
            raise LinearProjectionError("invalid_projection_disposition_checkpoint")
    if event["kind"] == "choice" and not tombstone and value.get("event_id") != event["key"]:
        raise LinearProjectionError("projection_choice_key_mismatch")
    if event["kind"] == "evidence_contract" and not tombstone:
        if value.get("slice_id") != event["key"]:
            raise LinearProjectionError("projection_evidence_key_mismatch")
        if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(value.get("owning_child", ""))):
            raise LinearProjectionError("projection_evidence_owner_invalid")
    revision = event.get("expected_revision")
    if not isinstance(revision, int) or revision < 0:
        raise LinearProjectionError("invalid_projection_revision")
    supersedes = event.get("supersedes_event_id")
    if supersedes is not None and (not isinstance(supersedes, str) or not supersedes):
        raise LinearProjectionError("invalid_projection_supersedes")
    if event.get("event_id") != _event_id(event):
        raise LinearProjectionError("projection_event_id_mismatch")


def encode_projection_comment(event: dict[str, Any]) -> str:
    validate_projection_event(event)
    material = _canonical(event)
    envelope = {
        "event": event,
        "sha256": hashlib.sha256(material).hexdigest(),
    }
    encoded = base64.urlsafe_b64encode(_canonical(envelope)).decode("ascii").rstrip("=")
    return f"{PROJECTION_PREFIX}{encoded} -->"


def _decode_projection(encoded: str) -> dict[str, Any]:
    try:
        envelope = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        if set(envelope) != {"event", "sha256"}:
            raise ValueError("unexpected envelope fields")
        event = envelope["event"]
        digest = envelope["sha256"]
        if not isinstance(event, dict) or not isinstance(digest, str):
            raise ValueError("invalid envelope")
        if not hmac.compare_digest(digest, hashlib.sha256(_canonical(event)).hexdigest()):
            raise ValueError("digest mismatch")
        validate_projection_event(event)
        return event
    except (
        binascii.Error, json.JSONDecodeError, KeyError, TypeError, ValueError,
        LinearProjectionError,
    ) as error:
        raise LinearProjectionError("malformed_projection_marker") from error


@dataclass(frozen=True)
class ReducedProjection:
    workstream_id: str
    revision: int
    events: tuple[dict[str, Any], ...]
    remote_ids: dict[str, str]
    snapshot: dict[str, Any]


def reduce_projection_comments(
    comments: list[dict[str, Any]], *, workstream_id: str,
    expected_plan_revision: str,
    authenticated_route: dict[str, str] | None = None,
    authenticated_source: dict[str, Any] | None = None,
) -> ReducedProjection:
    observed: dict[str, tuple[dict[str, Any], str, bytes]] = {}
    for comment in comments:
        body = comment.get("body") or ""
        if not isinstance(body, str):
            raise LinearProjectionError("malformed_projection_marker")
        if PROJECTION_PREFIX not in body:
            continue
        matches = PROJECTION_RE.findall(body)
        if len(matches) != 1 or body.count(PROJECTION_PREFIX) != 1:
            raise LinearProjectionError("malformed_projection_marker")
        event = _decode_projection(matches[0])
        if event["workstream_id"] != workstream_id:
            raise LinearProjectionError("workstream_id_mismatch")
        signature = _canonical(event)
        previous = observed.get(event["event_id"])
        if previous:
            reason = "duplicate_projection_event_id" if previous[2] == signature else "conflicting_projection_event_id"
            raise LinearProjectionError(f"{reason}:{event['event_id']}")
        remote_id = comment.get("id")
        if not isinstance(remote_id, str) or not remote_id:
            raise LinearProjectionError("projection_comment_missing_remote_id")
        observed[event["event_id"]] = (event, remote_id, signature)

    history = sorted(
        (item[0] for item in observed.values()),
        key=lambda item: (
            item["plan_revision"], item["expected_revision"],
            item["created_at"], item["event_id"],
        ),
    )
    events = sorted(
        (event for event in history if event["plan_revision"] == expected_plan_revision),
        key=lambda item: (item["expected_revision"], item["created_at"], item["event_id"]),
    )
    stale_events = [
        event for event in history if event["plan_revision"] != expected_plan_revision
    ]
    active: dict[tuple[str, str], dict[str, Any]] = {}
    heads: dict[tuple[str, str], dict[str, Any]] = {}
    for index, event in enumerate(events):
        if event["expected_revision"] > index:
            raise LinearProjectionError(
                f"projection_revision_gap:{event['event_id']}:{event['expected_revision']}:{index}"
            )
        identity = (event["kind"], event["key"])
        current = heads.get(identity)
        supersedes = event["supersedes_event_id"]
        if current is None and supersedes is not None:
            raise LinearProjectionError(f"projection_supersedes_missing:{event['event_id']}")
        if current is not None and supersedes != current["event_id"]:
            raise LinearProjectionError(f"projection_concurrent_conflict:{event['kind']}:{event['key']}")
        heads[identity] = event
        if event["value"] == TOMBSTONE:
            active.pop(identity, None)
        else:
            active[identity] = event

    by_kind: dict[str, list[dict[str, Any]]] = {kind: [] for kind in KINDS}
    for (kind, _key), event in active.items():
        by_kind[kind].append(deepcopy(event["value"]))
    for kind in by_kind:
        by_kind[kind].sort(key=lambda value: _canonical(value))
    for kind in SINGLETON_KINDS:
        if len(by_kind[kind]) > 1:
            raise LinearProjectionError(f"multiple_projection_singletons:{kind}")
    source = by_kind["source"][0] if by_kind["source"] else None
    if source is not None and source.get("sha256") != expected_plan_revision:
        raise LinearProjectionError("projection_source_plan_mismatch")
    if authenticated_source is not None and source is not None:
        source_identity = source.get("identity") or source.get("url")
        if source_identity != authenticated_source.get("identity"):
            raise LinearProjectionError("projection_source_identity_mismatch")
        if source.get("sha256") != authenticated_source.get("sha256"):
            raise LinearProjectionError("projection_source_bytes_mismatch")
    scope = by_kind["scope"][0] if by_kind["scope"] else None
    if authenticated_route is not None and scope is not None:
        linear = scope.get("linear") or {}
        for field in ("workspace_id", "team_id", "project_id", "root_issue_id"):
            if linear.get(field) != authenticated_route.get(field):
                raise LinearProjectionError(f"projection_route_mismatch:{field}")

    snapshot = {
        "scope": scope,
        "relations": by_kind["relation"],
        "choice_events": by_kind["choice"],
        "evidence_contracts": by_kind["evidence_contract"],
        "source": source,
        "provenance": by_kind["provenance"],
        "disposition": by_kind["disposition"][0] if by_kind["disposition"] else None,
        "projection_events": [deepcopy(event) for event in events],
        "projection_history": [deepcopy(event) for event in stale_events],
        "projection_revision": len(events),
        "projection_recovery": {
            "state": (
                "current" if any(by_kind.values())
                else "stale_plan" if stale_events
                else "not_found"
            ),
            "stale_plan_count": len(stale_events),
        },
    }
    return ReducedProjection(
        workstream_id=workstream_id, revision=len(events), events=tuple(events),
        remote_ids={event_id: item[1] for event_id, item in observed.items()},
        snapshot=snapshot,
    )


class LinearProjectionAdapter:
    supports_atomic_cas = False
    supports_append_only_events = True

    def __init__(
        self, client: GraphQLClient, *, issue_id: str, workstream_id: str,
        plan_revision: str, workspace_id: str | None = None,
        team_id: str | None = None, project_id: str | None = None,
        root_issue_id: str | None = None,
    ):
        self.client = client
        self.issue_id = issue_id
        self.workstream_id = workstream_id.upper()
        self.plan_revision = plan_revision
        self.workspace_id = workspace_id
        self.team_id = team_id
        self.project_id = project_id
        self.root_issue_id = root_issue_id
        if any((workspace_id, team_id, project_id, root_issue_id)) and not all(
            (workspace_id, team_id, project_id, root_issue_id)
        ):
            raise ValueError("Linear workspace, team, project, and root issue IDs must be supplied together")

    @classmethod
    def from_env(
        cls, *, issue_id: str, workstream_id: str, plan_revision: str,
        env: dict[str, str] | None = None, config_path: str | None = None,
    ) -> "LinearProjectionAdapter":
        from workstream_config import load_linear_api_key, resolve_linear_route

        values = os.environ if env is None else env
        token = load_linear_api_key(env=values)
        if not token:
            raise LinearProjectionError("linear_auth_unavailable")
        client = HttpGraphQLClient(token)
        route, _resolved = resolve_linear_route(config_path=config_path, env=values)
        if not route:
            route = bootstrap_linear_route(client, workstream_id)
        return cls(
            client, issue_id=issue_id, workstream_id=workstream_id,
            plan_revision=plan_revision, workspace_id=route.get("workspace_id"),
            team_id=route.get("team_id"), project_id=route.get("project_id"),
            root_issue_id=route.get("root_issue_id"),
        )

    def _comments(self) -> list[dict[str, Any]]:
        comments: list[dict[str, Any]] = []
        after: str | None = None
        seen: set[str] = set()
        while True:
            result = self.client.execute(COMMENTS_QUERY, {"issueId": self.issue_id, "after": after})
            issue = result.get("issue")
            if not issue or issue.get("identifier") != self.workstream_id:
                raise LinearProjectionError("Linear workstream issue not found or mismatched")
            if self.root_issue_id and issue.get("id") != self.root_issue_id:
                raise LinearProjectionError("projection_route_mismatch:root_issue_id")
            validate_issue_route(
                issue, workspace_id=self.workspace_id, team_id=self.team_id,
                project_id=self.project_id,
            )
            connection = issue.get("comments") or {}
            nodes = connection.get("nodes")
            page_info = connection.get("pageInfo")
            if not isinstance(nodes, list) or not isinstance(page_info, dict):
                raise LinearProjectionError("invalid Linear comment connection")
            comments.extend(nodes)
            if not page_info.get("hasNextPage"):
                return comments
            after = page_info.get("endCursor")
            if not isinstance(after, str) or not after or after in seen:
                raise LinearProjectionError("invalid Linear comment pagination cursor")
            seen.add(after)

    def state(self) -> ReducedProjection:
        return reduce_projection_comments(
            self._comments(), workstream_id=self.workstream_id,
            expected_plan_revision=self.plan_revision,
            authenticated_route={
                "workspace_id": self.workspace_id,
                "team_id": self.team_id,
                "project_id": self.project_id,
                "root_issue_id": self.root_issue_id,
            } if all((self.workspace_id, self.team_id, self.project_id, self.root_issue_id)) else None,
        )

    def append(self, event: dict[str, Any]) -> dict[str, Any]:
        validate_projection_event(event)
        if event["workstream_id"] != self.workstream_id or event["plan_revision"] != self.plan_revision:
            raise LinearProjectionError("projection_route_or_plan_mismatch")
        before = self.state()
        existing_id = before.remote_ids.get(event["event_id"])
        if existing_id:
            existing = next(item for item in before.events if item["event_id"] == event["event_id"])
            if existing != event:
                raise LinearProjectionError(f"conflicting_projection_event_id:{event['event_id']}")
            return {"event_id": event["event_id"], "remote_id": existing_id, "revision": before.revision}
        if event["expected_revision"] > before.revision:
            raise LinearProjectionError("projection_revision_ahead")
        current = next(
            (
                item for item in reversed(before.events)
                if item["kind"] == event["kind"] and item["key"] == event["key"]
            ),
            None,
        )
        if current is None and event["supersedes_event_id"] is not None:
            raise LinearProjectionError("projection_supersedes_missing")
        if current is not None and event["supersedes_event_id"] != current["event_id"]:
            raise LinearProjectionError(
                f"projection_concurrent_conflict:{event['kind']}:{event['key']}"
            )
        response = self.client.execute(
            COMMENT_CREATE_MUTATION,
            {"input": {"issueId": self.issue_id, "body": encode_projection_comment(event)}},
        )
        created = response.get("commentCreate") or {}
        comment = created.get("comment")
        if created.get("success") is not True or not comment or not comment.get("id"):
            raise LinearProjectionError("Linear comment creation returned no durable receipt")
        after = self.state()
        if after.remote_ids.get(event["event_id"]) != comment["id"]:
            raise LinearProjectionError("projection_append_not_observed")
        return {"event_id": event["event_id"], "remote_id": comment["id"], "revision": after.revision}
