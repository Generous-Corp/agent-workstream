#!/usr/bin/env python3
"""Resolve repository redirects and append one fenced identity update.

The provider's immutable repository ID is authority.  Coordinates are routing
data: an old coordinate may be retained as an alias only after authenticated
reads of both the requested and provider-returned canonical coordinates agree
on that immutable ID.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Callable
import urllib.error
import urllib.parse
import urllib.request

from workstream_config import load_linear_api_key, unique_object
from workstream_http import default_ssl_context
from workstream_linear import HttpGraphQLClient, LinearTransportError
from workstream_linear_events import (
    _proven_ledger_reservations, COMMENT_CREATE_MUTATION, encode_ledger_reservation,
    MAX_WORKSTREAM_ID_BYTES,
    ledger_boundary_slot_id, ledger_serialization_frontier,
    pending_ledger_reservations, reduce_event_comments, reduce_ledger_reservations,
)
from workstream_linear_projection import (
    build_projection_event, LinearProjectionAdapter, LinearProjectionError,
    reduce_projection_comments,
)
from workstream_scope import (
    canonical_repository, repository_key, ScopeError, UUID, validate_scope,
)


MAX_PROVIDER_BYTES = 1024 * 1024
MAX_REQUEST_BYTES = 64 * 1024
MAX_AUTHORITY_ID_BYTES = 256
MAX_REPOSITORY_ID_BYTES = 512
REQUEST_FIELDS = {
    "schema_version", "workstream_id", "authority", "plan_revision",
    "repository", "expected_frontier",
}
AUTHORITY_FIELDS = {"workspace_id", "team_id", "project_id", "root_issue_id"}
LINEAR_OFFICIAL_ENDPOINT = "https://api.linear.app/graphql"


class RepositoryIdentityError(RuntimeError):
    """A repository identity update cannot be proven or fenced."""


class _MutationTrackingClient:
    """Record only whether the exact projection mutation may have committed."""

    def __init__(self, client: Any):
        self.client = client
        self.mutation_attempted = False
        self.mutation_transport_unknown = False
        self.mutation_acknowledged = False

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        body = ((variables.get("input") or {}).get("body") or "")
        is_create = (
            "mutation WorkstreamDeltaCommentCreate" in query
            and "<!-- workstream-projection:v1:" in body
        )
        if is_create:
            self.mutation_attempted = True
            self.mutation_transport_unknown = False
            self.mutation_acknowledged = False
        try:
            result = self.client.execute(query, variables)
        except (OSError, TimeoutError, LinearTransportError):
            if is_create:
                self.mutation_transport_unknown = True
            raise
        if is_create:
            created = result.get("commentCreate") or {}
            self.mutation_acknowledged = (
                created.get("success") is True
                and isinstance(created.get("comment"), dict)
                and bool(created["comment"].get("id"))
            )
            if created.get("success") is True and not self.mutation_acknowledged:
                self.mutation_transport_unknown = True
        return result


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def _value_digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class _BoundedRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Permit one same-authority GitHub API redirect and record it."""

    def __init__(self) -> None:
        self.locations: list[str] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        parsed = urllib.parse.urlparse(newurl)
        if (
            len(self.locations) >= 1
            or parsed.scheme != "https"
            or parsed.hostname != "api.github.com"
        ):
            raise RepositoryIdentityError("github_redirect_chain_ambiguous")
        self.locations.append(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class GitHubRepositoryResolver:
    """Authenticated GitHub REST resolver with a bounded redirect chain."""

    def __init__(
        self, token: str, *, timeout: float = 20.0,
        opener_factory: Callable[..., Any] = urllib.request.build_opener,
    ):
        if not isinstance(token, str) or not token:
            raise RepositoryIdentityError("github_auth_unavailable")
        self.token = token
        self.timeout = timeout
        self.opener_factory = opener_factory

    def _read(self, coordinate: str, *, allow_redirect: bool) -> dict[str, Any]:
        slug = canonical_repository(coordinate)
        host, owner, name = slug.split("/", 2)
        if host != "github.com":
            raise RepositoryIdentityError("unsupported_repository_provider")
        handler = _BoundedRedirectHandler()
        opener = self.opener_factory(
            handler, urllib.request.HTTPSHandler(context=default_ssl_context()),
        )
        request = urllib.request.Request(
            f"https://api.github.com/repos/{owner}/{name}",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "agent-workstream",
            },
        )
        try:
            with opener.open(request, timeout=self.timeout) as response:
                raw = response.read(MAX_PROVIDER_BYTES + 1)
                final_url = response.geturl()
        except RepositoryIdentityError:
            raise
        except (OSError, urllib.error.URLError) as error:
            raise RepositoryIdentityError("github_repository_read_unavailable") from error
        if not allow_redirect and handler.locations:
            raise RepositoryIdentityError("github_canonical_coordinate_redirected")
        parsed_final = urllib.parse.urlparse(final_url)
        if parsed_final.scheme != "https" or parsed_final.hostname != "api.github.com":
            raise RepositoryIdentityError("github_response_authority_mismatch")
        if len(raw) > MAX_PROVIDER_BYTES:
            raise RepositoryIdentityError("github_repository_read_too_large")
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RepositoryIdentityError("github_repository_read_malformed") from error
        if not isinstance(payload, dict):
            raise RepositoryIdentityError("github_repository_read_malformed")
        try:
            resolved_slug = canonical_repository(
                f"github.com/{payload['full_name']}"
            )
        except (KeyError, ScopeError) as error:
            raise RepositoryIdentityError("github_repository_read_malformed") from error
        provider_ids = sorted({
            str(value) for value in (payload.get("id"), payload.get("node_id"))
            if value is not None and str(value)
        })
        if not provider_ids:
            raise RepositoryIdentityError("github_repository_id_missing")
        return {
            "requested_slug": slug,
            "resolved_slug": resolved_slug,
            "provider_ids": provider_ids,
            "redirect_count": len(handler.locations),
            "final_url": final_url,
        }

    def resolve(
        self, *, requested_slug: str, provider_repository_id: str,
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        old = self._read(requested_slug, allow_redirect=True)
        current = self._read(old["resolved_slug"], allow_redirect=False)
        if old["resolved_slug"] != current["resolved_slug"]:
            raise RepositoryIdentityError("github_redirect_resolution_changed")
        if old["provider_ids"] != current["provider_ids"]:
            raise RepositoryIdentityError("github_redirect_repository_id_mismatch")
        if provider_repository_id not in old["provider_ids"]:
            raise RepositoryIdentityError("github_repository_identity_mismatch")
        if old["redirect_count"] != 1:
            raise RepositoryIdentityError("github_redirect_not_single_hop")
        timestamp = observed_at or _utc_now()
        return {
            "provider": "github.com",
            "provider_repository_id": provider_repository_id,
            "repository_key": f"github.com:id:{provider_repository_id}",
            "requested_slug": old["requested_slug"],
            "resolved_slug": old["resolved_slug"],
            "observed_at": timestamp,
            "redirect_count": 1,
            "requested_response_url": old["final_url"],
            "canonical_response_url": current["final_url"],
            "authenticated": True,
        }


def _scope_head(state: Any) -> dict[str, Any]:
    heads = [
        event for event in state.events
        if event["kind"] == "scope" and event["key"] == "root"
    ]
    if not heads:
        raise RepositoryIdentityError("repository_scope_projection_missing")
    return heads[-1]


def _reserve_material_frontier(
    adapter: LinearProjectionAdapter, *, comments: list[dict[str, Any]],
    material_revision: int, intent_event: dict[str, Any],
) -> dict[str, Any]:
    """Claim the shared material boundary before writing the projection event."""
    from workstream_linear_checkpoints import reduce_checkpoint_comments

    reservations = reduce_ledger_reservations(
        comments, workstream_id=adapter.workstream_id,
    )
    same_intent = [
        (item, remote_id) for item, remote_id in reservations
        if item["intent_event"]["event_id"] == intent_event["event_id"]
    ]
    if same_intent:
        existing = same_intent[0][0]
        if (
            len(same_intent) != 1
            or existing["intent_event"] != intent_event
            or existing["material_revision"] != material_revision
            or existing["authority"] != adapter.authority
        ):
            raise RepositoryIdentityError(
                "repository_material_reservation_intent_conflict"
            )
        return {
            "event_id": intent_event["event_id"],
            "remote_id": same_intent[0][1],
        }
    checkpoints = reduce_checkpoint_comments(
        comments, workstream_id=adapter.workstream_id,
    )
    frontier = ledger_serialization_frontier(
        sorted(item["event_id"] for item in checkpoints.checkpoints), comments,
        workstream_id=adapter.workstream_id,
        authenticated_route=adapter.authority,
        current_plan_revision=adapter.plan_revision,
        material_revision=material_revision,
    )
    projection = reduce_projection_comments(
        comments, workstream_id=adapter.workstream_id,
        expected_plan_revision=adapter.plan_revision,
        authenticated_route=adapter.authority,
    )
    reservation = {
        "schema_version": 1,
        "workstream_id": adapter.workstream_id,
        "material_revision": material_revision,
        "plan_revision": adapter.plan_revision,
        "projection_revision": intent_event["expected_revision"],
        "projection_frontier_ids": [
            projection.remote_ids[event["event_id"]] for event in projection.events
        ],
        "frontier_ids": frontier,
        "authority": adapter.authority,
        "intent_kind": "repository_identity_projection",
        "intent_event": intent_event,
        "intent_sha256": _value_digest(intent_event),
    }
    slot_id = ledger_boundary_slot_id(
        adapter.workstream_id, material_revision, frontier, adapter.authority,
    )
    body = encode_ledger_reservation(reservation)
    adapter._assert_comment_id_capability()
    try:
        response = adapter.client.execute(COMMENT_CREATE_MUTATION, {"input": {
            "id": slot_id, "issueId": adapter.issue_id, "body": body,
        }})
    except (OSError, TimeoutError, LinearTransportError):
        after = adapter._comments()
        own = next((
            item for item, remote_id in reduce_ledger_reservations(
                after, workstream_id=adapter.workstream_id,
            )
            if remote_id == slot_id and item == reservation
        ), None)
        if own is not None:
            return {"event_id": intent_event["event_id"], "remote_id": slot_id}
        raise RepositoryIdentityError(
            "repository_material_serialization_slot_lost_reload_required"
        )
    created = response.get("commentCreate") or {}
    comment = created.get("comment")
    if (
        created.get("success") is not True
        or not isinstance(comment, dict)
        or comment.get("id") != slot_id
        or comment.get("body") != body
    ):
        raise RepositoryIdentityError("repository_material_reservation_unconfirmed")
    return {"event_id": intent_event["event_id"], "remote_id": slot_id}


def _matching_update(
    repository: dict[str, Any], resolution: dict[str, Any],
) -> list[dict[str, Any]]:
    updates = repository.get("identity_updates", [])
    if not isinstance(updates, list):
        raise RepositoryIdentityError("invalid_repository_identity_update_history")
    return [
        update for update in updates
        if isinstance(update, dict)
        and update.get("from") == resolution["requested_slug"]
        and update.get("to") == resolution["resolved_slug"]
        and update.get("repository_key") == resolution["repository_key"]
        and update.get("provider_repository_id") == resolution["provider_repository_id"]
    ]


def _identity_event_id(repository_key_value: str, requested: str, resolved: str) -> str:
    return "wsri_" + hashlib.sha256(_canonical([
        "repository-identity-update-v1", repository_key_value, requested, resolved,
    ])).hexdigest()[:32]


def _provider_evidence(resolution: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": "authenticated_provider_readback",
        "authenticated": True,
        "repository_key": resolution["repository_key"],
        "provider_repository_id": resolution["provider_repository_id"],
        "requested_slug": resolution["requested_slug"],
        "resolved_slug": resolution["resolved_slug"],
        "redirect_count": resolution["redirect_count"],
        "requested_response_url": resolution["requested_response_url"],
        "canonical_response_url": resolution["canonical_response_url"],
    }


def _recover_pending_resolution(
    adapter: LinearProjectionAdapter, request: dict[str, Any],
) -> dict[str, Any] | None:
    """Recover the exact stored provider proof before attempting a fresh read."""
    comments = adapter._comments()
    pending = pending_ledger_reservations(
        comments, workstream_id=adapter.workstream_id,
        authenticated_route=adapter.authority,
        current_plan_revision=adapter.plan_revision,
    )
    if len(pending) > 1:
        raise RepositoryIdentityError("repository_pending_identity_intent_ambiguous")
    recoverable = [
        item for item, _remote_id in _proven_ledger_reservations(
            comments, workstream_id=adapter.workstream_id,
            authenticated_route=adapter.authority,
            current_plan_revision=adapter.plan_revision,
        )
    ]
    frontier = request["expected_frontier"]
    candidates: list[dict[str, Any]] = []
    for reservation in recoverable:
        event = reservation["intent_event"]
        if (
            reservation["intent_kind"] != "repository_identity_projection"
            or reservation["material_revision"] != frontier["material_revision"]
            or reservation["projection_revision"] != frontier["projection_revision"]
            or event.get("expected_revision") != frontier["projection_revision"]
            or event.get("supersedes_event_id") != frontier["scope_event_id"]
            or event.get("kind") != "scope"
            or event.get("key") != "root"
        ):
            continue
        target = request["repository"]
        repositories = event.get("value", {}).get("repositories", [])
        matches = [
            repository for repository in repositories
            if isinstance(repository, dict)
            and repository.get("provider_repository_id")
                == target["provider_repository_id"]
        ]
        if len(matches) != 1:
            continue
        updates = matches[0].get("identity_updates", [])
        matching = [
            update for update in updates
            if isinstance(update, dict)
            and update.get("from") == target["requested_slug"]
            and update.get("provider_repository_id")
                == target["provider_repository_id"]
        ]
        if len(matching) != 1:
            continue
        update = matching[0]
        evidence = update.get("evidence")
        if not isinstance(evidence, list) or len(evidence) != 1:
            continue
        proof = evidence[0]
        if not isinstance(proof, dict):
            continue
        candidate = {
            "provider": "github.com",
            "provider_repository_id": target["provider_repository_id"],
            "repository_key": update.get("repository_key"),
            "requested_slug": target["requested_slug"],
            "resolved_slug": update.get("to"),
            "observed_at": update.get("observed_at"),
            "redirect_count": proof.get("redirect_count"),
            "requested_response_url": proof.get("requested_response_url"),
            "canonical_response_url": proof.get("canonical_response_url"),
            "authenticated": proof.get("authenticated"),
        }
        try:
            desired, rebuilt_update, _replay = _updated_scope(
                _scope_head(reduce_projection_comments(
                    comments, workstream_id=adapter.workstream_id,
                    expected_plan_revision=adapter.plan_revision,
                    authenticated_route=adapter.authority,
                ))["value"],
                candidate, root_id=adapter.workstream_id,
            )
            rebuilt = build_projection_event(
                workstream_id=adapter.workstream_id, kind="scope", key="root",
                value=desired, plan_revision=adapter.plan_revision,
                expected_revision=frontier["projection_revision"],
                created_at=candidate["observed_at"],
                supersedes_event_id=frontier["scope_event_id"],
                authority=adapter.authority,
            )
        except (KeyError, TypeError, ScopeError, LinearTransportError, RepositoryIdentityError):
            continue
        if rebuilt_update == update and rebuilt == event:
            candidates.append(candidate)
    if len(candidates) > 1:
        raise RepositoryIdentityError("repository_pending_identity_intent_ambiguous")
    if pending and not candidates:
        raise RepositoryIdentityError("repository_pending_identity_intent_conflict")
    return candidates[0] if candidates else None


def _validate_replay_update(
    repository: dict[str, Any], update: dict[str, Any], resolution: dict[str, Any],
) -> None:
    """Require a prior update to carry the complete deterministic proof schema."""
    expected_fields = {
        "event_id", "from", "to", "repository_key", "provider_repository_id",
        "observed_at", "effective_at", "evidence",
    }
    if set(update) != expected_fields:
        raise RepositoryIdentityError("repository_identity_update_replay_mismatch")
    expected_event_id = _identity_event_id(
        resolution["repository_key"], resolution["requested_slug"],
        resolution["resolved_slug"],
    )
    if update["event_id"] != expected_event_id:
        raise RepositoryIdentityError("repository_identity_update_replay_mismatch")
    if (
        not isinstance(update["observed_at"], str)
        or update["effective_at"] != update["observed_at"]
        or update["evidence"] != [_provider_evidence(resolution)]
    ):
        raise RepositoryIdentityError("repository_identity_update_replay_mismatch")
    expected_resolution = {
        "provider_repository_id": resolution["provider_repository_id"],
        "resolved_slug": resolution["resolved_slug"],
        "observed_at": update["observed_at"],
        "evidence": [{
            "kind": "authenticated_provider_readback",
            "authenticated": True,
            "provider_repository_id": resolution["provider_repository_id"],
            "resolved_slug": resolution["resolved_slug"],
        }],
    }
    if repository.get("identity_resolution") != expected_resolution:
        raise RepositoryIdentityError("repository_identity_update_replay_mismatch")


def _updated_scope(
    scope: dict[str, Any], resolution: dict[str, Any], *, root_id: str,
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    desired = deepcopy(scope)
    key = resolution["repository_key"]
    matches = [repo for repo in desired["repositories"] if repository_key(repo) == key]
    if len(matches) != 1:
        raise RepositoryIdentityError("repository_identity_selection_ambiguous")
    repository = matches[0]
    requested = resolution["requested_slug"]
    resolved = resolution["resolved_slug"]
    if requested != repository["slug"] and requested not in repository.get("aliases", []):
        raise RepositoryIdentityError("repository_old_coordinate_not_in_scope")
    for other in desired["repositories"]:
        if other is repository:
            continue
        if resolved == other.get("slug") or resolved in other.get("aliases", []):
            raise RepositoryIdentityError("repository_owner_coordinate_collision")

    existing = _matching_update(repository, resolution)
    if len(existing) > 1:
        raise RepositoryIdentityError("repository_identity_update_ambiguous")
    if existing:
        if repository["slug"] != resolved or requested not in repository.get("aliases", []):
            raise RepositoryIdentityError("repository_identity_update_incomplete")
        _validate_replay_update(repository, existing[0], resolution)
        validate_scope(
            desired, root_id=root_id,
            child_ids=set(desired.get("child_ownership", {})),
        )
        return desired, existing[0], True

    if repository["slug"] not in {requested, resolved}:
        raise RepositoryIdentityError("repository_redirect_multi_hop_requires_review")
    if repository["slug"] == resolved and requested not in repository.get("aliases", []):
        # A missing single alias can be repaired, but never by hopping from a
        # different historical coordinate.
        pass
    aliases = list(repository.get("aliases", []))
    if requested != resolved and requested not in aliases:
        aliases.append(requested)
    update = {
        "event_id": _identity_event_id(key, requested, resolved),
        "from": requested,
        "to": resolved,
        "repository_key": key,
        "provider_repository_id": resolution["provider_repository_id"],
        "observed_at": resolution["observed_at"],
        "effective_at": resolution["observed_at"],
        "evidence": [_provider_evidence(resolution)],
    }
    repository["slug"] = resolved
    repository["aliases"] = aliases
    repository["identity_updates"] = [*repository.get("identity_updates", []), update]
    repository["identity_resolution"] = {
        "provider_repository_id": resolution["provider_repository_id"],
        "resolved_slug": resolved,
        "observed_at": resolution["observed_at"],
        "evidence": [{
            "kind": "authenticated_provider_readback",
            "authenticated": True,
            "provider_repository_id": resolution["provider_repository_id"],
            "resolved_slug": resolved,
        }],
    }
    validate_scope(
        desired, root_id=root_id,
        child_ids=set(desired.get("child_ownership", {})),
    )
    return desired, update, False


def reconcile_repository_identity(
    adapter: LinearProjectionAdapter, *, resolution: dict[str, Any],
    expected_material_revision: int, expected_projection_revision: int,
    expected_scope_event_id: str, expected_scope_sha256: str,
) -> dict[str, Any]:
    """Append exactly one scope replacement, or return an exact no-op replay."""
    for name, value in (
        ("material", expected_material_revision),
        ("projection", expected_projection_revision),
    ):
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RepositoryIdentityError(f"invalid_repository_{name}_frontier")
    if not re.fullmatch(r"wsp_[0-9a-f]{32}", str(expected_scope_event_id)):
        raise RepositoryIdentityError("invalid_repository_scope_event_frontier")
    if not re.fullmatch(r"[0-9a-f]{64}", str(expected_scope_sha256)):
        raise RepositoryIdentityError("invalid_repository_scope_digest_frontier")
    required_resolution = {
        "provider", "provider_repository_id", "repository_key", "requested_slug",
        "resolved_slug", "observed_at", "redirect_count", "requested_response_url",
        "canonical_response_url", "authenticated",
    }
    if set(resolution) != required_resolution or resolution.get("authenticated") is not True:
        raise RepositoryIdentityError("invalid_repository_resolution")
    if resolution.get("redirect_count") != 1:
        raise RepositoryIdentityError("repository_redirect_not_single_hop")
    if canonical_repository(resolution["requested_slug"]) != resolution["requested_slug"] \
            or canonical_repository(resolution["resolved_slug"]) != resolution["resolved_slug"]:
        raise RepositoryIdentityError("repository_resolution_not_canonical")

    before_comments = adapter._comments()
    material = reduce_event_comments(before_comments, workstream_id=adapter.workstream_id)
    state = reduce_projection_comments(
        before_comments, workstream_id=adapter.workstream_id,
        expected_plan_revision=adapter.plan_revision,
        authenticated_route=adapter.authority,
    )
    head = _scope_head(state)
    desired, update, replay = _updated_scope(
        head["value"], resolution, root_id=adapter.workstream_id,
    )
    if replay:
        return {
            "disposition": "existing", "write_count": 0,
            "projection_revision": state.revision,
            "scope_event_id": head["event_id"], "identity_update": update,
        }
    if material.revision != expected_material_revision:
        raise RepositoryIdentityError("repository_material_frontier_stale_reload_required")
    if state.revision != expected_projection_revision:
        raise RepositoryIdentityError("repository_projection_frontier_stale_reload_required")
    if head["event_id"] != expected_scope_event_id:
        raise RepositoryIdentityError("repository_scope_event_stale_reload_required")
    if _value_digest(head["value"]) != expected_scope_sha256:
        raise RepositoryIdentityError("repository_scope_digest_stale_reload_required")

    # Re-read the complete shared comment stream immediately before mutation.
    # Any material/projection change already visible here refuses with zero writes.
    fenced_comments = adapter._comments()
    fenced_material = reduce_event_comments(
        fenced_comments, workstream_id=adapter.workstream_id,
    )
    fenced_state = reduce_projection_comments(
        fenced_comments, workstream_id=adapter.workstream_id,
        expected_plan_revision=adapter.plan_revision,
        authenticated_route=adapter.authority,
    )
    fenced_head = _scope_head(fenced_state)
    if fenced_material.revision != expected_material_revision:
        raise RepositoryIdentityError("repository_material_frontier_stale_reload_required")
    if fenced_state.revision != expected_projection_revision:
        raise RepositoryIdentityError("repository_projection_frontier_stale_reload_required")
    if fenced_head["event_id"] != expected_scope_event_id \
            or _value_digest(fenced_head["value"]) != expected_scope_sha256:
        raise RepositoryIdentityError("repository_scope_frontier_stale_reload_required")

    event = build_projection_event(
        workstream_id=adapter.workstream_id, kind="scope", key="root",
        value=desired, plan_revision=adapter.plan_revision,
        expected_revision=expected_projection_revision,
        created_at=resolution["observed_at"],
        supersedes_event_id=expected_scope_event_id, authority=adapter.authority,
    )
    _reserve_material_frontier(
        adapter, comments=fenced_comments,
        material_revision=expected_material_revision, intent_event=event,
    )
    try:
        receipt = adapter.append(
            event, expected_material_revision=expected_material_revision,
        )
    except (OSError, TimeoutError, LinearTransportError) as error:
        client = adapter.client
        known_loser = str(error).startswith((
            "projection_slot_lost_reload_required",
            "projection_concurrent_conflict",
        ))
        if not known_loser and (
            getattr(client, "mutation_transport_unknown", False)
            or getattr(client, "mutation_acknowledged", False)
        ):
            return {
                "disposition": "landed_unconfirmed",
                "reconcile_required": True,
                "write_count": "unknown",
                "expected_projection_revision": expected_projection_revision + 1,
                "scope_event_id": event["event_id"],
                "identity_update": update,
            }
        raise
    try:
        after = adapter.state()
    except (OSError, TimeoutError, LinearTransportError):
        return {
            "disposition": "landed_unconfirmed",
            "reconcile_required": True,
            "write_count": "unknown",
            "expected_projection_revision": expected_projection_revision + 1,
            "scope_event_id": event["event_id"],
            "identity_update": update,
        }
    applied = next(
        (item for item in after.events if item["event_id"] == event["event_id"]),
        None,
    )
    if applied != event:
        return {
            "disposition": "landed_unconfirmed",
            "reconcile_required": True,
            "write_count": "unknown",
            "expected_projection_revision": expected_projection_revision + 1,
            "scope_event_id": event["event_id"],
            "identity_update": update,
        }
    applied_repository = next(
        repo for repo in applied["value"]["repositories"]
        if repository_key(repo) == resolution["repository_key"]
    )
    matching = _matching_update(applied_repository, resolution)
    if len(matching) != 1:
        return {
            "disposition": "landed_unconfirmed",
            "reconcile_required": True,
            "write_count": "unknown",
            "expected_projection_revision": expected_projection_revision + 1,
            "scope_event_id": event["event_id"],
            "identity_update": update,
        }
    after_head = _scope_head(after)
    if after_head["event_id"] != event["event_id"]:
        return {
            "disposition": "created_superseded", "write_count": 1,
            "projection_revision": after.revision,
            "scope_event_id": event["event_id"],
            "superseded_by_scope_event_id": after_head["event_id"],
            "identity_update": update, "receipt": receipt,
        }
    return {
        "disposition": "created", "write_count": 1,
        "projection_revision": after.revision,
        "scope_event_id": event["event_id"], "identity_update": update,
        "receipt": receipt,
    }


def validate_request(request: dict[str, Any]) -> None:
    if not isinstance(request, dict) or set(request) != REQUEST_FIELDS:
        raise RepositoryIdentityError("invalid_repository_identity_request_fields")
    if (
        not isinstance(request.get("schema_version"), int)
        or isinstance(request.get("schema_version"), bool)
        or request["schema_version"] != 1
    ):
        raise RepositoryIdentityError("unsupported_repository_identity_request")
    if (
        not isinstance(request.get("workstream_id"), str)
        or len(request["workstream_id"].encode("utf-8")) > MAX_WORKSTREAM_ID_BYTES
        or not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", request["workstream_id"])
    ):
        raise RepositoryIdentityError("invalid_repository_identity_workstream")
    if not re.fullmatch(r"[0-9a-f]{64}", str(request.get("plan_revision", ""))):
        raise RepositoryIdentityError("invalid_repository_identity_plan_revision")
    authority = request.get("authority")
    if not isinstance(authority, dict) or set(authority) != AUTHORITY_FIELDS:
        raise RepositoryIdentityError("invalid_repository_identity_authority")
    if not all(
        isinstance(authority[field], str)
        and bool(authority[field])
        and authority[field].strip() == authority[field]
        and len(authority[field].encode("utf-8")) <= MAX_AUTHORITY_ID_BYTES
        for field in AUTHORITY_FIELDS
    ) or UUID.fullmatch(authority["root_issue_id"]) is None:
        raise RepositoryIdentityError("invalid_repository_identity_authority")
    repository = request.get("repository")
    if not isinstance(repository, dict) or set(repository) != {
        "requested_slug", "provider_repository_id",
    }:
        raise RepositoryIdentityError("invalid_repository_identity_target")
    try:
        requested_slug = canonical_repository(repository.get("requested_slug"))
    except (ScopeError, TypeError) as error:
        raise RepositoryIdentityError("invalid_repository_identity_target") from error
    if requested_slug != repository.get("requested_slug") or not isinstance(
        repository.get("provider_repository_id"), str
    ) or not repository["provider_repository_id"] or any(
        len(value.encode("utf-8")) > MAX_REPOSITORY_ID_BYTES
        for value in (requested_slug, repository["provider_repository_id"])
    ):
        raise RepositoryIdentityError("invalid_repository_identity_target")
    frontier = request.get("expected_frontier")
    required_frontier = {
        "material_revision", "projection_revision", "scope_event_id", "scope_sha256",
    }
    if not isinstance(frontier, dict) or set(frontier) != required_frontier:
        raise RepositoryIdentityError("invalid_repository_identity_frontier")
    for field in ("material_revision", "projection_revision"):
        value = frontier[field]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RepositoryIdentityError(
                f"invalid_repository_{field.removesuffix('_revision')}_frontier"
            )
    if not re.fullmatch(r"wsp_[0-9a-f]{32}", str(frontier["scope_event_id"])):
        raise RepositoryIdentityError("invalid_repository_scope_event_frontier")
    if not re.fullmatch(r"[0-9a-f]{64}", str(frontier["scope_sha256"])):
        raise RepositoryIdentityError("invalid_repository_scope_digest_frontier")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        with Path(args.request).open("rb") as request_file:
            raw = request_file.read(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES:
            raise RepositoryIdentityError("repository_identity_request_too_large")
        request = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
        )
        validate_request(request)
        if not args.apply:
            raise RepositoryIdentityError("repository_identity_apply_required")
        linear_token = load_linear_api_key()
        if not linear_token:
            raise RepositoryIdentityError("linear_auth_unavailable")
        authority = request["authority"]
        client = _MutationTrackingClient(HttpGraphQLClient(
            linear_token, LINEAR_OFFICIAL_ENDPOINT,
        ))
        adapter = LinearProjectionAdapter(
            client,
            issue_id=authority["root_issue_id"],
            workstream_id=request["workstream_id"],
            plan_revision=request["plan_revision"],
            workspace_id=authority["workspace_id"], team_id=authority["team_id"],
            project_id=authority["project_id"], root_issue_id=authority["root_issue_id"],
        )
        resolution = _recover_pending_resolution(adapter, request)
        if resolution is None:
            github_token = os.environ.get("GITHUB_TOKEN", "")
            resolution = GitHubRepositoryResolver(github_token).resolve(
                **request["repository"],
            )
        result = reconcile_repository_identity(
            adapter, resolution=resolution,
            expected_material_revision=request["expected_frontier"]["material_revision"],
            expected_projection_revision=request["expected_frontier"]["projection_revision"],
            expected_scope_event_id=request["expected_frontier"]["scope_event_id"],
            expected_scope_sha256=request["expected_frontier"]["scope_sha256"],
        )
        json.dump(result, sys.stdout, sort_keys=True, indent=2)
        sys.stdout.write("\n")
        return 3 if result.get("reconcile_required") is True else 0
    except (
        OSError, RecursionError, json.JSONDecodeError,
        LinearProjectionError, LinearTransportError,
        RepositoryIdentityError, ScopeError, ValueError,
    ) as error:
        print(f"repository identity update refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
