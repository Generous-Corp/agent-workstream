#!/usr/bin/env python3
"""Transport-neutral repository scope and cross-workstream relation contract."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlparse
from datetime import datetime


TOKEN = re.compile(r"^[A-Z][A-Z0-9]*-\d+$")
REPOSITORY = re.compile(r"^[A-Za-z0-9.-]+/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
NAMESPACE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,63}$")
HEAD = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
RELATION_TYPES = {"blocks", "blocked_by", "related"}
UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$")


class ScopeError(ValueError):
    pass


def is_issue_token(value: Any) -> bool:
    return isinstance(value, str) and TOKEN.fullmatch(value) is not None


def is_full_oid(value: Any) -> bool:
    return isinstance(value, str) and HEAD.fullmatch(value) is not None


def is_namespace(value: Any) -> bool:
    return isinstance(value, str) and NAMESPACE.fullmatch(value) is not None


def canonical_repository(value: str) -> str:
    """Normalize common Git remotes to host/owner/repository identity."""
    if not isinstance(value, str) or not value.strip():
        raise ScopeError("invalid_repository_slug")
    raw = value.strip()
    scp = re.fullmatch(r"(?:[^@/]+@)?([^:/]+):([^/]+)/(.+)", raw)
    if scp and "://" not in raw:
        host, owner, repository = scp.groups()
    elif "://" in raw:
        parsed = urlparse(raw)
        parts = [part for part in parsed.path.split("/") if part]
        if parsed.scheme not in {"http", "https", "ssh", "git"} or not parsed.hostname or len(parts) != 2:
            raise ScopeError("invalid_repository_slug")
        host, owner, repository = parsed.hostname, parts[0], parts[1]
    else:
        parts = raw.split("/")
        if len(parts) != 3:
            raise ScopeError("invalid_repository_slug")
        host, owner, repository = parts
    repository = repository[:-4] if repository.endswith(".git") else repository
    host = host.lower()
    if not all((host, owner, repository)):
        raise ScopeError("invalid_repository_slug")
    if host == "github.com":
        owner, repository = owner.lower(), repository.lower()
    result = f"{host}/{owner}/{repository}"
    if not REPOSITORY.fullmatch(result):
        raise ScopeError("invalid_repository_slug")
    return result


def _observed_at(value: Any, error: str) -> datetime:
    try:
        observed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError as exc:
        raise ScopeError(error) from exc
    if observed.tzinfo is None:
        raise ScopeError(error)
    return observed


def _repository_resolution(repository: dict[str, Any]) -> dict[str, Any]:
    slug = repository["slug"]
    provider_id = repository.get("provider_repository_id")
    resolution = repository.get("identity_resolution")
    if not isinstance(resolution, dict):
        raise ScopeError(f"repository_equivalence_unproven:{slug}")
    if resolution.get("provider_repository_id") != provider_id:
        raise ScopeError(f"provider_repository_id_mismatch:{slug}")
    if resolution.get("resolved_slug") != slug:
        raise ScopeError(f"stale_repository_resolution:{slug}")
    _observed_at(
        resolution.get("observed_at"),
        f"invalid_repository_resolution_timestamp:{slug}",
    )
    evidence = resolution.get("evidence")
    if not isinstance(evidence, list) or not any(
        isinstance(item, dict)
        and item.get("kind") == "authenticated_provider_readback"
        and item.get("authenticated") is True
        and item.get("provider_repository_id") == provider_id
        and item.get("resolved_slug") == slug
        for item in evidence
    ):
        raise ScopeError(f"repository_equivalence_unproven:{slug}")
    return resolution


def repository_key(repository: dict[str, Any]) -> str:
    """Return immutable provider identity, or a verified coordinate fallback."""
    slug = repository.get("slug")
    if not isinstance(slug, str) or canonical_repository(slug) != slug:
        raise ScopeError("invalid_repository_slug")
    host = slug.split("/", 1)[0]
    provider_id = repository.get("provider_repository_id")
    _repository_resolution(repository)
    if provider_id is not None:
        if not isinstance(provider_id, str) or not provider_id.strip():
            raise ScopeError(f"invalid_provider_repository_id:{slug}")
        return f"{host}:id:{provider_id}"
    return f"{host}:coordinate:{slug}"


def _validate_identity_history(repository: dict[str, Any], key: str) -> list[str]:
    slug = repository["slug"]
    aliases = repository.get("aliases", [])
    if not isinstance(aliases, list):
        raise ScopeError(f"invalid_repository_aliases:{slug}")
    canonical_aliases: list[str] = []
    for alias in aliases:
        canonical = canonical_repository(alias)
        if canonical != alias:
            raise ScopeError(f"repository_alias_not_canonical:{alias}")
        if canonical == slug or canonical in canonical_aliases:
            raise ScopeError(f"duplicate_repository_alias:{canonical}")
        canonical_aliases.append(canonical)
    updates = repository.get("identity_updates", [])
    if not isinstance(updates, list):
        raise ScopeError(f"invalid_identity_updates:{slug}")
    covered: set[str] = set()
    for update in updates:
        if not isinstance(update, dict):
            raise ScopeError(f"invalid_identity_update:{slug}")
        previous = update.get("from")
        current = update.get("to")
        if previous not in canonical_aliases or current != slug:
            raise ScopeError(f"stale_alias_routing:{previous}")
        _observed_at(update.get("observed_at"), f"invalid_identity_update_timestamp:{slug}")
        evidence = update.get("evidence")
        if (
            update.get("repository_key") != key
            or update.get("provider_repository_id") != repository.get("provider_repository_id")
            or not isinstance(evidence, list)
            or not any(
                isinstance(item, dict)
                and item.get("kind") == "authenticated_provider_readback"
                and item.get("authenticated") is True
                and item.get("repository_key") == key
                and item.get("provider_repository_id") == repository.get("provider_repository_id")
                and item.get("requested_slug") == previous
                and item.get("resolved_slug") == slug
                for item in evidence
            )
        ):
            raise ScopeError(f"unverified_identity_update:{previous}")
        if previous in covered:
            raise ScopeError(f"duplicate_identity_update:{previous}")
        covered.add(previous)
    if covered != set(canonical_aliases):
        raise ScopeError(f"alias_without_identity_update:{slug}")
    return canonical_aliases


def _is_monotonic_resolution_refresh(
    previous: dict[str, Any], current: dict[str, Any], slug: str,
) -> bool:
    """Accept an authenticated readback refresh without rewriting its claims."""
    if not isinstance(previous, dict) or not isinstance(current, dict):
        return False
    if set(previous) != set(current):
        return False
    previous_claim = {key: value for key, value in previous.items() if key != "observed_at"}
    current_claim = {key: value for key, value in current.items() if key != "observed_at"}
    if previous_claim != current_claim:
        return False
    try:
        return _observed_at(
            current.get("observed_at"),
            f"invalid_repository_resolution_timestamp:{slug}",
        ) >= _observed_at(
            previous.get("observed_at"),
            f"invalid_repository_resolution_timestamp:{slug}",
        )
    except ScopeError:
        return False


def _is_legacy_identity_backfill(
    previous: dict[str, Any], current: dict[str, Any], key: str,
    appended_aliases: list[str], appended_updates: list[dict[str, Any]],
) -> bool:
    """Recognize the bounded pre-v2 redirect receipt already stored in Linear."""
    if len(appended_aliases) != 1 or len(appended_updates) != 1:
        return False
    update = appended_updates[0]
    if not isinstance(update, dict) or set(update) != {
        "from", "to", "repository_key", "provider_repository_id",
        "observed_at", "evidence",
    }:
        return False
    slug = current.get("slug")
    provider_id = current.get("provider_repository_id")
    previous_slug = appended_aliases[0]
    evidence = update.get("evidence")
    expected_evidence = [{
        "kind": "authenticated_provider_readback",
        "authenticated": True,
        "repository_key": key,
        "provider_repository_id": provider_id,
        "requested_slug": previous_slug,
        "resolved_slug": slug,
    }]
    resolution = current.get("identity_resolution")
    previous_resolution = previous.get("identity_resolution")
    if (
        update.get("from") != previous_slug
        or update.get("to") != slug
        or update.get("repository_key") != key
        or update.get("provider_repository_id") != provider_id
        or update.get("evidence") != expected_evidence
        or not isinstance(resolution, dict)
        or set(resolution) != {
            "provider_repository_id", "resolved_slug", "observed_at", "evidence",
        }
        or resolution.get("provider_repository_id") != provider_id
        or resolution.get("resolved_slug") != slug
        or resolution.get("observed_at") != update.get("observed_at")
        or resolution.get("evidence") != [{
            "kind": "authenticated_provider_readback",
            "authenticated": True,
            "provider_repository_id": provider_id,
            "resolved_slug": slug,
        }]
        or not _is_monotonic_resolution_refresh(
            previous_resolution, resolution, str(slug),
        )
    ):
        return False
    return True


def _validate_repository_identity_transition(
    previous_scope: dict[str, Any], next_scope: dict[str, Any], *,
    authenticated_legacy_history: bool,
) -> None:
    """Prevent an ordinary scope replacement from rewriting identity history."""
    previous_repositories = previous_scope.get("repositories")
    next_repositories = next_scope.get("repositories")
    if not isinstance(previous_repositories, list) or not isinstance(next_repositories, list):
        raise ScopeError("repository_identity_transition_scope_invalid")
    next_by_key: dict[str, dict[str, Any]] = {}
    for repository in next_repositories:
        if not isinstance(repository, dict):
            raise ScopeError("repository_identity_transition_scope_invalid")
        key = repository_key(repository)
        if key in next_by_key:
            raise ScopeError(f"repository_identity_transition_ambiguous:{key}")
        next_by_key[key] = repository
    for previous in previous_repositories:
        if not isinstance(previous, dict):
            raise ScopeError("repository_identity_transition_scope_invalid")
        key = repository_key(previous)
        current = next_by_key.get(key)
        if current is None:
            if (
                previous.get("provider_repository_id") is not None
                or previous.get("aliases")
                or previous.get("identity_updates")
            ):
                raise ScopeError(
                    f"repository_identity_history_regressed:{key}:repository_removed"
                )
            continue
        previous_aliases = previous.get("aliases", [])
        current_aliases = current.get("aliases", [])
        previous_updates = previous.get("identity_updates", [])
        current_updates = current.get("identity_updates", [])
        previous_evidence = previous.get("evidence", [])
        current_evidence = current.get("evidence", [])
        _repository_resolution(current)
        _validate_identity_history(current, key)
        if (
            current.get("provider_repository_id")
            != previous.get("provider_repository_id")
            or not isinstance(previous_aliases, list)
            or not isinstance(current_aliases, list)
            or not all(isinstance(alias, str) for alias in previous_aliases)
            or not all(isinstance(alias, str) for alias in current_aliases)
            or current_aliases[:len(previous_aliases)] != previous_aliases
            or not isinstance(previous_updates, list)
            or not isinstance(current_updates, list)
            or current_updates[:len(previous_updates)] != previous_updates
            or not isinstance(previous_evidence, list)
            or not isinstance(current_evidence, list)
            or current_evidence[:len(previous_evidence)] != previous_evidence
        ):
            raise ScopeError(f"repository_identity_history_regressed:{key}")
        appended_updates = current_updates[len(previous_updates):]
        appended_aliases = current_aliases[len(previous_aliases):]
        if not appended_updates:
            if (
                current.get("slug") != previous.get("slug")
                or (
                    current.get("identity_resolution")
                    != previous.get("identity_resolution")
                    and not (
                        authenticated_legacy_history
                        and _is_monotonic_resolution_refresh(
                            previous.get("identity_resolution"),
                            current.get("identity_resolution"),
                            str(current.get("slug")),
                        )
                    )
                )
            ):
                raise ScopeError(f"repository_identity_history_regressed:{key}")
            continue
        if (
            authenticated_legacy_history
            and _is_legacy_identity_backfill(
                previous, current, key, appended_aliases, appended_updates,
            )
        ):
            continue
        latest = appended_updates[-1]
        resolution = current.get("identity_resolution")
        previous_slug = latest.get("from") if isinstance(latest, dict) else None
        current_slug = current.get("slug")
        identity_material = json.dumps(
            ["repository-identity-update-v1", key, previous_slug, current_slug],
            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        expected_event_id = "wsri_" + hashlib.sha256(identity_material).hexdigest()[:32]
        latest_evidence = (
            latest.get("evidence") if isinstance(latest, dict) else None
        )
        evidence_head = (
            latest_evidence[0]
            if isinstance(latest_evidence, list)
            and latest_evidence
            and isinstance(latest_evidence[0], dict)
            else {}
        )
        expected_update_evidence = [{
            "kind": "authenticated_provider_readback",
            "authenticated": True,
            "repository_key": key,
            "provider_repository_id": previous.get("provider_repository_id"),
            "requested_slug": previous_slug,
            "resolved_slug": current_slug,
            "redirect_count": 1,
            "requested_response_url": evidence_head.get("requested_response_url"),
            "canonical_response_url": evidence_head.get("canonical_response_url"),
        }]
        if (
            len(appended_updates) != 1
            or not isinstance(latest, dict)
            or not isinstance(resolution, dict)
            or set(latest) != {
                "event_id", "from", "to", "repository_key",
                "provider_repository_id", "observed_at", "effective_at", "evidence",
            }
            or latest.get("event_id") != expected_event_id
            or latest.get("provider_repository_id")
            != previous.get("provider_repository_id")
            or latest.get("repository_key") != key
            or latest.get("to") != current.get("slug")
            or latest.get("observed_at") != resolution.get("observed_at")
            or latest.get("effective_at") != latest.get("observed_at")
            or latest.get("evidence") != expected_update_evidence
            or not all(
                isinstance(expected_update_evidence[0].get(field), str)
                and expected_update_evidence[0][field]
                for field in ("requested_response_url", "canonical_response_url")
            )
            or resolution.get("provider_repository_id")
            != previous.get("provider_repository_id")
            or resolution.get("resolved_slug") != current.get("slug")
            or set(resolution) != {
                "provider_repository_id", "resolved_slug", "observed_at", "evidence",
            }
            or resolution.get("evidence") != [{
                "kind": "authenticated_provider_readback",
                "authenticated": True,
                "provider_repository_id": previous.get("provider_repository_id"),
                "resolved_slug": current_slug,
            }]
        ):
            raise ScopeError(f"repository_identity_history_regressed:{key}")


def validate_repository_identity_transition(
    previous_scope: dict[str, Any], next_scope: dict[str, Any],
) -> None:
    _validate_repository_identity_transition(
        previous_scope, next_scope, authenticated_legacy_history=False,
    )


def validate_authenticated_legacy_repository_identity_transition(
    previous_scope: dict[str, Any], next_scope: dict[str, Any],
) -> None:
    """Validate a legacy transition after its immutable receipt is authenticated."""
    _validate_repository_identity_transition(
        previous_scope, next_scope, authenticated_legacy_history=True,
    )


def validate_scope(scope: dict[str, Any], *, root_id: str,
                   child_ids: set[str] | None = None) -> None:
    if not is_issue_token(root_id):
        raise ScopeError("invalid_root_id")
    namespace = scope.get("namespace")
    if not is_namespace(namespace):
        raise ScopeError("invalid_namespace")
    linear = scope.get("linear")
    if not isinstance(linear, dict):
        raise ScopeError("linear_destination_required")
    for field in ("workspace_id", "team_id", "project_id"):
        if not isinstance(linear.get(field), str) or not linear[field].strip():
            raise ScopeError(f"linear_destination_missing:{field}")
    if not isinstance(linear.get("root_issue_id"), str) or not UUID.fullmatch(linear["root_issue_id"]):
        raise ScopeError("linear_destination_missing:root_issue_id")
    route = linear.get("route_verification")
    route_fields = ("workspace_id", "team_id", "project_id", "root_issue_id")
    if not isinstance(route, dict) or any(route.get(field) != linear[field] for field in route_fields):
        raise ScopeError("linear_route_readback_mismatch")
    _observed_at(route.get("observed_at"), "invalid_linear_route_timestamp")
    route_evidence = route.get("evidence")
    if not isinstance(route_evidence, list) or not any(
        isinstance(item, dict)
        and item.get("kind") == "authenticated_linear_readback"
        and item.get("authenticated") is True
        and all(item.get(field) == linear[field] for field in route_fields)
        for item in route_evidence
    ):
        raise ScopeError("linear_route_unverified")
    primary = scope.get("primary_repository")
    repositories = scope.get("repositories")
    if not isinstance(repositories, list) or not repositories:
        raise ScopeError("repositories_required")
    slugs: set[str] = set()
    keys: set[str] = set()
    routes: dict[str, str] = {}
    for repository in repositories:
        if not isinstance(repository, dict):
            raise ScopeError("invalid_repository")
        if "path" in repository or "worktree" in repository:
            raise ScopeError("local_path_is_not_repository_identity")
        slug = repository.get("slug")
        if not isinstance(slug, str) or not REPOSITORY.fullmatch(slug):
            raise ScopeError("invalid_repository_slug")
        if canonical_repository(slug) != slug:
            raise ScopeError(f"repository_not_canonical:{slug}")
        if slug in slugs:
            raise ScopeError(f"duplicate_repository:{slug}")
        slugs.add(slug)
        key = repository_key(repository)
        if key in keys:
            raise ScopeError(f"duplicate_repository_identity:{key}")
        keys.add(key)
        aliases = _validate_identity_history(repository, key)
        for route in [slug, *aliases]:
            existing = routes.get(route)
            if existing is not None and existing != key:
                raise ScopeError(f"repository_alias_collision:{route}")
            routes[route] = key
        head = repository.get("exact_head")
        if not is_full_oid(head):
            raise ScopeError(f"invalid_repository_head:{slug}")
        if not isinstance(repository.get("evidence", []), list):
            raise ScopeError(f"invalid_repository_evidence:{slug}")
    if primary not in keys:
        raise ScopeError("primary_repository_not_participating")
    ownership = scope.get("child_ownership")
    if not isinstance(ownership, dict):
        raise ScopeError("child_ownership_required")
    expected = child_ids or set()
    if set(ownership) != expected:
        missing = sorted(expected - set(ownership))
        unknown = sorted(set(ownership) - expected)
        if missing:
            raise ScopeError("unowned_children:" + ",".join(missing))
        raise ScopeError("unknown_owned_children:" + ",".join(unknown))
    for child_id, slug in ownership.items():
        if not is_issue_token(child_id):
            raise ScopeError(f"invalid_child_id:{child_id}")
        if slug not in keys:
            raise ScopeError(f"child_repository_not_participating:{child_id}")


def validate_relations(relations: list[dict[str, Any]], *, root_id: str,
                       workspace_id: str | None = None,
                       root_issue_id: str | None = None) -> None:
    if not isinstance(relations, list):
        raise ScopeError("relations_must_be_list")
    observed: set[tuple[str, str, str]] = set()
    for relation in relations:
        if not isinstance(relation, dict):
            raise ScopeError("invalid_relation")
        relation_type = relation.get("type")
        target = relation.get("target")
        if relation_type not in RELATION_TYPES:
            raise ScopeError(f"unknown_relation_type:{relation_type}")
        if not isinstance(target, dict):
            raise ScopeError("invalid_relation_target")
        target_workspace = target.get("workspace_id")
        target_issue = target.get("issue_id")
        target_identifier = target.get("identifier")
        if not isinstance(target_workspace, str) or not target_workspace.strip():
            raise ScopeError("invalid_relation_workspace")
        if not isinstance(target_issue, str) or not UUID.fullmatch(target_issue):
            raise ScopeError("invalid_relation_issue_id")
        if not is_issue_token(target_identifier):
            raise ScopeError("invalid_relation_identifier")
        if (
            workspace_id is not None and root_issue_id is not None
            and target_workspace == workspace_id and target_issue == root_issue_id
        ) or (
            (workspace_id is None or root_issue_id is None)
            and target_identifier == root_id
        ):
            raise ScopeError("self_relation")
        key = (relation_type, target_workspace, target_issue)
        if key in observed:
            raise ScopeError(f"duplicate_relation:{relation_type}:{target_workspace}:{target_issue}")
        observed.add(key)
    directed: dict[tuple[str, str], set[str]] = {}
    for relation_type, target_workspace, target_issue in observed:
        if relation_type in {"blocks", "blocked_by"}:
            directed.setdefault((target_workspace, target_issue), set()).add(relation_type)
    for (target_workspace, target_issue), relation_types in directed.items():
        if relation_types == {"blocks", "blocked_by"}:
            raise ScopeError(
                f"contradictory_relation:{target_workspace}:{target_issue}"
            )


def relation_target_key(target: dict[str, Any]) -> str:
    """Return the immutable lookup key for one already syntax-checked target."""
    return f"{target.get('workspace_id')}:{target.get('issue_id')}"


def validate_relation_graph(
    relations: list[dict[str, Any]], *, root_id: str, workspace_id: str,
    root_issue_id: str,
    resolve_target: Callable[[dict[str, Any]], dict[str, Any] | None]
    | Mapping[str, dict[str, Any]],
) -> None:
    """Validate target existence and directed inverse edges from live readback.

    ``validate_relations`` remains the transport-neutral syntax check.  This
    stronger boundary consumes authenticated target readback supplied by the
    caller; target display tokens are never treated as routing authority.
    """
    validate_relations(
        relations, root_id=root_id, workspace_id=workspace_id,
        root_issue_id=root_issue_id,
    )
    root_target = {
        "workspace_id": workspace_id,
        "issue_id": root_issue_id,
        "identifier": root_id,
    }
    for relation in relations:
        target = relation["target"]
        resolved = (
            resolve_target(target)
            if callable(resolve_target)
            else resolve_target.get(relation_target_key(target))
        )
        if not isinstance(resolved, dict):
            raise ScopeError(f"dangling_relation_target:{target['identifier']}")
        for field in ("workspace_id", "issue_id", "identifier"):
            if resolved.get(field) != target[field]:
                raise ScopeError(
                    f"relation_target_identity_mismatch:{target['identifier']}:{field}"
                )
        peer_relations = resolved.get("relations")
        if not isinstance(peer_relations, list):
            raise ScopeError(f"relation_target_readback_incomplete:{target['identifier']}")
        validate_relations(
            peer_relations, root_id=target["identifier"],
            workspace_id=target["workspace_id"], root_issue_id=target["issue_id"],
        )
        relation_type = relation["type"]
        if relation_type == "related":
            continue
        expected_inverse = "blocked_by" if relation_type == "blocks" else "blocks"
        peer_edges = [
            item["type"] for item in peer_relations
            if isinstance(item.get("target"), dict)
            and all(item["target"].get(field) == value
                    for field, value in root_target.items())
            and item.get("type") in {"blocks", "blocked_by"}
        ]
        if expected_inverse in peer_edges:
            continue
        if peer_edges:
            raise ScopeError(
                f"contradictory_relation_inverse:{relation_type}:{target['identifier']}"
            )
        raise ScopeError(
            f"missing_relation_inverse:{relation_type}:{target['identifier']}"
        )
