#!/usr/bin/env python3
"""Preview or append a fenced seal for authenticated legacy identity history."""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any

from workstream_config import load_linear_api_key, unique_object
from workstream_linear import HttpGraphQLClient, LinearTransportError
from workstream_linear_events import (
    COMMENT_CREATE_MUTATION, LinearEventError, reduce_event_comments,
    reduce_ledger_reservations,
)
from workstream_linear_projection import (
    decode_projection_receipt, _inspect_unsealed_identity_history,
    build_projection_event, encode_projection_comment,
    inspect_unsealed_identity_history, LinearProjectionAdapter,
    LinearProjectionError, PROJECTION_PREFIX, PROJECTION_RE,
    projection_slot_id, reduce_projection_comments,
)
from workstream_plan import plan_payload
from workstream_repository_identity import (
    _canonical, _reserve_material_frontier, _value_digest,
    GitHubRepositoryResolver, LINEAR_OFFICIAL_ENDPOINT, RepositoryIdentityError,
)
from workstream_scope import repository_key, UUID


MAX_REQUEST_BYTES = 64 * 1024
REQUEST_FIELDS = {
    "schema_version", "workstream_id", "authority", "plan_revision",
    "plan_source", "observed_at", "expected_frontier",
}
AUTHORITY_FIELDS = {"workspace_id", "team_id", "project_id", "root_issue_id"}
CANDIDATE_FIELDS = {
    "sealed_scope_event_id", "sealed_scope_value_sha256",
    "legacy_transitions", "sealed_projection_frontier_event_id",
    "sealed_projection_frontier_event_sha256",
    "legacy_projection_prefix_sha256",
}


class IdentityHistorySealError(RuntimeError):
    pass


def validate_request(request: dict[str, Any]) -> None:
    if not isinstance(request, dict) or set(request) != REQUEST_FIELDS:
        raise IdentityHistorySealError("invalid_identity_history_seal_request_fields")
    if request.get("schema_version") != 1 or isinstance(request.get("schema_version"), bool):
        raise IdentityHistorySealError("unsupported_identity_history_seal_request")
    if not re.fullmatch(r"[A-Z][A-Z0-9]*-\d+", str(request.get("workstream_id", ""))):
        raise IdentityHistorySealError("invalid_identity_history_seal_workstream")
    if not re.fullmatch(r"[0-9a-f]{64}", str(request.get("plan_revision", ""))):
        raise IdentityHistorySealError("invalid_identity_history_seal_plan")
    if not isinstance(request.get("plan_source"), str) or not request["plan_source"]:
        raise IdentityHistorySealError("invalid_identity_history_seal_plan_source")
    try:
        observed = datetime.fromisoformat(
            str(request.get("observed_at", "")).replace("Z", "+00:00")
        )
    except ValueError as error:
        raise IdentityHistorySealError(
            "invalid_identity_history_seal_timestamp"
        ) from error
    if observed.tzinfo is None:
        raise IdentityHistorySealError("invalid_identity_history_seal_timestamp")
    authority = request.get("authority")
    if (
        not isinstance(authority, dict)
        or set(authority) != AUTHORITY_FIELDS
        or not all(isinstance(authority[field], str) and authority[field] for field in AUTHORITY_FIELDS)
        or UUID.fullmatch(authority["root_issue_id"]) is None
    ):
        raise IdentityHistorySealError("invalid_identity_history_seal_authority")
    frontier = request.get("expected_frontier")
    if not isinstance(frontier, dict) or set(frontier) != {
        "material_revision", "projection_revision", *CANDIDATE_FIELDS,
    }:
        raise IdentityHistorySealError("invalid_identity_history_seal_frontier")
    for field in ("material_revision", "projection_revision"):
        if not isinstance(frontier[field], int) or isinstance(frontier[field], bool) or frontier[field] < 0:
            raise IdentityHistorySealError("invalid_identity_history_seal_frontier")
    for field in (
        "sealed_scope_event_id",
        "sealed_projection_frontier_event_id",
    ):
        if not re.fullmatch(r"wsp_[0-9a-f]{32}", str(frontier.get(field, ""))):
            raise IdentityHistorySealError("invalid_identity_history_seal_frontier")
    transitions = frontier.get("legacy_transitions")
    if (
        not isinstance(transitions, list) or not transitions
        or not all(
            isinstance(item, dict)
            and set(item) == {
                "predecessor_scope_event_id", "predecessor_scope_value_sha256",
                "transition_scope_event_id", "transition_scope_value_sha256",
            }
            and all(re.fullmatch(r"wsp_[0-9a-f]{32}", str(item.get(field, "")))
                    for field in ("predecessor_scope_event_id", "transition_scope_event_id"))
            and all(re.fullmatch(r"[0-9a-f]{64}", str(item.get(field, "")))
                    for field in ("predecessor_scope_value_sha256", "transition_scope_value_sha256"))
            for item in transitions
        )
        or len({item["transition_scope_event_id"] for item in transitions})
        != len(transitions)
    ):
        raise IdentityHistorySealError("invalid_identity_history_seal_frontier")
    for field in CANDIDATE_FIELDS - {
        "sealed_scope_event_id", "sealed_projection_frontier_event_id",
        "legacy_transitions",
    }:
        if not re.fullmatch(r"[0-9a-f]{64}", str(frontier.get(field, ""))):
            raise IdentityHistorySealError("invalid_identity_history_seal_frontier")


def _exact_candidate(inspection: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    candidate = inspection.get("candidate")
    if candidate != {field: expected[field] for field in CANDIDATE_FIELDS}:
        raise IdentityHistorySealError("identity_history_seal_candidate_mismatch")
    return candidate


def _scope_event(comments: list[dict[str, Any]], event_id: str) -> dict[str, Any]:
    matches = []
    for comment in comments:
        body = comment.get("body") or ""
        if PROJECTION_PREFIX not in body:
            continue
        encoded = PROJECTION_RE.findall(body)
        if len(encoded) != 1:
            continue
        event = decode_projection_receipt(comment, encoded[0])
        if event["kind"] == "scope" and event["key"] == "root" and event["event_id"] == event_id:
            matches.append(event)
    if len(matches) != 1:
        raise IdentityHistorySealError("identity_history_seal_scope_missing")
    return matches[0]


def _provider_proofs(scope: dict[str, Any], resolver: GitHubRepositoryResolver) -> list[dict[str, Any]]:
    proofs: list[dict[str, Any]] = []
    for repository in scope.get("repositories", []):
        key = repository_key(repository)
        provider_id = repository.get("provider_repository_id")
        if not isinstance(provider_id, str) or not provider_id:
            raise IdentityHistorySealError("identity_history_seal_provider_id_missing")
        routes = [
            resolver.resolve_route(
                requested_slug=route, provider_repository_id=provider_id,
                canonical_slug=repository["slug"],
            )
            for route in sorted([repository["slug"], *repository.get("aliases", [])])
        ]
        proofs.append({
            "repository_key": key,
            "provider_repository_id": provider_id,
            "canonical_slug": repository["slug"],
            "routes": routes,
        })
    return sorted(proofs, key=lambda item: item["repository_key"])


def _seal_event(
    adapter: LinearProjectionAdapter, candidate: dict[str, Any],
    scope_event: dict[str, Any], proofs: list[dict[str, Any]], observed_at: str,
    projection_revision: int,
    source: dict[str, Any], material_revision: int,
) -> dict[str, Any]:
    value = {
        "schema_version": 1,
        "root_issue_id": adapter.authority["root_issue_id"],
        "plan_revision": adapter.plan_revision,
        "source_identity": source["identity"],
        "source_sha256": source["sha256"],
        "expected_material_revision": material_revision,
        "expected_projection_revision": projection_revision,
        **candidate,
        "repositories": proofs,
        "repositories_sha256": hashlib.sha256(_canonical(proofs)).hexdigest(),
        "observed_at": observed_at,
    }
    if value["sealed_scope_value_sha256"] != _value_digest(scope_event["value"]):
        raise IdentityHistorySealError("identity_history_seal_scope_digest_mismatch")
    return build_projection_event(
        workstream_id=adapter.workstream_id, kind="identity_history_seal",
        key=candidate["sealed_scope_event_id"], value=value,
        plan_revision=adapter.plan_revision, expected_revision=projection_revision,
        created_at=observed_at, authority=adapter.authority,
    )


def _stored_intent(
    comments: list[dict[str, Any]], adapter: LinearProjectionAdapter,
    expected: dict[str, Any],
) -> dict[str, Any] | None:
    matches = []
    for reservation, _remote_id in reduce_ledger_reservations(
        comments, workstream_id=adapter.workstream_id,
    ):
        event = reservation.get("intent_event") or {}
        value = event.get("value") or {}
        if (
            event.get("kind") == "identity_history_seal"
            and event.get("authority") == adapter.authority
            and event.get("plan_revision") == adapter.plan_revision
            and all(value.get(field) == expected.get(field) for field in CANDIDATE_FIELDS)
            and value.get("expected_material_revision")
            == expected.get("material_revision")
            and value.get("expected_projection_revision")
            == expected.get("projection_revision")
        ):
            matches.append(event)
    if len(matches) > 1:
        raise IdentityHistorySealError("identity_history_seal_intent_ambiguous")
    return matches[0] if matches else None


def run(request: dict[str, Any], *, apply: bool) -> dict[str, Any]:
    validate_request(request)
    token = load_linear_api_key()
    if not token:
        raise IdentityHistorySealError("linear_auth_unavailable")
    authority = request["authority"]
    adapter = LinearProjectionAdapter(
        HttpGraphQLClient(token, LINEAR_OFFICIAL_ENDPOINT),
        issue_id=authority["root_issue_id"], workstream_id=request["workstream_id"],
        plan_revision=request["plan_revision"], **authority,
    )
    comments = adapter._comments()
    expected = request["expected_frontier"]
    material = reduce_event_comments(comments, workstream_id=adapter.workstream_id)

    try:
        strict = reduce_projection_comments(
            comments, workstream_id=adapter.workstream_id,
            expected_plan_revision=adapter.plan_revision,
            authenticated_route=adapter.authority,
        )
    except LinearProjectionError:
        strict = None
    if strict is not None:
        existing = [
            item for item in strict.events
            if item["kind"] == "identity_history_seal"
            and all(
                item["value"].get(field) == expected[field]
                for field in CANDIDATE_FIELDS
            )
        ]
        if len(existing) == 1:
            return {
                "disposition": "existing", "reconcile_required": False,
                "event_id": existing[0]["event_id"],
                "remote_id": strict.remote_ids[existing[0]["event_id"]],
                "projection_revision": strict.revision,
            }
        raise IdentityHistorySealError("identity_history_seal_existing_conflict")

    if material.revision != expected["material_revision"]:
        raise IdentityHistorySealError("identity_history_seal_material_frontier_stale")

    # Crash replay reads the durable full intent before any provider/source call.
    stored = _stored_intent(comments, adapter, expected)
    if stored is not None:
        event = stored
        # A shaped reservation is not replay authority. Prove that it owns the
        # exact deterministic shared-boundary slot and collision/frontier chain.
        _reserve_material_frontier(
            adapter, comments=comments,
            material_revision=expected["material_revision"], intent_event=event,
            permit_unsealed_legacy_candidates=True,
        )
        scope_event = _scope_event(comments, expected["sealed_scope_event_id"])
        if _value_digest(scope_event["value"]) != expected["sealed_scope_value_sha256"]:
            raise IdentityHistorySealError("identity_history_seal_scope_digest_mismatch")
    else:
        source = plan_payload(request["plan_source"], request["plan_source"])["source"]
        if source["sha256"] != adapter.plan_revision:
            raise IdentityHistorySealError("identity_history_seal_plan_digest_mismatch")
        inspection = inspect_unsealed_identity_history(
            comments, workstream_id=adapter.workstream_id,
            expected_plan_revision=adapter.plan_revision,
            authenticated_route=adapter.authority, authenticated_source=source,
            material_revision=material.revision,
        )
        candidate = _exact_candidate(inspection, expected)
        if inspection["projection_revision"] != expected["projection_revision"]:
            raise IdentityHistorySealError("identity_history_seal_projection_frontier_stale")
        scope_event = _scope_event(comments, candidate["sealed_scope_event_id"])
        github_token = os.environ.get("GITHUB_TOKEN", "")
        resolver = GitHubRepositoryResolver(github_token)
        proofs = _provider_proofs(scope_event["value"], resolver)
        event = _seal_event(
            adapter, candidate, scope_event, proofs, request["observed_at"],
            inspection["projection_revision"], source, material.revision,
        )
    preview = {
        "disposition": "preview", "reconcile_required": True,
        "event_id": event["event_id"], "sealed_scope_event_id": event["key"],
        "projection_revision": event["expected_revision"],
        "event": event,
    }
    if not apply:
        return preview

    fenced_comments = adapter._comments()
    fenced_material = reduce_event_comments(
        fenced_comments, workstream_id=adapter.workstream_id,
    )
    if fenced_material.revision != expected["material_revision"]:
        raise IdentityHistorySealError("identity_history_seal_frontier_stale")
    fenced_inspection = (
        _inspect_unsealed_identity_history if stored is not None
        else inspect_unsealed_identity_history
    )(
        fenced_comments, workstream_id=adapter.workstream_id,
        expected_plan_revision=adapter.plan_revision,
        authenticated_route=adapter.authority,
        authenticated_source=None if stored is not None else source,
        material_revision=fenced_material.revision,
    )
    _exact_candidate(fenced_inspection, expected)
    if fenced_inspection["projection_revision"] != event["expected_revision"]:
        raise IdentityHistorySealError("identity_history_seal_frontier_stale")
    _reserve_material_frontier(
        adapter, comments=fenced_comments,
        material_revision=expected["material_revision"], intent_event=event,
        permit_unsealed_legacy_candidates=True,
    )
    slot = projection_slot_id(
        adapter.workstream_id, adapter.plan_revision,
        event["expected_revision"], adapter.authority,
    )
    body = encode_projection_comment(event)
    adapter._assert_comment_id_capability()
    try:
        response = adapter.client.execute(COMMENT_CREATE_MUTATION, {"input": {
            "id": slot, "issueId": adapter.issue_id, "body": body,
        }})
        created = response.get("commentCreate") or {}
        if created.get("success") is not True:
            raise IdentityHistorySealError("identity_history_seal_write_unconfirmed")
    except (OSError, TimeoutError, LinearTransportError):
        pass
    after_comments = adapter._comments()
    after = reduce_projection_comments(
        after_comments, workstream_id=adapter.workstream_id,
        expected_plan_revision=adapter.plan_revision,
        authenticated_route=adapter.authority,
    )
    applied = [item for item in after.events if item["event_id"] == event["event_id"]]
    if len(applied) != 1 or applied[0] != event or after.remote_ids[event["event_id"]] != slot:
        raise IdentityHistorySealError("identity_history_seal_landed_unconfirmed")
    return {
        "disposition": "created", "reconcile_required": False,
        "event_id": event["event_id"], "remote_id": slot,
        "projection_revision": after.revision,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        with Path(args.request).open("rb") as handle:
            raw = handle.read(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES:
            raise IdentityHistorySealError("identity_history_seal_request_too_large")
        request = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object)
        result = run(request, apply=args.apply)
        json.dump(result, sys.stdout, sort_keys=True, indent=2)
        sys.stdout.write("\n")
        return 0 if args.apply else 3
    except (
        OSError, json.JSONDecodeError, LinearEventError, LinearProjectionError,
        LinearTransportError,
        RepositoryIdentityError, IdentityHistorySealError, ValueError,
    ) as error:
        print(f"repository identity seal refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
