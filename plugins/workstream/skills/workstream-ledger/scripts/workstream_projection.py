#!/usr/bin/env python3
"""Idempotently reconcile the required append-only Linear resume projection."""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_linear import (
    HttpGraphQLClient, LinearGraphQLTransport, LinearTransportError,
    resolve_authenticated_issue_route,
)
from workstream_linear_events import LinearCommentEventAdapter
from workstream_linear_projection import (
    build_projection_event, LinearProjectionAdapter, LinearProjectionError, TOMBSTONE,
)
from workstream_plan import plan_payload
from workstream_resume import add_material_history, compact_context, extract_token, ResumeError
from workstream_successor import choose_disposition, SuccessorError


REQUIRED_KINDS = {"scope", "source", "provenance"}


def _value_digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _active_heads(state: Any) -> dict[tuple[str, str], dict[str, Any]]:
    active: dict[tuple[str, str], dict[str, Any]] = {}
    for event in state.events:
        identity = (event["kind"], event["key"])
        if event["value"] == TOMBSTONE:
            active.pop(identity, None)
        else:
            active[identity] = event
    return active


def _latest_heads(state: Any) -> dict[tuple[str, str], dict[str, Any]]:
    """Return the latest event for every key, including tombstone heads."""
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for event in state.events:
        latest[(event["kind"], event["key"])] = event
    return latest


def projection_review_contract(state: Any) -> dict[str, Any]:
    """Return the exact remote projection surface a manifest must review."""
    return _contract_from_heads(state.revision, _active_heads(state))


def _contract_from_heads(
    revision: int, active: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    return {
        "expected_projection_revision": revision,
        "expected_active_heads": [
            {
                "kind": kind,
                "key": key,
                "event_id": event["event_id"],
                "value_sha256": _value_digest(event["value"]),
            }
            for (kind, key), event in sorted(active.items())
        ],
    }


def _reviewed_manifest(manifest: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    required = {
        "projection", "retirements", "expected_projection_revision",
        "expected_active_heads",
    }
    if not isinstance(manifest, dict) or set(manifest) != required:
        raise LinearProjectionError("manifest_review_contract_required")
    revision = manifest["expected_projection_revision"]
    if not isinstance(revision, int) or revision < 0:
        raise LinearProjectionError("invalid_manifest_projection_revision")
    heads = manifest["expected_active_heads"]
    if not isinstance(heads, list):
        raise LinearProjectionError("manifest_active_heads_must_be_list")
    identities: set[tuple[str, str]] = set()
    for index, head in enumerate(heads):
        if not isinstance(head, dict) or set(head) != {
            "kind", "key", "event_id", "value_sha256",
        }:
            raise LinearProjectionError(f"invalid_manifest_active_head:{index}")
        identity = (head.get("kind"), head.get("key"))
        if not all(isinstance(value, str) and value for value in identity):
            raise LinearProjectionError(f"invalid_manifest_active_head_identity:{index}")
        if identity in identities:
            raise LinearProjectionError(
                f"duplicate_manifest_active_head:{identity[0]}:{identity[1]}"
            )
        if not isinstance(head.get("event_id"), str) or not head["event_id"]:
            raise LinearProjectionError(f"invalid_manifest_active_head_event:{index}")
        if not re.fullmatch(r"[0-9a-f]{64}", str(head.get("value_sha256", ""))):
            raise LinearProjectionError(f"invalid_manifest_active_head_digest:{index}")
        identities.add(identity)
    retirements = manifest["retirements"]
    if not isinstance(retirements, list):
        raise LinearProjectionError("manifest_retirements_must_be_list")
    retired: set[tuple[str, str]] = set()
    for index, retirement in enumerate(retirements):
        if not isinstance(retirement, dict) or set(retirement) != {
            "kind", "key", "expected_event_id", "expected_value_sha256",
        }:
            raise LinearProjectionError(f"invalid_manifest_retirement:{index}")
        identity = (retirement.get("kind"), retirement.get("key"))
        if not all(isinstance(value, str) and value for value in identity):
            raise LinearProjectionError(f"invalid_manifest_retirement_identity:{index}")
        if identity in retired:
            raise LinearProjectionError(
                f"duplicate_manifest_retirement:{identity[0]}:{identity[1]}"
            )
        if not isinstance(retirement.get("expected_event_id"), str) or not retirement["expected_event_id"]:
            raise LinearProjectionError(f"invalid_manifest_retirement_event:{index}")
        if not re.fullmatch(r"[0-9a-f]{64}", str(retirement.get("expected_value_sha256", ""))):
            raise LinearProjectionError(f"invalid_manifest_retirement_digest:{index}")
        retired.add(identity)
    return _desired_items(manifest), retirements


def stable_live_readback(
    transport: LinearGraphQLTransport,
    comments: LinearCommentEventAdapter,
    token: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Double-collect both surfaces and refuse a mixed concurrent snapshot."""
    graph_before = transport.snapshot_for_root(token)
    comments_before = comments.comments()
    graph_after = transport.snapshot_for_root(token)
    comments_after = comments.comments()
    graph_fence = transport.snapshot_for_root(token)
    if (
        graph_before != graph_after
        or graph_after != graph_fence
        or comments_before != comments_after
    ):
        raise LinearProjectionError("projection_final_readback_changed_during_read")
    return graph_fence, comments_after


def _desired_items(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    items = manifest.get("projection")
    if not isinstance(items, list):
        raise LinearProjectionError("manifest_projection_must_be_list")
    seen: set[tuple[str, str]] = set()
    kinds: set[str] = set()
    result: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        if not isinstance(item, dict) or set(item) != {"kind", "key", "value"}:
            raise LinearProjectionError(f"invalid_manifest_projection_item:{index}")
        if item["kind"] == "disposition":
            raise LinearProjectionError("manifest_disposition_is_computed")
        identity = (item.get("kind"), item.get("key"))
        if not all(isinstance(value, str) and value for value in identity):
            raise LinearProjectionError(f"invalid_manifest_projection_identity:{index}")
        if identity in seen:
            raise LinearProjectionError(f"duplicate_manifest_projection_identity:{identity[0]}:{identity[1]}")
        if not isinstance(item.get("value"), dict):
            raise LinearProjectionError(f"invalid_manifest_projection_value:{index}")
        seen.add(identity)
        kinds.add(identity[0])
        result.append(deepcopy(item))
    missing = sorted(REQUIRED_KINDS - kinds)
    if missing:
        raise LinearProjectionError("manifest_projection_missing:" + ",".join(missing))
    if sum(item["kind"] == "scope" for item in result) != 1 or sum(
        item["kind"] == "source" for item in result
    ) != 1:
        raise LinearProjectionError("manifest_projection_singleton_invalid")
    return result


def reconcile_required_projection(
    adapter: LinearProjectionAdapter, snapshot: dict[str, Any],
    manifest: dict[str, Any], *, remote_head: str, created_at: str,
    authenticated_source: dict[str, Any],
) -> dict[str, Any]:
    """Append only missing/changed values and verify the complete current view."""
    if not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", remote_head):
        raise LinearProjectionError("verified_full_remote_head_required")
    desired, reviewed_retirements = _reviewed_manifest(manifest)
    scope_item = next(item for item in desired if item["kind"] == "scope")
    source_item = next(item for item in desired if item["kind"] == "source")
    source_identity = source_item["value"].get("identity") or source_item["value"].get("url")
    if source_identity != authenticated_source.get("identity"):
        raise LinearProjectionError("projection_source_identity_mismatch")
    if source_item["value"].get("sha256") != authenticated_source.get("sha256"):
        raise LinearProjectionError("projection_source_bytes_mismatch")
    if source_item["value"].get("sha256") != adapter.plan_revision:
        raise LinearProjectionError("root_plan_revision_source_bytes_mismatch")
    if all((adapter.workspace_id, adapter.team_id, adapter.project_id, adapter.root_issue_id)):
        linear = scope_item["value"].get("linear") or {}
        for field, expected in (
            ("workspace_id", adapter.workspace_id), ("team_id", adapter.team_id),
            ("project_id", adapter.project_id), ("root_issue_id", adapter.root_issue_id),
        ):
            if linear.get(field) != expected:
                raise LinearProjectionError(f"projection_route_mismatch:{field}")

    disposition_input = dict(snapshot)
    disposition_input.pop("disposition", None)
    # The reviewed provenance being persisted is part of this same durable
    # operation, so disposition must be derived from it on first creation too.
    disposition_input["provenance"] = [
        item["value"] for item in desired if item["kind"] == "provenance"
    ]
    decision = choose_disposition(disposition_input, remote_head=remote_head)
    disposition = {
        "disposition": decision["disposition"],
        "remote_head": remote_head,
        "recovered_from_checkpoint": decision.get("recovered_from_checkpoint"),
    }
    desired.append({"kind": "disposition", "key": "root", "value": disposition})
    desired_by_identity = {
        (item["kind"], item["key"]): item["value"] for item in desired
    }

    # Validate every event envelope before the first irreversible append.
    initial = adapter.state()
    observed_contract = projection_review_contract(initial)
    reviewed_contract = {
        "expected_projection_revision": manifest["expected_projection_revision"],
        "expected_active_heads": sorted(
            manifest["expected_active_heads"], key=lambda item: (item["kind"], item["key"])
        ),
    }
    if observed_contract != reviewed_contract:
        raise LinearProjectionError("projection_review_stale_reload_required")
    active_heads = _active_heads(initial)
    latest_heads = _latest_heads(initial)
    retirements: list[dict[str, Any]] = []
    for retirement in reviewed_retirements:
        identity = (retirement["kind"], retirement["key"])
        if identity in desired_by_identity:
            raise LinearProjectionError(
                f"projection_retirement_still_desired:{identity[0]}:{identity[1]}"
            )
        current = active_heads.get(identity)
        if current is None:
            raise LinearProjectionError(
                f"projection_retirement_missing:{identity[0]}:{identity[1]}"
            )
        if (
            current["event_id"] != retirement["expected_event_id"]
            or _value_digest(current["value"]) != retirement["expected_value_sha256"]
        ):
            raise LinearProjectionError(
                f"projection_retirement_stale:{identity[0]}:{identity[1]}"
            )
        retirements.append({
            "kind": identity[0], "key": identity[1], "value": TOMBSTONE,
        })

    for item in [*desired, *retirements]:
        build_projection_event(
            workstream_id=adapter.workstream_id,
            kind=item["kind"], key=item["key"], value=item["value"],
            plan_revision=adapter.plan_revision, expected_revision=0,
            created_at=created_at,
        )

    # Re-read the exact reviewed surface immediately before the first append.
    # A late unrelated key is as material as a changed reviewed head: neither
    # may be silently retained or tombstoned by this reconciliation.
    if projection_review_contract(adapter.state()) != observed_contract:
        raise LinearProjectionError("projection_review_stale_reload_required")

    receipts: list[dict[str, Any]] = []
    expected_revision = initial.revision
    expected_active_heads = dict(active_heads)
    expected_latest_heads = dict(latest_heads)
    for item in [*desired, *retirements]:
        state = adapter.state()
        if projection_review_contract(state) != _contract_from_heads(
            expected_revision, expected_active_heads,
        ):
            raise LinearProjectionError("projection_changed_during_reconcile")
        identity = (item["kind"], item["key"])
        active_current = expected_active_heads.get(identity)
        if active_current is not None and active_current["value"] == item["value"]:
            continue
        latest_current = expected_latest_heads.get(identity)
        event = build_projection_event(
            workstream_id=adapter.workstream_id,
            kind=item["kind"], key=item["key"], value=item["value"],
            plan_revision=adapter.plan_revision,
            expected_revision=expected_revision, created_at=created_at,
            supersedes_event_id=(
                latest_current["event_id"] if latest_current else None
            ),
        )
        receipts.append(adapter.append(event))
        expected_revision += 1
        expected_latest_heads[identity] = event
        if item["value"] == TOMBSTONE:
            expected_active_heads.pop(identity, None)
        else:
            expected_active_heads[identity] = event
        if projection_review_contract(adapter.state()) != _contract_from_heads(
            expected_revision, expected_active_heads,
        ):
            raise LinearProjectionError("projection_changed_during_reconcile")

    final = adapter.state()
    active: dict[tuple[str, str], dict[str, Any]] = {}
    for event in final.events:
        identity = (event["kind"], event["key"])
        if event["value"] == TOMBSTONE:
            active.pop(identity, None)
        else:
            active[identity] = event["value"]
    expected_active = {
        identity: deepcopy(event["value"])
        for identity, event in active_heads.items()
    }
    expected_active.update(deepcopy(desired_by_identity))
    for retirement in retirements:
        expected_active.pop((retirement["kind"], retirement["key"]), None)
    if active != expected_active:
        raise LinearProjectionError("projection_readback_not_exact")
    return {
        "workstream_id": adapter.workstream_id,
        "plan_revision": adapter.plan_revision,
        "projection_revision": final.revision,
        "writes": receipts,
        "disposition": disposition,
        "readback_verified": True,
        "projection_contract": projection_review_contract(final),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("token")
    parser.add_argument("manifest", help="reviewed projection JSON path")
    parser.add_argument("--remote-head", required=True)
    parser.add_argument("--plan-source", required=True)
    parser.add_argument("--plan-identity")
    parser.add_argument("--config")
    parser.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    args = parser.parse_args()
    try:
        token = extract_token(args.token)
        manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
        authenticated_source = plan_payload(args.plan_source, args.plan_identity)["source"]
        plan_revision = authenticated_source["sha256"]
        api_key = load_linear_api_key()
        if not api_key:
            raise LinearProjectionError("linear_auth_unavailable")
        client = HttpGraphQLClient(api_key, args.linear_endpoint)
        route, _ = resolve_linear_route(config_path=args.config)
        route = resolve_authenticated_issue_route(client, token, route)
        transport = LinearGraphQLTransport(
            client, team_id=route["team_id"], workspace_id=route["workspace_id"],
            project_id=route["project_id"],
        )
        graph = transport.snapshot_for_root(token)
        if graph["root"].get("plan_revision") != plan_revision:
            raise LinearProjectionError("root_plan_revision_source_bytes_mismatch")
        comments = LinearCommentEventAdapter(
            client, issue_id=token, workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
        ).comments()
        snapshot = add_material_history(
            graph, comments, token, authenticated_route=route,
            authenticated_source=authenticated_source,
        )
        adapter = LinearProjectionAdapter(
            client, issue_id=token, workstream_id=token,
            plan_revision=plan_revision, workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
            root_issue_id=route["root_issue_id"],
        )
        result = reconcile_required_projection(
            adapter, snapshot, manifest, remote_head=args.remote_head,
            created_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            authenticated_source=authenticated_source,
        )
        # Double-collect graph and comments so a concurrent root/child/checkpoint
        # mutation cannot be certified from a mixed pre/post-write snapshot.
        final_comments = LinearCommentEventAdapter(
            client, issue_id=token, workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
        )
        graph_after, comments_after = stable_live_readback(
            transport, final_comments, token,
        )
        verified = add_material_history(
            graph_after, comments_after, token, authenticated_route=route,
            authenticated_source=authenticated_source,
        )
        context = compact_context(
            verified, token, require_projection_authority=True,
        )
        choose_disposition(context, remote_head=args.remote_head)
        json.dump(result, sys.stdout, sort_keys=True, indent=2)
        sys.stdout.write("\n")
        return 0
    except (OSError, json.JSONDecodeError, LinearProjectionError, LinearTransportError, ResumeError,
            SuccessorError, ValueError) as error:
        print(f"workstream projection refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
