#!/usr/bin/env python3
"""Idempotently reconcile the required append-only Linear resume projection."""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
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
    desired = _desired_items(manifest)
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
    active_heads: dict[tuple[str, str], dict[str, Any]] = {}
    for event in initial.events:
        identity = (event["kind"], event["key"])
        if event["value"] == TOMBSTONE:
            active_heads.pop(identity, None)
        else:
            active_heads[identity] = event
    retirements = [
        {"kind": kind, "key": key, "value": TOMBSTONE}
        for (kind, key) in sorted(active_heads)
        if (kind, key) not in desired_by_identity
    ]

    for item in [*desired, *retirements]:
        build_projection_event(
            workstream_id=adapter.workstream_id,
            kind=item["kind"], key=item["key"], value=item["value"],
            plan_revision=adapter.plan_revision, expected_revision=0,
            created_at=created_at,
        )

    receipts: list[dict[str, Any]] = []
    for item in [*desired, *retirements]:
        state = adapter.state()
        current = next((
            event for event in reversed(state.events)
            if event["kind"] == item["kind"] and event["key"] == item["key"]
        ), None)
        if current is not None and current["value"] == item["value"]:
            continue
        event = build_projection_event(
            workstream_id=adapter.workstream_id,
            kind=item["kind"], key=item["key"], value=item["value"],
            plan_revision=adapter.plan_revision,
            expected_revision=state.revision, created_at=created_at,
            supersedes_event_id=current["event_id"] if current else None,
        )
        receipts.append(adapter.append(event))

    final = adapter.state()
    active: dict[tuple[str, str], dict[str, Any]] = {}
    for event in final.events:
        identity = (event["kind"], event["key"])
        if event["value"] == TOMBSTONE:
            active.pop(identity, None)
        else:
            active[identity] = event["value"]
    if active != desired_by_identity:
        raise LinearProjectionError("projection_readback_not_exact")
    return {
        "workstream_id": adapter.workstream_id,
        "plan_revision": adapter.plan_revision,
        "projection_revision": final.revision,
        "writes": receipts,
        "disposition": disposition,
        "readback_verified": True,
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
