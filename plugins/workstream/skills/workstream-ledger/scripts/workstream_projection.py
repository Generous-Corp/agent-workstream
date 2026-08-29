#!/usr/bin/env python3
"""Idempotently reconcile the required append-only Linear resume projection."""

from __future__ import annotations

import argparse
from collections.abc import Callable
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
from workstream_relation_readback import RelationReadbackError, read_relation_targets
from workstream_resume import add_material_history, compact_context, extract_token, ResumeError
from workstream_scope import ScopeError, validate_relation_graph
from workstream_successor import choose_disposition, SuccessorError


REQUIRED_KINDS = {"scope", "source", "provenance"}


def _value_digest(value: Any) -> str:
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
    legacy_events = (
        list(state.events)
        if state.events and all(event["schema_version"] == 1 for event in state.events)
        else []
    )
    quarantine = state.snapshot.get("projection_quarantined") or []
    return _contract_from_heads(
        state.revision, _active_heads(state),
        legacy_event_ids=[event["event_id"] for event in legacy_events],
        legacy_events_sha256=(
            _value_digest(legacy_events) if legacy_events else None
        ),
        quarantine_count=len(quarantine),
        quarantine_sha256=_value_digest(quarantine),
    )


def _contract_from_heads(
    revision: int, active: dict[tuple[str, str], dict[str, Any]],
    *, legacy_event_ids: list[str], legacy_events_sha256: str | None,
    quarantine_count: int, quarantine_sha256: str,
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
        "expected_legacy_v1_event_ids": list(legacy_event_ids),
        "expected_legacy_v1_events_sha256": legacy_events_sha256,
        "expected_projection_quarantine_count": quarantine_count,
        "expected_projection_quarantine_sha256": quarantine_sha256,
    }


def _reviewed_manifest(manifest: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    required = {
        "projection", "retirements", "expected_projection_revision",
        "expected_active_heads", "expected_legacy_v1_event_ids",
        "expected_legacy_v1_events_sha256", "expected_projection_quarantine_count",
        "expected_projection_quarantine_sha256",
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
    legacy_ids = manifest["expected_legacy_v1_event_ids"]
    legacy_digest = manifest["expected_legacy_v1_events_sha256"]
    if (
        not isinstance(legacy_ids, list)
        or legacy_ids != list(dict.fromkeys(legacy_ids))
        or not all(isinstance(event_id, str) and event_id for event_id in legacy_ids)
        or (
            legacy_ids
            and not re.fullmatch(r"[0-9a-f]{64}", str(legacy_digest or ""))
        )
        or (not legacy_ids and legacy_digest is not None)
    ):
        raise LinearProjectionError("invalid_manifest_legacy_v1_contract")
    quarantine_count = manifest["expected_projection_quarantine_count"]
    if (
        not isinstance(quarantine_count, int)
        or quarantine_count < 0
        or not re.fullmatch(
            r"[0-9a-f]{64}",
            str(manifest["expected_projection_quarantine_sha256"]),
        )
    ):
        raise LinearProjectionError("invalid_manifest_projection_quarantine_contract")
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


def load_material_history_for_projection_reconcile(
    snapshot: dict[str, Any], comments: list[dict[str, Any]], token: str,
    manifest: dict[str, Any], adapter: LinearProjectionAdapter, *,
    authenticated_route: dict[str, str], authenticated_source: dict[str, Any],
    relation_target_resolver: Callable[
        [list[dict[str, Any]]], dict[str, dict[str, Any]]
    ],
) -> tuple[dict[str, Any], frozenset[tuple[str, str]]]:
    """Load strict history, except for an exactly reviewed relation migration.

    Historical relation heads can predate the peer projection contract.  They
    may be inspected only by this reconcile boundary, and only when every head
    whose authenticated peer readback is incomplete is exactly retired or
    replaced by the reviewed manifest.  Ordinary resume never calls this
    helper and remains strict.
    """
    try:
        return add_material_history(
            snapshot, comments, token, authenticated_route=authenticated_route,
            authenticated_source=authenticated_source,
            relation_target_resolver=relation_target_resolver,
        ), frozenset()
    except RelationReadbackError:
        desired, reviewed_retirements = _reviewed_manifest(manifest)
        initial = adapter.state()
        if projection_review_contract(initial) != {
            "expected_projection_revision": manifest["expected_projection_revision"],
            "expected_active_heads": sorted(
                manifest["expected_active_heads"],
                key=lambda item: (item["kind"], item["key"]),
            ),
            "expected_legacy_v1_event_ids": manifest[
                "expected_legacy_v1_event_ids"
            ],
            "expected_legacy_v1_events_sha256": manifest[
                "expected_legacy_v1_events_sha256"
            ],
            "expected_projection_quarantine_count": manifest[
                "expected_projection_quarantine_count"
            ],
            "expected_projection_quarantine_sha256": manifest[
                "expected_projection_quarantine_sha256"
            ],
        }:
            raise LinearProjectionError("projection_review_stale_reload_required")

        active = _active_heads(initial)
        unresolved: set[tuple[str, str]] = set()
        for identity, event in active.items():
            if identity[0] != "relation":
                continue
            try:
                relation_target_resolver([deepcopy(event["value"])])
            except RelationReadbackError:
                unresolved.add(identity)
        if not unresolved:
            # Do not turn an unexpected batched-read failure into a bypass.
            raise

        desired_by_identity = {
            (item["kind"], item["key"]): item["value"] for item in desired
        }
        retirements_by_identity = {
            (item["kind"], item["key"]): item for item in reviewed_retirements
        }
        uncovered: list[str] = []
        for identity in sorted(unresolved):
            current = active[identity]
            replacement = desired_by_identity.get(identity)
            retirement = retirements_by_identity.get(identity)
            replaced = replacement is not None and replacement != current["value"]
            retired = retirement is not None and (
                retirement["expected_event_id"] == current["event_id"]
                and retirement["expected_value_sha256"]
                == _value_digest(current["value"])
            )
            if not replaced and not retired:
                uncovered.append(f"{identity[0]}:{identity[1]}")
        if uncovered:
            raise LinearProjectionError(
                "legacy_unresolved_relation_migration_required:"
                + ",".join(uncovered)
            )

        return add_material_history(
            snapshot, comments, token, authenticated_route=authenticated_route,
            authenticated_source=authenticated_source,
            permit_stale_lifecycle_for_reconcile=True,
        ), frozenset(unresolved)


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
    relation_target_resolver: (
        Callable[[list[dict[str, Any]]], dict[str, dict[str, Any]]] | None
    ) = None,
    legacy_unresolved_relation_heads: frozenset[tuple[str, str]] = frozenset(),
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
        "expected_legacy_v1_event_ids": manifest["expected_legacy_v1_event_ids"],
        "expected_legacy_v1_events_sha256": manifest[
            "expected_legacy_v1_events_sha256"
        ],
        "expected_projection_quarantine_count": manifest[
            "expected_projection_quarantine_count"
        ],
        "expected_projection_quarantine_sha256": manifest[
            "expected_projection_quarantine_sha256"
        ],
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

    for identity in legacy_unresolved_relation_heads:
        current = active_heads.get(identity)
        replacement = desired_by_identity.get(identity)
        retired = any(
            (item["kind"], item["key"]) == identity for item in retirements
        )
        if (
            current is None
            or identity[0] != "relation"
            or (not retired and (replacement is None or replacement == current["value"]))
        ):
            raise LinearProjectionError(
                f"legacy_unresolved_relation_migration_required:{identity[0]}:{identity[1]}"
            )

    effective_relations = {
        key: deepcopy(event["value"])
        for (kind, key), event in active_heads.items()
        if kind == "relation"
    }
    effective_relations.update({
        key: deepcopy(value)
        for (kind, key), value in desired_by_identity.items()
        if kind == "relation"
    })
    for retirement in retirements:
        if retirement["kind"] == "relation":
            effective_relations.pop(retirement["key"], None)
    if effective_relations:
        if relation_target_resolver is None:
            raise LinearProjectionError("relation_target_readback_required")
        relations = [effective_relations[key] for key in sorted(effective_relations)]
        try:
            validate_relation_graph(
                relations, root_id=adapter.workstream_id,
                workspace_id=adapter.workspace_id,
                root_issue_id=adapter.root_issue_id,
                resolve_target=relation_target_resolver(relations),
            )
        except ScopeError as error:
            raise LinearProjectionError(str(error)) from error

    migration_items = [
        item for item in [*desired, *retirements]
        if (item["kind"], item["key"]) in legacy_unresolved_relation_heads
    ]
    remaining_items = [
        item for item in [*desired, *retirements]
        if (item["kind"], item["key"]) not in legacy_unresolved_relation_heads
    ]
    write_items = [*migration_items, *remaining_items]

    for item in write_items:
        build_projection_event(
            workstream_id=adapter.workstream_id,
            kind=item["kind"], key=item["key"], value=item["value"],
            plan_revision=adapter.plan_revision, expected_revision=0,
            created_at=created_at,
            authority=adapter.authority,
        )

    # Re-read the exact reviewed surface immediately before the first append.
    # A late unrelated key is as material as a changed reviewed head: neither
    # may be silently retained or tombstoned by this reconciliation.
    if projection_review_contract(adapter.state()) != observed_contract:
        raise LinearProjectionError("projection_review_stale_reload_required")

    activation_receipt = None
    if initial.events and all(
        event["schema_version"] == 1 for event in initial.events
    ):
        legacy_event_ids = manifest["expected_legacy_v1_event_ids"]
        activation_receipt = adapter.activate_v2(
            created_at=created_at, expected_revision=initial.revision,
            expected_legacy_event_ids=legacy_event_ids,
            expected_legacy_events_sha256=manifest[
                "expected_legacy_v1_events_sha256"
            ],
        )
        activated = adapter.state()
        activated_contract = projection_review_contract(activated)
        if (
            activated.revision != initial.revision + 1
            or [event["event_id"] for event in activated.events[:initial.revision]]
            != legacy_event_ids
            or activated.events[-1]["kind"] != "cas_activation"
            or activated.events[-1]["value"].get("legacy_event_ids") != legacy_event_ids
            or activated_contract["expected_legacy_v1_event_ids"] != []
            or activated_contract["expected_legacy_v1_events_sha256"] is not None
            or activated_contract["expected_projection_quarantine_count"] != 0
        ):
            raise LinearProjectionError("projection_v2_activation_readback_mismatch")
        initial = activated
        observed_contract = activated_contract
        active_heads = _active_heads(initial)
        latest_heads = _latest_heads(initial)

    receipts: list[dict[str, Any]] = (
        [activation_receipt] if activation_receipt is not None else []
    )
    expected_revision = initial.revision
    expected_active_heads = dict(active_heads)
    expected_latest_heads = dict(latest_heads)
    for item in write_items:
        state = adapter.state()
        if projection_review_contract(state) != _contract_from_heads(
            expected_revision, expected_active_heads,
            legacy_event_ids=observed_contract["expected_legacy_v1_event_ids"],
            legacy_events_sha256=observed_contract[
                "expected_legacy_v1_events_sha256"
            ],
            quarantine_count=observed_contract[
                "expected_projection_quarantine_count"
            ],
            quarantine_sha256=observed_contract[
                "expected_projection_quarantine_sha256"
            ],
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
            authority=adapter.authority,
        )
        receipts.append(adapter.append(
            event,
            expected_quarantine_count=observed_contract[
                "expected_projection_quarantine_count"
            ],
            expected_quarantine_sha256=observed_contract[
                "expected_projection_quarantine_sha256"
            ],
        ))
        expected_revision += 1
        expected_latest_heads[identity] = event
        if item["value"] == TOMBSTONE:
            expected_active_heads.pop(identity, None)
        else:
            expected_active_heads[identity] = event
        if projection_review_contract(adapter.state()) != _contract_from_heads(
            expected_revision, expected_active_heads,
            legacy_event_ids=observed_contract["expected_legacy_v1_event_ids"],
            legacy_events_sha256=observed_contract[
                "expected_legacy_v1_events_sha256"
            ],
            quarantine_count=observed_contract[
                "expected_projection_quarantine_count"
            ],
            quarantine_sha256=observed_contract[
                "expected_projection_quarantine_sha256"
            ],
        ):
            raise LinearProjectionError("projection_changed_during_reconcile")

    final = adapter.state()
    if projection_review_contract(final) != _contract_from_heads(
        expected_revision, expected_active_heads,
        legacy_event_ids=observed_contract["expected_legacy_v1_event_ids"],
        legacy_events_sha256=observed_contract[
            "expected_legacy_v1_events_sha256"
        ],
        quarantine_count=observed_contract[
            "expected_projection_quarantine_count"
        ],
        quarantine_sha256=observed_contract[
            "expected_projection_quarantine_sha256"
        ],
    ):
        raise LinearProjectionError("projection_final_contract_mismatch")
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
    parser.add_argument(
        "--max-bytes", type=int, default=16 * 1024,
        help="maximum encoded full-resume context accepted after projection",
    )
    parser.add_argument(
        "--max-items", type=int, default=100,
        help="maximum full-resume item count accepted after projection",
    )
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
        adapter = LinearProjectionAdapter(
            client, issue_id=token, workstream_id=token,
            plan_revision=plan_revision, workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
            root_issue_id=route["root_issue_id"],
        )
        resolver = lambda relations: read_relation_targets(client, relations)
        snapshot, legacy_unresolved_relation_heads = (
            load_material_history_for_projection_reconcile(
                graph, comments, token, manifest, adapter,
                authenticated_route=route,
                authenticated_source=authenticated_source,
                relation_target_resolver=resolver,
            )
        )
        result = reconcile_required_projection(
            adapter, snapshot, manifest, remote_head=args.remote_head,
            created_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            authenticated_source=authenticated_source,
            relation_target_resolver=resolver,
            legacy_unresolved_relation_heads=legacy_unresolved_relation_heads,
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
            relation_target_resolver=lambda relations: read_relation_targets(
                client, relations,
            ),
        )
        context = compact_context(
            verified, token, max_bytes=args.max_bytes, max_items=args.max_items,
            require_projection_authority=True,
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
