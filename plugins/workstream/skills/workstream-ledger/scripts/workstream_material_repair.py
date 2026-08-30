#!/usr/bin/env python3
"""Preview or append one reviewed material-semantic repair control event."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_delta import Delta, event_id_for
from workstream_linear import (
    HttpGraphQLClient, LinearGraphQLTransport, resolve_authenticated_issue_route,
)
from workstream_linear_checkpoints import reduce_checkpoint_comments
from workstream_linear_events import (
    LinearCommentEventAdapter, apply_material_semantic_repairs,
    ledger_boundary_slot_id, ledger_serialization_frontier, material_frontier,
    reduce_event_comments,
)
from workstream_linear_projection import reduce_projection_comments, select_plan_generation
from workstream_plan import plan_payload
from workstream_resume import (
    _checkpoint_repair_frontier, _generation_repair_binding,
    _issue_graph_repair_frontier, _projection_repair_frontier,
    read_relation_targets,
)


def _load_manifest(path: str) -> dict:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("material repair manifest must be an object")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workstream")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--plan-source", required=True)
    parser.add_argument("--plan-identity")
    parser.add_argument("--config")
    parser.add_argument("--linear-endpoint", default="https://api.linear.app/graphql")
    parser.add_argument(
        "--prepare", action="store_true",
        help="treat --manifest as a reviewed payload and emit the pinned outer manifest",
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        manifest = _load_manifest(args.manifest)
        if args.prepare:
            if args.apply:
                raise ValueError("material_repair_prepare_cannot_apply")
            payload, control = manifest, None
        elif set(manifest) == {"schema_version", "control", "payload"} and manifest.get("schema_version") == 1:
            payload, control = manifest["payload"], manifest["control"]
        else:
            raise ValueError("malformed_material_repair_manifest")
        if not isinstance(payload, dict) or (control is not None and not isinstance(control, dict)):
            raise ValueError("malformed_material_repair_manifest")
        if payload.get("workstream_id") != args.workstream.upper():
            raise ValueError("material_semantic_repair_workstream_mismatch")
        token = load_linear_api_key()
        if not token:
            raise ValueError("linear_auth_unavailable")
        client = HttpGraphQLClient(token, args.linear_endpoint)
        declared, _ = resolve_linear_route(config_path=args.config)
        route = resolve_authenticated_issue_route(client, args.workstream, declared)
        if payload.get("authenticated_route") != route:
            raise ValueError("material_semantic_repair_authenticated_route_drift")
        source = plan_payload(
            args.plan_source,
            args.plan_identity or payload.get("authenticated_source", {}).get("identity"),
        )["source"]
        if payload.get("authenticated_source") != source:
            raise ValueError("material_semantic_repair_authenticated_source_drift")
        adapter = LinearCommentEventAdapter(
            client, issue_id=args.workstream,
            workspace_id=route["workspace_id"], team_id=route["team_id"],
            project_id=route["project_id"], root_issue_id=route["root_issue_id"],
            plan_revision=payload.get("generation", {}).get("plan_revision"),
        )
        comments = adapter.comments()
        raw = reduce_event_comments(comments, workstream_id=args.workstream)
        generation = select_plan_generation(
            comments, workstream_id=args.workstream,
            description_plan_revision=payload["generation"]["plan_revision"],
            authenticated_route=route,
        )
        generation_binding = {
            "plan_revision": generation["plan_revision"],
            "transition_tip_event_id": generation["transition_tip_event_id"],
            "activation_epoch": generation["activation_epoch"],
            "authority_origin": generation["authority_origin"],
        }
        projection = reduce_projection_comments(
            comments, workstream_id=args.workstream,
            expected_plan_revision=generation["plan_revision"],
            authenticated_route=route, authenticated_source=source,
        )
        checkpoints = reduce_checkpoint_comments(comments, workstream_id=args.workstream)
        graph_snapshot = LinearGraphQLTransport(
            client, team_id=route["team_id"], workspace_id=route["workspace_id"],
            project_id=route["project_id"],
        ).snapshot_for_root(args.workstream, include_child_comments=False)
        relations = projection.snapshot.get("relations") or []
        graph_frontier = _issue_graph_repair_frontier(
            graph_snapshot, relations,
            read_relation_targets(client, relations) if relations else {},
        )
        replay = control is not None and control.get("event_id") in raw.remote_ids
        base_revision = control.get("expected_revision") if control is not None else raw.revision
        expected_id = event_id_for(
            args.workstream, "material_semantic_repair", payload, base_revision,
            source="system",
        )
        required_control = {
            "kind", "source", "event_id", "expected_revision", "created_at",
            "remote_slot_id", "payload_sha256", "canonical_event_sha256",
            "comment_body_sha256",
        }
        frontier = ledger_serialization_frontier(
            sorted(item["event_id"] for item in checkpoints.checkpoints), comments,
            workstream_id=args.workstream, authenticated_route=route,
            current_plan_revision=generation["plan_revision"],
            material_revision=base_revision,
        )
        slot_frontier = (
            payload.get("ledger_serialization_frontier") if replay else frontier
        )
        expected_slot = ledger_boundary_slot_id(
            args.workstream, base_revision, slot_frontier, route,
        )
        if args.prepare:
            payload["ledger_serialization_frontier"] = frontier
            expected_id = event_id_for(
                args.workstream, "material_semantic_repair", payload, base_revision,
                source="system",
            )
        elif not replay and payload.get("ledger_serialization_frontier") != frontier:
            raise ValueError("material_repair_ledger_frontier_drift")
        from workstream_linear_events import _canonical_event, encode_event_comment
        import hashlib
        if control is None:
            created_at = payload.get("review_artifact", {}).get("reviewed_at")
            provisional = Delta(
                expected_id, args.workstream, "material_semantic_repair", "system",
                payload, base_revision, created_at,
            )
            body = encode_event_comment(provisional)
            control = {
                "kind": "material_semantic_repair", "source": "system",
                "event_id": expected_id, "expected_revision": base_revision,
                "created_at": created_at, "remote_slot_id": expected_slot,
                "payload_sha256": hashlib.sha256(json.dumps(
                    payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                ).encode()).hexdigest(),
                "canonical_event_sha256": _canonical_event(provisional)["sha256"],
                "comment_body_sha256": hashlib.sha256(body.encode()).hexdigest(),
            }
        if (
            set(control) != required_control
            or control.get("kind") != "material_semantic_repair"
            or control.get("source") != "system"
            or not isinstance(base_revision, int) or isinstance(base_revision, bool)
            or base_revision < 0
            or control.get("event_id") != expected_id
            or not isinstance(control.get("created_at"), str)
            or not control["created_at"]
        ):
            raise ValueError("material_repair_control_mismatch")
        if (not replay and raw.revision != base_revision) or (
            replay and raw.revision < base_revision + 1
        ):
            raise ValueError("material_repair_control_revision_drift")
        if control.get("remote_slot_id") != expected_slot:
            raise ValueError("material_repair_remote_slot_mismatch")
        if replay and raw.remote_ids[control["event_id"]] != expected_slot:
            raise ValueError("material_repair_existing_remote_slot_mismatch")
        candidate_id = control["event_id"]
        candidate = Delta(
            candidate_id, args.workstream, "material_semantic_repair", "system",
            payload, base_revision, control["created_at"],
        )
        candidate_body = encode_event_comment(candidate)
        if (
            control["payload_sha256"] != hashlib.sha256(json.dumps(
                payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode()).hexdigest()
            or control["canonical_event_sha256"] != _canonical_event(candidate)["sha256"]
            or control["comment_body_sha256"] != hashlib.sha256(
                candidate_body.encode()
            ).hexdigest()
        ):
            raise ValueError("material_repair_control_digest_mismatch")
        # Preview through the exact same two-pass validator by adding an inert
        # synthetic envelope. No remote capability or mutation is attempted.
        synthetic = comments if replay else [*comments, {
            "id": expected_slot, "body": candidate_body,
            "createdAt": candidate.created_at, "updatedAt": candidate.created_at,
        }]
        preview_raw = reduce_event_comments(synthetic, workstream_id=args.workstream)
        validated = apply_material_semantic_repairs(
            preview_raw, synthetic,
            checkpoint_frontier=_checkpoint_repair_frontier(
                checkpoints, count=payload["checkpoint_frontier"]["count"],
            ),
            projection_frontier=_projection_repair_frontier(
                projection, revision=payload["projection_frontier"]["revision"],
            ),
            generation=generation_binding, authenticated_route=route,
            authenticated_source=source,
            issue_graph_frontier=graph_frontier,
            ledger_serialization_frontier_value=frontier,
        )
        if args.prepare:
            json.dump({
                "schema_version": 1, "control": control, "payload": payload,
            }, sys.stdout, ensure_ascii=False, sort_keys=True, indent=2)
            sys.stdout.write("\n")
            return 0
        receipt = None
        if args.apply:
            receipt = adapter.apply(candidate)
            try:
                post_route = resolve_authenticated_issue_route(
                    client, args.workstream, declared,
                )
                post_source = plan_payload(
                    args.plan_source,
                    args.plan_identity or source.get("identity"),
                )["source"]
                post_comments = adapter.comments()
                post_raw = reduce_event_comments(
                    post_comments, workstream_id=args.workstream,
                )
                post_generation = select_plan_generation(
                    post_comments, workstream_id=args.workstream,
                    description_plan_revision=generation["description_plan_revision"],
                    authenticated_route=post_route,
                )
                post_generation_binding = {
                    "plan_revision": post_generation["plan_revision"],
                    "transition_tip_event_id": post_generation["transition_tip_event_id"],
                    "activation_epoch": post_generation["activation_epoch"],
                    "authority_origin": post_generation["authority_origin"],
                }
                post_projection = reduce_projection_comments(
                    post_comments, workstream_id=args.workstream,
                    expected_plan_revision=post_generation["plan_revision"],
                    authenticated_route=post_route, authenticated_source=post_source,
                )
                post_relations = post_projection.snapshot.get("relations") or []
                post_graph = _issue_graph_repair_frontier(
                    LinearGraphQLTransport(
                        client, team_id=post_route["team_id"],
                        workspace_id=post_route["workspace_id"],
                        project_id=post_route["project_id"],
                    ).snapshot_for_root(args.workstream, include_child_comments=False),
                    post_relations,
                    read_relation_targets(client, post_relations)
                    if post_relations else {},
                )
                validated = apply_material_semantic_repairs(
                    post_raw, post_comments,
                    checkpoint_frontier=_checkpoint_repair_frontier(
                        reduce_checkpoint_comments(
                            post_comments, workstream_id=args.workstream,
                        ), count=payload["checkpoint_frontier"]["count"],
                    ),
                    projection_frontier=_projection_repair_frontier(
                        post_projection,
                        revision=payload["projection_frontier"]["revision"],
                    ),
                    generation=post_generation_binding,
                    authenticated_route=post_route,
                    authenticated_source=post_source,
                    issue_graph_frontier=post_graph,
                    ledger_serialization_frontier_value=ledger_serialization_frontier(
                        sorted(item["event_id"] for item in reduce_checkpoint_comments(
                            post_comments, workstream_id=args.workstream,
                        ).checkpoints),
                        post_comments, workstream_id=args.workstream,
                        authenticated_route=post_route,
                        current_plan_revision=post_generation["plan_revision"],
                        material_revision=base_revision,
                    ),
                )
            except Exception as post_error:
                json.dump({
                    "applied": True, "replay": replay,
                    "event_id": candidate_id, "remote_slot_id": expected_slot,
                    "receipt": receipt.__dict__,
                    "post_resume_validation": "durable_partial_replay_required",
                    "error": str(post_error),
                }, sys.stdout, sort_keys=True, indent=2)
                sys.stdout.write("\n")
                return 3
        output = {
            "applied": bool(args.apply), "replay": replay,
            "event_id": candidate_id, "expected_revision": base_revision,
            "remote_slot_id": expected_slot,
            "raw_frontier": material_frontier(raw),
            "repair_count": len(validated.repair_bindings),
            "receipt": (receipt.__dict__ if receipt is not None else None),
            "post_resume_validation": "valid",
        }
        json.dump(output, sys.stdout, sort_keys=True, indent=2)
        sys.stdout.write("\n")
        return 0
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        print(f"material repair refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
