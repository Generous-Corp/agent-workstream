#!/usr/bin/env python3
"""Preview or append one reviewed material-semantic repair control event."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from workstream_config import load_linear_api_key, resolve_linear_route
from workstream_delta import Delta, MutationReceipt, canonical_sha256, event_id_for
from workstream_linear import (
    HttpGraphQLClient, LinearGraphQLTransport, resolve_authenticated_issue_route,
)
from workstream_linear_checkpoints import reduce_checkpoint_comments
from workstream_linear_events import (
    LinearCommentEventAdapter, apply_material_semantic_repairs,
    assert_exact_pinned_repair_comment, canonical_authenticated_source,
    encode_reviewed_repair_comment, ledger_boundary_slot_id,
    ledger_serialization_frontier, material_frontier,
    PinnedRepairPreconditionError, reduce_event_comments,
    validate_review_artifact_identity,
)
from workstream_linear_projection import reduce_projection_comments, select_plan_generation
from workstream_plan import plan_payload, source_bytes
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


def _verify_review_artifact(payload: dict, path: str | None) -> None:
    """Authenticate the exact locally reviewed normalization artifact."""
    if not path:
        raise ValueError("material_repair_review_artifact_file_required")
    material = Path(path).read_bytes()
    artifact = payload.get("review_artifact")
    if not isinstance(artifact, dict):
        raise ValueError("material_repair_review_artifact_missing")
    import hashlib
    if hashlib.sha256(material).hexdigest() != artifact.get("sha256"):
        raise ValueError("material_repair_review_artifact_digest_mismatch")
    try:
        reviewed = json.loads(material)
    except json.JSONDecodeError as error:
        raise ValueError("material_repair_review_artifact_malformed") from error
    if reviewed != {
        "schema_version": 1,
        "workstream_id": payload.get("workstream_id"),
        "target_bindings": payload.get("target_bindings"),
    }:
        raise ValueError("material_repair_review_artifact_content_mismatch")
    identity = artifact.get("identity")
    validate_review_artifact_identity(artifact)
    try:
        fetched, fetched_identity = source_bytes(identity, identity)
    except Exception as error:
        raise ValueError("material_repair_review_artifact_fetch_failed") from error
    if fetched_identity != identity or fetched != material:
        raise ValueError("material_repair_review_artifact_remote_mismatch")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workstream")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--plan-source", required=True)
    parser.add_argument("--plan-identity")
    parser.add_argument(
        "--review-artifact",
        help="local copy of the immutable reviewed target-binding artifact",
    )
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
            seed_fields = {
                "schema_version", "workstream_id", "target_bindings",
                "authenticated_route", "authenticated_source", "generation",
                "review_artifact", "strict_target_candidate_sha256",
            }
            if set(manifest) != seed_fields or manifest.get("schema_version") != 1:
                raise ValueError("malformed_material_repair_reviewed_seed")
            payload, control = dict(manifest), None
        elif set(manifest) == {"schema_version", "control", "payload"} and manifest.get("schema_version") == 1:
            payload, control = manifest["payload"], manifest["control"]
        else:
            raise ValueError("malformed_material_repair_manifest")
        if not isinstance(payload, dict) or (control is not None and not isinstance(control, dict)):
            raise ValueError("malformed_material_repair_manifest")
        if payload.get("workstream_id") != args.workstream.upper():
            raise ValueError("material_semantic_repair_workstream_mismatch")
        _verify_review_artifact(payload, args.review_artifact)
        token = load_linear_api_key()
        if not token:
            raise ValueError("linear_auth_unavailable")
        client = HttpGraphQLClient(token, args.linear_endpoint)
        declared, _ = resolve_linear_route(config_path=args.config)
        route = resolve_authenticated_issue_route(client, args.workstream, declared)
        reviewed_source = canonical_authenticated_source(
            payload.get("authenticated_source")
        )
        if control is None:
            payload["authenticated_source"] = reviewed_source
        elif payload.get("authenticated_source") != reviewed_source:
            raise ValueError("malformed_material_semantic_repair_authenticated_source")
        source = canonical_authenticated_source(plan_payload(
            args.plan_source,
            args.plan_identity or reviewed_source["identity"],
        )["source"])
        if reviewed_source != source:
            raise ValueError("material_semantic_repair_authenticated_source_drift")
        adapter = LinearCommentEventAdapter(
            client, issue_id=args.workstream,
            workspace_id=route["workspace_id"], team_id=route["team_id"],
            project_id=route["project_id"], root_issue_id=route["root_issue_id"],
            plan_revision=payload.get("generation", {}).get("plan_revision"),
        )
        comments = adapter.comments()
        raw = reduce_event_comments(comments, workstream_id=args.workstream)
        replay = control is not None and control.get("event_id") in raw.remote_ids
        historical_route = payload.get("authenticated_route", {})
        if replay:
            if any(
                historical_route.get(field) != route.get(field)
                for field in ("workspace_id", "root_issue_id")
            ):
                raise ValueError("material_semantic_repair_root_authority_drift")
            generation = {
                **payload["generation"],
                "description_plan_revision": payload["generation"]["plan_revision"],
            }
            generation_binding = payload["generation"]
            projection = None
            checkpoints = None
            graph_frontier = payload["issue_graph_frontier"]
        else:
            if historical_route != route:
                raise ValueError("material_semantic_repair_authenticated_route_drift")
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
            checkpoints = reduce_checkpoint_comments(
                comments, workstream_id=args.workstream,
            )
            graph_snapshot = LinearGraphQLTransport(
                client, team_id=route["team_id"], workspace_id=route["workspace_id"],
                project_id=route["project_id"],
            ).snapshot_for_root(
                args.workstream, include_child_comments=False,
                include_description=True,
            )
            relations = projection.snapshot.get("relations") or []
            graph_frontier = _issue_graph_repair_frontier(
                graph_snapshot, relations,
                read_relation_targets(client, relations) if relations else {},
            )
        if args.prepare:
            strict_candidate = payload.pop("strict_target_candidate_sha256", None)
            if (
                not isinstance(strict_candidate, str)
                or len(strict_candidate) != 64
                or any(ch not in "0123456789abcdef" for ch in strict_candidate)
                or len(payload.get("target_bindings", [])) != 2
                or not projection.events
            ):
                raise ValueError("material_repair_strict_target_candidate_required")
            if generation_binding != payload["generation"]:
                raise ValueError("material_semantic_repair_generation_drift")
            payload.update({
                "raw_frontier": material_frontier(raw),
                "checkpoint_frontier": _checkpoint_repair_frontier(checkpoints),
                "projection_frontier": _projection_repair_frontier(projection),
                "issue_graph_frontier": graph_frontier,
                "ledger_serialization_frontier": [],
            })
            seal_event = projection.events[-1]
            seal_remote_id = projection.remote_ids[seal_event["event_id"]]
            seal_comments = [
                item for item in comments if item.get("id") == seal_remote_id
            ]
            if len(seal_comments) != 1 or not isinstance(
                seal_comments[0].get("body"), str
            ):
                raise ValueError("material_repair_projection_seal_missing")
            source_events = [
                event for event in projection.events
                if event.get("kind") == "source"
                and event.get("value") == source
            ]
            if len(source_events) != 1:
                raise ValueError("material_repair_projection_source_missing")
            source_event = source_events[0]
            source_remote_id = projection.remote_ids[source_event["event_id"]]
            source_comments = [
                item for item in comments if item.get("id") == source_remote_id
            ]
            if len(source_comments) != 1 or not isinstance(
                source_comments[0].get("body"), str
            ):
                raise ValueError("material_repair_projection_source_missing")
            fences = {
                key: payload[key] for key in (
                    "checkpoint_frontier", "projection_frontier", "generation",
                    "authenticated_route", "authenticated_source",
                    "issue_graph_frontier",
                )
            }
            import hashlib
            payload["postwrite_oracle"] = {
                "schema_version": 1, "target_binding_count": 2,
                "target_bindings_sha256": canonical_sha256(
                    payload["target_bindings"]
                ),
                "strict_target_candidate_sha256": strict_candidate,
                "source_identity": source["identity"],
                "source_sha256": source["sha256"],
                "source_event_id": source_event["event_id"],
                "source_remote_comment_id": source_remote_id,
                "source_comment_body_sha256": hashlib.sha256(
                    source_comments[0]["body"].encode()
                ).hexdigest(),
                "source_event_sha256": canonical_sha256(source_event),
                "projection_seal_event_id": seal_event["event_id"],
                "projection_seal_remote_comment_id": seal_remote_id,
                "projection_seal_comment_body_sha256": hashlib.sha256(
                    seal_comments[0]["body"].encode()
                ).hexdigest(),
                "projection_seal_event_sha256": canonical_sha256(seal_event),
                "generation_tip_event_id": generation["transition_tip_event_id"],
                "fences_sha256": canonical_sha256(fences),
            }
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
        frontier = payload["ledger_serialization_frontier"] if replay else (
            ledger_serialization_frontier(
                sorted(item["event_id"] for item in checkpoints.checkpoints),
                comments, workstream_id=args.workstream,
                authenticated_route=route,
                current_plan_revision=generation["plan_revision"],
                material_revision=base_revision,
            )
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
        from workstream_linear_events import _canonical_event
        import hashlib
        if control is None:
            created_at = payload.get("review_artifact", {}).get("reviewed_at")
            provisional = Delta(
                expected_id, args.workstream, "material_semantic_repair", "system",
                payload, base_revision, created_at,
            )
            body = encode_reviewed_repair_comment(provisional)
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
        candidate_body = encode_reviewed_repair_comment(candidate)
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
        if replay:
            assert_exact_pinned_repair_comment(
                comments, candidate, remote_slot_id=expected_slot,
                comment_body_sha256=control["comment_body_sha256"],
            )
        # Preview through the exact same two-pass validator by adding an inert
        # synthetic envelope. No remote capability or mutation is attempted.
        synthetic = comments if replay else [*comments, {
            "id": expected_slot, "body": candidate_body,
            "createdAt": candidate.created_at, "updatedAt": candidate.created_at,
        }]
        preview_raw = reduce_event_comments(synthetic, workstream_id=args.workstream)
        validated = apply_material_semantic_repairs(
            preview_raw, synthetic,
            checkpoint_frontier=(
                payload["checkpoint_frontier"] if replay
                else _checkpoint_repair_frontier(
                    checkpoints, count=payload["checkpoint_frontier"]["count"],
                )
            ),
            projection_frontier=(
                payload["projection_frontier"] if replay
                else _projection_repair_frontier(
                    projection, revision=payload["projection_frontier"]["revision"],
                )
            ),
            generation=generation_binding, authenticated_route=route,
            authenticated_source=source,
            issue_graph_frontier=graph_frontier,
            ledger_serialization_frontier_value=frontier,
            validate_live_fences=not replay,
        )
        if args.prepare:
            json.dump({
                "schema_version": 1, "control": control, "payload": payload,
            }, sys.stdout, ensure_ascii=False, sort_keys=True, indent=2)
            sys.stdout.write("\n")
            return 0
        receipt = None
        reobserved_after_apply_error = False
        if args.apply:
            if not replay:
                final_route = resolve_authenticated_issue_route(
                    client, args.workstream, declared,
                )
                final_source = canonical_authenticated_source(plan_payload(
                    args.plan_source,
                    args.plan_identity or source.get("identity"),
                )["source"])
                final_comments = adapter.comments()
                final_raw = reduce_event_comments(
                    final_comments, workstream_id=args.workstream,
                )
                final_generation = select_plan_generation(
                    final_comments, workstream_id=args.workstream,
                    description_plan_revision=generation["description_plan_revision"],
                    authenticated_route=final_route,
                )
                final_projection = reduce_projection_comments(
                    final_comments, workstream_id=args.workstream,
                    expected_plan_revision=final_generation["plan_revision"],
                    authenticated_route=final_route,
                    authenticated_source=final_source,
                )
                final_checkpoints = reduce_checkpoint_comments(
                    final_comments, workstream_id=args.workstream,
                )
                final_relations = final_projection.snapshot.get("relations") or []
                final_graph = _issue_graph_repair_frontier(
                    LinearGraphQLTransport(
                        client, team_id=final_route["team_id"],
                        workspace_id=final_route["workspace_id"],
                        project_id=final_route["project_id"],
                    ).snapshot_for_root(
                        args.workstream, include_child_comments=False,
                        include_description=True,
                    ),
                    final_relations,
                    read_relation_targets(client, final_relations)
                    if final_relations else {},
                )
                final_generation_binding = {
                    "plan_revision": final_generation["plan_revision"],
                    "transition_tip_event_id": final_generation["transition_tip_event_id"],
                    "activation_epoch": final_generation["activation_epoch"],
                    "authority_origin": final_generation["authority_origin"],
                }
                final_ledger = ledger_serialization_frontier(
                    sorted(item["event_id"] for item in final_checkpoints.checkpoints),
                    final_comments, workstream_id=args.workstream,
                    authenticated_route=final_route,
                    current_plan_revision=final_generation["plan_revision"],
                    material_revision=base_revision,
                )
                if (
                    final_route != payload["authenticated_route"]
                    or final_source != payload["authenticated_source"]
                    or material_frontier(final_raw) != payload["raw_frontier"]
                    or final_generation_binding != payload["generation"]
                    or _checkpoint_repair_frontier(final_checkpoints)
                    != payload["checkpoint_frontier"]
                    or _projection_repair_frontier(final_projection)
                    != payload["projection_frontier"]
                    or final_graph != payload["issue_graph_frontier"]
                    or final_ledger != payload["ledger_serialization_frontier"]
                ):
                    raise ValueError("material_repair_final_prewrite_fence_drift")
            try:
                receipt = adapter.apply_pinned_repair(
                    candidate, expected_remote_slot=expected_slot,
                    expected_serialization_frontier=(
                        payload["ledger_serialization_frontier"]
                    ),
                    expected_comment_body_sha256=control[
                        "comment_body_sha256"
                    ],
                )
            except PinnedRepairPreconditionError:
                raise
            except Exception as apply_error:
                try:
                    uncertain_comments = adapter.comments()
                    uncertain_raw = reduce_event_comments(
                        uncertain_comments, workstream_id=args.workstream,
                    )
                    observed = next(
                        event for event in uncertain_raw.events
                        if event.event_id == candidate.event_id
                    )
                    if (
                        uncertain_raw.remote_ids[candidate.event_id] != expected_slot
                        or _canonical_event(observed) != _canonical_event(candidate)
                    ):
                        raise ValueError("repair_control_observation_mismatch")
                    assert_exact_pinned_repair_comment(
                        uncertain_comments, candidate,
                        remote_slot_id=expected_slot,
                        comment_body_sha256=control["comment_body_sha256"],
                    )
                    receipt = MutationReceipt(
                        candidate.event_id,
                        next(index for index, event in enumerate(
                            uncertain_raw.events, start=1,
                        ) if event.event_id == candidate.event_id),
                        expected_slot,
                    )
                    reobserved_after_apply_error = True
                except Exception:
                    json.dump({
                        "applied": None, "replay": replay,
                        "event_id": candidate_id, "remote_slot_id": expected_slot,
                        "receipt": None,
                        "recovery_state": "outcome_unknown_replay_required",
                        "postwrite_validation": {
                            "repair_reducer": "not_observed",
                            "production_compact_resume": "external_gate_required",
                            "production_full_resume": "external_gate_required",
                            "strict_generation_candidate": "external_gate_required",
                            "exact_manifest_replay": "required",
                        },
                        "error": str(apply_error),
                    }, sys.stdout, sort_keys=True, indent=2)
                    sys.stdout.write("\n")
                    return 3
            if replay:
                json.dump({
                    "applied": True, "replay": True,
                    "event_id": candidate_id,
                    "expected_revision": base_revision,
                    "remote_slot_id": expected_slot,
                    "raw_frontier": material_frontier(raw),
                    "repair_count": len(validated.repair_bindings),
                    "receipt": receipt.__dict__,
                    "recovery_state": "complete",
                    "postwrite_validation": {
                        "repair_reducer": "valid_historical_proof",
                        "production_compact_resume": "external_gate_required",
                        "production_full_resume": "external_gate_required",
                        "strict_generation_candidate": "external_gate_required",
                        "exact_manifest_replay": "valid",
                    },
                    "mutation_atomicity": {
                        "material_checkpoint_boundary": "deterministic_remote_slot",
                        "cross_surface": "historical_proof_replay_no_write",
                    },
                }, sys.stdout, sort_keys=True, indent=2)
                sys.stdout.write("\n")
                return 0
            try:
                post_route = resolve_authenticated_issue_route(
                    client, args.workstream, declared,
                )
                post_source = canonical_authenticated_source(plan_payload(
                    args.plan_source,
                    args.plan_identity or source.get("identity"),
                )["source"])
                post_comments = adapter.comments()
                post_raw = reduce_event_comments(
                    post_comments, workstream_id=args.workstream,
                )
                assert_exact_pinned_repair_comment(
                    post_comments, candidate, remote_slot_id=expected_slot,
                    comment_body_sha256=control["comment_body_sha256"],
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
                    ).snapshot_for_root(
                        args.workstream, include_child_comments=False,
                        include_description=True,
                    ),
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
                    "recovery_state": "durable_partial_replay_required",
                    "postwrite_validation": {
                        "repair_reducer": "failed",
                        "production_compact_resume": "external_gate_required",
                        "production_full_resume": "external_gate_required",
                        "strict_generation_candidate": "external_gate_required",
                        "exact_manifest_replay": "required",
                    },
                    "error": str(post_error),
                }, sys.stdout, sort_keys=True, indent=2)
                sys.stdout.write("\n")
                return 3
            if reobserved_after_apply_error:
                json.dump({
                    "applied": True, "replay": replay,
                    "event_id": candidate_id, "remote_slot_id": expected_slot,
                    "receipt": receipt.__dict__,
                    "recovery_state": "durable_partial_replay_required",
                    "postwrite_validation": {
                        "repair_reducer": "valid",
                        "production_compact_resume": "external_gate_required",
                        "production_full_resume": "external_gate_required",
                        "strict_generation_candidate": "external_gate_required",
                        "exact_manifest_replay": "required",
                    },
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
            "recovery_state": "complete" if args.apply else "preview_only",
            "postwrite_validation": {
                "repair_reducer": "valid" if args.apply else "preview_valid",
                "production_compact_resume": "external_gate_required",
                "production_full_resume": "external_gate_required",
                "strict_generation_candidate": "external_gate_required",
                "exact_manifest_replay": (
                    "valid" if args.apply and replay else "external_gate_required"
                ),
            },
            "mutation_atomicity": {
                "material_checkpoint_boundary": "deterministic_remote_slot",
                "cross_surface": "preflight_and_postcheck_non_transactional",
            },
        }
        json.dump(output, sys.stdout, sort_keys=True, indent=2)
        sys.stdout.write("\n")
        return 0
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        print(f"material repair refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
