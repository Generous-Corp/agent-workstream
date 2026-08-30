#!/usr/bin/env python3
from __future__ import annotations

import base64
import copy
import hashlib
import threading
import tempfile
import unittest
import json
from dataclasses import replace
from pathlib import Path
from unittest import mock

import workstream_linear_events as linear_events_module
from workstream_delta import Delta, DeltaJournal
from workstream_delta import RevisionConflict
from workstream_linear import LinearTransportError
from workstream_checkpoint import build_checkpoint
from workstream_linear_checkpoints import encode_checkpoint_comment
from workstream_linear_events import (
    EVENT_PREFIX,
    LinearCommentEventAdapter,
    LinearEventError,
    apply_material_semantic_repairs,
    encode_event_comment,
    encode_reviewed_repair_comment,
    ledger_boundary_slot_id,
    material_frontier,
    reduce_event_comments,
)
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, projection_slot_id,
)


def delta(event_id: str, payload: dict, expected_revision: int = 0) -> Delta:
    return Delta(
        event_id, "GEN-37", "requirement", "agent_discovery", payload,
        expected_revision, f"2026-08-20T00:00:0{payload.get('order', 0)}Z",
    )


class FakeCommentClient:
    """Thread-safe Linear fake; an optional barrier aligns initial reads."""

    def __init__(self, initial_readers: int = 0):
        self.comments: list[dict] = []
        self.calls: list[tuple[str, dict]] = []
        self.lock = threading.Lock()
        self.initial_barrier = (
            threading.Barrier(initial_readers) if initial_readers else None
        )
        self.initial_reads = 0
        self.workspace_id = "workspace"
        self.team_id = "team"
        self.project_id = "project"
        self.root_issue_id = "issue-37"

    def execute(self, query, variables):
        with self.lock:
            self.calls.append((query, variables))
        if "query WorkstreamDeltaComments" in query:
            wait = False
            with self.lock:
                if self.initial_barrier and self.initial_reads < self.initial_barrier.parties:
                    self.initial_reads += 1
                    wait = True
            if wait:
                self.initial_barrier.wait(timeout=2)
            with self.lock:
                nodes = [dict(comment) for comment in self.comments]
            return {
                "issue": {
                    "id": self.root_issue_id, "identifier": "GEN-37",
                    "team": {"id": self.team_id,
                             "organization": {"id": self.workspace_id}},
                    "project": {"id": self.project_id},
                    "comments": {
                        "nodes": nodes,
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                }
            }
        if "WorkstreamEventCommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}, {"name": "body"}]}}
        if "commentCreate" in query:
            with self.lock:
                comment_id = variables["input"]["id"]
                if any(item["id"] == comment_id for item in self.comments):
                    raise LinearTransportError("duplicate comment id")
                comment = {
                    "id": comment_id,
                    "body": variables["input"]["body"],
                    "createdAt": "now", "updatedAt": "now",
                }
                self.comments.append(comment)
            return {"commentCreate": {"success": True, "comment": dict(comment)}}
        raise AssertionError("unexpected GraphQL operation")


class LinearCommentEventAdapterTests(unittest.TestCase):
    def _repair_fixture(self):
        invalid = [
            Delta("flat-a", "GEN-37", "material_boundary", "system",
                  {"progress": "one", "next_action": "old"}, 0,
                  "2026-08-30T00:00:00Z"),
            Delta("flat-b", "GEN-37", "material_boundary", "system",
                  {"progress": "two", "next_action": "new"}, 1,
                  "2026-08-30T00:00:01Z"),
        ]
        # Historical construction bypasses the strict encoder on purpose.
        comments = []
        for index, event in enumerate(invalid):
            canonical = linear_events_module._canonical_event(event)
            import base64, json, hashlib
            encoded = base64.urlsafe_b64encode(json.dumps(
                canonical, sort_keys=True, separators=(",", ":"),
            ).encode()).decode().rstrip("=")
            comments.append({"id": f"remote-{index}", "body":
                             f"{linear_events_module.EVENT_PREFIX}{encoded} -->"})
        raw = reduce_event_comments(comments, workstream_id="GEN-37")
        route = {"workspace_id": "workspace", "team_id": "team",
                 "project_id": "project",
                 "root_issue_id": "33333333-3333-4333-8333-333333333333"}
        repair_slot = ledger_boundary_slot_id("GEN-37", 2, [], route)
        source = {"identity": "plan", "sha256": "a" * 64}
        checkpoint = {"count": 0, "revision": 0, "event_ids_sha256": "c",
                      "checkpoints_sha256": "d"}
        projection_source = build_projection_event(
            workstream_id="GEN-37", kind="source", key="root", value=source,
            plan_revision="a" * 64, expected_revision=0,
            created_at="2026-08-30T00:00:02Z", authority=route,
        )
        scope = {
            "namespace": "repair-test",
            "linear": {**route, "route_verification": {
                **route, "observed_at": "2026-08-30T00:00:00Z",
                "evidence": [{"kind": "authenticated_linear_readback",
                              "authenticated": True, **route}],
            }},
            "primary_repository": "github.com:id:R_test",
            "repositories": [{
                "slug": "github.com/generous-corp/test", "exact_head": "1" * 40,
                "provider_repository_id": "R_test", "aliases": [],
                "identity_resolution": {
                    "provider_repository_id": "R_test",
                    "resolved_slug": "github.com/generous-corp/test",
                    "observed_at": "2026-08-30T00:00:00Z",
                    "evidence": [{"kind": "authenticated_provider_readback",
                                  "authenticated": True,
                                  "provider_repository_id": "R_test",
                                  "resolved_slug": "github.com/generous-corp/test"}],
                },
                "identity_updates": [], "evidence": [],
            }],
            "child_ownership": {"GEN-38": "github.com:id:R_test",
                                "GEN-39": "github.com:id:R_test"},
        }
        projection_scope = build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root", value=scope,
            plan_revision="a" * 64, expected_revision=1,
            created_at="2026-08-30T00:00:03Z", authority=route,
        )
        projection_provenance = build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="repair-seal",
            value={"agent": "reviewer", "machine": "test",
                   "session_id": "repair-seal"}, plan_revision="a" * 64,
            expected_revision=2, created_at="2026-08-30T00:00:04Z",
            authority=route,
        )
        projection_seal = build_projection_event(
            workstream_id="GEN-37", kind="disposition", key="root",
            value={"disposition": "attach", "remote_head": "1" * 40,
                   "recovered_from_checkpoint": None},
            plan_revision="a" * 64, expected_revision=3,
            created_at="2026-08-30T00:00:05Z", authority=route,
        )
        projection = {"revision": 4,
                      "frontier_event_id": projection_seal["event_id"],
                      "events_sha256": "e"}
        generation = {"plan_revision": "a" * 64,
                      "transition_tip_event_id": None, "activation_epoch": None,
                      "authority_origin": "legacy_description"}
        graph = {"algorithm": "authenticated-root-children-relations-v1",
                 "sha256": "9" * 64}
        source_body = encode_projection_comment(projection_source)
        scope_body = encode_projection_comment(projection_scope)
        provenance_body = encode_projection_comment(projection_provenance)
        seal_body = encode_projection_comment(projection_seal)
        source_remote = projection_slot_id("GEN-37", "a" * 64, 0, route)
        scope_remote = projection_slot_id("GEN-37", "a" * 64, 1, route)
        provenance_remote = projection_slot_id("GEN-37", "a" * 64, 2, route)
        seal_remote = projection_slot_id("GEN-37", "a" * 64, 3, route)
        comments.extend([
            {"id": source_remote, "body": source_body},
            {"id": scope_remote, "body": scope_body},
            {"id": provenance_remote, "body": provenance_body},
            {"id": seal_remote, "body": seal_body},
        ])
        bindings = []
        for position, event in enumerate(invalid):
            body = comments[position]["body"]
            bindings.append({
                "event_id": event.event_id,
                "remote_comment_id": comments[position]["id"],
                "comment_body_sha256": hashlib.sha256(body.encode()).hexdigest(),
                "canonical_event_sha256": linear_events_module._canonical_event(event)["sha256"],
                "payload_sha256": linear_events_module.canonical_sha256(event.payload),
                "original_expected_revision": event.expected_revision,
                "original_index_zero_based": position,
                "original_applied_revision": position + 1,
                "replacement": {"boundary_id": f"repair:{event.event_id}", "changes": [{
                    "kind": "progress", "payload": dict(event.payload),
                }]},
            })
        fences = {
            "checkpoint_frontier": checkpoint, "projection_frontier": projection,
            "generation": generation, "authenticated_route": route,
            "authenticated_source": source, "issue_graph_frontier": graph,
        }
        oracle = {
            "schema_version": 1, "target_binding_count": 2,
            "target_bindings_sha256": linear_events_module.canonical_sha256(bindings),
            "strict_target_candidate_sha256": "2" * 64,
            "source_identity": source["identity"], "source_sha256": source["sha256"],
            "source_event_id": projection_source["event_id"],
            "source_remote_comment_id": source_remote,
            "source_comment_body_sha256": hashlib.sha256(
                source_body.encode()
            ).hexdigest(),
            "source_event_sha256": linear_events_module.canonical_sha256(
                projection_source
            ),
            "projection_seal_event_id": projection["frontier_event_id"],
            "projection_seal_remote_comment_id": seal_remote,
            "projection_seal_comment_body_sha256": hashlib.sha256(
                seal_body.encode()
            ).hexdigest(),
            "projection_seal_event_sha256": linear_events_module.canonical_sha256(
                projection_seal
            ),
            "generation_tip_event_id": generation["transition_tip_event_id"],
            "fences_sha256": linear_events_module.canonical_sha256(fences),
        }
        payload = {
            "schema_version": 1, "workstream_id": "GEN-37",
            "target_bindings": bindings, "raw_frontier": material_frontier(raw),
            "checkpoint_frontier": checkpoint, "projection_frontier": projection,
            "generation": generation, "authenticated_route": route,
            "authenticated_source": source,
            "issue_graph_frontier": graph,
            "ledger_serialization_frontier": [],
            "postwrite_oracle": oracle,
            "review_artifact": {
                "identity": "https://github.com/review/repo/blob/" + "1" * 40 + "/repair.json",
                "repository": "github.com/review/repo", "commit": "1" * 40,
                "path": "repair.json", "sha256": "f" * 64,
                "reviewed_at": "2026-08-30T00:01:00Z",
            },
        }
        control = Delta("repair", "GEN-37", "material_semantic_repair", "system",
                        payload, 2, "2026-08-30T00:01:00Z")
        comments.append({"id": repair_slot, "body": encode_reviewed_repair_comment(control)})
        return comments, payload, checkpoint, projection, generation, route, source, graph

    def test_two_pass_repair_preserves_raw_positions_and_overlays_boundaries(self):
        comments, payload, checkpoint, projection, generation, route, source, graph = self._repair_fixture()
        raw = reduce_event_comments(comments, workstream_id="GEN-37")
        effective = apply_material_semantic_repairs(
            raw, comments, checkpoint_frontier=checkpoint,
            projection_frontier=projection, generation=generation,
            authenticated_route=route,
            authenticated_source={**source, "bytes": 65753},
            issue_graph_frontier=graph,
            ledger_serialization_frontier_value=[],
        )
        self.assertEqual(effective.revision, 3)
        self.assertEqual([e.event_id for e in effective.raw_events],
                         ["flat-a", "flat-b", "repair"])
        self.assertEqual([e.event_id for e in effective.events],
                         ["flat-a", "flat-b", "repair"])
        self.assertEqual(effective.events[1].payload["changes"][0]["payload"]["next_action"], "new")
        self.assertEqual(len(effective.repair_bindings), 2)

    def test_historical_repair_proof_allows_authorized_successor_evolution(self):
        comments, _payload, checkpoint, projection, generation, route, source, graph = self._repair_fixture()
        raw = reduce_event_comments(comments, workstream_id="GEN-37")
        evolved_route = dict(route, team_id="new-team", project_id="new-project")
        effective = apply_material_semantic_repairs(
            raw, comments,
            checkpoint_frontier=dict(checkpoint, revision=99),
            projection_frontier=dict(projection, revision=99),
            generation=dict(generation, plan_revision="b" * 64,
                            transition_tip_event_id="successor"),
            authenticated_route=evolved_route,
            authenticated_source={"identity": "successor", "sha256": "b" * 64},
            issue_graph_frontier={"algorithm": "successor", "sha256": "8" * 64},
            ledger_serialization_frontier_value=["later-checkpoint"],
            validate_live_fences=False,
        )
        self.assertEqual(len(effective.repair_bindings), 2)
        with self.assertRaisesRegex(
            LinearEventError, "material_semantic_repair_.*_drift",
        ):
            apply_material_semantic_repairs(
                raw, comments, checkpoint_frontier=checkpoint,
                projection_frontier=projection, generation=generation,
                authenticated_route=evolved_route,
                authenticated_source=source, issue_graph_frontier=graph,
                ledger_serialization_frontier_value=[], validate_live_fences=True,
            )

    def test_repair_rejects_semantic_rewrite_and_noncanonical_boundary_id(self):
        for mutation in ("payload", "boundary"):
            comments, payload, checkpoint, projection, generation, route, source, graph = self._repair_fixture()
            changed = json.loads(json.dumps(payload))
            if mutation == "payload":
                changed["target_bindings"][0]["replacement"]["changes"][0]["payload"] = {
                    "progress": "edited"
                }
            else:
                changed["target_bindings"][0]["replacement"]["boundary_id"] = "reviewer-choice"
            changed["postwrite_oracle"]["target_bindings_sha256"] = (
                linear_events_module.canonical_sha256(changed["target_bindings"])
            )
            control = Delta(
                "repair", "GEN-37", "material_semantic_repair", "system",
                changed, 2, "2026-08-30T00:01:00Z",
            )
            comments[-1] = {"id": comments[-1]["id"],
                            "body": encode_reviewed_repair_comment(control)}
            with self.subTest(mutation=mutation), self.assertRaisesRegex(
                LinearEventError, "non_lossless_replacement",
            ):
                apply_material_semantic_repairs(
                    reduce_event_comments(comments, workstream_id="GEN-37"), comments,
                    checkpoint_frontier=checkpoint, projection_frontier=projection,
                    generation=generation, authenticated_route=route,
                    authenticated_source=source, issue_graph_frontier=graph,
                    ledger_serialization_frontier_value=[],
                )

    def test_repair_refuses_incomplete_target_set_at_intended_branch(self):
        comments, payload, checkpoint, projection, generation, route, source, graph = self._repair_fixture()
        third = Delta(
            "flat-c", "GEN-37", "material_boundary", "system",
            {"progress": "third malformed"}, 2, "2026-08-30T00:00:02Z",
        )
        canonical = linear_events_module._canonical_event(third)
        encoded = base64.urlsafe_b64encode(json.dumps(
            canonical, sort_keys=True, separators=(",", ":"),
        ).encode()).decode().rstrip("=")
        prefix = [*comments[:-1], {
            "id": "remote-third", "body": f"{EVENT_PREFIX}{encoded} -->",
        }]
        changed = json.loads(json.dumps(payload))
        changed["raw_frontier"] = material_frontier(
            reduce_event_comments(prefix, workstream_id="GEN-37")
        )
        changed["postwrite_oracle"]["target_binding_count"] = 2
        changed["postwrite_oracle"]["target_bindings_sha256"] = (
            linear_events_module.canonical_sha256(changed["target_bindings"])
        )
        control = Delta(
            "repair", "GEN-37", "material_semantic_repair", "system",
            changed, 3, "2026-08-30T00:01:00Z",
        )
        candidate = [*prefix, {
            "id": ledger_boundary_slot_id("GEN-37", 3, [], route),
            "body": encode_reviewed_repair_comment(control),
        }]
        with self.assertRaisesRegex(
            LinearEventError, "material_semantic_repair_incomplete_target_set",
        ):
            apply_material_semantic_repairs(
                reduce_event_comments(candidate, workstream_id="GEN-37"), candidate,
                checkpoint_frontier=checkpoint, projection_frontier=projection,
                generation=generation, authenticated_route=route,
                authenticated_source=source, issue_graph_frontier=graph,
                ledger_serialization_frontier_value=[],
            )

    def test_repair_target_validator_refuses_exact_duplicate_unknown_valid_and_drift_branches(self):
        for mutation, reason in (
            ("duplicate", "duplicate_material_semantic_repair_target"),
            ("unknown", "material_semantic_repair_forward_or_unknown_target"),
            ("valid", "material_semantic_repair_valid_target"),
            ("drift", "material_semantic_repair_target_drift"),
        ):
            comments, payload, checkpoint, projection, generation, route, source, graph = self._repair_fixture()
            changed = json.loads(json.dumps(payload))
            prefix = comments[:-1]
            expected_revision = 2
            if mutation == "duplicate":
                changed["target_bindings"][1] = copy.deepcopy(
                    changed["target_bindings"][0]
                )
            elif mutation == "unknown":
                changed["target_bindings"][1]["event_id"] = "unknown-target"
            elif mutation == "drift":
                changed["target_bindings"][1]["remote_comment_id"] = "wrong-remote"
            else:
                valid = Delta(
                    "valid-boundary", "GEN-37", "material_boundary", "system",
                    {"boundary_id": "valid", "changes": [{
                        "kind": "progress", "payload": {"progress": "valid"},
                    }]}, 2, "2026-08-30T00:00:02Z",
                )
                prefix = [*prefix, {
                    "id": "remote-valid", "body": encode_event_comment(valid),
                }]
                expected_revision = 3
                changed["raw_frontier"] = material_frontier(
                    reduce_event_comments(prefix, workstream_id="GEN-37")
                )
                changed["target_bindings"][1]["event_id"] = valid.event_id
            changed["postwrite_oracle"]["target_bindings_sha256"] = (
                linear_events_module.canonical_sha256(changed["target_bindings"])
            )
            control = Delta(
                "repair", "GEN-37", "material_semantic_repair", "system",
                changed, expected_revision, "2026-08-30T00:01:00Z",
            )
            candidate = [*prefix, {
                "id": ledger_boundary_slot_id(
                    "GEN-37", expected_revision, [], route,
                ),
                "body": encode_reviewed_repair_comment(control),
            }]
            with self.subTest(mutation=mutation), self.assertRaisesRegex(
                LinearEventError, reason,
            ):
                apply_material_semantic_repairs(
                    reduce_event_comments(candidate, workstream_id="GEN-37"),
                    candidate, checkpoint_frontier=checkpoint,
                    projection_frontier=projection, generation=generation,
                    authenticated_route=route, authenticated_source=source,
                    issue_graph_frontier=graph,
                    ledger_serialization_frontier_value=[],
                )

    def test_postwrite_oracle_each_bound_proof_refuses_at_exact_branch(self):
        mutations = {
            "source_identity": "material_semantic_repair_postwrite_oracle_source_identity_drift",
            "source_sha256": "material_semantic_repair_postwrite_oracle_source_sha256_drift",
            "source_event_id": "material_semantic_repair_postwrite_oracle_source_event_id_drift",
            "source_remote_comment_id": "material_semantic_repair_postwrite_oracle_source_remote_comment_id_drift",
            "source_comment_body_sha256": "material_semantic_repair_postwrite_oracle_source_comment_body_sha256_drift",
            "source_event_sha256": "material_semantic_repair_postwrite_oracle_source_event_sha256_drift",
            "projection_seal_event_id": "material_semantic_repair_postwrite_oracle_projection_seal_event_id_drift",
            "projection_seal_remote_comment_id": "material_semantic_repair_postwrite_oracle_projection_seal_remote_comment_id_drift",
            "projection_seal_comment_body_sha256": "material_semantic_repair_postwrite_oracle_projection_seal_comment_body_sha256_drift",
            "projection_seal_event_sha256": "material_semantic_repair_postwrite_oracle_projection_seal_event_sha256_drift",
            "fences_sha256": "material_semantic_repair_postwrite_oracle_fences_sha256_drift",
            "generation_tip_event_id": "material_semantic_repair_postwrite_oracle_generation_tip_event_id_drift",
        }
        for field, reason in mutations.items():
            comments, payload, checkpoint, projection, generation, route, source, graph = self._repair_fixture()
            changed = json.loads(json.dumps(payload))
            changed["postwrite_oracle"][field] = (
                "missing-remote" if "remote_comment_id" in field
                else "0" * 64 if field.endswith("sha256")
                else "wrong-event"
            )
            control = Delta(
                "repair", "GEN-37", "material_semantic_repair", "system",
                changed, 2, "2026-08-30T00:01:00Z",
            )
            candidate = [*comments[:-1], {
                "id": comments[-1]["id"],
                "body": encode_reviewed_repair_comment(control),
            }]
            with self.subTest(field=field), self.assertRaisesRegex(
                LinearEventError, reason,
            ):
                apply_material_semantic_repairs(
                    reduce_event_comments(candidate, workstream_id="GEN-37"),
                    candidate, checkpoint_frontier=checkpoint,
                    projection_frontier=projection, generation=generation,
                    authenticated_route=route, authenticated_source=source,
                    issue_graph_frontier=graph,
                    ledger_serialization_frontier_value=[],
                )

    def test_repair_reducer_rejects_noncanonical_review_artifact_identity(self):
        comments, payload, checkpoint, projection, generation, route, source, graph = self._repair_fixture()
        changed = json.loads(json.dumps(payload))
        changed["review_artifact"].update({
            "repository": "evil.example/review/repo",
            "identity": (
                "https://evil.example/review/repo/blob/" + "1" * 40
                + "/repair.json"
            ),
        })
        control = Delta(
            "repair", "GEN-37", "material_semantic_repair", "system",
            changed, 2, "2026-08-30T00:01:00Z",
        )
        candidate = [*comments[:-1], {
            "id": comments[-1]["id"],
            "body": encode_reviewed_repair_comment(control),
        }]
        with self.assertRaisesRegex(
            LinearEventError, "malformed_material_semantic_repair_artifact",
        ):
            apply_material_semantic_repairs(
                reduce_event_comments(candidate, workstream_id="GEN-37"),
                candidate, checkpoint_frontier=checkpoint,
                projection_frontier=projection, generation=generation,
                authenticated_route=route, authenticated_source=source,
                issue_graph_frontier=graph,
                ledger_serialization_frontier_value=[],
            )

    def test_repair_refuses_live_frontier_drift(self):
        cases = []
        comments, payload, checkpoint, projection, generation, route, source, graph = self._repair_fixture()
        drift = dict(checkpoint); drift["revision"] = 1
        cases.append((comments, drift, "checkpoint_frontier_drift"))
        for candidate, checkpoint_value, reason in cases:
            with self.subTest(reason=reason), self.assertRaisesRegex(LinearEventError, reason):
                apply_material_semantic_repairs(
                    reduce_event_comments(candidate, workstream_id="GEN-37"), candidate,
                    checkpoint_frontier=checkpoint_value,
                    projection_frontier=projection, generation=generation,
                    authenticated_route=route, authenticated_source=source,
                    issue_graph_frontier=graph,
                    ledger_serialization_frontier_value=[],
                )

        drift_inputs = {
            "projection_frontier": (dict(projection, revision=3),
                                    "projection_frontier_drift"),
            "generation": (dict(generation, activation_epoch="other"),
                           "generation_drift"),
            "authenticated_route": (dict(route, project_id="other"),
                                    "authenticated_route_drift"),
            "authenticated_source": (dict(source, sha256="b" * 64),
                                     "authenticated_source_drift"),
            "issue_graph_frontier": (dict(graph, sha256="8" * 64),
                                    "issue_graph_frontier_drift"),
        }
        baseline = {
            "checkpoint_frontier": checkpoint,
            "projection_frontier": projection, "generation": generation,
            "authenticated_route": route, "authenticated_source": source,
            "issue_graph_frontier": graph,
            "ledger_serialization_frontier_value": [],
        }
        for field, (value, reason) in drift_inputs.items():
            kwargs = dict(baseline); kwargs[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(LinearEventError, reason):
                apply_material_semantic_repairs(
                    reduce_event_comments(comments, workstream_id="GEN-37"),
                    comments, **kwargs,
                )

    def test_future_invalid_boundary_refuses_encoder_and_remote_preflight(self):
        bad = Delta("bad", "GEN-37", "material_boundary", "system",
                    {"progress": "flat"}, 0, "2026-08-30T00:00:00Z")
        with self.assertRaisesRegex(ValueError, "malformed_material_boundary"):
            encode_event_comment(bad)
        client = FakeCommentClient()
        with self.assertRaisesRegex(LinearEventError, "malformed_material_boundary"):
            LinearCommentEventAdapter(client, issue_id="GEN-37").apply(bad)
        self.assertFalse(any("commentCreate" in query for query, _ in client.calls))

    def test_repair_control_is_reserved_from_generic_encoder_and_adapter(self):
        comments, payload, *_ = self._repair_fixture()
        control = reduce_event_comments(
            comments, workstream_id="GEN-37",
        ).events[-1]
        with self.assertRaisesRegex(ValueError, "material_semantic_repair_reserved"):
            encode_event_comment(control)
        client = FakeCommentClient()
        with self.assertRaisesRegex(
            LinearEventError, "material_semantic_repair_reserved",
        ):
            LinearCommentEventAdapter(client, issue_id="GEN-37").apply(control)
        self.assertFalse(any("commentCreate" in query for query, _ in client.calls))

    def test_pinned_repair_runs_full_two_pass_validation_before_write(self):
        comments, _payload, *_rest, route, _source, _graph = self._repair_fixture()
        control = reduce_event_comments(
            comments, workstream_id="GEN-37",
        ).events[-1]
        payload = json.loads(json.dumps(control.payload))
        payload["target_bindings"][0]["replacement"]["boundary_id"] = "rewrite"
        payload["postwrite_oracle"]["target_bindings_sha256"] = (
            linear_events_module.canonical_sha256(payload["target_bindings"])
        )
        candidate = replace(control, payload=payload)
        client = FakeCommentClient()
        client.comments = comments[:-1]
        client.workspace_id = route["workspace_id"]
        client.team_id = route["team_id"]
        client.project_id = route["project_id"]
        client.root_issue_id = route["root_issue_id"]
        with self.assertRaisesRegex(
            LinearEventError, "pinned_repair_full_validation_failed",
        ):
            LinearCommentEventAdapter(
                client, issue_id="GEN-37", plan_revision="a" * 64, **route,
            ).apply_pinned_repair(
                candidate, expected_remote_slot=comments[-1]["id"],
                expected_serialization_frontier=[],
                expected_comment_body_sha256=hashlib.sha256(
                    encode_reviewed_repair_comment(candidate).encode()
                ).hexdigest(),
            )
        self.assertFalse(any("commentCreate" in query for query, _ in client.calls))

    def test_pinned_repair_existing_receipt_requires_exact_complete_remote_body(self):
        comments, _payload, *_rest, route, _source, _graph = self._repair_fixture()
        control = reduce_event_comments(
            comments, workstream_id="GEN-37",
        ).events[-1]
        expected_body = encode_reviewed_repair_comment(control)
        client = FakeCommentClient()
        client.comments = copy.deepcopy(comments)
        client.comments[-1]["body"] = "review prose\n" + expected_body
        client.workspace_id = route["workspace_id"]
        client.team_id = route["team_id"]
        client.project_id = route["project_id"]
        client.root_issue_id = route["root_issue_id"]
        with self.assertRaisesRegex(
            LinearEventError, "material_repair_pinned_comment_body_mismatch",
        ):
            LinearCommentEventAdapter(
                client, issue_id="GEN-37", plan_revision="a" * 64, **route,
            ).apply_pinned_repair(
                control, expected_remote_slot=comments[-1]["id"],
                expected_serialization_frontier=[],
                expected_comment_body_sha256=hashlib.sha256(
                    expected_body.encode()
                ).hexdigest(),
            )
        self.assertFalse(any("commentCreate" in query for query, _ in client.calls))

    def test_historical_invalid_exact_replay_is_receipt_only(self):
        comments, *_ = self._repair_fixture()
        client = FakeCommentClient()
        client.comments = [comments[0]]
        raw = reduce_event_comments(client.comments, workstream_id="GEN-37")
        replay = LinearCommentEventAdapter(client, issue_id="GEN-37").apply(raw.events[0])
        self.assertEqual(replay.remote_id, "remote-0")
        self.assertEqual(replay.revision, 1)
        self.assertFalse(any("commentCreate" in query for query, _ in client.calls))

    def test_concurrent_same_revision_has_one_durable_winner(self):
        client = FakeCommentClient(initial_readers=2)
        adapters = [
            LinearCommentEventAdapter(client, issue_id="GEN-37"),
            LinearCommentEventAdapter(client, issue_id="GEN-37"),
        ]
        deltas = [delta("event-a", {"order": 1}), delta("event-b", {"order": 2})]
        receipts = []
        failures = []

        def apply(index):
            try:
                receipts.append(adapters[index].apply(deltas[index]))
            except Exception as exc:  # captured so the main thread can assert it
                failures.append(exc)

        threads = [threading.Thread(target=apply, args=(index,)) for index in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertEqual(len(receipts), 1)
        self.assertEqual(len(failures), 1)
        self.assertTrue(
            isinstance(failures[0], RevisionConflict)
            or "expected revision 0, live revision 1" in str(failures[0])
            or "event_slot_lost_reload_required" in str(failures[0])
        )
        state = reduce_event_comments(client.comments, workstream_id="GEN-37")
        self.assertEqual(state.revision, 1)
        self.assertEqual({event.event_id for event in state.events}, {
            receipts[0].event_id
        })

    def test_stale_revision_refuses_before_a_second_comment(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        self.assertFalse(adapter.supports_atomic_cas)
        self.assertTrue(adapter.supports_append_only_events)
        first = DeltaJournal(":memory:")
        second = DeltaJournal(":memory:")
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        first.append("GEN-37", "requirement", {"text": "A"}, 0, event_id="event-a")
        second.append("GEN-37", "decision", {"text": "B"}, 0, event_id="event-b")

        first.apply(adapter)
        with self.assertRaisesRegex(RuntimeError, "expected revision 0, live revision 1"):
            second.apply(adapter)

        self.assertEqual(adapter.current_revision("GEN-37"), 1)
        self.assertEqual(first.pending(), [])
        self.assertEqual(len(second.pending()), 1)

    def test_crash_replay_returns_existing_event_without_second_comment(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        event = delta("event-a", {"order": 1})
        first = adapter.apply(event)
        replay = adapter.apply(event)
        self.assertEqual(first, replay)
        self.assertEqual(len(client.comments), 1)

    def test_replay_after_later_event_returns_own_stable_revision(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        first = delta("event-a", {"order": 1})
        adapter.apply(first)
        adapter.apply(delta("event-b", {"order": 2}, expected_revision=1))

        replay = adapter.apply(first)

        self.assertEqual(replay.revision, 1)
        self.assertEqual(replay.remote_id, client.comments[0]["id"])
        self.assertEqual(len(client.comments), 2)

    def test_crash_after_rebased_remote_accept_replays_from_fresh_journal(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        adapter.apply(delta("event-a", {"order": 1}))

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            journal = DeltaJournal(path)
            event_id = journal.append(
                "GEN-37", "requirement", {"order": 2}, 0,
                event_id="event-b", source="agent_discovery",
            )
            crashed = False

            def crash_after_remote_accept():
                nonlocal crashed
                if not crashed:
                    crashed = True
                    raise RuntimeError("crash after rebased remote accept")

            journal.commit_hook = crash_after_remote_accept
            with mock.patch.object(
                linear_events_module, "RevisionConflict", RevisionConflict
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "crash after rebased remote accept"
                ):
                    journal.apply_with_rebase(adapter)
            journal.commit_hook = None
            journal.close()

            remote = reduce_event_comments(
                client.comments, workstream_id="GEN-37"
            )
            rebased = next(
                event for event in remote.events if event.event_id == event_id
            )
            self.assertEqual(rebased.expected_revision, 1)

            fresh = DeltaJournal(path)
            self.addCleanup(fresh.close)
            self.assertEqual(fresh.pending()[0].expected_revision, 0)
            receipts = fresh.apply_with_rebase(adapter)

            self.assertEqual(receipts[0].event_id, event_id)
            self.assertEqual(receipts[0].revision, 2)
            self.assertEqual(fresh.pending(), [])
            self.assertEqual(len(client.comments), 2)

    def test_rebased_replay_rejects_reverse_revision_and_material_mismatch(self):
        client = FakeCommentClient()
        first = delta("event-a", {"order": 1})
        remote = delta("event-b", {"order": 2}, expected_revision=1)
        client.comments.extend([
            {"id": "legacy-1", "body": encode_event_comment(first)},
            {"id": "legacy-2", "body": encode_event_comment(remote)},
        ])
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        original = replace(remote, expected_revision=0)
        self.assertEqual(adapter.apply(original).revision, 2)

        mismatches = [
            replace(original, expected_revision=2),
            replace(original, payload={"order": 99}),
            replace(original, kind="decision"),
            replace(original, source="user_turn"),
            replace(original, created_at="2026-08-20T00:01:00Z"),
        ]
        for mismatch in mismatches:
            with self.subTest(mismatch=mismatch):
                with self.assertRaisesRegex(
                    LinearEventError, "conflicting_event_id:event-b"
                ):
                    adapter.apply(mismatch)
        with self.assertRaisesRegex(LinearEventError, "workstream_id_mismatch"):
            adapter.apply(replace(original, workstream_id="OPS-9"))

    def test_legacy_arbitrary_comment_id_history_remains_compatible(self):
        client = FakeCommentClient()
        first = delta("event-a", {"order": 1})
        client.comments.append({
            "id": "legacy-arbitrary-comment-id",
            "body": encode_event_comment(first),
            "createdAt": "then", "updatedAt": "then",
        })
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")

        replay = adapter.apply(first)
        second = adapter.apply(
            delta("event-b", {"order": 2}, expected_revision=1)
        )

        self.assertEqual(replay.remote_id, "legacy-arbitrary-comment-id")
        self.assertEqual(replay.revision, 1)
        self.assertEqual(second.revision, 2)
        self.assertEqual(len(client.comments), 2)

    def test_lost_create_response_reloads_exact_slot_as_replay(self):
        class LostResponseClient(FakeCommentClient):
            lost = False

            def execute(self, query, variables):
                if "commentCreate" in query and not self.lost:
                    self.lost = True
                    super().execute(query, variables)
                    raise LinearTransportError("response lost")
                return super().execute(query, variables)

        client = LostResponseClient()
        receipt = LinearCommentEventAdapter(client, issue_id="GEN-37").apply(
            delta("event-a", {"order": 1})
        )
        self.assertEqual(receipt.event_id, "event-a")
        self.assertEqual(receipt.remote_id, client.comments[0]["id"])
        self.assertEqual(len(client.comments), 1)

    def test_lost_event_response_refuses_concurrent_broken_stale_generation(self):
        broken = build_checkpoint(
            workstream_id="GEN-37", boundary_id="broken-stale",
            root_revision=1, plan_revision="old-plan",
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "old",
                "machine": "m1", "worktree": {
                    "state": "safe", "path": "/repo", "branch": "old",
                    "head": "head-old",
                },
            }, exact_head="head-old", evidence=[], blocker=None,
            next_action="old", predecessor_event_id="wsc_missing",
        )

        class LostResponseWithBrokenGeneration(FakeCommentClient):
            lost = False

            def execute(self, query, variables):
                if "commentCreate" in query and not self.lost:
                    self.lost = True
                    super().execute(query, variables)
                    self.comments.append({
                        "id": "arbitrary-broken-stale-id",
                        "body": encode_checkpoint_comment(broken),
                        "createdAt": "now", "updatedAt": "now",
                    })
                    raise LinearTransportError("response lost")
                return super().execute(query, variables)

        client = LostResponseWithBrokenGeneration()
        client.comments.append({
            "id": "legacy-material-1",
            "body": encode_event_comment(delta("event-1", {"order": 1})),
            "createdAt": "then", "updatedAt": "then",
        })

        with self.assertRaisesRegex(LinearEventError, "checkpoint_chain_truncated"):
            LinearCommentEventAdapter(client, issue_id="GEN-37").apply(
                delta("event-2", {"order": 2}, expected_revision=1)
            )

    def test_foreign_winner_at_same_revision_refuses(self):
        class ForeignWinnerClient(FakeCommentClient):
            injected = False

            def execute(self, query, variables):
                if "commentCreate" in query and not self.injected:
                    self.injected = True
                    self.comments.append({
                        "id": variables["input"]["id"],
                        "body": encode_event_comment(delta("foreign", {"order": 9})),
                        "createdAt": "now", "updatedAt": "now",
                    })
                    raise LinearTransportError("duplicate comment id")
                return super().execute(query, variables)

        with self.assertRaisesRegex(
            RuntimeError, "expected revision 0, live revision 1"
        ):
            LinearCommentEventAdapter(
                ForeignWinnerClient(), issue_id="GEN-37"
            ).apply(delta("event-a", {"order": 1}))

    def test_project_move_keeps_same_boundary_slot_and_collision(self):
        old_authority = {
            "workspace_id": "workspace", "team_id": "old-team",
            "project_id": "old-project", "root_issue_id": "issue-37",
        }
        new_authority = {
            "workspace_id": "workspace", "team_id": "new-team",
            "project_id": "new-project", "root_issue_id": "issue-37",
        }
        old_slot = ledger_boundary_slot_id("GEN-37", 0, [], old_authority)
        self.assertEqual(
            old_slot,
            ledger_boundary_slot_id("GEN-37", 0, [], new_authority),
        )
        self.assertEqual(
            old_slot,
            ledger_boundary_slot_id("OPS-9", 0, [], new_authority),
        )

        class MovedProjectClient(FakeCommentClient):
            injected = False

            def __init__(self):
                super().__init__()
                self.team_id = "new-team"
                self.project_id = "new-project"

            def execute(self, query, variables):
                if "commentCreate" in query and not self.injected:
                    self.injected = True
                    self.asserted_slot = variables["input"]["id"]
                    self.comments.append({
                        "id": old_slot,
                        "body": encode_event_comment(delta("old-winner", {"order": 9})),
                        "createdAt": "now", "updatedAt": "now",
                    })
                    raise LinearTransportError("duplicate comment id")
                return super().execute(query, variables)

        client = MovedProjectClient()
        with self.assertRaisesRegex(RuntimeError, "live revision 1"):
            LinearCommentEventAdapter(
                client, issue_id="GEN-37", workspace_id="workspace",
                team_id="new-team", project_id="new-project",
            ).apply(delta("new-writer", {"order": 1}))
        self.assertEqual(client.asserted_slot, old_slot)

    def test_checkpoint_winner_moves_event_to_new_shared_frontier(self):
        client = FakeCommentClient()
        client.comments.append({
            "id": "legacy-material-1",
            "body": encode_event_comment(delta("event-a", {"order": 1})),
            "createdAt": "then", "updatedAt": "then",
        })
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="checkpoint-wins",
            root_revision=1, plan_revision="plan-sha",
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "s1",
                "machine": "m1", "worktree": {
                    "state": "safe", "path": "/repo", "branch": "main",
                    "head": "head-1",
                },
            }, exact_head="head-1", evidence=[], blocker=None,
            next_action="continue", predecessor_event_id=None,
        )
        original_execute = client.execute
        injected = False

        def checkpoint_wins(query, variables):
            nonlocal injected
            if "commentCreate" in query and not injected:
                injected = True
                client.comments.append({
                    "id": variables["input"]["id"],
                    "body": encode_checkpoint_comment(checkpoint),
                    "createdAt": "now", "updatedAt": "now",
                })
                raise LinearTransportError("duplicate comment id")
            return original_execute(query, variables)

        client.execute = checkpoint_wins
        receipt = LinearCommentEventAdapter(client, issue_id="GEN-37").apply(
            delta("event-b", {"order": 2}, expected_revision=1)
        )

        self.assertEqual(receipt.revision, 2)
        self.assertEqual(len(client.comments), 3)
        self.assertEqual(len({comment["id"] for comment in client.comments}), 3)

    def test_concurrent_apply_with_rebase_serializes_stable_event_ids(self):
        client = FakeCommentClient(initial_readers=2)
        receipts = []
        failures = []

        def apply(event_id, order):
            journal = DeltaJournal(":memory:")
            try:
                journal.append(
                    "GEN-37", "requirement", {"order": order}, 0,
                    event_id=event_id, source="agent_discovery",
                )
                receipts.extend(journal.apply_with_rebase(
                    LinearCommentEventAdapter(client, issue_id="GEN-37")
                ))
            except Exception as exc:  # captured so the main thread can assert it
                failures.append(exc)
            finally:
                journal.close()

        threads = [
            threading.Thread(target=apply, args=("event-a", 1)),
            threading.Thread(target=apply, args=("event-b", 2)),
        ]
        # Some full-suite tests deliberately reload workstream_delta to verify
        # migrations. Keep the adapter's exception identity aligned with the
        # journal class under test while these worker threads execute.
        with mock.patch.object(
            linear_events_module, "RevisionConflict", RevisionConflict
        ):
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)

        self.assertFalse(failures)
        self.assertEqual({receipt.event_id for receipt in receipts}, {"event-a", "event-b"})
        state = reduce_event_comments(client.comments, workstream_id="GEN-37")
        self.assertEqual(state.revision, 2)
        self.assertEqual([event.expected_revision for event in state.events], [0, 1])
        self.assertEqual({event.event_id for event in state.events}, {"event-a", "event-b"})
        self.assertEqual(len(client.comments), 2)
        self.assertEqual(len({comment["id"] for comment in client.comments}), 2)

    def test_duplicate_and_conflicting_event_ids_fail_closed(self):
        original = delta("event-a", {"order": 1})
        conflicting = delta("event-a", {"order": 2})
        duplicate_comments = [
            {"id": "one", "body": encode_event_comment(original)},
            {"id": "two", "body": encode_event_comment(original)},
        ]
        with self.assertRaisesRegex(LinearEventError, "duplicate_event_id:event-a"):
            reduce_event_comments(duplicate_comments, workstream_id="GEN-37")
        conflicting_comments = [
            {"id": "one", "body": encode_event_comment(original)},
            {"id": "two", "body": encode_event_comment(conflicting)},
        ]
        with self.assertRaisesRegex(LinearEventError, "conflicting_event_id:event-a"):
            reduce_event_comments(conflicting_comments, workstream_id="GEN-37")

    def test_malformed_marker_and_revision_gap_fail_closed(self):
        with self.assertRaisesRegex(LinearEventError, "malformed_event_marker"):
            reduce_event_comments(
                [{"id": "bad", "body": "<!-- workstream-delta:v1:not-base64 -->"}],
                workstream_id="GEN-37",
            )
        with self.assertRaisesRegex(LinearEventError, "event_revision_gap"):
            reduce_event_comments(
                [{"id": "gap", "body": encode_event_comment(delta("event-gap", {}, 2))}],
                workstream_id="GEN-37",
            )

    def test_unavailable_auth_fails_before_client_or_network_exists(self):
        with self.assertRaisesRegex(LinearEventError, "linear_auth_unavailable"):
            LinearCommentEventAdapter.from_env(issue_id="GEN-37", env={})

    def test_configured_route_fences_comment_writes(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(
            client, issue_id="GEN-37", workspace_id="wrong",
            team_id="team", project_id="project",
        )
        with self.assertRaisesRegex(LinearEventError, "configured workspace"):
            adapter.apply(delta("event-a", {"order": 1}))
        self.assertFalse(any("commentCreate" in query for query, _ in client.calls))

    def test_immutable_root_issue_id_fences_comment_writes(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(
            client, issue_id="GEN-37", workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id="different-root",
        )
        with self.assertRaisesRegex(LinearEventError, "root_issue_id_mismatch"):
            adapter.apply(delta("event-a", {"order": 1}))
        self.assertFalse(any("commentCreate" in query for query, _ in client.calls))

    def test_from_env_consumes_config_route(self):
        route = {"workspace_id": "workspace", "team_id": "team", "project_id": "project"}
        client = mock.Mock()
        with mock.patch("workstream_config.resolve_linear_route", return_value=(route, None)), \
             mock.patch("workstream_linear_events.HttpGraphQLClient", return_value=client):
            adapter = LinearCommentEventAdapter.from_env(
                issue_id="GEN-37", env={"LINEAR_API_KEY": "secret"}
            )
        self.assertIs(adapter.client, client)
        self.assertEqual(adapter.workspace_id, "workspace")
        self.assertEqual(adapter.project_id, "project")


if __name__ == "__main__":
    unittest.main()
