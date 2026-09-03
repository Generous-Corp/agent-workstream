import copy
import base64
import hashlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from workstream_checkpoint import build_checkpoint
from workstream_generation import (
    _digest as generation_digest, build_retirement_proof,
    encode_generation_reservation, pending_generation_reservations,
)
from workstream_delta import Delta
from workstream_linear_checkpoints import encode_checkpoint_comment
from workstream_linear import LinearRateLimitedError
from workstream_linear_events import encode_event_comment, ledger_boundary_slot_id
import workstream_linear_events as linear_events_module
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, projection_slot_id,
)
import workstream_resume as MODULE


class ResumeTests(unittest.TestCase):
    def generation_sources(self):
        canonical = "https://github.com/acme/plans/blob/main/PLAN.md"
        immutable = (
            "https://github.com/acme/plans/blob/" + "1" * 40 + "/PLAN.md"
        )
        return canonical, immutable

    def test_plan_generation_freshness_unchanged_is_full_eligible(self):
        canonical, immutable = self.generation_sources()
        active = {"identity": immutable, "sha256": "a" * 64}
        with mock.patch.object(MODULE, "plan_payload", return_value={
            "source": {"identity": canonical, "sha256": "a" * 64},
        }), mock.patch(
            "workstream_generation.pending_generation_reservations",
            return_value=[],
        ):
            self.assertIsNone(MODULE.plan_generation_freshness(
                token="GEN-37", description=f"Canonical plan: {canonical}",
                active_source=active, comments=[], authenticated_route={
                    "workspace_id": "workspace", "team_id": "team",
                    "project_id": "project", "root_issue_id": "root",
                },
            ))

    def test_plan_generation_drift_returns_non_executable_bounded_remediation(self):
        canonical, immutable = self.generation_sources()
        active = {"identity": immutable, "sha256": "a" * 64}
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root",
        }
        with mock.patch.object(MODULE, "plan_payload", return_value={
            "source": {"identity": canonical, "sha256": "b" * 64},
        }), mock.patch(
            "workstream_generation.pending_generation_reservations",
            return_value=[],
        ):
            result = MODULE.plan_generation_freshness(
                token="GEN-37", description=f"Canonical plan: {canonical}",
                active_source=active, comments=[], authenticated_route=route,
                generation={
                    "plan_revision": "a" * 64,
                    "description_plan_revision": "a" * 64,
                    "transition_tip_event_id": "wsp_" + "1" * 32,
                    "activation_epoch": 1,
                    "authority_origin": "generation_transition",
                },
            )
        self.assertEqual(result["resume_authority"], "plan_generation_pending")
        self.assertFalse(result["executable"])
        self.assertEqual(result["active_source"]["sha256"], "a" * 64)
        self.assertEqual(result["canonical_live_source"]["sha256"], "b" * 64)
        self.assertIsNone(result["remediation"]["command"])
        self.assertTrue(result["remediation"]["selection_required"])
        self.assertNotIn(
            "--retirement-proof", json.dumps(result["remediation"]),
        )
        alternatives = {
            item["kind"]: item for item in result["remediation"]["alternatives"]
        }
        self.assertTrue(alternatives["reconcile_regressed_locator"]["available"])
        self.assertEqual(
            alternatives["reconcile_regressed_locator"]["command"][:3],
            ["workstreamctl", "root-transition", "reconcile-plan-url"],
        )
        self.assertTrue(alternatives["activate_new_generation"]["available"])
        with mock.patch.object(MODULE, "plan_payload", return_value={
            "source": {"identity": canonical, "sha256": "b" * 64},
        }), mock.patch(
            "workstream_generation.pending_generation_reservations",
            return_value=[],
        ):
            legacy = MODULE.plan_generation_freshness(
                token="GEN-37", description=f"Canonical plan: {canonical}",
                active_source=active, comments=[], authenticated_route=route,
                generation={
                    "plan_revision": "a" * 64,
                    "description_plan_revision": "a" * 64,
                    "transition_tip_event_id": None,
                    "activation_epoch": None,
                    "authority_origin": "legacy_description",
                },
            )
        legacy_alternatives = {
            item["kind"]: item
            for item in legacy["remediation"]["alternatives"]
        }
        self.assertFalse(
            legacy_alternatives["reconcile_regressed_locator"]["available"],
        )
        self.assertTrue(
            legacy_alternatives["activate_new_generation"]["available"],
        )
        self.assertLess(len(json.dumps(result).encode()), 24 * 1024)

    def test_pending_generation_reservation_surfaces_exact_replay_or_abort(self):
        canonical, immutable = self.generation_sources()
        reservation = {
            "schema_version": 4,
            "reservation_id": "wsgr_" + "1" * 32,
            "reservation_sha256": "2" * 64,
            "from_plan_revision": "a" * 64,
            "to_plan_revision": "b" * 64,
            "created_at": "2026-08-31T12:00:00Z",
            "native_root_sha256": "3" * 64,
            "source": {"identity": immutable, "sha256": "b" * 64},
            "retirement": {"reviewed": True},
            "activation_checkpoint": None,
            "remote_head": None,
        }
        with mock.patch.object(MODULE, "plan_payload", return_value={
            "source": {"identity": canonical, "sha256": "b" * 64},
        }), mock.patch(
            "workstream_generation.pending_generation_reservations",
            return_value=[reservation],
        ), mock.patch(
            "workstream_generation.validate_prepared_generation_transition",
            return_value=None,
        ):
            result = MODULE.plan_generation_freshness(
                token="GEN-37", description=f"Canonical plan: {canonical}",
                active_source={"identity": immutable, "sha256": "a" * 64},
                comments=[], authenticated_route={
                    "workspace_id": "workspace", "team_id": "team",
                    "project_id": "project", "root_issue_id": "root",
                },
            )
        pending = result["pending_generation_reservations"][0]
        self.assertIsNone(result["remediation"]["command"])
        self.assertIn(
            "pending reservation", result["remediation"]["selection_rule"],
        )
        self.assertFalse(any(
            item["available"] for item in result["remediation"]["alternatives"]
        ))
        self.assertEqual(pending["reservation_id"], reservation["reservation_id"])
        self.assertIn("--abort-reservation-id", pending["abort"]["command"])
        self.assertIn("2026-08-31T12:00:00Z", pending["continue"]["command"])
        self.assertIn("--expected-native-root-sha256", pending["continue"]["command"])

    def test_schema6_and_schema7_pending_generation_surface_handle_only_continuation(self):
        canonical, immutable = self.generation_sources()
        reservation = {
            "schema_version": 6,
            "reservation_id": "wsgr_" + "1" * 32,
            "reservation_sha256": "2" * 64,
            "from_plan_revision": "a" * 64,
            "to_plan_revision": "b" * 64,
            "created_at": "2026-09-01T12:00:00Z",
            "native_root_sha256": "3" * 64,
            "source": {"identity": immutable, "sha256": "b" * 64},
            "retirement": {"reviewed": True},
            "activation_checkpoint": None, "remote_head": None,
        }
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root",
        }

        def surface(prepared):
            with mock.patch.object(MODULE, "plan_payload", return_value={
                "source": {"identity": canonical, "sha256": "b" * 64},
            }), mock.patch(
                "workstream_generation.pending_generation_reservations",
                return_value=[reservation],
            ), mock.patch(
                "workstream_generation.validate_prepared_generation_transition",
                return_value={"event_id": "wsp_" + "9" * 32} if prepared else None,
            ):
                return MODULE.plan_generation_freshness(
                    token="GEN-37",
                    description=f"Canonical plan: {canonical}",
                    active_source={"identity": immutable, "sha256": "a" * 64},
                    comments=[], authenticated_route=route,
                )["pending_generation_reservations"][0]

        pending = surface(False)
        self.assertEqual(pending["continue"]["command"], [
            str(Path(sys.executable).resolve()),
            str(Path(MODULE.__file__).resolve().with_name(
                "workstream_generation.py"
            )),
            "continue", "GEN-37",
            "--reservation-id", reservation["reservation_id"],
            "--reservation-sha256", reservation["reservation_sha256"],
            "--apply",
        ])
        self.assertNotIn("materialize_files", pending["continue"])
        self.assertIn("--abort-reservation-id", pending["abort"]["command"])
        prepared = surface(True)
        self.assertFalse(prepared["abort"]["available"])
        self.assertIn("replay", prepared["abort"]["reason"])
        reservation["schema_version"] = 7
        schema7 = surface(False)
        self.assertTrue(schema7["continue"]["available"])
        self.assertEqual(
            schema7["continue"]["command"], pending["continue"]["command"],
        )

    def test_non_null_generation_replay_is_exact_and_legacy_is_abort_only(self):
        canonical, immutable = self.generation_sources()
        checkpoint = build_checkpoint(
            workstream_id="GEN-14", boundary_id="generation-replay",
            root_revision=7, plan_revision="b" * 64,
            before_status="Done", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "resume-session", "machine": "M3",
                "worktree": {
                    "state": "safe", "path": "/Volumes/Workshop/Code/wt",
                    "branch": "resume", "head": "4" * 40,
                },
            },
            exact_head="4" * 40, evidence=[], blocker=None,
            next_action="Finalize the reviewed generation.",
        )
        retirement = build_retirement_proof(
            predecessor_plan_revision="a" * 64,
            retired_at="2026-08-31T12:00:00Z", retired_writer_epoch=0,
            provenance_event_ids=["wsp_" + "5" * 32],
            checkpoint_event_ids=["wscp_" + "6" * 32],
        )
        reservation = {
            "schema_version": 4,
            "reservation_id": "wsgr_" + "1" * 32,
            "reservation_sha256": "2" * 64,
            "from_plan_revision": "a" * 64,
            "to_plan_revision": "b" * 64,
            "created_at": "2026-08-31T12:00:00Z",
            "native_root_sha256": "3" * 64,
            "source": {"identity": immutable, "sha256": "b" * 64},
            "retirement": retirement,
            "activation_checkpoint": checkpoint,
            "remote_head": "4" * 40,
        }

        def mocked_surface(items):
            with mock.patch.object(MODULE, "plan_payload", return_value={
                "source": {"identity": canonical, "sha256": "b" * 64},
            }), mock.patch(
                "workstream_generation.pending_generation_reservations",
                return_value=items,
            ):
                return MODULE.plan_generation_freshness(
                    token="GEN-14", description=f"Canonical plan: {canonical}",
                    active_source={"identity": immutable, "sha256": "a" * 64},
                    comments=[], authenticated_route={
                        "workspace_id": "workspace", "team_id": "team",
                        "project_id": "project", "root_issue_id": "root",
                    },
                )

        pending = mocked_surface([reservation])["pending_generation_reservations"][0]
        self.assertEqual(pending["replay_inputs"], {
            "source": reservation["source"],
            "retirement_proof": retirement,
            "activation_checkpoint": checkpoint,
            "remote_head": "4" * 40,
            "created_at": reservation["created_at"],
            "expected_native_root_sha256": "3" * 64,
        })
        self.assertFalse(any("<" in token for token in pending["continue"]["command"]))
        self.assertIn("4" * 40, pending["continue"]["command"])
        self.assertEqual(
            [item["content"] for item in pending["continue"]["materialize_files"]],
            [retirement, checkpoint],
        )
        for legacy_version in (2, 3):
            legacy = {
                "schema_version": legacy_version,
                "workstream_id": "GEN-14", "authority": {
                    "workspace_id": "workspace", "team_id": "team",
                    "project_id": "project", "root_issue_id": "root",
                },
                "mode": "activate",
                "from_plan_revision": "a" * 64,
                "to_plan_revision": "b" * 64,
                "activation_epoch": 0,
                "previous_control_event_id": None,
                "source": {"identity": immutable, "sha256": "b" * 64},
                "material_revision": 0,
                "checkpoint_event_ids": [], "ledger_frontier": [],
                "from_projection_revision": 0,
                "to_projection_revision": 0,
                "graph_frontier_sha256": "7" * 64,
                "candidate_resume_sha256": "8" * 64,
                "retirement": build_retirement_proof(
                    predecessor_plan_revision="a" * 64,
                    retired_at="2026-08-31T12:00:00Z",
                    retired_writer_epoch=0, provenance_event_ids=[],
                    checkpoint_event_ids=[],
                ),
                "created_at": "2026-08-31T12:00:00Z",
            }
            if legacy_version == 3:
                legacy["native_root_sha256"] = "3" * 64
            legacy["reservation_id"] = (
                "wsgr_" + generation_digest(legacy)[:32]
            )
            legacy_comments = [{
                "id": ledger_boundary_slot_id(
                    "GEN-14", 0, [], legacy["authority"],
                ),
                "body": encode_generation_reservation(legacy),
            }]
            reduced = pending_generation_reservations(
                legacy_comments, workstream_id="GEN-14",
                authenticated_route=legacy["authority"],
            )
            self.assertEqual(len(reduced), 1)
            with mock.patch.object(MODULE, "plan_payload", return_value={
                "source": {"identity": canonical, "sha256": "b" * 64},
            }):
                legacy_surface = MODULE.plan_generation_freshness(
                    token="GEN-14", description=f"Canonical plan: {canonical}",
                    active_source={
                        "identity": immutable, "sha256": "a" * 64,
                    },
                    comments=legacy_comments,
                    authenticated_route=legacy["authority"],
                )
            observed = legacy_surface["pending_generation_reservations"][0]
            self.assertFalse(observed["continue"]["available"])
            self.assertIn("abort", observed["continue"]["reason"])
            self.assertIn("--abort-reservation-id", observed["abort"]["command"])
            self.assertIn(legacy["reservation_id"], observed["abort"]["command"])

    def test_generation_pending_long_source_route_and_root_stay_under_24k(self):
        canonical, immutable = self.generation_sources()
        reservation = {
            "schema_version": 4,
            "reservation_id": "wsgr_" + "1" * 32,
            "reservation_sha256": "2" * 64,
            "from_plan_revision": "a" * 64,
            "to_plan_revision": "b" * 64,
            "created_at": "2026-08-31T12:00:00Z",
            "native_root_sha256": "3" * 64,
            "source": {
                "identity": immutable + "?diagnostic=" + "x" * 30000,
                "sha256": "b" * 64,
            },
            "retirement": {"reviewed": "y" * 30000},
            "activation_checkpoint": None,
            "remote_head": None,
        }
        with mock.patch.object(MODULE, "plan_payload", return_value={
            "source": {"identity": canonical, "sha256": "b" * 64},
        }), mock.patch(
            "workstream_generation.pending_generation_reservations",
            return_value=[reservation],
        ):
            result = MODULE.plan_generation_freshness(
                token="GEN-14", description=f"Canonical plan: {canonical}",
                active_source={"identity": immutable, "sha256": "a" * 64},
                comments=[], authenticated_route={
                    "workspace_id": "w" * 30000, "team_id": "t" * 30000,
                    "project_id": "p" * 30000, "root_issue_id": "r" * 30000,
                },
            )
        result["authenticated_route"] = {"diagnostic": "z" * 30000}
        result["root"] = {"title": "q" * 30000}
        bounded = MODULE.bound_plan_generation_pending(
            result, max_bytes=24 * 1024,
        )
        encoded = MODULE._default_output_bytes(bounded)
        self.assertLessEqual(len(encoded), 24 * 1024)
        self.assertEqual(bounded["active_plan_sha256"], "a" * 64)
        self.assertEqual(bounded["canonical_live_plan_sha256"], "b" * 64)
        self.assertEqual(bounded["pending_transition"]["reservation_count"], 1)
        self.assertEqual(len(bounded["pending_transition"]["sha256"]), 64)

    def test_gen14_terminal_native_cache_yields_to_finalized_generation_status(self):
        root = {
            "identifier": "GEN-14", "status": "Done",
            "status_type": "completed",
        }
        MODULE.apply_generation_execution_status(root, {
            "authority": "generation_local", "name": "In Progress",
            "type": "started",
        })
        self.assertEqual(root["status"], "In Progress")
        self.assertEqual(root["status_type"], "started")
        self.assertEqual(root["issue_status"], "Done")
        self.assertEqual(root["issue_status_type"], "completed")

    def test_zero_multiple_or_different_canonical_plan_refuses_without_fetch(self):
        canonical, immutable = self.generation_sources()
        active = {"identity": immutable, "sha256": "a" * 64}
        descriptions = (
            "No canonical plan here",
            f"Canonical plan: {canonical}\nCanonical plan: https://example.test/other.md",
            "Canonical plan: https://github.com/acme/plans/blob/main/OTHER.md",
        )
        for description in descriptions:
            with self.subTest(description=description), mock.patch.object(
                MODULE, "plan_payload"
            ) as fetch:
                with self.assertRaisesRegex(
                    ValueError,
                    "canonical_plan_source_(missing|ambiguous)|"
                    "canonical_plan_source_conflicts_active_generation",
                ):
                    MODULE.plan_generation_freshness(
                        token="GEN-37", description=description,
                        active_source=active, comments=[], authenticated_route={},
                    )
                fetch.assert_not_called()

    def snapshot(self):
        return {"root": {"identifier": "GEN-37", "url": "https://linear/GEN-37", "plan_revision": "sha",
                          "revision": 7, "status": "In Progress", "next_action": "resume"},
                "children": [{"identifier": "GEN-38", "title": "intake", "status": "In Progress", "next_action": "adapter"},
                             {"identifier": "GEN-39", "title": "delta", "status": "Done"}],
                "decisions": [{"id": "D1", "status": "accepted"}], "provenance": [{"machine": "M3"}]}

    def live_snapshot(self, snapshot, route):
        snapshot["root"].update({
            "id": route["root_issue_id"], "title": snapshot["root"].get("title", "Root"),
            "description": snapshot["root"].get("description", "Plan revision: sha"),
            "url": snapshot["root"].get("url", "https://linear/GEN-37"),
            "updatedAt": snapshot["root"].get("updatedAt", "now"), "parent": None,
            "team": {"id": route["team_id"],
                     "organization": {"id": route["workspace_id"]}},
            "project": {"id": route["project_id"]}, "assignee": None,
            "state": {"id": "started", "name": "In Progress", "type": "started"},
        })
        for index, child in enumerate(snapshot["children"]):
            child.update({
                "id": f"child-{index}",
                "url": f"https://linear/{child['identifier']}",
                "parent": {"id": route["root_issue_id"], "identifier": "GEN-37"},
                "team": {"id": route["team_id"],
                         "organization": {"id": route["workspace_id"]}},
                "project": {"id": route["project_id"]},
            })
        snapshot["child_comments"] = {
            child["identifier"]: [] for child in snapshot["children"]
            if str(child.get("status", "")).lower() not in MODULE.TERMINAL
        }
        return snapshot

    def full_authority_snapshot(self, snapshot, route=None):
        route = route or {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project",
            "root_issue_id": "33333333-3333-4333-8333-333333333333",
        }
        plan_revision = "a" * 64
        snapshot["root"]["plan_revision"] = plan_revision
        snapshot["root"].update({
            "id": route["root_issue_id"],
            "description": snapshot["root"].get(
                "description", f"Plan revision: {plan_revision}"
            ),
        })
        snapshot["root"]["issue_revision"] = snapshot["root"]["revision"]
        repository_key = "github.com:id:R_agent_workstream"
        scope = {
            "namespace": "agent-workstream",
            "linear": {
                **route,
                "route_verification": {
                    **route, "observed_at": "2026-08-31T12:00:00Z",
                    "evidence": [{
                        "kind": "authenticated_linear_readback",
                        "authenticated": True, **route,
                    }],
                },
            },
            "primary_repository": repository_key,
            "repositories": [{
                "slug": "github.com/generous-corp/agent-workstream",
                "exact_head": "b" * 40,
                "provider_repository_id": "R_agent_workstream",
                "aliases": [],
                "identity_resolution": {
                    "provider_repository_id": "R_agent_workstream",
                    "resolved_slug": "github.com/generous-corp/agent-workstream",
                    "observed_at": "2026-08-31T12:00:00Z",
                    "evidence": [{
                        "kind": "authenticated_provider_readback",
                        "authenticated": True,
                        "provider_repository_id": "R_agent_workstream",
                        "resolved_slug": "github.com/generous-corp/agent-workstream",
                    }],
                },
                "identity_updates": [], "evidence": [],
            }],
            "child_ownership": {
                child["identifier"]: repository_key
                for child in snapshot["children"]
            },
        }
        source = {
            "identity": "https://example.test/immutable-plan",
            "sha256": plan_revision,
        }
        provenance = [{
            "agent": "codex", "machine": "M5", "session_id": "session",
            "worktree": {
                "state": "safe", "path": "/repo/worktree",
                "branch": "fix/no-stranding", "head": "b" * 40,
            },
        }]
        disposition = {
            "disposition": "attach", "recovered_from_checkpoint": None,
            "remote_head": "b" * 40,
        }
        values = (
            ("scope", "root", scope), ("source", "root", source),
            ("provenance", "primary", provenance[0]),
            ("disposition", "root", disposition),
        )
        projection_events = []
        projected_values = [*values, *(
            ("relation", f"{relation['type']}:{relation['target']['identifier']}", relation)
            for relation in snapshot.get("relations", [])
        )]
        for index, (kind, key, value) in enumerate(projected_values):
            projection_events.append(build_projection_event(
                workstream_id="GEN-37", kind=kind, key=key, value=value,
                plan_revision=plan_revision, expected_revision=index,
                created_at=f"2026-08-31T12:00:0{index}Z", authority=route,
            ))
        snapshot.update({
            "scope": scope, "source": source, "provenance": provenance,
            "disposition": disposition,
            "relations": snapshot.get("relations", []), "choice_events": [],
            "evidence_contracts": [], "child_closures": [],
            "closure_reviews": [], "projection_events": projection_events,
            "projection_history": [], "projection_quarantined": [],
            "projection_unresolved_quarantine": [],
            "quarantine_disposition": None,
            "projection_revision": len(projection_events),
            "projection_recovery": {"state": "current", "stale_plan_count": 0},
            "authenticated_route": route, "authenticated_source": source,
            "latest_checkpoint": None,
            "checkpoint_recovery": {"state": "not_found", "stale_plan_count": 0},
        })
        empty_graph_sha256 = hashlib.sha256(b"[]").hexdigest()
        snapshot["dependency_graph"] = {
            "schema_version": 1,
            "authority": "child_dependency_authorization",
            "plan_revision": plan_revision,
            "route": route,
            "revision": 0,
            "sha256": empty_graph_sha256,
            "authorization_batches": [],
            "relations": [],
            "native_readback": "relations_and_inverseRelations",
            "ignored_non_dependency_count": 0,
            "observed_frontier": {
                "material_revision": snapshot.get("material_event_revision", 0),
                "projection_revision": len(projection_events),
                "graph_revision": 0,
                "graph_sha256": empty_graph_sha256,
            },
            "root_readback_sha256": MODULE.dependency_root_readback_sha256(
                snapshot["root"]
            ),
        }
        return snapshot

    def test_closure_receipt_body_is_audit_only_but_done_binding_is_bounded(self):
        base = self.snapshot()
        base["children"] = []
        snapshot = self.full_authority_snapshot(base)
        route = snapshot["authenticated_route"]
        plan = snapshot["root"]["plan_revision"]
        snapshot_sha256 = MODULE.closure_snapshot_digest(snapshot)
        closure_input_sha256 = "c" * 64
        github = {
            "repository": "generous-corp/agent-workstream",
            "provider_repository_id": "R_agent_workstream", "pr_number": 73,
            "pr_head": "b" * 40, "merged": True, "merge_sha": "d" * 40,
        }
        shipyard_body = {
            "schema_version": 1,
            "repository": github["repository"],
            "repository_key": "github.com:id:R_agent_workstream",
            "pr_number": 73, "head": "b" * 40,
            "disposition": "merged", "receipt_id": "shipyard-73",
        }
        shipyard = {**shipyard_body, "receipt_sha256": hashlib.sha256(json.dumps(
            shipyard_body, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()}
        review = {
            "schema_version": 1, "workstream_id": "GEN-37",
            "snapshot_sha256": snapshot_sha256,
            "closure_input_sha256": closure_input_sha256,
            "repository_key": "github.com:id:R_agent_workstream",
            "exact_head": "b" * 40, "verdict": "pass",
            "reviewer_agent": "claude", "reviewer_session_id": "reviewer",
            "implementer_session_id": "session",
            "reviewed_at": "2026-09-01T00:00:00Z",
            "review_artifact_identity": "audit-secret-" + ("x" * 4096),
            "review_artifact_sha256": "e" * 64,
            "trust_boundary": "shared_linear_credential",
            "procedural_independence": True,
        }
        receipt = {
            "criteria_checked": ["all accepted gates"],
            "children_checked": [],
            "evidence_categories_checked": [
                "decisions", "followups", "prs", "landing_receipts",
                "tests", "artifacts",
            ],
            "excluded": [], "deterministic_checks_passed": True,
            "semantic_review_invoked": True, "semantic_review_passed": True,
            "resume_token": "GEN-37", "context_url": "https://linear/GEN-37",
            "plan_revision": plan, "root_revision": snapshot["root"]["revision"],
            "final_disposition": "Done", "snapshot_sha256": snapshot_sha256,
            "closure_input_sha256": closure_input_sha256,
            "independent_review": review, "github": github,
            "shipyard_receipt_sha256": shipyard["receipt_sha256"],
        }
        receipt_event = build_projection_event(
            workstream_id="GEN-37", kind="closure_receipt", key=snapshot_sha256,
            value=receipt, plan_revision=plan,
            expected_revision=len(snapshot["projection_events"]),
            created_at="2026-09-01T00:01:00Z", authority=route,
        )
        receipt_sha256 = hashlib.sha256(json.dumps(
            receipt, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()
        lifecycle = {
            "status": "Done", "github": github, "shipyard_receipt": shipyard,
            "closure_input_sha256": closure_input_sha256,
            "snapshot_sha256": snapshot_sha256, "independent_review": review,
            "closure_receipt_sha256": receipt_sha256,
            "closure_receipt_event_id": receipt_event["event_id"],
        }
        lifecycle_event = build_projection_event(
            workstream_id="GEN-37", kind="lifecycle", key="root",
            value=lifecycle, plan_revision=plan,
            expected_revision=len(snapshot["projection_events"]) + 1,
            created_at="2026-09-01T00:02:00Z", authority=route,
        )
        snapshot["projection_events"].extend([receipt_event, lifecycle_event])
        snapshot["projection_revision"] += 2
        snapshot["closure_receipts"] = [receipt]
        snapshot["lifecycle"] = lifecycle
        snapshot["root"].update({
            "issue_status": snapshot["root"]["status"], "status": "Done",
            "closure_receipt": receipt_sha256,
        })
        snapshot["dependency_graph"]["observed_frontier"][
            "projection_revision"
        ] = snapshot["projection_revision"]

        ordinary = MODULE.compact_context(
            snapshot, "GEN-37", require_projection_authority=True,
        )
        self.assertEqual(ordinary["closure_receipt"], {
            "event_id": receipt_event["event_id"],
            "sha256": receipt_sha256, "snapshot_sha256": snapshot_sha256,
        })
        self.assertNotIn("audit-secret-", json.dumps(ordinary))
        audit = MODULE.compact_context(
            snapshot, "GEN-37", require_projection_authority=True,
            include_history=True, max_bytes=2**20, max_items=1000,
        )
        self.assertEqual(audit["closure_receipts"], [receipt])

    def test_repair_graph_frontier_binds_native_status_and_state_identity(self):
        snapshot = self.snapshot()
        snapshot["root"].update({
            "id": "root", "title": "Repair", "description": "full description",
            "next_action": "continue", "updatedAt": "2026-08-30T00:00:00Z",
            "revision": 7, "plan_revision": "a" * 64,
            "status": "In Progress", "status_type": "started",
            "state_id": "state-1", "state": {
                "id": "state-1", "name": "In Progress", "type": "started",
            },
        })
        baseline = MODULE._issue_graph_repair_frontier(snapshot, [], {})
        self.assertEqual(baseline["issues"]["root"]["state_id"], "state-1")
        for field, value in (
            ("status", "Blocked"), ("status_type", "canceled"),
            ("state_id", "state-2"),
            ("id", "root-2"), ("title", "Changed title"),
            ("description", "changed description"),
            ("next_action", "changed action"), ("revision", 8),
            ("plan_revision", "b" * 64),
            ("updatedAt", "2026-08-30T00:01:00Z"),
        ):
            changed = copy.deepcopy(snapshot)
            changed["root"][field] = value
            self.assertNotEqual(
                MODULE._issue_graph_repair_frontier(changed, [], {})["sha256"],
                baseline["sha256"], field,
            )
        for field, value in (
            ("id", "nested-state-2"), ("name", "Blocked"),
            ("type", "canceled"),
        ):
            changed = copy.deepcopy(snapshot)
            changed["root"]["state"][field] = value
            self.assertNotEqual(
                MODULE._issue_graph_repair_frontier(changed, [], {})["sha256"],
                baseline["sha256"], "state." + field,
            )
        snapshot["children"][0].update({
            "description": "Plan revision: " + "a" * 64 + "\nRevision: 4",
            "plan_revision": "a" * 64, "revision": 4,
        })
        baseline = MODULE._issue_graph_repair_frontier(snapshot, [], {})
        for field, value in (
            ("description", "changed child description"),
            ("plan_revision", "b" * 64), ("revision", 5),
            ("title", "changed child title"),
        ):
            changed = copy.deepcopy(snapshot)
            changed["children"][0][field] = value
            self.assertNotEqual(
                MODULE._issue_graph_repair_frontier(changed, [], {})["sha256"],
                baseline["sha256"], field,
            )
        relation = [{"type": "related", "target": {
            "workspace_id": "workspace", "issue_id": "target",
            "identifier": "GEN-50",
        }}]
        targets = {"workspace:target": {
            "id": "target", "identifier": "GEN-50", "status": "Todo",
        }}
        relation_baseline = MODULE._issue_graph_repair_frontier(
            snapshot, relation, targets,
        )
        changed_targets = copy.deepcopy(targets)
        changed_targets["workspace:target"]["identifier"] = "GEN-51"
        self.assertNotEqual(
            MODULE._issue_graph_repair_frontier(
                snapshot, relation, changed_targets,
            )["sha256"],
            relation_baseline["sha256"],
        )

    def test_repaired_history_joins_before_and_after_source_authentication(self):
        from test_workstream_linear_events import LinearCommentEventAdapterTests
        comments, _payload, _checkpoint, _projection, _generation, route, source, _graph = (
            LinearCommentEventAdapterTests()._repair_fixture()
        )
        graph = self.snapshot()
        graph["children"][1]["status"] = "In Progress"
        graph["children"][1]["next_action"] = "continue"
        graph["root"].update({
            "plan_revision": "a" * 64,
            "id": route["root_issue_id"],
            "team": {"id": route["team_id"],
                     "organization": {"id": route["workspace_id"]}},
            "project": {"id": route["project_id"]},
        })
        graph = self.live_snapshot(graph, route)
        provisional = MODULE.add_material_history(
            graph, comments, "GEN-37", authenticated_route=route,
        )
        resumed = MODULE.add_material_history(
            graph, comments, "GEN-37", authenticated_route=route,
            authenticated_source=source,
        )
        self.assertEqual(provisional["root"]["next_action"], "new")
        self.assertEqual(resumed["root"]["next_action"], "new")
        compact = MODULE.compact_context(resumed, "GEN-37")
        full = MODULE.compact_context(resumed, "GEN-37", include_history=True)
        self.assertEqual(compact["next_action"], "new")
        self.assertEqual(compact["material_semantic_repair"]["count"], 2)
        self.assertEqual(len(full["material_semantic_repairs"]), 2)
        self.assertEqual(
            [item["event_id"] for item in full["raw_material_events"][:2]],
            ["flat-a", "flat-b"],
        )

    def test_expected_missing_closures_requires_authority_validation(self):
        with self.assertRaisesRegex(
            MODULE.ResumeError,
            "expected_missing_terminal_closures_requires_projection_authority",
        ):
            MODULE.validate_snapshot(
                self.snapshot(), "GEN-37",
                expected_missing_terminal_closures=frozenset({"GEN-70"}),
            )

    def test_compact_context_keeps_root_and_only_nonterminal_children(self):
        snapshot = self.snapshot()
        snapshot["children"][0]["description"] = "large redundant prose"
        snapshot["children"][0]["owner"] = "agent"
        snapshot["children"][0]["blocker"] = {"text": "waiting"}
        context = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(context["workstream_id"], "GEN-37")
        self.assertEqual([c["identifier"] for c in context["children"]], ["GEN-38"])
        self.assertNotIn("description", context["children"][0])
        self.assertEqual(
            context["children"][0]["description_summary"],
            {
                "bytes": len("large redundant prose"),
                "sha256": hashlib.sha256(b"large redundant prose").hexdigest(),
            },
        )
        self.assertEqual(context["children"][0]["owner"], "agent")
        self.assertEqual(context["children"][0]["blocker"], {"text": "waiting"})

        full = MODULE.compact_context(snapshot, "GEN-37", include_history=True)
        self.assertEqual(full["children"][0], snapshot["children"][0])

    def test_token_mismatch_fails_closed(self):
        with self.assertRaises(MODULE.ResumeError):
            MODULE.compact_context(self.snapshot(), "GEN-40")

    def test_extracts_one_token_from_title_or_natural_language(self):
        self.assertEqual(MODULE.extract_token("pulp continuity GEN-37 #3"), "GEN-37")
        self.assertEqual(MODULE.extract_token("Execute this: resume gen-37 now"), "GEN-37")
        self.assertEqual(MODULE.extract_token("GEN-37 then GEN-37"), "GEN-37")

    def test_extracts_non_gen_team_token(self):
        self.assertEqual(MODULE.extract_token("resume ops-37 now"), "OPS-37")

    def test_zero_or_multiple_distinct_tokens_fail_closed(self):
        with self.assertRaisesRegex(MODULE.ResumeError, "missing_workstream_token"):
            MODULE.extract_token("pulp continuity")
        with self.assertRaisesRegex(MODULE.ResumeError, "multiple_workstream_tokens"):
            MODULE.extract_token("GEN-37 conflicts with GEN-38")

    def test_full_title_resolves_the_same_snapshot(self):
        context = MODULE.compact_context(self.snapshot(), "pulp continuity GEN-37 #3")
        self.assertEqual(context["workstream_id"], "GEN-37")

    def test_compact_context_exposes_recovered_linear_project_name(self):
        snapshot = self.snapshot()
        snapshot["root"]["project"] = {
            "id": "project", "name": "Linear Integration",
        }

        context = MODULE.compact_context(snapshot, "GEN-37")

        self.assertEqual(context["project_name"], "Linear Integration")

    def test_invalid_recovered_linear_project_name_does_not_downgrade_resume(self):
        for name in ("   ", "x" * (MODULE.MAX_PROJECT_NAME_BYTES + 1), 7):
            with self.subTest(name=name):
                snapshot = self.snapshot()
                snapshot["children"] = []
                snapshot["root"]["project"] = {"id": "project", "name": name}
                snapshot = self.full_authority_snapshot(snapshot)

                context = MODULE.compact_context(
                    snapshot, "GEN-37", require_projection_authority=True,
                )

                self.assertEqual(context["resume_authority"], "full")
                self.assertIsNone(context["project_name"])

    def test_missing_plan_revision_fails_closed(self):
        snapshot = self.snapshot(); del snapshot["root"]["plan_revision"]
        with self.assertRaises(MODULE.ResumeError):
            MODULE.validate_snapshot(snapshot, "GEN-37")

    def test_duplicate_child_fails_closed(self):
        snapshot = self.snapshot(); snapshot["children"].append(snapshot["children"][0].copy())
        with self.assertRaises(MODULE.ResumeError):
            MODULE.validate_snapshot(snapshot, "GEN-37")

    def test_nonterminal_child_without_next_action_fails_closed(self):
        snapshot = self.snapshot(); snapshot["children"][0].pop("next_action")
        with self.assertRaises(MODULE.ResumeError):
            MODULE.validate_snapshot(snapshot, "GEN-37")

    def test_child_material_log_and_checkpoint_override_stale_issue_prose(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        child = snapshot["children"][0]
        child["next_action"] = "stale issue-description action"
        events = [
            Delta(
                f"child-event-{index}", "GEN-38", "progress", "agent",
                {"next_action": f"event action {index}"} if index == 3 else {"step": index},
                index, f"2026-08-29T00:00:0{index}Z",
            )
            for index in range(4)
        ]
        checkpoint = build_checkpoint(
            workstream_id="GEN-38", boundary_id="child-current", root_revision=4,
            plan_revision="sha", before_status="In Progress",
            after_status="Blocked",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/child", "branch": "fix/child",
                    "head": "abc123",
                },
            },
            exact_head="abc123", evidence=[{"kind": "test", "id": "focused"}],
            blocker={"text": "await review"}, next_action="resume current child state",
        )
        snapshot["child_comments"]["GEN-38"] = [
            *[
                {"id": f"remote-{index}", "body": encode_event_comment(event)}
                for index, event in enumerate(events)
            ],
            {"id": "remote-checkpoint", "body": encode_checkpoint_comment(checkpoint)},
        ]

        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )
        context = MODULE.compact_context(enriched, "GEN-37")
        resumed = context["children"][0]

        self.assertNotIn("issue_next_action", resumed)
        self.assertEqual(resumed["next_action"], "resume current child state")
        self.assertEqual(resumed["blocker"], {"text": "await review"})
        self.assertEqual(resumed["material_event_revision"], 4)
        self.assertEqual(
            resumed["latest_checkpoint"]["checkpoint_event_id"], checkpoint["event_id"],
        )
        self.assertEqual(resumed["history"]["material_events"]["count"], 4)

    def test_child_full_history_retains_every_checkpoint_and_budgets_old_evidence(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        events = [
            Delta(
                f"child-history-event-{index}", "GEN-38", "progress", "agent",
                {"step": index}, index, f"2026-08-29T00:00:0{index}Z",
            )
            for index in range(2)
        ]
        execution = {
            "agent": "codex", "provider": "openai", "session_id": "session",
            "machine": "M5", "worktree": {
                "state": "safe", "path": "/repo/child", "branch": "fix/child",
                "head": "abc123",
            },
        }
        first = build_checkpoint(
            workstream_id="GEN-38", boundary_id="child-earlier", root_revision=1,
            plan_revision="sha", before_status="In Progress",
            after_status="In Progress", execution=execution, exact_head="abc123",
            evidence=[
                {"kind": "earlier-test", "id": str(index)} for index in range(20)
            ],
            blocker=None, next_action="older child action",
        )
        second = build_checkpoint(
            workstream_id="GEN-38", boundary_id="child-current", root_revision=2,
            plan_revision="sha", before_status="In Progress",
            after_status="In Progress", execution=execution, exact_head="abc123",
            evidence=[{"kind": "current-test", "id": "focused"}],
            blocker=None, next_action="current child action",
            predecessor_event_id=first["event_id"],
        )
        snapshot["child_comments"]["GEN-38"] = [
            *[
                {"id": f"child-event-{index}", "body": encode_event_comment(event)}
                for index, event in enumerate(events)
            ],
            {"id": "child-checkpoint-earlier", "body": encode_checkpoint_comment(first)},
            {"id": "child-checkpoint-current", "body": encode_checkpoint_comment(second)},
        ]
        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )

        bounded = MODULE.compact_context(
            enriched, "GEN-37", max_items=10,
        )["children"][0]
        self.assertNotIn("checkpoint_history", bounded)
        self.assertEqual(bounded["history"]["checkpoints"]["count"], 2)
        self.assertRegex(bounded["history"]["checkpoints"]["sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(bounded["next_action"], "current child action")
        self.assertEqual(bounded["latest_checkpoint"]["evidence"]["count"], 1)
        self.assertNotIn("items", bounded["latest_checkpoint"]["evidence"])

        full = MODULE.compact_context(
            enriched, "GEN-37", include_history=True, max_items=100,
        )["children"][0]
        self.assertEqual(
            [checkpoint["event_id"] for checkpoint in full["checkpoint_history"]],
            [first["event_id"], second["event_id"]],
        )
        self.assertEqual(full["checkpoint_history"][0]["evidence"], first["evidence"])
        truncated = json.loads(json.dumps(enriched))
        truncated["children"][0]["checkpoint_history"] = [
            truncated["children"][0]["checkpoint_history"][-1]
        ]
        with self.assertRaisesRegex(
            MODULE.ResumeError, "invalid_child_checkpoint_history:GEN-38"
        ):
            MODULE.compact_context(
                truncated, "GEN-37", include_history=True, max_items=100,
            )
        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(
                enriched, "GEN-37", include_history=True, max_items=10,
            )

    def test_child_stale_checkpoint_generation_must_be_a_complete_chain(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        truncated = build_checkpoint(
            workstream_id="GEN-38", boundary_id="stale-truncated",
            root_revision=2, plan_revision="old-sha",
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "old-session", "machine": "M3",
                "worktree": {"state": "unknown"},
            },
            exact_head=None, evidence=[], blocker=None,
            next_action="obsolete action",
            predecessor_event_id="missing-predecessor",
        )
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "stale-truncated", "body": encode_checkpoint_comment(truncated),
        }]

        with self.assertRaisesRegex(
            MODULE.ResumeError,
            "invalid_child_checkpoint_history:GEN-38:checkpoint_chain_truncated",
        ):
            MODULE.add_child_material_history(
                snapshot, snapshot["child_comments"], authenticated_route=route,
            )

    def test_legacy_string_child_blocker_is_preserved_as_structured_state(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        event = Delta(
            "legacy-child-blocker", "GEN-38", "material_boundary", "agent",
            {
                "boundary_id": "legacy-boundary",
                "changes": [{
                    "kind": "blocker",
                    "payload": {"blocker": "await exact-head checks"},
                }],
            },
            0, "2026-08-29T00:00:00Z",
        )
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "legacy-child-blocker-comment",
            "body": encode_event_comment(event),
        }]

        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )

        self.assertEqual(
            enriched["children"][0]["blocker"],
            {"text": "await exact-head checks"},
        )

    def test_malformed_child_material_log_refuses_resume(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "planted-malformed",
            "body": "<!-- workstream-delta:v1:not-valid-base64 -->",
        }]
        with self.assertRaisesRegex(MODULE.LinearEventError, "malformed_event_marker"):
            MODULE.add_child_material_history(
                snapshot, snapshot["child_comments"], authenticated_route=route,
            )

    def test_child_route_mismatch_is_separate_reconciliation_blocker(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        snapshot["children"][0]["project"] = {"id": "attacker-project"}
        before_action = snapshot["children"][0]["next_action"]
        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )
        child = enriched["children"][0]
        self.assertEqual(child["next_action"], before_action)
        self.assertEqual(child["reconciliation_blockers"], [{
            "kind": "native_child_cache_drift", "field": "project_id",
            "expected": "project", "observed": "attacker-project",
            "reconciliation_required": True,
        }])

    def test_child_collection_must_cover_every_nonterminal_child(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        with self.assertRaisesRegex(MODULE.ResumeError, "incomplete_child_comment_collection"):
            MODULE.add_child_material_history(
                snapshot, {}, authenticated_route=route,
            )

    def test_terminal_child_comments_are_allowed_in_complete_collection(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        snapshot["child_comments"]["GEN-39"] = [{"id": "terminal-comment"}]
        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )
        self.assertEqual(
            [child["identifier"] for child in enriched["children"]],
            ["GEN-38", "GEN-39"],
        )

    def test_root_event_in_child_log_cannot_overwrite_child_state(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        root_event = Delta(
            "root-event", "GEN-37", "progress", "agent",
            {"next_action": "must remain root-only"}, 0, "2026-08-29T00:00:00Z",
        )
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "misrouted-root-event", "body": encode_event_comment(root_event),
        }]
        with self.assertRaisesRegex(MODULE.LinearEventError, "workstream_id_mismatch"):
            MODULE.add_child_material_history(
                snapshot, snapshot["child_comments"], authenticated_route=route,
            )

    def test_child_material_obligations_count_toward_context_budget(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        event = Delta(
            "child-requirement", "GEN-38", "requirement", "agent",
            {"requirement": "preserve this exact child obligation"},
            0, "2026-08-29T00:00:00Z",
        )
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "child-requirement-comment", "body": encode_event_comment(event),
        }]
        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )
        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(enriched, "GEN-37", max_items=3)

    def test_child_checkpoint_evidence_counts_toward_context_budget(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        checkpoint = build_checkpoint(
            workstream_id="GEN-38", boundary_id="child-evidence", root_revision=0,
            plan_revision="sha", before_status="In Progress",
            after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/child", "branch": "fix/child",
                    "head": "abc123",
                },
            },
            exact_head="abc123",
            evidence=[{"kind": "test", "id": str(index)} for index in range(20)],
            blocker=None, next_action="resume child",
        )
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "child-checkpoint-comment",
            "body": encode_checkpoint_comment(checkpoint),
        }]
        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )

        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(enriched, "GEN-37", max_items=10)

    def test_child_without_comments_preserves_legacy_noop_surface(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        expected_children = json.loads(json.dumps(snapshot["children"]))

        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )

        self.assertEqual(enriched["children"], expected_children)
        self.assertNotIn("material_event_revision", enriched["children"][0])

    def test_nonterminal_root_without_next_action_fails_closed(self):
        snapshot = self.snapshot(); snapshot["root"].pop("next_action")
        with self.assertRaisesRegex(MODULE.ResumeError, "root missing next_action"):
            MODULE.validate_snapshot(snapshot, "GEN-37")

    def test_root_next_action_must_be_nonblank_text(self):
        for value in ({}, [], "   ", None):
            with self.subTest(value=value):
                snapshot = self.snapshot()
                snapshot["root"]["status"] = "Done"
                snapshot["root"]["next_action"] = value
                with self.assertRaisesRegex(MODULE.ResumeError, "invalid_root_next_action"):
                    MODULE.validate_snapshot(snapshot, "GEN-37")

    def test_context_budget_fails_loudly(self):
        with self.assertRaisesRegex(MODULE.ResumeError, "over_budget"):
            MODULE.compact_context(self.snapshot(), "GEN-37", max_bytes=10)

    def test_context_item_budget_fails_loudly(self):
        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(self.snapshot(), "GEN-37", max_items=2)

    def test_raw_transcripts_are_excluded_before_resume_budgeting(self):
        secret = "transcript-only-secret-" + "x" * 100_000
        snapshot = self.snapshot()
        snapshot["decisions"].append({
            "id": "D2", "status": "rejected", "rationale": "keep this",
            "raw_transcript": secret,
            "nested": {"transcript": secret, "summary": "keep summary"},
        })
        context = MODULE.compact_context(snapshot, "GEN-37", max_bytes=16 * 1024)
        encoded = json.dumps(context, sort_keys=True)
        self.assertNotIn("transcript-only-secret", encoded)
        self.assertNotIn("raw_transcript", encoded)
        self.assertEqual(context["decisions"][-1]["rationale"], "keep this")
        self.assertEqual(context["decisions"][-1]["nested"], {"summary": "keep summary"})

        snapshot["decisions"][-1]["rationale"] = secret
        with self.assertRaisesRegex(MODULE.ResumeError, "over_budget"):
            MODULE.compact_context(snapshot, "GEN-37", max_bytes=16 * 1024)

    def test_material_events_override_stale_issue_next_action(self):
        snapshot = self.snapshot()
        snapshot["root"]["revision"] = 91
        first = Delta(
            "event-a", "GEN-37", "requirement", "agent", {"next_action": "first"},
            0, "2026-08-21T00:00:00Z",
        )
        second = Delta(
            "event-b", "GEN-37", "progress", "agent", {"next_action": "current"},
            1, "2026-08-21T00:01:00Z",
        )
        comments = [
            {"id": "comment-a", "body": encode_event_comment(first)},
            {"id": "comment-b", "body": encode_event_comment(second)},
        ]

        enriched = MODULE.add_material_history(snapshot, comments, "GEN-37")
        context = MODULE.compact_context(enriched, "GEN-37")

        self.assertEqual(context["root_revision"], 2)
        self.assertEqual(context["issue_revision"], 91)
        self.assertEqual(context["material_event_revision"], 2)
        self.assertEqual(context["next_action"], "current")
        self.assertEqual(enriched["root"]["issue_revision"], 91)
        self.assertNotIn("material_events", context)
        self.assertEqual(context["history"]["material_events"]["count"], 2)
        self.assertEqual(context["history"]["material_events"]["latest"]["event_id"], "event-b")

        full = MODULE.compact_context(enriched, "GEN-37", include_history=True)
        self.assertEqual(
            [event["event_id"] for event in full["material_events"]],
            ["event-a", "event-b"],
        )
        self.assertTrue(full["history"]["included"])

    def test_material_history_recovers_latest_checkpoint_provenance(self):
        snapshot = self.snapshot()
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="implemented", root_revision=1,
            plan_revision="sha", before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session-1",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/worktree", "branch": "feature/resume",
                    "head": "abc123",
                },
            },
            exact_head="abc123", evidence=[{"kind": "test", "id": "unit"}],
            blocker=None, next_action="validate live resume",
        )
        comments = [
            {"id": "event-1", "body": encode_event_comment(Delta(
                "event-a", "GEN-37", "progress", "checkpoint", {}, 0,
                "2026-08-21T00:00:00Z",
            ))},
            {"id": "checkpoint-1", "body": encode_checkpoint_comment(checkpoint)},
        ]

        context = MODULE.compact_context(
            MODULE.add_material_history(snapshot, comments, "GEN-37"), "GEN-37"
        )

        self.assertEqual(context["latest_checkpoint"]["worktree"]["path"], "/repo/worktree")
        self.assertEqual(context["latest_checkpoint"]["provenance"]["latest"]["machine"], "M5")
        self.assertEqual(context["latest_checkpoint"]["evidence"]["count"], 1)
        self.assertNotIn("items", context["latest_checkpoint"]["evidence"])
        self.assertRegex(context["latest_checkpoint"]["provenance"]["sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(context["surface_availability"]["latest_checkpoint"], "available")
        self.assertEqual(context["next_action"], "validate live resume")
        full = MODULE.compact_context(
            MODULE.add_material_history(snapshot, comments, "GEN-37"), "GEN-37",
            include_history=True,
        )
        self.assertEqual(full["latest_checkpoint"]["provenance_chain"][0]["machine"], "M5")

    def test_material_history_recovers_selected_generation_activation_checkpoint(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.snapshot()
        snapshot["root"]["generation_transition_tip_event_id"] = "transition-tip"
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="generation-activation",
            root_revision=0, plan_revision="sha",
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "next",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/next", "branch": "next",
                    "head": "abc123",
                },
            },
            exact_head="abc123", evidence=[{"kind": "test", "id": "activation"}],
            blocker=None, next_action="Resume the activated generation.",
        )
        with mock.patch(
            "workstream_generation.selected_activation_checkpoints",
            return_value=[(checkpoint, "transition-comment")],
        ) as selected:
            enriched = MODULE.add_material_history(
                snapshot, [], "GEN-37", authenticated_route=route,
            )
        context = MODULE.compact_context(enriched, "GEN-37")
        self.assertEqual(
            context["latest_checkpoint"]["checkpoint_event_id"],
            checkpoint["event_id"],
        )
        self.assertEqual(
            context["latest_checkpoint"]["worktree"]["path"], "/repo/next",
        )
        selected.assert_called_once_with(
            [], workstream_id="GEN-37", transition_event_id="transition-tip",
            active_plan_revision="sha", authenticated_route=route,
        )

    def test_generation_activation_checkpoint_requires_authenticated_route(self):
        snapshot = self.snapshot()
        snapshot["root"]["generation_transition_tip_event_id"] = "transition-tip"
        with self.assertRaisesRegex(
            MODULE.ResumeError, "generation_activation_checkpoint_route_missing",
        ):
            MODULE.add_material_history(snapshot, [], "GEN-37")

    def test_material_event_after_checkpoint_supersedes_checkpoint_next_action(self):
        snapshot = self.snapshot()
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="checkpointed", root_revision=1,
            plan_revision="sha", before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session-1",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/worktree", "branch": "feature/resume",
                    "head": "abc123",
                },
            },
            exact_head="abc123", evidence=[], blocker=None,
            next_action="checkpoint action",
        )
        comments = [
            {"id": "event-1", "body": encode_event_comment(Delta(
                "event-a", "GEN-37", "progress", "agent",
                {"next_action": "acknowledged action"}, 0,
                "2026-08-21T00:00:00Z",
            ))},
            {"id": "checkpoint-1", "body": encode_checkpoint_comment(checkpoint)},
            {"id": "event-2", "body": encode_event_comment(Delta(
                "event-b", "GEN-37", "progress", "agent",
                {"next_action": "new action"}, 1,
                "2026-08-21T00:01:00Z",
            ))},
        ]
        context = MODULE.compact_context(
            MODULE.add_material_history(snapshot, comments, "GEN-37"), "GEN-37"
        )
        self.assertEqual(context["next_action"], "new action")

    def test_later_canonical_event_beats_checkpoint_despite_stale_writer_revision(self):
        snapshot = self.snapshot()
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="checkpointed", root_revision=1,
            plan_revision="sha", before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session-1",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/worktree", "branch": "feature/resume",
                    "head": "abc123",
                },
            },
            exact_head="abc123", evidence=[], blocker=None,
            next_action="checkpoint action",
        )
        comments = [
            {"id": "event-1", "body": encode_event_comment(Delta(
                "event-a", "GEN-37", "progress", "agent", {}, 0,
                "2026-08-21T00:00:00Z",
            ))},
            {"id": "checkpoint-1", "body": encode_checkpoint_comment(checkpoint)},
            {"id": "event-2", "body": encode_event_comment(Delta(
                "event-b", "GEN-37", "progress", "agent",
                {"next_action": "later action"}, 0,
                "2026-08-21T00:01:00Z",
            ))},
        ]
        context = MODULE.compact_context(
            MODULE.add_material_history(snapshot, comments, "GEN-37"), "GEN-37"
        )
        self.assertEqual(context["next_action"], "later action")

    def test_material_history_exposes_absent_checkpoint_without_fabrication(self):
        context = MODULE.compact_context(
            MODULE.add_material_history(self.snapshot(), [], "GEN-37"), "GEN-37"
        )
        self.assertIsNone(context["latest_checkpoint"])
        self.assertEqual(context["checkpoint_recovery"]["state"], "not_found")
        self.assertEqual(context["surface_availability"]["latest_checkpoint"], "available")

    def test_stale_plan_checkpoint_does_not_brick_current_resume(self):
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="old-plan", root_revision=0,
            plan_revision="old-sha", before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "old-session",
                "machine": "M3", "worktree": {"state": "unknown"},
            },
            exact_head=None, evidence=[], blocker=None, next_action="obsolete",
        )
        context = MODULE.compact_context(
            MODULE.add_material_history(
                self.snapshot(),
                [{"id": "old-checkpoint", "body": encode_checkpoint_comment(checkpoint)}],
                "GEN-37",
            ),
            "GEN-37",
        )
        self.assertIsNone(context["latest_checkpoint"])
        self.assertEqual(
            context["checkpoint_recovery"],
            {"state": "stale_plan", "stale_plan_count": 1},
        )

    def test_stale_root_checkpoint_generation_must_be_a_complete_chain(self):
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="old-plan-truncated",
            root_revision=2, plan_revision="old-sha",
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "old-session", "machine": "M3",
                "worktree": {"state": "unknown"},
            },
            exact_head=None, evidence=[], blocker=None, next_action="obsolete",
            predecessor_event_id="missing-predecessor",
        )
        with self.assertRaisesRegex(
            MODULE.ResumeError,
            "invalid_checkpoint_history:GEN-37:checkpoint_chain_truncated",
        ):
            MODULE.add_material_history(
                self.snapshot(), [{
                    "id": "old-checkpoint", "body": encode_checkpoint_comment(checkpoint),
                }], "GEN-37",
            )

    def test_checkpoint_cannot_claim_unrecorded_material_revision(self):
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="ahead", root_revision=1,
            plan_revision="sha", before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session-1",
                "machine": "M5", "worktree": {"state": "unknown"},
            },
            exact_head=None, evidence=[], blocker=None, next_action="cannot trust this",
        )
        with self.assertRaisesRegex(MODULE.ResumeError, "checkpoint_ahead"):
            MODULE.add_material_history(
                self.snapshot(),
                [{"id": "checkpoint-1", "body": encode_checkpoint_comment(checkpoint)}],
                "GEN-37",
            )

    def test_material_event_validation_fails_closed(self):
        snapshot = self.snapshot()
        event = {
            "event_id": "duplicate", "workstream_id": "GEN-37", "kind": "progress",
            "source": "agent", "payload": {}, "expected_revision": 0,
            "created_at": "2026-08-21T00:00:00Z",
        }
        snapshot["material_events"] = [event, dict(event)]
        snapshot["material_event_revision"] = 2
        with self.assertRaisesRegex(MODULE.ResumeError, "duplicate_material_event"):
            MODULE.compact_context(snapshot, "GEN-37")

    def test_event_next_action_must_be_nonblank_text(self):
        payloads = (
            {"next_action": {}},
            {"next_action": "   "},
            {"boundary_id": "turn-1", "changes": [
                {"kind": "next_action", "payload": {"next_action": []}},
            ]},
        )
        for index, payload in enumerate(payloads):
            with self.subTest(payload=payload):
                snapshot = self.snapshot()
                snapshot["root"]["revision"] = 1
                snapshot["material_events"] = [{
                    "event_id": f"event-{index}", "workstream_id": "GEN-37",
                    "kind": "material_boundary" if "changes" in payload else "progress",
                    "source": "agent", "payload": payload, "expected_revision": 0,
                    "created_at": "2026-08-21T00:00:00Z",
                }]
                snapshot["material_event_revision"] = 1
                with self.assertRaisesRegex(MODULE.ResumeError, "invalid_event_next_action"):
                    MODULE.compact_context(snapshot, "GEN-37")

    def test_offline_material_revision_must_match_root(self):
        snapshot = self.snapshot()
        snapshot["material_events"] = []
        snapshot["material_event_revision"] = 0
        with self.assertRaisesRegex(MODULE.ResumeError, "material_event_revision_mismatch"):
            MODULE.compact_context(snapshot, "GEN-37")

    def test_offline_fabricated_checkpoint_fails_closed(self):
        snapshot = self.snapshot()
        snapshot["latest_checkpoint"] = {
            "workstream_id": "GEN-37", "plan_revision": "sha",
        }
        snapshot["checkpoint_recovery"] = {"state": "current", "stale_plan_count": 0}
        with self.assertRaisesRegex(MODULE.ResumeError, "invalid_latest_checkpoint_fields"):
            MODULE.compact_context(snapshot, "GEN-37")

    def test_concurrent_conflicting_next_actions_fail_closed(self):
        comments = [
            {"id": "event-a", "body": encode_event_comment(Delta(
                "event-a", "GEN-37", "progress", "agent", {"next_action": "A"},
                0, "2026-08-21T00:00:00Z",
            ))},
            {"id": "event-b", "body": encode_event_comment(Delta(
                "event-b", "GEN-37", "progress", "agent", {"next_action": "B"},
                0, "2026-08-21T00:00:01Z",
            ))},
        ]
        with self.assertRaisesRegex(MODULE.ResumeError, "conflicting_concurrent_next_action"):
            MODULE.add_material_history(self.snapshot(), comments, "GEN-37")

    def test_material_boundary_nested_next_action_is_resumed(self):
        event = Delta(
            "event-boundary", "GEN-37", "material_boundary", "user_turn",
            {
                "boundary_id": "turn-1",
                "changes": [
                    {"kind": "requirement", "payload": {"text": "new scope"}},
                    {"kind": "next_action", "payload": {"next_action": "build the adapter"}},
                ],
            },
            0, "2026-08-21T00:00:00Z",
        )
        context = MODULE.compact_context(
            MODULE.add_material_history(
                self.snapshot(),
                [{"id": "event-boundary", "body": encode_event_comment(event)}],
                "GEN-37",
            ),
            "GEN-37",
        )
        self.assertEqual(context["next_action"], "build the adapter")

    def test_malformed_material_boundary_fails_closed(self):
        event = Delta(
            "event-boundary", "GEN-37", "material_boundary", "user_turn",
            {"boundary_id": "turn-1", "changes": "not-a-list"}, 0,
            "2026-08-21T00:00:00Z",
        )
        envelope = linear_events_module._canonical_event(event)
        encoded = base64.urlsafe_b64encode(json.dumps(
            envelope, sort_keys=True, separators=(",", ":"),
        ).encode()).decode().rstrip("=")
        body = f"{linear_events_module.EVENT_PREFIX}{encoded} -->"
        with self.assertRaisesRegex(MODULE.ResumeError, "malformed_material_boundary"):
            MODULE.add_material_history(
                self.snapshot(),
                [{"id": "event-boundary", "body": body}],
                "GEN-37",
            )

    def test_direct_snapshot_malformed_material_boundary_fails_closed(self):
        snapshot = self.snapshot()
        snapshot["root"]["revision"] = 1
        snapshot["material_events"] = [{
            "event_id": "event-boundary", "workstream_id": "GEN-37",
            "kind": "material_boundary", "source": "user_turn",
            "payload": {"boundary_id": "turn-1", "changes": "not-a-list"},
            "expected_revision": 0, "created_at": "2026-08-21T00:00:00Z",
        }]
        snapshot["material_event_revision"] = 1
        with self.assertRaisesRegex(MODULE.ResumeError, "malformed_material_boundary"):
            MODULE.compact_context(snapshot, "GEN-37")

    def test_compact_and_full_repair_surfaces_preserve_digest_and_originals(self):
        snapshot = self.snapshot()
        snapshot["children"] = []
        snapshot["root"]["revision"] = 2
        effective = [{
            "event_id": "flat", "workstream_id": "GEN-37",
            "kind": "material_boundary", "source": "system",
            "payload": {"boundary_id": "repair:flat", "changes": [{
                "kind": "progress", "payload": {"next_action": "normalized"},
            }]},
            "expected_revision": 0, "created_at": "2026-08-30T00:00:00Z",
        }, {
            "event_id": "repair", "workstream_id": "GEN-37",
            "kind": "material_semantic_repair", "source": "system",
            "payload": {}, "expected_revision": 1,
            "created_at": "2026-08-30T00:01:00Z",
        }]
        raw = copy.deepcopy(effective)
        raw[0]["payload"] = {"next_action": "normalized", "progress": "flat"}
        binding = {"event_id": "flat", "remote_comment_id": "remote-flat"}
        snapshot.update({
            "material_events": effective, "raw_material_events": raw,
            "material_semantic_repairs": [binding],
            "material_event_revision": 2,
        })
        compact = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(compact["next_action"], "normalized")
        self.assertEqual(compact["material_semantic_repair"]["count"], 1)
        self.assertNotIn("material_semantic_repairs", compact["history"])
        self.assertEqual(compact["history"]["raw_material_events"]["count"], 2)
        self.assertRegex(
            compact["history"]["raw_material_events"]["sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertNotIn("latest", compact["history"]["raw_material_events"])
        self.assertNotIn("raw_material_events", compact)
        full = MODULE.compact_context(snapshot, "GEN-37", include_history=True)
        self.assertEqual(full["raw_material_events"], raw)
        self.assertEqual(full["material_semantic_repairs"], [binding])

    def test_repair_audit_metadata_does_not_crowd_out_exact_obligations(self):
        snapshot = self.snapshot()
        snapshot["children"] = []
        snapshot["root"]["revision"] = 3
        effective = [{
            "event_id": "flat", "workstream_id": "GEN-37",
            "kind": "material_boundary", "source": "system",
            "payload": {"boundary_id": "repair:flat", "changes": [{
                "kind": "progress", "payload": {"next_action": "normalized"},
            }]},
            "expected_revision": 0, "created_at": "2026-08-30T00:00:00Z",
        }, {
            "event_id": "repair", "workstream_id": "GEN-37",
            "kind": "material_semantic_repair", "source": "system",
            "payload": {}, "expected_revision": 1,
            "created_at": "2026-08-30T00:01:00Z",
        }, {
            "event_id": "requirement", "workstream_id": "GEN-37",
            "kind": "requirement", "source": "user_turn",
            "payload": {"requirement": ""}, "expected_revision": 2,
            "created_at": "2026-08-30T00:02:00Z",
        }]
        raw = copy.deepcopy(effective)
        raw[0]["payload"] = {"next_action": "normalized", "progress": "flat"}
        binding = {"event_id": "flat", "remote_comment_id": "remote-flat"}
        snapshot.update({
            "material_events": effective, "raw_material_events": raw,
            "material_semantic_repairs": [binding],
            "material_event_revision": 3,
        })

        encode = lambda value: json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()
        baseline = MODULE.compact_context(
            snapshot, "GEN-37", max_bytes=1024 * 1024,
        )
        exact_requirement = "x" * (
            MODULE.DEFAULT_RESUME_MAX_BYTES - len(encode(baseline)) - 64
        )
        effective[-1]["payload"]["requirement"] = exact_requirement
        raw[-1]["payload"]["requirement"] = exact_requirement
        compact = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(
            compact["uncheckpointed_material_obligations"][-1]["payload"]
            ["requirement"],
            exact_requirement,
        )
        self.assertLessEqual(len(encode(compact)), MODULE.DEFAULT_RESUME_MAX_BYTES)

        # Reconstruct the redundant PR #48 metadata: it alone pushes the same
        # validated actionable context over the default contract.
        legacy = copy.deepcopy(compact)
        legacy["history"]["material_semantic_repairs"] = compact[
            "material_semantic_repair"
        ]
        legacy["history"]["raw_material_events"]["latest"] = {
            "event_id": "requirement", "kind": "requirement",
            "created_at": "2026-08-30T00:02:00Z",
        }
        self.assertGreater(len(encode(legacy)), MODULE.DEFAULT_RESUME_MAX_BYTES)

    def test_uncheckpointed_requirement_payload_is_not_replaced_by_digest(self):
        snapshot = self.snapshot()
        snapshot["root"]["revision"] = 1
        snapshot["material_events"] = [{
            "event_id": "requirement-1", "workstream_id": "GEN-37",
            "kind": "requirement", "source": "user_turn",
            "payload": {"requirement": "must preserve offline recovery"},
            "expected_revision": 0, "created_at": "2026-08-21T00:00:00Z",
        }]
        snapshot["material_event_revision"] = 1
        context = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(context["uncheckpointed_material_obligations"], [{
            "event_id": "requirement-1", "kind": "requirement",
            "payload": {"requirement": "must preserve offline recovery"},
        }])

    def test_default_budget_accepts_bounded_actionable_resume_over_16k(self):
        snapshot = self.snapshot()
        requirement = "preserve exact recovery context " + ("x" * 18000)
        snapshot["root"]["revision"] = 1
        snapshot["material_events"] = [{
            "event_id": "requirement-large", "workstream_id": "GEN-37",
            "kind": "requirement", "source": "user_turn",
            "payload": {"requirement": requirement},
            "expected_revision": 0, "created_at": "2026-08-21T00:00:00Z",
        }]
        snapshot["material_event_revision"] = 1

        with self.assertRaisesRegex(MODULE.ResumeError, "over_budget"):
            MODULE.compact_context(snapshot, "GEN-37", max_bytes=16 * 1024)
        context = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(
            context["uncheckpointed_material_obligations"][0]["payload"]
            ["requirement"],
            requirement,
        )
        self.assertLess(
            len(MODULE.json.dumps(
                context, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode()),
            MODULE.DEFAULT_RESUME_MAX_BYTES,
        )

    def test_mature_full_authority_resume_compacts_without_stranding(self):
        snapshot = self.snapshot()
        snapshot["children"] = []
        for index in range(6):
            snapshot["children"].append({
                "identifier": f"GEN-{38 + index}",
                "title": f"Open delivery slice {index}",
                "status": "In Progress",
                "next_action": (
                    f"Continue slice {index}: verify the exact landing boundary. "
                    + (chr(97 + index) * 3300)
                ),
                "blocker": {
                    "text": f"Retain blocker semantics for slice {index}. "
                    + (chr(65 + index) * 1700),
                },
            })
        snapshot["decisions"] = [{
            "id": "D-budget", "status": "accepted",
            "decision": "Keep the fixed 24 KiB default. " + ("d" * 4000),
        }]
        snapshot["material_events"] = [{
            "event_id": "requirement-current", "workstream_id": "GEN-37",
            "kind": "requirement", "source": "user_turn",
            "payload": {
                "requirement": "Preserve full resume authority. " + ("r" * 4000),
            },
            "expected_revision": 0, "created_at": "2026-08-31T12:01:00Z",
        }, {
            "event_id": "followup-current", "workstream_id": "GEN-37",
            "kind": "followup", "source": "user_turn",
            "payload": {
                "followup": "Run the deferred audit after recovery. " + ("f" * 3000),
            },
            "expected_revision": 1, "created_at": "2026-08-31T12:02:00Z",
        }]
        snapshot["material_event_revision"] = 2
        snapshot["root"]["revision"] = 2
        snapshot = self.live_snapshot(snapshot, {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project",
            "root_issue_id": "33333333-3333-4333-8333-333333333333",
        })
        snapshot = self.full_authority_snapshot(snapshot)
        requested_focus = {
            "kind": "owned_child", "identifier": "GEN-38",
            "issue_id": "child-0",
            "parent_issue_id": snapshot["authenticated_route"]["root_issue_id"],
            "root_identifier": "GEN-37",
            "repository_key": "github.com:id:R_agent_workstream",
            "status": "In Progress",
        }
        snapshot["requested_focus"] = copy.deepcopy(requested_focus)
        before = copy.deepcopy(snapshot)

        unbounded = MODULE.compact_context(
            snapshot, "GEN-37", max_bytes=64 * 1024,
            require_projection_authority=True,
        )
        unbounded_bytes = len(MODULE._default_output_bytes(unbounded))
        self.assertGreater(unbounded_bytes, 46_879)
        self.assertLess(unbounded_bytes, 49_000)
        with mock.patch.object(MODULE, "_CURRENT_DETAIL_EXCERPT_LIMITS", ()), \
             mock.patch.object(
                 MODULE, "_bounded_authority_envelope",
                 side_effect=lambda current, **_kwargs: current,
             ), mock.patch.object(
                 MODULE, "_fixed_frontier_authority_envelope",
                 side_effect=lambda current, **_kwargs: current,
             ):
            with self.assertRaisesRegex(
                MODULE.ResumeError,
                f"resume_context_over_budget:{unbounded_bytes}>"
                f"{MODULE.DEFAULT_RESUME_MAX_BYTES}",
            ):
                MODULE.compact_context(
                    snapshot, "GEN-37", require_projection_authority=True,
                )

        first = MODULE.compact_context(
            snapshot, "GEN-37", require_projection_authority=True,
        )
        second = MODULE.compact_context(
            snapshot, "GEN-37", require_projection_authority=True,
        )
        encoded = MODULE._default_output_bytes(first)
        self.assertLessEqual(len(encoded), MODULE.DEFAULT_RESUME_MAX_BYTES)
        self.assertEqual(first, second)
        self.assertEqual(snapshot, before)
        self.assertEqual(first["resume_authority"], "full")
        self.assertEqual(first["requested_focus"], requested_focus)
        self.assertEqual(unbounded["requested_focus"], requested_focus)
        self.assertEqual(
            first["context_schema"]["envelope"],
            "verbose_current_detail_v1",
        )
        self.assertEqual(
            first["deferred_audit_detail"]["state"],
            "verbose_current_detail_deferred",
        )
        self.assertGreater(first["deferred_audit_detail"]["field_count"], 0)
        self.assertTrue(
            first["deferred_audit_detail"]["hydration_required_before_action"]
        )
        self.assertRegex(
            first["deferred_audit_detail"]["fields_sha256"], r"^[0-9a-f]{64}$",
        )
        self.assertEqual(
            first["deferred_audit_detail"]["audit_route"]["command"],
            "workstreamctl resume GEN-38 "
            "--max-bytes 2147483647 --max-items 2147483647",
        )
        self.assertEqual(
            first["deferred_audit_detail"]["full_history_route"]["command"],
            "workstreamctl resume GEN-38 --include-history "
            "--max-bytes 2147483647 --max-items 2147483647",
        )
        audit_route = first["deferred_audit_detail"]["audit_route"]
        self.assertEqual(
            audit_route["launcher"],
            "current_workstream_resume_skill_script",
        )
        self.assertEqual(audit_route["args"], [
            "GEN-38", "--max-bytes", "2147483647",
            "--max-items", "2147483647",
        ])
        self.assertEqual(
            first["deferred_audit_detail"]["full_history_route"]["args"],
            ["GEN-38", "--include-history", "--max-bytes", "2147483647",
             "--max-items", "2147483647"],
        )
        launcher = (
            Path(MODULE.__file__).resolve().parents[2]
            / "workstream-resume" / "scripts" / "workstream_resume.py"
        )
        help_result = subprocess.run(
            [sys.executable, str(launcher), "--help"],
            capture_output=True, check=False,
        )
        self.assertEqual(help_result.returncode, 0, help_result.stderr.decode())
        for field in first["deferred_audit_detail"]["fields"]:
            self.assertTrue(field["json_pointer"].startswith("/"))
            self.assertRegex(field["sha256"], r"^[0-9a-f]{64}$")
            self.assertGreater(field["utf8_bytes"], 0)
        self.assertEqual(
            first["uncheckpointed_material_obligations"][0]["kind"],
            "requirement",
        )
        self.assertIn(
            "Preserve full resume authority",
            first["uncheckpointed_material_obligations"][0]["payload"]
            ["requirement"],
        )
        self.assertEqual(
            first["uncheckpointed_material_obligations"][1]["kind"],
            "followup",
        )
        self.assertEqual(first["decisions"][0]["id"], "D-budget")
        self.assertIn("Keep the fixed 24 KiB", first["decisions"][0]["decision"])
        self.assertEqual(len(first["children"]), 6)
        self.assertTrue(all(child["next_action"] for child in first["children"]))

        with mock.patch.object(MODULE, "_CURRENT_DETAIL_EXCERPT_LIMITS", ()):
            bounded = MODULE.compact_context(
                snapshot, "GEN-37", require_projection_authority=True,
            )
        self.assertEqual(
            bounded["context_schema"]["envelope"], "bounded_authority_v1",
        )
        self.assertEqual(
            bounded["execution_frontier"]["child_dependency_graph"]["sha256"],
            hashlib.sha256(b"[]").hexdigest(),
        )
        self.assertEqual(
            bounded["deferred_audit_detail"]["state"],
            "bounded_authority_envelope",
        )
        self.assertTrue(
            bounded["deferred_audit_detail"]
            ["hydration_required_before_action"]
        )
        self.assertLessEqual(
            len(MODULE._default_output_bytes(bounded)),
            MODULE.DEFAULT_RESUME_MAX_BYTES,
        )

    def test_oversized_exact_metadata_is_a_semantic_refusal_not_byte_stranding(self):
        cases = (
            ("identifier", "GEN-" + ("7" * 129),
             "workstream identifier exceeds schema byte limit"),
            ("plan_revision", "p" * 257,
             "plan revision exceeds schema byte limit"),
            ("revision", 1 << 63,
             "root revision must be a non-negative 64-bit integer"),
        )
        for field, value, error in cases:
            with self.subTest(field=field):
                snapshot = self.snapshot()
                snapshot["root"][field] = value
                token = value if field == "identifier" else "GEN-37"
                with self.assertRaisesRegex(MODULE.ResumeError, error):
                    MODULE.compact_context(snapshot, token)

        snapshot = self.full_authority_snapshot(self.snapshot())
        snapshot["authenticated_route"]["unbounded-extra-route-key"] = "x"
        with self.assertRaisesRegex(
            MODULE.ResumeError, "invalid_authenticated_route",
        ):
            MODULE.compact_context(
                snapshot, "GEN-37", require_projection_authority=True,
            )

        snapshot = self.snapshot()
        snapshot["authenticated_route"] = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root",
            "unbounded-extra-route-key": "x",
        }
        with self.assertRaisesRegex(
            MODULE.ResumeError, "invalid_authenticated_route",
        ):
            MODULE.compact_context(snapshot, "GEN-37")

    def test_high_cardinality_structured_state_uses_bounded_authority_envelope(self):
        snapshot = self.snapshot()
        snapshot["children"] = snapshot["children"][:1]
        snapshot["children"][0]["owner"] = "codex-owner"
        snapshot["relations"] = [{
            "type": "blocked_by", "target": {
                "workspace_id": "external-workspace",
                "issue_id": "11111111-1111-4111-8111-111111111111",
                "identifier": "OPS-900",
            },
        }]
        snapshot["decisions"] = [{
            "id": f"D-{index:02d}", "status": "accepted",
            "decision": f"Execute reviewed decision {index}",
            **{
                key: f"{key} semantics for {index} " + (key * 300)
                for key in (
                    "blocker", "followup", "message", "next_action", "notes",
                    "rationale", "reason", "recommendation", "requirement",
                    "review_condition", "summary", "text", "title",
                )
            },
            "implementation": {
                "steps": [{
                    "ordinal": step,
                    "opaque_audit_detail": (f"{index:02d}-{step:02d}-" * 800),
                } for step in range(8)],
            },
        } for index in range(96)]
        exact_route = {
            "workspace_id": "11111111-1111-4111-8111-111111111111",
            "team_id": "22222222-2222-4222-8222-222222222222",
            "project_id": "33333333-3333-4333-8333-333333333333",
            "root_issue_id": "44444444-4444-4444-8444-444444444444",
        }
        snapshot["root"]["url"] = (
            "https://linear.app/generous-corp/issue/GEN-37/"
            "agent-workstream-continuity"
        )
        snapshot = self.live_snapshot(snapshot, exact_route)
        snapshot["root"]["project"]["name"] = "Linear Integration"
        snapshot = self.full_authority_snapshot(snapshot, route=exact_route)
        requested_focus = {
            "kind": "owned_child", "identifier": "GEN-38",
            "issue_id": "child-0",
            "parent_issue_id": snapshot["authenticated_route"]["root_issue_id"],
            "root_identifier": "GEN-37",
            "repository_key": "github.com:id:R_agent_workstream",
            "status": "In Progress",
        }
        snapshot["requested_focus"] = copy.deepcopy(requested_focus)
        unbounded = MODULE.compact_context(
            snapshot, "GEN-37", max_bytes=8 * 1024 * 1024,
            require_projection_authority=True,
        )
        self.assertGreater(
            len(MODULE._default_output_bytes(unbounded)), 2 * 1024 * 1024,
        )

        context = MODULE.compact_context(
            snapshot, "GEN-37", require_projection_authority=True,
        )
        encoded = MODULE._default_output_bytes(context)
        self.assertLessEqual(len(encoded), MODULE.DEFAULT_RESUME_MAX_BYTES)
        self.assertEqual(context["resume_authority"], "full")
        self.assertEqual(context["context_url"], snapshot["root"]["url"])
        self.assertEqual(context["authenticated_route"], exact_route)
        self.assertEqual(
            context["authenticated_source"],
            snapshot["authenticated_source"],
        )
        self.assertEqual(context["project_name"], "Linear Integration")
        self.assertEqual(context["requested_focus"], requested_focus)
        self.assertEqual(unbounded["requested_focus"], requested_focus)
        self.assertEqual(
            context["context_schema"]["envelope"],
            "fixed_frontier_authority_v1",
        )
        self.assertEqual(
            context["deferred_audit_detail"]["state"],
            "fixed_frontier_authority_envelope",
        )
        self.assertTrue(
            context["deferred_audit_detail"]
            ["hydration_required_before_action"]
        )
        self.assertEqual(
            context["authority_scope"]["execution_frontier"],
            "complete_digest_bound_excerpts",
        )
        self.assertEqual(
            [decision[0] for decision in context["execution_frontier"]["decisions"]],
            [f"D-{index:02d}" for index in range(96)],
        )
        self.assertTrue(all(
            decision[1] == "accepted" and decision[2]
            for decision in context["execution_frontier"]["decisions"]
        ))
        decision_field = next(
            field for field in context["deferred_audit_detail"]["fields"]
            if field["json_pointer"] == "/decisions"
        )
        self.assertEqual(decision_field["item_count"], 96)
        self.assertEqual(
            decision_field["sha256"], hashlib.sha256(json.dumps(
                unbounded["decisions"], ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode()).hexdigest(),
        )
        child_field = next(
            field for field in context["deferred_audit_detail"]["fields"]
            if field["json_pointer"] == "/children"
        )
        self.assertEqual(
            child_field["sha256"], hashlib.sha256(json.dumps(
                unbounded["children"], ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode()).hexdigest(),
        )
        self.assertNotIn(
            "--include-history",
            context["deferred_audit_detail"]["audit_route"]["command"],
        )
        self.assertIn(
            "--include-history",
            context["deferred_audit_detail"]["full_history_route"]["command"],
        )
        self.assertEqual(
            context["deferred_audit_detail"]["audit_route"]["args"][0],
            "GEN-38",
        )
        self.assertEqual(
            context["deferred_audit_detail"]["full_history_route"]["args"][0],
            "GEN-38",
        )
        child = context["execution_frontier"]["children"][0]
        self.assertEqual(child[1], "In Progress")
        self.assertEqual(child[2], "codex-owner")
        self.assertEqual(child[4], "adapter")
        self.assertEqual(
            context["execution_frontier"]["dependencies"],
            [["blocked_by", "OPS-900"]],
        )
        self.assertEqual(
            context["execution_frontier"]["child_dependency_graph"]["relations"],
            [],
        )
        self.assertEqual(
            context["execution_frontier"]["child_dependency_graph"]
            ["authority"]["sha256"],
            hashlib.sha256(b"[]").hexdigest(),
        )
        self.assertEqual(
            context["deferred_audit_detail"]["hydration_selectors"]
            ["child_dependency_graph"],
            ".dependency_graph",
        )

    def test_hydration_route_does_not_infer_child_from_verbose_content(self):
        snapshot = self.snapshot()
        snapshot["children"] = snapshot["children"][:1]
        snapshot["root"]["next_action"] = (
            "Resume GEN-38 after reviewing the child. " + ("x" * 40_000)
        )
        snapshot = self.full_authority_snapshot(snapshot)
        context = MODULE.compact_context(
            snapshot, "GEN-37", require_projection_authority=True,
        )
        self.assertNotIn("requested_focus", context)
        self.assertEqual(
            context["deferred_audit_detail"]["audit_route"]["args"][0],
            "GEN-37",
        )
        self.assertEqual(
            context["deferred_audit_detail"]["full_history_route"]["args"][0],
            "GEN-37",
        )

    def test_oversized_forged_child_focus_refuses_before_hydration_routing(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project",
            "root_issue_id": "33333333-3333-4333-8333-333333333333",
        }

        def oversized(kind):
            snapshot = self.snapshot()
            snapshot["children"] = snapshot["children"][:1]
            if kind == "verbose":
                snapshot["root"]["next_action"] = "Continue. " + ("v" * 40_000)
            else:
                snapshot["decisions"] = [{
                    "id": f"D-{index:02d}", "status": "accepted",
                    "decision": "Review " + (f"{index:02d}" * 4_000),
                } for index in range(96)]
            snapshot = self.full_authority_snapshot(
                self.live_snapshot(snapshot, route)
            )
            snapshot["requested_focus"] = {
                "kind": "owned_child", "identifier": "GEN-38",
                "issue_id": "child-0", "parent_issue_id": route["root_issue_id"],
                "root_identifier": "GEN-37",
                "repository_key": "github.com:id:R_agent_workstream",
                "status": "In Progress",
            }
            return snapshot

        def nonexistent(focus):
            focus["identifier"] = "GEN-999"
            focus["issue_id"] = "child-999"

        mutations = {
            "nonexistent": nonexistent,
            "uuid": lambda focus: focus.__setitem__("issue_id", "child-forged"),
            "repository": lambda focus: focus.__setitem__(
                "repository_key", "github.com:id:R_other",
            ),
            "state": lambda focus: focus.__setitem__("status", "Done"),
        }
        for kind in ("verbose", "fixed"):
            for name, mutate in mutations.items():
                with self.subTest(envelope=kind, mutation=name):
                    snapshot = oversized(kind)
                    mutate(snapshot["requested_focus"])
                    with mock.patch.object(
                        MODULE, "_compact_verbose_current_detail",
                        wraps=MODULE._compact_verbose_current_detail,
                    ) as compactor, self.assertRaisesRegex(
                        MODULE.ResumeError, "invalid_requested_child_focus",
                    ):
                        MODULE.compact_context(
                            snapshot, "GEN-37",
                            require_projection_authority=True,
                        )
                    compactor.assert_not_called()

    def test_mature_resume_contradiction_refuses_before_compaction(self):
        snapshot = self.snapshot()
        snapshot["material_events"] = [{
            "event_id": "action-a", "workstream_id": "GEN-37",
            "kind": "requirement", "source": "user_turn",
            "payload": {"next_action": "take route A " + ("a" * 26000)},
            "expected_revision": 0, "created_at": "2026-08-31T12:01:00Z",
        }, {
            "event_id": "action-b", "workstream_id": "GEN-37",
            "kind": "requirement", "source": "user_turn",
            "payload": {"next_action": "take route B " + ("b" * 26000)},
            "expected_revision": 0, "created_at": "2026-08-31T12:02:00Z",
        }]
        snapshot["material_event_revision"] = 2
        snapshot["root"]["revision"] = 2
        snapshot = self.full_authority_snapshot(snapshot)
        with self.assertRaisesRegex(
            MODULE.ResumeError, "conflicting_concurrent_next_action:0",
        ):
            MODULE.compact_context(
                snapshot, "GEN-37", require_projection_authority=True,
            )

    def test_fixed_frontier_obligation_rows_retain_exact_source_indexes(self):
        context = {
            "context_schema": {
                "name": "agent-workstream.resume-context", "version": 2,
                "representation": "compact_validated",
            },
            "workstream_id": "GEN-37", "plan_revision": "a" * 64,
            "root_revision": 3, "material_event_revision": 3,
            "resume_authority": "full",
            "uncheckpointed_material_obligations": [{
                "event_id": "root-event", "kind": "requirement",
                "payload": {"requirement": "root action"},
            }],
            "children": [{
                "identifier": "GEN-38", "status": "In Progress",
                "next_action": "child action",
                "uncheckpointed_material_obligations": [{
                    "event_id": "child-event", "kind": "followup",
                    "payload": {"followup": "child followup"},
                }],
                "pending_child_proposals": [{
                    "proposal_id": "proposal-1", "next_action": "review proposal",
                }],
            }],
            "decisions": [], "choice_events": [], "relations": [],
        }
        envelope = MODULE._fixed_frontier_authority_envelope(
            context, token="GEN-37",
        )
        rows = envelope["execution_frontier"]["obligations"]
        self.assertEqual([row[0] for row in rows], [
            ["root", 0], ["child", 0, 0], ["proposal", 0, 0],
        ])
        self.assertEqual(
            envelope["execution_frontier"]["columns"]["obligations"],
            ["source", "child", "id", "kind", "action"],
        )
        self.assertEqual(
            envelope["deferred_audit_detail"]["obligation_selector_rules"],
            {
                "root": ".uncheckpointed_material_obligations[source[1]]",
                "child": (
                    ".children[source[1]].uncheckpointed_material_obligations"
                    "[source[2]]"
                ),
                "proposal": (
                    ".children[source[1]].pending_child_proposals[source[2]]"
                ),
            },
        )

    def test_max_item_child_frontier_cannot_reintroduce_byte_refusal(self):
        snapshot = self.snapshot()
        snapshot["decisions"] = []
        snapshot["children"] = [{
            "identifier": f"GEN-{38 + index}",
            "title": f"Executable child {index} " + ("t" * 500),
            "status": "In Progress", "status_type": "started",
            "owner": "\\\n" * 10,
            "next_action": "\n\\" * 10,
            "blocker": "\\\n" * 10,
            "review_condition": f"review-{index} " + ("v" * 700),
            "reconciliation_blockers": [{
                "kind": "drift", "field": "state", "expected": "x" * 800,
                "observed": "y" * 800, "reconciliation_required": True,
            }],
        } for index in range(99)]
        snapshot = self.full_authority_snapshot(snapshot)

        context = MODULE.compact_context(
            snapshot, "GEN-37", require_projection_authority=True,
        )
        encoded = MODULE._default_output_bytes(context)
        self.assertLessEqual(len(encoded), MODULE.DEFAULT_RESUME_MAX_BYTES)
        self.assertEqual(context["resume_authority"], "full")
        self.assertEqual(
            context["context_schema"]["envelope"],
            "fixed_frontier_authority_v1",
        )
        self.assertEqual(context["authority_scope"]["item_count"], 100)
        self.assertEqual(len(context["execution_frontier"]["children"]), 99)
        self.assertEqual(
            context["execution_frontier"]["columns"]["children"],
            ["id", "status", "owner", "repository", "next", "blocker"],
        )
        first_child = context["execution_frontier"]["children"][0]
        self.assertEqual(first_child[:2], ["GEN-38", "started"])
        self.assertIn("~#", first_child[2])
        for row in context["execution_frontier"]["children"]:
            for cell in row:
                if isinstance(cell, str):
                    self.assertLessEqual(
                        len(json.dumps(cell, ensure_ascii=False).encode()), 24,
                    )

    def test_checkpoint_fence_uses_ordered_position_not_stale_writer_revision(self):
        prefix = {
            "event_id": "progress-1", "workstream_id": "GEN-37", "kind": "progress",
            "source": "agent", "payload": {}, "expected_revision": 0,
            "created_at": "2026-08-21T00:00:00Z",
        }
        cases = (
            ("requirement", {"requirement": "R"}, "requirement"),
            ("blocker", {"blocker": "B"}, "blocker"),
            ("decision", {"decision": "D"}, "decision"),
            ("followup", {"followup": "F"}, "followup"),
            ("material_boundary", {"boundary_id": "b", "changes": [
                {"kind": "requirement", "payload": {"requirement": "nested"}},
            ]}, "requirement"),
        )
        for kind, payload, expected_kind in cases:
            with self.subTest(kind=kind):
                concurrent = {
                    "event_id": f"{kind}-2", "workstream_id": "GEN-37", "kind": kind,
                    "source": "agent", "payload": payload, "expected_revision": 0,
                    "created_at": "2026-08-21T00:00:01Z",
                }
                obligations = MODULE._uncheckpointed_material_obligations(
                    [prefix, concurrent], checkpoint_revision=1,
                )
                self.assertEqual(len(obligations), 1)
                self.assertEqual(obligations[0]["kind"], expected_kind)

    def test_material_history_counts_toward_item_budget(self):
        snapshot = MODULE.add_material_history(
            self.snapshot(), [{"id": "event", "body": encode_event_comment(Delta(
                "event-a", "GEN-37", "progress", "agent", {}, 0,
                "2026-08-21T00:00:00Z",
            ))}], "GEN-37",
        )
        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(
                snapshot, "GEN-37", max_items=2, include_history=True,
            )

    def test_stale_plan_checkpoint_only_summarizes_acknowledged_prefix(self):
        events = [{
            "event_id": f"event-{index}", "kind": "requirement",
            "payload": {"requirement": f"requirement-{index}"},
        } for index in range(4)]
        remaining, summary = MODULE._compact_stale_plan_obligations(
            events, [{"root_revision": 2}],
        )

        self.assertEqual(remaining, [{
            "event_id": "event-2", "kind": "requirement",
            "payload": {"requirement": "requirement-2"},
        }, {
            "event_id": "event-3", "kind": "requirement",
            "payload": {"requirement": "requirement-3"},
        }])
        self.assertEqual(summary["checkpoint_root_revision"], 2)
        self.assertEqual(summary["acknowledged_count"], 2)
        self.assertEqual(summary["uncheckpointed_count"], 2)
        changed = copy.deepcopy(events)
        changed[0]["payload"]["requirement"] = "planted-mutation"
        _remaining, changed_summary = MODULE._compact_stale_plan_obligations(
            changed, [{"root_revision": 2}],
        )
        self.assertNotEqual(summary["sha256"], changed_summary["sha256"])
        with self.assertRaisesRegex(
            MODULE.ResumeError,
            "child_stale_checkpoint_ahead_of_material_event_log:5>4",
        ):
            MODULE._compact_stale_plan_obligations(
                events, [{"root_revision": 5}],
            )

    def test_resume_preserves_typed_choice_scope_and_relations_when_supplied(self):
        snapshot = self.snapshot()
        snapshot["scope"] = {
            "namespace": "pulp-playback",
            "linear": {"workspace_id": "ws", "team_id": "team", "project_id": "project",
                       "root_issue_id": "33333333-3333-4333-8333-333333333333",
                       "route_verification": {
                           "workspace_id": "ws", "team_id": "team", "project_id": "project",
                           "root_issue_id": "33333333-3333-4333-8333-333333333333",
                           "observed_at": "2026-08-21T11:00:00Z",
                           "evidence": [{"kind": "authenticated_linear_readback", "authenticated": True,
                                         "workspace_id": "ws", "team_id": "team", "project_id": "project",
                                         "root_issue_id": "33333333-3333-4333-8333-333333333333"}]}},
            "primary_repository": "github.com:id:R_pulp",
            "repositories": [{"slug": "github.com/generous-corp/pulp", "exact_head": "a" * 40,
                              "provider_repository_id": "R_pulp", "aliases": [],
                              "identity_resolution": {"provider_repository_id": "R_pulp",
                                                      "resolved_slug": "github.com/generous-corp/pulp",
                                                      "observed_at": "2026-08-21T11:00:00Z",
                                                      "evidence": [{"kind": "authenticated_provider_readback",
                                                                    "authenticated": True,
                                                                    "provider_repository_id": "R_pulp",
                                                                    "resolved_slug": "github.com/generous-corp/pulp"}]},
                              "identity_updates": [], "evidence": []}],
            "child_ownership": {"GEN-38": "github.com:id:R_pulp",
                                "GEN-39": "github.com:id:R_pulp"},
        }
        snapshot["relations"] = [{"type": "blocked_by", "target": {
            "workspace_id": "ws-other", "issue_id": "11111111-1111-4111-8111-111111111111",
            "identifier": "GEN-50",
        }}]
        snapshot["choice_events"] = []
        snapshot["evidence_contracts"] = []
        snapshot["material_events"] = []
        snapshot["material_event_revision"] = 0
        snapshot["authenticated_route"] = {
            "workspace_id": "ws", "team_id": "team", "project_id": "project",
            "root_issue_id": "33333333-3333-4333-8333-333333333333",
        }
        snapshot["dependency_graph"] = {
            "schema_version": 1,
            "authority": "child_dependency_authorization",
            "plan_revision": "sha",
            "route": snapshot["authenticated_route"],
            "revision": 0,
            "sha256": hashlib.sha256(b"[]").hexdigest(),
            "authorization_batches": [],
            "relations": [],
            "native_readback": "relations_and_inverseRelations",
            "ignored_non_dependency_count": 0,
            "observed_frontier": {
                "material_revision": 0, "projection_revision": 0,
                "graph_revision": 0,
                "graph_sha256": hashlib.sha256(b"[]").hexdigest(),
            },
            "root_readback_sha256": "0" * 64,
        }
        snapshot["root"].update({
            "id": snapshot["authenticated_route"]["root_issue_id"],
            "title": "Root", "description": "Plan revision: sha",
            "url": "https://linear/GEN-37", "updatedAt": "now", "parent": None,
            "team": {"id": "team", "organization": {"id": "ws"}},
            "project": {"id": "project"}, "assignee": None,
            "state": {"id": "started", "name": "In Progress", "type": "started"},
        })
        snapshot["dependency_graph"]["root_readback_sha256"] = (
            MODULE.dependency_root_readback_sha256(snapshot["root"])
        )
        snapshot["root"]["issue_revision"] = snapshot["root"]["revision"]
        snapshot["root"]["revision"] = 0
        snapshot["latest_checkpoint"] = None
        snapshot["checkpoint_recovery"] = {
            "state": "not_found", "stale_plan_count": 0,
        }
        context = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(context["scope"]["linear"]["project_id"], "project")
        self.assertNotIn("route_verification", context["scope"]["linear"])
        self.assertRegex(context["scope"]["validated_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(context["relations"][0]["target"]["identifier"], "GEN-50")
        self.assertEqual(context["dependency_graph"]["relations"], [])
        self.assertTrue(all(value == "available" for value in context["surface_availability"].values()))

        mutated = copy.deepcopy(snapshot)
        mutated["dependency_graph"]["observed_frontier"]["material_revision"] = 999
        with self.assertRaisesRegex(
            MODULE.ResumeError, "dependency_resume_frontier_mismatch",
        ):
            MODULE.compact_context(mutated, "GEN-37")
        mutated = copy.deepcopy(snapshot)
        mutated["dependency_graph"]["root_readback_sha256"] = "f" * 64
        with self.assertRaisesRegex(
            MODULE.ResumeError, "dependency_resume_root_mismatch",
        ):
            MODULE.compact_context(mutated, "GEN-37")

    def test_full_authority_requires_explicit_authenticated_dependency_graph(self):
        for mutation in ("missing", "null"):
            snapshot = self.snapshot()
            if mutation == "null":
                snapshot["dependency_graph"] = None
            with self.assertRaisesRegex(
                MODULE.ResumeError, "authenticated_dependency_graph_missing",
            ):
                MODULE.validate_snapshot(
                    snapshot, "GEN-37", require_projection_authority=True,
                )

    def test_legacy_live_snapshot_exposes_untransported_factory_surfaces(self):
        context = MODULE.compact_context(self.snapshot(), "GEN-37")
        self.assertEqual(context["surface_availability"]["scope"], "transport_unimplemented")
        self.assertEqual(context["surface_availability"]["evidence_contracts"], "transport_unimplemented")

    def test_live_cli_normalizes_title_before_fetch(self):
        authenticated_route = {"workspace_id": "workspace", "team_id": "team",
                               "project_id": "project", "root_issue_id": "root-uuid"}
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = self.live_snapshot(
            self.snapshot(), authenticated_route,
        )
        client = mock.Mock()
        comments = mock.Mock()
        comments.comments.return_value = []
        with mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client), \
             mock.patch.object(MODULE, "resolve_authenticated_resume_target", return_value=(authenticated_route, "GEN-37", None)), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", return_value=transport), \
             mock.patch.object(MODULE, "LinearCommentEventAdapter", return_value=comments), \
             mock.patch.object(MODULE, "load_linear_api_key", return_value="secret"), \
             mock.patch.object(MODULE.sys, "argv", ["workstream_resume.py", "pulp GEN-37 #3", "--linear-team-id", "team", "--inspection-only"]), \
             mock.patch.object(MODULE.sys, "stdout"):
            self.assertEqual(MODULE.main(), 0)
        transport.snapshot_for_root.assert_called_once_with(
            "GEN-37", include_child_comments=True, include_description=True,
        )
        comments.comments.assert_called_once_with()

    def test_owned_child_focus_survives_ordinary_and_audit_contexts(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project",
            "root_issue_id": "33333333-3333-4333-8333-333333333333",
        }
        for token in ("GEN-91", "GEN-92", "GEN-93", "GEN-94"):
            with self.subTest(token=token):
                raw = self.snapshot()
                raw["children"] = [{
                    "identifier": token, "title": token,
                    "status": "In Progress", "next_action": "Continue",
                }]
                live = self.live_snapshot(raw, route)
                native = copy.deepcopy(live["children"])
                authorized = self.full_authority_snapshot(live)
                focus = MODULE.validate_requested_child_focus(
                    focus={
                        "kind": "owned_child", "identifier": token,
                        "issue_id": "child-0",
                        "parent_issue_id": route["root_issue_id"],
                        "root_identifier": "GEN-37",
                    }, native_children=native,
                    authorized_snapshot=authorized, route=route,
                )
                authorized["requested_focus"] = focus
                ordinary = MODULE.compact_context(
                    authorized, "GEN-37", max_bytes=2_147_483_647,
                    max_items=2_147_483_647,
                    require_projection_authority=True,
                    require_dependency_graph=True,
                )
                audit = MODULE.compact_context(
                    authorized, "GEN-37", max_bytes=2_147_483_647,
                    max_items=2_147_483_647,
                    require_projection_authority=True,
                    require_dependency_graph=True, include_history=True,
                )
                self.assertEqual(ordinary["requested_focus"], focus)
                self.assertEqual(audit["requested_focus"], focus)
                self.assertEqual(ordinary["workstream_id"], "GEN-37")

    def test_child_focus_refuses_missing_unowned_and_route_drift(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        raw = self.live_snapshot(self.snapshot(), route)
        native = copy.deepcopy(raw["children"])
        authorized = {
            "children": copy.deepcopy(raw["children"]),
            "scope": {"child_ownership": {
                "GEN-38": "github.com:id:R_repo",
            }},
        }
        focus = {
            "kind": "owned_child", "identifier": "GEN-38",
            "issue_id": "child-0", "parent_issue_id": "root-uuid",
            "root_identifier": "GEN-37",
        }
        cases = []
        missing = copy.deepcopy(native)
        missing.clear()
        cases.append((missing, authorized, "requested_child_native"))
        unowned = copy.deepcopy(authorized)
        unowned["scope"]["child_ownership"] = {}
        cases.append((native, unowned, "requested_child_unowned"))
        drifted = copy.deepcopy(native)
        drifted[0]["project"] = {"id": "other"}
        cases.append((drifted, authorized, "requested_child_native_identity_mismatch"))
        malformed = copy.deepcopy(authorized)
        malformed["children"][0]["id"] = "other"
        cases.append((native, malformed, "requested_child_authorized_identity_mismatch"))
        for observed, snapshot, reason in cases:
            with self.subTest(reason=reason), self.assertRaisesRegex(
                MODULE.ResumeError, reason,
            ):
                MODULE.validate_requested_child_focus(
                    focus=focus, native_children=observed,
                    authorized_snapshot=snapshot, route=route,
                )

    def test_live_child_handle_executes_root_authority_and_dependency_audit(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        focus = {
            "kind": "owned_child", "identifier": "GEN-43",
            "issue_id": "child-43", "parent_issue_id": "root-uuid",
            "root_identifier": "GEN-37",
        }
        child = {
            "id": "child-43", "identifier": "GEN-43",
            "status": "In Progress", "parent": {
                "id": "root-uuid", "identifier": "GEN-37",
            }, "project": {"id": "project"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
        }
        graph = {
            "root": {
                "id": "root-uuid", "identifier": "GEN-37",
                "description": "Plan revision: " + "a" * 64,
                "plan_revision": "a" * 64,
            },
            "children": [copy.deepcopy(child)], "child_comments": {"GEN-43": []},
        }
        source = {"identity": "https://example.test/root-plan", "sha256": "a" * 64}
        provisional = {**copy.deepcopy(graph), "source": source}
        final = {
            **copy.deepcopy(graph), "source": source,
            "material_event_revision": 4, "projection_revision": 7,
            "scope": {"child_ownership": {"GEN-43": "github.com:id:R_repo"}},
        }
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = copy.deepcopy(graph)
        comments = mock.Mock()
        comments.comments.return_value = []
        dependency = mock.Mock()
        dependency.read_authorized_graph.return_value = {"authority": "root"}
        dependency_constructor = mock.Mock(return_value=dependency)
        add_material = mock.Mock(side_effect=(provisional, final))
        stdout = io.StringIO()

        def compact(snapshot, token, *_args, **kwargs):
            self.assertEqual(token, "GEN-37")
            self.assertTrue(kwargs["include_history"])
            self.assertEqual(snapshot["requested_focus"]["identifier"], "GEN-43")
            self.assertEqual(snapshot["dependency_graph"], {"authority": "root"})
            return {
                "workstream_id": token, "resume_authority": "full",
                "requested_focus": snapshot["requested_focus"],
            }

        client = mock.Mock()
        with mock.patch.object(MODULE, "resolve_linear_route", return_value=(None, None)), \
             mock.patch.object(MODULE, "resolve_authenticated_resume_target", return_value=(route, "GEN-37", focus)), \
             mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", return_value=transport), \
             mock.patch.object(MODULE, "LinearCommentEventAdapter", return_value=comments), \
             mock.patch.object(MODULE, "select_plan_generation", return_value={
                 "plan_revision": "a" * 64, "description_plan_revision": "a" * 64,
             }), \
             mock.patch.object(MODULE, "bind_active_plan_generation", side_effect=lambda value, *_args, **_kwargs: value), \
             mock.patch("workstream_linear_projection.child_mutation_authorizations_from_comments", return_value=[]), \
             mock.patch.object(MODULE, "add_live_child_material_history", side_effect=lambda value, **_kwargs: value), \
             mock.patch.object(MODULE, "add_material_history", add_material), \
             mock.patch.object(MODULE, "plan_payload", return_value={"source": source}), \
             mock.patch.object(MODULE, "plan_generation_freshness", return_value=None), \
             mock.patch.object(MODULE, "LinearChildDependencyAdapter", dependency_constructor), \
             mock.patch.object(MODULE, "load_linear_api_key", return_value="secret"), \
             mock.patch.object(MODULE, "compact_context", side_effect=compact), \
             mock.patch.object(MODULE.sys, "argv", [
                 "workstream_resume.py", "GEN-43", "--include-history",
             ]), \
             mock.patch.object(MODULE.sys, "stdout", stdout):
            self.assertEqual(MODULE.main(), 0)
        transport.snapshot_for_root.assert_called_once_with(
            "GEN-37", include_child_comments=True, include_description=True,
        )
        comments_class_call = comments.comments.call_count
        self.assertEqual(comments_class_call, 1)
        dependency_constructor.assert_called_once_with(
            client, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id="root-uuid",
            root_identifier="GEN-37", plan_revision="a" * 64,
        )
        self.assertEqual(json.loads(stdout.getvalue())["requested_focus"]
                         ["identifier"], "GEN-43")
        self.assertFalse(any("mutation" in str(call).lower()
                             for call in client.mock_calls))

    def test_child_handle_root_source_drift_refuses_without_writes(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        graph = {
            "root": {
                "id": "root-uuid", "identifier": "GEN-37",
                "description": "Canonical plan: https://example.test/root",
                "plan_revision": "a" * 64,
            },
            "children": [], "child_comments": {},
        }
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = copy.deepcopy(graph)
        comments = mock.Mock()
        comments.comments.return_value = []
        client = mock.Mock()
        stderr = io.StringIO()
        with mock.patch.object(MODULE, "resolve_linear_route", return_value=(None, None)), \
             mock.patch.object(MODULE, "resolve_authenticated_resume_target", return_value=(route, "GEN-37", {
                 "kind": "owned_child", "identifier": "GEN-43",
                 "issue_id": "child-43", "parent_issue_id": "root-uuid",
                 "root_identifier": "GEN-37",
             })), \
             mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", return_value=transport), \
             mock.patch.object(MODULE, "LinearCommentEventAdapter", return_value=comments), \
             mock.patch.object(MODULE, "select_plan_generation", return_value={
                 "plan_revision": "a" * 64, "description_plan_revision": "a" * 64,
             }), \
             mock.patch.object(MODULE, "bind_active_plan_generation", side_effect=lambda value, *_args, **_kwargs: value), \
             mock.patch("workstream_linear_projection.child_mutation_authorizations_from_comments", return_value=[]), \
             mock.patch.object(MODULE, "add_live_child_material_history", side_effect=lambda value, **_kwargs: value), \
             mock.patch.object(MODULE, "add_material_history", return_value={
                 **graph, "source": {
                     "identity": "https://example.test/root", "sha256": "a" * 64,
                 },
             }), \
             mock.patch.object(MODULE, "plan_payload", side_effect=MODULE.ResumeError("projection_source_bytes_mismatch")), \
             mock.patch.object(MODULE, "load_linear_api_key", return_value="secret"), \
             mock.patch.object(MODULE.sys, "argv", ["workstream_resume.py", "GEN-43"]), \
             mock.patch.object(MODULE.sys, "stderr", stderr):
            self.assertEqual(MODULE.main(), 2)
        self.assertIn("projection_source_bytes_mismatch", stderr.getvalue())
        transport.snapshot_for_root.assert_called_once_with(
            "GEN-37", include_child_comments=True, include_description=True,
        )
        self.assertFalse(any("mutation" in str(call).lower()
                             for call in client.mock_calls))

    def test_live_cli_automatically_uses_repository_config_route(self):
        route = {
            "workspace_id": "workspace", "team_id": "team", "project_id": "project"
        }
        authenticated_route = {**route, "root_issue_id": "root-uuid"}
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = self.live_snapshot(
            self.snapshot(), authenticated_route,
        )
        client = mock.Mock()
        constructor = mock.Mock(return_value=transport)
        comments = mock.Mock()
        comments.comments.return_value = []
        with mock.patch.object(MODULE, "resolve_linear_route", return_value=(route, Path(".workstream.json"))), \
             mock.patch.object(MODULE, "resolve_authenticated_resume_target", return_value=(authenticated_route, "GEN-37", None)), \
             mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", constructor), \
             mock.patch.object(MODULE, "LinearCommentEventAdapter", return_value=comments), \
             mock.patch.object(MODULE, "load_linear_api_key", return_value="secret"), \
             mock.patch.object(MODULE.sys, "argv", ["workstream_resume.py", "GEN-37", "--inspection-only"]), \
             mock.patch.object(MODULE.sys, "stdout"):
            self.assertEqual(MODULE.main(), 0)

        constructor.assert_called_once_with(
            client, team_id="team", workspace_id="workspace", project_id="project"
        )
        transport.snapshot_for_root.assert_called_once_with(
            "GEN-37", include_child_comments=True, include_description=True,
        )
        comments.comments.assert_called_once_with()

    def test_live_cli_bootstraps_route_from_token_without_repo_config(self):
        route = {"workspace_id": "workspace", "team_id": "team",
                 "project_id": "project", "root_issue_id": "root-uuid"}
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = self.live_snapshot(
            self.snapshot(), route,
        )
        client = mock.Mock()
        comments = mock.Mock()
        comments.comments.return_value = []
        constructor = mock.Mock(return_value=transport)
        with mock.patch.object(MODULE, "resolve_linear_route", return_value=(None, None)), \
             mock.patch.object(MODULE, "resolve_authenticated_resume_target", return_value=(route, "GEN-37", None)) as bootstrap, \
             mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", constructor), \
             mock.patch.object(MODULE, "LinearCommentEventAdapter", return_value=comments), \
             mock.patch.object(MODULE, "load_linear_api_key", return_value="secret"), \
             mock.patch.object(MODULE.sys, "argv", ["workstream_resume.py", "GEN-37", "--inspection-only"]), \
             mock.patch.object(MODULE.sys, "stdout"):
            self.assertEqual(MODULE.main(), 0)
        bootstrap.assert_called_once_with(client, "GEN-37", None)
        constructor.assert_called_once_with(
            client, team_id="team", workspace_id="workspace", project_id="project"
        )

    def test_repeated_live_full_resume_is_read_only_and_authenticates_source_first(self):
        graph = self.snapshot()
        graph["root"]["plan_revision"] = "a" * 64
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project",
            "root_issue_id": "33333333-3333-4333-8333-333333333333",
        }
        graph = self.live_snapshot(graph, route)
        child_comments = graph.pop("child_comments")
        authenticated_source = {
            "identity": "https://example.test/immutable-plan",
            "sha256": "a" * 64, "bytes": 123,
        }
        graph["root"]["description"] = (
            "Canonical plan: " + authenticated_source["identity"]
        )
        before = MODULE.add_material_history(
            graph, [], "GEN-37", authenticated_route=route,
            authenticated_source=authenticated_source,
        )
        github = {
            "repository": "generous-corp/agent-workstream",
            "provider_repository_id": "R_agent_workstream", "pr_number": 41,
            "pr_head": "c" * 40, "merged": True, "merge_sha": "d" * 40,
        }
        shipyard_body = {
            "schema_version": 1,
            "repository": github["repository"],
            "repository_key": "github.com:id:R_agent_workstream",
            "pr_number": github["pr_number"], "head": github["pr_head"],
            "disposition": "merged", "receipt_id": "receipt-41",
        }
        shipyard = {
            **shipyard_body,
            "receipt_sha256": hashlib.sha256(json.dumps(
                shipyard_body, sort_keys=True, separators=(",", ":"),
            ).encode()).hexdigest(),
        }
        lifecycle = build_projection_event(
            workstream_id="GEN-37", kind="lifecycle", key="root",
            value={
                "status": "Landed — acceptance review required",
                "github": github, "shipyard_receipt": shipyard,
                "closure_input_sha256": "b" * 64,
                "snapshot_sha256": MODULE.closure_snapshot_digest(before),
                "independent_review": None, "closure_receipt_sha256": None,
            },
            plan_revision="a" * 64, expected_revision=0,
            created_at="2026-08-28T12:00:00Z", authority=route,
        )
        comments_payload = [{
            "id": projection_slot_id(
                "GEN-37", "a" * 64, 0, route,
            ),
            "body": encode_projection_comment(lifecycle),
        }]
        preauthentication = MODULE.add_material_history(
            graph, comments_payload, "GEN-37", authenticated_route=route,
            permit_stale_lifecycle_for_reconcile=True,
        )
        self.assertEqual(
            preauthentication["lifecycle_recovery"]["state"], "stale_snapshot",
        )
        transport = mock.Mock()
        transport.snapshot_for_root.side_effect = lambda *_args, **_kwargs: copy.deepcopy({
            **graph, "children": [dict(child) for child in graph["children"]],
            "child_comments": child_comments,
            "root": {
                **graph["root"],
                "description": (
                    "Canonical plan: " + authenticated_source["identity"]
                ),
            },
        })
        comments = mock.Mock()
        comments.comments.return_value = comments_payload
        dependency_graph = {
            "schema_version": 1,
            "authority": "child_dependency_authorization",
            "plan_revision": "a" * 64,
            "route": route,
            "revision": 0,
            "sha256": hashlib.sha256(b"[]").hexdigest(),
            "authorization_batches": [],
            "relations": [],
            "native_readback": "relations_and_inverseRelations",
            "ignored_non_dependency_count": 0,
            "observed_frontier": {
                "material_revision": 0, "projection_revision": 1,
                "graph_revision": 0,
                "graph_sha256": hashlib.sha256(b"[]").hexdigest(),
            },
            "root_readback_sha256": MODULE.dependency_root_readback_sha256(
                before["root"],
            ),
        }
        dependency_adapter = mock.Mock()
        dependency_adapter.read_authorized_graph.return_value = dependency_graph
        dependency_constructor = mock.Mock(return_value=dependency_adapter)
        stdout = io.StringIO()

        def verified_context(snapshot, *_args, **_kwargs):
            self.assertEqual(
                snapshot["root"]["status"],
                "Landed — acceptance review required",
            )
            self.assertNotIn("lifecycle_recovery", snapshot)
            self.assertEqual(snapshot["dependency_graph"], dependency_graph)
            return {"status": snapshot["root"]["status"]}

        with mock.patch.object(MODULE, "resolve_linear_route", return_value=(None, None)), \
             mock.patch.object(MODULE, "resolve_authenticated_resume_target", return_value=(route, "GEN-37", None)), \
             mock.patch.object(MODULE, "HttpGraphQLClient", return_value=mock.Mock()), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", return_value=transport), \
             mock.patch.object(MODULE, "LinearCommentEventAdapter", return_value=comments), \
             mock.patch.object(
                 MODULE, "LinearChildDependencyAdapter", dependency_constructor,
             ), \
             mock.patch.object(MODULE, "load_linear_api_key", return_value="secret"), \
             mock.patch.object(
                 MODULE, "plan_payload", return_value={"source": authenticated_source},
             ), \
             mock.patch.object(MODULE, "compact_context", side_effect=verified_context), \
             mock.patch.object(
                 MODULE.sys, "argv", [
                     "workstream_resume.py", "GEN-37",
                     "--plan-source", authenticated_source["identity"],
                 ],
             ), \
             mock.patch.object(MODULE.sys, "stdout", stdout):
            self.assertEqual(MODULE.main(), 0)
            first = json.loads(stdout.getvalue())
            stdout.seek(0)
            stdout.truncate(0)
            self.assertEqual(MODULE.main(), 0)
            second = json.loads(stdout.getvalue())
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "Landed — acceptance review required")
        self.assertEqual(transport.snapshot_for_root.call_count, 2)
        self.assertEqual(comments.comments.call_count, 2)
        self.assertEqual(dependency_constructor.call_count, 2)
        dependency_constructor.assert_called_with(
            mock.ANY, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=route["root_issue_id"],
            root_identifier="GEN-37", plan_revision="a" * 64,
        )
        graph["root"]["next_action"] = "new material state after reconciliation"
        with self.assertRaisesRegex(
            MODULE.ResumeError, "lifecycle_snapshot_stale_reconcile_required",
        ):
            MODULE.add_material_history(
                graph, comments_payload, "GEN-37", authenticated_route=route,
                authenticated_source=authenticated_source,
            )

    def test_gen14_ordinary_resume_drift_activation_and_full_recovery(self):
        import test_workstream_generation_transition as generation_fixture
        import workstream_generation as generation_cli_module
        import workstream_root_transition as root_transition_module
        from workstream_linear_projection import (
            LinearProjectionAdapter, reduce_projection_comments,
        )

        token = "GEN-14"
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project",
            "root_issue_id": "33333333-3333-4333-8333-333333333333",
        }
        canonical = "https://github.com/acme/plans/blob/main/PLAN.md"
        old_identity = (
            "https://github.com/acme/plans/blob/" + "1" * 40 + "/PLAN.md"
        )
        new_identity = (
            "https://github.com/acme/plans/blob/" + "2" * 40 + "/PLAN.md"
        )
        old_plan = tempfile.NamedTemporaryFile("w+", suffix=".md")
        new_plan = tempfile.NamedTemporaryFile("w+", suffix=".md")
        self.addCleanup(old_plan.close)
        self.addCleanup(new_plan.close)
        old_plan.write("# GEN-14 original plan\n")
        old_plan.flush()
        new_plan.write("# GEN-14 revised plan\n")
        new_plan.flush()
        old_digest = hashlib.sha256(Path(old_plan.name).read_bytes()).hexdigest()
        new_digest = hashlib.sha256(Path(new_plan.name).read_bytes()).hexdigest()
        canonical_live_digest = [new_digest]

        with mock.patch.object(generation_fixture, "WORKSTREAM", token), \
             mock.patch.object(generation_fixture, "AUTHORITY", route):
            client = generation_fixture.FakeClient()
            original_execute = client.execute
            comment_reads = []

            def recording_execute(query, variables):
                if "query WorkstreamDeltaComments" in query:
                    comment_reads.append(copy.deepcopy(variables))
                if "mutation WorkstreamRootTransition(" in query:
                    update = copy.deepcopy(variables["input"])
                    self.assertEqual(set(update), {"description"})
                    client.description = update["description"]
                    client.graph_nonce = "locator-reconciled"
                    return {"issueUpdate": {
                        "success": True, "issue": client.root_issue(),
                    }}
                response = original_execute(query, variables)
                if (
                    "mutation WorkstreamDeltaCommentCreate" in query
                    and "<!-- workstream-root-transition:v1:"
                    in str(variables.get("input", {}).get("body", ""))
                ):
                    receipt = response["commentCreate"]["comment"]
                    client.graph_nonce = receipt["updatedAt"]
                return response

            client.execute = recording_execute
            client.description = (
                f"Plan revision: {old_digest}\nCanonical plan: {canonical}\n"
                "Next action: Review the revised plan."
            )
            client.graph_status = "Done"
            client.graph_status_type = "completed"
            generation_fixture.project_full(
                client, old_digest, identity=old_identity,
            )
            linear = generation_fixture.LinearGraphQLTransport(
                client, workspace_id="workspace", team_id="team",
                project_id="project",
            )

            def prepare_operator_contract():
                graph = linear.snapshot_for_root(
                    token, include_description=True,
                    include_child_comments=True,
                )
                return generation_cli_module.prepare_generation_operator_contract(
                    comments=copy.deepcopy(client.comments), graph=graph,
                    workstream_id=token, authority=route,
                    description_plan_revision=old_digest,
                    target_source={
                        "identity": new_identity, "sha256": new_digest,
                    },
                    created_at="2026-08-31T12:00:00Z",
                    remote_head="e" * 40,
                    started_state=generation_fixture.STARTED_STATE,
                )

            operator_contract = prepare_operator_contract()
            target = generation_fixture.adapter(client, new_digest)
            target_revision = target.state().revision
            for index, item in enumerate(
                operator_contract["projection_preview"]["manifest"]["projection"]
            ):
                target.append(generation_fixture.build_projection_event(
                    workstream_id=token, kind=item["kind"], key=item["key"],
                    value=copy.deepcopy(item["value"]),
                    plan_revision=new_digest,
                    expected_revision=target_revision,
                    created_at=f"target-{index}", authority=route,
                ))
                target_revision += 1
            target.append(generation_fixture.build_projection_event(
                workstream_id=token, kind="disposition", key="root",
                value={
                    "disposition": "attach", "remote_head": "e" * 40,
                    "recovered_from_checkpoint": None,
                },
                plan_revision=new_digest, expected_revision=target_revision,
                created_at="target-disposition", authority=route,
            ))
            operator_contract = prepare_operator_contract()
            self.assertEqual(
                operator_contract["projection_preview"]["phase"],
                "activation_ready",
            )
            initial_comment_count = len(client.comments)

            def source_payload(location, identity=None):
                if location == canonical:
                    return {"source": {
                        "identity": canonical,
                        "sha256": canonical_live_digest[0],
                        "bytes": len(Path(new_plan.name).read_bytes()),
                    }}
                if location == old_plan.name or location == old_identity:
                    return {"source": {
                        "identity": identity or old_identity,
                        "sha256": old_digest,
                        "bytes": len(Path(old_plan.name).read_bytes()),
                    }}
                if location == new_plan.name or location == new_identity:
                    return {"source": {
                        "identity": identity or new_identity,
                        "sha256": new_digest,
                        "bytes": len(Path(new_plan.name).read_bytes()),
                    }}
                raise AssertionError(f"unexpected plan source: {location}")

            def ordinary_resume(plan_path, identity):
                output = io.StringIO()
                reads_before = len(comment_reads)
                with mock.patch.object(
                    MODULE, "resolve_linear_route", return_value=(None, None),
                ), mock.patch.object(
                    MODULE, "resolve_authenticated_resume_target", return_value=(route, token, None),
                ), mock.patch.object(
                    MODULE, "HttpGraphQLClient", return_value=client,
                ), mock.patch.object(
                    MODULE, "load_linear_api_key", return_value="secret",
                ), mock.patch.object(
                    MODULE, "plan_payload", side_effect=source_payload,
                ), mock.patch.object(
                    MODULE.sys, "argv", [
                        "workstream_resume.py", token,
                        "--plan-source", plan_path,
                        "--plan-identity", identity,
                    ],
                ), mock.patch.object(MODULE.sys, "stdout", output):
                    self.assertEqual(MODULE.main(), 0)
                self.assertGreater(len(comment_reads), reads_before)
                return json.loads(output.getvalue())

            def generation_cli(arguments):
                output = io.StringIO()
                error = io.StringIO()
                with mock.patch.object(
                    generation_cli_module, "_route_and_client",
                    return_value=(client, route),
                ), mock.patch.object(
                    generation_cli_module.sys, "argv",
                    ["workstream_generation.py", *arguments],
                ), mock.patch.object(
                    generation_cli_module.sys, "stdout", output,
                ), mock.patch.object(
                    generation_cli_module.sys, "stderr", error,
                ):
                    code = generation_cli_module.main()
                self.assertEqual((code, error.getvalue()), (0, ""))
                return json.loads(output.getvalue())

            pending = ordinary_resume(old_plan.name, old_identity)
            self.assertEqual(
                pending["resume_authority"], "plan_generation_pending",
            )
            self.assertFalse(pending["executable"])
            self.assertEqual(pending["active_source"]["sha256"], old_digest)
            self.assertEqual(
                pending["canonical_live_source"]["sha256"], new_digest,
            )
            self.assertLessEqual(
                len(MODULE._default_output_bytes(pending)), 24 * 1024,
            )
            self.assertEqual(client.graph_status, "Done")

            client.graph_status = "In Progress"
            client.graph_status_type = "started"
            client.graph_state_id = generation_fixture.STARTED_STATE["id"]
            operator_file = tempfile.NamedTemporaryFile("w+", suffix=".json")
            self.addCleanup(operator_file.close)
            json.dump(operator_contract, operator_file)
            operator_file.flush()
            base_generation_args = [
                "activate", token,
                "--plan-source", new_plan.name,
                "--plan-identity", new_identity,
                "--operator-contract", operator_file.name,
                "--created-at", "2026-08-31T12:00:00Z",
            ]
            preview = generation_cli(base_generation_args)
            self.assertFalse(preview["apply"])
            self.assertEqual(preview["command"], "activate")
            proof = preview["native_root_activation_proof"]
            activated = generation_cli([
                *base_generation_args, "--apply",
                "--expected-native-root-sha256", proof["sha256"],
            ])
            writes_after_activation = len(client.comments)
            replay = generation_cli([
                *base_generation_args, "--apply",
                "--expected-native-root-sha256", proof["sha256"],
            ])
            self.assertTrue(replay["replay"])
            self.assertEqual(len(client.comments), writes_after_activation)
            self.assertEqual(
                activated["two_phase_finalization"]["execution_status"]["name"],
                "In Progress",
            )

            client.graph_status = "Done"
            client.graph_status_type = "completed"
            first = ordinary_resume(new_plan.name, new_identity)
            second = ordinary_resume(new_plan.name, new_identity)
            self.assertEqual(first, second)
            self.assertEqual(first["resume_authority"], "full")
            self.assertEqual(first["workstream_id"], token)
            self.assertEqual(first["plan_revision"], new_digest)
            self.assertEqual(first["source"], {
                "identity": new_identity, "sha256": new_digest,
            })
            self.assertEqual(first["status"], "In Progress")
            self.assertEqual(first["native_issue_status"], "Done")
            self.assertEqual(first["authenticated_route"], route)
            self.assertEqual(len(client.comments), writes_after_activation)
            self.assertEqual(client.root_issue()["id"], route["root_issue_id"])
            self.assertEqual(
                client.root_issue()["project"]["id"], route["project_id"],
            )
            self.assertEqual(client.children, [])
            source_events = []
            for digest in (old_digest, new_digest):
                state = reduce_projection_comments(
                    client.comments, workstream_id=token,
                    expected_plan_revision=digest,
                    authenticated_route=route,
                )
                source_events.extend(
                    event for event in state.events if event["kind"] == "source"
                )
            self.assertEqual(len(source_events), 2)
            self.assertEqual(
                {event["value"]["sha256"] for event in source_events},
                {old_digest, new_digest},
            )
            self.assertGreater(writes_after_activation, initial_comment_count)

            client.description = client.description.replace(canonical, old_identity)
            locator_pending = ordinary_resume(new_plan.name, new_identity)
            self.assertEqual(
                locator_pending["resume_authority"], "plan_generation_pending",
            )
            self.assertEqual(
                locator_pending["active_source"]["sha256"], new_digest,
            )
            self.assertEqual(
                locator_pending["canonical_live_source"]["sha256"], old_digest,
            )
            with self.assertRaisesRegex(
                generation_cli_module.WorkstreamGenerationError,
        "same_generation_reopen_requires_open_child",
            ):
                prepare_operator_contract()

            locator_source = {
                "identity": new_identity, "sha256": new_digest,
            }

            def locator_validator(snapshot, comments):
                return root_transition_module.validate_active_locator_authorization(
                    source=locator_source, token=token, authority=route,
                    comments=comments, graph=snapshot,
                )

            locator = root_transition_module.RootTransitionTransport(
                client, token=token, authority=route,
                operator_validator=locator_validator,
            )
            root_before = copy.deepcopy(client.root_issue())
            comments_before_locator = copy.deepcopy(client.comments)
            projections_before = {}
            for digest in (old_digest, new_digest):
                projections_before[digest] = tuple(
                    reduce_projection_comments(
                        client.comments, workstream_id=token,
                        expected_plan_revision=digest,
                        authenticated_route=route,
                    ).events
                )
            locator_preview = locator.preview(
                operation="reconcile-plan-url", target=new_identity,
            )
            locator_result = locator.apply(
                operation="reconcile-plan-url", target=new_identity,
                expected_snapshot_sha256=locator_preview[
                    "expected_snapshot_sha256"
                ],
                expected_frontier_sha256=locator_preview[
                    "expected_frontier_sha256"
                ],
                expected_intent_sha256=locator_preview["intent_sha256"],
            )
            self.assertEqual(
                locator_result["result"], "applied_or_exact_replay",
            )
            self.assertEqual(
                client.comments[:len(comments_before_locator)],
                comments_before_locator,
            )
            self.assertEqual(
                len(client.comments), len(comments_before_locator) + 1,
            )
            for digest in (old_digest, new_digest):
                self.assertEqual(tuple(reduce_projection_comments(
                    client.comments, workstream_id=token,
                    expected_plan_revision=digest,
                    authenticated_route=route,
                ).events), projections_before[digest])
            root_after = client.root_issue()
            for field in ("id", "identifier", "project", "state"):
                self.assertEqual(root_after[field], root_before[field])

            repaired = ordinary_resume(new_plan.name, new_identity)
            self.assertEqual(repaired["resume_authority"], "full")
            comments_after_locator = len(client.comments)
            locator_replay = locator.apply(
                operation="reconcile-plan-url", target=new_identity,
                expected_snapshot_sha256=locator_preview[
                    "expected_snapshot_sha256"
                ],
                expected_frontier_sha256=locator_preview[
                    "expected_frontier_sha256"
                ],
                expected_intent_sha256=locator_preview["intent_sha256"],
            )
            self.assertEqual(
                locator_replay["result"], "applied_or_exact_replay",
            )
            self.assertEqual(len(client.comments), comments_after_locator)

            client.description = client.description.replace(new_identity, canonical)

            post_finalization_digest = "f" * 64
            canonical_live_digest[0] = post_finalization_digest
            drift_after_finalization = ordinary_resume(
                new_plan.name, new_identity,
            )
            self.assertEqual(
                drift_after_finalization["resume_authority"],
                "plan_generation_pending",
            )
            self.assertFalse(drift_after_finalization["executable"])
            self.assertEqual(
                drift_after_finalization["root"]["native_status_observed"],
                "Done",
            )
            self.assertEqual(
                drift_after_finalization["root"][
                    "native_status_type_observed"
                ],
                "completed",
            )
            self.assertEqual(
                drift_after_finalization["active_source"]["sha256"],
                new_digest,
            )
            self.assertEqual(
                drift_after_finalization["canonical_live_source"]["sha256"],
                post_finalization_digest,
            )

    def test_relation_target_readback_reconstructs_exact_lifecycle_digest(self):
        graph = self.snapshot()
        graph["root"]["plan_revision"] = "a" * 64
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project",
            "root_issue_id": "33333333-3333-4333-8333-333333333333",
        }
        target = {
            "workspace_id": "workspace",
            "issue_id": "22222222-2222-4222-8222-222222222222",
            "identifier": "GEN-50",
        }
        relation = {"type": "related", "target": target}
        relation_event = build_projection_event(
            workstream_id="GEN-37", kind="relation", key="related:GEN-50",
            value=relation, plan_revision="a" * 64, expected_revision=0,
            created_at="2026-08-28T11:59:00Z", authority=route,
        )
        relation_comment = {
            "id": projection_slot_id("GEN-37", "a" * 64, 0, route),
            "body": encode_projection_comment(relation_event),
        }
        target_readback = {
            f"workspace:{target['issue_id']}": {**target, "relations": []},
        }
        before = MODULE.add_material_history(
            graph, [relation_comment], "GEN-37", authenticated_route=route,
            relation_target_resolver=lambda _relations: target_readback,
        )
        github = {
            "repository": "generous-corp/agent-workstream",
            "provider_repository_id": "R_agent_workstream", "pr_number": 41,
            "pr_head": "c" * 40, "merged": True, "merge_sha": "d" * 40,
        }
        shipyard_body = {
            "schema_version": 1, "repository": github["repository"],
            "repository_key": "github.com:id:R_agent_workstream",
            "pr_number": 41, "head": github["pr_head"],
            "disposition": "merged", "receipt_id": "receipt-41",
        }
        shipyard = {**shipyard_body, "receipt_sha256": hashlib.sha256(json.dumps(
            shipyard_body, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()}
        lifecycle = build_projection_event(
            workstream_id="GEN-37", kind="lifecycle", key="root", value={
                "status": "Landed — acceptance review required",
                "github": github, "shipyard_receipt": shipyard,
                "closure_input_sha256": "b" * 64,
                "snapshot_sha256": MODULE.closure_snapshot_digest(before),
                "independent_review": None, "closure_receipt_sha256": None,
            }, plan_revision="a" * 64, expected_revision=1,
            created_at="2026-08-28T12:00:00Z", authority=route,
        )
        comments = [relation_comment, {
            "id": projection_slot_id("GEN-37", "a" * 64, 1, route),
            "body": encode_projection_comment(lifecycle),
        }]
        with self.assertRaisesRegex(
            MODULE.ResumeError, "lifecycle_snapshot_stale_reconcile_required",
        ):
            MODULE.add_material_history(
                graph, comments, "GEN-37", authenticated_route=route,
            )
        resumed = MODULE.add_material_history(
            graph, comments, "GEN-37", authenticated_route=route,
            relation_target_resolver=lambda _relations: target_readback,
        )
        self.assertEqual(
            resumed["root"]["status"], "Landed — acceptance review required",
        )
        self.assertNotIn("lifecycle_recovery", resumed)
        self.assertEqual(
            MODULE.closure_snapshot_digest(resumed),
            lifecycle["value"]["snapshot_sha256"],
        )

    def test_snapshot_cli_is_inspection_only_even_when_forged_as_authenticated(self):
        snapshot = self.snapshot()
        snapshot["authenticated_source"] = {
            "identity": "https://attacker.invalid/plan",
            "sha256": "sha",
        }
        snapshot["resume_authority"] = "full"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "forged.json"
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            stderr = io.StringIO()
            with mock.patch.object(
                MODULE.sys, "argv", ["workstream_resume.py", "GEN-37", str(path)]
            ), mock.patch.object(MODULE.sys, "stderr", stderr):
                self.assertEqual(MODULE.main(), 2)
        self.assertIn("snapshot_input_requires_inspection_only", stderr.getvalue())

    def test_live_cli_surfaces_rate_limit_as_one_safe_machine_reason(self):
        stderr = io.StringIO()
        with mock.patch.object(
            MODULE.sys, "argv", ["workstream_resume.py", "GEN-37"]
        ), mock.patch.object(
            MODULE, "resolve_linear_route", return_value=({
                "workspace_id": "workspace", "team_id": "team",
                "project_id": "project",
            }, None),
        ), mock.patch.object(
            MODULE, "load_linear_api_key", return_value="secret-token"
        ), mock.patch.object(
            MODULE, "resolve_authenticated_resume_target",
            side_effect=LinearRateLimitedError(http_status=400),
        ), mock.patch.object(MODULE.sys, "stderr", stderr):
            self.assertEqual(MODULE.main(), 2)

        self.assertEqual(
            stderr.getvalue(),
            "workstream resume refused: linear_rate_limited\n",
        )
        self.assertNotIn("secret-token", stderr.getvalue())

    def test_snapshot_cli_accepts_explicit_inspection_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot.json"
            path.write_text(json.dumps(self.snapshot()), encoding="utf-8")
            stdout = io.StringIO()
            with mock.patch.object(
                MODULE.sys, "argv",
                ["workstream_resume.py", "GEN-37", str(path), "--inspection-only"],
            ), mock.patch.object(MODULE.sys, "stdout", stdout):
                self.assertEqual(MODULE.main(), 0)
        self.assertEqual(json.loads(stdout.getvalue())["resume_authority"], "inspection_only")

    def test_cli_stdout_uses_exact_budgeted_serializer_including_newline(self):
        payload = {"padding": "", "resume_authority": "inspection_only"}
        payload["padding"] = "x" * (
            MODULE.DEFAULT_RESUME_MAX_BYTES
            - len(MODULE._default_output_bytes(payload))
        )
        self.assertEqual(
            len(MODULE._default_output_bytes(payload)),
            MODULE.DEFAULT_RESUME_MAX_BYTES,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot.json"
            path.write_text("{}", encoding="utf-8")
            stdout = io.StringIO()
            with mock.patch.object(
                MODULE.sys, "argv", [
                    "workstream_resume.py", "GEN-37", str(path),
                    "--inspection-only",
                ],
            ), mock.patch.object(
                MODULE, "compact_context", return_value=payload,
            ), mock.patch.object(MODULE.sys, "stdout", stdout):
                self.assertEqual(MODULE.main(), 0)
        emitted = stdout.getvalue().encode("utf-8")
        self.assertEqual(emitted, MODULE._default_output_bytes(payload))
        self.assertEqual(len(emitted), MODULE.DEFAULT_RESUME_MAX_BYTES)
        self.assertTrue(emitted.endswith(b"\n"))
        self.assertEqual(emitted.count(b"\n"), 1)


if __name__ == "__main__":
    unittest.main()
