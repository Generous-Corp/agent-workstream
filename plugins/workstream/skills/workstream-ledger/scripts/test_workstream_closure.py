#!/usr/bin/env python3
import unittest

from workstream_closure import review
from workstream_choices import record_choice


class ClosureTests(unittest.TestCase):
    def evidence(self):
        return {"a": {"satisfied": True}, "decisions": [], "followups": [], "prs": [], "shipyard_receipts": [], "tests": [], "artifacts": []}

    def factory_snapshot(self, snapshot):
        head = "a" * 40
        key = "github.com:id:R_pulp"
        child_ids = [child["identifier"] for child in snapshot["children"]]
        receipt = {"kind": "unit-test", "passed": True, "repository_key": key,
                   "exact_head": head, "proof": "unit test receipt"}
        na = lambda reason: {"status": "not_applicable", "reason": reason}
        contracts = []
        for child_id in child_ids:
            contracts.append({
                "slice_id": child_id.lower(), "owning_child": child_id,
                "repository": "github.com/generous-corp/pulp", "repository_key": key,
                "plan_revision": "sha", "exact_head": head,
                "layers": {
                    "architecture": na("Covered by accepted plan"),
                    "logic": {"status": "required", "methods": ["unit"], "receipts": [receipt]},
                    "component": na("Pure logical slice"), "adapter": na("No adapter"),
                    "e2e": na("No user journey"), "visual": na("No visual output"),
                    "operational": na("No operational mutation"),
                    "negative_control": {"status": "required", "failure_detected": True,
                                         "receipts": [{**receipt, "kind": "negative-control"}]},
                },
            })
        snapshot.update({
            "scope": {
                "namespace": "test-workstream",
                "linear": {"workspace_id": "ws", "team_id": "team", "project_id": "project",
                           "root_issue_id": "33333333-3333-4333-8333-333333333333",
                           "route_verification": {
                               "workspace_id": "ws", "team_id": "team", "project_id": "project",
                               "root_issue_id": "33333333-3333-4333-8333-333333333333",
                               "observed_at": "2026-08-21T11:00:00Z",
                               "evidence": [{"kind": "authenticated_linear_readback", "authenticated": True,
                                             "workspace_id": "ws", "team_id": "team",
                                             "project_id": "project",
                                             "root_issue_id": "33333333-3333-4333-8333-333333333333"}]}},
                "primary_repository": key,
                "repositories": [{"slug": "github.com/generous-corp/pulp",
                                  "provider_repository_id": "R_pulp", "aliases": [],
                                  "identity_resolution": {"provider_repository_id": "R_pulp",
                                                          "resolved_slug": "github.com/generous-corp/pulp",
                                                          "observed_at": "2026-08-21T11:00:00Z",
                                                          "evidence": [{"kind": "authenticated_provider_readback",
                                                                        "authenticated": True,
                                                                        "provider_repository_id": "R_pulp",
                                                                        "resolved_slug": "github.com/generous-corp/pulp"}]},
                                  "identity_updates": [], "exact_head": head, "evidence": []}],
                "child_ownership": {child_id: key for child_id in child_ids},
            },
            "relations": [], "choice_events": [], "evidence_contracts": contracts,
            "surface_availability": {field: "available" for field in
                                     ("scope", "relations", "choice_events", "evidence_contracts")},
        })
        return snapshot

    def test_complete_snapshot_emits_receipt(self):
        snapshot = self.factory_snapshot({"root": {"identifier": "GEN-37", "url": "https://linear/GEN-37", "plan_revision": "sha", "revision": 2, "status": "In Progress"}, "children": [{"identifier": "GEN-38", "status": "Done", "owner": "agent"}]})
        result = review(
            snapshot,
            expected_plan_revision="sha",
            criteria=["a"],
            evidence=self.evidence(),
            semantic_review_invoked=True,
            semantic_review_passed=True,
            exact_head="a" * 40,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["receipt"]["final_disposition"], "Done")

    def test_deterministic_pass_without_semantic_review_stays_landed(self):
        snapshot = self.factory_snapshot({
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37", "plan_revision": "sha", "revision": 2, "status": "Landed — acceptance review required"},
            "children": [{"identifier": "GEN-38", "status": "Done", "owner": "agent"}],
        })
        result = review(
            snapshot, expected_plan_revision="sha", criteria=["a"], evidence=self.evidence(),
            exact_head="a" * 40,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(
            result["receipt"]["final_disposition"],
            "Landed — acceptance review required",
        )

    def test_invoking_semantic_review_without_a_pass_does_not_emit_done(self):
        snapshot = self.factory_snapshot({
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37", "plan_revision": "sha", "revision": 2, "status": "Landed — acceptance review required"},
            "children": [{"identifier": "GEN-38", "status": "Done", "owner": "agent"}],
        })
        result = review(
            snapshot,
            expected_plan_revision="sha",
            criteria=["a"],
            evidence=self.evidence(),
            semantic_review_invoked=True,
            exact_head="a" * 40,
        )
        self.assertTrue(result["ok"])
        self.assertFalse(result["receipt"]["semantic_review_passed"])
        self.assertEqual(
            result["receipt"]["final_disposition"],
            "Landed — acceptance review required",
        )

        invalid = review(
            snapshot,
            expected_plan_revision="sha",
            criteria=["a"],
            evidence=self.evidence(),
            semantic_review_passed=True,
            exact_head="a" * 40,
        )
        self.assertFalse(invalid["ok"])
        self.assertIn("semantic_review_result_without_invocation", invalid["errors"])

    def test_open_followup_is_not_done(self):
        snapshot = {"root": {"identifier": "GEN-37", "url": "https://linear/GEN-37", "plan_revision": "sha", "revision": 2, "status": "Done"}, "children": [{"identifier": "GEN-38", "status": "Todo", "owner": "agent", "next_action": "finish"}]}
        result = review(snapshot, expected_plan_revision="sha", criteria=["a"], evidence=self.evidence())
        self.assertFalse(result["ok"])
        self.assertIn("done_with_open_children", result["errors"])

    def test_landed_open_required_child_never_emits_done(self):
        snapshot = {
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37", "plan_revision": "sha", "revision": 2, "status": "Landed — acceptance review required"},
            "children": [{"identifier": "GEN-38", "status": "Todo", "owner": "agent", "next_action": "finish"}],
        }
        result = review(snapshot, expected_plan_revision="sha", criteria=["a"], evidence=self.evidence())
        self.assertFalse(result["ok"])
        self.assertIn("required_child_open:GEN-38", result["errors"])
        self.assertIsNone(result["receipt"])

    def test_absent_required_child_never_emits_done(self):
        snapshot = {
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37", "plan_revision": "sha", "revision": 2, "status": "Landed — acceptance review required"},
            "children": [],
        }
        result = review(
            snapshot,
            expected_plan_revision="sha",
            criteria=["a"],
            evidence=self.evidence(),
            required_child_ids={"GEN-38"},
        )
        self.assertFalse(result["ok"])
        self.assertIn("missing_child:GEN-38", result["errors"])
        self.assertIsNone(result["receipt"])

    def test_blocked_child_requires_review_condition(self):
        snapshot = {
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37", "plan_revision": "sha", "revision": 2, "status": "Landed — acceptance review required"},
            "children": [{"identifier": "GEN-38", "status": "Blocked", "owner": "agent", "next_action": "retry"}],
        }
        result = review(snapshot, expected_plan_revision="sha", criteria=["a"], evidence=self.evidence())
        self.assertFalse(result["ok"])
        self.assertIn("blocked_without_review_condition:GEN-38", result["errors"])

    def test_missing_criterion_and_category_fail_closed(self):
        snapshot = {"root": {"identifier": "GEN-37", "url": "https://linear/GEN-37", "plan_revision": "old", "revision": 2}, "children": []}
        result = review(snapshot, expected_plan_revision="sha", criteria=["a"], evidence={})
        self.assertFalse(result["ok"])
        self.assertIn("plan_sync_required", result["errors"])
        self.assertIn("criterion_not_proven:a", result["errors"])
        self.assertIn("missing_evidence_category:prs", result["errors"])

    def test_closure_reconciles_typed_high_risk_choices(self):
        snapshot = {
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37",
                     "plan_revision": "sha", "revision": 2, "status": "In Progress"},
            "children": [{"identifier": "GEN-38", "status": "Done", "owner": "agent"}],
        }
        choice = record_choice(
            choice_id="choice-authority", workstream_id="GEN-37", owning_child="GEN-38",
            namespace="pulp-playback", repository="github.com/generous-corp/pulp", repository_key="github.com:id:R_pulp",
            plan_revision="sha", git_head="a" * 40, created_at="2026-08-21T12:00:00Z",
            spec_gap="Writer authority unspecified", decision="Worker writes",
            alternatives=["Coordinator writes"], reach="system", irreversible=False,
            domains=["authority"], technical_confidence="high", intent_confidence="low",
        )
        result = review(
            snapshot, expected_plan_revision="sha", criteria=["a"],
            evidence=self.evidence(), choice_events=[choice], exact_head="a" * 40,
        )
        self.assertFalse(result["ok"])
        self.assertIn("choice_landing_blocked:choice-authority", result["errors"])

    def test_untransported_factory_surface_cannot_close_done(self):
        snapshot = self.factory_snapshot({
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37",
                     "plan_revision": "sha", "revision": 2, "status": "In Progress"},
            "children": [{"identifier": "GEN-38", "status": "Done", "owner": "agent"}],
        })
        snapshot["surface_availability"]["relations"] = "transport_unimplemented"
        result = review(
            snapshot, expected_plan_revision="sha", criteria=["a"], evidence=self.evidence(),
            semantic_review_invoked=True, semantic_review_passed=True, exact_head="a" * 40,
        )
        self.assertFalse(result["ok"])
        self.assertIn("transport_unimplemented:relations", result["errors"])

    def test_repository_head_keyset_and_each_child_evidence_are_closure_gates(self):
        snapshot = self.factory_snapshot({
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37",
                     "plan_revision": "sha", "revision": 2, "status": "In Progress"},
            "children": [{"identifier": "GEN-38", "status": "Done", "owner": "agent"}],
        })
        snapshot["evidence_contracts"] = []
        result = review(
            snapshot, expected_plan_revision="sha", criteria=["a"], evidence=self.evidence(),
            repository_heads={"github.com:id:R_wrong": "a" * 40},
        )
        self.assertFalse(result["ok"])
        self.assertIn("repository_head_keyset_mismatch", result["errors"])
        self.assertIn("missing_evidence_contract:GEN-38", result["errors"])

    def test_malformed_present_scope_never_skips_validation_or_emits_done(self):
        snapshot = {
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37",
                     "plan_revision": "sha", "revision": 2, "status": "In Progress"},
            "children": [],
            "scope": "not-a-scope", "relations": [], "choice_events": [],
            "evidence_contracts": [],
            "surface_availability": {field: "available" for field in
                                     ("scope", "relations", "choice_events", "evidence_contracts")},
        }
        result = review(
            snapshot, expected_plan_revision="sha", criteria=[], evidence={
                "decisions": [], "followups": [], "prs": [], "shipyard_receipts": [],
                "tests": [], "artifacts": [],
            }, repository_heads={}, semantic_review_invoked=True,
            semantic_review_passed=True,
        )
        self.assertFalse(result["ok"])
        self.assertIn("durable_surface_malformed:scope", result["errors"])
        self.assertIsNone(result["receipt"])


if __name__ == "__main__":
    unittest.main()
