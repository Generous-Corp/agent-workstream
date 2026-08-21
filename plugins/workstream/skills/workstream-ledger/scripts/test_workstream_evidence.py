#!/usr/bin/env python3
import unittest

from workstream_evidence import closure_ready, evidence_errors


class EvidenceContractTests(unittest.TestCase):
    def contract(self):
        na = lambda reason: {"status": "not_applicable", "reason": reason}
        receipt = lambda kind, **extra: {
            "kind": kind, "passed": True, "repository_key": "github.com:id:R_pulp",
            "exact_head": "a" * 40, "proof": f"{kind} receipt", **extra,
        }
        return {
            "slice_id": "cache-owner", "owning_child": "GEN-38",
            "repository": "github.com/generous-corp/pulp",
            "repository_key": "github.com:id:R_pulp",
            "plan_revision": "plan-a", "exact_head": "a" * 40,
            "layers": {
                "architecture": {"status": "required", "owned_seam": "cache adapter",
                                 "trust_boundary": "worker to disk", "allowed_side_effects": ["cache write"],
                                 "receipts": [receipt("review", status="accepted")]},
                "logic": {"status": "required", "methods": ["unit", "property"],
                          "receipts": [receipt("test")]},
                "component": {"status": "required", "uses_fakes": True,
                              "fake_scope": "external_edge_only", "receipts": [receipt("test")]},
                "adapter": {"status": "required", "mode": "contract_fake", "receipts": [receipt("test")]},
                "e2e": na("No user journey in this schema-only slice"),
                "visual": na("No visual output"),
                "operational": {"status": "required", "receipts": [receipt("test-run")]},
                "negative_control": {"status": "required", "failure_detected": True,
                                     "receipts": [receipt("planted-invalid-input")]},
            },
        }

    def test_complete_layered_contract_is_closure_ready(self):
        self.assertTrue(closure_ready(self.contract()))

    def test_internal_fakes_and_ambiguous_adapter_proof_are_rejected(self):
        contract = self.contract()
        contract["layers"]["component"]["fake_scope"] = "whole_system"
        contract["layers"]["adapter"].pop("mode")
        errors = evidence_errors(contract)
        self.assertIn("component_fake_crosses_internal_seam", errors)
        self.assertIn("adapter_mode_ambiguous", errors)

    def test_screenshots_never_replace_behavioral_proof(self):
        contract = self.contract()
        contract["layers"]["visual"] = {"status": "required", "primary_proof": True,
                                        "receipts": [{"kind": "screenshot", "passed": True,
                                                      "repository_key": "github.com:id:R_pulp",
                                                      "exact_head": "a" * 40,
                                                      "proof": "captured screenshot"}]}
        self.assertIn("screenshot_cannot_be_primary_proof", evidence_errors(contract))

    def test_exact_head_receipt_and_negative_control_are_semantic_gates(self):
        contract = self.contract()
        contract["layers"]["operational"]["receipts"][0]["exact_head"] = "b" * 40
        contract["layers"]["negative_control"]["failure_detected"] = False
        errors = evidence_errors(contract)
        self.assertIn("stale_operational_receipt", errors)
        self.assertIn("negative_control_did_not_detect_failure", errors)

    def test_every_layer_must_be_explained_even_when_not_applicable(self):
        contract = self.contract()
        del contract["layers"]["e2e"]
        self.assertIn("missing_layer:e2e", evidence_errors(contract))

    def test_failed_receipt_is_not_proof_and_repository_is_canonical(self):
        contract = self.contract()
        contract["layers"]["logic"]["receipts"] = [{"kind": "test", "passed": False,
                                                       "repository_key": "github.com:id:R_pulp",
                                                       "exact_head": "a" * 40,
                                                       "proof": "failing test receipt"}]
        contract["repository"] = "https://github.com/Generous-Corp/pulp.git"
        errors = evidence_errors(contract)
        self.assertIn("unsuccessful_receipt:logic", errors)
        self.assertIn("invalid:repository_not_canonical", errors)

    def test_owning_child_must_be_stable_issue_token(self):
        contract = self.contract()
        contract["owning_child"] = "cache-child"
        self.assertIn("invalid:owning_child", evidence_errors(contract))

    def test_receipt_for_another_repository_or_head_is_not_proof(self):
        contract = self.contract()
        receipt = contract["layers"]["logic"]["receipts"][0]
        receipt["repository_key"] = "github.com:id:R_other"
        self.assertIn("unsuccessful_receipt:logic", evidence_errors(contract))


if __name__ == "__main__":
    unittest.main()
