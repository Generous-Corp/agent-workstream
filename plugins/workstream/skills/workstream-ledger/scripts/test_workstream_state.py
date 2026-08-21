import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("workstream_state.py")
SPEC = importlib.util.spec_from_file_location("workstream_state", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
sys.modules["workstream_state"] = MODULE
SPEC.loader.exec_module(MODULE)


class StateTests(unittest.TestCase):
    def base(self):
        return {"revision": 4, "plan_revision": "plan-a", "status": "In Progress", "pr_head": "abc",
                "children": [{"key": "c1", "status": "done", "owner": "a"}]}

    def test_cas_preserves_history_and_rejects_stale_writer(self):
        root = self.base()
        updated = MODULE.apply_delta(root, 4, {"next_action": "test"})
        self.assertEqual(updated["revision"], 5)
        self.assertEqual(updated["history"][0]["delta"]["next_action"], "test")
        with self.assertRaises(MODULE.StateConflict):
            MODULE.apply_delta(root, 3, {"owner": "stale"})

    def test_live_merge_is_landed_not_done(self):
        result = MODULE.reconcile_external(self.base(), {"merged": True, "merge_sha": "m1", "pr_head": "abc"})
        self.assertEqual(result["status"], "Landed — acceptance review required")

    def test_live_head_drift_invalidates_receipt_and_records_contradiction(self):
        result = MODULE.reconcile_external(self.base(), {"merged": False, "pr_head": "def"})
        self.assertFalse(result["receipt_valid"])
        self.assertEqual(result["contradictions"][0]["kind"], "head_drift")
        self.assertEqual(result["pr_head"], "abc")

    def test_merge_at_a_different_head_fails_closed(self):
        result = MODULE.reconcile_external(
            self.base(), {"merged": True, "merge_sha": "m-wrong", "pr_head": "def"}
        )
        self.assertEqual(result["status"], "In Progress")
        self.assertEqual(result["pr_head"], "abc")
        self.assertNotIn("merge_sha", result)
        self.assertFalse(result["receipt_valid"])
        self.assertEqual(result["contradictions"][0]["kind"], "head_drift")

    def test_merge_without_both_exact_heads_fails_closed(self):
        root = self.base()
        root.pop("pr_head")
        result = MODULE.reconcile_external(
            root, {"merged": True, "merge_sha": "m-unknown", "pr_head": "def"}
        )
        self.assertEqual(result["status"], "In Progress")
        self.assertNotIn("merge_sha", result)
        self.assertEqual(result["contradictions"][0]["kind"], "exact_head_unavailable")

    def test_merge_without_merge_sha_fails_closed(self):
        result = MODULE.reconcile_external(
            self.base(), {"merged": True, "pr_head": "abc"}
        )
        self.assertEqual(result["status"], "In Progress")
        self.assertNotIn("merge_sha", result)
        self.assertFalse(result["receipt_valid"])
        self.assertEqual(result["contradictions"][0]["kind"], "merge_sha_unavailable")

    def test_closure_names_missing_children_and_plan_drift(self):
        errors = MODULE.closure_errors(self.base(), expected_plan_revision="plan-b", required_child_keys={"c1", "c2"})
        self.assertIn("plan_sync_required", errors)
        self.assertIn("missing_child:c2", errors)

    def test_closure_rejects_unowned_blocker_and_open_done_parent(self):
        root = self.base(); root["status"] = "Done"; root["children"].append({"key": "c2", "status": "blocked"})
        errors = MODULE.closure_errors(root, expected_plan_revision="plan-a", required_child_keys={"c1", "c2"})
        self.assertIn("unowned_nonterminal:c2", errors)
        self.assertIn("blocked_without_next_action:c2", errors)
        self.assertIn("blocked_without_review_condition:c2", errors)
        self.assertIn("done_with_open_children", errors)

    def test_landed_open_followup_cannot_emit_done_receipt(self):
        root = self.base(); root["status"] = "Done"; root["closure_receipt"] = None
        errors = MODULE.closure_errors(root, expected_plan_revision="plan-a", required_child_keys={"c1"},
                                       live={"merged": True, "pr_head": "abc"})
        self.assertIn("semantic_done_without_closure_receipt", errors)

    def test_landed_required_open_child_blocks_closure(self):
        root = self.base()
        root["status"] = "Landed — acceptance review required"
        root["children"] = [{"key": "c1", "status": "todo", "owner": "agent", "next_action": "finish"}]
        errors = MODULE.closure_errors(
            root, expected_plan_revision="plan-a", required_child_keys={"c1"}
        )
        self.assertIn("required_child_open:c1", errors)


if __name__ == "__main__":
    unittest.main()
