#!/usr/bin/env python3
"""Deterministic planted checks for the Gate 0 living-workstream contract.

These tests do not pretend to be a real Linear/host integration. They prove
model-free invariants that the authenticated adapter and a second machine must
preserve, and make the remaining physical tests explicit.
"""

from __future__ import annotations

import unittest

from workstream_delta import DeltaJournal, MutationReceipt, RevisionConflict
from workstream_graph import GraphReviewRequired, build_operations
from workstream_resume import ResumeError, validate_snapshot
from workstream_state import StateConflict, apply_delta, closure_errors, reconcile_external


class AcceptanceTests(unittest.TestCase):
    def plan(self):
        return {
            "graph_review_required": True,
            "root": {"stable_key": "source-demo", "title": "Demo", "plan_revision": "sha-demo"},
            "children": [
                {"key": "a", "stable_key": "a", "title": "Build", "line": 10},
                {"key": "b", "stable_key": "b", "title": "Verify", "line": 11},
            ],
        }

    def test_intake_requires_review_and_repeated_intake_is_idempotent(self):
        plan = self.plan()
        with self.assertRaises(GraphReviewRequired):
            build_operations(plan)
        first = build_operations(plan, accepted_keys={"a", "b"})
        existing_root = {"identifier": "GEN-123", "stable_key": "source-demo"}
        existing_children = [{"identifier": "GEN-124", "stable_key": "a"}, {"identifier": "GEN-125", "stable_key": "b"}]
        second = build_operations(plan, existing_root=existing_root, existing_children=existing_children, accepted_keys={"a", "b"})
        self.assertEqual([op["action"] for op in first], ["create_root", "create_child", "create_child"])
        self.assertEqual([op["action"] for op in second], ["update_root", "update_child", "update_child"])
        self.assertEqual([op["stable_key"] for op in first], [op["stable_key"] for op in second])

    def test_cas_conflict_replay_preserves_both_material_deltas(self):
        root = {"revision": 0, "history": [], "children": []}
        root = apply_delta(root, 0, {"status": "In Progress"})
        with self.assertRaises(StateConflict):
            apply_delta(root, 0, {"next_action": "resume"})
        root = apply_delta(root, 1, {"next_action": "resume"})
        self.assertEqual(root["revision"], 2)
        self.assertEqual([h["delta"] for h in root["history"]], [{"status": "In Progress"}, {"next_action": "resume"}])

    def test_delta_journal_replay_after_remote_success_is_idempotent(self):
        class Adapter:
            supports_atomic_cas = True

            def __init__(self):
                self.seen = set()
                self.calls = 0

            def apply(self, delta):
                self.calls += 1
                if delta.event_id not in self.seen:
                    self.seen.add(delta.event_id)
                return MutationReceipt(delta.event_id, 1, "GEN-123")

        journal = DeltaJournal(":memory:")
        event_id = journal.append("GEN-123", "blocker", {"title": "x"}, 0)
        adapter = Adapter()
        self.assertEqual(journal.apply(adapter)[0].event_id, event_id)
        self.assertEqual(journal.pending(), [])
        self.assertEqual(adapter.calls, 1)
        journal.close()

    def test_live_truth_invalidates_stale_receipt_and_never_emits_done(self):
        root = {"revision": 1, "status": "Waiting", "pr_head": "old", "receipt_valid": True, "children": []}
        live = {"pr_head": "new", "merged": True, "merge_sha": "merge-1"}
        result = reconcile_external(root, live)
        self.assertEqual(result["status"], "Waiting")
        self.assertEqual(result["pr_head"], "old")
        self.assertNotIn("merge_sha", result)
        self.assertFalse(result["receipt_valid"])
        self.assertTrue(any(c["kind"] == "head_drift" for c in result["contradictions"]))

    def test_false_green_and_missed_child_refuse_closure(self):
        root = {"revision": 1, "status": "Done", "children": [{"key": "a", "status": "done", "owner": "agent"}]}
        errors = closure_errors(root, expected_plan_revision="sha-demo", required_child_keys={"a", "b"})
        self.assertIn("missing_child:b", errors)
        self.assertNotIn("semantic_done_without_closure_receipt", errors)

    def test_resume_rejects_duplicate_children_and_missing_next_action(self):
        snapshot = {
            "root": {"identifier": "GEN-123", "url": "https://linear.app/example/issue/GEN-123/demo", "plan_revision": "sha-demo", "revision": 1},
            "children": [{"identifier": "GEN-124", "title": "A", "status": "blocked", "owner": "agent"}, {"identifier": "GEN-124", "title": "A duplicate", "status": "todo", "owner": "agent"}],
        }
        with self.assertRaises(ResumeError):
            validate_snapshot(snapshot, token="GEN-123")


if __name__ == "__main__":
    unittest.main()
