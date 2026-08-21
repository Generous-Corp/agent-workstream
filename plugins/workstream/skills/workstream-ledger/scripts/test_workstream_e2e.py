#!/usr/bin/env python3
"""Small offline end-to-end proof of intake -> resume -> closure boundaries."""

import unittest

from test_workstream_linear import FakeClient
from workstream_closure import review
from workstream_linear import LinearGraphQLTransport
from workstream_resume import compact_context
from workstream_successor import choose_disposition


class WorkstreamE2ETests(unittest.TestCase):
    def test_legacy_transport_flow_is_explicitly_not_factory_contract_closure_ready(self):
        plan = {
            "graph_review_required": True,
            "root": {"stable_key": "source-demo", "title": "Demo", "plan_revision": "sha-demo", "next_action": "Implement the child."},
            "children": [{"key": "a", "stable_key": "a", "title": "Build", "next_action": "Implement the child."}],
        }
        transport = LinearGraphQLTransport(FakeClient(), team_id="team")
        transport.apply_reviewed_plan(plan, accepted_keys={"a"})
        snapshot = transport.snapshot_for_root("GEN-1")
        context = compact_context(snapshot, "GEN-1")
        self.assertEqual(context["workstream_id"], "GEN-1")
        snapshot["provenance"] = {"worktree": {"state": "stale", "path": "/old", "head": "old"}}
        self.assertEqual(choose_disposition(snapshot)["disposition"], "create_successor")
        snapshot["children"][0].update({"status": "Done", "owner": "agent", "next_action": "complete"})
        result = review(snapshot, expected_plan_revision="sha-demo", criteria=["a"], evidence={
            "a": {"satisfied": True}, "decisions": [], "followups": [], "prs": [],
            "shipyard_receipts": [], "tests": [], "artifacts": [],
        })
        self.assertFalse(result["ok"])
        self.assertIn("transport_unimplemented:scope", result["errors"])
        self.assertIn("transport_unimplemented:evidence_contracts", result["errors"])


if __name__ == "__main__":
    unittest.main()
