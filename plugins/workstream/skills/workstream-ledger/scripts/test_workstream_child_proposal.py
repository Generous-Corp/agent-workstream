#!/usr/bin/env python3
import unittest

from workstream_child_proposal import (
    activated_comments, append_proposal, build_proposal, encode_proposal,
    proposal_slot_id,
)
from workstream_linear import LinearTransportError


CHILD = "22222222-2222-4222-8222-222222222222"
PLAN = "a" * 64
RECORD = {
    "event_id": "child-event", "workstream_id": "GEN-38",
    "kind": "progress", "source": "user_turn", "payload": {"ok": True},
    "expected_revision": 0, "created_at": "now",
}


class PagedClient:
    def __init__(self, proposal):
        self.proposal = proposal
        self.calls = []
    def execute(self, query, variables):
        self.calls.append((query, variables))
        if "WorkstreamDeltaComments" in query:
            if variables["after"] is None:
                return {"issue": {"comments": {"nodes": [{"id": "plain", "body": "x"}],
                    "pageInfo": {"hasNextPage": True, "endCursor": "next"}}}}
            return {"issue": {"comments": {"nodes": [{
                "id": proposal_slot_id(CHILD, self.proposal["proposal_id"]),
                "body": encode_proposal(self.proposal),
            }], "pageInfo": {"hasNextPage": False, "endCursor": None}}}}
        raise AssertionError("append should find the second-page proposal")


class ChildProposalTests(unittest.TestCase):
    def proposal(self):
        return build_proposal(
            "event", RECORD, child_workstream_id="GEN-38",
            child_issue_id=CHILD, plan_revision=PLAN,
        )

    def authorization(self, proposal, remote_id):
        return {"value": {
            "proposal_id": proposal["proposal_id"],
            "proposal_remote_id": remote_id,
            "record_sha256": proposal["record_sha256"],
            "mutation_kind": "event", "child_workstream_id": "GEN-38",
            "child_issue_id": CHILD, "plan_revision": PLAN,
        }}

    def test_append_replay_finds_full_paginated_connection(self):
        proposal = self.proposal(); client = PagedClient(proposal)
        receipt = append_proposal(client, proposal)
        self.assertEqual(receipt["disposition"], "existing")
        self.assertEqual([call[1]["after"] for call in client.calls], [None, "next"])

    def test_duplicate_and_conflicting_proposals_fail_closed(self):
        proposal = self.proposal(); remote = proposal_slot_id(CHILD, proposal["proposal_id"])
        auth = self.authorization(proposal, remote)
        base = {"id": remote, "body": encode_proposal(proposal)}
        with self.assertRaisesRegex(LinearTransportError, "duplicate_child_proposal"):
            activated_comments([base, {**base, "id": "other"}], [auth],
                               child_workstream_id="GEN-38", child_issue_id=CHILD)

    def test_foreign_child_or_plan_proposal_refuses_activation(self):
        for field, value in (("child_issue_id", "33333333-3333-4333-8333-333333333333"),
                             ("plan_revision", "b" * 64)):
            with self.subTest(field=field):
                proposal = self.proposal(); proposal[field] = value
                # Rebuild the envelope fields without changing the record identity.
                body = encode_proposal(proposal)
                remote = proposal_slot_id(CHILD, proposal["proposal_id"])
                auth = self.authorization(proposal, remote)
                auth["value"][field] = CHILD if field == "child_issue_id" else PLAN
                with self.assertRaisesRegex(LinearTransportError, "mismatch"):
                    activated_comments([{"id": remote, "body": body}], [auth],
                                       child_workstream_id="GEN-38", child_issue_id=CHILD)


if __name__ == "__main__":
    unittest.main()
