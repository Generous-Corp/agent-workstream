#!/usr/bin/env python3
import base64
import hashlib
import unittest

from workstream_child_proposal import (
    _canonical, activated_comments, append_proposal, build_proposal,
    decode_proposal, encode_proposal, pending_proposal_obligations, PREFIX,
    proposal_id, proposal_slot_id,
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

    def malformed_body(self, kind, record):
        value = {
            "schema_version": 1,
            "proposal_id": proposal_id(kind, record),
            "kind": kind, "child_workstream_id": "GEN-38",
            "child_issue_id": CHILD, "plan_revision": PLAN,
            "record": record,
            "record_sha256": hashlib.sha256(_canonical(record)).hexdigest(),
        }
        envelope = {
            "proposal": value,
            "sha256": hashlib.sha256(_canonical(value)).hexdigest(),
        }
        encoded = base64.urlsafe_b64encode(_canonical(envelope)).decode().rstrip("=")
        return value, f"{PREFIX}{encoded} -->"

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

    def test_activated_retired_plan_proposal_is_not_pending_or_foreign(self):
        proposal = self.proposal()
        remote = proposal_slot_id(CHILD, proposal["proposal_id"])
        auth = self.authorization(proposal, remote)
        comments = [{"id": remote, "body": encode_proposal(proposal)}]
        self.assertEqual(pending_proposal_obligations(
            comments, [auth], child_workstream_id="GEN-38",
            child_issue_id=CHILD, plan_revision="b" * 64,
        ), [])
        self.assertEqual(len(activated_comments(
            comments, [auth], child_workstream_id="GEN-38",
            child_issue_id=CHILD,
        )), 2)

    def test_decode_refuses_digested_bogus_kind_and_record(self):
        record = {}
        value = {
            "schema_version": 1,
            "proposal_id": proposal_id("bogus", record),
            "kind": "bogus", "child_workstream_id": "GEN-38",
            "child_issue_id": CHILD, "plan_revision": PLAN,
            "record": record,
            "record_sha256": hashlib.sha256(_canonical(record)).hexdigest(),
        }
        envelope = {
            "proposal": value,
            "sha256": hashlib.sha256(_canonical(value)).hexdigest(),
        }
        encoded = base64.urlsafe_b64encode(_canonical(envelope)).decode().rstrip("=")
        with self.assertRaisesRegex(LinearTransportError, "malformed_child_proposal"):
            decode_proposal(f"{PREFIX}{encoded} -->")

    def test_non_mapping_supported_records_fail_closed_at_build_and_encode(self):
        for kind in ("event", "checkpoint"):
            for record in (None, []):
                with self.subTest(kind=kind, record=record):
                    with self.assertRaisesRegex(ValueError, "invalid child proposal record"):
                        build_proposal(
                            kind, record, child_workstream_id="GEN-38",
                            child_issue_id=CHILD, plan_revision=PLAN,
                        )
                    value, _ = self.malformed_body(kind, record)
                    with self.assertRaisesRegex(ValueError, "invalid child proposal record"):
                        encode_proposal(value)

    def test_non_mapping_supported_records_refuse_decode_pending_and_activation(self):
        for kind in ("event", "checkpoint"):
            for record in (None, []):
                with self.subTest(kind=kind, record=record):
                    value, body = self.malformed_body(kind, record)
                    remote = proposal_slot_id(CHILD, value["proposal_id"])
                    comment = {"id": remote, "body": body}
                    with self.assertRaisesRegex(
                        LinearTransportError, "malformed_child_proposal"
                    ):
                        decode_proposal(body)
                    with self.assertRaisesRegex(
                        LinearTransportError, "malformed_child_proposal"
                    ):
                        pending_proposal_obligations(
                            [comment], [], child_workstream_id="GEN-38",
                            child_issue_id=CHILD, plan_revision=PLAN,
                        )
                    authorization = self.authorization(value, remote)
                    authorization["value"]["mutation_kind"] = kind
                    with self.assertRaisesRegex(
                        LinearTransportError, "malformed_child_proposal"
                    ):
                        activated_comments(
                            [comment], [authorization],
                            child_workstream_id="GEN-38", child_issue_id=CHILD,
                        )


if __name__ == "__main__":
    unittest.main()
