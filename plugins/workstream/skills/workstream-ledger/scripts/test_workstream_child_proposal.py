#!/usr/bin/env python3
import base64
import hashlib
import unittest

from workstream_checkpoint import build_checkpoint
from workstream_child_proposal import (
    _append_proposal, _canonical, activated_comments, build_proposal,
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
        record = proposal["record"]
        expected_material_revision = 0
        predecessor_event_id = None
        if isinstance(record, dict):
            expected_material_revision = (
                record["expected_revision"] if proposal["kind"] == "event"
                else record["root_revision"]
            )
            if proposal["kind"] == "checkpoint":
                predecessor_event_id = record.get("predecessor_event_id")
        return {"value": {
            "proposal_id": proposal["proposal_id"],
            "proposal_remote_id": remote_id,
            "record_sha256": proposal["record_sha256"],
            "mutation_kind": "event", "child_workstream_id": "GEN-38",
            "child_issue_id": CHILD, "plan_revision": PLAN,
            "expected_material_revision": expected_material_revision,
            "predecessor_event_id": predecessor_event_id,
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
        receipt = _append_proposal(client, proposal)
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

    def test_global_multi_child_authorizations_filter_only_exact_identity(self):
        retired = self.proposal()
        retired_remote = proposal_slot_id(CHILD, retired["proposal_id"])
        other_child = "33333333-3333-4333-8333-333333333333"
        other_record = {**RECORD, "event_id": "other", "workstream_id": "GEN-39"}
        other = build_proposal(
            "event", other_record, child_workstream_id="GEN-39",
            child_issue_id=other_child, plan_revision="b" * 64,
        )
        other_remote = proposal_slot_id(other_child, other["proposal_id"])
        other_auth = {"value": {
            "proposal_id": other["proposal_id"],
            "proposal_remote_id": other_remote,
            "record_sha256": other["record_sha256"],
            "mutation_kind": "event", "child_workstream_id": "GEN-39",
            "child_issue_id": other_child, "plan_revision": "b" * 64,
            "expected_material_revision": other_record["expected_revision"],
            "predecessor_event_id": None,
        }}
        authorizations = [self.authorization(retired, retired_remote), other_auth]

        self.assertEqual(pending_proposal_obligations(
            [{"id": retired_remote, "body": encode_proposal(retired)}],
            authorizations, child_workstream_id="GEN-38",
            child_issue_id=CHILD, plan_revision="b" * 64,
        ), [])
        self.assertEqual(len(pending_proposal_obligations(
            [{"id": other_remote, "body": encode_proposal(other)}],
            [authorizations[0]], child_workstream_id="GEN-39",
            child_issue_id=other_child, plan_revision="b" * 64,
        )), 1)

        mismatched = self.authorization(retired, retired_remote)
        mismatched["value"]["record_sha256"] = "0" * 64
        with self.assertRaisesRegex(LinearTransportError, "foreign_child_proposal"):
            pending_proposal_obligations(
                [{"id": retired_remote, "body": encode_proposal(retired)}],
                [mismatched], child_workstream_id="GEN-38",
                child_issue_id=CHILD, plan_revision="b" * 64,
            )

    def test_altered_authorization_frontier_cannot_activate_or_suppress(self):
        proposal = self.proposal()
        remote = proposal_slot_id(CHILD, proposal["proposal_id"])
        comment = {"id": remote, "body": encode_proposal(proposal)}
        for field, value in (
            ("expected_material_revision", 1),
            ("predecessor_event_id", "wsc_wrong"),
        ):
            with self.subTest(field=field):
                authorization = self.authorization(proposal, remote)
                authorization["value"][field] = value
                with self.assertRaisesRegex(
                    LinearTransportError, "activated_child_proposal_mismatch",
                ):
                    activated_comments(
                        [comment], [authorization],
                        child_workstream_id="GEN-38", child_issue_id=CHILD,
                    )
                self.assertEqual(len(pending_proposal_obligations(
                    [comment], [authorization], child_workstream_id="GEN-38",
                    child_issue_id=CHILD, plan_revision=PLAN,
                )), 1)

    def test_wrapper_and_record_identity_must_match_before_encoding(self):
        with self.assertRaisesRegex(ValueError, "child proposal identity mismatch"):
            build_proposal(
                "event", {**RECORD, "workstream_id": "GEN-39"},
                child_workstream_id="GEN-38", child_issue_id=CHILD,
                plan_revision=PLAN,
            )
        checkpoint = build_checkpoint(
            workstream_id="GEN-38", boundary_id="identity", root_revision=0,
            plan_revision=PLAN, before_status="In Progress",
            after_status="In Progress", execution={
                "agent": "test", "provider": "test", "session_id": "test",
                "machine": "test", "worktree": {"state": "unavailable"},
            }, exact_head=None, evidence=[], blocker=None,
            next_action="continue",
        )
        with self.assertRaisesRegex(ValueError, "child proposal identity mismatch"):
            build_proposal(
                "checkpoint", checkpoint, child_workstream_id="GEN-38",
                child_issue_id=CHILD, plan_revision="b" * 64,
            )

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
