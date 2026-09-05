#!/usr/bin/env python3
"""One malformed material boundary must not brick a workstream.

A `material_boundary` payload missing `boundary_id` is rejected by
`validate_material_event_semantics`. Two independent properties must hold so
that rejection stays local to the offending record:

* a proposal carrying such a payload is refused *before* any remote publish, so
  the malformed record never reaches the ledger; and
* an already-published one is quarantined with a named verdict when history is
  replayed, so every later read of the workstream still succeeds.

The second property is what makes the first recoverable rather than merely
preventive: without it, a single bad record is unreadable forever and no
subsequent write can proceed.
"""

from __future__ import annotations

import base64
import hashlib
import unittest

from workstream_child_proposal import (
    _canonical, activated_comments, build_proposal, PREFIX, proposal_id,
    proposal_slot_id,
)


CHILD_ISSUE_ID = "22222222-2222-4222-8222-222222222222"
CHILD_TOKEN = "GEN-94"
PLAN = "a" * 64

VALID_PAYLOAD = {
    "boundary_id": "boundary-under-test",
    "changes": [{"kind": "progress", "payload": {"ok": True}}],
}
# The defect as it actually occurred: `boundary` where the schema requires
# `boundary_id`. Everything else about the record is well formed.
MALFORMED_PAYLOAD = {
    "boundary": "boundary-under-test",
    "changes": [{"kind": "progress", "payload": {"ok": True}}],
}


def _record(payload: dict, *, event_id: str) -> dict:
    return {
        "event_id": event_id,
        "workstream_id": CHILD_TOKEN,
        "kind": "material_boundary",
        "source": "agent_discovery",
        "payload": payload,
        "expected_revision": 4,
        "created_at": "2026-09-05T01:00:58Z",
    }


def _published_proposal(record: dict) -> dict:
    """A proposal already stored remotely, built without the write-path guard.

    Constructed directly rather than through ``build_proposal`` because the
    point of the guard is that ``build_proposal`` will refuse this record. The
    malformed proposal this models is one that reached the ledger before the
    guard existed.
    """
    return {
        "schema_version": 1,
        "kind": "event",
        "child_workstream_id": CHILD_TOKEN,
        "child_issue_id": CHILD_ISSUE_ID,
        "plan_revision": PLAN,
        "record": record,
        "record_sha256": hashlib.sha256(_canonical(record)).hexdigest(),
        "proposal_id": proposal_id("event", record),
    }


def _authorization(proposal: dict) -> dict:
    return {
        "value": {
            "child_workstream_id": CHILD_TOKEN,
            "child_issue_id": CHILD_ISSUE_ID,
            "proposal_id": proposal["proposal_id"],
            "record_sha256": proposal["record_sha256"],
            "mutation_kind": "event",
            "plan_revision": PLAN,
            "proposal_remote_id": proposal_slot_id(
                CHILD_ISSUE_ID, proposal["proposal_id"],
            ),
            "expected_material_revision": proposal["record"]["expected_revision"],
            "predecessor_event_id": None,
        },
    }


def _legacy_encode_proposal(value: dict) -> str:
    """Encode exactly as the unguarded writer did, bypassing today's validation.

    ``encode_proposal`` now refuses a malformed record, which is the fix. This
    reproduces the bytes a pre-fix writer stored, because that is the history a
    reader has to survive.
    """
    envelope = {
        "proposal": value,
        "sha256": hashlib.sha256(_canonical(value)).hexdigest(),
    }
    encoded = base64.urlsafe_b64encode(_canonical(envelope)).decode().rstrip("=")
    return f"{PREFIX}{encoded} -->"


def _authorized_pair(payload: dict, *, event_id: str) -> tuple[list, list]:
    proposal = _published_proposal(_record(payload, event_id=event_id))
    comment = {
        "id": proposal_slot_id(CHILD_ISSUE_ID, proposal["proposal_id"]),
        "body": _legacy_encode_proposal(proposal),
    }
    return [comment], [_authorization(proposal)]


class BuildProposalRefusesBeforePublish(unittest.TestCase):
    """Defect 1a: nothing malformed may reach the ledger."""

    def test_malformed_material_boundary_is_refused(self):
        with self.assertRaises(ValueError) as caught:
            build_proposal(
                "event", _record(MALFORMED_PAYLOAD, event_id="wsd_malformed"),
                child_workstream_id=CHILD_TOKEN,
                child_issue_id=CHILD_ISSUE_ID, plan_revision=PLAN,
            )
        message = str(caught.exception)
        self.assertIn("boundary_id", message)
        self.assertIn("wsd_malformed", message)

    def test_valid_material_boundary_still_builds(self):
        """Negative control: the guard must not reject well-formed records."""
        proposal = build_proposal(
            "event", _record(VALID_PAYLOAD, event_id="wsd_valid"),
            child_workstream_id=CHILD_TOKEN,
            child_issue_id=CHILD_ISSUE_ID, plan_revision=PLAN,
        )
        self.assertEqual(proposal["kind"], "event")
        self.assertEqual(proposal["record"]["payload"], VALID_PAYLOAD)


class ReadPathQuarantinesInsteadOfRefusing(unittest.TestCase):
    """Defect 1b: an already-published malformed record stays readable."""

    def test_workstream_remains_readable(self):
        comments, authorizations = _authorized_pair(
            MALFORMED_PAYLOAD, event_id="wsd_malformed",
        )
        quarantine: list[dict] = []
        result = activated_comments(
            comments, authorizations, child_workstream_id=CHILD_TOKEN,
            child_issue_id=CHILD_ISSUE_ID, quarantine=quarantine,
        )
        self.assertEqual(len(result), len(comments))
        self.assertEqual(len(quarantine), 1)

    def test_verdict_names_the_record_and_the_field(self):
        """Defect 2: a named verdict, not an opaque refusal."""
        comments, authorizations = _authorized_pair(
            MALFORMED_PAYLOAD, event_id="wsd_malformed",
        )
        quarantine: list[dict] = []
        activated_comments(
            comments, authorizations, child_workstream_id=CHILD_TOKEN,
            child_issue_id=CHILD_ISSUE_ID, quarantine=quarantine,
        )
        verdict = quarantine[0]
        self.assertEqual(verdict["verdict"], "quarantined_undecodable_record")
        self.assertEqual(verdict["origin"], "preexisting_record")
        self.assertEqual(verdict["event_id"], "wsd_malformed")
        self.assertEqual(verdict["field"], "boundary_id")
        self.assertEqual(verdict["child_workstream_id"], CHILD_TOKEN)

    def test_valid_authorized_proposal_is_still_activated(self):
        """Negative control: quarantine must not swallow good history.

        Without this, a fix that quarantined *everything* would pass the two
        tests above while silently discarding the workstream.
        """
        comments, authorizations = _authorized_pair(
            VALID_PAYLOAD, event_id="wsd_valid",
        )
        quarantine: list[dict] = []
        result = activated_comments(
            comments, authorizations, child_workstream_id=CHILD_TOKEN,
            child_issue_id=CHILD_ISSUE_ID, quarantine=quarantine,
        )
        self.assertEqual(quarantine, [])
        self.assertEqual(len(result), len(comments) + 1)

    def test_valid_record_survives_alongside_a_malformed_one(self):
        """The decisive case: one bad record must not take the good one down."""
        bad_comments, bad_auth = _authorized_pair(
            MALFORMED_PAYLOAD, event_id="wsd_malformed",
        )
        good_comments, good_auth = _authorized_pair(
            VALID_PAYLOAD, event_id="wsd_valid",
        )
        quarantine: list[dict] = []
        result = activated_comments(
            [*bad_comments, *good_comments], [*bad_auth, *good_auth],
            child_workstream_id=CHILD_TOKEN,
            child_issue_id=CHILD_ISSUE_ID, quarantine=quarantine,
        )
        self.assertEqual([item["event_id"] for item in quarantine],
                         ["wsd_malformed"])
        self.assertEqual(len(result), 3)


if __name__ == "__main__":
    unittest.main()
