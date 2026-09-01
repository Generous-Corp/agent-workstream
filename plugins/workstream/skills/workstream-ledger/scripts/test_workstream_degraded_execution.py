#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import replace
from copy import deepcopy
import tempfile
import unittest
from pathlib import Path

from workstream_degraded_execution import (
    AuthenticatedGrantIssuer, DegradedExecutionError,
    DegradedExecutionOutbox, authorize_active_owner,
)
from workstream_delta import DeltaJournal, MutationReceipt

HEAD = "a" * 40
PLAN = "b" * 64
OWNER = {
    "agent": "codex", "provider": "openai", "session_id": "owner-session",
    "machine": "M5", "session_incarnation": "incarnation-1",
}


def full_context():
    return {
        "resume_authority": "full", "authority_scope": "executable_current",
        "workstream_id": "GEN-14", "owner": deepcopy(OWNER),
        "authenticated_route": {
            "workspace_uuid": "workspace-uuid", "team_uuid": "team-uuid",
            "project_uuid": "project-uuid", "root_issue_uuid": "root-uuid",
        },
        "source": {"identity": "https://example.test/plan", "sha256": PLAN},
        "generation": {
            "plan_revision": PLAN, "activation_epoch": 4,
            "transition_tip_event_id": "transition-4",
        },
        "frontiers": {
            "material_revision": 7, "projection_revision": 4,
            "checkpoint_event_id": "checkpoint-7",
            "graph_frontier_sha256": "c" * 64,
        },
        "worktree": {
            "path": "/safe/worktree", "branch": "fix/exact", "head": HEAD,
            "state": "safe",
        },
        "repository": {"repository_key": "github.com:id:R_repo", "exact_head": HEAD},
        "shipyard": {
            "run_id": "sy-exact", "repository_key": "github.com:id:R_repo",
            "exact_head": HEAD, "ownership_state": "accepted",
        },
        "snapshot_sha256": "d" * 64,
        "authenticated_at": "2026-09-01T04:00:00Z",
        "snapshot_created_at": "2026-09-01T03:59:59Z",
    }


class FakeAdapter:
    supports_atomic_cas = False
    supports_append_only_events = True

    def __init__(self):
        self.events = {}
        self.revision = 7
        self.apply_calls = 0

    def apply(self, delta):
        self.apply_calls += 1
        receipt = self.events.get(delta.event_id)
        if receipt is None:
            self.revision = max(self.revision, delta.expected_revision) + 1
            receipt = MutationReceipt(delta.event_id, self.revision,
                                      f"linear:{delta.event_id}")
            self.events[delta.event_id] = receipt
        return receipt

    def current_revision(self, _workstream_id):
        return self.revision


class DegradedExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.journal = DeltaJournal(Path(self.temp.name) / "outbox.sqlite3")
        self.live = full_context()
        self.issuer = AuthenticatedGrantIssuer(lambda value: value is self.live)
        self.grant = self.issuer.issue(self.live, turn_id="turn-1",
                                      lifetime_seconds=10, monotonic_now=100)
        self.outbox = DegradedExecutionOutbox(self.journal, self.grant, self.issuer)

    def tearDown(self):
        self.journal.close()
        self.temp.cleanup()

    def record(self, **overrides):
        args = {
            "requester": OWNER, "turn_id": "turn-1",
            "tracking_failure": "linear_http_503",
            "write_outcome": "read_only_failed", "monotonic_now": 101,
            "boundary_id": "compile-fix",
            "changes": [{"kind": "progress", "payload": {"next": "test"}}],
        }
        args.update(overrides)
        return self.outbox.record_boundary(**args)

    def test_active_owner_can_deliver_but_not_claim_tracking_or_closure(self):
        result = self.record()
        self.assertEqual(result["resume_authority"], "degraded_continuation")
        self.assertTrue(result["provider_or_local_implementation_allowed"])
        self.assertTrue(result["exact_head_shipyard_handoff_or_landing_allowed"])
        self.assertFalse(result["linear_mutation_allowed"])
        self.assertFalse(result["root_or_generation_transition_allowed"])
        self.assertFalse(result["semantic_closure_allowed"])
        self.assertFalse(result["linear_resume_or_handoff_certification_allowed"])
        self.assertEqual(len(self.journal.pending()), 1)
        self.assertEqual(self.outbox.certification_blockers(), [
            "tracking_reconciliation_required", "semantic_closure_blocked",
            "linear_resume_handoff_certification_blocked",
        ])

    def test_fresh_copied_wrong_turn_and_expired_grants_refuse_without_write(self):
        cases = [
            {"requester": {**OWNER, "session_id": "fresh"}},
            {"turn_id": "turn-2"},
            {"monotonic_now": 110},
        ]
        for overrides in cases:
            with self.subTest(overrides=overrides), self.assertRaises(DegradedExecutionError):
                self.record(**overrides)
        copied = replace(self.grant)
        with self.assertRaisesRegex(DegradedExecutionError, "live_same_process"):
            authorize_active_owner(
                copied, requester=OWNER, turn_id="turn-1",
                tracking_failure="linear_http_503", write_outcome="no_request_sent",
                monotonic_now=101)
        self.assertEqual(self.journal.pending(), [])

    def test_only_narrow_prewrite_or_readonly_availability_failures_degrade(self):
        for failure in ("linear_auth_unavailable", "linear_http_404", "unknown"):
            with self.subTest(failure=failure), self.assertRaisesRegex(
                    DegradedExecutionError, "not_retriable"):
                self.record(tracking_failure=failure)
        for outcome in ("unknown_after_write", "write_timeout", "accepted_unknown"):
            with self.subTest(outcome=outcome), self.assertRaisesRegex(
                    DegradedExecutionError, "ambiguous_postwrite"):
                self.record(write_outcome=outcome)
        allowed = self.record(tracking_failure="linear_read_timeout",
                              write_outcome="no_request_sent")
        self.assertTrue(allowed["durable_local_outbox"])

    def test_lifecycle_mutations_refuse_but_discovered_deltas_buffer(self):
        for kind in ("scope", "generation", "closure", "attach", "successor"):
            with self.subTest(kind=kind), self.assertRaisesRegex(
                    DegradedExecutionError, "degraded_change_forbidden"):
                self.record(boundary_id=f"forbid-{kind}",
                            changes=[{"kind": kind, "payload": {"x": 1}}])
        result = self.record(boundary_id="discovery", changes=[
            {"kind": "requirement", "payload": {"text": "new edge"}},
            {"kind": "decision", "payload": {"text": "preserve evidence"}},
        ])
        self.assertTrue(result["durable_local_outbox"])

    def test_reconciliation_requires_fresh_live_full_validation_and_is_idempotent(self):
        first = self.record()
        replay = self.record()
        self.assertEqual(first["event_id"], replay["event_id"])
        self.assertTrue(replay["replay"])
        self.assertEqual(len(self.journal.pending()), 1)
        adapter = FakeAdapter()

        copied_context = deepcopy(self.live)
        with self.assertRaisesRegex(DegradedExecutionError, "authenticated_full"):
            self.outbox.reconcile(adapter, live_context=copied_context)
        self.assertEqual(adapter.apply_calls, 0)

        receipts = self.outbox.reconcile(adapter, live_context=self.live)
        self.assertEqual(len(receipts), 1)
        self.assertEqual(adapter.apply_calls, 1)
        self.assertEqual(len(adapter.events), 1)
        self.assertEqual(self.outbox.reconcile(adapter, live_context=self.live), [])
        self.assertEqual(adapter.apply_calls, 1)
        post = self.record()
        self.assertTrue(post["replay"])
        self.assertEqual(len(self.journal.pending()), 0)

    def test_reconciliation_binding_drift_refuses_before_remote_mutation(self):
        self.record()
        cases = []
        for key, mutation in (
            ("owner", lambda value: value["owner"].update(session_id="other")),
            ("authenticated_route", lambda value: value["authenticated_route"].update(
                project_uuid="other-project")),
            ("source", lambda value: (value["source"].update(sha256="e" * 64),
                                      value["generation"].update(plan_revision="e" * 64))),
            ("generation", lambda value: value["generation"].update(activation_epoch=5)),
            ("frontiers", lambda value: value["frontiers"].update(projection_revision=5)),
            ("worktree", lambda value: value["worktree"].update(branch="other")),
            ("repository", lambda value: (value["repository"].update(
                repository_key="github.com:id:R_other"), value["shipyard"].update(
                repository_key="github.com:id:R_other"))),
            ("shipyard", lambda value: value["shipyard"].update(run_id="sy-other")),
            ("snapshot_sha256", lambda value: value.update(snapshot_sha256="f" * 64)),
            ("authenticated_at", lambda value: value.update(
                authenticated_at="2026-09-01T04:00:01Z")),
            ("snapshot_created_at", lambda value: value.update(
                snapshot_created_at="2026-09-01T03:59:58Z")),
        ):
            drifted = deepcopy(self.live)
            mutation(drifted)
            cases.append((key, drifted))
        for key, drifted in cases:
            with self.subTest(key=key):
                issuer = AuthenticatedGrantIssuer(lambda value, expected=drifted: value is expected)
                outbox = DegradedExecutionOutbox(self.journal, self.grant, issuer)
                adapter = FakeAdapter()
                with self.assertRaisesRegex(DegradedExecutionError,
                                            f"binding_changed:{key}"):
                    outbox.reconcile(adapter, live_context=drifted)
                self.assertEqual(adapter.apply_calls, 0)

    def test_unrelated_pending_workstream_does_not_block_this_workstream(self):
        self.journal.append_boundary(
            "GEN-99", "other", [{"kind": "progress", "payload": {"x": 1}}], 0)
        self.assertEqual(self.outbox.certification_blockers(), [])

    def test_validator_rejection_cannot_mint_authority(self):
        with self.assertRaisesRegex(DegradedExecutionError, "authenticated_full"):
            AuthenticatedGrantIssuer(lambda _value: False).issue(
                full_context(), turn_id="turn-x", monotonic_now=100)


if __name__ == "__main__":
    unittest.main()
