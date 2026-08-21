#!/usr/bin/env python3
import unittest

from workstream_checkpoint import acknowledge_checkpoint, build_checkpoint
from workstream_recovery_canary import RecoveryCanaryError, evaluate


class RecoveryCanaryTests(unittest.TestCase):
    def checkpoint(self, *, acknowledged=True, workstream="GEN-37"):
        item = build_checkpoint(
            workstream_id=workstream, boundary_id="pre-death", root_revision=4,
            plan_revision="plan-sha", before_status="In Progress", after_status="In Progress",
            execution={"agent": "codex", "provider": "openai", "session_id": "source-session",
                       "machine": "M1", "worktree": {"state": "unavailable"}},
            exact_head="a" * 40, evidence=[{"kind": "test", "id": "focused"}], blocker=None,
            next_action="resume on M3", predecessor_event_id=None,
        )
        return acknowledge_checkpoint(item, "linear-comment-4", 4) if acknowledged else item

    def record(self, checkpoint, workstream="GEN-37"):
        return {
            "resume_token": workstream,
            "canonical_plan": {"url": "https://github.com/org/plans/blob/main/plan.md", "revision": "plan-sha"},
            "root": {"identifier": workstream, "context_url": f"https://linear.app/team/issue/{workstream}/demo", "revision": 4},
            "source": {"agent": "codex", "provider": "openai", "session_id": "source-session", "machine": "M1"},
            "source_termination": {"phase": "after_remote_ack_before_final_response", "process_unavailable": True},
            "recovery": {"agent": "claude", "provider": "anthropic", "session_id": "recovery-session", "machine": "M3"},
            "remote_observation": {"event_id": checkpoint["event_id"], "remote_id": "linear-comment-4", "applied_revision": 4},
        }

    def test_exact_cross_machine_record_passes_without_live_claim(self):
        checkpoint = self.checkpoint()
        receipt = evaluate(self.record(checkpoint), [checkpoint])
        self.assertEqual(receipt["result"], "contract_pass")
        self.assertEqual(receipt["evidence_scope"], "supplied_observation_only")
        self.assertFalse(receipt["live_mutations_performed"])
        self.assertEqual(receipt["next_action"], "resume on M3")

    def test_non_gen_team_token_passes(self):
        checkpoint = self.checkpoint(workstream="OPS-37")
        receipt = evaluate(self.record(checkpoint, workstream="OPS-37"), [checkpoint])
        self.assertEqual(receipt["resume_token"], "OPS-37")

    def test_death_before_remote_ack_is_refused(self):
        pending = self.checkpoint(acknowledged=False)
        record = self.record(pending)
        record["source_termination"]["phase"] = "before_remote_ack"
        with self.assertRaisesRegex(RecoveryCanaryError, "source_death_not_after_remote_ack"):
            evaluate(record, [pending])

    def test_pending_checkpoint_cannot_be_promoted_by_claimed_observation(self):
        pending = self.checkpoint(acknowledged=False)
        with self.assertRaisesRegex(RecoveryCanaryError, "checkpoint_not_remote_acknowledged"):
            evaluate(self.record(pending), [pending])

    def test_plan_root_and_remote_ack_mismatches_fail_closed(self):
        checkpoint = self.checkpoint()
        for mutate, error in (
            (lambda r: r["canonical_plan"].update(revision="other"), "plan_sync_required"),
            (lambda r: r["root"].update(identifier="GEN-38"), "root_token_mismatch"),
            (lambda r: r["remote_observation"].update(remote_id="stale"), "remote_ack_observation_mismatch"),
        ):
            record = self.record(checkpoint)
            mutate(record)
            with self.assertRaisesRegex(RecoveryCanaryError, error):
                evaluate(record, [checkpoint])

    def test_same_machine_or_session_is_not_physical_recovery(self):
        checkpoint = self.checkpoint()
        record = self.record(checkpoint)
        record["recovery"]["machine"] = "M1"
        with self.assertRaisesRegex(RecoveryCanaryError, "recovery_machine_not_distinct"):
            evaluate(record, [checkpoint])
        record = self.record(checkpoint)
        record["recovery"]["session_id"] = "source-session"
        with self.assertRaisesRegex(RecoveryCanaryError, "recovery_session_not_distinct"):
            evaluate(record, [checkpoint])


if __name__ == "__main__":
    unittest.main()
