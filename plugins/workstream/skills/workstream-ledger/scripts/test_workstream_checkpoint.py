#!/usr/bin/env python3

import unittest

from workstream_checkpoint import (
    CheckpointError,
    acknowledge_checkpoint,
    build_checkpoint,
    recover_latest,
)


class CheckpointTests(unittest.TestCase):
    def checkpoint(self, *, session, revision, predecessor=None, machine="M5", state="safe", workstream="GEN-37", plan="plan-sha"):
        return build_checkpoint(
            workstream_id=workstream,
            boundary_id=f"boundary-{revision}",
            root_revision=revision,
            plan_revision=plan,
            before_status="In Progress",
            after_status="Blocked" if revision == 2 else "In Progress",
            execution={
                "agent": "codex" if revision != 2 else "claude",
                "provider": "openai" if revision != 2 else "anthropic",
                "session_id": session,
                "machine": machine,
                "worktree": {"state": state, "path": "/repo", "branch": "feature/demo", "head": f"head-{revision}"},
            },
            exact_head=f"head-{revision}",
            evidence=[{"kind": "test", "id": f"test-{revision}"}],
            blocker={"text": "waiting on review", "owner": "daniel"} if revision == 2 else None,
            next_action=f"step-{revision}",
            predecessor_event_id=predecessor,
        )

    def test_checkpoint_captures_pre_final_response_boundary_and_ack(self):
        checkpoint = self.checkpoint(session="session-a", revision=1)
        self.assertEqual(checkpoint["acknowledgement"]["state"], "pending")
        self.assertEqual(checkpoint["status"], {"before": "In Progress", "after": "In Progress"})
        acknowledged = acknowledge_checkpoint(checkpoint, remote_id="comment-1", applied_revision=1)
        self.assertEqual(acknowledged["event_id"], checkpoint["event_id"])
        self.assertEqual(acknowledged["acknowledgement"]["state"], "remote_acknowledged")
        self.assertEqual(acknowledged["acknowledgement"]["remote_id"], "comment-1")

    def test_synthetic_three_record_chain_deduplicates_and_recovers_tip(self):
        first = acknowledge_checkpoint(self.checkpoint(session="session-a", revision=1), "r1", 1)
        second = acknowledge_checkpoint(
            self.checkpoint(session="session-b", revision=2, predecessor=first["event_id"]),
            "r2",
            2,
        )
        third = acknowledge_checkpoint(
            self.checkpoint(
                session="session-c",
                revision=3,
                predecessor=second["event_id"],
                machine="M3",
                state="unavailable",
            ),
            "r3",
            3,
        )

        recovered = recover_latest([first, second, second, third], "GEN-37", expected_plan_revision="plan-sha")

        self.assertEqual(recovered["checkpoint_event_id"], third["event_id"])
        self.assertEqual(recovered["root_revision"], 3)
        self.assertEqual(recovered["next_action"], "step-3")
        self.assertEqual(recovered["worktree"]["state"], "unavailable")
        self.assertEqual(
            [(item["agent"], item["session_id"], item["machine"]) for item in recovered["provenance_chain"]],
            [("codex", "session-a", "M5"), ("claude", "session-b", "M5"), ("codex", "session-c", "M3")],
        )

    def test_recovery_rejects_a_truncated_predecessor_chain(self):
        first = acknowledge_checkpoint(self.checkpoint(session="session-a", revision=1), "r1", 1)
        second = acknowledge_checkpoint(
            self.checkpoint(session="session-b", revision=2, predecessor=first["event_id"]),
            "r2",
            2,
        )
        with self.assertRaisesRegex(CheckpointError, "checkpoint_chain_truncated"):
            recover_latest([second], "GEN-37", expected_plan_revision="plan-sha")

    def test_checkpoint_accepts_non_gen_team_token(self):
        checkpoint = self.checkpoint(session="session-a", revision=1, workstream="OPS-37")
        self.assertEqual(checkpoint["workstream_id"], "OPS-37")

    def test_checkpoint_rejects_invalid_token_and_incomplete_safe_worktree(self):
        arguments = {
            "boundary_id": "b",
            "root_revision": 1,
            "plan_revision": "plan-sha",
            "before_status": "Todo",
            "after_status": "Todo",
            "exact_head": "head-1",
            "evidence": [],
            "blocker": None,
            "next_action": "continue",
        }
        with self.assertRaisesRegex(CheckpointError, "invalid_workstream_id"):
            build_checkpoint(
                workstream_id="not-an-issue",
                execution={
                    "agent": "codex", "provider": "openai", "session_id": "s",
                    "machine": "M5", "worktree": {"state": "safe"},
                },
                **arguments,
            )
        with self.assertRaisesRegex(CheckpointError, "execution.worktree.path"):
            build_checkpoint(
                workstream_id="GEN-37",
                execution={
                    "agent": "codex", "provider": "openai", "session_id": "s",
                    "machine": "M5", "worktree": {"state": "safe"},
                },
                **arguments,
            )

    def test_recovery_rejects_plan_drift_and_unacknowledged_latest_boundary(self):
        checkpoint = self.checkpoint(session="session-a", revision=1)
        with self.assertRaisesRegex(CheckpointError, "checkpoint_not_remote_acknowledged"):
            recover_latest([checkpoint], "GEN-37", expected_plan_revision="plan-sha")
        acknowledged = acknowledge_checkpoint(checkpoint, "r1", 1)
        with self.assertRaisesRegex(CheckpointError, "plan_sync_required"):
            recover_latest([acknowledged], "GEN-37", expected_plan_revision="other")

    def test_recovery_validates_all_generations_and_selects_exact_plan(self):
        old = acknowledge_checkpoint(
            self.checkpoint(session="old", revision=1, plan="old-plan"), "old-id", 1,
        )
        current = acknowledge_checkpoint(
            self.checkpoint(session="current", revision=2, plan="current-plan"),
            "current-id", 2,
        )
        successor = acknowledge_checkpoint(
            self.checkpoint(
                session="current-2", revision=3, plan="current-plan",
                predecessor=current["event_id"],
            ),
            "current-id-2", 3,
        )

        recovered = recover_latest(
            [successor, old, current], "GEN-37",
            expected_plan_revision="current-plan",
        )

        self.assertEqual(recovered["checkpoint_event_id"], successor["event_id"])
        self.assertEqual(
            [item["session_id"] for item in recovered["provenance_chain"]],
            ["current", "current-2"],
        )

    def test_broken_stale_generation_refuses_current_recovery(self):
        stale = acknowledge_checkpoint(
            self.checkpoint(
                session="stale", revision=1, plan="old-plan",
                predecessor="wsc_missing",
            ),
            "stale-id", 1,
        )
        current = acknowledge_checkpoint(
            self.checkpoint(session="current", revision=2, plan="current-plan"),
            "current-id", 2,
        )
        with self.assertRaisesRegex(CheckpointError, "checkpoint_chain_truncated"):
            recover_latest(
                [stale, current], "GEN-37",
                expected_plan_revision="current-plan",
            )

    def test_same_event_id_with_different_bytes_is_corruption(self):
        checkpoint = acknowledge_checkpoint(self.checkpoint(session="session-a", revision=1), "r1", 1)
        changed = {**checkpoint, "next_action": "silently changed"}
        with self.assertRaisesRegex(CheckpointError, "checkpoint_event_collision"):
            recover_latest([checkpoint, changed], "GEN-37", expected_plan_revision="plan-sha")


if __name__ == "__main__":
    unittest.main()
