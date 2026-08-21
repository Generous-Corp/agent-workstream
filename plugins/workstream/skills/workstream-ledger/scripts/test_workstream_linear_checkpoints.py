#!/usr/bin/env python3
from __future__ import annotations

import unittest

from workstream_checkpoint import acknowledge_checkpoint, build_checkpoint
from workstream_linear_checkpoints import (
    CHECKPOINT_PREFIX,
    LinearCheckpointAdapter,
    LinearCheckpointError,
    encode_checkpoint_comment,
    reduce_checkpoint_comments,
)


def checkpoint(revision: int, predecessor: str | None = None) -> dict:
    return build_checkpoint(
        workstream_id="GEN-37",
        boundary_id=f"boundary-{revision}",
        root_revision=revision,
        plan_revision="plan-sha",
        before_status="In Progress",
        after_status="In Progress",
        execution={
            "agent": "codex",
            "provider": "openai",
            "session_id": f"session-{revision}",
            "machine": "test-machine",
            "worktree": {
                "state": "safe",
                "path": "/repo",
                "branch": "feature/demo",
                "head": f"head-{revision}",
            },
        },
        exact_head=f"head-{revision}",
        evidence=[{"kind": "test", "id": f"test-{revision}"}],
        blocker=None,
        next_action=f"step-{revision}",
        predecessor_event_id=predecessor,
    )


class FakeCommentClient:
    def __init__(self):
        self.comments: list[dict] = []
        self.calls: list[tuple[str, dict]] = []
        self.identifier = "GEN-37"
        self.hide_created_comment = False
        self.pages: list[list[dict]] | None = None

    def execute(self, query, variables):
        self.calls.append((query, variables))
        if "query WorkstreamDeltaComments" in query:
            if self.pages is not None:
                index = 0 if variables["after"] is None else int(variables["after"])
                nodes = [dict(item) for item in self.pages[index]]
                has_next = index + 1 < len(self.pages)
                return {
                    "issue": {
                        "id": "issue-37",
                        "identifier": self.identifier,
                        "comments": {
                            "nodes": nodes,
                            "pageInfo": {
                                "hasNextPage": has_next,
                                "endCursor": str(index + 1) if has_next else None,
                            },
                        },
                    }
                }
            nodes = [] if self.hide_created_comment else [dict(c) for c in self.comments]
            return {
                "issue": {
                    "id": "issue-37",
                    "identifier": self.identifier,
                    "comments": {
                        "nodes": nodes,
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                }
            }
        if "commentCreate" in query:
            comment = {
                "id": f"comment-{len(self.comments) + 1}",
                "body": variables["input"]["body"],
                "createdAt": "now",
                "updatedAt": "now",
            }
            self.comments.append(comment)
            return {"commentCreate": {"success": True, "comment": dict(comment)}}
        raise AssertionError("unexpected GraphQL operation")


class LinearCheckpointAdapterTests(unittest.TestCase):
    def adapter(self, client=None):
        return LinearCheckpointAdapter(
            client or FakeCommentClient(), issue_id="issue-37", workstream_id="GEN-37"
        )

    def test_persist_derives_ack_only_after_remote_readback(self):
        client = FakeCommentClient()
        pending = checkpoint(1)

        acknowledged = self.adapter(client).persist(pending)

        self.assertEqual(pending["acknowledgement"]["state"], "pending")
        self.assertEqual(acknowledged["event_id"], pending["event_id"])
        self.assertEqual(
            acknowledged["acknowledgement"],
            {
                "state": "remote_acknowledged",
                "remote_id": "comment-1",
                "applied_revision": 1,
            },
        )
        self.assertNotIn("remote_acknowledged", client.comments[0]["body"])

    def test_crash_replay_returns_existing_ack_without_second_comment(self):
        client = FakeCommentClient()
        adapter = self.adapter(client)
        pending = checkpoint(1)

        first = adapter.persist(pending)
        replay = adapter.persist(pending)

        self.assertEqual(first, replay)
        self.assertEqual(len(client.comments), 1)

    def test_complete_paginated_chain_recovers_latest_boundary(self):
        first = checkpoint(1)
        second = checkpoint(2, first["event_id"])
        comments = [
            {"id": "comment-1", "body": encode_checkpoint_comment(first)},
            {"id": "comment-2", "body": encode_checkpoint_comment(second)},
        ]
        client = FakeCommentClient()
        client.pages = [[comments[0]], [comments[1]]]

        recovered = self.adapter(client).recover(expected_plan_revision="plan-sha")

        self.assertEqual(recovered["checkpoint_event_id"], second["event_id"])
        self.assertEqual(recovered["next_action"], "step-2")
        self.assertEqual(
            [item["session_id"] for item in recovered["provenance_chain"]],
            ["session-1", "session-2"],
        )
        comment_queries = [q for q, _ in client.calls if "Comments" in q]
        self.assertEqual(len(comment_queries), 2)

    def test_duplicate_marker_and_wrong_issue_identity_fail_closed(self):
        record = checkpoint(1)
        marker = encode_checkpoint_comment(record)
        with self.assertRaisesRegex(
            LinearCheckpointError, "duplicate_checkpoint_event_id"
        ):
            reduce_checkpoint_comments(
                [
                    {"id": "comment-1", "body": marker},
                    {"id": "comment-2", "body": marker},
                ],
                workstream_id="GEN-37",
            )

        client = FakeCommentClient()
        client.identifier = "GEN-38"
        with self.assertRaisesRegex(LinearCheckpointError, "workstream_id_mismatch"):
            self.adapter(client).persist(record)

    def test_malformed_or_unobserved_write_fails_closed(self):
        with self.assertRaisesRegex(LinearCheckpointError, "malformed_checkpoint_marker"):
            reduce_checkpoint_comments(
                [{"id": "bad", "body": f"{CHECKPOINT_PREFIX}not-base64 -->"}],
                workstream_id="GEN-37",
            )

        client = FakeCommentClient()
        client.hide_created_comment = True
        with self.assertRaisesRegex(
            LinearCheckpointError, "checkpoint_append_not_observed"
        ):
            self.adapter(client).persist(checkpoint(1))

    def test_ack_conflict_and_plan_drift_fail_closed(self):
        client = FakeCommentClient()
        adapter = self.adapter(client)
        pending = checkpoint(1)
        adapter.persist(pending)
        wrong_ack = acknowledge_checkpoint(pending, "other-comment", 1)
        with self.assertRaisesRegex(
            LinearCheckpointError, "checkpoint_acknowledgement_conflict"
        ):
            adapter.persist(wrong_ack)
        with self.assertRaisesRegex(Exception, "plan_sync_required"):
            adapter.recover(expected_plan_revision="other-plan")

    def test_auth_is_required_before_network_client_exists(self):
        with self.assertRaisesRegex(LinearCheckpointError, "linear_auth_unavailable"):
            LinearCheckpointAdapter.from_env(
                issue_id="issue-37", workstream_id="GEN-37", env={}
            )


if __name__ == "__main__":
    unittest.main()
