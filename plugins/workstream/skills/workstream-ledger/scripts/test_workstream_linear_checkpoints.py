#!/usr/bin/env python3
from __future__ import annotations

import unittest
from unittest import mock

from workstream_checkpoint import acknowledge_checkpoint, build_checkpoint
from workstream_delta import Delta
from workstream_linear import LinearTransportError
from workstream_linear_checkpoints import (
    CHECKPOINT_PREFIX,
    LinearCheckpointAdapter,
    LinearCheckpointError,
    encode_checkpoint_comment,
    reduce_checkpoint_comments,
)
from workstream_linear_events import encode_event_comment


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


def material_comment(revision: int) -> dict:
    event = Delta(
        f"event-{revision}", "GEN-37", "material_boundary", "system",
        {"revision": revision}, revision - 1,
        f"2026-08-20T00:00:{revision:02d}Z",
    )
    return {
        "id": f"material-{revision}", "body": encode_event_comment(event),
        "createdAt": "now", "updatedAt": "now",
    }


class FakeCommentClient:
    def __init__(self):
        self.comments: list[dict] = []
        self.calls: list[tuple[str, dict]] = []
        self.identifier = "GEN-37"
        self.hide_created_comment = False
        self.pages: list[list[dict]] | None = None

    def seed_material(self, revision: int) -> None:
        self.comments.extend(material_comment(index) for index in range(1, revision + 1))

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
                        "team": {"id": "team", "organization": {"id": "workspace"}},
                        "project": {"id": "project"},
                        "comments": {
                            "nodes": nodes,
                            "pageInfo": {
                                "hasNextPage": has_next,
                                "endCursor": str(index + 1) if has_next else None,
                            },
                        },
                    }
                }
            nodes = [
                dict(c) for c in self.comments
                if not self.hide_created_comment or c["id"].startswith("material-")
            ]
            return {
                "issue": {
                    "id": "issue-37",
                    "identifier": self.identifier,
                    "team": {"id": "team", "organization": {"id": "workspace"}},
                    "project": {"id": "project"},
                    "comments": {
                        "nodes": nodes,
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                }
            }
        if "WorkstreamEventCommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}, {"name": "body"}]}}
        if "commentCreate" in query:
            comment_id = variables["input"]["id"]
            if any(item["id"] == comment_id for item in self.comments):
                raise LinearTransportError("duplicate comment id")
            comment = {
                "id": comment_id,
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
        client.seed_material(1)
        pending = checkpoint(1)

        acknowledged = self.adapter(client).persist(pending)

        self.assertEqual(pending["acknowledgement"]["state"], "pending")
        self.assertEqual(acknowledged["event_id"], pending["event_id"])
        self.assertEqual(
            acknowledged["acknowledgement"],
            {
                "state": "remote_acknowledged",
                "remote_id": client.comments[-1]["id"],
                "applied_revision": 1,
            },
        )
        self.assertNotIn("remote_acknowledged", client.comments[0]["body"])

    def test_crash_replay_returns_existing_ack_without_second_comment(self):
        client = FakeCommentClient()
        client.seed_material(1)
        adapter = self.adapter(client)
        pending = checkpoint(1)

        first = adapter.persist(pending)
        replay = adapter.persist(pending)

        self.assertEqual(first, replay)
        self.assertEqual(len(client.comments), 2)

    def test_lost_create_response_reloads_exact_slot_as_replay(self):
        class LostResponseClient(FakeCommentClient):
            lost = False

            def execute(self, query, variables):
                if "commentCreate" in query and not self.lost:
                    self.lost = True
                    super().execute(query, variables)
                    raise LinearTransportError("response lost")
                return super().execute(query, variables)

        client = LostResponseClient()
        client.seed_material(1)
        result = self.adapter(client).persist(checkpoint(1))
        self.assertEqual(result["acknowledgement"]["remote_id"], client.comments[-1]["id"])
        self.assertEqual(len(client.comments), 2)

    def test_foreign_successor_winner_refuses_without_a_fork(self):
        client = FakeCommentClient()
        client.seed_material(1)
        adapter = self.adapter(client)
        first = adapter.persist(checkpoint(1))
        client.comments.append(material_comment(2))
        intended = checkpoint(2, first["event_id"])
        foreign = build_checkpoint(
            workstream_id="GEN-37", boundary_id="foreign", root_revision=2,
            plan_revision="plan-sha", before_status="In Progress",
            after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "foreign",
                "machine": "other", "worktree": {
                    "state": "safe", "path": "/other", "branch": "foreign",
                    "head": "foreign-head",
                },
            },
            exact_head="foreign-head", evidence=[], blocker=None,
            next_action="foreign", predecessor_event_id=first["event_id"],
        )
        original_execute = client.execute
        injected = False

        def race(query, variables):
            nonlocal injected
            if "commentCreate" in query and not injected:
                injected = True
                client.comments.append({
                    "id": variables["input"]["id"],
                    "body": encode_checkpoint_comment(foreign),
                    "createdAt": "now", "updatedAt": "now",
                })
                raise LinearTransportError("duplicate comment id")
            return original_execute(query, variables)

        client.execute = race
        with self.assertRaisesRegex(
            LinearCheckpointError, "checkpoint_slot_lost_reload_required"
        ):
            adapter.persist(intended)
        checkpoints = reduce_checkpoint_comments(
            client.comments, workstream_id="GEN-37"
        ).checkpoints
        self.assertEqual(len(checkpoints), 2)

    def test_stale_root_revision_and_predecessor_refuse_before_write(self):
        client = FakeCommentClient()
        client.seed_material(2)
        adapter = self.adapter(client)
        first = adapter.persist(checkpoint(2))
        writes_before = len([
            query for query, _ in client.calls if "commentCreate" in query
        ])
        with self.assertRaisesRegex(
            LinearCheckpointError, "checkpoint_predecessor_stale_reload_required"
        ):
            adapter.persist(checkpoint(2, "wsc_wrong"))
        with self.assertRaisesRegex(
            LinearCheckpointError, "checkpoint_root_revision_stale_reload_required"
        ):
            adapter.persist(checkpoint(3, first["event_id"]))
        writes_after = len([
            query for query, _ in client.calls if "commentCreate" in query
        ])
        self.assertEqual(writes_before, writes_after)

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
        client.seed_material(1)
        client.hide_created_comment = True
        with self.assertRaisesRegex(
            LinearCheckpointError, "checkpoint_append_not_observed"
        ):
            self.adapter(client).persist(checkpoint(1))

    def test_ack_conflict_and_plan_drift_fail_closed(self):
        client = FakeCommentClient()
        client.seed_material(1)
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

    def test_configured_route_fences_checkpoint_writes(self):
        client = FakeCommentClient()
        adapter = LinearCheckpointAdapter(
            client, issue_id="issue-37", workstream_id="GEN-37",
            workspace_id="workspace", team_id="team", project_id="wrong",
        )
        with self.assertRaisesRegex(LinearCheckpointError, "configured project"):
            adapter.persist(checkpoint(1))
        self.assertFalse(any("commentCreate" in query for query, _ in client.calls))

    def test_from_env_consumes_config_route(self):
        route = {"workspace_id": "workspace", "team_id": "team", "project_id": "project"}
        client = mock.Mock()
        with mock.patch("workstream_config.resolve_linear_route", return_value=(route, None)), \
             mock.patch("workstream_linear_checkpoints.HttpGraphQLClient", return_value=client):
            adapter = LinearCheckpointAdapter.from_env(
                issue_id="issue-37", workstream_id="GEN-37",
                env={"LINEAR_API_KEY": "secret"},
            )
        self.assertIs(adapter.client, client)
        self.assertEqual(adapter.workspace_id, "workspace")
        self.assertEqual(adapter.project_id, "project")


if __name__ == "__main__":
    unittest.main()
