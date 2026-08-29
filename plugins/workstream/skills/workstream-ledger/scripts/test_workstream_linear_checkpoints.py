#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
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
from workstream_linear_events import (
    LinearCommentEventAdapter,
    encode_ledger_reservation,
    encode_event_comment,
    ledger_boundary_slot_id,
)


def checkpoint(
    revision: int, predecessor: str | None = None, *, plan: str = "plan-sha",
) -> dict:
    return build_checkpoint(
        workstream_id="GEN-37",
        boundary_id=f"boundary-{revision}",
        root_revision=revision,
        plan_revision=plan,
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

    def test_checkpoint_refuses_while_shared_identity_reservation_is_pending(self):
        client = FakeCommentClient()
        authority = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "issue-37",
        }
        from workstream_linear_projection import build_projection_event
        intent = build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root",
            value={"test": "intent"}, plan_revision="a" * 64,
            expected_revision=0, created_at="2026-08-29T12:00:00Z",
            authority=authority,
        )
        reservation = {
            "schema_version": 1, "workstream_id": "GEN-37",
            "material_revision": 0, "plan_revision": "a" * 64,
            "projection_revision": 0, "projection_frontier_ids": [],
            "frontier_ids": [], "authority": authority,
            "intent_kind": "repository_identity_projection",
            "intent_event": intent,
            "intent_sha256": hashlib.sha256(json.dumps(
                intent, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode()).hexdigest(),
        }
        client.comments.append({
            "id": ledger_boundary_slot_id("GEN-37", 0, [], authority),
            "body": encode_ledger_reservation(reservation),
            "createdAt": "now", "updatedAt": "now",
        })
        client.comments.extend({
            "id": f"malformed-after-{index}",
            "body": "<!-- workstream-ledger-reservation:v1:not-valid -->",
            "createdAt": f"now-{index}", "updatedAt": f"now-{index}",
        } for index in range(4))
        with self.assertRaisesRegex(LinearTransportError, "ledger_boundary_reserved"):
            self.adapter(client).persist(checkpoint(0, plan="a" * 64))
        self.assertFalse(any("commentCreate" in query for query, _ in client.calls))

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

    def test_event_winner_forces_checkpoint_rebuild_at_new_revision(self):
        client = FakeCommentClient()
        client.seed_material(1)
        adapter = self.adapter(client)
        pending = checkpoint(1)
        original_execute = client.execute
        injected = False

        def event_wins(query, variables):
            nonlocal injected
            if "commentCreate" in query and not injected:
                injected = True
                event = Delta(
                    "event-2", "GEN-37", "material_boundary", "system",
                    {"revision": 2}, 1, "2026-08-20T00:00:02Z",
                )
                client.comments.append({
                    "id": variables["input"]["id"],
                    "body": encode_event_comment(event),
                    "createdAt": "now", "updatedAt": "now",
                })
                raise LinearTransportError("duplicate comment id")
            return original_execute(query, variables)

        client.execute = event_wins
        with self.assertRaisesRegex(
            LinearCheckpointError,
            "checkpoint_material_revision_advanced_reload_and_rebuild_required",
        ):
            adapter.persist(pending)
        self.assertFalse(any(
            "workstream-checkpoint:v1" in comment["body"]
            for comment in client.comments
        ))

        rebuilt = adapter.persist(checkpoint(2))
        self.assertEqual(rebuilt["root_revision"], 2)
        self.assertEqual(len(client.comments), 3)

    def test_legacy_arbitrary_checkpoint_ids_remain_compatible(self):
        client = FakeCommentClient()
        client.seed_material(1)
        first = checkpoint(1)
        client.comments.append({
            "id": "legacy-arbitrary-checkpoint-id",
            "body": encode_checkpoint_comment(first),
            "createdAt": "then", "updatedAt": "then",
        })
        adapter = self.adapter(client)

        replay = adapter.persist(first)
        client.comments.append(material_comment(2))
        second = adapter.persist(checkpoint(2, first["event_id"]))

        self.assertEqual(
            replay["acknowledgement"]["remote_id"],
            "legacy-arbitrary-checkpoint-id",
        )
        self.assertEqual(second["root_revision"], 2)
        self.assertEqual(len(client.comments), 4)

    def test_multi_generation_persist_recover_replay_uses_current_chain(self):
        client = FakeCommentClient()
        client.seed_material(1)
        adapter = self.adapter(client)
        old = adapter.persist(checkpoint(1, plan="old-plan"))
        client.comments.append(material_comment(2))
        current = adapter.persist(checkpoint(2, plan="current-plan"))
        client.comments.append(material_comment(3))
        successor = checkpoint(
            3, current["event_id"], plan="current-plan",
        )

        result = adapter.persist(successor)
        replay = adapter.persist(successor)
        recovered = adapter.recover(expected_plan_revision="current-plan")

        self.assertEqual(result, replay)
        self.assertEqual(recovered["checkpoint_event_id"], result["event_id"])
        self.assertEqual(
            [item["session_id"] for item in recovered["provenance_chain"]],
            ["session-2", "session-3"],
        )
        self.assertEqual(len(client.comments), 6)
        expected_slot = ledger_boundary_slot_id(
            "GEN-37", 3, sorted([old["event_id"], current["event_id"]]),
            {
                "workspace_id": "workspace", "team_id": "team",
                "project_id": "project", "root_issue_id": "issue-37",
            },
        )
        self.assertEqual(result["acknowledgement"]["remote_id"], expected_slot)

    def test_arbitrary_remote_ids_preserve_multi_generation_current_chain(self):
        client = FakeCommentClient()
        client.seed_material(3)
        old = checkpoint(1, plan="old-plan")
        current = checkpoint(2, plan="current-plan")
        successor = checkpoint(
            3, current["event_id"], plan="current-plan",
        )
        client.comments.extend([
            {
                "id": "arbitrary-old-id",
                "body": encode_checkpoint_comment(old),
                "createdAt": "then", "updatedAt": "then",
            },
            {
                "id": "arbitrary-current-id",
                "body": encode_checkpoint_comment(current),
                "createdAt": "then", "updatedAt": "then",
            },
        ])
        adapter = self.adapter(client)

        replay = adapter.persist(current)
        persisted = adapter.persist(successor)
        recovered = adapter.recover(expected_plan_revision="current-plan")

        self.assertEqual(
            replay["acknowledgement"]["remote_id"], "arbitrary-current-id",
        )
        self.assertEqual(recovered["checkpoint_event_id"], persisted["event_id"])
        self.assertEqual(
            [item["session_id"] for item in recovered["provenance_chain"]],
            ["session-2", "session-3"],
        )

    def test_public_recover_distinguishes_empty_from_stale_plan(self):
        empty = self.adapter(FakeCommentClient())
        with self.assertRaisesRegex(Exception, "checkpoint_not_found"):
            empty.recover(expected_plan_revision="current-plan")

        client = FakeCommentClient()
        client.seed_material(1)
        self.adapter(client).persist(checkpoint(1, plan="old-plan"))
        with self.assertRaisesRegex(Exception, "plan_sync_required"):
            self.adapter(client).recover(expected_plan_revision="current-plan")

    def test_broken_stale_generation_refuses_recover_persist_and_event(self):
        client = FakeCommentClient()
        client.seed_material(2)
        broken = checkpoint(1, "wsc_missing", plan="old-plan")
        current = checkpoint(2, plan="current-plan")
        client.comments.extend([
            {
                "id": "arbitrary-old-generation-id",
                "body": encode_checkpoint_comment(broken),
                "createdAt": "then", "updatedAt": "then",
            },
            {
                "id": "arbitrary-current-generation-id",
                "body": encode_checkpoint_comment(current),
                "createdAt": "then", "updatedAt": "then",
            },
        ])
        adapter = self.adapter(client)
        writes_before = len([
            query for query, _ in client.calls if "commentCreate" in query
        ])

        with self.assertRaisesRegex(Exception, "checkpoint_chain_truncated"):
            adapter.recover(expected_plan_revision="current-plan")
        with self.assertRaisesRegex(Exception, "checkpoint_chain_truncated"):
            adapter.persist(current)
        with self.assertRaisesRegex(Exception, "checkpoint_chain_truncated"):
            LinearCommentEventAdapter(client, issue_id="GEN-37").apply(
                Delta(
                    "event-3", "GEN-37", "material_boundary", "system",
                    {"revision": 3}, 2, "2026-08-20T00:00:03Z",
                )
            )
        writes_after = len([
            query for query, _ in client.calls if "commentCreate" in query
        ])
        self.assertEqual(writes_before, writes_after)

    def test_cross_type_cas_uses_full_multi_generation_frontier(self):
        client = FakeCommentClient()
        client.seed_material(1)
        adapter = self.adapter(client)
        old = adapter.persist(checkpoint(1, plan="old-plan"))
        client.comments.append(material_comment(2))
        current = adapter.persist(checkpoint(2, plan="current-plan"))

        receipt = LinearCommentEventAdapter(client, issue_id="GEN-37").apply(
            Delta(
                "event-3", "GEN-37", "material_boundary", "system",
                {"revision": 3}, 2, "2026-08-20T00:00:03Z",
            )
        )

        expected_slot = ledger_boundary_slot_id(
            "GEN-37", 2, sorted([old["event_id"], current["event_id"]]),
            {
                "workspace_id": "workspace", "team_id": "team",
                "project_id": "project", "root_issue_id": "issue-37",
            },
        )
        self.assertEqual(receipt.remote_id, expected_slot)

    def test_legacy_checkpoint_ahead_requires_quarantine_remediation(self):
        client = FakeCommentClient()
        client.seed_material(1)
        ahead = checkpoint(2)
        client.comments.append({
            "id": "legacy-checkpoint-ahead",
            "body": encode_checkpoint_comment(ahead),
            "createdAt": "then", "updatedAt": "then",
        })
        adapter = self.adapter(client)

        # Exact replay remains safe and does not mutate the malformed history.
        replay = adapter.persist(ahead)
        self.assertEqual(
            replay["acknowledgement"]["remote_id"], "legacy-checkpoint-ahead"
        )
        writes_before = len([
            query for query, _ in client.calls if "commentCreate" in query
        ])
        with self.assertRaisesRegex(
            LinearCheckpointError, "checkpoint_material_history_incomplete"
        ):
            adapter.persist(checkpoint(3, ahead["event_id"]))
        with self.assertRaisesRegex(
            Exception, "checkpoint_material_history_incomplete"
        ):
            LinearCommentEventAdapter(client, issue_id="GEN-37").apply(
                Delta(
                    "event-2", "GEN-37", "material_boundary", "system",
                    {"revision": 2}, 1, "2026-08-20T00:00:02Z",
                )
            )
        writes_after = len([
            query for query, _ in client.calls if "commentCreate" in query
        ])
        self.assertEqual(writes_before, writes_after)

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
            LinearCheckpointError, "checkpoint_material_history_incomplete"
        ):
            adapter.persist(checkpoint(3, first["event_id"]))
        writes_after = len([
            query for query, _ in client.calls if "commentCreate" in query
        ])
        self.assertEqual(writes_before, writes_after)

    def test_successor_revision_must_advance_predecessor(self):
        client = FakeCommentClient()
        client.seed_material(2)
        adapter = self.adapter(client)
        first = adapter.persist(checkpoint(2))
        with self.assertRaisesRegex(
            LinearCheckpointError, "checkpoint_successor_revision_not_monotonic"
        ):
            adapter.persist(checkpoint(2, first["event_id"]))

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
