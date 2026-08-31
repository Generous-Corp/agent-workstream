#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from test_workstream_child_state import (
    CHILD_ID, PLAN, ROOT_ID, ROUTE, FakeChildStateClient,
)
from workstream_checkpoint import build_checkpoint
from workstream_delta import Delta
import workstream_child_event
import workstream_child_checkpoint
import workstream_child_origin_repair
from workstream_linear_events import encode_event_comment
from workstream_linear_checkpoints import encode_checkpoint_comment
from workstream_resume import add_child_material_history, compact_context


REVIEW_IDENTITY = (
    "https://github.com/example/private-planning/blob/"
    + "c" * 40 + "/gen43-origin-review.json"
)


class LegacyChildOriginRepairTests(unittest.TestCase):
    def client(self) -> FakeChildStateClient:
        client = FakeChildStateClient()
        client.root_comments = client.root_comments[:2]
        for index in range(48):
            event = Delta(
                event_id=f"legacy-child-{index}", workstream_id="GEN-38",
                kind="progress", source="agent_discovery",
                payload={"index": index}, expected_revision=index,
                created_at=f"2026-08-30T00:00:{index:02d}Z",
            )
            client.child_comments.append({
                "id": f"child-event-{index}",
                "body": encode_event_comment(event),
                "createdAt": event.created_at, "updatedAt": event.created_at,
            })
        predecessor = None
        for index in range(7):
            checkpoint = build_checkpoint(
                workstream_id="GEN-38", boundary_id=f"legacy-checkpoint-{index}",
                root_revision=index + 1, plan_revision=PLAN,
                before_status="In Progress", after_status="In Progress",
                execution={
                    "agent": "codex", "provider": "openai",
                    "session_id": f"legacy-session-{index}", "machine": "M3",
                    "worktree": {
                        "state": "safe", "path": "/repo/legacy",
                        "branch": "legacy", "head": "b" * 40,
                    },
                }, exact_head="b" * 40, evidence=[], blocker=None,
                next_action="continue", predecessor_event_id=predecessor,
            )
            predecessor = checkpoint["event_id"]
            client.child_comments.append({
                "id": f"child-checkpoint-{index}",
                "body": encode_checkpoint_comment(checkpoint),
                "createdAt": f"2026-08-30T00:01:{index:02d}Z",
                "updatedAt": f"2026-08-30T00:01:{index:02d}Z",
            })
        return client

    def common(self) -> list[str]:
        return [
            "GEN-37", "--root-issue-id", ROOT_ID,
            "--child-workstream-id", "GEN-38", "--child-issue-id", CHILD_ID,
            "--plan-revision", PLAN, "--workspace-id", "workspace",
            "--team-id", "team", "--project-id", "project",
            "--created-at", "2026-08-30T01:00:00Z",
            "--custodian", "codex-gen37-root",
            "--writers-retired-at", "2026-08-30T00:59:00Z",
        ]

    def patches(self):
        return (
            mock.patch(
                "workstream_child_origin_repair.resolve_linear_route",
                return_value=(ROUTE, None),
            ),
            mock.patch(
                "workstream_child_origin_repair.load_linear_api_key",
                return_value="secret",
            ),
        )

    def seal(self, client: FakeChildStateClient):
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            preview = workstream_child_origin_repair.run(
                self.common(), client_factory=lambda _token: client,
            )
        material = json.dumps(
            preview, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        with tempfile.TemporaryDirectory() as directory:
            review = Path(directory) / "review.json"
            review.write_bytes(material)
            route_patch, auth_patch = self.patches()
            with route_patch, auth_patch:
                result = workstream_child_origin_repair.run(
                    [*self.common(), "--review", str(review),
                     "--review-identity", REVIEW_IDENTITY, "--apply"],
                    client_factory=lambda _token: client,
                    source_loader=lambda _identity, _expected: (
                        material, REVIEW_IDENTITY,
                    ),
                )
        return preview, result

    def test_reviewed_repair_preserves_legacy_history_and_unlocks_child_event(self):
        client = self.client()
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            preview = workstream_child_origin_repair.run(
                self.common(), client_factory=lambda _token: client,
            )
        value = preview["value"]
        self.assertEqual(
            value["child_history"]["material_frontier"]["revision"], 48,
        )
        self.assertEqual(len(value["child_history"]["material_receipts"]), 48)
        self.assertEqual(len(value["child_history"]["checkpoint_receipts"]), 7)
        before_child = deepcopy(client.child_comments)
        with tempfile.TemporaryDirectory() as directory:
            review = Path(directory) / "review.json"
            review_material = json.dumps(
                preview, sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")
            review.write_bytes(review_material)
            route_patch, auth_patch = self.patches()
            with route_patch, auth_patch:
                result = workstream_child_origin_repair.run(
                    [*self.common(), "--review", str(review),
                     "--review-identity", REVIEW_IDENTITY, "--apply"],
                    client_factory=lambda _token: client,
                    source_loader=lambda _identity, _expected: (
                        review_material, REVIEW_IDENTITY,
                    ),
                )
                replay = workstream_child_origin_repair.run(
                    [*self.common(), "--review", str(review),
                     "--review-identity", REVIEW_IDENTITY, "--apply"],
                    client_factory=lambda _token: client,
                    source_loader=lambda _identity, _expected: (
                        review_material, REVIEW_IDENTITY,
                    ),
                )
        self.assertEqual(result["receipt"]["disposition"], "created")
        self.assertEqual(replay["receipt"]["disposition"], "existing")
        self.assertEqual(client.child_comments, before_child)
        self.assertEqual(len([
            call for call in client.calls
            if "commentCreate" in call[0]
            and call[1]["input"]["issueId"] == "GEN-37"
        ]), 1)

        event_args = [
            "GEN-37", "--root-issue-id", ROOT_ID,
            "--child-workstream-id", "GEN-38", "--child-issue-id", CHILD_ID,
            "--plan-revision", PLAN, "--workspace-id", "workspace",
            "--team-id", "team", "--project-id", "project", "--apply",
            "--kind", "progress", "--source", "agent_discovery",
            "--expected-revision", "48", "--created-at", "later",
            "--payload-json", '{"next_action":"continue"}',
        ]
        with mock.patch(
            "workstream_child_target.resolve_linear_route",
            return_value=(ROUTE, None),
        ), mock.patch(
            "workstream_child_target.load_linear_api_key", return_value="secret",
        ):
            child_result = workstream_child_event.run(
                event_args, client_factory=lambda _token: client,
            )
        self.assertEqual(
            child_result["authorization"]["event"]["value"]["child_origin"]["kind"],
            "existing_child_origin_seal",
        )
        checkpoint = build_checkpoint(
            workstream_id="GEN-38", boundary_id="post-repair-checkpoint",
            root_revision=49, plan_revision=PLAN,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "new",
                "machine": "M3", "worktree": {
                    "state": "safe", "path": "/repo/legacy", "branch": "legacy",
                    "head": "b" * 40,
                },
            }, exact_head="b" * 40, evidence=[], blocker=None,
            next_action="checkpointed",
            predecessor_event_id=value["child_history"]["checkpoint_receipts"][-1]["event_id"],
        )
        with tempfile.TemporaryDirectory() as directory:
            checkpoint_path = Path(directory) / "checkpoint.json"
            checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")
            with mock.patch(
                "workstream_child_target.resolve_linear_route",
                return_value=(ROUTE, None),
            ), mock.patch(
                "workstream_child_target.load_linear_api_key", return_value="secret",
            ):
                checkpoint_result = workstream_child_checkpoint.run([
                    "GEN-37", "--root-issue-id", ROOT_ID,
                    "--child-workstream-id", "GEN-38", "--child-issue-id", CHILD_ID,
                    "--plan-revision", PLAN, "--workspace-id", "workspace",
                    "--team-id", "team", "--project-id", "project", "--apply",
                    "--checkpoint", str(checkpoint_path),
                    "--material-revision", "49", "--predecessor-event-id",
                    value["child_history"]["checkpoint_receipts"][-1]["event_id"],
                ], client_factory=lambda _token: client)
        self.assertEqual(
            checkpoint_result["authorization"]["event"]["value"]["child_origin"]["kind"],
            "existing_child_origin_seal",
        )
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 0,
                "status": "In Progress", "status_type": "started",
                "next_action": "continue root",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "parent": {"id": ROOT_ID},
                "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        resumed = compact_context(add_child_material_history(
            snapshot, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=deepcopy(client.root_comments),
        ), "GEN-37")
        self.assertEqual(resumed["children"][0]["next_action"], "checkpointed")

    def test_stale_child_history_refuses_without_root_mutation(self):
        client = self.client()
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            preview = workstream_child_origin_repair.run(
                self.common(), client_factory=lambda _token: client,
            )
        late = Delta(
            event_id="late", workstream_id="GEN-38", kind="progress",
            source="agent_discovery", payload={}, expected_revision=48,
            created_at="later",
        )
        client.child_comments.append({
            "id": "late", "body": encode_event_comment(late),
            "createdAt": "later", "updatedAt": "later",
        })
        with tempfile.TemporaryDirectory() as directory:
            review = Path(directory) / "review.json"
            review_material = json.dumps(
                preview, sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")
            review.write_bytes(review_material)
            route_patch, auth_patch = self.patches()
            with route_patch, auth_patch, self.assertRaisesRegex(
                Exception, "child_origin_review_stale|child_history_changed",
            ):
                workstream_child_origin_repair.run(
                    [*self.common(), "--review", str(review),
                     "--review-identity", REVIEW_IDENTITY, "--apply"],
                    client_factory=lambda _token: client,
                    source_loader=lambda _identity, _expected: (
                        review_material, REVIEW_IDENTITY,
                    ),
                )
        self.assertEqual(len(client.root_comments), 2)

    def test_native_parent_drift_refuses_without_mutation(self):
        client = self.client()
        client.child_parent_id = "22222222-2222-4222-8222-222222222222"
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, self.assertRaisesRegex(
            Exception, "native_parent_mismatch",
        ):
            workstream_child_origin_repair.run(
                self.common(), client_factory=lambda _token: client,
            )
        self.assertEqual(len(client.root_comments), 2)

    def test_post_seal_direct_child_marker_is_never_absorbed(self):
        client = self.client()
        self.seal(client)
        late = Delta(
            event_id="late-direct", workstream_id="GEN-38", kind="progress",
            source="agent_discovery", payload={"next_action": "bypass"},
            expected_revision=48, created_at="later",
        )
        client.child_comments.append({
            "id": "late-direct", "body": encode_event_comment(late),
            "createdAt": "later", "updatedAt": "later",
        })
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "description_plan_revision": PLAN,
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "status": "In Progress", "status_type": "started",
                "parent": {"id": ROOT_ID}, "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        with self.assertRaisesRegex(
            Exception, "child_legacy_write_after_origin_seal",
        ):
            add_child_material_history(
                snapshot, {"GEN-38": deepcopy(client.child_comments)},
                authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
                root_comments=deepcopy(client.root_comments),
            )

    def test_lost_append_response_converges_only_to_exact_seal(self):
        client = self.client()
        client.transport_error_after_root_append = True
        _preview, result = self.seal(client)
        self.assertEqual(result["receipt"]["disposition"], "created")
        self.assertEqual(len(client.root_comments), 3)


if __name__ == "__main__":
    unittest.main()
