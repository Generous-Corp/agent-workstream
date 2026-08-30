#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from workstream_checkpoint import build_checkpoint
import workstream_child_checkpoint
import workstream_child_event
from workstream_linear import LinearTransportError
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, projection_slot_id,
)
from workstream_resume import add_child_material_history, compact_context


PLAN = "a" * 64
ROOT_ID = "11111111-1111-4111-8111-111111111111"
CHILD_ID = "22222222-2222-4222-8222-222222222222"
ROUTE = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
}


class FakeChildStateClient:
    def __init__(self):
        scope = self.scope_event({"GEN-38": "github.com:id:R_repo"})
        self.root_comments: list[dict] = [{
            "id": projection_slot_id(
                scope["workstream_id"], scope["plan_revision"],
                scope["expected_revision"], scope["authority"],
            ),
            "body": encode_projection_comment(scope),
            "createdAt": "2026-08-30T00:00:00Z",
            "updatedAt": "2026-08-30T00:00:00Z",
        }]
        self.child_comments: list[dict] = []
        self.calls: list[tuple[str, dict]] = []

    @staticmethod
    def scope_event(child_ownership):
        return build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root",
            value={
                "namespace": "child-state-tests",
                "linear": {**ROUTE, "root_issue_id": ROOT_ID},
                "primary_repository": "github.com:id:R_repo",
                "repositories": [], "child_ownership": child_ownership,
            },
            plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-30T00:00:00Z",
            authority={**ROUTE, "root_issue_id": ROOT_ID},
        )

    @staticmethod
    def issue(identifier, issue_id, comments):
        return {
            "id": issue_id, "identifier": identifier,
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project"},
            "comments": {
                "nodes": deepcopy(comments),
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            },
        }

    def execute(self, query, variables):
        self.calls.append((query, deepcopy(variables)))
        if "query WorkstreamChildTarget" in query:
            return {
                "root": {
                    **self.issue("GEN-37", ROOT_ID, self.root_comments),
                    "description": f"Plan revision: {PLAN}", "parent": None,
                },
                "child": {
                    **self.issue("GEN-38", CHILD_ID, self.child_comments),
                    "parent": {"id": ROOT_ID, "identifier": "GEN-37"},
                },
            }
        if "query WorkstreamDeltaComments" in query:
            identifier = variables["issueId"]
            if identifier == "GEN-37":
                return {"issue": self.issue("GEN-37", ROOT_ID, self.root_comments)}
            if identifier == "GEN-38":
                return {"issue": self.issue("GEN-38", CHILD_ID, self.child_comments)}
            raise AssertionError(f"unexpected issue: {identifier}")
        if "CommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}]}}
        if "commentCreate" in query:
            item = variables["input"]
            if item["issueId"] != "GEN-38":
                raise AssertionError("child command attempted a root comment write")
            if any(comment["id"] == item["id"] for comment in self.child_comments):
                raise LinearTransportError("duplicate comment id")
            comment = {
                "id": item["id"], "body": item["body"],
                "createdAt": "2026-08-30T00:00:00Z",
                "updatedAt": "2026-08-30T00:00:00Z",
            }
            self.child_comments.append(comment)
            return {"commentCreate": {"success": True, "comment": deepcopy(comment)}}
        raise AssertionError(f"unexpected GraphQL operation: {query[:80]}")


class WorkstreamChildStateTests(unittest.TestCase):
    def common(self):
        return [
            "GEN-37", "--root-issue-id", ROOT_ID,
            "--child-workstream-id", "GEN-38", "--child-issue-id", CHILD_ID,
            "--plan-revision", PLAN, "--workspace-id", "workspace",
            "--team-id", "team", "--project-id", "project", "--apply",
        ]

    def patches(self):
        return (
            mock.patch(
                "workstream_child_target.resolve_linear_route",
                return_value=(ROUTE, None),
            ),
            mock.patch(
                "workstream_child_target.load_linear_api_key",
                return_value="secret",
            ),
        )

    def test_child_event_and_checkpoint_leave_root_unchanged_and_resume(self):
        client = FakeChildStateClient()
        root_comments_before = deepcopy(client.root_comments)
        event_args = [
            *self.common(), "--kind", "material_boundary",
            "--source", "agent_discovery", "--expected-revision", "0",
            "--created-at", "2026-08-30T00:00:00Z", "--payload-json",
            json.dumps({
                "boundary_id": "child-ready",
                "changes": [{
                    "kind": "progress", "payload": {
                        "next_action": "Run the child acceptance proof.",
                        "blocker": {"kind": "review", "owner": "maintainer"},
                    },
                }],
            }),
        ]
        checkpoint = build_checkpoint(
            workstream_id="GEN-38", boundary_id="child-checkpoint",
            root_revision=1, plan_revision=PLAN,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "child",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/child", "branch": "child",
                    "head": "b" * 40,
                },
            }, exact_head="b" * 40, evidence=[{"kind": "test", "id": "focused"}],
            blocker={"kind": "review", "owner": "maintainer"},
            next_action="Run the child acceptance proof.",
        )
        with tempfile.TemporaryDirectory() as directory:
            checkpoint_path = Path(directory) / "checkpoint.json"
            checkpoint_path.write_text(json.dumps(checkpoint))
            checkpoint_args = [
                *self.common(), "--checkpoint", str(checkpoint_path),
                "--material-revision", "1", "--no-predecessor",
            ]
            route_patch, auth_patch = self.patches()
            with route_patch, auth_patch:
                first_event = workstream_child_event.run(
                    event_args, client_factory=lambda _token: client,
                )
                first_checkpoint = workstream_child_checkpoint.run(
                    checkpoint_args, client_factory=lambda _token: client,
                )
                writes = len(client.child_comments)
                replay_event = workstream_child_event.run(
                    event_args, client_factory=lambda _token: client,
                )
                replay_checkpoint = workstream_child_checkpoint.run(
                    checkpoint_args, client_factory=lambda _token: client,
                )

        self.assertEqual(client.root_comments, root_comments_before)
        self.assertEqual(len(client.child_comments), writes)
        self.assertEqual(first_event["receipt"], replay_event["receipt"])
        self.assertEqual(
            first_checkpoint["receipt"]["event_id"],
            replay_checkpoint["receipt"]["event_id"],
        )
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 7,
                "status": "In Progress", "status_type": "started",
                "next_action": "Continue the root.",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child work",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "next_action": "stale issue prose",
                "parent": {"id": ROOT_ID}, "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        enriched = add_child_material_history(
            snapshot, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
        )
        child = compact_context(enriched, "GEN-37")["children"][0]
        self.assertEqual(child["next_action"], "Run the child acceptance proof.")
        self.assertEqual(child["blocker"], {
            "kind": "review", "owner": "maintainer",
        })
        self.assertEqual(child["latest_checkpoint"]["exact_head"], "b" * 40)
        self.assertEqual(
            child["latest_checkpoint"]["checkpoint_event_id"],
            checkpoint["event_id"],
        )

    def test_child_target_identity_mismatch_refuses_before_comment_write(self):
        client = FakeChildStateClient()
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", "{}",
        ]
        route_patch, auth_patch = self.patches()
        original = client.execute
        with route_patch, auth_patch, mock.patch.object(
            client, "execute", wraps=original,
        ) as execute:
            def wrong_child(query, variables):
                result = original(query, variables)
                if "query WorkstreamChildTarget" in query:
                    result["child"]["id"] = "33333333-3333-4333-8333-333333333333"
                return result

            execute.side_effect = wrong_child
            with self.assertRaisesRegex(
                LinearTransportError, "child_target_identity_mismatch",
            ):
                workstream_child_event.run(
                    args, client_factory=lambda _token: client,
                )
        self.assertEqual(len(client.root_comments), 1)
        self.assertEqual(client.child_comments, [])

    def test_unowned_child_refuses_before_comment_write(self):
        client = FakeChildStateClient()
        scope = client.scope_event({})
        client.root_comments[0].update({
            "id": projection_slot_id(
                scope["workstream_id"], scope["plan_revision"],
                scope["expected_revision"], scope["authority"],
            ),
            "body": encode_projection_comment(scope),
        })
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", "{}",
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, self.assertRaisesRegex(
            Exception, "child_target_not_owned:GEN-38",
        ):
            workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        self.assertEqual(len(client.root_comments), 1)
        self.assertEqual(client.child_comments, [])


if __name__ == "__main__":
    unittest.main()
