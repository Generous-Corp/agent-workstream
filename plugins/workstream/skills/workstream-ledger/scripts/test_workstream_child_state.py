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
import workstream_child_proposal_activate
from workstream_linear import LinearTransportError
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, LinearProjectionAdapter,
    projection_slot_id,
    TOMBSTONE,
)
from workstream_resume import add_child_material_history, compact_context


PLAN = "a" * 64
ROOT_ID = "11111111-1111-4111-8111-111111111111"
CHILD_ID = "07104fd8-924f-40d8-b7e2-fe2f87f76657"
ROUTE = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
}


class FakeChildStateClient:
    def __init__(self):
        scope = self.scope_event({"GEN-38": "github.com:id:R_repo"})
        source = build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"identity": "https://example.test/plan", "sha256": PLAN},
            plan_revision=PLAN, expected_revision=1,
            created_at="2026-08-30T00:00:01Z",
            authority={**ROUTE, "root_issue_id": ROOT_ID},
        )
        origin = build_projection_event(
            workstream_id="GEN-37", kind="child_extension_authorization",
            key=CHILD_ID, value={
                "root_issue_id": ROOT_ID,
                "route": {**ROUTE, "root_issue_id": ROOT_ID},
                "source": {"identity": "https://example.test/plan", "sha256": PLAN},
                "plan_revision": PLAN, "reviewed_candidate_key": "a",
                "child_issue_id": CHILD_ID, "expected_material_revision": 0,
                "expected_projection_revision": 2,
                "initial_state": "planned_pending_projection",
            }, plan_revision=PLAN, expected_revision=2,
            created_at="2026-08-30T00:00:02Z",
            authority={**ROUTE, "root_issue_id": ROOT_ID},
        )
        self.root_comments: list[dict] = [{
            "id": projection_slot_id(
                scope["workstream_id"], scope["plan_revision"],
                scope["expected_revision"], scope["authority"],
            ),
            "body": encode_projection_comment(scope),
            "createdAt": "2026-08-30T00:00:00Z",
            "updatedAt": "2026-08-30T00:00:00Z",
        }, {
            "id": projection_slot_id(
                source["workstream_id"], source["plan_revision"],
                source["expected_revision"], source["authority"],
            ), "body": encode_projection_comment(source),
            "createdAt": "2026-08-30T00:00:01Z",
            "updatedAt": "2026-08-30T00:00:01Z",
        }, {
            "id": projection_slot_id(
                origin["workstream_id"], origin["plan_revision"],
                origin["expected_revision"], origin["authority"],
            ), "body": encode_projection_comment(origin),
            "createdAt": "2026-08-30T00:00:02Z",
            "updatedAt": "2026-08-30T00:00:02Z",
        }]
        self.child_comments: list[dict] = []
        self.calls: list[tuple[str, dict]] = []
        self.crash_after_root_append = False
        self.transport_error_after_root_append = False
        self.child_parent_id = ROOT_ID

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
        if (
            "query WorkstreamChildTarget" in query
            or "query WorkstreamChildOriginRepairTarget" in query
        ):
            return {
                "root": {
                    **self.issue("GEN-37", ROOT_ID, self.root_comments),
                    "description": f"Plan revision: {PLAN}", "parent": None,
                    "createdAt": "2026-08-01T00:00:00Z",
                    "state": {
                        "id": "state-started", "name": "In Progress",
                        "type": "started",
                    },
                    "assignee": {"id": "assignee"},
                },
                "child": {
                    **self.issue("GEN-38", CHILD_ID, self.child_comments),
                    "description": "Legacy child description",
                    "createdAt": "2026-08-01T00:00:00Z",
                    "state": {
                        "id": "state-started", "name": "In Progress",
                        "type": "started",
                    },
                    "assignee": {"id": "assignee"},
                    "parent": {
                        "id": self.child_parent_id,
                        "identifier": (
                            "GEN-37" if self.child_parent_id == ROOT_ID else "GEN-99"
                        ),
                    },
                },
            }
        if "query WorkstreamChildOriginNativeReadback" in query:
            return {"issue": {
                **self.issue("GEN-38", CHILD_ID, self.child_comments),
                "description": "Legacy child description",
                "createdAt": "2026-08-01T00:00:00Z",
                "state": {
                    "id": "state-started", "name": "In Progress",
                    "type": "started",
                },
                "assignee": {"id": "assignee"},
                "parent": {"id": self.child_parent_id, "identifier": (
                    "GEN-37" if self.child_parent_id == ROOT_ID else "GEN-99"
                )},
            }}
        if "query WorkstreamChildMutationTarget" in query:
            return {"issue": {
                **self.issue("GEN-38", CHILD_ID, self.child_comments),
                "parent": {"id": ROOT_ID, "identifier": "GEN-37"},
            }}
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
            target = (
                self.child_comments if item["issueId"] == "GEN-38"
                else self.root_comments if item["issueId"] == "GEN-37"
                else None
            )
            if target is None:
                raise AssertionError("unexpected comment target")
            if any(comment["id"] == item["id"] for comment in target):
                raise LinearTransportError("duplicate comment id")
            comment = {
                "id": item["id"], "body": item["body"],
                "createdAt": "2026-08-30T00:00:00Z",
                "updatedAt": "2026-08-30T00:00:00Z",
            }
            target.append(comment)
            if item["issueId"] == "GEN-37" and self.transport_error_after_root_append:
                self.transport_error_after_root_append = False
                raise LinearTransportError("lost response after durable append")
            if item["issueId"] == "GEN-37" and self.crash_after_root_append:
                self.crash_after_root_append = False
                raise SystemExit("death after root activation")
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

    def test_real_projection_adapter_recognizes_legacy_origin_replay(self):
        client = FakeChildStateClient()
        receipt = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **ROUTE, root_issue_id=ROOT_ID,
        ).replay_legacy_child_extension(
            source={"identity": "https://example.test/plan", "sha256": PLAN},
            reviewed_candidate_key="a", child_issue_id=CHILD_ID,
            require_existing=True,
        )
        self.assertEqual(receipt["disposition"], "legacy_existing")
        self.assertEqual(receipt["event"]["key"], CHILD_ID)

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

        self.assertEqual(
            client.root_comments[:len(root_comments_before)], root_comments_before,
        )
        self.assertEqual(len(client.root_comments), len(root_comments_before) + 2)
        self.assertTrue(all(
            "Run the child acceptance proof." not in comment["body"]
            for comment in client.root_comments
        ))
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
            root_comments=deepcopy(client.root_comments),
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
        self.assertEqual(len(client.root_comments), 3)
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
        self.assertEqual(len(client.root_comments), 3)
        self.assertEqual(client.child_comments, [])

    def test_death_before_root_activation_leaves_inert_recoverable_proposal(self):
        client = FakeChildStateClient()
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", '{"next_action":"recover"}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, mock.patch(
            "workstream_linear_projection.LinearProjectionAdapter.reserve_child_mutation",
            side_effect=OSError("death before activation"),
        ):
            with self.assertRaisesRegex(OSError, "death before activation"):
                workstream_child_event.run(args, client_factory=lambda _token: client)
        self.assertEqual(len(client.root_comments), 3)
        self.assertEqual(len(client.child_comments), 1)
        self.assertNotIn("workstream-delta:v1", client.child_comments[0]["body"])
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 0,
                "status": "In Progress", "status_type": "started",
                "next_action": "root action",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "next_action": "issue action",
                "parent": {"id": ROOT_ID}, "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        pending_context = compact_context(add_child_material_history(
            snapshot, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=deepcopy(client.root_comments),
        ), "GEN-37")
        pending = pending_context["children"][0]["pending_child_proposals"]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending_context["children"][0]["next_action"], "issue action")
        self.assertNotIn("record", pending[0])
        activation_args = [
            *self.common(), "--proposal-id", pending[0]["proposal_id"],
            "--proposal-remote-id", pending[0]["proposal_remote_id"],
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            result = workstream_child_proposal_activate.run(
                activation_args, client_factory=lambda _token: client,
            )
            root_writes = len(client.root_comments)
            child_writes = len(client.child_comments)
            replay = workstream_child_proposal_activate.run(
                activation_args, client_factory=lambda _token: client,
            )
        self.assertEqual(result["authorization"]["disposition"], "created")
        self.assertEqual(replay["authorization"]["disposition"], "existing")
        self.assertEqual(len(client.root_comments), root_writes)
        self.assertEqual(len(client.child_comments), child_writes)

    def test_reparent_race_cannot_transfer_root_authority_and_resume_reports_drift(self):
        client = FakeChildStateClient()
        other_root = "33333333-3333-4333-8333-333333333333"
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", '{"next_action":"original root action"}',
        ]
        original_select = LinearProjectionAdapter.select_owned_child_generation
        def select_then_reparent(adapter, **values):
            result = original_select(adapter, **values)
            client.child_parent_id = other_root
            return result
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, mock.patch.object(
            LinearProjectionAdapter, "select_owned_child_generation",
            new=select_then_reparent,
        ):
            result = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        grant = result["authorization"]["event"]
        self.assertEqual(grant["authority"]["root_issue_id"], ROOT_ID)
        self.assertEqual(grant["value"]["child_origin"]["kind"],
                         "child_extension_authorization")
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 0,
                "status": "In Progress", "status_type": "started",
                "next_action": "root action",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "next_action": "issue action",
                "parent": {"id": other_root}, "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        child = compact_context(add_child_material_history(
            snapshot, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=deepcopy(client.root_comments),
        ), "GEN-37")["children"][0]
        self.assertEqual(child["next_action"], "original root action")
        self.assertEqual(child["reconciliation_blockers"][0]["field"],
                         "parent_issue_id")
        with self.assertRaises(Exception):
            LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=PLAN, **ROUTE, root_issue_id=other_root,
            ).state()

    def test_death_after_root_activation_replays_without_second_write(self):
        client = FakeChildStateClient(); client.crash_after_root_append = True
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", '{"next_action":"recover"}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, self.assertRaisesRegex(
            SystemExit, "death after root activation",
        ):
            workstream_child_event.run(args, client_factory=lambda _token: client)
        root_writes = len(client.root_comments)
        child_writes = len(client.child_comments)
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            result = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        self.assertEqual(result["authorization"]["disposition"], "existing")
        self.assertEqual(len(client.root_comments), root_writes)
        self.assertEqual(len(client.child_comments), child_writes)

    def test_exact_activation_replays_after_scope_removes_child(self):
        client = FakeChildStateClient()
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", '{"next_action":"historical"}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            first = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        initial_scope = client.scope_event({"GEN-38": "github.com:id:R_repo"})
        removed = build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root",
            value=TOMBSTONE, plan_revision=PLAN, expected_revision=4,
            created_at="later",
            supersedes_event_id=initial_scope["event_id"],
            authority={**ROUTE, "root_issue_id": ROOT_ID},
        )
        client.root_comments.append({
            "id": projection_slot_id(
                removed["workstream_id"], removed["plan_revision"],
                removed["expected_revision"], removed["authority"],
            ), "body": encode_projection_comment(removed),
            "createdAt": "later", "updatedAt": "later",
        })
        root_count = len(client.root_comments); child_count = len(client.child_comments)
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            replay = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        self.assertEqual(first["receipt"], replay["receipt"])
        self.assertEqual(len(client.root_comments), root_count)
        self.assertEqual(len(client.child_comments), child_count)

    def test_different_payload_at_authorized_child_frontier_stays_inert(self):
        client = FakeChildStateClient()
        base = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            workstream_child_event.run(
                [*base, "--payload-json", '{"next_action":"first"}'],
                client_factory=lambda _token: client,
            )
        root_count = len(client.root_comments)
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, self.assertRaisesRegex(
            LinearTransportError, "material_frontier_stale",
        ):
            workstream_child_event.run(
                [*base, "--payload-json", '{"next_action":"different"}'],
                client_factory=lambda _token: client,
            )
        self.assertEqual(len(client.root_comments), root_count)
        self.assertEqual(len(client.child_comments), 2)

    def test_conflicting_explicit_event_id_refuses_before_second_grant(self):
        client = FakeChildStateClient()
        first = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--event-id", "explicit-child-event", "--payload-json", '{"v":1}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            workstream_child_event.run(first, client_factory=lambda _token: client)
        root_count = len(client.root_comments)
        second = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "1", "--created-at", "later",
            "--event-id", "explicit-child-event", "--payload-json", '{"v":2}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, self.assertRaisesRegex(
            Exception, "event_id_already_authorized",
        ):
            workstream_child_event.run(second, client_factory=lambda _token: client)
        self.assertEqual(len(client.root_comments), root_count)

    def test_second_child_event_receipt_reduces_full_authoritative_history(self):
        client = FakeChildStateClient()
        def event(revision, value):
            return [
                *self.common(), "--kind", "progress", "--source", "user_turn",
                "--expected-revision", str(revision), "--created-at", "now",
                "--payload-json", json.dumps({"value": value}),
            ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            first = workstream_child_event.run(
                event(0, "first"), client_factory=lambda _token: client,
            )
            second = workstream_child_event.run(
                event(1, "second"), client_factory=lambda _token: client,
            )
            root_writes = len(client.root_comments)
            child_writes = len(client.child_comments)
            replay = workstream_child_event.run(
                event(1, "second"), client_factory=lambda _token: client,
            )
        self.assertEqual(first["receipt"]["revision"], 1)
        self.assertEqual(second["receipt"]["revision"], 2)
        self.assertEqual(replay["receipt"], second["receipt"])
        self.assertEqual(len(client.root_comments), root_writes)
        self.assertEqual(len(client.child_comments), child_writes)

    def test_resume_refuses_activated_grant_with_missing_child_origin(self):
        client = FakeChildStateClient()
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", '{"next_action":"must stay authenticated"}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            result = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        grant = result["authorization"]["event"]
        bad = build_projection_event(
            workstream_id=grant["workstream_id"], kind=grant["kind"],
            key=grant["key"], value={**grant["value"], "child_origin": {}},
            plan_revision=grant["plan_revision"],
            expected_revision=grant["expected_revision"],
            created_at=grant["created_at"], authority=grant["authority"],
        )
        grant_comment = client.root_comments[-1]
        grant_comment["body"] = encode_projection_comment(bad)
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 0,
                "status": "In Progress", "status_type": "started",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "parent": {"id": ROOT_ID},
                "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        with self.assertRaisesRegex(Exception, "child_origin_provenance"):
            add_child_material_history(
                snapshot, {"GEN-38": deepcopy(client.child_comments)},
                authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
                root_comments=deepcopy(client.root_comments),
            )

    def test_nonmonotonic_checkpoint_refuses_before_second_grant(self):
        client = FakeChildStateClient()
        def checkpoint(boundary, predecessor):
            return build_checkpoint(
                workstream_id="GEN-38", boundary_id=boundary,
                root_revision=0, plan_revision=PLAN,
                before_status="In Progress", after_status="In Progress",
                execution={
                    "agent": "codex", "provider": "openai",
                    "session_id": boundary, "machine": "M5", "worktree": {
                        "state": "safe", "path": "/repo/child",
                        "branch": "child", "head": "b" * 40,
                    },
                }, exact_head="b" * 40, evidence=[], blocker=None,
                next_action="continue", predecessor_event_id=predecessor,
            )
        with tempfile.TemporaryDirectory() as directory:
            first = checkpoint("first", None)
            first_path = Path(directory) / "first.json"
            first_path.write_text(json.dumps(first))
            route_patch, auth_patch = self.patches()
            with route_patch, auth_patch:
                workstream_child_checkpoint.run([
                    *self.common(), "--checkpoint", str(first_path),
                    "--material-revision", "0", "--no-predecessor",
                ], client_factory=lambda _token: client)
            root_count = len(client.root_comments)
            second = checkpoint("second", first["event_id"])
            second_path = Path(directory) / "second.json"
            second_path.write_text(json.dumps(second))
            route_patch, auth_patch = self.patches()
            with route_patch, auth_patch, self.assertRaisesRegex(
                Exception, "checkpoint_frontier_stale",
            ):
                workstream_child_checkpoint.run([
                    *self.common(), "--checkpoint", str(second_path),
                    "--material-revision", "0",
                    "--predecessor-event-id", first["event_id"],
                ], client_factory=lambda _token: client)
        self.assertEqual(len(client.root_comments), root_count)


if __name__ == "__main__":
    unittest.main()
