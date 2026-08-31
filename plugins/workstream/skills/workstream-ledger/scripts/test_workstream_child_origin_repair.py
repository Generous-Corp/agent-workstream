#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from test_workstream_child_state import (
    CHILD_ID, PLAN, ROOT_ID, ROUTE, FakeChildStateClient,
)
from workstream_checkpoint import build_checkpoint
from workstream_delta import Delta
import workstream_child_event
import workstream_child_checkpoint
import workstream_child_proposal_activate
import workstream_child_origin_repair
from workstream_linear_events import encode_event_comment
from workstream_linear_checkpoints import encode_checkpoint_comment
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, LinearProjectionAdapter,
    legacy_child_origin_repairs_from_comments, projection_slot_id,
)
from workstream_child_proposal import _append_proposal, build_proposal
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

    def populate_legacy_history(self, client: FakeChildStateClient):
        populated = self.client()
        client.root_comments = deepcopy(populated.root_comments)
        client.child_comments = deepcopy(populated.child_comments)
        return client

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

    def apply_preview(self, client: FakeChildStateClient, preview):
        material = json.dumps(
            preview, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        with tempfile.TemporaryDirectory() as directory:
            review = Path(directory) / "review.json"
            review.write_bytes(material)
            route_patch, auth_patch = self.patches()
            with route_patch, auth_patch:
                return workstream_child_origin_repair.run(
                    [*self.common(), "--review", str(review),
                     "--review-identity", REVIEW_IDENTITY, "--apply"],
                    client_factory=lambda _token: client,
                    source_loader=lambda _identity, _expected: (
                        material, REVIEW_IDENTITY,
                    ),
                )

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

    def test_multiple_origins_across_generations_refuse_without_write(self):
        client = self.client()
        first = {
            "value": {"child_issue_id": CHILD_ID},
            "plan_revision": "b" * 64,
        }
        second = {
            "value": {"child_issue_id": CHILD_ID},
            "plan_revision": PLAN,
        }
        before = deepcopy(client.root_comments)
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, mock.patch(
            "workstream_child_origin_repair.legacy_child_origin_repairs_from_comments",
            return_value=[first, second],
        ), self.assertRaisesRegex(Exception, "child_origin_repair_ambiguous"):
            workstream_child_origin_repair.run(
                self.common(), client_factory=lambda _token: client,
            )
        self.assertEqual(client.root_comments, before)

    def test_reordered_or_changed_reviewed_prefix_refuses_without_write(self):
        for mutation in ("reordered", "changed"):
            with self.subTest(mutation=mutation):
                client = self.client()
                route_patch, auth_patch = self.patches()
                with route_patch, auth_patch:
                    preview = workstream_child_origin_repair.run(
                        self.common(), client_factory=lambda _token: client,
                    )
                if mutation == "reordered":
                    first = client.child_comments[0]["body"]
                    client.child_comments[0]["body"] = client.child_comments[1]["body"]
                    client.child_comments[1]["body"] = first
                else:
                    client.child_comments[0]["body"] += " tampered"
                with self.assertRaisesRegex(
                    Exception, "child_origin_review_stale|child_history_changed",
                ):
                    self.apply_preview(client, preview)
                self.assertEqual(len(client.root_comments), 2)

    def test_planted_projection_slot_race_refuses_without_seal(self):
        class SlotRaceClient(FakeChildStateClient):
            planted = False

            def execute(self, query, variables):
                if (
                    "commentCreate" in query
                    and variables["input"]["issueId"] == "GEN-37"
                    and not self.planted
                ):
                    self.planted = True
                    winner = build_projection_event(
                        workstream_id="GEN-37", kind="provenance", key="racer",
                        value={"agent": "racer", "machine": "M5",
                               "session_id": "concurrent"},
                        plan_revision=PLAN, expected_revision=2,
                        created_at="2026-08-30T01:00:00Z",
                        authority={**ROUTE, "root_issue_id": ROOT_ID},
                    )
                    self.root_comments.append({
                        "id": variables["input"]["id"],
                        "body": encode_projection_comment(winner),
                        "createdAt": winner["created_at"],
                        "updatedAt": winner["created_at"],
                    })
                return super().execute(query, variables)

        client = self.populate_legacy_history(SlotRaceClient())
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            preview = workstream_child_origin_repair.run(
                self.common(), client_factory=lambda _token: client,
            )
        with self.assertRaisesRegex(
            Exception, "projection_slot_lost_reload_required",
        ):
            self.apply_preview(client, preview)
        self.assertEqual(len(client.root_comments), 3)

    def test_preflight_root_and_child_append_races_add_no_seal(self):
        for target in ("root", "child"):
            with self.subTest(target=target):
                client = self.client()
                route_patch, auth_patch = self.patches()
                with route_patch, auth_patch:
                    preview = workstream_child_origin_repair.run(
                        self.common(), client_factory=lambda _token: client,
                    )
                if target == "root":
                    event = build_projection_event(
                        workstream_id="GEN-37", kind="provenance", key="preflight",
                        value={"agent": "racer", "machine": "M5",
                               "session_id": "preflight"},
                        plan_revision=PLAN, expected_revision=2,
                        created_at="2026-08-30T00:59:59Z",
                        authority={**ROUTE, "root_issue_id": ROOT_ID},
                    )
                    client.root_comments.append({
                        "id": projection_slot_id(
                            event["workstream_id"], event["plan_revision"],
                            event["expected_revision"], event["authority"],
                        ),
                        "body": encode_projection_comment(event),
                        "createdAt": event["created_at"],
                        "updatedAt": event["created_at"],
                    })
                else:
                    event = Delta(
                        event_id="preflight-child", workstream_id="GEN-38",
                        kind="progress", source="agent_discovery", payload={},
                        expected_revision=48, created_at="2026-08-30T00:59:59Z",
                    )
                    client.child_comments.append({
                        "id": "preflight-child", "body": encode_event_comment(event),
                        "createdAt": event.created_at, "updatedAt": event.created_at,
                    })
                before_root = len(client.root_comments)
                with self.assertRaisesRegex(
                    Exception,
                    "child_origin_review_stale|child_origin_repair_child_history_changed",
                ):
                    self.apply_preview(client, preview)
                self.assertEqual(len(client.root_comments), before_root)

    def test_postread_root_and_child_append_races_fail_closed(self):
        class PostreadRaceClient(FakeChildStateClient):
            def __init__(self, target):
                super().__init__()
                self.target = target
                self.seal_appended = False
                self.injected = False

            def execute(self, query, variables):
                if (
                    "commentCreate" in query
                    and variables["input"]["issueId"] == "GEN-37"
                ):
                    response = super().execute(query, variables)
                    self.seal_appended = True
                    return response
                if self.seal_appended and not self.injected:
                    if self.target == "root" and "query WorkstreamDeltaComments" in query and variables["issueId"] == "GEN-37":
                        self.injected = True
                        event = build_projection_event(
                            workstream_id="GEN-37", kind="provenance", key="late",
                            value={"agent": "racer", "machine": "M5",
                                   "session_id": "postread"},
                            plan_revision=PLAN, expected_revision=3,
                            created_at="2026-08-30T01:00:01Z",
                            authority={**ROUTE, "root_issue_id": ROOT_ID},
                        )
                        self.root_comments.append({
                            "id": projection_slot_id(
                                event["workstream_id"], event["plan_revision"],
                                event["expected_revision"], event["authority"],
                            ),
                            "body": encode_projection_comment(event),
                            "createdAt": event["created_at"],
                            "updatedAt": event["created_at"],
                        })
                    elif self.target == "child" and "query WorkstreamDeltaComments" in query and variables["issueId"] == "GEN-38":
                        self.injected = True
                        event = Delta(
                            event_id="late-child", workstream_id="GEN-38",
                            kind="progress", source="agent_discovery", payload={},
                            expected_revision=48, created_at="later",
                        )
                        self.child_comments.append({
                            "id": "late-child", "body": encode_event_comment(event),
                            "createdAt": "later", "updatedAt": "later",
                        })
                return super().execute(query, variables)

        for target, error in (
            ("root", "postread_root_drift"),
            ("child", "postread_child_drift"),
        ):
            with self.subTest(target=target):
                client = self.populate_legacy_history(PostreadRaceClient(target))
                route_patch, auth_patch = self.patches()
                with route_patch, auth_patch:
                    preview = workstream_child_origin_repair.run(
                        self.common(), client_factory=lambda _token: client,
                    )
                with self.assertRaisesRegex(Exception, error):
                    self.apply_preview(client, preview)
                self.assertTrue(client.injected)

    def test_root_native_race_before_seal_refuses_without_write(self):
        class RootNativeRaceClient(FakeChildStateClient):
            def execute(self, query, variables):
                result = super().execute(query, variables)
                if "query WorkstreamRootOriginNativeReadback" in query:
                    result["issue"]["state"]["id"] = "changed-state"
                return result

        client = self.populate_legacy_history(RootNativeRaceClient())
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            preview = workstream_child_origin_repair.run(
                self.common(), client_factory=lambda _token: client,
            )
        with self.assertRaisesRegex(Exception, "native_root_drift"):
            self.apply_preview(client, preview)
        self.assertEqual(len(client.root_comments), 2)

    def test_post_seal_direct_checkpoint_refuses_target_reserve_and_activation(self):
        client = self.client()
        preview, _result = self.seal(client)
        event = Delta(
            event_id="authorized-candidate", workstream_id="GEN-38",
            kind="progress", source="agent_discovery",
            payload={"next_action": "must not activate"},
            expected_revision=48, created_at="later",
        )
        record = {
            "event_id": event.event_id, "workstream_id": event.workstream_id,
            "kind": event.kind, "source": event.source,
            "payload": event.payload,
            "expected_revision": event.expected_revision,
            "created_at": event.created_at,
        }
        proposal = build_proposal(
            "event", record, child_workstream_id="GEN-38",
            child_issue_id=CHILD_ID, plan_revision=PLAN,
        )
        proposal_receipt = _append_proposal(client, proposal)
        checkpoint = build_checkpoint(
            workstream_id="GEN-38", boundary_id="direct-bypass",
            root_revision=48, plan_revision=PLAN,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "legacy", "provider": "legacy",
                "session_id": "retired", "machine": "M3",
                "worktree": {
                    "state": "safe", "path": "/repo/legacy",
                    "branch": "legacy", "head": "b" * 40,
                },
            }, exact_head="b" * 40, evidence=[], blocker=None,
            next_action="must not reduce",
            predecessor_event_id=(
                preview["value"]["child_history"]
                ["checkpoint_receipts"][-1]["event_id"]
            ),
        )
        client.child_comments.append({
            "id": "direct-checkpoint",
            "body": encode_checkpoint_comment(checkpoint),
            "createdAt": "later", "updatedAt": "later",
        })
        before_root = len(client.root_comments)
        before_child = len(client.child_comments)

        target_args = [
            "GEN-37", "--root-issue-id", ROOT_ID,
            "--child-workstream-id", "GEN-38", "--child-issue-id", CHILD_ID,
            "--plan-revision", PLAN, "--workspace-id", "workspace",
            "--team-id", "team", "--project-id", "project", "--apply",
            "--kind", "progress", "--source", "agent_discovery",
            "--expected-revision", "48", "--created-at", "later",
            "--payload-json", "{}",
        ]
        with mock.patch(
            "workstream_child_target.resolve_linear_route",
            return_value=(ROUTE, None),
        ), mock.patch(
            "workstream_child_target.load_linear_api_key", return_value="secret",
        ), self.assertRaisesRegex(
            Exception, "child_legacy_write_after_origin_seal",
        ):
            workstream_child_event.run(
                target_args, client_factory=lambda _token: client,
            )

        projection = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **ROUTE, root_issue_id=ROOT_ID,
        )
        selected = projection.select_owned_child_generation(
            description_plan_revision=PLAN, child_workstream_id="GEN-38",
            child_issue_id=CHILD_ID, proposal_id=proposal["proposal_id"],
        )
        generation = {
            key: selected[key] for key in (
                "plan_revision", "description_plan_revision",
                "transition_tip_event_id", "activation_epoch",
                "authority_origin", "workstream_id", "authority", "source",
            )
        }
        with self.assertRaisesRegex(
            Exception, "child_legacy_write_after_origin_seal",
        ):
            projection.reserve_child_mutation(
                proposal=proposal,
                proposal_remote_id=proposal_receipt["remote_id"],
                child_identity={
                    "identifier": "GEN-38", "id": CHILD_ID,
                    "parent_issue_id": ROOT_ID, "route": ROUTE,
                },
                generation_authority=generation,
                scope_event_id=selected["scope_event_id"],
                scope_value_sha256=selected["scope_value_sha256"],
                repository_owner=selected["child_repository_owner"],
                child_origin=selected["child_origin"],
                expected_projection_revision=selected["projection_revision"],
            )

        activation_args = [
            "GEN-37", "--root-issue-id", ROOT_ID,
            "--child-workstream-id", "GEN-38", "--child-issue-id", CHILD_ID,
            "--plan-revision", PLAN, "--workspace-id", "workspace",
            "--team-id", "team", "--project-id", "project", "--apply",
            "--proposal-id", proposal["proposal_id"],
            "--proposal-remote-id", proposal_receipt["remote_id"],
        ]
        with mock.patch(
            "workstream_child_target.resolve_linear_route",
            return_value=(ROUTE, None),
        ), mock.patch(
            "workstream_child_target.load_linear_api_key", return_value="secret",
        ), self.assertRaisesRegex(
            Exception, "child_legacy_write_after_origin_seal",
        ):
            workstream_child_proposal_activate.run(
                activation_args, client_factory=lambda _token: client,
            )
        self.assertEqual(len(client.root_comments), before_root)
        self.assertEqual(len(client.child_comments), before_child)

    def test_origin_seal_survives_authenticated_generation_retirement(self):
        client = self.client()
        self.seal(client)
        authority = {**ROUTE, "root_issue_id": ROOT_ID}
        seals = legacy_child_origin_repairs_from_comments(
            client.root_comments, workstream_id="GEN-37",
            description_plan_revision=PLAN, authenticated_route=authority,
        )
        self.assertEqual(len(seals), 1)
        retired = seals[0]
        current_plan = "d" * 64
        transition = {
            "kind": "generation_transition",
            "value": {
                "from": {
                    "plan_revision": PLAN,
                    "projection_revision": retired["expected_revision"] + 1,
                },
                "to": {"plan_revision": current_plan},
            },
        }

        def reduced(_comments, *, expected_plan_revision, **_kwargs):
            return SimpleNamespace(
                events=[retired] if expected_plan_revision == PLAN else [],
            )

        with mock.patch(
            "workstream_linear_projection.select_plan_generation",
            return_value={"plan_revision": current_plan},
        ), mock.patch(
            "workstream_generation.generation_controls",
            return_value=[transition],
        ), mock.patch(
            "workstream_linear_projection.reduce_projection_comments",
            side_effect=reduced,
        ):
            recovered = legacy_child_origin_repairs_from_comments(
                client.root_comments, workstream_id="GEN-37",
                description_plan_revision=current_plan,
                authenticated_route=authority,
            )
        self.assertEqual(recovered, [retired])

    def test_mutable_root_updates_preserve_resume_target_and_reserve_authority(self):
        class MutableRootClient(FakeChildStateClient):
            def __init__(self):
                super().__init__()
                self.mutable = False

            def execute(self, query, variables):
                result = super().execute(query, variables)
                if self.mutable:
                    target = (
                        result.get("root")
                        if "query WorkstreamChildTarget" in query
                        else result.get("issue")
                        if "query WorkstreamRootOriginNativeReadback" in query
                        else None
                    )
                    if isinstance(target, dict):
                        target["description"] = (
                            f"Plan revision: {PLAN}\nLegitimate updated prose"
                        )
                        if "state" in target:
                            target["state"] = {
                                "id": "state-updated", "name": "Review",
                                "type": "started",
                            }
                            target["assignee"] = {"id": "new-assignee"}
                return result

        client = self.populate_legacy_history(MutableRootClient())
        self.seal(client)
        client.mutable = True
        event_args = [
            "GEN-37", "--root-issue-id", ROOT_ID,
            "--child-workstream-id", "GEN-38", "--child-issue-id", CHILD_ID,
            "--plan-revision", PLAN, "--workspace-id", "workspace",
            "--team-id", "team", "--project-id", "project", "--apply",
            "--kind", "progress", "--source", "agent_discovery",
            "--expected-revision", "48", "--created-at", "later",
            "--payload-json", '{"next_action":"continue after root update"}',
        ]
        with mock.patch(
            "workstream_child_target.resolve_linear_route",
            return_value=(ROUTE, None),
        ), mock.patch(
            "workstream_child_target.load_linear_api_key", return_value="secret",
        ):
            result = workstream_child_event.run(
                event_args, client_factory=lambda _token: client,
            )
        self.assertEqual(result["authorization"]["disposition"], "created")
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "description_plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 4,
                "status": "Review", "status_type": "started",
                "next_action": "continue",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "url": "https://linear.test/GEN-38",
                "status": "In Progress", "status_type": "started",
                "parent": {"id": ROOT_ID}, "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        resumed = compact_context(add_child_material_history(
            snapshot, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=deepcopy(client.root_comments),
        ), "GEN-37")
        self.assertEqual(
            resumed["children"][0]["next_action"],
            "continue after root update",
        )

    def test_consumer_refuses_root_uuid_route_or_parent_drift(self):
        class ImmutableRootDriftClient(FakeChildStateClient):
            def __init__(self):
                super().__init__()
                self.drift = None

            def execute(self, query, variables):
                result = super().execute(query, variables)
                if (
                    self.drift is not None
                    and "query WorkstreamRootOriginNativeReadback" in query
                ):
                    root = result["issue"]
                    if self.drift == "uuid":
                        root["id"] = "33333333-3333-4333-8333-333333333333"
                    elif self.drift == "route":
                        root["team"]["id"] = "other-team"
                    elif self.drift == "parent":
                        root["parent"] = {
                            "id": "44444444-4444-4444-8444-444444444444",
                            "identifier": "GEN-1",
                        }
                return result

        for drift in ("uuid", "route", "parent"):
            with self.subTest(drift=drift):
                client = self.populate_legacy_history(ImmutableRootDriftClient())
                self.seal(client)
                client.drift = drift
                before_root = len(client.root_comments)
                before_child = len(client.child_comments)
                args = [
                    "GEN-37", "--root-issue-id", ROOT_ID,
                    "--child-workstream-id", "GEN-38",
                    "--child-issue-id", CHILD_ID,
                    "--plan-revision", PLAN, "--workspace-id", "workspace",
                    "--team-id", "team", "--project-id", "project", "--apply",
                    "--kind", "progress", "--source", "agent_discovery",
                    "--expected-revision", "48", "--created-at", "later",
                    "--payload-json", "{}",
                ]
                with mock.patch(
                    "workstream_child_target.resolve_linear_route",
                    return_value=(ROUTE, None),
                ), mock.patch(
                    "workstream_child_target.load_linear_api_key",
                    return_value="secret",
                ), self.assertRaisesRegex(
                    Exception,
                    "child_origin_repair_root_identity_drift|configured team",
                ):
                    workstream_child_event.run(
                        args, client_factory=lambda _token: client,
                    )
                self.assertEqual(len(client.root_comments), before_root)
                self.assertEqual(len(client.child_comments), before_child)


if __name__ == "__main__":
    unittest.main()
