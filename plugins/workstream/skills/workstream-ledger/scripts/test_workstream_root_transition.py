#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy
import unittest

from workstream_root_transition import (
    RootTransitionError, RootTransitionTransport,
)


TOKEN = "GEN-14"
ROOT_ID = "33333333-3333-4333-8333-333333333333"
AUTHORITY = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
    "root_issue_id": ROOT_ID,
}
PINNED = "https://github.com/example/private-plans/blob/" + "a" * 40 + "/plan.md"
MAIN = "https://github.com/example/private-plans/blob/main/plan.md"
STARTED_STATE = "44444444-4444-4444-8444-444444444444"


class FakeClient:
    def __init__(self):
        self.description = (
            "Human context before.\n\nCanonical plan: " + PINNED
            + "\n\nPlan revision: " + "f" * 64 + "\nHuman context after."
        )
        self.state = {"id": "done-state", "name": "Done", "type": "completed"}
        self.updated_at = "before"
        self.comments = [{
            "id": "ordinary", "body": "preserved", "createdAt": "t0", "updatedAt": "t0",
        }]
        self.calls: list[tuple[str, dict]] = []

    def root(self):
        return {
            "id": ROOT_ID, "identifier": TOKEN, "title": "Root",
            "description": self.description, "url": "https://linear.test/GEN-14",
            "updatedAt": self.updated_at, "archivedAt": None, "parent": None,
            "project": {"id": "project"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "assignee": {"id": "owner"}, "state": deepcopy(self.state),
        }

    def execute(self, query, variables):
        self.calls.append((query, deepcopy(variables)))
        if "WorkstreamRoute" in query:
            return {
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project", "teams": {"nodes": [{"id": "team"}]}},
            }
        if "WorkstreamIssues" in query:
            return {"team": {"issues": {"nodes": [self.root()], "pageInfo": {
                "hasNextPage": False, "endCursor": None,
            }}}}
        if "WorkstreamDeltaComments" in query:
            return {"issue": {
                "id": ROOT_ID, "identifier": TOKEN,
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                "comments": {"nodes": deepcopy(self.comments), "pageInfo": {
                    "hasNextPage": False, "endCursor": None,
                }},
            }}
        if "CommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}]}}
        if "WorkstreamDeltaCommentCreate" in query:
            value = variables["input"]
            existing = next((item for item in self.comments if item["id"] == value["id"]), None)
            if existing is None:
                existing = {
                    "id": value["id"], "body": value["body"],
                    "createdAt": "reservation", "updatedAt": "reservation",
                }
                self.comments.append(existing)
            return {"commentCreate": {"success": True, "comment": deepcopy(existing)}}
        if "WorkstreamRootTransitionState" in query:
            return {
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "workflowState": {
                    "id": variables["stateId"], "name": "In Progress",
                    "type": "started", "team": {"id": "team"},
                },
            }
        if "WorkstreamRootTransition" in query:
            update = variables["input"]
            if "description" in update:
                self.description = update["description"]
            if "stateId" in update:
                self.state = {"id": update["stateId"], "name": "In Progress", "type": "started"}
            self.updated_at = "after"
            return {"issueUpdate": {"success": True, "issue": self.root()}}
        raise AssertionError(query)


class RootTransitionTests(unittest.TestCase):
    def transport(self, fake):
        return RootTransitionTransport(fake, token=TOKEN, authority=AUTHORITY)

    def test_plan_url_preview_is_zero_write_and_apply_preserves_other_text(self):
        fake = FakeClient()
        transport = self.transport(fake)
        preview = transport.preview(operation="plan-url", target=MAIN)
        self.assertFalse(preview["apply"])
        self.assertFalse(any("mutation " in query for query, _ in fake.calls))
        original = fake.description
        result = transport.apply(
            operation="plan-url", target=MAIN,
            expected_snapshot_sha256=preview["expected_snapshot_sha256"],
            expected_frontier_sha256=preview["expected_frontier_sha256"],
        )
        self.assertTrue(result["apply"])
        self.assertEqual(fake.description, original.replace(PINNED, MAIN))
        self.assertIn("Plan revision: " + "f" * 64, fake.description)
        self.assertFalse(result["conditional_update_available"])
        self.assertFalse(any("issueCreate" in query for query, _ in fake.calls))

    def test_plan_url_exact_replay_is_zero_write(self):
        fake = FakeClient()
        transport = self.transport(fake)
        preview = transport.preview(operation="plan-url", target=MAIN)
        args = {
            "operation": "plan-url", "target": MAIN,
            "expected_snapshot_sha256": preview["expected_snapshot_sha256"],
            "expected_frontier_sha256": preview["expected_frontier_sha256"],
        }
        transport.apply(**args)
        writes = sum("WorkstreamRootTransition(" in query for query, _ in fake.calls)
        fake.comments.append({
            "id": "later", "body": "later generation event",
            "createdAt": "later", "updatedAt": "later",
        })
        replay = transport.apply(**args)
        self.assertEqual(sum("WorkstreamRootTransition(" in query for query, _ in fake.calls), writes)
        self.assertEqual(len([
            item for item in fake.comments
            if item["id"] == preview["reservation_slot_id"]
        ]), 1)
        self.assertEqual(replay["post_read_status"], "exact_replay_frontier_advanced")

    def test_plan_url_refuses_zero_multiple_and_different_document(self):
        for description, message in (
            ("No plan", "canonical_plan_source_missing"),
            (f"Canonical plan: {PINNED} {MAIN}", "canonical_plan_source_ambiguous"),
        ):
            fake = FakeClient()
            fake.description = description
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                self.transport(fake).preview(operation="plan-url", target=MAIN)
        fake = FakeClient()
        other = "https://github.com/example/private-plans/blob/main/other.md"
        with self.assertRaisesRegex(RootTransitionError, "different_document"):
            self.transport(fake).preview(operation="plan-url", target=other)

    def test_apply_refuses_snapshot_or_comment_frontier_drift_without_write(self):
        for mutate in ("snapshot", "frontier"):
            fake = FakeClient()
            transport = self.transport(fake)
            preview = transport.preview(operation="plan-url", target=MAIN)
            if mutate == "snapshot":
                fake.description += "\nConcurrent prose."
            else:
                fake.comments.append({
                    "id": "concurrent", "body": "new", "createdAt": "t1", "updatedAt": "t1",
                })
            with self.subTest(mutate=mutate), self.assertRaisesRegex(
                RootTransitionError, "snapshot_drift|frontier_drift"
            ):
                transport.apply(
                    operation="plan-url", target=MAIN,
                    expected_snapshot_sha256=preview["expected_snapshot_sha256"],
                    expected_frontier_sha256=preview["expected_frontier_sha256"],
                )
            self.assertFalse(any("WorkstreamRootTransition(" in query for query, _ in fake.calls))

    def test_reopen_requires_terminal_root_and_reviewed_started_state(self):
        fake = FakeClient()
        transport = self.transport(fake)
        preview = transport.preview(operation="reopen", target=STARTED_STATE)
        result = transport.apply(
            operation="reopen", target=STARTED_STATE,
            expected_snapshot_sha256=preview["expected_snapshot_sha256"],
            expected_frontier_sha256=preview["expected_frontier_sha256"],
        )
        self.assertEqual(result["final_root"]["state"], {
            "id": STARTED_STATE, "name": "In Progress", "type": "started",
        })
        writes = sum("WorkstreamRootTransition(" in query for query, _ in fake.calls)
        transport.apply(
            operation="reopen", target=STARTED_STATE,
            expected_snapshot_sha256=preview["expected_snapshot_sha256"],
            expected_frontier_sha256=preview["expected_frontier_sha256"],
        )
        self.assertEqual(sum("WorkstreamRootTransition(" in query for query, _ in fake.calls), writes)

        another = FakeClient()
        another.state = {"id": "todo", "name": "Todo", "type": "unstarted"}
        with self.assertRaisesRegex(RootTransitionError, "requires_terminal"):
            self.transport(another).preview(operation="reopen", target=STARTED_STATE)

    def test_reservation_collision_refuses_before_issue_update(self):
        fake = FakeClient()
        transport = self.transport(fake)
        preview = transport.preview(operation="plan-url", target=MAIN)
        fake.comments.append({
            "id": preview["reservation_slot_id"], "body": "competing intent",
            "createdAt": "race", "updatedAt": "race",
        })
        with self.assertRaisesRegex(RootTransitionError, "frontier_drift|slot_conflict"):
            transport.apply(
                operation="plan-url", target=MAIN,
                expected_snapshot_sha256=preview["expected_snapshot_sha256"],
                expected_frontier_sha256=preview["expected_frontier_sha256"],
            )
        self.assertFalse(any("WorkstreamRootTransition(" in query for query, _ in fake.calls))


if __name__ == "__main__":
    unittest.main()
