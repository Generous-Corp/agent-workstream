#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy
import io
import sys
import unittest
from unittest import mock

from workstream_root_transition import (
    RootTransitionError, RootTransitionTransport, main as root_main,
)
from workstream_linear import LinearTransportError


TOKEN = "GEN-14"
ROOT_ID = "33333333-3333-4333-8333-333333333333"
AUTHORITY = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
    "root_issue_id": ROOT_ID,
}
PINNED = "https://github.com/example/private-plans/blob/" + "a" * 40 + "/plan.md"
MAIN = "https://github.com/example/private-plans/blob/main/plan.md"
STARTED_STATE = "44444444-4444-4444-8444-444444444444"
OPERATOR_AUTHORIZATION = {
    "schema_version": 1, "contract_sha256": "1" * 64,
    "source": {"identity": MAIN, "sha256": "f" * 64},
    "generation": {
        "from_plan_revision": "e" * 64, "target_plan_revision": "f" * 64,
        "activation_epoch": 1, "previous_control_event_id": None,
    },
    "native_transition": {
        "operation": "reopen",
        "target_state": {
            "id": STARTED_STATE, "name": "In Progress", "type": "started",
            "team_id": "team",
        },
    },
    "retirement_sha256": "2" * 64, "frontiers_sha256": "3" * 64,
}


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
        self.after_issue_update = None
        self.before_comment_create = None

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
        if "WorkstreamResumeRoot" in query:
            return {"issue": {**self.root(), "children": {
                "nodes": [], "pageInfo": {
                    "hasNextPage": False, "endCursor": None,
                },
            }}}
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
            if self.before_comment_create is not None:
                callback, self.before_comment_create = self.before_comment_create, None
                callback(value)
            existing = next((item for item in self.comments if item["id"] == value["id"]), None)
            if existing is not None:
                raise LinearTransportError("comment id collision")
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
            if self.after_issue_update is not None:
                self.after_issue_update()
            return {"issueUpdate": {"success": True, "issue": self.root()}}
        raise AssertionError(query)


class RootTransitionTests(unittest.TestCase):

    def test_apply_missing_fences_refuses_before_auth_or_network(self):
        argv = [
            "workstream_root_transition.py", "plan-url", TOKEN,
            "--to", MAIN, "--operator-contract", "review.json",
            "--plan-source", "PLAN.md", "--apply",
        ]
        stderr = io.StringIO()
        with mock.patch.object(sys, "argv", argv), mock.patch(
            "workstream_root_transition.resolve_linear_route",
            side_effect=AssertionError("invalid local CLI reached auth/network"),
        ), mock.patch.object(sys, "stderr", stderr):
            self.assertEqual(root_main(), 2)
        self.assertIn("root_transition_expected_fence_invalid", stderr.getvalue())
    def transport(self, fake):
        return RootTransitionTransport(
            fake, token=TOKEN, authority=AUTHORITY,
            operator_authorization=OPERATOR_AUTHORIZATION,
        )

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
            expected_intent_sha256=preview["intent_sha256"],
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
            "expected_intent_sha256": preview["intent_sha256"],
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
        with self.assertRaisesRegex(
            RootTransitionError, "different_document|not_authorized_by_candidate"
        ):
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
                    expected_intent_sha256=preview["intent_sha256"],
                )
            self.assertFalse(any("WorkstreamRootTransition(" in query for query, _ in fake.calls))

    def test_live_operator_recomputation_refuses_after_validation_drift_zero_write(self):
        fake = FakeClient()
        observed_frontiers = []

        def validate(_snapshot, comments):
            observed_frontiers.append([item["id"] for item in comments])
            if any(item.get("id") == "post-validation-drift" for item in comments):
                raise RootTransitionError("operator_contract_live_state_drift")
            return OPERATOR_AUTHORIZATION

        transport = RootTransitionTransport(
            fake, token=TOKEN, authority=AUTHORITY,
            operator_validator=validate,
        )
        preview = transport.preview(operation="plan-url", target=MAIN)
        self.assertEqual(observed_frontiers, [["ordinary"]])
        fake.comments.append({
            "id": "post-validation-drift", "body": "new projection material",
            "createdAt": "t1", "updatedAt": "t1",
        })
        calls_before = len(fake.calls)
        with self.assertRaisesRegex(
            RootTransitionError, "operator_contract_live_state_drift"
        ):
            transport.apply(
                operation="plan-url", target=MAIN,
                expected_snapshot_sha256=preview["expected_snapshot_sha256"],
                expected_frontier_sha256=preview["expected_frontier_sha256"],
                expected_intent_sha256=preview["intent_sha256"],
            )
        new_calls = fake.calls[calls_before:]
        self.assertFalse(any(
            "WorkstreamDeltaCommentCreate" in query
            or "WorkstreamRootTransition(" in query
            for query, _ in new_calls
        ))

        prewrite = FakeClient()

        def validate_prewrite(_snapshot, comments):
            if any(item.get("id") == "prewrite-drift" for item in comments):
                raise RootTransitionError("operator_contract_prewrite_drift")
            return OPERATOR_AUTHORIZATION

        def drift_after_reservation():
            prewrite.comments.append({
                "id": "prewrite-drift", "body": "new projection material",
                "createdAt": "t2", "updatedAt": "t2",
            })

        transport = RootTransitionTransport(
            prewrite, token=TOKEN, authority=AUTHORITY,
            operator_validator=validate_prewrite,
            after_reservation_created=drift_after_reservation,
        )
        preview = transport.preview(operation="plan-url", target=MAIN)
        with self.assertRaisesRegex(
            RootTransitionError, "operator_contract_prewrite_drift"
        ):
            transport.apply(
                operation="plan-url", target=MAIN,
                expected_snapshot_sha256=preview["expected_snapshot_sha256"],
                expected_frontier_sha256=preview["expected_frontier_sha256"],
                expected_intent_sha256=preview["intent_sha256"],
            )
        self.assertTrue(any(
            "WorkstreamDeltaCommentCreate" in query for query, _ in prewrite.calls
        ))
        self.assertFalse(any(
            "WorkstreamRootTransition(" in query for query, _ in prewrite.calls
        ))

    def test_reopen_requires_terminal_root_and_reviewed_started_state(self):
        fake = FakeClient()
        transport = self.transport(fake)
        unreviewed_started_state = "55555555-5555-4555-8555-555555555555"
        with self.assertRaisesRegex(
            RootTransitionError, "reopen_state_not_authorized_by_candidate"
        ):
            transport.preview(operation="reopen", target=unreviewed_started_state)
        self.assertFalse(any(
            "WorkstreamRootTransitionState" in query for query, _ in fake.calls
        ))
        preview = transport.preview(operation="reopen", target=STARTED_STATE)
        result = transport.apply(
            operation="reopen", target=STARTED_STATE,
            expected_snapshot_sha256=preview["expected_snapshot_sha256"],
            expected_frontier_sha256=preview["expected_frontier_sha256"],
            expected_intent_sha256=preview["intent_sha256"],
        )
        self.assertEqual(result["final_root"]["state"], {
            "id": STARTED_STATE, "name": "In Progress", "type": "started",
        })
        writes = sum("WorkstreamRootTransition(" in query for query, _ in fake.calls)
        transport.apply(
            operation="reopen", target=STARTED_STATE,
            expected_snapshot_sha256=preview["expected_snapshot_sha256"],
            expected_frontier_sha256=preview["expected_frontier_sha256"],
            expected_intent_sha256=preview["intent_sha256"],
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
                expected_intent_sha256=preview["intent_sha256"],
            )
        self.assertFalse(any("WorkstreamRootTransition(" in query for query, _ in fake.calls))

    def test_operation_and_target_substitution_refuse_before_reservation(self):
        fake = FakeClient()
        transport = self.transport(fake)
        preview = transport.preview(operation="plan-url", target=MAIN)
        for operation, target in (
            ("reopen", STARTED_STATE),
            ("plan-url", "https://github.com/EXAMPLE/private-plans/blob/main/plan.md"),
        ):
            with self.subTest(operation=operation), self.assertRaisesRegex(
                RootTransitionError, "intent_mismatch|not_authorized_by_candidate"
            ):
                transport.apply(
                    operation=operation, target=target,
                    expected_snapshot_sha256=preview["expected_snapshot_sha256"],
                    expected_frontier_sha256=preview["expected_frontier_sha256"],
                    expected_intent_sha256=preview["intent_sha256"],
                )
        self.assertFalse(any("CommentCreate" in query for query, _ in fake.calls))
        self.assertFalse(any("WorkstreamRootTransition(" in query for query, _ in fake.calls))

    def test_crash_after_create_leaves_explicit_pending_and_no_automatic_owner(self):
        fake = FakeClient()
        preview = self.transport(fake).preview(operation="plan-url", target=MAIN)
        args = {
            "operation": "plan-url", "target": MAIN,
            "expected_snapshot_sha256": preview["expected_snapshot_sha256"],
            "expected_frontier_sha256": preview["expected_frontier_sha256"],
            "expected_intent_sha256": preview["intent_sha256"],
        }

        def crash():
            raise RuntimeError("process died")

        with self.assertRaisesRegex(RuntimeError, "process died"):
            RootTransitionTransport(
                fake, token=TOKEN, authority=AUTHORITY,
                operator_authorization=OPERATOR_AUTHORIZATION,
                after_reservation_created=crash,
            ).apply(**args)
        with self.assertRaisesRegex(
            RootTransitionError, "reservation_pending_review_new_preview_required"
        ):
            self.transport(fake).apply(**args)
        self.assertFalse(any("WorkstreamRootTransition(" in query for query, _ in fake.calls))

    def test_two_client_create_race_allows_only_proven_creator_to_update(self):
        fake = FakeClient()
        transport = self.transport(fake)
        preview = transport.preview(operation="plan-url", target=MAIN)
        args = {
            "operation": "plan-url", "target": MAIN,
            "expected_snapshot_sha256": preview["expected_snapshot_sha256"],
            "expected_frontier_sha256": preview["expected_frontier_sha256"],
            "expected_intent_sha256": preview["intent_sha256"],
        }

        def competing_create(value):
            fake.comments.append({
                "id": value["id"], "body": value["body"],
                "createdAt": "winner", "updatedAt": "winner",
            })

        fake.before_comment_create = competing_create
        with self.assertRaisesRegex(
            RootTransitionError, "reservation_not_owned_by_this_process"
        ):
            transport.apply(**args)
        self.assertFalse(any("WorkstreamRootTransition(" in query for query, _ in fake.calls))

    def test_reservation_deletion_or_replacement_refuses_at_prewrite(self):
        for mode in ("delete", "replace"):
            fake = FakeClient()
            preview = self.transport(fake).preview(operation="plan-url", target=MAIN)

            def corrupt():
                reservation = next(
                    item for item in fake.comments
                    if item["id"] == preview["reservation_slot_id"]
                )
                if mode == "delete":
                    fake.comments.remove(reservation)
                else:
                    reservation["body"] = "replaced"

            transport = RootTransitionTransport(
                fake, token=TOKEN, authority=AUTHORITY,
                operator_authorization=OPERATOR_AUTHORIZATION,
                after_reservation_created=corrupt,
            )
            with self.subTest(mode=mode), self.assertRaisesRegex(
                RootTransitionError, "reservation_changed_or_missing"
            ):
                transport.apply(
                    operation="plan-url", target=MAIN,
                    expected_snapshot_sha256=preview["expected_snapshot_sha256"],
                    expected_frontier_sha256=preview["expected_frontier_sha256"],
                    expected_intent_sha256=preview["intent_sha256"],
                )
            self.assertFalse(any("WorkstreamRootTransition(" in query for query, _ in fake.calls))

    def test_reservation_deletion_or_replacement_refuses_at_postread(self):
        for mode in ("delete", "replace"):
            fake = FakeClient()
            preview = self.transport(fake).preview(operation="plan-url", target=MAIN)

            def corrupt():
                reservation = next(
                    item for item in fake.comments
                    if item["id"] == preview["reservation_slot_id"]
                )
                if mode == "delete":
                    fake.comments.remove(reservation)
                else:
                    reservation["body"] = "replaced"

            fake.after_issue_update = corrupt
            with self.subTest(mode=mode), self.assertRaisesRegex(
                RootTransitionError, "reservation_changed_or_missing"
            ):
                self.transport(fake).apply(
                    operation="plan-url", target=MAIN,
                    expected_snapshot_sha256=preview["expected_snapshot_sha256"],
                    expected_frontier_sha256=preview["expected_frontier_sha256"],
                    expected_intent_sha256=preview["intent_sha256"],
                )
            self.assertTrue(any("WorkstreamRootTransition(" in query for query, _ in fake.calls))


if __name__ == "__main__":
    unittest.main()
