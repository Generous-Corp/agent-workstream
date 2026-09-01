#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy
import io
import sys
import unittest
from types import SimpleNamespace
from unittest import mock

from workstream_root_transition import (
    RootTransitionError, RootTransitionTransport, main as root_main,
    validate_active_locator_authorization,
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
ACTIVE_PINNED = (
    "https://github.com/example/private-plans/blob/" + "b" * 40 + "/plan.md"
)
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
LOCATOR_AUTHORIZATION = {
    "schema_version": 1,
    "authorization_kind": "active_generation_plan_locator",
    "source": {"identity": ACTIVE_PINNED, "sha256": "f" * 64},
    "generation": {
        "plan_revision": "f" * 64,
        "description_plan_revision": "f" * 64,
        "transition_tip_event_id": "wsp_" + "1" * 32,
        "activation_epoch": 1,
        "authority_origin": "generation_transition",
    },
    "projection": {
        "revision": 4,
        "frontier_event_id": "wsp_" + "2" * 32,
        "events_sha256": "3" * 64,
        "source_event_id": "wsp_" + "4" * 32,
        "source_value_sha256": "5" * 64,
    },
}


class FakeClient:
    def __init__(self):
        self.token = TOKEN
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
        self.comment_updates_root = True
        self.lose_issue_update_response_once = False

    def root(self):
        return {
            "id": ROOT_ID, "identifier": self.token, "title": "Root",
            "description": self.description, "url": f"https://linear.test/{self.token}",
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
                "id": ROOT_ID, "identifier": self.token,
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
                "createdAt": "reservation-created",
                "updatedAt": "reservation-root-clock",
            }
            self.comments.append(existing)
            if self.comment_updates_root:
                self.updated_at = "reservation-root-clock"
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
            if self.lose_issue_update_response_once:
                self.lose_issue_update_response_once = False
                raise LinearTransportError("response lost after accepted issue update")
            return {"issueUpdate": {"success": True, "issue": self.root()}}
        raise AssertionError(query)


class RootTransitionTests(unittest.TestCase):

    def test_apply_missing_fences_refuses_before_auth_or_network(self):
        for argv in (
            [
                "workstream_root_transition.py", "plan-url", TOKEN,
                "--to", MAIN, "--operator-contract", "review.json",
                "--plan-source", "PLAN.md", "--apply",
            ],
            [
                "workstream_root_transition.py", "reconcile-plan-url", TOKEN,
                "--to", ACTIVE_PINNED, "--apply",
            ],
        ):
            stderr = io.StringIO()
            with self.subTest(command=argv[1]), mock.patch.object(
                sys, "argv", argv,
            ), mock.patch(
                "workstream_root_transition.resolve_linear_route",
                side_effect=AssertionError("invalid local CLI reached auth/network"),
            ), mock.patch.object(sys, "stderr", stderr):
                self.assertEqual(root_main(), 2)
            self.assertIn(
                "root_transition_expected_fence_invalid", stderr.getvalue(),
            )

    def transport(self, fake):
        return RootTransitionTransport(
            fake, token=TOKEN, authority=AUTHORITY,
            operator_authorization=OPERATOR_AUTHORIZATION,
        )

    def locator_transport(self, fake):
        return RootTransitionTransport(
            fake, token=TOKEN, authority=AUTHORITY,
            operator_authorization=LOCATOR_AUTHORIZATION,
        )

    def test_locator_cli_normalizes_authenticated_plan_payload_source(self):
        argv = [
            "workstream_root_transition.py", "reconcile-plan-url", TOKEN,
            "--to", ACTIVE_PINNED,
        ]
        transport = mock.Mock()
        transport.preview.return_value = {"apply": False}
        stdout = io.StringIO()
        with mock.patch.object(sys, "argv", argv), mock.patch.object(
            sys, "stdout", stdout,
        ), mock.patch(
            "workstream_root_transition.resolve_linear_route",
            return_value=({
                key: AUTHORITY[key]
                for key in ("workspace_id", "team_id", "project_id")
            }, {}),
        ), mock.patch(
            "workstream_root_transition.load_linear_api_key", return_value="key",
        ), mock.patch(
            "workstream_root_transition.HttpGraphQLClient",
        ) as client_type, mock.patch(
            "workstream_root_transition.bootstrap_linear_route",
            return_value=AUTHORITY,
        ), mock.patch(
            "workstream_root_transition.plan_payload",
            return_value={"source": {
                "identity": ACTIVE_PINNED, "sha256": "f" * 64, "bytes": 42,
            }},
        ), mock.patch(
            "workstream_root_transition.RootTransitionTransport",
            return_value=transport,
        ) as transport_type, mock.patch(
            "workstream_root_transition.validate_active_locator_authorization",
            return_value=LOCATOR_AUTHORIZATION,
        ) as validate:
            self.assertEqual(root_main(), 0)
            validator = transport_type.call_args.kwargs["operator_validator"]
            self.assertEqual(validator({"root": {}}, []), LOCATOR_AUTHORIZATION)
        validate.assert_called_once_with(
            source={"identity": ACTIVE_PINNED, "sha256": "f" * 64},
            token=TOKEN, authority=AUTHORITY, comments=[], graph={"root": {}},
        )
        transport.preview.assert_called_once_with(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
        )
        self.assertTrue(client_type.called)

    def test_locator_reconcile_apply_replay_and_fresh_noop(self):
        fake = FakeClient()
        original_root = fake.root()
        original_comments = deepcopy(fake.comments)
        transport = self.locator_transport(fake)
        preview = transport.preview(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
        )
        self.assertFalse(preview["apply"])
        result = transport.apply(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
            expected_snapshot_sha256=preview["expected_snapshot_sha256"],
            expected_frontier_sha256=preview["expected_frontier_sha256"],
            expected_intent_sha256=preview["intent_sha256"],
        )
        self.assertEqual(result["result"], "applied_or_exact_replay")
        self.assertEqual(fake.description, original_root["description"].replace(
            PINNED, ACTIVE_PINNED,
        ))
        self.assertEqual(len(fake.comments), len(original_comments) + 1)
        writes = sum(
            "WorkstreamRootTransition(" in query for query, _ in fake.calls
        )
        replay = transport.apply(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
            expected_snapshot_sha256=preview["expected_snapshot_sha256"],
            expected_frontier_sha256=preview["expected_frontier_sha256"],
            expected_intent_sha256=preview["intent_sha256"],
        )
        self.assertEqual(replay["result"], "applied_or_exact_replay")
        self.assertEqual(sum(
            "WorkstreamRootTransition(" in query for query, _ in fake.calls
        ), writes)
        self.assertEqual(len(fake.comments), len(original_comments) + 1)

        current = FakeClient()
        current.description = current.description.replace(PINNED, ACTIVE_PINNED)
        current_transport = self.locator_transport(current)
        current_preview = current_transport.preview(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
        )
        comments_before = deepcopy(current.comments)
        noop = current_transport.apply(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
            expected_snapshot_sha256=current_preview["expected_snapshot_sha256"],
            expected_frontier_sha256=current_preview["expected_frontier_sha256"],
            expected_intent_sha256=current_preview["intent_sha256"],
        )
        self.assertEqual(noop["result"], "already_current_noop")
        self.assertEqual(current.comments, comments_before)
        self.assertFalse(any(
            "WorkstreamRootTransition(" in query for query, _ in current.calls
        ))

    def test_locator_reconcile_tolerates_only_its_own_updated_at_drift(self):
        def production_transport(fake):
            fake.token = "GEN-37"
            fake.comment_updates_root = True
            return RootTransitionTransport(
                fake, token="GEN-37", authority=AUTHORITY,
                operator_authorization=LOCATOR_AUTHORIZATION,
            )

        applied = FakeClient()
        transport = production_transport(applied)
        preview = transport.preview(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
        )
        result = transport.apply(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
            expected_snapshot_sha256=preview["expected_snapshot_sha256"],
            expected_frontier_sha256=preview["expected_frontier_sha256"],
            expected_intent_sha256=preview["intent_sha256"],
        )
        self.assertEqual(result["result"], "applied_or_exact_replay")
        self.assertIn("Canonical plan: " + ACTIVE_PINNED, applied.description)
        self.assertEqual(len([
            item for item in applied.comments
            if item["id"] == preview["reservation_slot_id"]
        ]), 1)

        response_lost = FakeClient()
        replay_transport = production_transport(response_lost)
        replay_preview = replay_transport.preview(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
        )
        replay_args = {
            "operation": "reconcile-plan-url", "target": ACTIVE_PINNED,
            "expected_snapshot_sha256": replay_preview["expected_snapshot_sha256"],
            "expected_frontier_sha256": replay_preview["expected_frontier_sha256"],
            "expected_intent_sha256": replay_preview["intent_sha256"],
        }
        response_lost.lose_issue_update_response_once = True
        with self.assertRaisesRegex(LinearTransportError, "response lost"):
            replay_transport.apply(**replay_args)
        issue_writes = sum(
            "WorkstreamRootTransition(" in query for query, _ in response_lost.calls
        )
        replay = replay_transport.apply(**replay_args)
        self.assertEqual(replay["result"], "applied_or_exact_replay")
        self.assertEqual(sum(
            "WorkstreamRootTransition(" in query for query, _ in response_lost.calls
        ), issue_writes)
        self.assertEqual(len([
            item for item in response_lost.comments
            if item["id"] == replay_preview["reservation_slot_id"]
        ]), 1)

        concurrent = FakeClient()
        concurrent_transport = production_transport(concurrent)
        concurrent_preview = concurrent_transport.preview(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
        )

        def external_drift():
            concurrent.description += "\nConcurrent external edit."
            concurrent.updated_at = "external"

        concurrent_transport.after_reservation_created = external_drift
        with self.assertRaisesRegex(
            RootTransitionError, "root_transition_prewrite_drift",
        ):
            concurrent_transport.apply(
                operation="reconcile-plan-url", target=ACTIVE_PINNED,
                expected_snapshot_sha256=concurrent_preview["expected_snapshot_sha256"],
                expected_frontier_sha256=concurrent_preview["expected_frontier_sha256"],
                expected_intent_sha256=concurrent_preview["intent_sha256"],
            )
        self.assertFalse(any(
            "WorkstreamRootTransition(" in query for query, _ in concurrent.calls
        ))
        self.assertEqual(len([
            item for item in concurrent.comments
            if item["id"] == concurrent_preview["reservation_slot_id"]
        ]), 1)

        clock_only = FakeClient()
        clock_transport = production_transport(clock_only)
        clock_preview = clock_transport.preview(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
        )
        clock_transport.after_reservation_created = lambda: setattr(
            clock_only, "updated_at", "external-clock-only",
        )
        with self.assertRaisesRegex(
            RootTransitionError, "root_transition_prewrite_drift",
        ):
            clock_transport.apply(
                operation="reconcile-plan-url", target=ACTIVE_PINNED,
                expected_snapshot_sha256=clock_preview["expected_snapshot_sha256"],
                expected_frontier_sha256=clock_preview["expected_frontier_sha256"],
                expected_intent_sha256=clock_preview["intent_sha256"],
            )
        self.assertFalse(any(
            "WorkstreamRootTransition(" in query for query, _ in clock_only.calls
        ))

    def test_locator_reconcile_preserves_one_markdown_link(self):
        fake = FakeClient()
        fake.description = fake.description.replace(
            "Canonical plan: " + PINNED,
            f"Canonical plan: [{PINNED}](<{PINNED}>)",
        )
        transport = self.locator_transport(fake)
        preview = transport.preview(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
        )
        result = transport.apply(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
            expected_snapshot_sha256=preview["expected_snapshot_sha256"],
            expected_frontier_sha256=preview["expected_frontier_sha256"],
            expected_intent_sha256=preview["intent_sha256"],
        )
        self.assertEqual(result["result"], "applied_or_exact_replay")
        self.assertIn(
            f"Canonical plan: [{ACTIVE_PINNED}](<{ACTIVE_PINNED}>)",
            fake.description,
        )
        self.assertNotIn(PINNED, fake.description)
        writes = sum(
            "WorkstreamRootTransition(" in query for query, _ in fake.calls
        )
        comments = len(fake.comments)
        replay = transport.apply(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
            expected_snapshot_sha256=preview["expected_snapshot_sha256"],
            expected_frontier_sha256=preview["expected_frontier_sha256"],
            expected_intent_sha256=preview["intent_sha256"],
        )
        self.assertEqual(replay["result"], "applied_or_exact_replay")
        self.assertEqual(sum(
            "WorkstreamRootTransition(" in query for query, _ in fake.calls
        ), writes)
        self.assertEqual(len(fake.comments), comments)

        current = FakeClient()
        current.description = current.description.replace(
            "Canonical plan: " + PINNED,
            f"Canonical plan: [{ACTIVE_PINNED}](<{ACTIVE_PINNED}>)",
        )
        current_transport = self.locator_transport(current)
        current_preview = current_transport.preview(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
        )
        current_comments = deepcopy(current.comments)
        noop = current_transport.apply(
            operation="reconcile-plan-url", target=ACTIVE_PINNED,
            expected_snapshot_sha256=current_preview["expected_snapshot_sha256"],
            expected_frontier_sha256=current_preview["expected_frontier_sha256"],
            expected_intent_sha256=current_preview["intent_sha256"],
        )
        self.assertEqual(noop["result"], "already_current_noop")
        self.assertEqual(current.comments, current_comments)

    def test_candidate_plan_transition_preserves_one_markdown_link(self):
        fake = FakeClient()
        fake.description = fake.description.replace(
            "Canonical plan: " + PINNED,
            f"Canonical plan: [{PINNED}]({PINNED})",
        )
        transport = self.transport(fake)
        preview = transport.preview(operation="plan-url", target=MAIN)
        result = transport.apply(
            operation="plan-url", target=MAIN,
            expected_snapshot_sha256=preview["expected_snapshot_sha256"],
            expected_frontier_sha256=preview["expected_frontier_sha256"],
            expected_intent_sha256=preview["intent_sha256"],
        )
        self.assertEqual(result["result"], "applied_or_exact_replay")
        self.assertIn(f"Canonical plan: [{MAIN}]({MAIN})", fake.description)
        self.assertNotIn(PINNED, fake.description)

    def test_locator_reconcile_refuses_duplicate_labeled_lines(self):
        fake = FakeClient()
        fake.description += "\nCanonical plan: " + PINNED
        with self.assertRaisesRegex(
            RootTransitionError, "canonical_plan_url_occurrence_ambiguous",
        ):
            self.locator_transport(fake).preview(
                operation="reconcile-plan-url", target=ACTIVE_PINNED,
            )

    def test_locator_reconcile_refuses_third_same_line_occurrence(self):
        fake = FakeClient()
        fake.description = fake.description.replace(
            "Canonical plan: " + PINNED,
            f"Canonical plan: [{PINNED}](<{PINNED}>) {PINNED}",
        )
        with self.assertRaisesRegex(
            RootTransitionError, "canonical_plan_url_occurrence_ambiguous",
        ):
            self.locator_transport(fake).preview(
                operation="reconcile-plan-url", target=ACTIVE_PINNED,
            )

    def test_locator_reconcile_refuses_authority_or_target_substitution(self):
        fake = FakeClient()
        with self.assertRaisesRegex(
            RootTransitionError, "locator_authorization_required",
        ):
            self.transport(fake).preview(
                operation="reconcile-plan-url", target=ACTIVE_PINNED,
            )
        with self.assertRaisesRegex(
            RootTransitionError, "candidate_authorization_required",
        ):
            self.locator_transport(fake).preview(
                operation="plan-url", target=ACTIVE_PINNED,
            )
        other = (
            "https://github.com/example/private-plans/blob/"
            + "c" * 40 + "/other.md"
        )
        with self.assertRaisesRegex(
            RootTransitionError, "target_not_active_source|different_plan_document",
        ):
            self.locator_transport(fake).preview(
                operation="reconcile-plan-url", target=other,
            )
        for description, error in (
            ("No canonical plan", "canonical_plan_source_missing"),
            (f"Canonical plan: {PINNED} {ACTIVE_PINNED}",
             "canonical_plan_source_ambiguous"),
        ):
            malformed = FakeClient()
            malformed.description = description
            with self.subTest(error=error), self.assertRaisesRegex(
                ValueError, error,
            ):
                self.locator_transport(malformed).preview(
                    operation="reconcile-plan-url", target=ACTIVE_PINNED,
                )

    def test_active_locator_authorization_binds_exact_generation_and_source(self):
        source = deepcopy(LOCATOR_AUTHORIZATION["source"])
        selected = deepcopy(LOCATOR_AUTHORIZATION["generation"])
        source_event = {
            "kind": "source", "key": "root",
            "event_id": "wsp_" + "4" * 32,
            "value": deepcopy(source),
        }
        frontier = {
            "kind": "disposition", "key": "root",
            "event_id": "wsp_" + "2" * 32, "value": {},
        }
        state = SimpleNamespace(
            revision=2, events=(source_event, frontier),
            snapshot={"source": deepcopy(source)},
        )
        graph = {"root": {"plan_revision": "e" * 64}}
        with mock.patch(
            "workstream_root_transition.select_plan_generation",
            return_value=selected,
        ), mock.patch(
            "workstream_root_transition.reduce_projection_comments",
            return_value=state,
        ), mock.patch(
            "workstream_generation.assert_no_pending_generation_reservation",
        ):
            result = validate_active_locator_authorization(
                source=source, token=TOKEN, authority=AUTHORITY,
                comments=[], graph=graph,
            )
        self.assertEqual(result["source"], source)
        self.assertEqual(result["generation"], selected)
        self.assertEqual(result["projection"]["revision"], 2)
        self.assertEqual(
            result["projection"]["source_event_id"], source_event["event_id"],
        )

        for changed, error in (
            ({**selected, "authority_origin": "legacy_description"},
             "structured_active_generation_required"),
            ({**selected, "plan_revision": "0" * 64},
             "target_not_active_source"),
        ):
            with self.subTest(error=error), mock.patch(
                "workstream_root_transition.select_plan_generation",
                return_value=changed,
            ), self.assertRaisesRegex(RootTransitionError, error):
                validate_active_locator_authorization(
                    source=source, token=TOKEN, authority=AUTHORITY,
                    comments=[], graph=graph,
                )
        mismatched_state = SimpleNamespace(
            revision=2,
            events=({**source_event, "value": {
                "identity": ACTIVE_PINNED, "sha256": "0" * 64,
            }}, frontier),
            snapshot={"source": {
                "identity": ACTIVE_PINNED, "sha256": "0" * 64,
            }},
        )
        with mock.patch(
            "workstream_root_transition.select_plan_generation",
            return_value=selected,
        ), mock.patch(
            "workstream_root_transition.reduce_projection_comments",
            return_value=mismatched_state,
        ), mock.patch(
            "workstream_generation.assert_no_pending_generation_reservation",
        ), self.assertRaisesRegex(
            RootTransitionError, "active_source_mismatch",
        ):
            validate_active_locator_authorization(
                source=source, token=TOKEN, authority=AUTHORITY,
                comments=[], graph=graph,
            )

        with mock.patch(
            "workstream_root_transition.select_plan_generation",
            return_value=selected,
        ), mock.patch(
            "workstream_generation.assert_no_pending_generation_reservation",
            side_effect=LinearTransportError("generation_boundary_reserved:test"),
        ), self.assertRaisesRegex(
            RootTransitionError, "generation_boundary_reserved",
        ):
            validate_active_locator_authorization(
                source=source, token=TOKEN, authority=AUTHORITY,
                comments=[], graph=graph,
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
