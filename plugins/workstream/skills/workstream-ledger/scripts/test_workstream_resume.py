import copy
import base64
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from workstream_checkpoint import build_checkpoint
from workstream_delta import Delta
from workstream_linear_checkpoints import encode_checkpoint_comment
from workstream_linear_events import encode_event_comment
import workstream_linear_events as linear_events_module
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, projection_slot_id,
)
import workstream_resume as MODULE


class ResumeTests(unittest.TestCase):
    def snapshot(self):
        return {"root": {"identifier": "GEN-37", "url": "https://linear/GEN-37", "plan_revision": "sha",
                          "revision": 7, "status": "In Progress", "next_action": "resume"},
                "children": [{"identifier": "GEN-38", "title": "intake", "status": "In Progress", "next_action": "adapter"},
                             {"identifier": "GEN-39", "title": "delta", "status": "Done"}],
                "decisions": [{"id": "D1", "status": "accepted"}], "provenance": [{"machine": "M3"}]}

    def live_snapshot(self, snapshot, route):
        snapshot["root"].update({
            "id": route["root_issue_id"], "title": snapshot["root"].get("title", "Root"),
            "description": snapshot["root"].get("description", "Plan revision: sha"),
            "url": snapshot["root"].get("url", "https://linear/GEN-37"),
            "updatedAt": snapshot["root"].get("updatedAt", "now"), "parent": None,
            "team": {"id": route["team_id"],
                     "organization": {"id": route["workspace_id"]}},
            "project": {"id": route["project_id"]}, "assignee": None,
            "state": {"id": "started", "name": "In Progress", "type": "started"},
        })
        for index, child in enumerate(snapshot["children"]):
            child.update({
                "id": f"child-{index}",
                "url": f"https://linear/{child['identifier']}",
                "parent": {"id": route["root_issue_id"], "identifier": "GEN-37"},
                "team": {"id": route["team_id"],
                         "organization": {"id": route["workspace_id"]}},
                "project": {"id": route["project_id"]},
            })
        snapshot["child_comments"] = {
            child["identifier"]: [] for child in snapshot["children"]
            if str(child.get("status", "")).lower() not in MODULE.TERMINAL
        }
        return snapshot

    def test_repair_graph_frontier_binds_native_status_and_state_identity(self):
        snapshot = self.snapshot()
        snapshot["root"].update({
            "id": "root", "title": "Repair", "description": "full description",
            "next_action": "continue", "updatedAt": "2026-08-30T00:00:00Z",
            "revision": 7, "plan_revision": "a" * 64,
            "status": "In Progress", "status_type": "started",
            "state_id": "state-1", "state": {
                "id": "state-1", "name": "In Progress", "type": "started",
            },
        })
        baseline = MODULE._issue_graph_repair_frontier(snapshot, [], {})
        self.assertEqual(baseline["issues"]["root"]["state_id"], "state-1")
        for field, value in (
            ("status", "Blocked"), ("status_type", "canceled"),
            ("state_id", "state-2"),
            ("id", "root-2"), ("title", "Changed title"),
            ("description", "changed description"),
            ("next_action", "changed action"), ("revision", 8),
            ("plan_revision", "b" * 64),
            ("updatedAt", "2026-08-30T00:01:00Z"),
        ):
            changed = copy.deepcopy(snapshot)
            changed["root"][field] = value
            self.assertNotEqual(
                MODULE._issue_graph_repair_frontier(changed, [], {})["sha256"],
                baseline["sha256"], field,
            )
        for field, value in (
            ("id", "nested-state-2"), ("name", "Blocked"),
            ("type", "canceled"),
        ):
            changed = copy.deepcopy(snapshot)
            changed["root"]["state"][field] = value
            self.assertNotEqual(
                MODULE._issue_graph_repair_frontier(changed, [], {})["sha256"],
                baseline["sha256"], "state." + field,
            )
        snapshot["children"][0].update({
            "description": "Plan revision: " + "a" * 64 + "\nRevision: 4",
            "plan_revision": "a" * 64, "revision": 4,
        })
        baseline = MODULE._issue_graph_repair_frontier(snapshot, [], {})
        for field, value in (
            ("description", "changed child description"),
            ("plan_revision", "b" * 64), ("revision", 5),
            ("title", "changed child title"),
        ):
            changed = copy.deepcopy(snapshot)
            changed["children"][0][field] = value
            self.assertNotEqual(
                MODULE._issue_graph_repair_frontier(changed, [], {})["sha256"],
                baseline["sha256"], field,
            )
        relation = [{"type": "related", "target": {
            "workspace_id": "workspace", "issue_id": "target",
            "identifier": "GEN-50",
        }}]
        targets = {"workspace:target": {
            "id": "target", "identifier": "GEN-50", "status": "Todo",
        }}
        relation_baseline = MODULE._issue_graph_repair_frontier(
            snapshot, relation, targets,
        )
        changed_targets = copy.deepcopy(targets)
        changed_targets["workspace:target"]["identifier"] = "GEN-51"
        self.assertNotEqual(
            MODULE._issue_graph_repair_frontier(
                snapshot, relation, changed_targets,
            )["sha256"],
            relation_baseline["sha256"],
        )

    def test_repaired_history_joins_before_and_after_source_authentication(self):
        from test_workstream_linear_events import LinearCommentEventAdapterTests
        comments, _payload, _checkpoint, _projection, _generation, route, source, _graph = (
            LinearCommentEventAdapterTests()._repair_fixture()
        )
        graph = self.snapshot()
        graph["children"][1]["status"] = "In Progress"
        graph["children"][1]["next_action"] = "continue"
        graph["root"].update({
            "plan_revision": "a" * 64,
            "id": route["root_issue_id"],
            "team": {"id": route["team_id"],
                     "organization": {"id": route["workspace_id"]}},
            "project": {"id": route["project_id"]},
        })
        graph = self.live_snapshot(graph, route)
        provisional = MODULE.add_material_history(
            graph, comments, "GEN-37", authenticated_route=route,
        )
        resumed = MODULE.add_material_history(
            graph, comments, "GEN-37", authenticated_route=route,
            authenticated_source=source,
        )
        self.assertEqual(provisional["root"]["next_action"], "new")
        self.assertEqual(resumed["root"]["next_action"], "new")
        compact = MODULE.compact_context(resumed, "GEN-37")
        full = MODULE.compact_context(resumed, "GEN-37", include_history=True)
        self.assertEqual(compact["next_action"], "new")
        self.assertEqual(compact["material_semantic_repair"]["count"], 2)
        self.assertEqual(len(full["material_semantic_repairs"]), 2)
        self.assertEqual(
            [item["event_id"] for item in full["raw_material_events"][:2]],
            ["flat-a", "flat-b"],
        )

    def test_expected_missing_closures_requires_authority_validation(self):
        with self.assertRaisesRegex(
            MODULE.ResumeError,
            "expected_missing_terminal_closures_requires_projection_authority",
        ):
            MODULE.validate_snapshot(
                self.snapshot(), "GEN-37",
                expected_missing_terminal_closures=frozenset({"GEN-70"}),
            )

    def test_compact_context_keeps_root_and_only_nonterminal_children(self):
        snapshot = self.snapshot()
        snapshot["children"][0]["description"] = "large redundant prose"
        snapshot["children"][0]["owner"] = "agent"
        snapshot["children"][0]["blocker"] = {"text": "waiting"}
        context = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(context["workstream_id"], "GEN-37")
        self.assertEqual([c["identifier"] for c in context["children"]], ["GEN-38"])
        self.assertNotIn("description", context["children"][0])
        self.assertEqual(
            context["children"][0]["description_summary"],
            {
                "bytes": len("large redundant prose"),
                "sha256": hashlib.sha256(b"large redundant prose").hexdigest(),
            },
        )
        self.assertEqual(context["children"][0]["owner"], "agent")
        self.assertEqual(context["children"][0]["blocker"], {"text": "waiting"})

        full = MODULE.compact_context(snapshot, "GEN-37", include_history=True)
        self.assertEqual(full["children"][0], snapshot["children"][0])

    def test_token_mismatch_fails_closed(self):
        with self.assertRaises(MODULE.ResumeError):
            MODULE.compact_context(self.snapshot(), "GEN-40")

    def test_extracts_one_token_from_title_or_natural_language(self):
        self.assertEqual(MODULE.extract_token("pulp continuity GEN-37 #3"), "GEN-37")
        self.assertEqual(MODULE.extract_token("Execute this: resume gen-37 now"), "GEN-37")
        self.assertEqual(MODULE.extract_token("GEN-37 then GEN-37"), "GEN-37")

    def test_extracts_non_gen_team_token(self):
        self.assertEqual(MODULE.extract_token("resume ops-37 now"), "OPS-37")

    def test_zero_or_multiple_distinct_tokens_fail_closed(self):
        with self.assertRaisesRegex(MODULE.ResumeError, "missing_workstream_token"):
            MODULE.extract_token("pulp continuity")
        with self.assertRaisesRegex(MODULE.ResumeError, "multiple_workstream_tokens"):
            MODULE.extract_token("GEN-37 conflicts with GEN-38")

    def test_full_title_resolves_the_same_snapshot(self):
        context = MODULE.compact_context(self.snapshot(), "pulp continuity GEN-37 #3")
        self.assertEqual(context["workstream_id"], "GEN-37")

    def test_missing_plan_revision_fails_closed(self):
        snapshot = self.snapshot(); del snapshot["root"]["plan_revision"]
        with self.assertRaises(MODULE.ResumeError):
            MODULE.validate_snapshot(snapshot, "GEN-37")

    def test_duplicate_child_fails_closed(self):
        snapshot = self.snapshot(); snapshot["children"].append(snapshot["children"][0].copy())
        with self.assertRaises(MODULE.ResumeError):
            MODULE.validate_snapshot(snapshot, "GEN-37")

    def test_nonterminal_child_without_next_action_fails_closed(self):
        snapshot = self.snapshot(); snapshot["children"][0].pop("next_action")
        with self.assertRaises(MODULE.ResumeError):
            MODULE.validate_snapshot(snapshot, "GEN-37")

    def test_child_material_log_and_checkpoint_override_stale_issue_prose(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        child = snapshot["children"][0]
        child["next_action"] = "stale issue-description action"
        events = [
            Delta(
                f"child-event-{index}", "GEN-38", "progress", "agent",
                {"next_action": f"event action {index}"} if index == 3 else {"step": index},
                index, f"2026-08-29T00:00:0{index}Z",
            )
            for index in range(4)
        ]
        checkpoint = build_checkpoint(
            workstream_id="GEN-38", boundary_id="child-current", root_revision=4,
            plan_revision="sha", before_status="In Progress",
            after_status="Blocked",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/child", "branch": "fix/child",
                    "head": "abc123",
                },
            },
            exact_head="abc123", evidence=[{"kind": "test", "id": "focused"}],
            blocker={"text": "await review"}, next_action="resume current child state",
        )
        snapshot["child_comments"]["GEN-38"] = [
            *[
                {"id": f"remote-{index}", "body": encode_event_comment(event)}
                for index, event in enumerate(events)
            ],
            {"id": "remote-checkpoint", "body": encode_checkpoint_comment(checkpoint)},
        ]

        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )
        context = MODULE.compact_context(enriched, "GEN-37")
        resumed = context["children"][0]

        self.assertNotIn("issue_next_action", resumed)
        self.assertEqual(resumed["next_action"], "resume current child state")
        self.assertEqual(resumed["blocker"], {"text": "await review"})
        self.assertEqual(resumed["material_event_revision"], 4)
        self.assertEqual(
            resumed["latest_checkpoint"]["checkpoint_event_id"], checkpoint["event_id"],
        )
        self.assertEqual(resumed["history"]["material_events"]["count"], 4)

    def test_child_full_history_retains_every_checkpoint_and_budgets_old_evidence(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        events = [
            Delta(
                f"child-history-event-{index}", "GEN-38", "progress", "agent",
                {"step": index}, index, f"2026-08-29T00:00:0{index}Z",
            )
            for index in range(2)
        ]
        execution = {
            "agent": "codex", "provider": "openai", "session_id": "session",
            "machine": "M5", "worktree": {
                "state": "safe", "path": "/repo/child", "branch": "fix/child",
                "head": "abc123",
            },
        }
        first = build_checkpoint(
            workstream_id="GEN-38", boundary_id="child-earlier", root_revision=1,
            plan_revision="sha", before_status="In Progress",
            after_status="In Progress", execution=execution, exact_head="abc123",
            evidence=[
                {"kind": "earlier-test", "id": str(index)} for index in range(20)
            ],
            blocker=None, next_action="older child action",
        )
        second = build_checkpoint(
            workstream_id="GEN-38", boundary_id="child-current", root_revision=2,
            plan_revision="sha", before_status="In Progress",
            after_status="In Progress", execution=execution, exact_head="abc123",
            evidence=[{"kind": "current-test", "id": "focused"}],
            blocker=None, next_action="current child action",
            predecessor_event_id=first["event_id"],
        )
        snapshot["child_comments"]["GEN-38"] = [
            *[
                {"id": f"child-event-{index}", "body": encode_event_comment(event)}
                for index, event in enumerate(events)
            ],
            {"id": "child-checkpoint-earlier", "body": encode_checkpoint_comment(first)},
            {"id": "child-checkpoint-current", "body": encode_checkpoint_comment(second)},
        ]
        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )

        bounded = MODULE.compact_context(
            enriched, "GEN-37", max_items=10,
        )["children"][0]
        self.assertNotIn("checkpoint_history", bounded)
        self.assertEqual(bounded["history"]["checkpoints"]["count"], 2)
        self.assertRegex(bounded["history"]["checkpoints"]["sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(bounded["next_action"], "current child action")
        self.assertEqual(bounded["latest_checkpoint"]["evidence"]["count"], 1)
        self.assertNotIn("items", bounded["latest_checkpoint"]["evidence"])

        full = MODULE.compact_context(
            enriched, "GEN-37", include_history=True, max_items=100,
        )["children"][0]
        self.assertEqual(
            [checkpoint["event_id"] for checkpoint in full["checkpoint_history"]],
            [first["event_id"], second["event_id"]],
        )
        self.assertEqual(full["checkpoint_history"][0]["evidence"], first["evidence"])
        truncated = json.loads(json.dumps(enriched))
        truncated["children"][0]["checkpoint_history"] = [
            truncated["children"][0]["checkpoint_history"][-1]
        ]
        with self.assertRaisesRegex(
            MODULE.ResumeError, "invalid_child_checkpoint_history:GEN-38"
        ):
            MODULE.compact_context(
                truncated, "GEN-37", include_history=True, max_items=100,
            )
        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(
                enriched, "GEN-37", include_history=True, max_items=10,
            )

    def test_child_stale_checkpoint_generation_must_be_a_complete_chain(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        truncated = build_checkpoint(
            workstream_id="GEN-38", boundary_id="stale-truncated",
            root_revision=2, plan_revision="old-sha",
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "old-session", "machine": "M3",
                "worktree": {"state": "unknown"},
            },
            exact_head=None, evidence=[], blocker=None,
            next_action="obsolete action",
            predecessor_event_id="missing-predecessor",
        )
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "stale-truncated", "body": encode_checkpoint_comment(truncated),
        }]

        with self.assertRaisesRegex(
            MODULE.ResumeError,
            "invalid_child_checkpoint_history:GEN-38:checkpoint_chain_truncated",
        ):
            MODULE.add_child_material_history(
                snapshot, snapshot["child_comments"], authenticated_route=route,
            )

    def test_legacy_string_child_blocker_is_preserved_as_structured_state(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        event = Delta(
            "legacy-child-blocker", "GEN-38", "material_boundary", "agent",
            {
                "boundary_id": "legacy-boundary",
                "changes": [{
                    "kind": "blocker",
                    "payload": {"blocker": "await exact-head checks"},
                }],
            },
            0, "2026-08-29T00:00:00Z",
        )
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "legacy-child-blocker-comment",
            "body": encode_event_comment(event),
        }]

        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )

        self.assertEqual(
            enriched["children"][0]["blocker"],
            {"text": "await exact-head checks"},
        )

    def test_malformed_child_material_log_refuses_resume(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "planted-malformed",
            "body": "<!-- workstream-delta:v1:not-valid-base64 -->",
        }]
        with self.assertRaisesRegex(MODULE.LinearEventError, "malformed_event_marker"):
            MODULE.add_child_material_history(
                snapshot, snapshot["child_comments"], authenticated_route=route,
            )

    def test_child_route_mismatch_is_separate_reconciliation_blocker(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        snapshot["children"][0]["project"] = {"id": "attacker-project"}
        before_action = snapshot["children"][0]["next_action"]
        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )
        child = enriched["children"][0]
        self.assertEqual(child["next_action"], before_action)
        self.assertEqual(child["reconciliation_blockers"], [{
            "kind": "native_child_cache_drift", "field": "project_id",
            "expected": "project", "observed": "attacker-project",
            "reconciliation_required": True,
        }])

    def test_child_collection_must_cover_every_nonterminal_child(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        with self.assertRaisesRegex(MODULE.ResumeError, "incomplete_child_comment_collection"):
            MODULE.add_child_material_history(
                snapshot, {}, authenticated_route=route,
            )

    def test_root_event_in_child_log_cannot_overwrite_child_state(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        root_event = Delta(
            "root-event", "GEN-37", "progress", "agent",
            {"next_action": "must remain root-only"}, 0, "2026-08-29T00:00:00Z",
        )
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "misrouted-root-event", "body": encode_event_comment(root_event),
        }]
        with self.assertRaisesRegex(MODULE.LinearEventError, "workstream_id_mismatch"):
            MODULE.add_child_material_history(
                snapshot, snapshot["child_comments"], authenticated_route=route,
            )

    def test_child_material_obligations_count_toward_context_budget(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        event = Delta(
            "child-requirement", "GEN-38", "requirement", "agent",
            {"requirement": "preserve this exact child obligation"},
            0, "2026-08-29T00:00:00Z",
        )
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "child-requirement-comment", "body": encode_event_comment(event),
        }]
        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )
        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(enriched, "GEN-37", max_items=3)

    def test_child_checkpoint_evidence_counts_toward_context_budget(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        checkpoint = build_checkpoint(
            workstream_id="GEN-38", boundary_id="child-evidence", root_revision=0,
            plan_revision="sha", before_status="In Progress",
            after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/child", "branch": "fix/child",
                    "head": "abc123",
                },
            },
            exact_head="abc123",
            evidence=[{"kind": "test", "id": str(index)} for index in range(20)],
            blocker=None, next_action="resume child",
        )
        snapshot["child_comments"]["GEN-38"] = [{
            "id": "child-checkpoint-comment",
            "body": encode_checkpoint_comment(checkpoint),
        }]
        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )

        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(enriched, "GEN-37", max_items=10)

    def test_child_without_comments_preserves_legacy_noop_surface(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.live_snapshot(self.snapshot(), route)
        expected_children = json.loads(json.dumps(snapshot["children"]))

        enriched = MODULE.add_child_material_history(
            snapshot, snapshot["child_comments"], authenticated_route=route,
        )

        self.assertEqual(enriched["children"], expected_children)
        self.assertNotIn("material_event_revision", enriched["children"][0])

    def test_nonterminal_root_without_next_action_fails_closed(self):
        snapshot = self.snapshot(); snapshot["root"].pop("next_action")
        with self.assertRaisesRegex(MODULE.ResumeError, "root missing next_action"):
            MODULE.validate_snapshot(snapshot, "GEN-37")

    def test_root_next_action_must_be_nonblank_text(self):
        for value in ({}, [], "   ", None):
            with self.subTest(value=value):
                snapshot = self.snapshot()
                snapshot["root"]["status"] = "Done"
                snapshot["root"]["next_action"] = value
                with self.assertRaisesRegex(MODULE.ResumeError, "invalid_root_next_action"):
                    MODULE.validate_snapshot(snapshot, "GEN-37")

    def test_context_budget_fails_loudly(self):
        with self.assertRaisesRegex(MODULE.ResumeError, "over_budget"):
            MODULE.compact_context(self.snapshot(), "GEN-37", max_bytes=10)

    def test_context_item_budget_fails_loudly(self):
        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(self.snapshot(), "GEN-37", max_items=2)

    def test_raw_transcripts_are_excluded_before_resume_budgeting(self):
        secret = "transcript-only-secret-" + "x" * 100_000
        snapshot = self.snapshot()
        snapshot["decisions"].append({
            "id": "D2", "status": "rejected", "rationale": "keep this",
            "raw_transcript": secret,
            "nested": {"transcript": secret, "summary": "keep summary"},
        })
        context = MODULE.compact_context(snapshot, "GEN-37", max_bytes=16 * 1024)
        encoded = json.dumps(context, sort_keys=True)
        self.assertNotIn("transcript-only-secret", encoded)
        self.assertNotIn("raw_transcript", encoded)
        self.assertEqual(context["decisions"][-1]["rationale"], "keep this")
        self.assertEqual(context["decisions"][-1]["nested"], {"summary": "keep summary"})

        snapshot["decisions"][-1]["rationale"] = secret
        with self.assertRaisesRegex(MODULE.ResumeError, "over_budget"):
            MODULE.compact_context(snapshot, "GEN-37", max_bytes=16 * 1024)

    def test_material_events_override_stale_issue_next_action(self):
        snapshot = self.snapshot()
        snapshot["root"]["revision"] = 91
        first = Delta(
            "event-a", "GEN-37", "requirement", "agent", {"next_action": "first"},
            0, "2026-08-21T00:00:00Z",
        )
        second = Delta(
            "event-b", "GEN-37", "progress", "agent", {"next_action": "current"},
            1, "2026-08-21T00:01:00Z",
        )
        comments = [
            {"id": "comment-a", "body": encode_event_comment(first)},
            {"id": "comment-b", "body": encode_event_comment(second)},
        ]

        enriched = MODULE.add_material_history(snapshot, comments, "GEN-37")
        context = MODULE.compact_context(enriched, "GEN-37")

        self.assertEqual(context["root_revision"], 2)
        self.assertEqual(context["issue_revision"], 91)
        self.assertEqual(context["material_event_revision"], 2)
        self.assertEqual(context["next_action"], "current")
        self.assertEqual(enriched["root"]["issue_revision"], 91)
        self.assertNotIn("material_events", context)
        self.assertEqual(context["history"]["material_events"]["count"], 2)
        self.assertEqual(context["history"]["material_events"]["latest"]["event_id"], "event-b")

        full = MODULE.compact_context(enriched, "GEN-37", include_history=True)
        self.assertEqual(
            [event["event_id"] for event in full["material_events"]],
            ["event-a", "event-b"],
        )
        self.assertTrue(full["history"]["included"])

    def test_material_history_recovers_latest_checkpoint_provenance(self):
        snapshot = self.snapshot()
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="implemented", root_revision=1,
            plan_revision="sha", before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session-1",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/worktree", "branch": "feature/resume",
                    "head": "abc123",
                },
            },
            exact_head="abc123", evidence=[{"kind": "test", "id": "unit"}],
            blocker=None, next_action="validate live resume",
        )
        comments = [
            {"id": "event-1", "body": encode_event_comment(Delta(
                "event-a", "GEN-37", "progress", "checkpoint", {}, 0,
                "2026-08-21T00:00:00Z",
            ))},
            {"id": "checkpoint-1", "body": encode_checkpoint_comment(checkpoint)},
        ]

        context = MODULE.compact_context(
            MODULE.add_material_history(snapshot, comments, "GEN-37"), "GEN-37"
        )

        self.assertEqual(context["latest_checkpoint"]["worktree"]["path"], "/repo/worktree")
        self.assertEqual(context["latest_checkpoint"]["provenance"]["latest"]["machine"], "M5")
        self.assertEqual(context["latest_checkpoint"]["evidence"]["count"], 1)
        self.assertNotIn("items", context["latest_checkpoint"]["evidence"])
        self.assertRegex(context["latest_checkpoint"]["provenance"]["sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(context["surface_availability"]["latest_checkpoint"], "available")
        self.assertEqual(context["next_action"], "validate live resume")
        full = MODULE.compact_context(
            MODULE.add_material_history(snapshot, comments, "GEN-37"), "GEN-37",
            include_history=True,
        )
        self.assertEqual(full["latest_checkpoint"]["provenance_chain"][0]["machine"], "M5")

    def test_material_history_recovers_selected_generation_activation_checkpoint(self):
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": "root-uuid",
        }
        snapshot = self.snapshot()
        snapshot["root"]["generation_transition_tip_event_id"] = "transition-tip"
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="generation-activation",
            root_revision=0, plan_revision="sha",
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "next",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/next", "branch": "next",
                    "head": "abc123",
                },
            },
            exact_head="abc123", evidence=[{"kind": "test", "id": "activation"}],
            blocker=None, next_action="Resume the activated generation.",
        )
        with mock.patch(
            "workstream_generation.selected_activation_checkpoints",
            return_value=[(checkpoint, "transition-comment")],
        ) as selected:
            enriched = MODULE.add_material_history(
                snapshot, [], "GEN-37", authenticated_route=route,
            )
        context = MODULE.compact_context(enriched, "GEN-37")
        self.assertEqual(
            context["latest_checkpoint"]["checkpoint_event_id"],
            checkpoint["event_id"],
        )
        self.assertEqual(
            context["latest_checkpoint"]["worktree"]["path"], "/repo/next",
        )
        selected.assert_called_once_with(
            [], workstream_id="GEN-37", transition_event_id="transition-tip",
            active_plan_revision="sha", authenticated_route=route,
        )

    def test_generation_activation_checkpoint_requires_authenticated_route(self):
        snapshot = self.snapshot()
        snapshot["root"]["generation_transition_tip_event_id"] = "transition-tip"
        with self.assertRaisesRegex(
            MODULE.ResumeError, "generation_activation_checkpoint_route_missing",
        ):
            MODULE.add_material_history(snapshot, [], "GEN-37")

    def test_material_event_after_checkpoint_supersedes_checkpoint_next_action(self):
        snapshot = self.snapshot()
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="checkpointed", root_revision=1,
            plan_revision="sha", before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session-1",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/worktree", "branch": "feature/resume",
                    "head": "abc123",
                },
            },
            exact_head="abc123", evidence=[], blocker=None,
            next_action="checkpoint action",
        )
        comments = [
            {"id": "event-1", "body": encode_event_comment(Delta(
                "event-a", "GEN-37", "progress", "agent",
                {"next_action": "acknowledged action"}, 0,
                "2026-08-21T00:00:00Z",
            ))},
            {"id": "checkpoint-1", "body": encode_checkpoint_comment(checkpoint)},
            {"id": "event-2", "body": encode_event_comment(Delta(
                "event-b", "GEN-37", "progress", "agent",
                {"next_action": "new action"}, 1,
                "2026-08-21T00:01:00Z",
            ))},
        ]
        context = MODULE.compact_context(
            MODULE.add_material_history(snapshot, comments, "GEN-37"), "GEN-37"
        )
        self.assertEqual(context["next_action"], "new action")

    def test_later_canonical_event_beats_checkpoint_despite_stale_writer_revision(self):
        snapshot = self.snapshot()
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="checkpointed", root_revision=1,
            plan_revision="sha", before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session-1",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/worktree", "branch": "feature/resume",
                    "head": "abc123",
                },
            },
            exact_head="abc123", evidence=[], blocker=None,
            next_action="checkpoint action",
        )
        comments = [
            {"id": "event-1", "body": encode_event_comment(Delta(
                "event-a", "GEN-37", "progress", "agent", {}, 0,
                "2026-08-21T00:00:00Z",
            ))},
            {"id": "checkpoint-1", "body": encode_checkpoint_comment(checkpoint)},
            {"id": "event-2", "body": encode_event_comment(Delta(
                "event-b", "GEN-37", "progress", "agent",
                {"next_action": "later action"}, 0,
                "2026-08-21T00:01:00Z",
            ))},
        ]
        context = MODULE.compact_context(
            MODULE.add_material_history(snapshot, comments, "GEN-37"), "GEN-37"
        )
        self.assertEqual(context["next_action"], "later action")

    def test_material_history_exposes_absent_checkpoint_without_fabrication(self):
        context = MODULE.compact_context(
            MODULE.add_material_history(self.snapshot(), [], "GEN-37"), "GEN-37"
        )
        self.assertIsNone(context["latest_checkpoint"])
        self.assertEqual(context["checkpoint_recovery"]["state"], "not_found")
        self.assertEqual(context["surface_availability"]["latest_checkpoint"], "available")

    def test_stale_plan_checkpoint_does_not_brick_current_resume(self):
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="old-plan", root_revision=0,
            plan_revision="old-sha", before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "old-session",
                "machine": "M3", "worktree": {"state": "unknown"},
            },
            exact_head=None, evidence=[], blocker=None, next_action="obsolete",
        )
        context = MODULE.compact_context(
            MODULE.add_material_history(
                self.snapshot(),
                [{"id": "old-checkpoint", "body": encode_checkpoint_comment(checkpoint)}],
                "GEN-37",
            ),
            "GEN-37",
        )
        self.assertIsNone(context["latest_checkpoint"])
        self.assertEqual(
            context["checkpoint_recovery"],
            {"state": "stale_plan", "stale_plan_count": 1},
        )

    def test_stale_root_checkpoint_generation_must_be_a_complete_chain(self):
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="old-plan-truncated",
            root_revision=2, plan_revision="old-sha",
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "old-session", "machine": "M3",
                "worktree": {"state": "unknown"},
            },
            exact_head=None, evidence=[], blocker=None, next_action="obsolete",
            predecessor_event_id="missing-predecessor",
        )
        with self.assertRaisesRegex(
            MODULE.ResumeError,
            "invalid_checkpoint_history:GEN-37:checkpoint_chain_truncated",
        ):
            MODULE.add_material_history(
                self.snapshot(), [{
                    "id": "old-checkpoint", "body": encode_checkpoint_comment(checkpoint),
                }], "GEN-37",
            )

    def test_checkpoint_cannot_claim_unrecorded_material_revision(self):
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="ahead", root_revision=1,
            plan_revision="sha", before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "session-1",
                "machine": "M5", "worktree": {"state": "unknown"},
            },
            exact_head=None, evidence=[], blocker=None, next_action="cannot trust this",
        )
        with self.assertRaisesRegex(MODULE.ResumeError, "checkpoint_ahead"):
            MODULE.add_material_history(
                self.snapshot(),
                [{"id": "checkpoint-1", "body": encode_checkpoint_comment(checkpoint)}],
                "GEN-37",
            )

    def test_material_event_validation_fails_closed(self):
        snapshot = self.snapshot()
        event = {
            "event_id": "duplicate", "workstream_id": "GEN-37", "kind": "progress",
            "source": "agent", "payload": {}, "expected_revision": 0,
            "created_at": "2026-08-21T00:00:00Z",
        }
        snapshot["material_events"] = [event, dict(event)]
        snapshot["material_event_revision"] = 2
        with self.assertRaisesRegex(MODULE.ResumeError, "duplicate_material_event"):
            MODULE.compact_context(snapshot, "GEN-37")

    def test_event_next_action_must_be_nonblank_text(self):
        payloads = (
            {"next_action": {}},
            {"next_action": "   "},
            {"boundary_id": "turn-1", "changes": [
                {"kind": "next_action", "payload": {"next_action": []}},
            ]},
        )
        for index, payload in enumerate(payloads):
            with self.subTest(payload=payload):
                snapshot = self.snapshot()
                snapshot["root"]["revision"] = 1
                snapshot["material_events"] = [{
                    "event_id": f"event-{index}", "workstream_id": "GEN-37",
                    "kind": "material_boundary" if "changes" in payload else "progress",
                    "source": "agent", "payload": payload, "expected_revision": 0,
                    "created_at": "2026-08-21T00:00:00Z",
                }]
                snapshot["material_event_revision"] = 1
                with self.assertRaisesRegex(MODULE.ResumeError, "invalid_event_next_action"):
                    MODULE.compact_context(snapshot, "GEN-37")

    def test_offline_material_revision_must_match_root(self):
        snapshot = self.snapshot()
        snapshot["material_events"] = []
        snapshot["material_event_revision"] = 0
        with self.assertRaisesRegex(MODULE.ResumeError, "material_event_revision_mismatch"):
            MODULE.compact_context(snapshot, "GEN-37")

    def test_offline_fabricated_checkpoint_fails_closed(self):
        snapshot = self.snapshot()
        snapshot["latest_checkpoint"] = {
            "workstream_id": "GEN-37", "plan_revision": "sha",
        }
        snapshot["checkpoint_recovery"] = {"state": "current", "stale_plan_count": 0}
        with self.assertRaisesRegex(MODULE.ResumeError, "invalid_latest_checkpoint_fields"):
            MODULE.compact_context(snapshot, "GEN-37")

    def test_concurrent_conflicting_next_actions_fail_closed(self):
        comments = [
            {"id": "event-a", "body": encode_event_comment(Delta(
                "event-a", "GEN-37", "progress", "agent", {"next_action": "A"},
                0, "2026-08-21T00:00:00Z",
            ))},
            {"id": "event-b", "body": encode_event_comment(Delta(
                "event-b", "GEN-37", "progress", "agent", {"next_action": "B"},
                0, "2026-08-21T00:00:01Z",
            ))},
        ]
        with self.assertRaisesRegex(MODULE.ResumeError, "conflicting_concurrent_next_action"):
            MODULE.add_material_history(self.snapshot(), comments, "GEN-37")

    def test_material_boundary_nested_next_action_is_resumed(self):
        event = Delta(
            "event-boundary", "GEN-37", "material_boundary", "user_turn",
            {
                "boundary_id": "turn-1",
                "changes": [
                    {"kind": "requirement", "payload": {"text": "new scope"}},
                    {"kind": "next_action", "payload": {"next_action": "build the adapter"}},
                ],
            },
            0, "2026-08-21T00:00:00Z",
        )
        context = MODULE.compact_context(
            MODULE.add_material_history(
                self.snapshot(),
                [{"id": "event-boundary", "body": encode_event_comment(event)}],
                "GEN-37",
            ),
            "GEN-37",
        )
        self.assertEqual(context["next_action"], "build the adapter")

    def test_malformed_material_boundary_fails_closed(self):
        event = Delta(
            "event-boundary", "GEN-37", "material_boundary", "user_turn",
            {"boundary_id": "turn-1", "changes": "not-a-list"}, 0,
            "2026-08-21T00:00:00Z",
        )
        envelope = linear_events_module._canonical_event(event)
        encoded = base64.urlsafe_b64encode(json.dumps(
            envelope, sort_keys=True, separators=(",", ":"),
        ).encode()).decode().rstrip("=")
        body = f"{linear_events_module.EVENT_PREFIX}{encoded} -->"
        with self.assertRaisesRegex(MODULE.ResumeError, "malformed_material_boundary"):
            MODULE.add_material_history(
                self.snapshot(),
                [{"id": "event-boundary", "body": body}],
                "GEN-37",
            )

    def test_direct_snapshot_malformed_material_boundary_fails_closed(self):
        snapshot = self.snapshot()
        snapshot["root"]["revision"] = 1
        snapshot["material_events"] = [{
            "event_id": "event-boundary", "workstream_id": "GEN-37",
            "kind": "material_boundary", "source": "user_turn",
            "payload": {"boundary_id": "turn-1", "changes": "not-a-list"},
            "expected_revision": 0, "created_at": "2026-08-21T00:00:00Z",
        }]
        snapshot["material_event_revision"] = 1
        with self.assertRaisesRegex(MODULE.ResumeError, "malformed_material_boundary"):
            MODULE.compact_context(snapshot, "GEN-37")

    def test_compact_and_full_repair_surfaces_preserve_digest_and_originals(self):
        snapshot = self.snapshot()
        snapshot["children"] = []
        snapshot["root"]["revision"] = 2
        effective = [{
            "event_id": "flat", "workstream_id": "GEN-37",
            "kind": "material_boundary", "source": "system",
            "payload": {"boundary_id": "repair:flat", "changes": [{
                "kind": "progress", "payload": {"next_action": "normalized"},
            }]},
            "expected_revision": 0, "created_at": "2026-08-30T00:00:00Z",
        }, {
            "event_id": "repair", "workstream_id": "GEN-37",
            "kind": "material_semantic_repair", "source": "system",
            "payload": {}, "expected_revision": 1,
            "created_at": "2026-08-30T00:01:00Z",
        }]
        raw = copy.deepcopy(effective)
        raw[0]["payload"] = {"next_action": "normalized", "progress": "flat"}
        binding = {"event_id": "flat", "remote_comment_id": "remote-flat"}
        snapshot.update({
            "material_events": effective, "raw_material_events": raw,
            "material_semantic_repairs": [binding],
            "material_event_revision": 2,
        })
        compact = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(compact["next_action"], "normalized")
        self.assertEqual(compact["material_semantic_repair"]["count"], 1)
        self.assertNotIn("material_semantic_repairs", compact["history"])
        self.assertEqual(compact["history"]["raw_material_events"]["count"], 2)
        self.assertRegex(
            compact["history"]["raw_material_events"]["sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertNotIn("latest", compact["history"]["raw_material_events"])
        self.assertNotIn("raw_material_events", compact)
        full = MODULE.compact_context(snapshot, "GEN-37", include_history=True)
        self.assertEqual(full["raw_material_events"], raw)
        self.assertEqual(full["material_semantic_repairs"], [binding])

    def test_repair_audit_metadata_does_not_crowd_out_exact_obligations(self):
        snapshot = self.snapshot()
        snapshot["children"] = []
        snapshot["root"]["revision"] = 3
        effective = [{
            "event_id": "flat", "workstream_id": "GEN-37",
            "kind": "material_boundary", "source": "system",
            "payload": {"boundary_id": "repair:flat", "changes": [{
                "kind": "progress", "payload": {"next_action": "normalized"},
            }]},
            "expected_revision": 0, "created_at": "2026-08-30T00:00:00Z",
        }, {
            "event_id": "repair", "workstream_id": "GEN-37",
            "kind": "material_semantic_repair", "source": "system",
            "payload": {}, "expected_revision": 1,
            "created_at": "2026-08-30T00:01:00Z",
        }, {
            "event_id": "requirement", "workstream_id": "GEN-37",
            "kind": "requirement", "source": "user_turn",
            "payload": {"requirement": ""}, "expected_revision": 2,
            "created_at": "2026-08-30T00:02:00Z",
        }]
        raw = copy.deepcopy(effective)
        raw[0]["payload"] = {"next_action": "normalized", "progress": "flat"}
        binding = {"event_id": "flat", "remote_comment_id": "remote-flat"}
        snapshot.update({
            "material_events": effective, "raw_material_events": raw,
            "material_semantic_repairs": [binding],
            "material_event_revision": 3,
        })

        encode = lambda value: json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode()
        baseline = MODULE.compact_context(
            snapshot, "GEN-37", max_bytes=1024 * 1024,
        )
        exact_requirement = "x" * (
            MODULE.DEFAULT_RESUME_MAX_BYTES - len(encode(baseline)) - 64
        )
        effective[-1]["payload"]["requirement"] = exact_requirement
        raw[-1]["payload"]["requirement"] = exact_requirement
        compact = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(
            compact["uncheckpointed_material_obligations"][-1]["payload"]
            ["requirement"],
            exact_requirement,
        )
        self.assertLessEqual(len(encode(compact)), MODULE.DEFAULT_RESUME_MAX_BYTES)

        # Reconstruct the redundant PR #48 metadata: it alone pushes the same
        # validated actionable context over the default contract.
        legacy = copy.deepcopy(compact)
        legacy["history"]["material_semantic_repairs"] = compact[
            "material_semantic_repair"
        ]
        legacy["history"]["raw_material_events"]["latest"] = {
            "event_id": "requirement", "kind": "requirement",
            "created_at": "2026-08-30T00:02:00Z",
        }
        self.assertGreater(len(encode(legacy)), MODULE.DEFAULT_RESUME_MAX_BYTES)

    def test_uncheckpointed_requirement_payload_is_not_replaced_by_digest(self):
        snapshot = self.snapshot()
        snapshot["root"]["revision"] = 1
        snapshot["material_events"] = [{
            "event_id": "requirement-1", "workstream_id": "GEN-37",
            "kind": "requirement", "source": "user_turn",
            "payload": {"requirement": "must preserve offline recovery"},
            "expected_revision": 0, "created_at": "2026-08-21T00:00:00Z",
        }]
        snapshot["material_event_revision"] = 1
        context = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(context["uncheckpointed_material_obligations"], [{
            "event_id": "requirement-1", "kind": "requirement",
            "payload": {"requirement": "must preserve offline recovery"},
        }])

    def test_default_budget_accepts_bounded_actionable_resume_over_16k(self):
        snapshot = self.snapshot()
        requirement = "preserve exact recovery context " + ("x" * 18000)
        snapshot["root"]["revision"] = 1
        snapshot["material_events"] = [{
            "event_id": "requirement-large", "workstream_id": "GEN-37",
            "kind": "requirement", "source": "user_turn",
            "payload": {"requirement": requirement},
            "expected_revision": 0, "created_at": "2026-08-21T00:00:00Z",
        }]
        snapshot["material_event_revision"] = 1

        with self.assertRaisesRegex(MODULE.ResumeError, "over_budget"):
            MODULE.compact_context(snapshot, "GEN-37", max_bytes=16 * 1024)
        context = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(
            context["uncheckpointed_material_obligations"][0]["payload"]
            ["requirement"],
            requirement,
        )
        self.assertLess(
            len(MODULE.json.dumps(
                context, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode()),
            MODULE.DEFAULT_RESUME_MAX_BYTES,
        )

    def test_checkpoint_fence_uses_ordered_position_not_stale_writer_revision(self):
        prefix = {
            "event_id": "progress-1", "workstream_id": "GEN-37", "kind": "progress",
            "source": "agent", "payload": {}, "expected_revision": 0,
            "created_at": "2026-08-21T00:00:00Z",
        }
        cases = (
            ("requirement", {"requirement": "R"}, "requirement"),
            ("blocker", {"blocker": "B"}, "blocker"),
            ("decision", {"decision": "D"}, "decision"),
            ("followup", {"followup": "F"}, "followup"),
            ("material_boundary", {"boundary_id": "b", "changes": [
                {"kind": "requirement", "payload": {"requirement": "nested"}},
            ]}, "requirement"),
        )
        for kind, payload, expected_kind in cases:
            with self.subTest(kind=kind):
                concurrent = {
                    "event_id": f"{kind}-2", "workstream_id": "GEN-37", "kind": kind,
                    "source": "agent", "payload": payload, "expected_revision": 0,
                    "created_at": "2026-08-21T00:00:01Z",
                }
                obligations = MODULE._uncheckpointed_material_obligations(
                    [prefix, concurrent], checkpoint_revision=1,
                )
                self.assertEqual(len(obligations), 1)
                self.assertEqual(obligations[0]["kind"], expected_kind)

    def test_material_history_counts_toward_item_budget(self):
        snapshot = MODULE.add_material_history(
            self.snapshot(), [{"id": "event", "body": encode_event_comment(Delta(
                "event-a", "GEN-37", "progress", "agent", {}, 0,
                "2026-08-21T00:00:00Z",
            ))}], "GEN-37",
        )
        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(
                snapshot, "GEN-37", max_items=2, include_history=True,
            )

    def test_stale_plan_checkpoint_only_summarizes_acknowledged_prefix(self):
        events = [{
            "event_id": f"event-{index}", "kind": "requirement",
            "payload": {"requirement": f"requirement-{index}"},
        } for index in range(4)]
        remaining, summary = MODULE._compact_stale_plan_obligations(
            events, [{"root_revision": 2}],
        )

        self.assertEqual(remaining, [{
            "event_id": "event-2", "kind": "requirement",
            "payload": {"requirement": "requirement-2"},
        }, {
            "event_id": "event-3", "kind": "requirement",
            "payload": {"requirement": "requirement-3"},
        }])
        self.assertEqual(summary["checkpoint_root_revision"], 2)
        self.assertEqual(summary["acknowledged_count"], 2)
        self.assertEqual(summary["uncheckpointed_count"], 2)
        changed = copy.deepcopy(events)
        changed[0]["payload"]["requirement"] = "planted-mutation"
        _remaining, changed_summary = MODULE._compact_stale_plan_obligations(
            changed, [{"root_revision": 2}],
        )
        self.assertNotEqual(summary["sha256"], changed_summary["sha256"])
        with self.assertRaisesRegex(
            MODULE.ResumeError,
            "child_stale_checkpoint_ahead_of_material_event_log:5>4",
        ):
            MODULE._compact_stale_plan_obligations(
                events, [{"root_revision": 5}],
            )

    def test_resume_preserves_typed_choice_scope_and_relations_when_supplied(self):
        snapshot = self.snapshot()
        snapshot["scope"] = {
            "namespace": "pulp-playback",
            "linear": {"workspace_id": "ws", "team_id": "team", "project_id": "project",
                       "root_issue_id": "33333333-3333-4333-8333-333333333333",
                       "route_verification": {
                           "workspace_id": "ws", "team_id": "team", "project_id": "project",
                           "root_issue_id": "33333333-3333-4333-8333-333333333333",
                           "observed_at": "2026-08-21T11:00:00Z",
                           "evidence": [{"kind": "authenticated_linear_readback", "authenticated": True,
                                         "workspace_id": "ws", "team_id": "team", "project_id": "project",
                                         "root_issue_id": "33333333-3333-4333-8333-333333333333"}]}},
            "primary_repository": "github.com:id:R_pulp",
            "repositories": [{"slug": "github.com/generous-corp/pulp", "exact_head": "a" * 40,
                              "provider_repository_id": "R_pulp", "aliases": [],
                              "identity_resolution": {"provider_repository_id": "R_pulp",
                                                      "resolved_slug": "github.com/generous-corp/pulp",
                                                      "observed_at": "2026-08-21T11:00:00Z",
                                                      "evidence": [{"kind": "authenticated_provider_readback",
                                                                    "authenticated": True,
                                                                    "provider_repository_id": "R_pulp",
                                                                    "resolved_slug": "github.com/generous-corp/pulp"}]},
                              "identity_updates": [], "evidence": []}],
            "child_ownership": {"GEN-38": "github.com:id:R_pulp",
                                "GEN-39": "github.com:id:R_pulp"},
        }
        snapshot["relations"] = [{"type": "blocked_by", "target": {
            "workspace_id": "ws-other", "issue_id": "11111111-1111-4111-8111-111111111111",
            "identifier": "GEN-50",
        }}]
        snapshot["choice_events"] = []
        snapshot["evidence_contracts"] = []
        snapshot["material_events"] = []
        snapshot["material_event_revision"] = 0
        snapshot["authenticated_route"] = {
            "workspace_id": "ws", "team_id": "team", "project_id": "project",
            "root_issue_id": "33333333-3333-4333-8333-333333333333",
        }
        snapshot["dependency_graph"] = {
            "schema_version": 1,
            "authority": "child_dependency_authorization",
            "plan_revision": "sha",
            "route": snapshot["authenticated_route"],
            "revision": 0,
            "sha256": hashlib.sha256(b"[]").hexdigest(),
            "authorization_batches": [],
            "relations": [],
            "native_readback": "relations_and_inverseRelations",
            "ignored_non_dependency_count": 0,
            "observed_frontier": {
                "material_revision": 0, "projection_revision": 0,
                "graph_revision": 0,
                "graph_sha256": hashlib.sha256(b"[]").hexdigest(),
            },
            "root_readback_sha256": "0" * 64,
        }
        snapshot["root"].update({
            "id": snapshot["authenticated_route"]["root_issue_id"],
            "title": "Root", "description": "Plan revision: sha",
            "url": "https://linear/GEN-37", "updatedAt": "now", "parent": None,
            "team": {"id": "team", "organization": {"id": "ws"}},
            "project": {"id": "project"}, "assignee": None,
            "state": {"id": "started", "name": "In Progress", "type": "started"},
        })
        snapshot["dependency_graph"]["root_readback_sha256"] = (
            MODULE.dependency_root_readback_sha256(snapshot["root"])
        )
        snapshot["root"]["issue_revision"] = snapshot["root"]["revision"]
        snapshot["root"]["revision"] = 0
        snapshot["latest_checkpoint"] = None
        snapshot["checkpoint_recovery"] = {
            "state": "not_found", "stale_plan_count": 0,
        }
        context = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(context["scope"]["linear"]["project_id"], "project")
        self.assertNotIn("route_verification", context["scope"]["linear"])
        self.assertRegex(context["scope"]["validated_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(context["relations"][0]["target"]["identifier"], "GEN-50")
        self.assertEqual(context["dependency_graph"]["relations"], [])
        self.assertTrue(all(value == "available" for value in context["surface_availability"].values()))

        mutated = copy.deepcopy(snapshot)
        mutated["dependency_graph"]["observed_frontier"]["material_revision"] = 999
        with self.assertRaisesRegex(
            MODULE.ResumeError, "dependency_resume_frontier_mismatch",
        ):
            MODULE.compact_context(mutated, "GEN-37")
        mutated = copy.deepcopy(snapshot)
        mutated["dependency_graph"]["root_readback_sha256"] = "f" * 64
        with self.assertRaisesRegex(
            MODULE.ResumeError, "dependency_resume_root_mismatch",
        ):
            MODULE.compact_context(mutated, "GEN-37")

    def test_full_authority_requires_explicit_authenticated_dependency_graph(self):
        for mutation in ("missing", "null"):
            snapshot = self.snapshot()
            if mutation == "null":
                snapshot["dependency_graph"] = None
            with self.assertRaisesRegex(
                MODULE.ResumeError, "authenticated_dependency_graph_missing",
            ):
                MODULE.validate_snapshot(
                    snapshot, "GEN-37", require_projection_authority=True,
                )

    def test_legacy_live_snapshot_exposes_untransported_factory_surfaces(self):
        context = MODULE.compact_context(self.snapshot(), "GEN-37")
        self.assertEqual(context["surface_availability"]["scope"], "transport_unimplemented")
        self.assertEqual(context["surface_availability"]["evidence_contracts"], "transport_unimplemented")

    def test_live_cli_normalizes_title_before_fetch(self):
        authenticated_route = {"workspace_id": "workspace", "team_id": "team",
                               "project_id": "project", "root_issue_id": "root-uuid"}
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = self.live_snapshot(
            self.snapshot(), authenticated_route,
        )
        client = mock.Mock()
        comments = mock.Mock()
        comments.comments.return_value = []
        with mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client), \
             mock.patch.object(MODULE, "resolve_authenticated_issue_route", return_value=authenticated_route), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", return_value=transport), \
             mock.patch.object(MODULE, "LinearCommentEventAdapter", return_value=comments), \
             mock.patch.object(MODULE, "load_linear_api_key", return_value="secret"), \
             mock.patch.object(MODULE.sys, "argv", ["workstream_resume.py", "pulp GEN-37 #3", "--linear-team-id", "team", "--inspection-only"]), \
             mock.patch.object(MODULE.sys, "stdout"):
            self.assertEqual(MODULE.main(), 0)
        transport.snapshot_for_root.assert_called_once_with(
            "GEN-37", include_child_comments=True,
        )
        comments.comments.assert_called_once_with()

    def test_live_cli_automatically_uses_repository_config_route(self):
        route = {
            "workspace_id": "workspace", "team_id": "team", "project_id": "project"
        }
        authenticated_route = {**route, "root_issue_id": "root-uuid"}
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = self.live_snapshot(
            self.snapshot(), authenticated_route,
        )
        client = mock.Mock()
        constructor = mock.Mock(return_value=transport)
        comments = mock.Mock()
        comments.comments.return_value = []
        with mock.patch.object(MODULE, "resolve_linear_route", return_value=(route, Path(".workstream.json"))), \
             mock.patch.object(MODULE, "resolve_authenticated_issue_route", return_value=authenticated_route), \
             mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", constructor), \
             mock.patch.object(MODULE, "LinearCommentEventAdapter", return_value=comments), \
             mock.patch.object(MODULE, "load_linear_api_key", return_value="secret"), \
             mock.patch.object(MODULE.sys, "argv", ["workstream_resume.py", "GEN-37", "--inspection-only"]), \
             mock.patch.object(MODULE.sys, "stdout"):
            self.assertEqual(MODULE.main(), 0)

        constructor.assert_called_once_with(
            client, team_id="team", workspace_id="workspace", project_id="project"
        )
        transport.snapshot_for_root.assert_called_once_with(
            "GEN-37", include_child_comments=True,
        )
        comments.comments.assert_called_once_with()

    def test_live_cli_bootstraps_route_from_token_without_repo_config(self):
        route = {"workspace_id": "workspace", "team_id": "team",
                 "project_id": "project", "root_issue_id": "root-uuid"}
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = self.live_snapshot(
            self.snapshot(), route,
        )
        client = mock.Mock()
        comments = mock.Mock()
        comments.comments.return_value = []
        constructor = mock.Mock(return_value=transport)
        with mock.patch.object(MODULE, "resolve_linear_route", return_value=(None, None)), \
             mock.patch.object(MODULE, "resolve_authenticated_issue_route", return_value=route) as bootstrap, \
             mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", constructor), \
             mock.patch.object(MODULE, "LinearCommentEventAdapter", return_value=comments), \
             mock.patch.object(MODULE, "load_linear_api_key", return_value="secret"), \
             mock.patch.object(MODULE.sys, "argv", ["workstream_resume.py", "GEN-37", "--inspection-only"]), \
             mock.patch.object(MODULE.sys, "stdout"):
            self.assertEqual(MODULE.main(), 0)
        bootstrap.assert_called_once_with(client, "GEN-37", None)
        constructor.assert_called_once_with(
            client, team_id="team", workspace_id="workspace", project_id="project"
        )

    def test_repeated_live_full_resume_is_read_only_and_authenticates_source_first(self):
        graph = self.snapshot()
        graph["root"]["plan_revision"] = "a" * 64
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project",
            "root_issue_id": "33333333-3333-4333-8333-333333333333",
        }
        graph = self.live_snapshot(graph, route)
        child_comments = graph.pop("child_comments")
        authenticated_source = {
            "identity": "https://example.test/immutable-plan",
            "sha256": "a" * 64, "bytes": 123,
        }
        before = MODULE.add_material_history(
            graph, [], "GEN-37", authenticated_route=route,
            authenticated_source=authenticated_source,
        )
        github = {
            "repository": "generous-corp/agent-workstream",
            "provider_repository_id": "R_agent_workstream", "pr_number": 41,
            "pr_head": "c" * 40, "merged": True, "merge_sha": "d" * 40,
        }
        shipyard_body = {
            "schema_version": 1,
            "repository": github["repository"],
            "repository_key": "github.com:id:R_agent_workstream",
            "pr_number": github["pr_number"], "head": github["pr_head"],
            "disposition": "merged", "receipt_id": "receipt-41",
        }
        shipyard = {
            **shipyard_body,
            "receipt_sha256": hashlib.sha256(json.dumps(
                shipyard_body, sort_keys=True, separators=(",", ":"),
            ).encode()).hexdigest(),
        }
        lifecycle = build_projection_event(
            workstream_id="GEN-37", kind="lifecycle", key="root",
            value={
                "status": "Landed — acceptance review required",
                "github": github, "shipyard_receipt": shipyard,
                "closure_input_sha256": "b" * 64,
                "snapshot_sha256": MODULE.closure_snapshot_digest(before),
                "independent_review": None, "closure_receipt_sha256": None,
            },
            plan_revision="a" * 64, expected_revision=0,
            created_at="2026-08-28T12:00:00Z", authority=route,
        )
        comments_payload = [{
            "id": projection_slot_id(
                "GEN-37", "a" * 64, 0, route,
            ),
            "body": encode_projection_comment(lifecycle),
        }]
        preauthentication = MODULE.add_material_history(
            graph, comments_payload, "GEN-37", authenticated_route=route,
            permit_stale_lifecycle_for_reconcile=True,
        )
        self.assertEqual(
            preauthentication["lifecycle_recovery"]["state"], "stale_snapshot",
        )
        transport = mock.Mock()
        transport.snapshot_for_root.side_effect = lambda *_args, **_kwargs: copy.deepcopy({
            **graph, "children": [dict(child) for child in graph["children"]],
            "child_comments": child_comments,
        })
        comments = mock.Mock()
        comments.comments.return_value = comments_payload
        dependency_graph = {
            "schema_version": 1,
            "authority": "child_dependency_authorization",
            "plan_revision": "a" * 64,
            "route": route,
            "revision": 0,
            "sha256": hashlib.sha256(b"[]").hexdigest(),
            "authorization_batches": [],
            "relations": [],
            "native_readback": "relations_and_inverseRelations",
            "ignored_non_dependency_count": 0,
            "observed_frontier": {
                "material_revision": 0, "projection_revision": 1,
                "graph_revision": 0,
                "graph_sha256": hashlib.sha256(b"[]").hexdigest(),
            },
            "root_readback_sha256": MODULE.dependency_root_readback_sha256(
                before["root"],
            ),
        }
        dependency_adapter = mock.Mock()
        dependency_adapter.read_authorized_graph.return_value = dependency_graph
        dependency_constructor = mock.Mock(return_value=dependency_adapter)
        stdout = io.StringIO()

        def verified_context(snapshot, *_args, **_kwargs):
            self.assertEqual(
                snapshot["root"]["status"],
                "Landed — acceptance review required",
            )
            self.assertNotIn("lifecycle_recovery", snapshot)
            self.assertEqual(snapshot["dependency_graph"], dependency_graph)
            return {"status": snapshot["root"]["status"]}

        with mock.patch.object(MODULE, "resolve_linear_route", return_value=(None, None)), \
             mock.patch.object(MODULE, "resolve_authenticated_issue_route", return_value=route), \
             mock.patch.object(MODULE, "HttpGraphQLClient", return_value=mock.Mock()), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", return_value=transport), \
             mock.patch.object(MODULE, "LinearCommentEventAdapter", return_value=comments), \
             mock.patch.object(
                 MODULE, "LinearChildDependencyAdapter", dependency_constructor,
             ), \
             mock.patch.object(MODULE, "load_linear_api_key", return_value="secret"), \
             mock.patch.object(
                 MODULE, "plan_payload", return_value={"source": authenticated_source},
             ), \
             mock.patch.object(MODULE, "compact_context", side_effect=verified_context), \
             mock.patch.object(
                 MODULE.sys, "argv", [
                     "workstream_resume.py", "GEN-37",
                     "--plan-source", authenticated_source["identity"],
                 ],
             ), \
             mock.patch.object(MODULE.sys, "stdout", stdout):
            self.assertEqual(MODULE.main(), 0)
            first = json.loads(stdout.getvalue())
            stdout.seek(0)
            stdout.truncate(0)
            self.assertEqual(MODULE.main(), 0)
            second = json.loads(stdout.getvalue())
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "Landed — acceptance review required")
        self.assertEqual(transport.snapshot_for_root.call_count, 2)
        self.assertEqual(comments.comments.call_count, 2)
        self.assertEqual(dependency_constructor.call_count, 2)
        dependency_constructor.assert_called_with(
            mock.ANY, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=route["root_issue_id"],
            root_identifier="GEN-37", plan_revision="a" * 64,
        )
        graph["root"]["next_action"] = "new material state after reconciliation"
        with self.assertRaisesRegex(
            MODULE.ResumeError, "lifecycle_snapshot_stale_reconcile_required",
        ):
            MODULE.add_material_history(
                graph, comments_payload, "GEN-37", authenticated_route=route,
                authenticated_source=authenticated_source,
            )

    def test_relation_target_readback_reconstructs_exact_lifecycle_digest(self):
        graph = self.snapshot()
        graph["root"]["plan_revision"] = "a" * 64
        route = {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project",
            "root_issue_id": "33333333-3333-4333-8333-333333333333",
        }
        target = {
            "workspace_id": "workspace",
            "issue_id": "22222222-2222-4222-8222-222222222222",
            "identifier": "GEN-50",
        }
        relation = {"type": "related", "target": target}
        relation_event = build_projection_event(
            workstream_id="GEN-37", kind="relation", key="related:GEN-50",
            value=relation, plan_revision="a" * 64, expected_revision=0,
            created_at="2026-08-28T11:59:00Z", authority=route,
        )
        relation_comment = {
            "id": projection_slot_id("GEN-37", "a" * 64, 0, route),
            "body": encode_projection_comment(relation_event),
        }
        target_readback = {
            f"workspace:{target['issue_id']}": {**target, "relations": []},
        }
        before = MODULE.add_material_history(
            graph, [relation_comment], "GEN-37", authenticated_route=route,
            relation_target_resolver=lambda _relations: target_readback,
        )
        github = {
            "repository": "generous-corp/agent-workstream",
            "provider_repository_id": "R_agent_workstream", "pr_number": 41,
            "pr_head": "c" * 40, "merged": True, "merge_sha": "d" * 40,
        }
        shipyard_body = {
            "schema_version": 1, "repository": github["repository"],
            "repository_key": "github.com:id:R_agent_workstream",
            "pr_number": 41, "head": github["pr_head"],
            "disposition": "merged", "receipt_id": "receipt-41",
        }
        shipyard = {**shipyard_body, "receipt_sha256": hashlib.sha256(json.dumps(
            shipyard_body, sort_keys=True, separators=(",", ":"),
        ).encode()).hexdigest()}
        lifecycle = build_projection_event(
            workstream_id="GEN-37", kind="lifecycle", key="root", value={
                "status": "Landed — acceptance review required",
                "github": github, "shipyard_receipt": shipyard,
                "closure_input_sha256": "b" * 64,
                "snapshot_sha256": MODULE.closure_snapshot_digest(before),
                "independent_review": None, "closure_receipt_sha256": None,
            }, plan_revision="a" * 64, expected_revision=1,
            created_at="2026-08-28T12:00:00Z", authority=route,
        )
        comments = [relation_comment, {
            "id": projection_slot_id("GEN-37", "a" * 64, 1, route),
            "body": encode_projection_comment(lifecycle),
        }]
        with self.assertRaisesRegex(
            MODULE.ResumeError, "lifecycle_snapshot_stale_reconcile_required",
        ):
            MODULE.add_material_history(
                graph, comments, "GEN-37", authenticated_route=route,
            )
        resumed = MODULE.add_material_history(
            graph, comments, "GEN-37", authenticated_route=route,
            relation_target_resolver=lambda _relations: target_readback,
        )
        self.assertEqual(
            resumed["root"]["status"], "Landed — acceptance review required",
        )
        self.assertNotIn("lifecycle_recovery", resumed)
        self.assertEqual(
            MODULE.closure_snapshot_digest(resumed),
            lifecycle["value"]["snapshot_sha256"],
        )

    def test_snapshot_cli_is_inspection_only_even_when_forged_as_authenticated(self):
        snapshot = self.snapshot()
        snapshot["authenticated_source"] = {
            "identity": "https://attacker.invalid/plan",
            "sha256": "sha",
        }
        snapshot["resume_authority"] = "full"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "forged.json"
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            stderr = io.StringIO()
            with mock.patch.object(
                MODULE.sys, "argv", ["workstream_resume.py", "GEN-37", str(path)]
            ), mock.patch.object(MODULE.sys, "stderr", stderr):
                self.assertEqual(MODULE.main(), 2)
        self.assertIn("snapshot_input_requires_inspection_only", stderr.getvalue())

    def test_snapshot_cli_accepts_explicit_inspection_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot.json"
            path.write_text(json.dumps(self.snapshot()), encoding="utf-8")
            stdout = io.StringIO()
            with mock.patch.object(
                MODULE.sys, "argv",
                ["workstream_resume.py", "GEN-37", str(path), "--inspection-only"],
            ), mock.patch.object(MODULE.sys, "stdout", stdout):
                self.assertEqual(MODULE.main(), 0)
        self.assertEqual(json.loads(stdout.getvalue())["resume_authority"], "inspection_only")


if __name__ == "__main__":
    unittest.main()
