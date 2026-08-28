import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock

from workstream_checkpoint import build_checkpoint
from workstream_delta import Delta
from workstream_linear_checkpoints import encode_checkpoint_comment
from workstream_linear_events import encode_event_comment


SCRIPT = Path(__file__).with_name("workstream_resume.py")
SPEC = importlib.util.spec_from_file_location("workstream_resume", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
sys.modules["workstream_resume"] = MODULE
SPEC.loader.exec_module(MODULE)


class ResumeTests(unittest.TestCase):
    def snapshot(self):
        return {"root": {"identifier": "GEN-37", "url": "https://linear/GEN-37", "plan_revision": "sha",
                          "revision": 7, "status": "In Progress", "next_action": "resume"},
                "children": [{"identifier": "GEN-38", "title": "intake", "status": "In Progress", "next_action": "adapter"},
                             {"identifier": "GEN-39", "title": "delta", "status": "Done"}],
                "decisions": [{"id": "D1", "status": "accepted"}], "provenance": [{"machine": "M3"}]}

    def test_compact_context_keeps_root_and_only_nonterminal_children(self):
        context = MODULE.compact_context(self.snapshot(), "GEN-37")
        self.assertEqual(context["workstream_id"], "GEN-37")
        self.assertEqual([c["identifier"] for c in context["children"]], ["GEN-38"])

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

    def test_nonterminal_root_without_next_action_fails_closed(self):
        snapshot = self.snapshot(); snapshot["root"].pop("next_action")
        with self.assertRaisesRegex(MODULE.ResumeError, "root missing next_action"):
            MODULE.validate_snapshot(snapshot, "GEN-37")

    def test_context_budget_fails_loudly(self):
        with self.assertRaisesRegex(MODULE.ResumeError, "over_budget"):
            MODULE.compact_context(self.snapshot(), "GEN-37", max_bytes=10)

    def test_context_item_budget_fails_loudly(self):
        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(self.snapshot(), "GEN-37", max_items=2)

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
        self.assertEqual([event["event_id"] for event in context["material_events"]], ["event-a", "event-b"])

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
        self.assertEqual(context["latest_checkpoint"]["provenance_chain"][0]["machine"], "M5")
        self.assertEqual(context["surface_availability"]["latest_checkpoint"], "available")

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
        with self.assertRaisesRegex(MODULE.ResumeError, "malformed_material_boundary"):
            MODULE.add_material_history(
                self.snapshot(),
                [{"id": "event-boundary", "body": encode_event_comment(event)}],
                "GEN-37",
            )

    def test_material_history_counts_toward_item_budget(self):
        snapshot = MODULE.add_material_history(
            self.snapshot(), [{"id": "event", "body": encode_event_comment(Delta(
                "event-a", "GEN-37", "progress", "agent", {}, 0,
                "2026-08-21T00:00:00Z",
            ))}], "GEN-37",
        )
        with self.assertRaisesRegex(MODULE.ResumeError, "over_item_budget"):
            MODULE.compact_context(snapshot, "GEN-37", max_items=2)

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
        snapshot["root"]["issue_revision"] = snapshot["root"]["revision"]
        snapshot["root"]["revision"] = 0
        snapshot["latest_checkpoint"] = None
        snapshot["checkpoint_recovery"] = {
            "state": "not_found", "stale_plan_count": 0,
        }
        context = MODULE.compact_context(snapshot, "GEN-37")
        self.assertEqual(context["scope"]["linear"]["project_id"], "project")
        self.assertEqual(context["relations"][0]["target"]["identifier"], "GEN-50")
        self.assertTrue(all(value == "available" for value in context["surface_availability"].values()))

    def test_legacy_live_snapshot_exposes_untransported_factory_surfaces(self):
        context = MODULE.compact_context(self.snapshot(), "GEN-37")
        self.assertEqual(context["surface_availability"]["scope"], "transport_unimplemented")
        self.assertEqual(context["surface_availability"]["evidence_contracts"], "transport_unimplemented")

    def test_live_cli_normalizes_title_before_fetch(self):
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = self.snapshot()
        client = mock.Mock()
        comments = mock.Mock()
        comments.comments.return_value = []
        authenticated_route = {"workspace_id": "workspace", "team_id": "team",
                               "project_id": "project", "root_issue_id": "root-uuid"}
        with mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client), \
             mock.patch.object(MODULE, "resolve_authenticated_issue_route", return_value=authenticated_route), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", return_value=transport), \
             mock.patch.object(MODULE, "LinearCommentEventAdapter", return_value=comments), \
             mock.patch.object(MODULE, "load_linear_api_key", return_value="secret"), \
             mock.patch.object(MODULE.sys, "argv", ["workstream_resume.py", "pulp GEN-37 #3", "--linear-team-id", "team", "--inspection-only"]), \
             mock.patch.object(MODULE.sys, "stdout"):
            self.assertEqual(MODULE.main(), 0)
        transport.snapshot_for_root.assert_called_once_with("GEN-37")
        comments.comments.assert_called_once_with()

    def test_live_cli_automatically_uses_repository_config_route(self):
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = self.snapshot()
        client = mock.Mock()
        route = {
            "workspace_id": "workspace", "team_id": "team", "project_id": "project"
        }
        constructor = mock.Mock(return_value=transport)
        comments = mock.Mock()
        comments.comments.return_value = []
        with mock.patch.object(MODULE, "resolve_linear_route", return_value=(route, Path(".workstream.json"))), \
             mock.patch.object(MODULE, "resolve_authenticated_issue_route", return_value={**route, "root_issue_id": "root-uuid"}), \
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
        transport.snapshot_for_root.assert_called_once_with("GEN-37")
        comments.comments.assert_called_once_with()

    def test_live_cli_bootstraps_route_from_token_without_repo_config(self):
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = self.snapshot()
        client = mock.Mock()
        comments = mock.Mock()
        comments.comments.return_value = []
        route = {"workspace_id": "workspace", "team_id": "team",
                 "project_id": "project", "root_issue_id": "root-uuid"}
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


if __name__ == "__main__":
    unittest.main()
