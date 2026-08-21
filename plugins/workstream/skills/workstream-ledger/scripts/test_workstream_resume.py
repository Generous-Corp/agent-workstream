import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock


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
        with mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", return_value=transport), \
             mock.patch.object(MODULE.os, "environ", {"LINEAR_API_KEY": "secret"}), \
             mock.patch.object(MODULE.sys, "argv", ["workstream_resume.py", "pulp GEN-37 #3", "--linear-team-id", "team"]), \
             mock.patch.object(MODULE.sys, "stdout"):
            self.assertEqual(MODULE.main(), 0)
        transport.snapshot_for_root.assert_called_once_with("GEN-37")

    def test_live_cli_automatically_uses_repository_config_route(self):
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = self.snapshot()
        client = mock.Mock()
        route = {
            "workspace_id": "workspace", "team_id": "team", "project_id": "project"
        }
        constructor = mock.Mock(return_value=transport)
        with mock.patch.object(MODULE, "resolve_linear_route", return_value=(route, Path(".workstream.json"))), \
             mock.patch.object(MODULE, "HttpGraphQLClient", return_value=client), \
             mock.patch.object(MODULE, "LinearGraphQLTransport", constructor), \
             mock.patch.object(MODULE.os, "environ", {"LINEAR_API_KEY": "secret"}), \
             mock.patch.object(MODULE.sys, "argv", ["workstream_resume.py", "GEN-37"]), \
             mock.patch.object(MODULE.sys, "stdout"):
            self.assertEqual(MODULE.main(), 0)

        constructor.assert_called_once_with(
            client, team_id="team", workspace_id="workspace", project_id="project"
        )
        transport.snapshot_for_root.assert_called_once_with("GEN-37")


if __name__ == "__main__":
    unittest.main()
