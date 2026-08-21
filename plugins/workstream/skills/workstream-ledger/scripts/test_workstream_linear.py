#!/usr/bin/env python3
import os
import ssl
import unittest
from unittest import mock

from workstream_http import default_ssl_context
from workstream_linear import (
    HttpGraphQLClient,
    LinearGraphQLTransport,
    LinearTransportError,
    MARKER,
    parse_next_action,
    parse_plan_revision,
    parse_root_revision,
)


class FakeClient:
    def __init__(self):
        self.issues = []
        self.next_id = 1
        self.calls = []

    def execute(self, query, variables):
        self.calls.append((query, variables))
        if "query WorkstreamRoute" in query:
            return {
                "team": {"id": variables["teamId"], "organization": {"id": "workspace"}},
                "project": {
                    "id": variables["projectId"],
                    "teams": {"nodes": [{"id": variables["teamId"]}]},
                },
            }
        if "query WorkstreamIssues" in query:
            return {"team": {"issues": {"nodes": self.issues[:]}}}
        if "issueCreate" in query:
            data = variables["input"]
            issue = {"id": f"id-{self.next_id}", "identifier": f"GEN-{self.next_id}", "title": data["title"], "description": data["description"], "url": f"https://linear.test/{self.next_id}", "updatedAt": "now", "state": {"name": "Todo", "type": "unstarted"}, "parent": {"id": data.get("parentId")} if data.get("parentId") else None, "project": {"id": data["projectId"]} if data.get("projectId") else None}
            self.next_id += 1
            self.issues.append(issue)
            return {"issueCreate": {"success": True, "issue": issue}}
        if "issueUpdate" in query:
            identifier = variables["id"]
            issue = next(i for i in self.issues if i["identifier"] == identifier or i["id"] == identifier)
            issue.update({k: v for k, v in variables["input"].items() if k in {"title", "description"}})
            return {"issueUpdate": {"success": True, "issue": issue}}
        raise AssertionError("unexpected mutation")


class LinearTransportTests(unittest.TestCase):
    def test_http_client_passes_an_explicit_ssl_context(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        with mock.patch("workstream_linear.urllib.request.urlopen", return_value=response) as urlopen, \
             mock.patch("workstream_linear.json.load", return_value={"data": {"ok": True}}):
            result = HttpGraphQLClient("token", ssl_context=context).execute("query { ok }", {})

        self.assertEqual(result, {"ok": True})
        self.assertIs(urlopen.call_args.kwargs["context"], context)

    def test_macos_framework_python_uses_system_ca_bundle(self):
        paths = ssl.DefaultVerifyPaths(None, None, "SSL_CERT_FILE", "/missing", "SSL_CERT_DIR", "/missing-dir")
        fallback = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with mock.patch("workstream_http.sys.platform", "darwin"), \
             mock.patch("workstream_http.ssl.get_default_verify_paths", return_value=paths), \
             mock.patch("workstream_http.os.path.isfile", side_effect=lambda path: path == "/etc/ssl/cert.pem"), \
             mock.patch("workstream_http.ssl.create_default_context", return_value=fallback) as create:
            self.assertIs(default_ssl_context(), fallback)

        create.assert_called_once_with(cafile="/etc/ssl/cert.pem")

    def test_existing_default_ca_configuration_is_preserved(self):
        paths = ssl.DefaultVerifyPaths("/configured.pem", None, "SSL_CERT_FILE", "/configured.pem", "SSL_CERT_DIR", "/certs")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with mock.patch("workstream_http.sys.platform", "darwin"), \
             mock.patch("workstream_http.ssl.get_default_verify_paths", return_value=paths), \
             mock.patch("workstream_http.os.path.isfile") as isfile, \
             mock.patch("workstream_http.ssl.create_default_context", return_value=context) as create:
            self.assertIs(default_ssl_context(), context)

        create.assert_called_once_with()
        isfile.assert_not_called()

    def plan(self):
        return {"graph_review_required": True, "root": {"stable_key": "source-demo", "title": "Demo", "plan_revision": "sha-demo"}, "children": [{"key": "a", "stable_key": "a", "title": "Build"}]}

    def test_reviewed_plan_creates_one_root_and_child(self):
        fake = FakeClient()
        result = LinearGraphQLTransport(fake, team_id="team").apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        self.assertEqual(len(fake.issues), 2)
        self.assertEqual(result["root"]["description"].splitlines()[0], MARKER.pattern.replace("([^ >]+)", "source-demo"))

    def test_configured_route_is_verified_and_applied_to_every_create(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(
            fake, team_id="team", workspace_id="workspace", project_id="project"
        )
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})

        route_calls = [variables for query, variables in fake.calls if "WorkstreamRoute" in query]
        create_inputs = [variables["input"] for query, variables in fake.calls if "issueCreate" in query]
        self.assertEqual(route_calls, [{"teamId": "team", "projectId": "project"}])
        self.assertEqual(len(create_inputs), 2)
        self.assertTrue(all(item["projectId"] == "project" for item in create_inputs))

    def test_from_config_requires_and_consumes_complete_route(self):
        fake = FakeClient()
        route = {
            "workspace_id": "workspace", "team_id": "team", "project_id": "project"
        }
        with mock.patch("workstream_config.resolve_linear_route", return_value=(route, None)):
            transport = LinearGraphQLTransport.from_config(fake)
        self.assertEqual(transport.workspace_id, "workspace")
        self.assertEqual(transport.team_id, "team")
        self.assertEqual(transport.project_id, "project")

    def test_workspace_mismatch_fails_before_issue_read_or_write(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(
            fake, team_id="team", workspace_id="wrong", project_id="project"
        )
        with self.assertRaisesRegex(LinearTransportError, "not in the configured workspace"):
            transport.snapshot()
        self.assertFalse(any("WorkstreamIssues" in query or "issueCreate" in query for query, _ in fake.calls))

    def test_project_fence_excludes_same_marker_from_another_project(self):
        fake = FakeClient()
        other = {
            "id": "other-root", "identifier": "GEN-99", "title": "Demo",
            "description": "<!-- workstream-key:source-demo -->\nPlan revision: sha-demo",
            "url": "https://linear.test/99", "updatedAt": "now",
            "state": {"name": "Todo", "type": "unstarted"}, "parent": None,
            "project": {"id": "other-project"},
        }
        fake.issues.append(other)
        transport = LinearGraphQLTransport(
            fake, team_id="team", workspace_id="workspace", project_id="project"
        )

        result = transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})

        self.assertNotEqual(result["root"]["id"], "other-root")
        self.assertEqual(len(fake.issues), 3)

    def test_repeated_intake_uses_existing_markers(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(fake, team_id="team")
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        self.assertEqual(len(fake.issues), 2)
        self.assertEqual(sum("issueCreate" in query for query, _ in fake.calls), 2)
        self.assertEqual(sum("issueUpdate" in query for query, _ in fake.calls), 0)

    def test_reviewed_child_next_action_round_trips_through_live_snapshot(self):
        fake = FakeClient()
        plan = self.plan()
        plan["root"]["next_action"] = "Review child graph."
        plan["children"][0]["next_action"] = "Run focused tests."
        transport = LinearGraphQLTransport(fake, team_id="team")
        transport.apply_reviewed_plan(plan, accepted_keys={"a"})

        snapshot = transport.snapshot_for_root("GEN-1")

        self.assertEqual(snapshot["root"]["next_action"], "Review child graph.")
        self.assertEqual(snapshot["children"][0]["next_action"], "Run focused tests.")
        self.assertEqual(snapshot["root"]["revision"], 0)

    def test_repeated_intake_preserves_existing_mutable_next_action(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(fake, team_id="team")
        plan = self.plan()
        plan["root"]["next_action"] = "Static plan action."
        transport.apply_reviewed_plan(plan, accepted_keys={"a"})
        fake.issues[0]["description"] = fake.issues[0]["description"].replace(
            "Current next action: Static plan action.",
            "Current next action: Keep this live action.",
        )
        fake.issues[0]["description"] += "\n\nWhy: preserve this human-authored context."

        transport.apply_reviewed_plan(plan, accepted_keys={"a"})

        description = fake.issues[0]["description"]
        self.assertEqual(parse_next_action(description), "Keep this live action.")
        self.assertIn("Why: preserve this human-authored context.", description)
        self.assertNotIn("Static plan action.", description)

    def test_unreviewed_plan_fails_before_network_mutation(self):
        fake = FakeClient()
        plan = self.plan()
        transport = LinearGraphQLTransport(fake, team_id="team")
        with self.assertRaises(ValueError):
            transport.apply_reviewed_plan(plan, accepted_keys=None)
        self.assertEqual(len(fake.issues), 0)

    def test_expected_revision_refuses_root_overwrite_without_remote_cas(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(fake, team_id="team")
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        fake.calls.clear()

        with self.assertRaisesRegex(LinearTransportError, "remote_cas_unavailable"):
            transport.apply_reviewed_plan(
                self.plan(), accepted_keys={"a"}, expected_revision=0
            )

        self.assertFalse(any("issueUpdate" in query for query, _ in fake.calls))

    def test_expected_revision_refuses_initial_create_without_remote_cas(self):
        fake = FakeClient()
        with self.assertRaisesRegex(LinearTransportError, "remote_cas_unavailable"):
            LinearGraphQLTransport(fake, team_id="team").apply_reviewed_plan(
                self.plan(), accepted_keys={"a"}, expected_revision=0
            )
        self.assertFalse(any("issueCreate" in query for query, _ in fake.calls))

    def test_changed_plan_refuses_existing_graph_without_remote_cas(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(fake, team_id="team")
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        changed = self.plan()
        changed["root"]["plan_revision"] = "sha-changed"
        fake.calls.clear()

        with self.assertRaisesRegex(LinearTransportError, "remote_cas_unavailable"):
            transport.apply_reviewed_plan(changed, accepted_keys={"a"})

        self.assertEqual(len(fake.calls), 1)
        self.assertIn("query WorkstreamIssues", fake.calls[0][0])

    def test_root_snapshot_is_bounded_and_token_addressable(self):
        fake = FakeClient()
        transport = LinearGraphQLTransport(fake, team_id="team")
        transport.apply_reviewed_plan(self.plan(), accepted_keys={"a"})
        snapshot = transport.snapshot_for_root("GEN-1")
        self.assertEqual(snapshot["root"]["identifier"], "GEN-1")
        self.assertEqual(len(snapshot["children"]), 1)
        self.assertEqual(snapshot["children"][0]["status"], "Todo")

    def test_snapshot_paginates_before_resolving_root_and_children(self):
        class PagedClient:
            def __init__(self):
                self.afters = []

            def execute(self, query, variables):
                self.afters.append(variables["after"])
                if variables["after"] is None:
                    return {"team": {"issues": {
                        "nodes": [{"id": "unrelated", "identifier": "GEN-999"}],
                        "pageInfo": {"hasNextPage": True, "endCursor": "page-2"},
                    }}}
                return {"team": {"issues": {
                    "nodes": [
                        {
                            "id": "root", "identifier": "GEN-37", "title": "Root",
                            "description": "Plan revision: plan\nLedger revision: 2\nCurrent next action: Continue.",
                            "url": "https://linear.test/GEN-37",
                            "state": {"name": "In Progress"}, "parent": None,
                        },
                        {
                            "id": "child", "identifier": "GEN-38", "title": "Child",
                            "description": "Current next action: Finish.",
                            "url": "https://linear.test/GEN-38",
                            "state": {"name": "Todo"}, "parent": {"id": "root"},
                        },
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }}}

        client = PagedClient()
        snapshot = LinearGraphQLTransport(client, team_id="team").snapshot_for_root("GEN-37")
        self.assertEqual(client.afters, [None, "page-2"])
        self.assertEqual(snapshot["root"]["identifier"], "GEN-37")
        self.assertEqual([child["identifier"] for child in snapshot["children"]], ["GEN-38"])

    def test_next_action_parser_accepts_plain_and_markdown_bold_labels(self):
        self.assertEqual(
            parse_next_action("Current next action (2026-08-20): Re-run the canary."),
            "Re-run the canary.",
        )
        self.assertEqual(
            parse_next_action("**Current next action (2026-08-21):** Review the receipt."),
            "Review the receipt.",
        )
        self.assertEqual(
            parse_next_action("- **Current next action:** Resume from the root."),
            "Resume from the root.",
        )
        self.assertEqual(
            parse_next_action("Next action: Continue the live proof."),
            "Continue the live proof.",
        )
        self.assertEqual(
            parse_next_action(
                "Acceptance: preserve durable state. Next action: Run the canary."
            ),
            "Run the canary.",
        )
        self.assertIsNone(parse_next_action("MCP transport next action: not canonical"))

    def test_live_root_legacy_revision_labels_are_readable(self):
        description = (
            "Exact intake identity: plan revision SHA-256 "
            "`458a99c16cec2cdc649e26bb973fcfb0eb28f7a9e1b05335a78272db8745ffa1`.\n\n"
            "Ledger CAS revision: 1 (adapter-owned material-state revision)."
        )
        self.assertEqual(
            parse_plan_revision(description),
            "458a99c16cec2cdc649e26bb973fcfb0eb28f7a9e1b05335a78272db8745ffa1",
        )
        self.assertEqual(parse_root_revision(description), 1)

    def test_live_snapshot_resume_uses_next_actions_from_descriptions(self):
        fake = FakeClient()
        fake.issues = [
            {
                "id": "id-1", "identifier": "GEN-1", "title": "Demo",
                "description": "Plan revision: sha-demo\nLedger revision: 3\n**Current next action (2026-08-20):** Resume safely.",
                "url": "https://linear.test/1", "updatedAt": "now",
                "state": {"name": "In Progress", "type": "started"}, "parent": None,
            },
            {
                "id": "id-2", "identifier": "GEN-2", "title": "Build",
                "description": "Current next action (2026-08-20): Run focused tests.",
                "url": "https://linear.test/2", "updatedAt": "now",
                "state": {"name": "Todo", "type": "unstarted"},
                "parent": {"id": "id-1", "identifier": "GEN-1"},
            },
        ]
        snapshot = LinearGraphQLTransport(fake, team_id="team").snapshot_for_root("GEN-1")
        self.assertEqual(snapshot["root"]["next_action"], "Resume safely.")
        self.assertEqual(snapshot["children"][0]["next_action"], "Run focused tests.")

        from workstream_resume import compact_context
        context = compact_context(snapshot, "GEN-1")
        self.assertEqual(context["next_action"], "Resume safely.")
        self.assertEqual(context["children"][0]["next_action"], "Run focused tests.")


if __name__ == "__main__":
    unittest.main()
