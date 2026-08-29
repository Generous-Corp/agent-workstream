#!/usr/bin/env python3
import unittest
from unittest import mock

from workstream_graph import GraphReviewRequired
import workstream_extend_child


class FakeTransport:
    calls = []

    def __init__(self, client, **route):
        self.client = client
        self.route = route

    def extend_existing_root_reviewed_child(self, plan, **values):
        type(self).calls.append((plan, values, self.route, self.client))
        return {
            "source": plan["source"],
            "plan_revision": plan["root"]["plan_revision"],
            "route": self.route,
            "frontier": values["expected_frontier"],
            "authorization": {
                "event": {"event_id": "wsp-grant"},
                "remote_id": "comment-grant",
                "revision": values["expected_frontier"]["projection_revision"] + 1,
            },
            "initial_state": "planned_pending_projection",
            "receipt": {
                "stable_key": values["reviewed_candidate_key"],
                "id": "00000000-0000-4000-8000-000000000123",
                "identifier": "GEN-123",
                "url": "https://linear.test/GEN-123",
                "title": "Shipyard resume report",
                "parent_id": values["root_issue_id"],
                "updated_at": "now",
                "disposition": "created",
            },
        }


class FakeProjection:
    calls = []

    def __init__(self, client, **values):
        self.client = client
        self.values = values
        type(self).calls.append((client, values))


class WorkstreamExtendChildTests(unittest.TestCase):
    PLAN_REVISION = "a" * 64
    ROOT_ID = "409c1423-f949-4655-9f5f-d3213d7b434f"

    def setUp(self):
        FakeTransport.calls = []
        FakeProjection.calls = []

    @classmethod
    def args(cls):
        return [
            "plan.md",
            "--identity", "https://example.test/plan.md",
            "--plan-revision", cls.PLAN_REVISION,
            "--workstream-id", "GEN-37",
            "--root-issue-id", cls.ROOT_ID,
            "--candidate-key", "item-resume-report",
            "--material-revision", "51",
            "--projection-revision", "73",
            "--workspace-id", "workspace",
            "--team-id", "team",
            "--project-id", "project",
            "--apply",
        ]

    @classmethod
    def plan(cls):
        return {
            "schema_version": 1,
            "source": {
                "identity": "https://example.test/plan.md",
                "sha256": cls.PLAN_REVISION,
                "bytes": 100,
            },
            "root": {
                "stable_key": "source-demo",
                "title": "Demo",
                "plan_revision": cls.PLAN_REVISION,
            },
            "children": [{
                "key": "item-resume-report",
                "title": "Shipyard resume report",
                "next_action": "Implement the report",
            }],
            "graph_review_required": True,
        }

    def run_success(self):
        client = object()
        with mock.patch.object(
            workstream_extend_child, "plan_payload", return_value=self.plan()
        ), mock.patch.object(
            workstream_extend_child, "load_linear_api_key", return_value="secret"
        ), mock.patch.object(
            workstream_extend_child, "LinearGraphQLTransport", FakeTransport
        ), mock.patch.object(
            workstream_extend_child, "LinearProjectionAdapter", FakeProjection
        ):
            result = workstream_extend_child.run(
                self.args(), client_factory=lambda token: client
            )
        return result, client

    def test_exact_request_wires_one_existing_root_child_extension(self):
        result, client = self.run_success()

        self.assertEqual(result["workstream_id"], "GEN-37")
        self.assertEqual(result["receipt"]["identifier"], "GEN-123")
        self.assertEqual(result["receipt"]["parent_id"], self.ROOT_ID)
        self.assertEqual(result["initial_state"], "planned_pending_projection")
        self.assertEqual(result["frontier"], {
            "material_revision": 51, "projection_revision": 73,
        })
        self.assertNotIn("secret", repr(result))
        self.assertEqual(len(FakeTransport.calls), 1)
        _plan, values, route, observed_client = FakeTransport.calls[0]
        self.assertIs(observed_client, client)
        self.assertEqual(route, {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project",
        })
        self.assertEqual(values["root_issue_id"], self.ROOT_ID)
        self.assertEqual(values["reviewed_candidate_key"], "item-resume-report")
        self.assertIsInstance(values["authorization_adapter"], FakeProjection)
        self.assertIs(FakeProjection.calls[0][0], client)
        self.assertEqual(FakeProjection.calls[0][1]["root_issue_id"], self.ROOT_ID)

    def test_missing_apply_refuses_before_source_auth_or_network(self):
        args = self.args()
        args.remove("--apply")
        with mock.patch.object(workstream_extend_child, "plan_payload") as source, \
             mock.patch.object(workstream_extend_child, "load_linear_api_key") as auth:
            with self.assertRaises(SystemExit):
                workstream_extend_child.run(args)
        source.assert_not_called()
        auth.assert_not_called()

    def test_changed_plan_refuses_before_auth_or_network(self):
        plan = self.plan()
        plan["root"]["plan_revision"] = "b" * 64
        client_factory = mock.Mock()
        with mock.patch.object(
            workstream_extend_child, "plan_payload", return_value=plan
        ), mock.patch.object(
            workstream_extend_child, "load_linear_api_key"
        ) as auth:
            with self.assertRaisesRegex(GraphReviewRequired, "changed after review"):
                workstream_extend_child.run(
                    self.args(), client_factory=client_factory
                )
        auth.assert_not_called()
        client_factory.assert_not_called()

    def test_unknown_candidate_refuses_before_auth_or_network(self):
        client_factory = mock.Mock()
        with mock.patch.object(
            workstream_extend_child, "plan_payload", return_value=self.plan()
        ), mock.patch.object(
            workstream_extend_child, "load_linear_api_key"
        ) as auth:
            args = self.args()
            args[args.index("item-resume-report")] = "missing-candidate"
            with self.assertRaisesRegex(GraphReviewRequired, "did not reproduce"):
                workstream_extend_child.run(args, client_factory=client_factory)
        auth.assert_not_called()
        client_factory.assert_not_called()

    def test_partial_route_refuses_before_auth(self):
        args = self.args()
        for flag, value in (("--workspace-id", "workspace"), ("--project-id", "project")):
            index = args.index(flag)
            del args[index:index + 2]
        with mock.patch.object(
            workstream_extend_child, "plan_payload", return_value=self.plan()
        ), mock.patch.object(
            workstream_extend_child, "load_linear_api_key"
        ) as auth:
            with self.assertRaises(ValueError):
                workstream_extend_child.run(args)
        auth.assert_not_called()


if __name__ == "__main__":
    unittest.main()
