#!/usr/bin/env python3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from test_workstream_linear import FakeClient
from workstream_graph import GraphReviewRequired
import workstream_intake


class WorkstreamIntakeTests(unittest.TestCase):
    def write_plan(self):
        temp = tempfile.TemporaryDirectory()
        path = Path(temp.name) / "plan.md"
        path.write_text("# Demo\n\n## Build\n\n## Verify\n", encoding="utf-8")
        self.addCleanup(temp.cleanup)
        return path

    @staticmethod
    def route_args():
        return [
            "--workspace-id", "workspace",
            "--team-id", "team",
            "--project-id", "project",
        ]

    def test_intake_requires_an_explicit_review_before_reading_source(self):
        with self.assertRaises(GraphReviewRequired):
            workstream_intake.run(
                ["/does/not/exist.md", "--identity", "plan:demo",
                 "--plan-revision", "0" * 64, *self.route_args()]
            )

    def test_intake_requires_a_complete_explicit_route(self):
        path = self.write_plan()
        revision = workstream_intake.plan_payload(str(path), "plan:demo")["root"]["plan_revision"]
        with self.assertRaisesRegex(ValueError, "complete Linear"):
            workstream_intake.run([
                str(path), "--identity", "plan:demo", "--plan-revision", revision,
                "--accept-none",
            ])

    def test_accept_none_creates_only_the_reviewed_root(self):
        path = self.write_plan()
        fake = FakeClient()
        preview = workstream_intake.plan_payload(str(path), "plan:demo")
        with mock.patch.object(workstream_intake, "load_linear_api_key", return_value="secret"):
            result = workstream_intake.run([
                str(path), "--identity", "plan:demo",
                "--plan-revision", preview["root"]["plan_revision"],
                "--accept-none", *self.route_args(),
            ], client_factory=lambda token: fake)

        self.assertEqual(len(fake.issues), 1)
        self.assertEqual(result["receipts"]["children"], [])

    def test_reviewed_intake_returns_exact_root_and_child_receipts(self):
        path = self.write_plan()
        fake = FakeClient()
        preview = workstream_intake.plan_payload(str(path), "plan:demo")
        accepted = preview["children"][0]["key"]
        with mock.patch.object(workstream_intake, "load_linear_api_key", return_value="secret"):
            result = workstream_intake.run([
                str(path), "--identity", "plan:demo",
                "--plan-revision", preview["root"]["plan_revision"],
                "--accept-key", accepted, *self.route_args(),
            ], client_factory=lambda token: fake)

        self.assertEqual(result["source"]["identity"], "plan:demo")
        self.assertEqual(result["route"], {
            "workspace_id": "workspace", "team_id": "team", "project_id": "project",
        })
        self.assertEqual(result["receipts"]["root"]["identifier"], "GEN-1")
        self.assertEqual(
            [item["stable_key"] for item in result["receipts"]["children"]],
            [accepted],
        )
        self.assertNotIn("secret", repr(result))

    def test_unknown_reviewed_key_never_mutates(self):
        path = self.write_plan()
        fake = FakeClient()
        revision = workstream_intake.plan_payload(str(path), "plan:demo")["root"]["plan_revision"]
        with mock.patch.object(workstream_intake, "load_linear_api_key", return_value="secret"):
            with self.assertRaisesRegex(GraphReviewRequired, "not present"):
                workstream_intake.run([
                    str(path), "--identity", "plan:demo",
                    "--plan-revision", revision,
                    "--accept-key", "not-a-candidate", *self.route_args(),
                ], client_factory=lambda token: fake)
        self.assertEqual(fake.calls, [])

    def test_changed_source_after_review_refuses_before_auth_or_network(self):
        path = self.write_plan()
        fake = FakeClient()
        with mock.patch.object(workstream_intake, "load_linear_api_key") as auth:
            with self.assertRaisesRegex(GraphReviewRequired, "changed after review"):
                workstream_intake.run([
                    str(path), "--identity", "plan:demo",
                    "--plan-revision", "0" * 64,
                    "--accept-none", *self.route_args(),
                ], client_factory=lambda token: fake)
        auth.assert_not_called()
        self.assertEqual(fake.calls, [])


if __name__ == "__main__":
    unittest.main()
