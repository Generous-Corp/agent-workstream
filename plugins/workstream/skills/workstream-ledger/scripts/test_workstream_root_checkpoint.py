#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from workstream_delta import Delta
from workstream_linear_events import LinearCommentEventAdapter
from workstream_generation import strict_candidate_loader
from workstream_resume import ResumeError
from workstream_root_transition import RootTransitionError
import workstream_root_checkpoint as checkpoint_cli
import test_workstream_generation_transition as fixture


class RootCheckpointTests(unittest.TestCase):
    def test_gen14_sized_legacy_root_checkpoints_resumes_and_replays(self):
        token = "GEN-14"
        route = {
            **fixture.AUTHORITY,
            "root_issue_id": fixture.AUTHORITY["root_issue_id"],
        }
        with patch.object(fixture, "WORKSTREAM", token):
            client = fixture.FakeClient()
            plan = tempfile.NamedTemporaryFile("w+", suffix=".md")
            self.addCleanup(plan.close)
            plan_text = "# GEN-14 checkpoint fixture\n"
            plan.write(plan_text)
            plan.flush()
            digest = hashlib.sha256(plan_text.encode()).hexdigest()
            client.description = (
                f"Plan revision: {digest}\nNext action: Continue migration."
            )
            fixture.project_full(client, digest, identity=plan.name)
            material = LinearCommentEventAdapter(
                client, issue_id=token, plan_revision=digest, **route,
            )
            for revision in range(180):
                material.apply(Delta(
                    f"material-{revision}", token, "requirement",
                    f"requirement-{revision}",
                    {"requirement": f"preserve-{revision}"},
                    revision, f"t-{revision:03d}",
                ))

            common = [
                token, "--boundary-id", "material-180",
                "--created-at", "2026-09-01T06:30:00Z",
                "--agent", "codex", "--provider", "openai",
                "--session-id", "session-1", "--machine", "M5",
                "--worktree-state", "safe",
                "--worktree-path", "/tmp/gen14",
                "--worktree-branch", "gen14",
                "--worktree-head", "e" * 40,
                "--exact-head", "e" * 40,
                "--before-status", "In Progress",
                "--after-status", "In Progress",
                "--next-action", "Prepare the reviewed plan generation.",
            ]
            with patch.object(
                checkpoint_cli, "_client_and_route",
                return_value=(client, route),
            ):
                loader = strict_candidate_loader(
                    client, token=token, authority=route,
                    plan_source=plan.name, plan_identity=plan.name,
                    max_bytes=24 * 1024, max_items=100,
                )
                with self.assertRaisesRegex(
                    ResumeError, "resume_context_over_item_budget",
                ):
                    loader(digest)
                preview = checkpoint_cli.run(common)
                self.assertFalse(preview["apply"])
                self.assertEqual(preview["material_revision"], 180)
                self.assertEqual(preview["writes_performed"], 0)
                resume = {
                    "resume_authority": "full", "executable": True,
                    "plan_revision": digest, "dependency_graph": {},
                    "latest_checkpoint": {
                        "checkpoint_event_id": preview["checkpoint"]["event_id"],
                        "root_revision": 180,
                        "acknowledgement": {
                            "remote_id": preview["deterministic_slot_id"],
                        },
                    },
                }
                with patch.object(
                    checkpoint_cli, "_ordinary_resume", return_value=resume,
                ):
                    applied = checkpoint_cli.run([
                        *common, "--apply", "--expected-material-revision", "180",
                        "--expected-preview-sha256", preview["preview_sha256"],
                    ])
                self.assertEqual(applied["resume_authority"], "full")
                self.assertEqual(applied["writes_performed"], 2)
                self.assertEqual(loader(digest)["resume_authority"], "full")
                writes = len(client.mutations)
                with patch.object(
                    checkpoint_cli, "_ordinary_resume", return_value=resume,
                ):
                    replay = checkpoint_cli.run([
                        *common, "--apply", "--expected-material-revision", "180",
                        "--expected-preview-sha256", preview["preview_sha256"],
                    ])
                self.assertEqual(replay["resume_authority"], "full")
                self.assertEqual(replay["writes_performed"], 0)
                self.assertEqual(len(client.mutations), writes)

    def test_apply_requires_reviewed_fences_before_live_access(self):
        with self.assertRaisesRegex(
            ValueError, "requires expected material revision and preview digest",
        ):
            checkpoint_cli.run([
                "GEN-14", "--boundary-id", "b", "--agent", "codex",
                "--created-at", "2026-09-01T06:30:00Z",
                "--provider", "openai", "--session-id", "s",
                "--machine", "M5", "--worktree-state", "unknown",
                "--before-status", "In Progress",
                "--after-status", "In Progress", "--next-action", "Continue",
                "--apply",
            ])

    def test_child_token_refuses_root_checkpoint(self):
        token = "GEN-72"
        route = fixture.AUTHORITY
        with patch.object(fixture, "WORKSTREAM", token):
            client = fixture.FakeClient()
            original = client.root_issue

            def child_issue():
                issue = original()
                issue["parent"] = {"id": "parent", "identifier": "GEN-14"}
                return issue

            client.root_issue = child_issue
            with tempfile.NamedTemporaryFile("w+", suffix=".md") as plan:
                plan.write("# child\n")
                plan.flush()
                digest = hashlib.sha256(b"# child\n").hexdigest()
                client.description = (
                    f"Plan revision: {digest}\nNext action: Continue."
                )
                fixture.project_full(client, digest, identity=plan.name)
                with patch.object(
                    checkpoint_cli, "_client_and_route",
                    return_value=(client, route),
                ), self.assertRaisesRegex(
                    RootTransitionError, "requires_active_root_issue",
                ):
                    checkpoint_cli.run([
                        token, "--created-at", "2026-09-01T06:30:00Z",
                        "--agent", "codex", "--provider", "openai",
                        "--session-id", "s", "--machine", "M5",
                        "--worktree-state", "unknown",
                        "--before-status", "In Progress",
                        "--after-status", "In Progress",
                        "--next-action", "Continue.",
                    ])

    def test_ordinary_resume_oracle_uses_production_command_and_budget_contract(self):
        payload = {
            "resume_authority": "full", "executable": True,
            "dependency_graph": {},
        }
        completed = subprocess.CompletedProcess(
            [], 0, stdout=json.dumps(payload), stderr="",
        )
        with patch.object(
            checkpoint_cli.subprocess, "run", return_value=completed,
        ) as invoked:
            self.assertEqual(
                checkpoint_cli._ordinary_resume("GEN-14"), payload,
            )
        command = invoked.call_args.args[0]
        self.assertEqual(command[-1], "GEN-14")
        self.assertTrue(command[-2].endswith("workstream_resume.py"))
        self.assertEqual(invoked.call_args.kwargs["timeout"], 60)

        completed.stdout = json.dumps({
            "resume_authority": "full", "executable": True,
        })
        with patch.object(
            checkpoint_cli.subprocess, "run", return_value=completed,
        ), self.assertRaisesRegex(
            Exception, "ordinary_resume_not_bounded_full",
        ):
            checkpoint_cli._ordinary_resume("GEN-14")


if __name__ == "__main__":
    unittest.main()
