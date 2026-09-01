#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import unittest
from copy import deepcopy
from unittest.mock import patch

from workstream_delta import Delta
from workstream_linear_events import LinearCommentEventAdapter
from workstream_linear_checkpoints import (
    encode_checkpoint_comment, LinearCheckpointAdapter,
)
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
                    "resume_authority": "full", "workstream_id": token,
                    "plan_revision": digest,
                    "source": {"identity": plan.name, "sha256": digest},
                    "authenticated_route": route,
                    "dependency_graph": {
                        "route": route, "plan_revision": digest,
                    },
                    "latest_checkpoint": {
                        "checkpoint_event_id": preview["checkpoint"]["event_id"],
                        "root_revision": 180,
                        "acknowledgement": {
                            "remote_id": preview["deterministic_slot_id"],
                        },
                    },
                }
                # Simulate process death after the checkpoint append and
                # before the reviewed disposition candidate is written.
                update_count = 0

                def linear_updates_root_timestamp(_item, observed):
                    nonlocal update_count
                    update_count += 1
                    observed.graph_nonce = f"comment-write-{update_count}"

                client.before_each_create = linear_updates_root_timestamp
                LinearCheckpointAdapter(
                    client, issue_id=token, workstream_id=token,
                    workspace_id=route["workspace_id"],
                    team_id=route["team_id"], project_id=route["project_id"],
                ).persist(preview["checkpoint"])
                checkpoint_only_preview = checkpoint_cli.run(common)
                self.assertNotEqual(
                    checkpoint_only_preview["preview_sha256"],
                    preview["preview_sha256"],
                )
                original_persist = LinearCheckpointAdapter.persist

                def no_op_replay_with_unrelated_timestamp_drift(
                    adapter, event, **kwargs,
                ):
                    receipt = original_persist(adapter, event, **kwargs)
                    client.graph_nonce = "unrelated-no-op-replay-drift"
                    return receipt

                with patch.object(
                    LinearCheckpointAdapter, "persist",
                    new=no_op_replay_with_unrelated_timestamp_drift,
                ), self.assertRaises(
                    checkpoint_cli.CheckpointPartialApplyError,
                ) as raised:
                    checkpoint_cli.run([
                        *common, "--apply", "--expected-material-revision", "180",
                        "--expected-preview-sha256",
                        checkpoint_only_preview["preview_sha256"],
                    ])
                self.assertEqual(
                    raised.exception.payload["reason"],
                    "checkpoint_applied_but_native_root_drift",
                )
                checkpoint_only_preview = checkpoint_cli.run(common)
                original_append = checkpoint_cli.LinearProjectionAdapter.append

                def disposition_commits_then_response_is_lost(
                    projection_adapter, event, **kwargs,
                ):
                    original_append(
                        projection_adapter, event, **kwargs,
                    )
                    raise checkpoint_cli.LinearTransportError(
                        "lost projection response after commit",
                    )

                writes_before_apply = len(client.mutations)
                with patch.object(
                    checkpoint_cli, "_ordinary_resume", return_value=resume,
                ), patch.object(
                    checkpoint_cli.LinearProjectionAdapter, "append",
                    new=disposition_commits_then_response_is_lost,
                ):
                    applied = checkpoint_cli.run([
                        *common, "--apply", "--expected-material-revision", "180",
                        "--expected-preview-sha256",
                        checkpoint_only_preview["preview_sha256"],
                    ])
                self.assertEqual(applied["resume_authority"], "full")
                self.assertEqual(applied["writes_performed"], 1)
                self.assertEqual(len(client.mutations), writes_before_apply + 1)
                self.assertEqual(loader(digest)["resume_authority"], "full")
                writes = len(client.mutations)
                replay_preview = checkpoint_cli.run(common)
                with patch.object(
                    checkpoint_cli, "_ordinary_resume", return_value=resume,
                ):
                    replay = checkpoint_cli.run([
                        *common, "--apply", "--expected-material-revision", "180",
                        "--expected-preview-sha256",
                        replay_preview["preview_sha256"],
                    ])
                self.assertEqual(replay["resume_authority"], "full")
                self.assertEqual(replay["writes_performed"], 0)
                self.assertEqual(len(client.mutations), writes)

    def test_description_and_child_drift_during_own_append_still_refuse(self):
        token = "GEN-14"
        route = fixture.AUTHORITY

        class ChildAwareClient(fixture.FakeClient):
            def execute(self, query, variables):
                result = super().execute(query, variables)
                if "query WorkstreamResumeRoot" in query:
                    result["issue"]["children"]["nodes"] = deepcopy(
                        self.children,
                    )
                return result

        child = {
            "id": "child-id", "identifier": "GEN-72", "title": "Child",
            "description": "Next action: Continue.",
            "url": "https://linear.test/GEN-72", "updatedAt": "child-time",
            "archivedAt": None,
            "parent": {"id": route["root_issue_id"], "identifier": token},
            "project": {"id": route["project_id"]},
            "team": {
                "id": route["team_id"],
                "organization": {"id": route["workspace_id"]},
            },
            "assignee": None,
            "state": {"id": "child-state", "name": "In Progress",
                      "type": "started"},
            "comments": {"nodes": [], "pageInfo": {
                "hasNextPage": False, "endCursor": None,
            }},
        }
        with patch.object(fixture, "WORKSTREAM", token):
            for drift_kind in ("description", "child"):
                with self.subTest(drift_kind=drift_kind):
                    client = ChildAwareClient()
                    with tempfile.NamedTemporaryFile("w+", suffix=".md") as plan:
                        text = f"# {drift_kind} drift\n"
                        plan.write(text)
                        plan.flush()
                        digest = hashlib.sha256(text.encode()).hexdigest()
                        client.description = (
                            f"Plan revision: {digest}\nNext action: Continue."
                        )
                        fixture.project_full(client, digest, identity=plan.name)
                        common = [
                            token, "--created-at", "2026-09-01T06:30:00Z",
                            "--agent", "codex", "--provider", "openai",
                            "--session-id", "s", "--machine", "M5",
                            "--worktree-state", "unknown",
                            "--before-status", "In Progress",
                            "--after-status", "In Progress",
                            "--next-action", "Continue",
                        ]
                        with patch.object(
                            checkpoint_cli, "_client_and_route",
                            return_value=(client, route),
                        ):
                            preview = checkpoint_cli.run(common)

                            def drift(item, observed):
                                if "workstream-checkpoint:v1" not in item["body"]:
                                    return
                                if drift_kind == "description":
                                    observed.description += "\nConcurrent edit."
                                else:
                                    observed.children.append(deepcopy(child))

                            client.before_each_create = drift
                            with self.assertRaises(
                                checkpoint_cli.CheckpointPartialApplyError,
                            ) as raised:
                                checkpoint_cli.run([
                                    *common, "--apply",
                                    "--expected-material-revision", "0",
                                    "--expected-preview-sha256",
                                    preview["preview_sha256"],
                                ])
                        self.assertEqual(
                            raised.exception.payload["reason"],
                            "checkpoint_applied_but_native_root_drift",
                        )

    def test_exact_checkpoint_at_wrong_remote_slot_refuses_before_projection(self):
        token = "GEN-14"
        route = fixture.AUTHORITY
        with patch.object(fixture, "WORKSTREAM", token):
            client = fixture.FakeClient()
            with tempfile.NamedTemporaryFile("w+", suffix=".md") as plan:
                text = "# wrong slot\n"
                plan.write(text)
                plan.flush()
                digest = hashlib.sha256(text.encode()).hexdigest()
                client.description = (
                    f"Plan revision: {digest}\nNext action: Continue."
                )
                fixture.project_full(client, digest, identity=plan.name)
                common = [
                    token, "--created-at", "2026-09-01T06:30:00Z",
                    "--agent", "codex", "--provider", "openai",
                    "--session-id", "s", "--machine", "M5",
                    "--worktree-state", "unknown",
                    "--before-status", "In Progress",
                    "--after-status", "In Progress", "--next-action", "Continue",
                ]
                with patch.object(
                    checkpoint_cli, "_client_and_route",
                    return_value=(client, route),
                ):
                    preview = checkpoint_cli.run(common)
                    client.comments.append({
                        "id": "wrong-remote-slot",
                        "body": encode_checkpoint_comment(preview["checkpoint"]),
                        "createdAt": "now", "updatedAt": "now",
                    })
                    with self.assertRaises(
                        checkpoint_cli.CheckpointPartialApplyError,
                    ) as raised:
                        checkpoint_cli.run([
                            *common, "--apply",
                            "--expected-material-revision", "0",
                            "--expected-preview-sha256", preview["preview_sha256"],
                        ])
                self.assertEqual(
                    raised.exception.payload["reason"],
                    "checkpoint_receipt_remote_slot_mismatch",
                )
                self.assertEqual(
                    raised.exception.payload["checkpoint"]["receipt"]
                    ["acknowledgement"]["remote_id"],
                    "wrong-remote-slot",
                )

    def test_native_state_id_drift_during_checkpoint_is_applied_but_not_success(self):
        token = "GEN-14"
        route = fixture.AUTHORITY
        with patch.object(fixture, "WORKSTREAM", token):
            client = fixture.FakeClient()
            with tempfile.NamedTemporaryFile("w+", suffix=".md") as plan:
                text = "# state drift\n"
                plan.write(text)
                plan.flush()
                digest = hashlib.sha256(text.encode()).hexdigest()
                client.description = (
                    f"Plan revision: {digest}\nNext action: Continue."
                )
                fixture.project_full(client, digest, identity=plan.name)
                common = [
                    token, "--created-at", "2026-09-01T06:30:00Z",
                    "--agent", "codex", "--provider", "openai",
                    "--session-id", "s", "--machine", "M5",
                    "--worktree-state", "unknown",
                    "--before-status", "In Progress",
                    "--after-status", "In Progress", "--next-action", "Continue",
                ]
                with patch.object(
                    checkpoint_cli, "_client_and_route",
                    return_value=(client, route),
                ):
                    preview = checkpoint_cli.run(common)

                    def drift(item, observed):
                        if "workstream-checkpoint:v1" in item["body"]:
                            observed.graph_state_id = "changed-state-id"

                    client.before_each_create = drift
                    with self.assertRaises(
                        checkpoint_cli.CheckpointPartialApplyError,
                    ) as raised:
                        checkpoint_cli.run([
                            *common, "--apply",
                            "--expected-material-revision", "0",
                            "--expected-preview-sha256", preview["preview_sha256"],
                        ])
                payload = raised.exception.payload
                self.assertEqual(
                    payload["reason"],
                    "checkpoint_applied_but_native_root_drift",
                )
                self.assertEqual(
                    payload["checkpoint"]["receipt"]["event_id"],
                    preview["checkpoint"]["event_id"],
                )
                self.assertEqual(
                    payload["replay_guidance"].split()[0], "Rerun",
                )

    def test_native_state_id_drift_during_resume_is_applied_but_not_success(self):
        token = "GEN-14"
        route = fixture.AUTHORITY
        with patch.object(fixture, "WORKSTREAM", token):
            client = fixture.FakeClient()
            with tempfile.NamedTemporaryFile("w+", suffix=".md") as plan:
                text = "# resume state drift\n"
                plan.write(text)
                plan.flush()
                digest = hashlib.sha256(text.encode()).hexdigest()
                source = {"identity": plan.name, "sha256": digest}
                client.description = (
                    f"Plan revision: {digest}\nNext action: Continue."
                )
                fixture.project_full(client, digest, identity=plan.name)
                common = [
                    token, "--created-at", "2026-09-01T06:30:00Z",
                    "--agent", "codex", "--provider", "openai",
                    "--session-id", "s", "--machine", "M5",
                    "--worktree-state", "unknown",
                    "--before-status", "In Progress",
                    "--after-status", "In Progress", "--next-action", "Continue",
                ]
                with patch.object(
                    checkpoint_cli, "_client_and_route",
                    return_value=(client, route),
                ):
                    preview = checkpoint_cli.run(common)
                    resume = {
                        "resume_authority": "full", "workstream_id": token,
                        "plan_revision": digest, "source": source,
                        "authenticated_route": route,
                        "dependency_graph": {
                            "route": route, "plan_revision": digest,
                        },
                        "latest_checkpoint": {
                            "checkpoint_event_id": preview["checkpoint"]["event_id"],
                            "root_revision": 0,
                            "acknowledgement": {
                                "remote_id": preview["deterministic_slot_id"],
                            },
                        },
                    }

                    def resume_then_drift(*_args, **_kwargs):
                        client.graph_state_id = "drifted-during-resume"
                        return resume

                    with patch.object(
                        checkpoint_cli, "_ordinary_resume",
                        side_effect=resume_then_drift,
                    ), self.assertRaises(
                        checkpoint_cli.CheckpointPartialApplyError,
                    ) as raised:
                        checkpoint_cli.run([
                            *common, "--apply",
                            "--expected-material-revision", "0",
                            "--expected-preview-sha256", preview["preview_sha256"],
                        ])
                payload = raised.exception.payload
                self.assertEqual(
                    payload["reason"],
                    "checkpoint_applied_but_resume_native_root_drift",
                )
                self.assertEqual(
                    payload["checkpoint"]["receipt"]["event_id"],
                    preview["checkpoint"]["event_id"],
                )
                self.assertEqual(
                    payload["projection"]["receipt"]["event_id"],
                    preview["projection_candidate"]["event_id"],
                )

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
        route = fixture.AUTHORITY
        source = {"identity": "/tmp/plan.md", "sha256": "a" * 64}
        args = checkpoint_cli.parser().parse_args([
            "GEN-14", "--config", "/tmp/workstream.json",
            "--created-at", "2026-09-01T06:30:00Z",
            "--agent", "codex", "--provider", "openai",
            "--session-id", "s", "--machine", "M5",
            "--worktree-state", "unknown",
            "--before-status", "In Progress",
            "--after-status", "In Progress", "--next-action", "Continue",
        ])
        payload = {
            "resume_authority": "full", "workstream_id": "GEN-14",
            "plan_revision": source["sha256"], "source": source,
            "authenticated_route": route,
            "dependency_graph": {
                "route": route, "plan_revision": source["sha256"],
            },
        }
        completed = subprocess.CompletedProcess(
            [], 0, stdout=json.dumps(payload), stderr="",
        )
        with patch.object(
            checkpoint_cli.subprocess, "run", return_value=completed,
        ) as invoked:
            self.assertEqual(
                checkpoint_cli._ordinary_resume(
                    "GEN-14", args=args, route=route, source=source,
                ),
                payload,
            )
        command = invoked.call_args.args[0]
        self.assertEqual(command[2], "GEN-14")
        self.assertTrue(command[1].endswith("workstream_resume.py"))
        for flag, expected in (
            ("--config", "/tmp/workstream.json"),
            ("--linear-workspace-id", route["workspace_id"]),
            ("--linear-team-id", route["team_id"]),
            ("--linear-project-id", route["project_id"]),
            ("--linear-endpoint", args.linear_endpoint),
            ("--plan-source", source["identity"]),
            ("--plan-identity", source["identity"]),
            ("--max-bytes", str(24 * 1024)), ("--max-items", "100"),
        ):
            self.assertEqual(command[command.index(flag) + 1], expected)
        self.assertEqual(invoked.call_args.kwargs["timeout"], 60)

        completed.stdout = json.dumps({
            **payload, "dependency_graph": None,
        })
        with patch.object(
            checkpoint_cli.subprocess, "run", return_value=completed,
        ), self.assertRaisesRegex(
            Exception, "ordinary_resume_not_bounded_full",
        ):
            checkpoint_cli._ordinary_resume(
                "GEN-14", args=args, route=route, source=source,
            )


if __name__ == "__main__":
    unittest.main()
