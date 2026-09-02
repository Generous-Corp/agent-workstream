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
import workstream_resume as resume_cli
import workstream_root_checkpoint as checkpoint_cli
import test_workstream_generation_transition as fixture


class RootCheckpointTests(unittest.TestCase):
    def test_proposed_resume_rejects_stale_recovered_checkpoint_before_write(self):
        token = "GEN-37"
        route = fixture.AUTHORITY
        client = fixture.FakeClient()
        with tempfile.NamedTemporaryFile("w+", suffix=".md") as plan:
            text = "# stale recovery\n"
            plan.write(text); plan.flush()
            digest = hashlib.sha256(text.encode()).hexdigest()
            client.description = f"Plan revision: {digest}\nNext action: Continue"
            fixture.project_full(client, digest, identity=plan.name)
            graph = checkpoint_cli.LinearGraphQLTransport(
                client, workspace_id=route["workspace_id"],
                team_id=route["team_id"], project_id=route["project_id"],
            ).snapshot_for_root(token, include_description=True,
                                include_child_comments=True)
            comments = checkpoint_cli.LinearProjectionAdapter(
                client, issue_id=token, workstream_id=token,
                plan_revision=digest, **route,
            )._comments()
            checkpoint = checkpoint_cli.build_checkpoint(
                workstream_id=token, boundary_id="material-0", root_revision=0,
                plan_revision=digest, before_status="In Progress",
                after_status="In Progress", execution={"agent": "codex",
                "provider": "openai", "session_id": "s", "machine": "M5",
                "worktree": {"state": "safe", "path": "/tmp/x",
                "branch": "main", "head": "e" * 40}}, exact_head="e" * 40,
                evidence=[], blocker=None, next_action="Continue",
            )
            current = checkpoint_cli.reduce_projection_comments(
                comments, workstream_id=token, expected_plan_revision=digest,
                authenticated_route=route,
            ).events[-1]
            candidate = checkpoint_cli.build_projection_event(
                workstream_id=token, kind="disposition", key="root",
                value={"disposition": "attach", "remote_head": "e" * 40,
                       "recovered_from_checkpoint": "wsc_" + "0" * 32},
                plan_revision=digest, expected_revision=4, created_at="4",
                supersedes_event_id=current["event_id"], authority=route,
            )
            with self.assertRaisesRegex(
                checkpoint_cli.LinearTransportError,
                "checkpoint_proposed_resume_refused:disposition_checkpoint_stale",
            ):
                checkpoint_cli._validate_proposed_full_authority(
                    client=client, graph=graph, comments=comments,
                    checkpoint=checkpoint,
                    checkpoint_remote_id="slot-checkpoint",
                    projection_candidate=candidate, checkpoint_replay=False,
                    projection_replay=False, workstream_id=token, route=route,
                    source={"identity": plan.name, "sha256": digest},
                    selected_generation={"plan_revision": digest,
                    "description_plan_revision": digest,
                    "transition_tip_event_id": None,
                    "activation_epoch": None,
                    "authority_origin": "generation_genesis"},
                )

    def test_real_run_accepts_and_replays_multi_checkpoint_fixed_envelope(self):
        token = "GEN-37"
        route = {
            **fixture.AUTHORITY,
            "root_issue_id": fixture.AUTHORITY["root_issue_id"],
        }
        client = fixture.FakeClient()
        plan = tempfile.NamedTemporaryFile("w+", suffix=".md")
        self.addCleanup(plan.close)
        plan.write("# multi-checkpoint fixed-envelope fixture\n")
        plan.flush()
        digest = hashlib.sha256(
            b"# multi-checkpoint fixed-envelope fixture\n"
        ).hexdigest()
        # The authenticated producer carries derived byte metadata; the
        # authority contract must bind only identity+sha256 so the consumer's
        # canonical source remains stable across transports.
        source = {
            "identity": plan.name, "sha256": digest,
            "bytes": len(b"# multi-checkpoint fixed-envelope fixture\n"),
        }
        client.description = f"Plan revision: {digest}\nNext action: Continue"
        fixture.project_full(client, digest, identity=plan.name)
        material = LinearCommentEventAdapter(
            client, issue_id=token, plan_revision=digest, **route,
        )

        def command(boundary: int, session: str, created_at: str) -> list[str]:
            return [
                token, "--boundary-id", f"material-{boundary}",
                "--created-at", created_at, "--agent", "codex",
                "--provider", "openai", "--session-id", session,
                "--machine", "M5", "--worktree-state", "safe",
                "--worktree-path", "/tmp/gen37", "--worktree-branch", "gen37",
                "--worktree-head", "e" * 40, "--exact-head", "e" * 40,
                "--before-status", "In Progress", "--after-status", "In Progress",
                "--next-action", "Continue",
            ]

        with patch.object(fixture, "WORKSTREAM", token), patch.object(
            checkpoint_cli, "_client_and_route", return_value=(client, route),
        ):
            for revision in range(10):
                material.apply(Delta(
                    f"material-{revision}", token, "requirement",
                    f"requirement-{revision}", {"requirement": f"keep-{revision}"},
                    revision, f"t-{revision:03d}",
                ))
            predecessor_command = command(
                10, "session-1", "2026-09-01T06:30:00Z",
            )
            predecessor_preview = checkpoint_cli.run(predecessor_command)
            predecessor_resume = {
                "resume_authority": "full", "workstream_id": token,
                "plan_revision": digest, "source": source,
                "authenticated_route": route,
                "dependency_graph": {"route": route, "plan_revision": digest},
                "latest_checkpoint": {
                    "checkpoint_event_id": predecessor_preview["checkpoint"]["event_id"],
                    "root_revision": 10,
                    "acknowledgement": {
                        "remote_id": predecessor_preview["deterministic_slot_id"],
                    },
                },
            }
            with patch.object(
                checkpoint_cli, "_ordinary_resume", return_value=predecessor_resume,
            ):
                predecessor = checkpoint_cli.run([
                    *predecessor_command, "--apply",
                    "--expected-material-revision", "10",
                    "--expected-preview-sha256",
                    predecessor_preview["preview_sha256"],
                ])
            self.assertEqual(predecessor["resume_authority"], "full")

            for revision in range(10, 20):
                material.apply(Delta(
                    f"material-{revision}", token, "requirement",
                    f"requirement-{revision}", {"requirement": f"keep-{revision}"},
                    revision, f"t-{revision:03d}",
                ))
            successor_command = command(
                20, "session-2", "2026-09-01T06:31:00Z",
            )
            successor_preview = checkpoint_cli.run(successor_command)

            def live_fixed_envelope(*_args, **_kwargs):
                linear = checkpoint_cli.LinearGraphQLTransport(
                    client, workspace_id=route["workspace_id"],
                    team_id=route["team_id"], project_id=route["project_id"],
                )
                graph = linear.snapshot_for_root(
                    token, include_description=True, include_child_comments=True,
                )
                graph.pop("child_comments", None)
                comments = checkpoint_cli.LinearProjectionAdapter(
                    client, issue_id=token, workstream_id=token,
                    plan_revision=digest, **route,
                )._comments()
                joined = resume_cli.add_material_history(
                    graph, comments, token, authenticated_route=route,
                    authenticated_source=source,
                )
                full = resume_cli.compact_context(
                    joined, token, max_bytes=2_147_483_647,
                    max_items=2_147_483_647,
                    require_projection_authority=True,
                    require_dependency_graph=False,
                )
                # Keep cardinality within the production 100-item contract but
                # force the exact context handed to the fixed-envelope producer
                # beyond the ordinary 24 KiB byte budget.
                full["decisions"] = [{
                    "id": "D-oversized", "status": "accepted",
                    "next_action": "review-" + ("x" * (30 * 1024)),
                }]
                self.assertGreater(
                    len(resume_cli._default_output_bytes(full)), 24 * 1024,
                )
                checkpoint_adapter = LinearCheckpointAdapter(
                    client, issue_id=token, workstream_id=token,
                    workspace_id=route["workspace_id"],
                    team_id=route["team_id"], project_id=route["project_id"],
                )
                normalized_tip = checkpoint_adapter._recover_checkpoint_generations(
                    checkpoint_adapter._state(),
                )[digest]
                self.assertEqual(
                    len(normalized_tip["provenance_chain"]), 2,
                )
                envelope = resume_cli._fixed_frontier_authority_envelope(
                    full, token=token,
                    binding_checkpoint=normalized_tip,
                )
                envelope["deferred_audit_detail"].update({
                    "original_context_bytes": len(
                        resume_cli._default_output_bytes(full)
                    ),
                    "audit_route": {"launcher": "fixture"},
                    "full_history_route": {"launcher": "fixture"},
                })
                encoded = resume_cli._default_output_bytes(envelope)
                self.assertLessEqual(len(encoded), 24 * 1024)
                self.assertEqual(
                    envelope["context_schema"]["envelope"],
                    "fixed_frontier_authority_v1",
                )
                return subprocess.CompletedProcess(
                    [], 0, stdout=resume_cli._default_output_text(envelope),
                    stderr="",
                )

            writes_before = len(client.mutations)
            with patch.object(
                checkpoint_cli.subprocess, "run", side_effect=live_fixed_envelope,
            ):
                successor = checkpoint_cli.run([
                    *successor_command, "--apply",
                    "--expected-material-revision", "20",
                    "--expected-preview-sha256", successor_preview["preview_sha256"],
                ])
            self.assertEqual(successor["resume_authority"], "full")
            self.assertEqual(len(client.mutations), writes_before + 2)

            writes_before_replay = len(client.mutations)
            replay_preview = checkpoint_cli.run(successor_command)
            with patch.object(
                checkpoint_cli.subprocess, "run", side_effect=live_fixed_envelope,
            ):
                replay = checkpoint_cli.run([
                    *successor_command, "--apply",
                    "--expected-material-revision", "20",
                    "--expected-preview-sha256", replay_preview["preview_sha256"],
                ])
            self.assertEqual(replay["resume_authority"], "full")
            self.assertEqual(replay["writes_performed"], 0)
            self.assertEqual(len(client.mutations), writes_before_replay)

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

    def test_ordinary_resume_accepts_bound_fixed_frontier_checkpoint(self):
        route = fixture.AUTHORITY
        source = {"identity": "/tmp/plan.md", "sha256": "b" * 64}
        args = checkpoint_cli.parser().parse_args([
            "GEN-37", "--created-at", "2026-09-01T06:30:00Z", "--agent", "codex",
            "--provider", "openai", "--session-id", "s", "--machine", "M5",
            "--worktree-state", "safe", "--before-status", "In Progress",
            "--after-status", "In Progress", "--next-action", "Continue",
        ])
        expected = {"checkpoint_event_id": "cp-1", "root_revision": 58,
                    "workstream_id": "GEN-37", "plan_revision": source["sha256"],
                    "status": {"before": "In Progress", "after": "In Progress"},
                    "exact_head": None, "evidence": [], "blocker": None,
                    "next_action": "Continue", "worktree": {"state": "safe"},
                    "acknowledgement": {"state": "remote_acknowledged",
                        "remote_id": "remote-cp-1", "applied_revision": 58},
                    "provenance_chain": []}
        payload = {
            "context_schema": {"name": "agent-workstream.resume-context", "version": 2,
                "representation": "compact_validated", "envelope": "fixed_frontier_authority_v1"},
            "resume_authority": "full", "workstream_id": "GEN-37",
            "plan_revision": source["sha256"], "authenticated_source": source,
            "authenticated_route": route,
            "authority_binding": {
                "route_sha256": checkpoint_cli._digest(route),
                "source_sha256": checkpoint_cli._digest(source),
                "checkpoint_sha256": checkpoint_cli._digest({
                    "workstream_id": "GEN-37", "checkpoint_event_id": "cp-1",
                    "root_revision": 58, "plan_revision": source["sha256"],
                    "status": expected["status"], "exact_head": None,
                    "evidence": [], "blocker": None, "next_action": "Continue",
                    "worktree": {"state": "safe"}, "acknowledgement": {
                        "state": "remote_acknowledged",
                        "remote_id": "remote-cp-1", "applied_revision": 58,
                    }, "provenance_chain": [],
                }),
            },
            "authority_scope": {
                "history_validation": "complete_authenticated",
                "execution_frontier": "complete_digest_bound_excerpts",
                "item_count": 1, "omitted_items_claimed_executable": False,
                "truncated_cell_count": 1,
                "truncated_cell_marker": "~#<sha256-prefix>",
                "truncated_cell_rule": "hydrate selected source row before action",
            },
            "execution_frontier": {
                "root": {"status": None, "next": None, "blocker": None},
                "children": [], "obligations": [],
                "decisions": [], "choices": [], "dependencies": [],
                "child_dependency_graph": {"authority": {}, "relations": []},
                "columns": {
                    "children": [], "obligations": [], "decisions": [],
                    "choices": [], "dependencies": [],
                    "child_dependency_graph.relations": [],
                },
                "disposition": None, "checkpoint": "~#deadbeef",
            },
            "deferred_audit_detail": {
                "state": "fixed_frontier_authority_envelope",
                "hydration_required_before_action": True,
                "algorithm": "fixed-six-slot-frontier-v1", "fields": [],
                "fields_sha256": "0" * 64, "full_context_sha256": "1" * 64,
                "hydration_selectors": {}, "obligation_selector_rules": {},
                "hydration_recipe": "hydrate", "original_context_bytes": 30_000,
                "audit_route": {}, "full_history_route": {},
            },
        }
        completed = subprocess.CompletedProcess([], 0, stdout=json.dumps(payload), stderr="")
        with patch.object(checkpoint_cli.subprocess, "run", return_value=completed):
            self.assertEqual(
                checkpoint_cli._ordinary_resume(
                    "GEN-37", args=args, route=route, source=source,
                    expected_checkpoint=expected, expected_remote_id="remote-cp-1",
                ), payload,
            )
        payload["authority_binding"]["checkpoint_sha256"] = "0" * 64
        completed.stdout = json.dumps(payload)
        with patch.object(checkpoint_cli.subprocess, "run", return_value=completed), self.assertRaisesRegex(
            Exception, "checkpoint_mismatch",
        ):
            checkpoint_cli._ordinary_resume(
                "GEN-37", args=args, route=route, source=source,
                expected_checkpoint=expected, expected_remote_id="remote-cp-1",
            )

    def test_ordinary_resume_rejects_malformed_fixed_frontier_shapes(self):
        route = fixture.AUTHORITY
        source = {"identity": "/tmp/plan.md", "sha256": "b" * 64}
        args = checkpoint_cli.parser().parse_args([
            "GEN-37", "--created-at", "2026-09-01T06:30:00Z", "--agent", "codex",
            "--provider", "openai", "--session-id", "s", "--machine", "M5",
            "--worktree-state", "safe", "--before-status", "In Progress",
            "--after-status", "In Progress", "--next-action", "Continue",
        ])
        base = {
            "context_schema": {"name": "agent-workstream.resume-context", "version": 2,
                "representation": "compact_validated", "envelope": "fixed_frontier_authority_v1"},
            "resume_authority": "full", "workstream_id": "GEN-37",
            "plan_revision": source["sha256"], "authenticated_source": source,
            "authenticated_route": route,
            "authority_binding": {
                "route_sha256": checkpoint_cli._digest(route),
                "source_sha256": checkpoint_cli._digest(source),
                "checkpoint_sha256": checkpoint_cli._digest(None),
            },
            "authority_scope": {
                "history_validation": "complete_authenticated",
                "execution_frontier": "complete_digest_bound_excerpts",
                "item_count": 1, "omitted_items_claimed_executable": False,
                "truncated_cell_count": 1,
                "truncated_cell_marker": "~#<sha256-prefix>",
                "truncated_cell_rule": "hydrate selected source row before action",
            },
            "execution_frontier": {
                "root": {"status": None, "next": None, "blocker": None},
                "children": [], "obligations": [],
                "decisions": [], "choices": [], "dependencies": [],
                "child_dependency_graph": {"authority": {}, "relations": []},
                "columns": {
                    "children": [], "obligations": [], "decisions": [],
                    "choices": [], "dependencies": [],
                    "child_dependency_graph.relations": [],
                },
                "checkpoint": "~#deadbeef", "disposition": None,
            },
            "deferred_audit_detail": {
                "state": "fixed_frontier_authority_envelope",
                "hydration_required_before_action": True,
                "algorithm": "fixed-six-slot-frontier-v1", "fields": [],
                "fields_sha256": "0" * 64, "full_context_sha256": "1" * 64,
                "hydration_selectors": {}, "obligation_selector_rules": {},
                "hydration_recipe": "hydrate", "original_context_bytes": 30_000,
                "audit_route": {}, "full_history_route": {},
            },
        }
        cases = []
        malformed = deepcopy(base); malformed["context_schema"] = "bad"; cases.append(malformed)
        malformed = deepcopy(base); malformed["authority_binding"]["extra"] = 1; cases.append(malformed)
        malformed = deepcopy(base); malformed["authority_binding"]["route_sha256"] = "0" * 64; cases.append(malformed)
        malformed = deepcopy(base); malformed["authority_binding"]["source_sha256"] = "0" * 64; cases.append(malformed)
        malformed = deepcopy(base); malformed["execution_frontier"]["checkpoint"] = "garbage"; cases.append(malformed)
        malformed = deepcopy(base); malformed.pop("deferred_audit_detail"); cases.append(malformed)
        malformed = deepcopy(base); malformed["execution_frontier"].pop("obligations"); cases.append(malformed)
        malformed = deepcopy(base); malformed["authority_scope"]["item_count"] = True; cases.append(malformed)
        malformed = deepcopy(base); malformed["authority_scope"]["item_count"] = 0; cases.append(malformed)
        malformed = deepcopy(base); malformed["execution_frontier"]["extra"] = 1; cases.append(malformed)
        for payload in cases:
            completed = subprocess.CompletedProcess([], 0, stdout=json.dumps(payload), stderr="")
            with patch.object(checkpoint_cli.subprocess, "run", return_value=completed), self.assertRaisesRegex(
                Exception, "ordinary_resume_not_bounded_full",
            ):
                checkpoint_cli._ordinary_resume(
                    "GEN-37", args=args, route=route, source=source,
                )
        for payload in (None, []):
            completed = subprocess.CompletedProcess(
                [], 0, stdout=json.dumps(payload), stderr="",
            )
            with patch.object(
                checkpoint_cli.subprocess, "run", return_value=completed,
            ), self.assertRaisesRegex(
                checkpoint_cli.LinearTransportError,
                "checkpoint_ordinary_resume_invalid_json",
            ):
                checkpoint_cli._ordinary_resume(
                    "GEN-37", args=args, route=route, source=source,
                )


if __name__ == "__main__":
    unittest.main()
