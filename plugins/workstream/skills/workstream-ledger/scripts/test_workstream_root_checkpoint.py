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
from workstream_child_dependencies import ChildDependencyError
import workstream_resume as resume_cli
import workstream_root_checkpoint as checkpoint_cli
import workstream_linear_projection as projection_module
import test_workstream_generation_transition as fixture
import test_workstream_linear_projection as projection_fixture


class RootCheckpointTests(unittest.TestCase):
    def _minimal_checkpoint_fixture(self):
        token = "GEN-37"
        route = fixture.AUTHORITY
        client = fixture.FakeClient()
        plan = tempfile.NamedTemporaryFile("w+", suffix=".md")
        self.addCleanup(plan.close)
        text = "# checkpoint dependency oracle\n"
        plan.write(text); plan.flush()
        digest = hashlib.sha256(text.encode()).hexdigest()
        client.description = f"Plan revision: {digest}\nNext action: Continue"
        fixture.project_full(client, digest, identity=plan.name)
        client.mutations.clear()
        command = [
            token, "--boundary-id", "material-0",
            "--created-at", "2026-09-01T06:30:00Z", "--agent", "codex",
            "--provider", "openai", "--session-id", "oracle",
            "--machine", "M5", "--worktree-state", "safe",
            "--worktree-path", "/tmp/gen37", "--worktree-branch", "gen37",
            "--worktree-head", "e" * 40, "--exact-head", "e" * 40,
            "--before-status", "In Progress", "--after-status", "In Progress",
            "--next-action", "Continue",
        ]
        return client, route, command

    def _checkpoint_only_state(
        self, *, plan_text="# compensation fixture\n",
        client_factory=fixture.FakeClient, persist=True,
        resolved_quarantine=False,
    ):
        client, route, command = self._minimal_checkpoint_fixture()
        plan_identity = next(
            event["value"]["identity"]
            for event in checkpoint_cli.LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=client.description.split(":", 1)[1].split()[0],
                **route,
            ).state().events
            if event["kind"] == "source"
        )
        # _minimal_checkpoint_fixture owns this temporary file.  Rebuild the
        # client around caller-selected canonical bytes before preview.
        with open(plan_identity, "w", encoding="utf-8") as handle:
            handle.write(plan_text)
        digest = hashlib.sha256(plan_text.encode()).hexdigest()
        client = client_factory()
        client.description = f"Plan revision: {digest}\nNext action: Continue"
        fixture.project_full(client, digest, identity=plan_identity)
        if resolved_quarantine:
            legacy = projection_fixture.legacy_event(
                "provenance", "late-v1", {
                    "agent": "legacy", "machine": "M3",
                    "session_id": "resolved-late-v1",
                }, 4, "resolved-late-v1",
            )
            legacy["plan_revision"] = digest
            legacy["event_id"] = projection_module._event_id(legacy)
            client.comments.append(projection_fixture.legacy_comment(
                legacy, "resolved-late-v1",
            ))
            projection = checkpoint_cli.LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=digest, **route,
            )
            projection.append(checkpoint_cli.build_projection_event(
                workstream_id="GEN-37", kind="quarantine_disposition",
                key="root", value={
                    "event_ids": [legacy["event_id"]],
                    "events_sha256": checkpoint_cli._digest([legacy]),
                    "review_artifact_identity": (
                        "https://example.test/reviews/resolved-v1.md"
                    ),
                    "review_artifact_sha256": "d" * 64,
                    "reviewed_at": "2026-09-01T06:29:00Z",
                }, plan_revision=digest, expected_revision=4,
                created_at="resolved-quarantine", authority=route,
            ))
            state = projection.state()
            self.assertEqual(
                state.snapshot["projection_quarantined"], [legacy],
            )
            self.assertEqual(
                state.snapshot["projection_unresolved_quarantine"], [],
            )
        client.mutations.clear()
        with patch.object(
            checkpoint_cli, "_client_and_route", return_value=(client, route),
        ):
            preview = checkpoint_cli.run(command)
        native_before = checkpoint_cli._root_surface(client, route, "GEN-37")
        checkpoint_adapter = LinearCheckpointAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            issue_uuid=route["root_issue_id"], workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
        )
        receipt = (
            checkpoint_adapter.persist(preview["checkpoint"])
            if persist else None
        )
        loader = strict_candidate_loader(
            client, token="GEN-37", authority=route,
            plan_source=plan_identity, plan_identity=plan_identity,
            max_bytes=24 * 1024, max_items=100,
        )
        return (
            client, route, command, preview, native_before, receipt, loader,
            digest, plan_identity,
        )

    def _predecessor_pointer_state(self, *, wrong_predecessor=False):
        (
            client, route, command, first_preview, _native, _receipt, _loader,
            digest, plan_identity,
        ) = self._checkpoint_only_state(persist=False)
        checkpoint_adapter = LinearCheckpointAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            issue_uuid=route["root_issue_id"], workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
        )
        first_receipt = checkpoint_adapter.persist(first_preview["checkpoint"])
        projection = checkpoint_cli.LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=digest, **route,
        )
        projection.append(
            first_preview["projection_candidate"], expected_material_revision=0,
            expected_quarantine_count=0,
            expected_quarantine_sha256=checkpoint_cli._digest([]),
        )
        if wrong_predecessor:
            state = projection.state()
            current = next(
                event for event in reversed(state.events)
                if event["kind"] == "disposition" and event["key"] == "root"
            )
            projection.append(checkpoint_cli.build_projection_event(
                workstream_id="GEN-37", kind="disposition", key="root",
                value={
                    **current["value"],
                    "recovered_from_checkpoint": "wsc_" + "f" * 32,
                }, plan_revision=digest, expected_revision=state.revision,
                created_at="wrong-predecessor",
                supersedes_event_id=current["event_id"], authority=route,
            ))
        LinearCommentEventAdapter(
            client, issue_id="GEN-37", plan_revision=digest, **route,
        ).apply(Delta(
            "material-1", "GEN-37", "requirement", "second-boundary",
            {"requirement": "continue"}, 0, "material-1",
        ))
        second_command = list(command)
        for flag, value in (
            ("--boundary-id", "material-1"),
            ("--created-at", "2026-09-01T06:31:00Z"),
            ("--session-id", "successor"),
        ):
            second_command[second_command.index(flag) + 1] = value
        with patch.object(
            checkpoint_cli, "_client_and_route", return_value=(client, route),
        ):
            preview = checkpoint_cli.run(second_command)
        self.assertEqual(
            preview["checkpoint"]["predecessor_event_id"],
            first_receipt["event_id"],
        )
        native_before = checkpoint_cli._root_surface(client, route, "GEN-37")
        receipt = checkpoint_adapter.persist(preview["checkpoint"])
        loader = strict_candidate_loader(
            client, token="GEN-37", authority=route,
            plan_source=plan_identity, plan_identity=plan_identity,
            max_bytes=24 * 1024, max_items=100,
        )
        return (
            client, route, second_command, preview, native_before, receipt,
            loader, digest, first_receipt,
        )

    def _assert_projection_loss_compensates(self, *, resolved_quarantine):
        (
            client, route, command, _manual_preview, _native, _receipt, loader,
            digest, _plan_identity,
        ) = self._checkpoint_only_state(
            persist=False, resolved_quarantine=resolved_quarantine,
        )
        # Reconstruct the production boundary through one ordinary apply: the
        # checkpoint becomes acknowledged, its disposition still names the
        # predecessor because the projection request is not accepted, and the
        # first strict resume observes that exact durable state.
        original_append = checkpoint_cli.LinearProjectionAdapter.append
        append_attempts = 0

        def lose_first_projection(adapter, event, **kwargs):
            nonlocal append_attempts
            append_attempts += 1
            if append_attempts == 1:
                raise checkpoint_cli.LinearTransportError(
                    "projection request refused before commit"
                )
            return original_append(adapter, event, **kwargs)

        resume_calls = 0

        def exact_resume_state(*_args, **_kwargs):
            nonlocal resume_calls
            resume_calls += 1
            if resume_calls == 1:
                with self.assertRaisesRegex(
                    ResumeError,
                    "disposition_checkpoint_stale_reconcile_required",
                ):
                    loader(digest)
                raise checkpoint_cli.LinearTransportError(
                    "checkpoint_ordinary_resume_refused:"
                    + (
                        "workstream resume refused: "
                        if resolved_quarantine else ""
                    )
                    + "disposition_checkpoint_stale_reconcile_required"
                )
            self.assertEqual(loader(digest)["resume_authority"], "full")
            checkpoint_adapter = LinearCheckpointAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                issue_uuid=route["root_issue_id"],
                workspace_id=route["workspace_id"], team_id=route["team_id"],
                project_id=route["project_id"],
            )
            tip = checkpoint_adapter._recover_checkpoint_generations(
                checkpoint_adapter._state(),
            )[digest]
            return {
                "resume_authority": "full", "plan_revision": digest,
                "latest_checkpoint": {
                    "checkpoint_event_id": tip["checkpoint_event_id"],
                    "root_revision": tip["root_revision"],
                    "acknowledgement": tip["acknowledgement"],
                },
            }

        writes_before = len(client.mutations)
        with patch.object(
            checkpoint_cli, "_client_and_route", return_value=(client, route),
        ), patch.object(
            checkpoint_cli.LinearProjectionAdapter, "append",
            new=lose_first_projection,
        ), patch.object(
            checkpoint_cli, "_ordinary_resume", side_effect=exact_resume_state,
        ), patch.object(
            checkpoint_cli, "_compensate_checkpoint_projection",
            wraps=checkpoint_cli._compensate_checkpoint_projection,
        ) as compensate_spy:
            replay_preview = checkpoint_cli.run(command)
            applied = checkpoint_cli.run([
                *command, "--apply", "--expected-material-revision", "0",
                "--expected-preview-sha256", replay_preview["preview_sha256"],
            ])
        self.assertEqual(compensate_spy.call_count, 1)
        self.assertEqual(resume_calls, 2)
        self.assertEqual(append_attempts, 2)
        self.assertEqual(applied["resume_authority"], "full")
        self.assertEqual(applied["writes_performed"], 2)
        self.assertEqual(len(client.mutations), writes_before + 2)
        final_projection = checkpoint_cli.LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=digest, **route,
        ).state()
        disposition = final_projection.snapshot["disposition"]
        self.assertEqual(
            disposition["recovered_from_checkpoint"],
            replay_preview["checkpoint"]["event_id"],
        )
        if resolved_quarantine:
            self.assertEqual(
                len(final_projection.snapshot["projection_quarantined"]), 1,
            )
            self.assertEqual(
                final_projection.snapshot["projection_unresolved_quarantine"],
                [],
            )
        self.assertEqual(loader(digest)["resume_authority"], "full")

        # The exact retry is a genuine zero-write replay and never invokes the
        # compensation path again.
        writes_before_replay = len(client.mutations)
        resume_calls = 1
        with patch.object(
            checkpoint_cli, "_client_and_route", return_value=(client, route),
        ), patch.object(
            checkpoint_cli, "_ordinary_resume", side_effect=exact_resume_state,
        ), patch.object(
            checkpoint_cli, "_compensate_checkpoint_projection",
            wraps=checkpoint_cli._compensate_checkpoint_projection,
        ) as replay_spy:
            final_preview = checkpoint_cli.run(command)
            final = checkpoint_cli.run([
                *command, "--apply", "--expected-material-revision", "0",
                "--expected-preview-sha256", final_preview["preview_sha256"],
            ])
        self.assertEqual(final["writes_performed"], 0)
        self.assertEqual(replay_spy.call_count, 0)
        self.assertEqual(len(client.mutations), writes_before_replay)

    def test_projection_loss_stale_resume_compensates_once_and_replays(self):
        self._assert_projection_loss_compensates(resolved_quarantine=False)

    def test_resolved_quarantine_stale_resume_compensates_and_replays(self):
        self._assert_projection_loss_compensates(resolved_quarantine=True)

    def test_exact_predecessor_pointer_compensates_to_successor_and_replays(self):
        (
            client, route, command, preview, native_before, receipt, loader,
            digest, predecessor_receipt,
        ) = self._predecessor_pointer_state()
        disposition_before = checkpoint_cli.LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=digest, **route,
        ).state().snapshot["disposition"]
        self.assertEqual(
            disposition_before["recovered_from_checkpoint"],
            predecessor_receipt["event_id"],
        )
        with self.assertRaisesRegex(
            ResumeError, "disposition_checkpoint_stale_reconcile_required",
        ):
            loader(digest)
        writes = len(client.mutations)
        with patch.object(
            checkpoint_cli, "_compensate_checkpoint_projection",
            wraps=checkpoint_cli._compensate_checkpoint_projection,
        ) as spy:
            correction = checkpoint_cli._compensate_checkpoint_projection(
                client=client, preview=preview, route=route,
                native_before=native_before, checkpoint_receipt=receipt,
                args=checkpoint_cli.parser().parse_args(command),
            )
        self.assertEqual(spy.call_count, 1)
        self.assertIsNotNone(correction)
        self.assertEqual(len(client.mutations), writes + 1)
        self.assertEqual(loader(digest)["resume_authority"], "full")
        self.assertEqual(
            checkpoint_cli.LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=digest, **route,
            ).state().snapshot["disposition"]["recovered_from_checkpoint"],
            receipt["event_id"],
        )

        # The public command sees both records as exact replays, performs no
        # compensation, and writes nothing.
        checkpoint_state = LinearCheckpointAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            issue_uuid=route["root_issue_id"], workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
        )
        tip = checkpoint_state._recover_checkpoint_generations(
            checkpoint_state._state(),
        )[digest]
        full = {
            "resume_authority": "full", "plan_revision": digest,
            "latest_checkpoint": {
                "checkpoint_event_id": tip["checkpoint_event_id"],
                "root_revision": tip["root_revision"],
                "acknowledgement": tip["acknowledgement"],
            },
        }
        writes = len(client.mutations)
        with patch.object(
            checkpoint_cli, "_client_and_route", return_value=(client, route),
        ), patch.object(
            checkpoint_cli, "_ordinary_resume", return_value=full,
        ), patch.object(
            checkpoint_cli, "_compensate_checkpoint_projection",
            wraps=checkpoint_cli._compensate_checkpoint_projection,
        ) as replay_spy:
            replay_preview = checkpoint_cli.run(command)
            replay = checkpoint_cli.run([
                *command, "--apply", "--expected-material-revision", "1",
                "--expected-preview-sha256", replay_preview["preview_sha256"],
            ])
        self.assertEqual(replay["writes_performed"], 0)
        self.assertEqual(replay_spy.call_count, 0)
        self.assertEqual(len(client.mutations), writes)

    def test_wrong_reviewed_predecessor_pointer_refuses_zero_writes(self):
        (
            client, route, command, preview, native_before, receipt, _loader,
            digest, predecessor_receipt,
        ) = self._predecessor_pointer_state(wrong_predecessor=True)
        current_pointer = checkpoint_cli.LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=digest, **route,
        ).state().snapshot["disposition"]["recovered_from_checkpoint"]
        self.assertNotEqual(current_pointer, predecessor_receipt["event_id"])
        writes = len(client.mutations)
        with patch.object(
            checkpoint_cli, "_compensate_checkpoint_projection",
            wraps=checkpoint_cli._compensate_checkpoint_projection,
        ) as spy:
            correction = checkpoint_cli._compensate_checkpoint_projection(
                client=client, preview=preview, route=route,
                native_before=native_before, checkpoint_receipt=receipt,
                args=checkpoint_cli.parser().parse_args(command),
            )
        self.assertEqual(spy.call_count, 1)
        self.assertIsNone(correction)
        self.assertEqual(len(client.mutations), writes)
        self.assertEqual(
            checkpoint_cli.LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=digest, **route,
            ).state().snapshot["disposition"]["recovered_from_checkpoint"],
            current_pointer,
        )

    def test_compensation_refuses_every_changed_authority_surface_zero_writes(self):
        def invoke(state, *, dependency_error=None):
            (
                client, route, command, preview, native_before, receipt, _loader,
                _digest_value, _plan_identity,
            ) = state
            args = checkpoint_cli.parser().parse_args(command)
            writes = len(client.mutations)
            from contextlib import nullcontext
            context = (
                patch.object(
                    checkpoint_cli.LinearChildDependencyAdapter,
                    "read_authorized_graph_for_snapshot",
                    side_effect=dependency_error,
                )
                if dependency_error is not None else nullcontext()
            )
            with context, patch.object(
                checkpoint_cli, "_compensate_checkpoint_projection",
                wraps=checkpoint_cli._compensate_checkpoint_projection,
            ) as spy:
                result = checkpoint_cli._compensate_checkpoint_projection(
                    client=client, preview=preview, route=route,
                    native_before=native_before, checkpoint_receipt=receipt,
                    args=args,
                )
            self.assertEqual(spy.call_count, 1)
            self.assertIsNone(result)
            self.assertEqual(len(client.mutations), writes)

        # Source bytes drift under the same identity.
        state = self._checkpoint_only_state(plan_text="# source before\n")
        with open(state[-1], "w", encoding="utf-8") as handle:
            handle.write("# source after\n")
        invoke(state)

        # Source identity drift with identical bytes is independently fenced.
        state = self._checkpoint_only_state(plan_text="# identity bytes\n")
        client, route, _, _, _, _, _, digest, source_identity = state
        replacement = tempfile.NamedTemporaryFile("w+", suffix=".md")
        self.addCleanup(replacement.close)
        replacement.write("# identity bytes\n")
        replacement.flush()
        projection = checkpoint_cli.LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=digest, **route,
        )
        projection_state = projection.state()
        current_source = next(
            event for event in reversed(projection_state.events)
            if event["kind"] == "source" and event["key"] == "root"
        )
        projection.append(checkpoint_cli.build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"identity": replacement.name, "sha256": digest},
            plan_revision=digest, expected_revision=projection_state.revision,
            created_at="source-identity-drift",
            supersedes_event_id=current_source["event_id"], authority=route,
        ))
        self.assertNotEqual(source_identity, replacement.name)
        invoke(state)

        # Human-visible lifecycle/native state transition.
        class ChildAwareClient(fixture.FakeClient):
            def execute(self, query, variables):
                result = super().execute(query, variables)
                if "query WorkstreamResumeRoot" in query:
                    result["issue"]["children"]["nodes"] = deepcopy(self.children)
                return result

        state = self._checkpoint_only_state()
        state[0].graph_status = "Done"
        state[0].graph_status_type = "completed"
        state[0].graph_state_id = "done-state"
        invoke(state)

        # Native child graph drift.
        state = self._checkpoint_only_state(client_factory=ChildAwareClient)
        state[0].children.append({
            "id": "child-id", "identifier": "GEN-72", "title": "Child",
            "description": "Next action: Continue.",
            "url": "https://linear.test/GEN-72", "updatedAt": "child-time",
            "archivedAt": None,
            "parent": {"id": state[1]["root_issue_id"], "identifier": "GEN-37"},
            "project": {"id": state[1]["project_id"]},
            "team": {"id": state[1]["team_id"],
                     "organization": {"id": state[1]["workspace_id"]}},
            "assignee": None,
            "state": {"id": "child-state", "name": "In Progress",
                      "type": "started"},
            "comments": {"nodes": [], "pageInfo": {
                "hasNextPage": False, "endCursor": None,
            }},
        })
        invoke(state)

        # The full fresh producer oracle, not a projection-only shortcut,
        # refuses relation/dependency readback drift.
        for reason in (
            "dependency_relation_frontier_changed",
            "authenticated_dependency_graph_missing",
        ):
            with self.subTest(reason=reason):
                invoke(
                    self._checkpoint_only_state(),
                    dependency_error=ChildDependencyError(reason),
                )

    def test_compensation_refuses_quarantine_newer_checkpoint_and_projection_race(self):
        # A late schema-v1 event is unresolved quarantine, never repair input.
        state = self._checkpoint_only_state()
        client, route, command, preview, native_before, receipt, _, digest, _ = state
        legacy = projection_fixture.legacy_event(
            "provenance", "late-v1",
            {"agent": "legacy", "machine": "M3", "session_id": "late-v1"},
            4, "late-v1",
        )
        legacy["plan_revision"] = digest
        legacy["event_id"] = projection_module._event_id(legacy)
        client.comments.append(projection_fixture.legacy_comment(legacy, "late-v1"))
        writes = len(client.mutations)
        with patch.object(
            checkpoint_cli, "_compensate_checkpoint_projection",
            wraps=checkpoint_cli._compensate_checkpoint_projection,
        ) as spy:
            result = checkpoint_cli._compensate_checkpoint_projection(
                client=client, preview=preview, route=route,
                native_before=native_before, checkpoint_receipt=receipt,
                args=checkpoint_cli.parser().parse_args(command),
            )
        self.assertEqual(spy.call_count, 1)
        self.assertIsNone(result)
        self.assertEqual(len(client.mutations), writes)

        # A newer canonical material/checkpoint frontier makes the old pointer
        # ambiguous and cannot be rolled back by compensation.
        state = self._checkpoint_only_state()
        client, route, command, preview, native_before, receipt, _, digest, _ = state
        LinearCommentEventAdapter(
            client, issue_id="GEN-37", plan_revision=digest, **route,
        ).apply(Delta(
            "material-1", "GEN-37", "requirement", "new-frontier",
            {"requirement": "newer"}, 0, "later-material",
        ))
        newer = checkpoint_cli.build_checkpoint(
            workstream_id="GEN-37", boundary_id="material-1", root_revision=1,
            plan_revision=digest, before_status="In Progress",
            after_status="In Progress", execution={
                "agent": "codex", "provider": "openai", "session_id": "newer",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/tmp/newer", "branch": "newer",
                    "head": "e" * 40,
                },
            }, exact_head="e" * 40, evidence=[], blocker=None,
            next_action="Continue", predecessor_event_id=receipt["event_id"],
        )
        LinearCheckpointAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            issue_uuid=route["root_issue_id"], workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
        ).persist(newer)
        writes = len(client.mutations)
        result = checkpoint_cli._compensate_checkpoint_projection(
            client=client, preview=preview, route=route,
            native_before=native_before, checkpoint_receipt=receipt,
            args=checkpoint_cli.parser().parse_args(command),
        )
        self.assertIsNone(result)
        self.assertEqual(len(client.mutations), writes)

        # A competing projection wins after the fresh oracle but before our
        # CAS append.  The helper emits no correction and never rebases.
        state = self._checkpoint_only_state()
        client, route, command, preview, native_before, receipt, _, digest, _ = state
        original_append = checkpoint_cli.LinearProjectionAdapter.append
        raced = False

        def inject_competitor(adapter, candidate, **kwargs):
            nonlocal raced
            if not raced:
                raced = True
                current = adapter.state()
                competitor = checkpoint_cli.build_projection_event(
                    workstream_id="GEN-37", kind="provenance", key="race",
                    value={"agent": "codex", "machine": "M3",
                           "session_id": "competitor"},
                    plan_revision=digest, expected_revision=current.revision,
                    created_at="race", authority=route,
                )
                original_append(
                    adapter, competitor, expected_material_revision=0,
                    expected_quarantine_count=0,
                    expected_quarantine_sha256=checkpoint_cli._digest([]),
                )
            return original_append(adapter, candidate, **kwargs)

        writes = len(client.mutations)
        with patch.object(
            checkpoint_cli.LinearProjectionAdapter, "append",
            new=inject_competitor,
        ):
            result = checkpoint_cli._compensate_checkpoint_projection(
                client=client, preview=preview, route=route,
                native_before=native_before, checkpoint_receipt=receipt,
                args=checkpoint_cli.parser().parse_args(command),
            )
        self.assertIsNone(result)
        self.assertEqual(len(client.mutations), writes + 1)
        self.assertFalse(any(
            event["kind"] == "disposition"
            and event["value"].get("recovered_from_checkpoint") == receipt["event_id"]
            for event in checkpoint_cli.LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=digest, **route,
            ).state().events
        ))

    def test_non_stale_resume_refusal_never_invokes_compensation(self):
        state = self._checkpoint_only_state()
        client, route, command = state[:3]
        original_append = checkpoint_cli.LinearProjectionAdapter.append
        attempts = 0

        def refuse_first(adapter, event, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise checkpoint_cli.LinearTransportError("projection unavailable")
            return original_append(adapter, event, **kwargs)

        with patch.object(
            checkpoint_cli, "_client_and_route", return_value=(client, route),
        ), patch.object(
            checkpoint_cli.LinearProjectionAdapter, "append", new=refuse_first,
        ), patch.object(
            checkpoint_cli, "_ordinary_resume",
            side_effect=checkpoint_cli.LinearTransportError(
                "outer:checkpoint_ordinary_resume_refused:workstream resume "
                "refused: disposition_checkpoint_stale_reconcile_required"
            ),
        ), patch.object(
            checkpoint_cli, "_compensate_checkpoint_projection",
            wraps=checkpoint_cli._compensate_checkpoint_projection,
        ) as spy:
            preview = checkpoint_cli.run(command)
            with self.assertRaises(
                checkpoint_cli.CheckpointPartialApplyError,
            ) as raised:
                checkpoint_cli.run([
                    *command, "--apply", "--expected-material-revision", "0",
                    "--expected-preview-sha256", preview["preview_sha256"],
                ])
        self.assertEqual(spy.call_count, 0)
        self.assertEqual(
            raised.exception.payload["reason"],
            "checkpoint_projection_apply_unknown_replay_required",
        )

    def test_stale_resume_refusal_parser_accepts_only_canonical_envelopes(self):
        accepted = (
            "checkpoint_ordinary_resume_refused:"
            "disposition_checkpoint_stale_reconcile_required",
            "checkpoint_ordinary_resume_refused:workstream resume refused: "
            "disposition_checkpoint_stale_reconcile_required",
        )
        for value in accepted:
            self.assertTrue(
                checkpoint_cli._is_exact_stale_checkpoint_resume_refusal(
                    checkpoint_cli.LinearTransportError(value)
                )
            )
        for value in (
            "outer:" + accepted[1],
            accepted[1] + ":extra",
            "checkpoint_ordinary_resume_refused:transport failed; "
            "disposition_checkpoint_stale_reconcile_required",
            "checkpoint_ordinary_resume_refused:workstream resume refused: "
            "prefix disposition_checkpoint_stale_reconcile_required",
        ):
            self.assertFalse(
                checkpoint_cli._is_exact_stale_checkpoint_resume_refusal(
                    checkpoint_cli.LinearTransportError(value)
                )
            )

    def test_pre_oracle_projection_advance_refuses_compensation_zero_writes(self):
        state = self._checkpoint_only_state()
        client, route, command, preview, native_before, receipt, _, digest, _ = state
        projection = checkpoint_cli.LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=digest, **route,
        )
        current = projection.state()
        projection.append(checkpoint_cli.build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="intervening",
            value={"agent": "codex", "machine": "M1",
                   "session_id": "intervening"},
            plan_revision=digest, expected_revision=current.revision,
            created_at="intervening", authority=route,
        ))
        writes = len(client.mutations)
        with patch.object(
            checkpoint_cli, "_compensate_checkpoint_projection",
            wraps=checkpoint_cli._compensate_checkpoint_projection,
        ) as spy:
            result = checkpoint_cli._compensate_checkpoint_projection(
                client=client, preview=preview, route=route,
                native_before=native_before, checkpoint_receipt=receipt,
                args=checkpoint_cli.parser().parse_args(command),
            )
        self.assertEqual(spy.call_count, 1)
        self.assertIsNone(result)
        self.assertEqual(len(client.mutations), writes)
        self.assertIsNone(projection.state().snapshot["disposition"]
                          ["recovered_from_checkpoint"])

    def test_durable_compensation_receipt_survives_second_resume_refusal(self):
        (
            client, route, command, _preview, _native, _receipt, _loader,
            _digest_value, _plan_identity,
        ) = self._checkpoint_only_state(persist=False)
        original_append = checkpoint_cli.LinearProjectionAdapter.append
        append_attempts = 0

        def lose_initial_projection(adapter, event, **kwargs):
            nonlocal append_attempts
            append_attempts += 1
            if append_attempts == 1:
                raise checkpoint_cli.LinearTransportError(
                    "initial projection refused before commit"
                )
            return original_append(adapter, event, **kwargs)

        # Checkpoint is mutation one.  Compensation is mutation two; simulate
        # its response being lost after Linear durably commits it.
        client.commit_then_fail_at.add(2)
        resume_failures = iter((
            checkpoint_cli.LinearTransportError(
                "checkpoint_ordinary_resume_refused:workstream resume refused: "
                "disposition_checkpoint_stale_reconcile_required"
            ),
            checkpoint_cli.LinearTransportError(
                "checkpoint_ordinary_resume_refused:workstream resume refused: "
                "resume_context_over_budget"
            ),
        ))
        with patch.object(
            checkpoint_cli, "_client_and_route", return_value=(client, route),
        ), patch.object(
            checkpoint_cli.LinearProjectionAdapter, "append",
            new=lose_initial_projection,
        ), patch.object(
            checkpoint_cli, "_ordinary_resume", side_effect=resume_failures,
        ):
            preview = checkpoint_cli.run(command)
            with self.assertRaises(
                checkpoint_cli.CheckpointPartialApplyError,
            ) as raised:
                checkpoint_cli.run([
                    *command, "--apply", "--expected-material-revision", "0",
                    "--expected-preview-sha256", preview["preview_sha256"],
                ])
        payload = raised.exception.payload
        self.assertEqual(
            payload["reason"],
            "checkpoint_compensation_applied_but_resume_refused",
        )
        correction = payload["projection"]["receipt"]
        self.assertEqual(correction["event_id"], preview["projection_candidate"]
                         ["event_id"])
        self.assertIsInstance(correction["remote_id"], str)
        self.assertEqual(correction["revision"],
                         preview["projection_candidate"]["expected_revision"] + 1)
        self.assertEqual(correction["acknowledgement"], {
            "state": "remote_acknowledged",
            "remote_id": correction["remote_id"],
            "applied_revision": correction["revision"],
        })
        self.assertEqual(payload["failure"], {
            "stage": "post_compensation_ordinary_resume",
            "reason": (
                "checkpoint_ordinary_resume_refused:workstream resume refused: "
                "resume_context_over_budget"
            ),
        })
        self.assertEqual(len(client.mutations), 2)

    def test_native_dependency_graph_missing_or_invalid_refuses_zero_writes(self):
        for failure in (
            ChildDependencyError("authenticated_dependency_graph_missing"),
            ChildDependencyError("invalid_dependency_issue_graph"),
        ):
            with self.subTest(reason=str(failure)):
                client, route, command = self._minimal_checkpoint_fixture()
                with patch.object(
                    checkpoint_cli, "_client_and_route",
                    return_value=(client, route),
                ), patch.object(
                    checkpoint_cli.LinearChildDependencyAdapter,
                    "read_authorized_graph_for_snapshot",
                    side_effect=failure,
                ), self.assertRaisesRegex(
                    checkpoint_cli.LinearTransportError,
                    "checkpoint_proposed_resume_refused",
                ):
                    checkpoint_cli.run(command)
                self.assertEqual(client.mutations, [])

    def test_native_dependency_context_over_budget_refuses_zero_writes(self):
        client, route, command = self._minimal_checkpoint_fixture()
        with patch.object(
            checkpoint_cli, "_client_and_route", return_value=(client, route),
        ), patch.object(
            resume_cli, "compact_context",
            side_effect=ResumeError("resume_context_over_budget"),
        ), self.assertRaisesRegex(
            checkpoint_cli.LinearTransportError,
            "checkpoint_proposed_resume_refused:resume_context_over_budget",
        ):
            checkpoint_cli.run(command)
        self.assertEqual(client.mutations, [])

    def test_legacy_projection_quarantine_refuses_preview_and_append_fence(self):
        client, route, command = self._minimal_checkpoint_fixture()
        # Inject a genuine schema-v1 event after the modern projection.  This
        # is the same late-writer shape that production reduces into the
        # unresolved quarantine surface.
        digest = client.description.split(":", 1)[1].split()[0]
        legacy = projection_fixture.legacy_event(
        "provenance", "late-v1", {
            "agent": "legacy", "machine": "M3", "session_id": "late-v1",
        },
            4, "2026-09-01T06:31:00Z",
        )
        legacy["plan_revision"] = digest
        legacy["event_id"] = projection_module._event_id(legacy)
        client.comments.append(
            projection_fixture.legacy_comment(legacy, "legacy-quarantine")
        )
        client.mutations.clear()
        state = checkpoint_cli.LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=digest, **route,
        ).state()
        quarantine = state.snapshot["projection_unresolved_quarantine"]
        self.assertEqual(len(quarantine), 1)
        with patch.object(
            checkpoint_cli, "_client_and_route", return_value=(client, route),
        ), self.assertRaisesRegex(
            checkpoint_cli.LinearTransportError,
            "checkpoint_projection_unresolved_quarantine_refused",
        ):
            checkpoint_cli.run(command)
        self.assertEqual(client.mutations, [])

        # The native projection transport independently fences an append using
        # the exact quarantined event list and digest; a stale/wrong fence is
        # a zero-write refusal rather than an implicit quarantine repair.
        adapter = checkpoint_cli.LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=digest, **route,
        )
        current = adapter.state()
        candidate = checkpoint_cli.build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="fenced",
            value={"agent": "codex", "machine": "M5", "session_id": "fenced"},
            plan_revision=digest, expected_revision=current.revision,
            created_at="2026-09-01T06:32:00Z", authority=route,
        )
        with self.assertRaisesRegex(
            checkpoint_cli.LinearProjectionError,
            "projection_quarantine_changed_reload_required",
        ):
            adapter.append(
                candidate, expected_material_revision=0,
                expected_quarantine_count=1,
                expected_quarantine_sha256="0" * 64,
            )
        self.assertEqual(client.mutations, [])

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
