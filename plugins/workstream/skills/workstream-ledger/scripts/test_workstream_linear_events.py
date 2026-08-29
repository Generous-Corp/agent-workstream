#!/usr/bin/env python3
from __future__ import annotations

import threading
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

import workstream_linear_events as linear_events_module
from workstream_delta import Delta, DeltaJournal
from workstream_delta import RevisionConflict
from workstream_linear import LinearTransportError
from workstream_checkpoint import build_checkpoint
from workstream_linear_checkpoints import encode_checkpoint_comment
from workstream_linear_events import (
    LinearCommentEventAdapter,
    LinearEventError,
    encode_event_comment,
    ledger_boundary_slot_id,
    reduce_event_comments,
)


def delta(event_id: str, payload: dict, expected_revision: int = 0) -> Delta:
    return Delta(
        event_id, "GEN-37", "requirement", "agent_discovery", payload,
        expected_revision, f"2026-08-20T00:00:0{payload.get('order', 0)}Z",
    )


class FakeCommentClient:
    """Thread-safe Linear fake; an optional barrier aligns initial reads."""

    def __init__(self, initial_readers: int = 0):
        self.comments: list[dict] = []
        self.calls: list[tuple[str, dict]] = []
        self.lock = threading.Lock()
        self.initial_barrier = (
            threading.Barrier(initial_readers) if initial_readers else None
        )
        self.initial_reads = 0
        self.workspace_id = "workspace"
        self.team_id = "team"
        self.project_id = "project"
        self.root_issue_id = "issue-37"

    def execute(self, query, variables):
        with self.lock:
            self.calls.append((query, variables))
        if "query WorkstreamDeltaComments" in query:
            wait = False
            with self.lock:
                if self.initial_barrier and self.initial_reads < self.initial_barrier.parties:
                    self.initial_reads += 1
                    wait = True
            if wait:
                self.initial_barrier.wait(timeout=2)
            with self.lock:
                nodes = [dict(comment) for comment in self.comments]
            return {
                "issue": {
                    "id": self.root_issue_id, "identifier": "GEN-37",
                    "team": {"id": self.team_id,
                             "organization": {"id": self.workspace_id}},
                    "project": {"id": self.project_id},
                    "comments": {
                        "nodes": nodes,
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                }
            }
        if "WorkstreamEventCommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}, {"name": "body"}]}}
        if "commentCreate" in query:
            with self.lock:
                comment_id = variables["input"]["id"]
                if any(item["id"] == comment_id for item in self.comments):
                    raise LinearTransportError("duplicate comment id")
                comment = {
                    "id": comment_id,
                    "body": variables["input"]["body"],
                    "createdAt": "now", "updatedAt": "now",
                }
                self.comments.append(comment)
            return {"commentCreate": {"success": True, "comment": dict(comment)}}
        raise AssertionError("unexpected GraphQL operation")


class LinearCommentEventAdapterTests(unittest.TestCase):
    def test_concurrent_same_revision_has_one_durable_winner(self):
        client = FakeCommentClient(initial_readers=2)
        adapters = [
            LinearCommentEventAdapter(client, issue_id="GEN-37"),
            LinearCommentEventAdapter(client, issue_id="GEN-37"),
        ]
        deltas = [delta("event-a", {"order": 1}), delta("event-b", {"order": 2})]
        receipts = []
        failures = []

        def apply(index):
            try:
                receipts.append(adapters[index].apply(deltas[index]))
            except Exception as exc:  # captured so the main thread can assert it
                failures.append(exc)

        threads = [threading.Thread(target=apply, args=(index,)) for index in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=3)

        self.assertEqual(len(receipts), 1)
        self.assertEqual(len(failures), 1)
        self.assertTrue(
            isinstance(failures[0], RevisionConflict)
            or "expected revision 0, live revision 1" in str(failures[0])
            or "event_slot_lost_reload_required" in str(failures[0])
        )
        state = reduce_event_comments(client.comments, workstream_id="GEN-37")
        self.assertEqual(state.revision, 1)
        self.assertEqual({event.event_id for event in state.events}, {
            receipts[0].event_id
        })

    def test_stale_revision_refuses_before_a_second_comment(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        self.assertFalse(adapter.supports_atomic_cas)
        self.assertTrue(adapter.supports_append_only_events)
        first = DeltaJournal(":memory:")
        second = DeltaJournal(":memory:")
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        first.append("GEN-37", "requirement", {"text": "A"}, 0, event_id="event-a")
        second.append("GEN-37", "decision", {"text": "B"}, 0, event_id="event-b")

        first.apply(adapter)
        with self.assertRaisesRegex(RuntimeError, "expected revision 0, live revision 1"):
            second.apply(adapter)

        self.assertEqual(adapter.current_revision("GEN-37"), 1)
        self.assertEqual(first.pending(), [])
        self.assertEqual(len(second.pending()), 1)

    def test_crash_replay_returns_existing_event_without_second_comment(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        event = delta("event-a", {"order": 1})
        first = adapter.apply(event)
        replay = adapter.apply(event)
        self.assertEqual(first, replay)
        self.assertEqual(len(client.comments), 1)

    def test_replay_after_later_event_returns_own_stable_revision(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        first = delta("event-a", {"order": 1})
        adapter.apply(first)
        adapter.apply(delta("event-b", {"order": 2}, expected_revision=1))

        replay = adapter.apply(first)

        self.assertEqual(replay.revision, 1)
        self.assertEqual(replay.remote_id, client.comments[0]["id"])
        self.assertEqual(len(client.comments), 2)

    def test_crash_after_rebased_remote_accept_replays_from_fresh_journal(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        adapter.apply(delta("event-a", {"order": 1}))

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite3"
            journal = DeltaJournal(path)
            event_id = journal.append(
                "GEN-37", "requirement", {"order": 2}, 0,
                event_id="event-b", source="agent_discovery",
            )
            crashed = False

            def crash_after_remote_accept():
                nonlocal crashed
                if not crashed:
                    crashed = True
                    raise RuntimeError("crash after rebased remote accept")

            journal.commit_hook = crash_after_remote_accept
            with mock.patch.object(
                linear_events_module, "RevisionConflict", RevisionConflict
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "crash after rebased remote accept"
                ):
                    journal.apply_with_rebase(adapter)
            journal.commit_hook = None
            journal.close()

            remote = reduce_event_comments(
                client.comments, workstream_id="GEN-37"
            )
            rebased = next(
                event for event in remote.events if event.event_id == event_id
            )
            self.assertEqual(rebased.expected_revision, 1)

            fresh = DeltaJournal(path)
            self.addCleanup(fresh.close)
            self.assertEqual(fresh.pending()[0].expected_revision, 0)
            receipts = fresh.apply_with_rebase(adapter)

            self.assertEqual(receipts[0].event_id, event_id)
            self.assertEqual(receipts[0].revision, 2)
            self.assertEqual(fresh.pending(), [])
            self.assertEqual(len(client.comments), 2)

    def test_rebased_replay_rejects_reverse_revision_and_material_mismatch(self):
        client = FakeCommentClient()
        first = delta("event-a", {"order": 1})
        remote = delta("event-b", {"order": 2}, expected_revision=1)
        client.comments.extend([
            {"id": "legacy-1", "body": encode_event_comment(first)},
            {"id": "legacy-2", "body": encode_event_comment(remote)},
        ])
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        original = replace(remote, expected_revision=0)
        self.assertEqual(adapter.apply(original).revision, 2)

        mismatches = [
            replace(original, expected_revision=2),
            replace(original, payload={"order": 99}),
            replace(original, kind="decision"),
            replace(original, source="user_turn"),
            replace(original, created_at="2026-08-20T00:01:00Z"),
        ]
        for mismatch in mismatches:
            with self.subTest(mismatch=mismatch):
                with self.assertRaisesRegex(
                    LinearEventError, "conflicting_event_id:event-b"
                ):
                    adapter.apply(mismatch)
        with self.assertRaisesRegex(LinearEventError, "workstream_id_mismatch"):
            adapter.apply(replace(original, workstream_id="OPS-9"))

    def test_legacy_arbitrary_comment_id_history_remains_compatible(self):
        client = FakeCommentClient()
        first = delta("event-a", {"order": 1})
        client.comments.append({
            "id": "legacy-arbitrary-comment-id",
            "body": encode_event_comment(first),
            "createdAt": "then", "updatedAt": "then",
        })
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")

        replay = adapter.apply(first)
        second = adapter.apply(
            delta("event-b", {"order": 2}, expected_revision=1)
        )

        self.assertEqual(replay.remote_id, "legacy-arbitrary-comment-id")
        self.assertEqual(replay.revision, 1)
        self.assertEqual(second.revision, 2)
        self.assertEqual(len(client.comments), 2)

    def test_lost_create_response_reloads_exact_slot_as_replay(self):
        class LostResponseClient(FakeCommentClient):
            lost = False

            def execute(self, query, variables):
                if "commentCreate" in query and not self.lost:
                    self.lost = True
                    super().execute(query, variables)
                    raise LinearTransportError("response lost")
                return super().execute(query, variables)

        client = LostResponseClient()
        receipt = LinearCommentEventAdapter(client, issue_id="GEN-37").apply(
            delta("event-a", {"order": 1})
        )
        self.assertEqual(receipt.event_id, "event-a")
        self.assertEqual(receipt.remote_id, client.comments[0]["id"])
        self.assertEqual(len(client.comments), 1)

    def test_foreign_winner_at_same_revision_refuses(self):
        class ForeignWinnerClient(FakeCommentClient):
            injected = False

            def execute(self, query, variables):
                if "commentCreate" in query and not self.injected:
                    self.injected = True
                    self.comments.append({
                        "id": variables["input"]["id"],
                        "body": encode_event_comment(delta("foreign", {"order": 9})),
                        "createdAt": "now", "updatedAt": "now",
                    })
                    raise LinearTransportError("duplicate comment id")
                return super().execute(query, variables)

        with self.assertRaisesRegex(
            RuntimeError, "expected revision 0, live revision 1"
        ):
            LinearCommentEventAdapter(
                ForeignWinnerClient(), issue_id="GEN-37"
            ).apply(delta("event-a", {"order": 1}))

    def test_project_move_keeps_same_boundary_slot_and_collision(self):
        old_authority = {
            "workspace_id": "workspace", "team_id": "old-team",
            "project_id": "old-project", "root_issue_id": "issue-37",
        }
        new_authority = {
            "workspace_id": "workspace", "team_id": "new-team",
            "project_id": "new-project", "root_issue_id": "issue-37",
        }
        old_slot = ledger_boundary_slot_id("GEN-37", 0, [], old_authority)
        self.assertEqual(
            old_slot,
            ledger_boundary_slot_id("GEN-37", 0, [], new_authority),
        )
        self.assertEqual(
            old_slot,
            ledger_boundary_slot_id("OPS-9", 0, [], new_authority),
        )

        class MovedProjectClient(FakeCommentClient):
            injected = False

            def __init__(self):
                super().__init__()
                self.team_id = "new-team"
                self.project_id = "new-project"

            def execute(self, query, variables):
                if "commentCreate" in query and not self.injected:
                    self.injected = True
                    self.asserted_slot = variables["input"]["id"]
                    self.comments.append({
                        "id": old_slot,
                        "body": encode_event_comment(delta("old-winner", {"order": 9})),
                        "createdAt": "now", "updatedAt": "now",
                    })
                    raise LinearTransportError("duplicate comment id")
                return super().execute(query, variables)

        client = MovedProjectClient()
        with self.assertRaisesRegex(RuntimeError, "live revision 1"):
            LinearCommentEventAdapter(
                client, issue_id="GEN-37", workspace_id="workspace",
                team_id="new-team", project_id="new-project",
            ).apply(delta("new-writer", {"order": 1}))
        self.assertEqual(client.asserted_slot, old_slot)

    def test_checkpoint_winner_moves_event_to_new_shared_frontier(self):
        client = FakeCommentClient()
        client.comments.append({
            "id": "legacy-material-1",
            "body": encode_event_comment(delta("event-a", {"order": 1})),
            "createdAt": "then", "updatedAt": "then",
        })
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="checkpoint-wins",
            root_revision=1, plan_revision="plan-sha",
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "s1",
                "machine": "m1", "worktree": {
                    "state": "safe", "path": "/repo", "branch": "main",
                    "head": "head-1",
                },
            }, exact_head="head-1", evidence=[], blocker=None,
            next_action="continue", predecessor_event_id=None,
        )
        original_execute = client.execute
        injected = False

        def checkpoint_wins(query, variables):
            nonlocal injected
            if "commentCreate" in query and not injected:
                injected = True
                client.comments.append({
                    "id": variables["input"]["id"],
                    "body": encode_checkpoint_comment(checkpoint),
                    "createdAt": "now", "updatedAt": "now",
                })
                raise LinearTransportError("duplicate comment id")
            return original_execute(query, variables)

        client.execute = checkpoint_wins
        receipt = LinearCommentEventAdapter(client, issue_id="GEN-37").apply(
            delta("event-b", {"order": 2}, expected_revision=1)
        )

        self.assertEqual(receipt.revision, 2)
        self.assertEqual(len(client.comments), 3)
        self.assertEqual(len({comment["id"] for comment in client.comments}), 3)

    def test_concurrent_apply_with_rebase_serializes_stable_event_ids(self):
        client = FakeCommentClient(initial_readers=2)
        receipts = []
        failures = []

        def apply(event_id, order):
            journal = DeltaJournal(":memory:")
            try:
                journal.append(
                    "GEN-37", "requirement", {"order": order}, 0,
                    event_id=event_id, source="agent_discovery",
                )
                receipts.extend(journal.apply_with_rebase(
                    LinearCommentEventAdapter(client, issue_id="GEN-37")
                ))
            except Exception as exc:  # captured so the main thread can assert it
                failures.append(exc)
            finally:
                journal.close()

        threads = [
            threading.Thread(target=apply, args=("event-a", 1)),
            threading.Thread(target=apply, args=("event-b", 2)),
        ]
        # Some full-suite tests deliberately reload workstream_delta to verify
        # migrations. Keep the adapter's exception identity aligned with the
        # journal class under test while these worker threads execute.
        with mock.patch.object(
            linear_events_module, "RevisionConflict", RevisionConflict
        ):
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)

        self.assertFalse(failures)
        self.assertEqual({receipt.event_id for receipt in receipts}, {"event-a", "event-b"})
        state = reduce_event_comments(client.comments, workstream_id="GEN-37")
        self.assertEqual(state.revision, 2)
        self.assertEqual([event.expected_revision for event in state.events], [0, 1])
        self.assertEqual({event.event_id for event in state.events}, {"event-a", "event-b"})
        self.assertEqual(len(client.comments), 2)
        self.assertEqual(len({comment["id"] for comment in client.comments}), 2)

    def test_duplicate_and_conflicting_event_ids_fail_closed(self):
        original = delta("event-a", {"order": 1})
        conflicting = delta("event-a", {"order": 2})
        duplicate_comments = [
            {"id": "one", "body": encode_event_comment(original)},
            {"id": "two", "body": encode_event_comment(original)},
        ]
        with self.assertRaisesRegex(LinearEventError, "duplicate_event_id:event-a"):
            reduce_event_comments(duplicate_comments, workstream_id="GEN-37")
        conflicting_comments = [
            {"id": "one", "body": encode_event_comment(original)},
            {"id": "two", "body": encode_event_comment(conflicting)},
        ]
        with self.assertRaisesRegex(LinearEventError, "conflicting_event_id:event-a"):
            reduce_event_comments(conflicting_comments, workstream_id="GEN-37")

    def test_malformed_marker_and_revision_gap_fail_closed(self):
        with self.assertRaisesRegex(LinearEventError, "malformed_event_marker"):
            reduce_event_comments(
                [{"id": "bad", "body": "<!-- workstream-delta:v1:not-base64 -->"}],
                workstream_id="GEN-37",
            )
        with self.assertRaisesRegex(LinearEventError, "event_revision_gap"):
            reduce_event_comments(
                [{"id": "gap", "body": encode_event_comment(delta("event-gap", {}, 2))}],
                workstream_id="GEN-37",
            )

    def test_unavailable_auth_fails_before_client_or_network_exists(self):
        with self.assertRaisesRegex(LinearEventError, "linear_auth_unavailable"):
            LinearCommentEventAdapter.from_env(issue_id="GEN-37", env={})

    def test_configured_route_fences_comment_writes(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(
            client, issue_id="GEN-37", workspace_id="wrong",
            team_id="team", project_id="project",
        )
        with self.assertRaisesRegex(LinearEventError, "configured workspace"):
            adapter.apply(delta("event-a", {"order": 1}))
        self.assertFalse(any("commentCreate" in query for query, _ in client.calls))

    def test_from_env_consumes_config_route(self):
        route = {"workspace_id": "workspace", "team_id": "team", "project_id": "project"}
        client = mock.Mock()
        with mock.patch("workstream_config.resolve_linear_route", return_value=(route, None)), \
             mock.patch("workstream_linear_events.HttpGraphQLClient", return_value=client):
            adapter = LinearCommentEventAdapter.from_env(
                issue_id="GEN-37", env={"LINEAR_API_KEY": "secret"}
            )
        self.assertIs(adapter.client, client)
        self.assertEqual(adapter.workspace_id, "workspace")
        self.assertEqual(adapter.project_id, "project")


if __name__ == "__main__":
    unittest.main()
