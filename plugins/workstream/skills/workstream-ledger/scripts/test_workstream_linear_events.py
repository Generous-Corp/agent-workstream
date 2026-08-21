#!/usr/bin/env python3
from __future__ import annotations

import threading
import unittest
from unittest import mock

from workstream_delta import Delta, DeltaJournal
from workstream_linear_events import (
    LinearCommentEventAdapter,
    LinearEventError,
    encode_event_comment,
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
                    "id": "issue-37", "identifier": "GEN-37",
                    "team": {"id": "team", "organization": {"id": "workspace"}},
                    "project": {"id": "project"},
                    "comments": {
                        "nodes": nodes,
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                }
            }
        if "commentCreate" in query:
            with self.lock:
                comment = {
                    "id": f"comment-{len(self.comments) + 1}",
                    "body": variables["input"]["body"],
                    "createdAt": "now", "updatedAt": "now",
                }
                self.comments.append(comment)
            return {"commentCreate": {"success": True, "comment": dict(comment)}}
        raise AssertionError("unexpected GraphQL operation")


class LinearCommentEventAdapterTests(unittest.TestCase):
    def test_concurrent_same_revision_appends_preserve_both_deltas(self):
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

        self.assertFalse(failures)
        self.assertEqual({receipt.event_id for receipt in receipts}, {"event-a", "event-b"})
        state = reduce_event_comments(client.comments, workstream_id="GEN-37")
        self.assertEqual(state.revision, 2)
        self.assertEqual({event.event_id for event in state.events}, {"event-a", "event-b"})

    def test_delta_journals_use_append_capability_without_claiming_cas(self):
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
        second.apply(adapter)

        self.assertEqual(adapter.current_revision("GEN-37"), 2)
        self.assertEqual(first.pending(), [])
        self.assertEqual(second.pending(), [])

    def test_crash_replay_returns_existing_event_without_second_comment(self):
        client = FakeCommentClient()
        adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        event = delta("event-a", {"order": 1})
        first = adapter.apply(event)
        replay = adapter.apply(event)
        self.assertEqual(first, replay)
        self.assertEqual(len(client.comments), 1)

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
