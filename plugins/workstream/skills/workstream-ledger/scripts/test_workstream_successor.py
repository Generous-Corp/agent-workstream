#!/usr/bin/env python3
import unittest

from workstream_successor import SuccessorError, choose_disposition, successor_command


class SuccessorTests(unittest.TestCase):
    def test_non_gen_team_token_attaches(self):
        snapshot = {"root": {"identifier": "OPS-37"}, "provenance": {"worktree": {"state": "safe", "head": "abc"}}}
        result = choose_disposition(snapshot, remote_head="abc")
        self.assertEqual(result["workstream"], "OPS-37")

    def test_current_clean_matching_worktree_attaches(self):
        snapshot = {"root": {"identifier": "GEN-37"}, "provenance": {"worktree": {"state": "safe", "head": "abc"}}}
        self.assertEqual(choose_disposition(snapshot, remote_head="abc")["disposition"], "attach")

    def test_compact_provenance_requires_projection_bound_latest(self):
        head = "a" * 40
        snapshot = {
            "root": {"identifier": "GEN-37"},
            "context_schema": {
                "name": "agent-workstream.resume-context", "version": 2,
                "representation": "compact_validated",
            },
            "provenance": {
                "worktree_authority_count": 1,
                "worktree_authority_ambiguous": False,
                "latest": {"worktree": {"state": "safe", "head": head}},
                "latest_projection_head": {
                    "key": "session", "event_id": "event",
                    "value_sha256": "b" * 64,
                },
            },
        }
        self.assertEqual(
            choose_disposition(snapshot, remote_head=head)["disposition"], "attach",
        )
        full = {
            "root": {"identifier": "GEN-37"},
            "provenance": [{"worktree": {"state": "safe", "head": head}}],
        }
        self.assertEqual(
            choose_disposition(full, remote_head=head)["disposition"], "attach",
        )
        snapshot["provenance"].update({
            "worktree_authority_count": 0,
            "latest": None,
            "latest_projection_head": None,
        })
        self.assertEqual(
            choose_disposition(snapshot, remote_head=head)["disposition"],
            "create_successor",
        )
        full["provenance"] = []
        self.assertEqual(
            choose_disposition(full, remote_head=head)["disposition"],
            "create_successor",
        )

    def test_compact_and_full_multiple_worktrees_refuse_identically(self):
        head = "a" * 40
        compact = {
            "root": {"identifier": "GEN-37"},
            "context_schema": {
                "name": "agent-workstream.resume-context", "version": 2,
                "representation": "compact_validated",
            },
            "provenance": {
                "worktree_authority_count": 2,
                "worktree_authority_ambiguous": True,
                "latest": None, "latest_projection_head": None,
            },
        }
        full = {
            "root": {"identifier": "GEN-37"},
            "provenance": [
                {"worktree": {"state": "safe", "head": head}},
                {"worktree": {"state": "safe", "head": head}},
            ],
        }
        for snapshot in (compact, full):
            with self.assertRaisesRegex(
                SuccessorError, "multiple projected worktree authorities",
            ):
                choose_disposition(snapshot, remote_head=head)

    def test_compact_and_full_dirty_or_stale_worktree_create_same_successor(self):
        remote_head = "a" * 40
        for state in ("dirty", "stale"):
            with self.subTest(state=state):
                worktree = {
                    "state": state, "path": f"/{state}", "head": "b" * 40,
                }
                compact = {
                    "root": {"identifier": "GEN-37"},
                    "context_schema": {
                        "name": "agent-workstream.resume-context", "version": 2,
                        "representation": "compact_validated",
                    },
                    "provenance": {
                        "worktree_authority_count": 1,
                        "worktree_authority_ambiguous": False,
                        "latest": {"worktree": worktree},
                        "latest_projection_head": {
                            "key": state, "event_id": f"event-{state}",
                            "value_sha256": "c" * 64,
                        },
                    },
                }
                full = {
                    "root": {"identifier": "GEN-37"},
                    "provenance": [{"worktree": worktree}],
                }
                compact_result = choose_disposition(
                    compact, remote_head=remote_head,
                )
                full_result = choose_disposition(full, remote_head=remote_head)
                self.assertEqual(
                    compact_result["disposition"], full_result["disposition"],
                )
                self.assertEqual(
                    compact_result["predecessor"], full_result["predecessor"],
                )
                self.assertEqual(compact_result["reason"], full_result["reason"])

    def test_compact_and_full_safe_plus_dirty_refuse_identically(self):
        head = "a" * 40
        compact = {
            "root": {"identifier": "GEN-37"},
            "context_schema": {
                "name": "agent-workstream.resume-context", "version": 2,
                "representation": "compact_validated",
            },
            "provenance": {
                "worktree_authority_count": 2,
                "worktree_authority_ambiguous": True,
                "latest": None, "latest_projection_head": None,
            },
        }
        full = {
            "root": {"identifier": "GEN-37"},
            "provenance": [
                {"worktree": {"state": "safe", "head": head}},
                {"worktree": {
                    "state": "dirty", "path": "/dirty", "head": "b" * 40,
                }},
            ],
        }
        for snapshot in (compact, full):
            with self.assertRaisesRegex(
                SuccessorError, "multiple projected worktree authorities",
            ):
                choose_disposition(snapshot, remote_head=head)

    def test_recovered_checkpoint_is_the_worktree_authority(self):
        snapshot = {
            "root": {"identifier": "GEN-37"},
            "provenance": {"worktree": {"state": "stale", "head": "old"}},
            "latest_checkpoint": {
                "checkpoint_event_id": "wsc-current",
                "worktree": {"state": "safe", "head": "current"},
            },
        }
        result = choose_disposition(snapshot, remote_head="current")
        self.assertEqual(result["disposition"], "attach")
        self.assertEqual(result["recovered_from_checkpoint"], "wsc-current")

    def test_recorded_disposition_must_match_recovered_checkpoint_and_live_head(self):
        head = "a" * 40
        snapshot = {
            "workstream_id": "GEN-37",
            "latest_checkpoint": {
                "checkpoint_event_id": "wsc-current",
                "worktree": {"state": "safe", "head": head},
            },
            "disposition": {"disposition": "attach", "remote_head": head,
                            "recovered_from_checkpoint": "wsc-current"},
        }
        result = choose_disposition(snapshot, remote_head=head)
        self.assertFalse(result["durable_projection_required"])
        snapshot["disposition"]["remote_head"] = "b" * 40
        with self.assertRaisesRegex(SuccessorError, "recorded_disposition_conflict:remote_head"):
            choose_disposition(snapshot, remote_head=head)

    def test_stale_worktree_creates_successor_from_remote(self):
        snapshot = {"root": {"identifier": "GEN-37"}, "provenance": {"worktree": {"state": "stale", "path": "/old", "head": "old"}}}
        remote_head = "a" * 40
        result = successor_command(snapshot, remote_repo="/repo", remote_ref="origin/main", remote_head=remote_head, successor_path="/new", branch="gen37-successor")
        self.assertEqual(result["disposition"], "create_successor")
        self.assertEqual(result["command"], ["git", "-C", "/repo", "worktree", "add", "-b", "gen37-successor", "/new", remote_head])
        self.assertEqual(result["verified_remote"], {"ref": "origin/main", "head": remote_head})
        self.assertEqual(result["predecessor"]["path"], "/old")

    def test_successor_command_refuses_without_verified_remote_head(self):
        snapshot = {"root": {"identifier": "GEN-37"}, "provenance": {"worktree": {"state": "unavailable"}}}
        with self.assertRaisesRegex(SuccessorError, "verified full remote head"):
            successor_command(
                snapshot,
                remote_repo="/repo",
                remote_ref="origin/main",
                successor_path="/new",
                branch="gen37-successor",
            )

    def test_safe_label_without_remote_head_does_not_attach(self):
        snapshot = {"root": {"identifier": "GEN-37"}, "provenance": {"worktree": {"state": "safe", "head": "abc"}}}
        result = choose_disposition(snapshot)
        self.assertEqual(result["disposition"], "create_successor")
        self.assertEqual(result["reason"], "current remote head is unavailable")

    def test_successor_command_never_compares_a_worktree_head_to_itself(self):
        snapshot = {"root": {"identifier": "GEN-37"}, "provenance": {"worktree": {"state": "safe", "head": "old"}}}
        result = successor_command(
            snapshot,
            remote_repo="/repo",
            remote_ref="origin/main",
            remote_head="b" * 40,
            successor_path="/new",
            branch="gen37-successor",
        )
        self.assertEqual(result["disposition"], "create_successor")
        self.assertEqual(result["reason"], "worktree does not match current remote head")

    def test_unknown_token_fails_closed(self):
        with self.assertRaises(SuccessorError):
            choose_disposition({"root": {"identifier": "not-an-issue"}})

    def test_attach_does_not_emit_successor_command(self):
        head = "a" * 40
        snapshot = {"root": {"identifier": "GEN-37"}, "provenance": {"worktree": {"state": "safe", "head": head}}}
        with self.assertRaises(SuccessorError):
            successor_command(
                snapshot,
                remote_repo="/repo",
                remote_ref="origin/main",
                remote_head=head,
                successor_path="/new",
                branch="b",
            )


if __name__ == "__main__":
    unittest.main()
