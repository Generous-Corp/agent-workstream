#!/usr/bin/env python3

from copy import deepcopy
import unittest

from workstream_child_completion import (
    ChildCompletionError, apply_child_completion,
    build_child_completion_transaction, _post_child_valid,
)
from workstream_linear_projection import build_projection_event
import test_workstream_child_completion_prepare as fixture


def gated_state():
    state = fixture.state(include_evidence=True)
    gate = {
        "schema_version": 1, "minimum_writer_version": "0.4.82",
        "legacy_writer_count": 0, "plan_revision": fixture.PLAN,
        "observed_at": "2026-09-02T10:00:00Z",
        "writers": [{
            "writer_id": f"shipyard-{machine}", "machine_id": machine,
            "version": "0.4.82", "source_commit": "c" * 40,
            "source_tree_sha256": "d" * 64,
            "observed_at": "2026-09-02T10:00:00Z",
        } for machine in ("M1", "M3", "M5")],
    }
    state.events.append(build_projection_event(
        workstream_id="GEN-91", kind="writer_fleet_gate", key="root",
        value=gate, plan_revision=fixture.PLAN,
        expected_revision=state.revision, created_at="2026-09-02T10:00:00Z",
        authority=fixture.ROUTE,
    ))
    state.revision += 1
    state.remote_ids = {
        event["event_id"]: f"remote-{index}"
        for index, event in enumerate(state.events)
    }
    return state


def completion_snapshot():
    snap = fixture.snapshot()
    snap["root"].update({
        "id": fixture.ROOT_ID, "status_type": "started",
        "status": "In Progress", "state_id": "started",
        "state": {"id": "started", "name": "In Progress", "type": "started"},
        "title": "Root", "description": "Root description", "updatedAt": "before",
        "archivedAt": None, "parent": None, "project": {"id": "project"},
        "team": {"id": "team", "organization": {"id": "workspace"}},
        "assignee": None,
    })
    snap["children"][0].update({
        "title": "Child", "description": "Child description",
        "url": "https://linear.test/GEN-92", "updatedAt": "before",
        "archivedAt": None,
        "state": {"id": "started", "name": "In Progress", "type": "started"},
    })
    return snap


def transaction(state=None, snap=None):
    snap = snap or completion_snapshot()
    return build_child_completion_transaction(
        snap, state or gated_state(), root_token="GEN-91",
        child_token="GEN-92", evidence_contract=fixture.evidence_contract(),
        authenticated_source=fixture.SOURCE,
        authenticated_route=fixture.ROUTE,
        completed_state={"id": "done", "name": "Done", "type": "completed"},
        created_at="2026-09-02T10:01:00Z", root_material_revision=3,
        root_checkpoint_event_ids=["root-checkpoint"], root_comment_frontier=[],
        root_serialization_frontier=["root-checkpoint"],
        child_material_revision=2, child_checkpoint_event_ids=["child-checkpoint"],
        child_comment_frontier=[],
    )


class FakeAdapter:
    def __init__(self, tx, phase="open", resume="full"):
        self.tx, self.phase, self.resume = tx, phase, resume
        self.calls = []

    def surface(self):
        return {
            "phase": self.phase,
            "frontiers_sha256": self.tx["frontiers_sha256"],
            "closure": (self.tx["prospective_child_closure"]
                        if self.phase == "complete" else None),
        }

    def reserve(self, _transaction):
        self.calls.append("reserve")
        self.phase = "reserved_open"
        return "created"

    def update_child_state(self, child_id, state_id):
        self.calls.append(("update", child_id, state_id))
        self.phase = "reserved_completed"

    def append_closure(self, event):
        self.calls.append(("closure", event["event_id"]))
        self.phase = "complete"

    def full_resume(self):
        self.calls.append("resume")
        return {"resume_authority": self.resume}


class ChildCompletionTests(unittest.TestCase):
    def test_preview_is_zero_write(self):
        tx = transaction()
        adapter = FakeAdapter(tx)
        result = apply_child_completion(adapter, tx, apply=False)
        self.assertFalse(result["apply"])
        self.assertEqual(adapter.calls, [])

    def test_normal_success_reserves_updates_closes_and_requires_full(self):
        tx = transaction()
        adapter = FakeAdapter(tx)
        result = apply_child_completion(adapter, tx, apply=True)
        self.assertEqual(result["operation_status"], "complete")
        self.assertEqual([item if isinstance(item, str) else item[0]
                          for item in adapter.calls],
                         ["reserve", "update", "closure", "resume"])

    def test_post_native_crash_replay_only_finalizes_closure(self):
        tx = transaction()
        adapter = FakeAdapter(tx, phase="reserved_completed")
        apply_child_completion(adapter, tx, apply=True)
        self.assertEqual([item if isinstance(item, str) else item[0]
                          for item in adapter.calls], ["closure", "resume"])

    def test_pre_native_crash_replay_continues_without_second_reservation(self):
        tx = transaction()
        adapter = FakeAdapter(tx, phase="reserved_open")
        apply_child_completion(adapter, tx, apply=True)
        self.assertEqual([item if isinstance(item, str) else item[0]
                          for item in adapter.calls],
                         ["update", "closure", "resume"])

    def test_complete_replay_is_noop_except_resume(self):
        tx = transaction()
        adapter = FakeAdapter(tx, phase="complete")
        apply_child_completion(adapter, tx, apply=True)
        self.assertEqual(adapter.calls, ["resume"])

    def test_frontier_race_refuses_before_write(self):
        tx = transaction()
        adapter = FakeAdapter(tx)
        original = adapter.surface
        adapter.surface = lambda: {**original(), "frontiers_sha256": "0" * 64}
        with self.assertRaisesRegex(ChildCompletionError, "frontier_drift"):
            apply_child_completion(adapter, tx, apply=True)
        self.assertEqual(adapter.calls, [])

    def test_nonfull_resume_is_never_success(self):
        tx = transaction()
        with self.assertRaisesRegex(ChildCompletionError, "full_resume_required"):
            apply_child_completion(FakeAdapter(tx, resume="partial"), tx, apply=True)

    def test_old_writer_gate_refuses(self):
        state = gated_state()
        state.events[-1] = deepcopy(state.events[-1])
        state.events[-1]["value"]["minimum_writer_version"] = "0.4.81"
        with self.assertRaisesRegex(ChildCompletionError, "fleet_gate_required"):
            transaction(state)

    def test_planned_or_archived_native_issue_refuses(self):
        snap = completion_snapshot()
        snap["children"][0]["status_type"] = "planned"
        snap["children"][0]["state"]["type"] = "planned"
        with self.assertRaisesRegex(ChildCompletionError, "started_native"):
            transaction(snap=snap)
        snap = completion_snapshot()
        snap["root"]["archivedAt"] = "2026-09-02T10:02:00Z"
        with self.assertRaisesRegex(ChildCompletionError, "unarchived"):
            transaction(snap=snap)

    def test_native_fence_binds_preserved_fields_and_full_state(self):
        tx = transaction()
        native = tx["native_child_before"]
        self.assertEqual(native["title"], "Child")
        self.assertEqual(native["description"], "Child description")
        self.assertEqual(native["url"], "https://linear.test/GEN-92")
        self.assertEqual(native["updatedAt"], "before")
        self.assertEqual(native["state"]["type"], "started")
        self.assertIn("native_root_sha256", tx["frontiers"])
        self.assertIn("dependency_graph_sha256", tx["frontiers"])

    def test_post_native_clock_may_change_while_preserved_fields_match(self):
        tx = transaction()
        before = tx["native_child_before"]
        after = deepcopy(before)
        after["state"] = {"id": "done", "name": "Done", "type": "completed"}
        after["state_id"] = "done"
        after["status"] = "Done"
        after["status_type"] = "completed"
        after["updatedAt"] = "after"
        self.assertTrue(_post_child_valid(
            after, before, tx["completed_state"],
        ))


if __name__ == "__main__":
    unittest.main()
