import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("workstream_delta.py")
SPEC = importlib.util.spec_from_file_location("workstream_delta", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
sys.modules["workstream_delta"] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeLinear:
    def __init__(self):
        self.revision = 0
        self.applied = {}
        self.calls = []
        self.state = []
        self.supports_atomic_cas = True

    def apply(self, delta):
        if delta.event_id in self.applied:
            return self.applied[delta.event_id]
        if delta.expected_revision != self.revision:
            raise MODULE.RevisionConflict(
                f"expected {delta.expected_revision}, current {self.revision}"
            )
        self.revision += 1
        receipt = MODULE.MutationReceipt(delta.event_id, self.revision, f"linear-{self.revision}")
        self.applied[delta.event_id] = receipt
        self.calls.append(delta.event_id)
        self.state.append(delta.payload)
        return receipt

    def current_revision(self, workstream_id):
        return self.revision


class DeltaJournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.journal = MODULE.DeltaJournal(Path(self.temp.name) / "journal.sqlite3")

    def tearDown(self):
        self.journal.close()
        self.temp.cleanup()

    def test_append_is_durable_and_duplicate_is_one_event(self):
        payload = {"title": "new blocker", "owner": "agent"}
        first = self.journal.append("GEN-37", "blocker", payload, 0)
        second = self.journal.append("GEN-37", "blocker", payload, 0)
        self.assertEqual(first, second)
        self.assertEqual([d.event_id for d in self.journal.pending()], [first])

    def test_reused_event_id_with_different_material_is_rejected(self):
        event = self.journal.append(
            "GEN-37", "decision", {"value": "first"}, 0, event_id="same"
        )
        with self.assertRaisesRegex(ValueError, "event_id_collision"):
            self.journal.append(
                "GEN-37", "decision", {"value": "second"}, 0, event_id="same"
            )
        self.assertEqual(event, "same")
        self.assertEqual(self.journal.pending()[0].payload, {"value": "first"})

    def test_source_is_part_of_generated_event_identity(self):
        first = self.journal.append(
            "GEN-37", "decision", {"value": "same"}, 0, source="user_turn"
        )
        second = self.journal.append(
            "GEN-37", "decision", {"value": "same"}, 0, source="agent_discovery"
        )
        self.assertNotEqual(first, second)

    def test_replay_after_remote_success_before_local_ack_is_idempotent(self):
        event = self.journal.append("GEN-37", "decision", {"value": "keep"}, 0)
        adapter = FakeLinear()
        calls = {"count": 0}

        def fail_ack_once():
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("crash after remote mutation")

        self.journal.commit_hook = fail_ack_once
        with self.assertRaises(RuntimeError):
            self.journal.apply(adapter)
        self.journal.commit_hook = None
        self.journal.close()
        self.journal = MODULE.DeltaJournal(Path(self.temp.name) / "journal.sqlite3")
        self.assertEqual([d.event_id for d in self.journal.pending()], [event])
        self.assertEqual(len(self.journal.apply(adapter)), 1)
        self.assertEqual(adapter.calls, [event])
        self.assertEqual(self.journal.pending(), [])

    def test_revision_conflict_preserves_pending_delta_until_reread(self):
        self.journal.append("GEN-37", "requirement", {"text": "A"}, 0)
        adapter = FakeLinear()
        adapter.revision = 1  # another agent won revision 0
        with self.assertRaises(MODULE.RevisionConflict):
            self.journal.apply(adapter)
        self.assertEqual(len(self.journal.pending()), 1)
        self.assertEqual(adapter.calls, [])

    def test_distinct_deltas_at_same_revision_do_not_silently_overwrite(self):
        a = self.journal.append("GEN-37", "requirement", {"text": "A"}, 0)
        b = self.journal.append("GEN-37", "requirement", {"text": "B"}, 0)
        self.assertNotEqual(a, b)
        adapter = FakeLinear()
        self.assertEqual(len(self.journal.apply(adapter, limit=1)), 1)
        with self.assertRaises(MODULE.RevisionConflict):
            self.journal.apply(adapter)
        self.assertEqual(len(self.journal.pending()), 1)

    def test_concurrent_journals_rebase_loser_without_changing_event_identity(self):
        other = MODULE.DeltaJournal(Path(self.temp.name) / "other.sqlite3")
        self.addCleanup(other.close)
        first = self.journal.append(
            "GEN-37", "blocker", {"text": "A"}, 0, source="agent_discovery"
        )
        second = other.append(
            "GEN-37", "decision", {"text": "B"}, 0, source="agent_discovery"
        )
        adapter = FakeLinear()

        self.journal.apply_with_rebase(adapter)
        receipts = other.apply_with_rebase(adapter)

        self.assertEqual([receipt.event_id for receipt in receipts], [second])
        self.assertEqual(adapter.revision, 2)
        self.assertEqual(adapter.calls, [first, second])
        self.assertEqual(adapter.state, [{"text": "A"}, {"text": "B"}])
        applied = other.db.execute(
            "SELECT event_id, expected_revision, applied_revision FROM material_deltas"
        ).fetchone()
        self.assertEqual(applied, (second, 0, 2))

    def test_rebase_refuses_non_atomic_remote_before_any_mutation(self):
        class BestEffortAdapter(FakeLinear):
            supports_atomic_cas = False

        self.journal.append("GEN-37", "decision", {"text": "A"}, 0)
        adapter = BestEffortAdapter()
        adapter.supports_atomic_cas = False
        with self.assertRaisesRegex(MODULE.RemoteCASUnavailable, "remote_cas_unavailable"):
            self.journal.apply_with_rebase(adapter)
        with self.assertRaisesRegex(MODULE.RemoteCASUnavailable, "remote_cas_unavailable"):
            self.journal.apply(adapter)
        self.assertEqual(adapter.calls, [])
        self.assertEqual(len(self.journal.pending()), 1)

    def test_material_boundary_is_one_mutation_and_no_delta_is_no_write(self):
        adapter = FakeLinear()
        event = self.journal.append_boundary(
            "GEN-37",
            "boundary-1",
            [
                {"kind": "blocker", "payload": {"text": "A"}},
                {"kind": "followup", "payload": {"text": "B"}},
            ],
            0,
            source="agent_discovery",
        )
        self.assertIsNotNone(event)
        self.assertIsNone(
            self.journal.append_boundary("GEN-37", "boundary-noop", [], 0)
        )
        self.assertEqual(len(self.journal.pending()), 1)
        self.journal.apply_with_rebase(adapter)
        self.assertEqual(adapter.calls, [event])
        self.assertEqual(adapter.revision, 1)
        self.assertEqual(adapter.state[0]["boundary_id"], "boundary-1")
        self.assertEqual(len(adapter.state[0]["changes"]), 2)

    def test_malformed_boundary_refuses_before_local_journal_append(self):
        with self.assertRaisesRegex(ValueError, "malformed_material_boundary"):
            self.journal.append(
                "GEN-37", "material_boundary", {"progress": "flat"}, 0,
            )
        with self.assertRaisesRegex(ValueError, "malformed_material_boundary"):
            self.journal.append_boundary(
                "GEN-37", "boundary", [{"kind": "progress", "payload": {},
                                           "extra": True}], 0,
            )
        self.assertEqual(self.journal.pending(), [])

    def test_repair_control_has_no_business_semantics(self):
        payload = {
            "schema_version": 1, "workstream_id": "GEN-37",
            "target_bindings": [{}], "raw_frontier": {},
            "checkpoint_frontier": {}, "projection_frontier": {},
            "generation": {}, "authenticated_route": {},
            "authenticated_source": {}, "issue_graph_frontier": {},
            "ledger_serialization_frontier": [], "postwrite_oracle": {},
            "review_artifact": {},
        }
        event = MODULE.Delta(
            "repair", "GEN-37", MODULE.MATERIAL_REPAIR_KIND, "system",
            payload, 0, "2026-08-30T00:00:00Z",
        )
        self.assertEqual(MODULE.interpret_material_event(event), ())
        with self.assertRaisesRegex(ValueError, "material_semantic_repair_reserved"):
            self.journal.append(
                "GEN-37", MODULE.MATERIAL_REPAIR_KIND, payload, 0,
                source="system",
            )
        self.assertEqual(self.journal.pending(), [])

    def test_agent_discovered_delta_survives_process_reopen_before_apply(self):
        event = self.journal.append(
            "GEN-37",
            "followup",
            {"text": "discovered after tool output"},
            0,
            source="agent_discovery",
        )
        path = self.journal.path
        self.journal.close()
        self.journal = MODULE.DeltaJournal(path)
        pending = self.journal.pending()
        self.assertEqual([delta.event_id for delta in pending], [event])
        self.assertEqual(pending[0].source, "agent_discovery")


if __name__ == "__main__":
    unittest.main()
