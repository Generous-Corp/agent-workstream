#!/usr/bin/env python3
import unittest

from workstream_choices import (
    ChoiceError, audit_choice, closure_blockers, record_choice, reduce_choices,
    supersede_choice,
)


HEAD_A = "a" * 40
HEAD_B = "b" * 40


class ChoiceEventTests(unittest.TestCase):
    def record(self, **overrides):
        values = dict(
            choice_id="choice-cache-authority", workstream_id="GEN-37",
            owning_child="GEN-38", namespace="pulp-playback",
            repository="github.com/generous-corp/pulp", repository_key="github.com:id:R_pulp",
            plan_revision="plan-a", git_head=HEAD_A,
            created_at="2026-08-21T12:00:00Z", spec_gap="Cache owner unspecified",
            decision="Worker owns cache writes", alternatives=["Coordinator writes"],
            reach="system", irreversible=False, domains=["concurrency", "authority"],
            technical_confidence="high", intent_confidence="low",
        )
        values.update(overrides)
        return record_choice(**values)

    def test_high_risk_choice_blocks_until_fresh_read_only_audit_accepts_current_head(self):
        record = self.record()
        self.assertEqual(
            closure_blockers([record], plan_revision="plan-a", exact_head=HEAD_A),
            ["choice_landing_blocked:choice-cache-authority"],
        )
        audit = audit_choice(
            choice_id=record["choice_id"], workstream_id="GEN-37",
            owning_child="GEN-38", namespace="pulp-playback", repository="github.com/generous-corp/pulp", repository_key="github.com:id:R_pulp",
            plan_revision="plan-a", git_head=HEAD_B,
            created_at="2026-08-21T13:00:00Z", recorded_event_id=record["event_id"],
            verdict="accepted", rationale="Ownership invariant is explicit",
            auditor="fresh-agent",
        )
        self.assertEqual(
            closure_blockers([record, audit], plan_revision="plan-a", exact_head=HEAD_B), []
        )

    def test_reversible_low_risk_choice_may_remain_provisional(self):
        record = self.record(
            choice_id="choice-label", reach="local", domains=[],
            decision="Use short label", irreversible=False,
        )
        audit = audit_choice(
            choice_id=record["choice_id"], workstream_id="GEN-37",
            owning_child="GEN-38", namespace="pulp-playback", repository="github.com/generous-corp/pulp", repository_key="github.com:id:R_pulp",
            plan_revision="plan-a", git_head=HEAD_A,
            created_at="2026-08-21T13:00:00Z", recorded_event_id=record["event_id"],
            verdict="provisional", rationale="Cheap to revise", auditor="fresh-agent",
        )
        view = reduce_choices([audit, record])[record["choice_id"]]
        self.assertFalse(view["landing_blocked"])

    def test_low_risk_choice_still_requires_explicit_audit_verdict(self):
        record = self.record(choice_id="choice-label", reach="local", domains=[])
        self.assertTrue(reduce_choices([record])["choice-label"]["landing_blocked"])

    def test_high_risk_choice_cannot_be_marked_provisional(self):
        record = self.record()
        audit = audit_choice(
            choice_id=record["choice_id"], workstream_id="GEN-37",
            owning_child="GEN-38", namespace="pulp-playback", repository="github.com/generous-corp/pulp", repository_key="github.com:id:R_pulp",
            plan_revision="plan-a", git_head=HEAD_A,
            created_at="2026-08-21T13:00:00Z", recorded_event_id=record["event_id"],
            verdict="provisional", rationale="Wait and see", auditor="fresh-agent",
        )
        with self.assertRaisesRegex(ChoiceError, "cannot_be_provisional"):
            reduce_choices([record, audit])

    def test_must_fix_blocks_and_supersession_preserves_history_but_removes_block(self):
        record = self.record(domains=[], reach="local")
        audit = audit_choice(
            choice_id=record["choice_id"], workstream_id="GEN-37",
            owning_child="GEN-38", namespace="pulp-playback", repository="github.com/generous-corp/pulp", repository_key="github.com:id:R_pulp",
            plan_revision="plan-a", git_head=HEAD_A,
            created_at="2026-08-21T13:00:00Z", recorded_event_id=record["event_id"],
            verdict="must_fix", rationale="Wrong owner", auditor="fresh-agent",
        )
        self.assertTrue(reduce_choices([record, audit])[record["choice_id"]]["landing_blocked"])
        retired = supersede_choice(
            choice_id=record["choice_id"], workstream_id="GEN-37",
            owning_child="GEN-38", namespace="pulp-playback", repository="github.com/generous-corp/pulp", repository_key="github.com:id:R_pulp",
            plan_revision="plan-a", git_head=HEAD_A,
            created_at="2026-08-21T14:00:00Z", target_event_id=record["event_id"],
            reason="Replaced by explicit coordinator ownership",
            successor_choice_id="choice-coordinator-authority",
        )
        successor = self.record(
            choice_id="choice-coordinator-authority",
            decision="Coordinator owns writes", created_at="2026-08-21T13:30:00Z",
            domains=[], reach="local",
        )
        view = reduce_choices([record, audit, retired, successor])[record["choice_id"]]
        self.assertFalse(view["active"])
        self.assertEqual(len(view["audits"]), 1)

    def test_audit_cannot_silently_move_choice_to_another_child(self):
        record = self.record()
        audit = audit_choice(
            choice_id=record["choice_id"], workstream_id="GEN-37",
            owning_child="GEN-99", namespace="pulp-playback", repository="github.com/generous-corp/pulp", repository_key="github.com:id:R_pulp",
            plan_revision="plan-a", git_head=HEAD_A,
            created_at="2026-08-21T13:00:00Z", recorded_event_id=record["event_id"],
            verdict="accepted", rationale="Looks fine", auditor="fresh-agent",
        )
        with self.assertRaisesRegex(ChoiceError, "ownership_changed"):
            reduce_choices([record, audit])

    def test_closure_detects_plan_and_head_drift(self):
        record = self.record(domains=[], reach="local")
        self.assertEqual(
            closure_blockers([record], plan_revision="plan-b", exact_head=HEAD_B),
            ["choice_head_not_reconciled:choice-cache-authority",
             "choice_landing_blocked:choice-cache-authority",
             "choice_plan_drift:choice-cache-authority"],
        )

    def test_unknown_domain_typo_cannot_bypass_high_risk_policy(self):
        with self.assertRaisesRegex(ChoiceError, "unknown_domains"):
            self.record(domains=["securty"])

    def test_supersession_cannot_name_a_missing_successor(self):
        record = self.record(domains=[], reach="local")
        retired = supersede_choice(
            choice_id=record["choice_id"], workstream_id="GEN-37", owning_child="GEN-38",
            namespace="pulp-playback", repository="github.com/generous-corp/pulp", repository_key="github.com:id:R_pulp",
            plan_revision="plan-a", git_head=HEAD_A, created_at="2026-08-21T14:00:00Z",
            target_event_id=record["event_id"], reason="Replace it",
            successor_choice_id="choice-missing",
        )
        with self.assertRaisesRegex(ChoiceError, "successor_choice_not_found"):
            reduce_choices([record, retired])

    def test_timestamp_requires_iso_timezone_for_deterministic_ordering(self):
        with self.assertRaisesRegex(ChoiceError, "created_at_requires_timezone"):
            self.record(created_at="2026-08-21T12:00:00")

    def test_repository_transfer_changes_route_not_immutable_choice_ownership(self):
        record = self.record(repository="github.com/danielraffel/pulp")
        audit = audit_choice(
            choice_id=record["choice_id"], workstream_id="GEN-37", owning_child="GEN-38",
            namespace="pulp-playback", repository="github.com/generous-corp/pulp",
            repository_key="github.com:id:R_pulp", plan_revision="plan-a", git_head=HEAD_A,
            created_at="2026-08-21T13:00:00Z", recorded_event_id=record["event_id"],
            verdict="accepted", rationale="Provider identity is unchanged", auditor="fresh-agent",
        )
        self.assertFalse(reduce_choices([record, audit])[record["choice_id"]]["landing_blocked"])


if __name__ == "__main__":
    unittest.main()
