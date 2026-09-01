#!/usr/bin/env python3
from __future__ import annotations

import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest

from test_workstream_closure import ClosureTests
from test_workstream_linear_projection import (
    AUTHORITY, FakeProjectionClient, legacy_comment, legacy_event,
    live_graph_with_empty_child_comments, PLAN, ROOT_UUID, scope,
)
from workstream_child_dependencies import LinearChildDependencyAdapter
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, LinearProjectionAdapter,
    projection_slot_id,
)
from workstream_reconcile import (
    _bounded_command, authenticated_reconcile_snapshot, canonical_digest,
    github_token_from_command, GitHubTruthReader, parse_repository_bindings,
    ReconcileError, reconcile_lifecycle,
    ShipyardTruthReader,
)
from workstream_relation_readback import read_relation_targets
from workstream_resume import (
    add_material_history, closure_snapshot_digest, compact_context, ResumeError,
)


HEAD = "a" * 40
MERGE = "b" * 40
KEY = "github.com:id:R_pulp"
HEAD_2 = "c" * 40
MERGE_2 = "d" * 40
KEY_2 = "github.com:id:R_vellum"


def github_truth():
    return {
        "repository": "generous-corp/pulp", "provider_repository_id": "R_pulp",
        "pr_number": 41, "pr_head": HEAD, "merged": True, "merge_sha": MERGE,
    }


def shipyard_truth():
    value = {
        "schema_version": 1, "repository": "generous-corp/pulp",
        "repository_key": KEY, "pr_number": 41, "head": HEAD,
        "disposition": "merged", "receipt_id": "shipyard-receipt-41",
    }
    return {**value, "receipt_sha256": canonical_digest(value)}


def github_truth_2():
    return {
        "repository": "generous-corp/vellum", "provider_repository_id": "R_vellum",
        "pr_number": 52, "pr_head": HEAD_2, "merged": True, "merge_sha": MERGE_2,
    }


def shipyard_truth_2():
    value = {
        "schema_version": 1, "repository": "generous-corp/vellum",
        "repository_key": KEY_2, "pr_number": 52, "head": HEAD_2,
        "disposition": "merged", "receipt_id": "shipyard-receipt-52",
    }
    return {**value, "receipt_sha256": canonical_digest(value)}


def closure_input():
    return {
        "criteria": ["a"], "evidence": ClosureTests().evidence(),
        "excluded": [], "required_child_ids": ["GEN-38"],
    }


def snapshot():
    value = ClosureTests().factory_snapshot({
        "root": {
            "identifier": "GEN-37", "url": "https://linear/GEN-37",
            "plan_revision": "sha", "revision": 2, "status": "In Progress",
        },
        "children": [{"identifier": "GEN-38", "status": "Done", "owner": "agent"}],
    })
    value["projection_revision"] = 0
    value["projection_events"] = []
    value["lifecycle"] = None
    value["provenance"] = [{
        "agent": "codex", "machine": "M5", "session_id": "implementer-session",
    }]
    return value


def multi_repository_snapshot():
    value = snapshot()
    value["children"].append({
        "identifier": "GEN-39", "status": "Done", "owner": "agent",
    })
    second_repository = {
        "slug": "github.com/generous-corp/vellum", "provider_repository_id": "R_vellum",
        "aliases": [], "identity_resolution": {
            "provider_repository_id": "R_vellum",
            "resolved_slug": "github.com/generous-corp/vellum",
            "observed_at": "2026-08-21T11:00:00Z",
            "evidence": [{"kind": "authenticated_provider_readback", "authenticated": True,
                          "provider_repository_id": "R_vellum",
                          "resolved_slug": "github.com/generous-corp/vellum"}],
        }, "identity_updates": [], "exact_head": HEAD_2, "evidence": [],
    }
    value["scope"]["repositories"].append(second_repository)
    value["scope"]["child_ownership"]["GEN-39"] = KEY_2
    second_contract = json.loads(json.dumps(value["evidence_contracts"][0]))
    second_contract.update({
        "slice_id": "gen-39", "owning_child": "GEN-39",
        "repository": "github.com/generous-corp/vellum",
        "repository_key": KEY_2, "exact_head": HEAD_2,
    })
    for layer in second_contract["layers"].values():
        for receipt in layer.get("receipts", []):
            receipt.update({"repository_key": KEY_2, "exact_head": HEAD_2})
    value["evidence_contracts"].append(second_contract)
    return value


class Response:
    def __init__(self, value):
        self.value = json.dumps(value).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self, _limit):
        return self.value


class ReconcileTests(unittest.TestCase):
    def test_reconcile_selects_finalized_generation_and_execution_status(self):
        from test_workstream_generation_transition import (
            AUTHORITY as GENERATION_AUTHORITY,
            GenerationTransitionTests, NEW, WORKSTREAM,
        )
        from workstream_linear import LinearGraphQLTransport

        generation = GenerationTransitionTests()
        generation.setUp()
        from test_workstream_generation_transition import project_full
        project_full(generation.client, NEW)
        generation_transport = generation.native_and_source_fenced_transport({
            "identity": f"https://example.test/{NEW}", "sha256": NEW,
        })
        proof = generation_transport.preview_activate(
            target_plan_revision=NEW, created_at="now",
            retirement=generation.retirement(),
        )["native_root_activation_proof"]
        generation_transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=generation.retirement(),
            expected_native_root_sha256=proof["sha256"],
        )
        generation.client.graph_status = "Done"
        generation.client.graph_status_type = "completed"
        transport = LinearGraphQLTransport(
            generation.client, workspace_id="workspace", team_id="team",
            project_id="project",
        )
        raw = transport.snapshot_for_root(
            WORKSTREAM, include_description=True, include_child_comments=True,
        )
        comments = deepcopy(generation.client.comments)
        dependency_adapter = LinearChildDependencyAdapter(
            generation.client, workspace_id="workspace", team_id="team",
            project_id="project",
            root_issue_id=GENERATION_AUTHORITY["root_issue_id"],
            root_identifier=WORKSTREAM, plan_revision=NEW,
        )

        reconciled = authenticated_reconcile_snapshot(
            raw, comments, WORKSTREAM,
            authenticated_route=GENERATION_AUTHORITY,
            authenticated_source={
                "identity": f"https://example.test/{NEW}", "sha256": NEW,
            },
            dependency_adapter=dependency_adapter,
            reread=lambda: (deepcopy(raw), deepcopy(comments)),
        )

        self.assertEqual(reconciled["root"]["plan_revision"], NEW)
        self.assertEqual(reconciled["root"]["status"], "In Progress")
        self.assertEqual(reconciled["root"]["status_type"], "started")
        self.assertEqual(reconciled["root"]["issue_status"], "Done")
        self.assertEqual(reconciled["root"]["issue_status_type"], "completed")
        self.assertRegex(reconciled["dependency_graph"]["sha256"], r"^[0-9a-f]{64}$")

    def adapter(self, client):
        return LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision="sha",
            workspace_id="workspace", team_id="team", project_id="project",
            root_issue_id=ROOT_UUID,
        )

    def review_receipt(self, snap, material):
        return {
            "schema_version": 1, "workstream_id": "GEN-37",
            "snapshot_sha256": closure_snapshot_digest(snap),
            "closure_input_sha256": canonical_digest(material),
            "repository_key": KEY, "exact_head": HEAD, "verdict": "pass",
            "reviewer_agent": "claude", "reviewer_session_id": "reviewer-session",
            "implementer_session_id": "implementer-session",
            "reviewed_at": "2026-08-28T12:00:00Z",
            "review_artifact_identity": (
                "https://github.com/Generous-Corp/agent-workstream/"
                f"blob/{HEAD}/reviews/GEN-37.md"
            ),
            "review_artifact_sha256": "c" * 64,
            "trust_boundary": "shared_linear_credential",
            "procedural_independence": True,
        }

    def persist_review(self, client, snap, material):
        if not any(
            event["kind"] == "provenance" for event in self.adapter(client).state().events
        ):
            provenance = build_projection_event(
                workstream_id="GEN-37", kind="provenance",
                key="implementer-session", value=snap["provenance"][0],
                plan_revision="sha", expected_revision=snap["projection_revision"],
                created_at="2026-08-28T11:59:00Z",
                authority=self.adapter(client).authority,
            )
            self.adapter(client).append(provenance)
            snap["projection_events"] = list(self.adapter(client).state().events)
            snap["projection_revision"] += 1
        receipt = self.review_receipt(snap, material)
        event = build_projection_event(
            workstream_id="GEN-37", kind="closure_review",
            key=receipt["snapshot_sha256"], value=receipt, plan_revision="sha",
            expected_revision=snap["projection_revision"],
            created_at="2026-08-28T12:00:00Z",
            authority=self.adapter(client).authority,
        )
        self.adapter(client).append(event)
        snap["projection_events"] = list(self.adapter(client).state().events)
        snap["projection_revision"] += 1
        snap["closure_reviews"] = [receipt]
        return receipt

    def aggregate_review_receipt(self, snap, material):
        truths = [
            {"repository_key": KEY, "github": github_truth(),
             "shipyard_receipt": shipyard_truth()},
            {"repository_key": KEY_2, "github": github_truth_2(),
             "shipyard_receipt": shipyard_truth_2()},
        ]
        return {
            "schema_version": 2, "workstream_id": "GEN-37",
            "snapshot_sha256": closure_snapshot_digest(snap),
            "closure_input_sha256": canonical_digest(material),
            "repository_heads": {KEY: HEAD, KEY_2: HEAD_2},
            "repository_truth_sha256": canonical_digest(truths),
            "verdict": "pass", "reviewer_agent": "claude",
            "reviewer_session_id": "reviewer-session",
            "implementer_session_id": "implementer-session",
            "reviewed_at": "2026-08-28T12:00:00Z",
            "review_artifact_identity": "https://example.test/reviews/GEN-37.md",
            "review_artifact_sha256": "c" * 64,
            "trust_boundary": "shared_linear_credential",
            "procedural_independence": True,
        }

    def persist_aggregate_review(self, client, snap, material):
        provenance = build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="implementer-session",
            value=snap["provenance"][0], plan_revision="sha",
            expected_revision=snap["projection_revision"],
            created_at="2026-08-28T11:59:00Z", authority=self.adapter(client).authority,
        )
        self.adapter(client).append(provenance)
        snap["projection_events"] = list(self.adapter(client).state().events)
        snap["projection_revision"] += 1
        receipt = self.aggregate_review_receipt(snap, material)
        event = build_projection_event(
            workstream_id="GEN-37", kind="closure_review",
            key=receipt["snapshot_sha256"], value=receipt, plan_revision="sha",
            expected_revision=snap["projection_revision"],
            created_at="2026-08-28T12:00:00Z", authority=self.adapter(client).authority,
        )
        self.adapter(client).append(event)
        snap["projection_events"] = list(self.adapter(client).state().events)
        snap["projection_revision"] += 1
        snap["closure_reviews"] = [receipt]
        return receipt

    def test_multi_repository_landing_and_semantic_closure_are_aggregate(self):
        client = FakeProjectionClient()
        snap = multi_repository_snapshot()
        material = closure_input(); material["required_child_ids"] = ["GEN-38", "GEN-39"]
        landed = reconcile_lifecycle(
            snapshot=snap, adapter=self.adapter(client),
            github=[github_truth(), github_truth_2()],
            shipyard=[shipyard_truth(), shipyard_truth_2()], closure_input=material,
            independent_review=None, created_at="2026-08-28T12:00:00Z",
        )
        self.assertEqual(landed["status"], "Landed — acceptance review required")
        self.assertEqual(
            [item["repository_key"] for item in landed["lifecycle"]["repositories"]],
            [KEY, KEY_2],
        )

        client = FakeProjectionClient()
        snap = multi_repository_snapshot()
        receipt = self.persist_aggregate_review(client, snap, material)
        done = reconcile_lifecycle(
            snapshot=snap, adapter=self.adapter(client),
            github=[github_truth(), github_truth_2()],
            shipyard=[shipyard_truth(), shipyard_truth_2()], closure_input=material,
            independent_review=receipt, created_at="2026-08-28T12:00:00Z",
        )
        self.assertEqual(done["status"], "Done")

    def test_multi_repository_missing_or_drifted_truth_blocks_aggregate_with_zero_writes(self):
        for github_items, error in (
            ([github_truth()], "repository_truth_keyset_mismatch"),
            ([github_truth(), {**github_truth_2(), "pr_head": "e" * 40}],
             f"github_truth_scope_mismatch:{KEY_2}"),
        ):
            with self.subTest(error=error):
                client = FakeProjectionClient()
                snap = multi_repository_snapshot()
                material = closure_input(); material["required_child_ids"] = ["GEN-38", "GEN-39"]
                with self.assertRaisesRegex(ReconcileError, error):
                    reconcile_lifecycle(
                        snapshot=snap, adapter=self.adapter(client), github=github_items,
                        shipyard=[shipyard_truth(), shipyard_truth_2()], closure_input=material,
                        independent_review=None, created_at="2026-08-28T12:00:00Z",
                    )
                self.assertEqual(client.comments, [])

    def test_landed_persists_and_fresh_replay_is_zero_write(self):
        client = FakeProjectionClient()
        snap = snapshot()
        first = reconcile_lifecycle(
            snapshot=snap, adapter=self.adapter(client), github=github_truth(),
            shipyard=shipyard_truth(), closure_input=closure_input(),
            independent_review=None, created_at="2026-08-28T12:00:00Z",
        )
        self.assertEqual(first["status"], "Landed — acceptance review required")
        self.assertEqual(len(first["writes"]), 1)
        snap["projection_revision"] = 1
        replay = reconcile_lifecycle(
            snapshot=snap, adapter=self.adapter(client), github=github_truth(),
            shipyard=shipyard_truth(), closure_input=closure_input(),
            independent_review=None, created_at="2026-08-28T12:01:00Z",
        )
        self.assertEqual(replay["writes"], [])
        self.assertEqual(len(client.comments), 1)

    def test_done_requires_independent_exact_snapshot_review_and_persists_digest(self):
        client = FakeProjectionClient()
        snap = snapshot()
        material = closure_input()
        receipt = self.persist_review(client, snap, material)
        result = reconcile_lifecycle(
            snapshot=snap, adapter=self.adapter(client), github=github_truth(),
            shipyard=shipyard_truth(), closure_input=material,
            independent_review=receipt,
            created_at="2026-08-28T12:00:00Z",
        )
        self.assertEqual(result["status"], "Done")
        self.assertRegex(result["lifecycle"]["closure_receipt_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(len(client.comments), 3)

    def test_done_restart_after_remote_write_replays_without_second_write(self):
        client = FakeProjectionClient()
        snap = snapshot()
        material = closure_input()
        receipt = self.persist_review(client, snap, material)
        first = reconcile_lifecycle(
            snapshot=snap, adapter=self.adapter(client), github=github_truth(),
            shipyard=shipyard_truth(), closure_input=material,
            independent_review=receipt, created_at="2026-08-28T12:00:00Z",
        )
        post = snapshot()
        post["root"]["issue_status"] = post["root"]["status"]
        post["root"]["status"] = "Done"
        post["root"]["closure_receipt"] = first["lifecycle"]["closure_receipt_sha256"]
        post["lifecycle"] = first["lifecycle"]
        post["projection_events"] = list(self.adapter(client).state().events)
        post["projection_revision"] = 3
        post["closure_reviews"] = [receipt]
        replay = reconcile_lifecycle(
            snapshot=post, adapter=self.adapter(client), github=github_truth(),
            shipyard=shipyard_truth(), closure_input=material,
            independent_review=receipt, created_at="2026-08-28T12:01:00Z",
        )
        self.assertEqual(replay["writes"], [])
        self.assertEqual(len(client.comments), 3)
        status_only = reconcile_lifecycle(
            snapshot=post, adapter=self.adapter(client), github=github_truth(),
            shipyard=shipyard_truth(), closure_input=material,
            independent_review=None, created_at="2026-08-28T12:02:00Z",
        )
        self.assertEqual(status_only["status"], "Done")
        self.assertEqual(status_only["writes"], [])

        changed = closure_input(); changed["criteria"] = ["a", "new-gate"]
        changed["evidence"]["new-gate"] = {"satisfied": True}
        with self.assertRaisesRegex(ReconcileError, "done_lifecycle_cannot"):
            reconcile_lifecycle(
                snapshot=post, adapter=self.adapter(client), github=github_truth(),
                shipyard=shipyard_truth(), closure_input=changed,
                independent_review=None, created_at="2026-08-28T12:03:00Z",
            )
        self.assertEqual(len(client.comments), 3)

    def test_stale_done_requires_new_exact_snapshot_review_then_repairs_done_to_done(self):
        client = FakeProjectionClient()
        original = snapshot(); material = closure_input()
        original_review = self.persist_review(client, original, material)
        first = reconcile_lifecycle(
            snapshot=original, adapter=self.adapter(client), github=github_truth(),
            shipyard=shipyard_truth(), closure_input=material,
            independent_review=original_review, created_at="2026-08-28T12:00:00Z",
        )

        changed = snapshot()
        changed["root"]["next_action"] = "review newly discovered exact state"
        changed["lifecycle"] = first["lifecycle"]
        changed["projection_events"] = list(self.adapter(client).state().events)
        changed["projection_revision"] = 3
        changed["closure_reviews"] = [original_review]
        with self.assertRaisesRegex(ReconcileError, "done_lifecycle_cannot"):
            reconcile_lifecycle(
                snapshot=changed, adapter=self.adapter(client), github=github_truth(),
                shipyard=shipyard_truth(), closure_input=material,
                independent_review=None, created_at="2026-08-28T12:01:00Z",
            )
        self.assertEqual(len(client.comments), 3)

        new_review = self.persist_review(client, changed, material)
        repaired = reconcile_lifecycle(
            snapshot=changed, adapter=self.adapter(client), github=github_truth(),
            shipyard=shipyard_truth(), closure_input=material,
            independent_review=new_review, created_at="2026-08-28T12:02:00Z",
        )
        self.assertEqual(repaired["status"], "Done")
        self.assertNotEqual(
            repaired["lifecycle"]["snapshot_sha256"], first["lifecycle"]["snapshot_sha256"],
        )
        self.assertEqual(len(client.comments), 5)
        self.assertEqual(
            self.adapter(client).state().snapshot["lifecycle"], repaired["lifecycle"],
        )

    def test_same_session_or_stale_review_refuses_before_write(self):
        for mutation in ("same_session", "stale_snapshot"):
            with self.subTest(mutation=mutation):
                client = FakeProjectionClient()
                snap = snapshot()
                material = closure_input()
                receipt = self.review_receipt(snap, material)
                if mutation == "same_session":
                    receipt["reviewer_session_id"] = "implementer-session"
                else:
                    receipt["snapshot_sha256"] = "0" * 64
                with self.assertRaises(ReconcileError):
                    reconcile_lifecycle(
                        snapshot=snap, adapter=self.adapter(client), github=github_truth(),
                        shipyard=shipyard_truth(), closure_input=material,
                        independent_review=receipt, created_at="2026-08-28T12:00:00Z",
                    )
                self.assertEqual(client.comments, [])

    def test_local_only_review_assertion_is_not_closure_authority(self):
        client = FakeProjectionClient()
        snap = snapshot(); material = closure_input()
        with self.assertRaisesRegex(ReconcileError, "independent_review_not_durable"):
            reconcile_lifecycle(
                snapshot=snap, adapter=self.adapter(client), github=github_truth(),
                shipyard=shipyard_truth(), closure_input=material,
                independent_review=self.review_receipt(snap, material),
                created_at="2026-08-28T12:00:00Z",
            )
        self.assertEqual(client.comments, [])

    def test_review_without_artifact_identity_or_digest_refuses(self):
        for missing in ("review_artifact_identity", "review_artifact_sha256"):
            with self.subTest(missing=missing):
                client = FakeProjectionClient()
                snap = snapshot(); material = closure_input()
                receipt = self.review_receipt(snap, material)
                receipt.pop(missing)
                with self.assertRaisesRegex(ReconcileError, "independent_review_schema_mismatch"):
                    reconcile_lifecycle(
                        snapshot=snap, adapter=self.adapter(client), github=github_truth(),
                        shipyard=shipyard_truth(), closure_input=material,
                        independent_review=receipt, created_at="2026-08-28T12:00:00Z",
                    )
                self.assertEqual(client.comments, [])

    def test_multiple_unordered_implementer_provenance_refuses(self):
        client = FakeProjectionClient()
        snap = snapshot(); material = closure_input()
        snap["provenance"].append({
            "agent": "claude", "machine": "M3", "session_id": "other-session",
        })
        receipt = self.review_receipt(snap, material)
        with self.assertRaisesRegex(
            ReconcileError, "implementer_session_ambiguous_or_unacknowledged",
        ):
            reconcile_lifecycle(
                snapshot=snap, adapter=self.adapter(client), github=github_truth(),
                shipyard=shipyard_truth(), closure_input=material,
                independent_review=receipt, created_at="2026-08-28T12:00:00Z",
            )
        self.assertEqual(client.comments, [])

    def test_stale_projection_or_provider_truth_refuses_with_zero_writes(self):
        client = FakeProjectionClient()
        snap = snapshot()
        stale = github_truth(); stale["pr_head"] = "c" * 40
        with self.assertRaisesRegex(ReconcileError, "github_truth_scope_mismatch"):
            reconcile_lifecycle(
                snapshot=snap, adapter=self.adapter(client), github=stale,
                shipyard=shipyard_truth(), closure_input=closure_input(),
                independent_review=None, created_at="2026-08-28T12:00:00Z",
            )
        self.assertEqual(client.comments, [])

    def test_late_v1_blocker_is_visible_and_refuses_lifecycle_until_reviewed(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN,
            **AUTHORITY,
        )
        values = [
            ("scope", "root", scope()),
            ("source", "root", {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }),
            ("provenance", "implementer-session", {
                "agent": "codex", "machine": "M5", "session_id": "implementer-session",
            }),
            ("disposition", "root", {
                "disposition": "attach", "remote_head": HEAD,
                "recovered_from_checkpoint": None,
            }),
        ]
        for revision, (kind, key, value) in enumerate(values):
            adapter.append(build_projection_event(
                workstream_id="GEN-37", kind=kind, key=key, value=value,
                plan_revision=PLAN, expected_revision=revision,
                created_at=f"2026-08-28T10:0{revision}:00Z", authority=AUTHORITY,
            ))
        graph = snapshot()
        graph["root"]["plan_revision"] = PLAN
        graph["root"]["next_action"] = "await exact landing review"
        graph["children"][0]["title"] = "Owned acceptance slice"
        graph["children"][0]["status"] = "Canceled"
        authenticated_source = {
            "sha256": PLAN, "identity": "https://example.test/plan",
        }
        dependency_adapter = LinearChildDependencyAdapter(
            client, workspace_id=AUTHORITY["workspace_id"],
            team_id=AUTHORITY["team_id"], project_id=AUTHORITY["project_id"],
            root_issue_id=AUTHORITY["root_issue_id"], root_identifier="GEN-37",
            plan_revision=PLAN,
        )

        def reconciled_snapshot():
            live = live_graph_with_empty_child_comments(graph)
            comments = [dict(item) for item in client.comments]
            return authenticated_reconcile_snapshot(
                live, comments, "GEN-37", authenticated_route=AUTHORITY,
                authenticated_source=authenticated_source,
                dependency_adapter=dependency_adapter,
                reread=lambda: (deepcopy(live), deepcopy(comments)),
            )

        before_lifecycle = reconciled_snapshot()
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="lifecycle", key="root",
            value={
                "status": "Landed — acceptance review required",
                "github": github_truth(), "shipyard_receipt": shipyard_truth(),
                "closure_input_sha256": canonical_digest(closure_input()),
                "snapshot_sha256": closure_snapshot_digest(before_lifecycle),
                "independent_review": None, "closure_receipt_sha256": None,
            },
            plan_revision=PLAN, expected_revision=4,
            created_at="2026-08-28T10:04:00Z", authority=AUTHORITY,
        ))
        blocker = legacy_event(
            "relation", "blocks:GEN-99",
            {"type": "blocks", "target": {
                "workspace_id": "workspace", "issue_id": "late-blocker-uuid",
                "identifier": "GEN-99",
            }},
            4, "2026-08-28T10:05:00Z",
        )
        client.comments.append(legacy_comment(blocker, "late-v1-blocker-comment"))
        graph["root"]["next_action"] = "review quarantined legacy blocker"
        resumed = reconciled_snapshot()
        status = compact_context(resumed, "GEN-37", require_projection_authority=True)
        self.assertEqual(
            status["lifecycle_recovery"]["state"], "blocked_unresolved_quarantine",
        )
        self.assertEqual(status["projection_quarantine"]["count"], 1)
        self.assertRegex(status["projection_quarantine"]["sha256"], r"^[0-9a-f]{64}$")
        audit = compact_context(
            resumed, "GEN-37", require_projection_authority=True, include_history=True,
        )
        self.assertEqual(
            [event["event_id"] for event in audit["projection_unresolved_quarantine"]],
            [blocker["event_id"]],
        )
        without_quarantine = json.loads(json.dumps(resumed))
        without_quarantine["projection_quarantined"] = []
        without_quarantine["projection_unresolved_quarantine"] = []
        self.assertNotEqual(
            closure_snapshot_digest(resumed), closure_snapshot_digest(without_quarantine),
        )
        writes_before = len(client.comments)
        with self.assertRaisesRegex(ReconcileError, "quarantine_review_required"):
            reconcile_lifecycle(
                snapshot=resumed, adapter=adapter, github=github_truth(),
                shipyard=shipyard_truth(), closure_input=closure_input(),
                independent_review=None, created_at="2026-08-28T10:06:00Z",
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_changed_exact_snapshot_fence_refuses_before_write(self):
        client = FakeProjectionClient()
        snap = snapshot()
        changed = snapshot(); changed["children"][0]["status"] = "In Progress"
        with self.assertRaisesRegex(ReconcileError, "closure_snapshot_changed_reload_required"):
            reconcile_lifecycle(
                snapshot=snap, adapter=self.adapter(client), github=github_truth(),
                shipyard=shipyard_truth(), closure_input=closure_input(),
                independent_review=None, created_at="2026-08-28T12:00:00Z",
                snapshot_fence=lambda: changed,
            )
        self.assertEqual(client.comments, [])

    def test_snapshot_change_after_append_is_not_reported_as_success(self):
        client = FakeProjectionClient()
        snap = snapshot()
        changed = snapshot(); changed["children"][0]["status"] = "In Progress"
        reads = iter((snap, changed))
        with self.assertRaisesRegex(ReconcileError, "closure_snapshot_changed_after_append"):
            reconcile_lifecycle(
                snapshot=snap, adapter=self.adapter(client), github=github_truth(),
                shipyard=shipyard_truth(), closure_input=closure_input(),
                independent_review=None, created_at="2026-08-28T12:00:00Z",
                snapshot_fence=lambda: next(reads),
            )
        self.assertEqual(len(client.comments), 1)
        graph = {
            **changed,
            "root": {**changed["root"], "next_action": "review changed child"},
        }
        with self.assertRaisesRegex(ResumeError, "lifecycle_snapshot_stale"):
            add_material_history(graph, client.comments, "GEN-37")
        recoverable = add_material_history(
            graph, client.comments, "GEN-37",
            permit_stale_lifecycle_for_reconcile=True,
        )
        self.assertEqual(recoverable["lifecycle_recovery"]["state"], "stale_snapshot")
        self.assertEqual(recoverable["root"]["status"], "In Progress")

    def test_closure_input_cannot_omit_scoped_child(self):
        client = FakeProjectionClient()
        material = closure_input(); material["required_child_ids"] = []
        with self.assertRaisesRegex(ReconcileError, "closure_input_scope_mismatch"):
            reconcile_lifecycle(
                snapshot=snapshot(), adapter=self.adapter(client), github=github_truth(),
                shipyard=shipyard_truth(), closure_input=material,
                independent_review=None, created_at="2026-08-28T12:00:00Z",
            )
        self.assertEqual(client.comments, [])

    def test_resume_derives_status_from_lifecycle_projection(self):
        client = FakeProjectionClient()
        graph = {
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37",
                     "plan_revision": "sha", "revision": 0,
                     "status": "In Progress", "next_action": "old"},
            "children": [], "decisions": [], "provenance": [],
        }
        before = add_material_history(graph, [], "GEN-37")
        lifecycle = {
            "status": "Landed — acceptance review required",
            "github": github_truth(), "shipyard_receipt": shipyard_truth(),
            "closure_input_sha256": canonical_digest(closure_input()),
            "snapshot_sha256": closure_snapshot_digest(before),
            "independent_review": None, "closure_receipt_sha256": None,
        }
        self.adapter(client).append(build_projection_event(
            workstream_id="GEN-37", kind="lifecycle", key="root", value=lifecycle,
            plan_revision="sha", expected_revision=0,
            created_at="2026-08-28T12:00:00Z",
            authority=self.adapter(client).authority,
        ))
        resumed = add_material_history(graph, client.comments, "GEN-37")
        self.assertEqual(resumed["root"]["status"], "Landed — acceptance review required")
        self.assertIsNone(resumed["root"]["closure_receipt"])

    def test_github_reader_rejects_head_drift(self):
        payload = {
            "number": 41, "merged": True, "merged_at": "now", "merge_commit_sha": MERGE,
            "head": {"sha": "c" * 40},
            "base": {"repo": {"id": 123, "node_id": "R_pulp",
                                "full_name": "Generous-Corp/pulp"}},
        }
        reader = GitHubTruthReader("token", opener=lambda *_args, **_kwargs: Response(payload))
        with self.assertRaisesRegex(ReconcileError, "github_head_drift"):
            reader.read(repository="Generous-Corp/pulp", provider_repository_id="R_pulp",
                        pr_number=41, expected_head=HEAD)

    def test_github_token_cannot_be_sent_to_cli_selected_authority(self):
        with self.assertRaisesRegex(ReconcileError, "github_api_authority"):
            GitHubTruthReader("token", api_base="https://attacker.example")

    def test_fixed_argv_shipyard_reader_and_timeout(self):
        receipt = shipyard_truth()
        command = [sys.executable, "-c", f"import json; print(json.dumps({receipt!r}))"]
        observed = ShipyardTruthReader(command, timeout=2).read(
            repository="Generous-Corp/pulp", repository_key_value=KEY,
            pr_number=41, expected_head=HEAD,
        )
        self.assertEqual(observed["receipt_id"], "shipyard-receipt-41")
        with self.assertRaisesRegex(ReconcileError, "shipyard_truth_timeout"):
            ShipyardTruthReader(
                [sys.executable, "-c", "import time; time.sleep(2)"], timeout=0.05,
            ).read(repository="Generous-Corp/pulp", repository_key_value=KEY,
                   pr_number=41, expected_head=HEAD)

    def test_repeatable_repository_groups_and_aggregate_shipyard_receipt(self):
        raw_bindings = [
            json.dumps({"repository": "Generous-Corp/pulp", "repository_id": "R_pulp",
                        "pr": 41, "expected_head": HEAD}),
            json.dumps({"repository": "Generous-Corp/vellum", "repository_id": "R_vellum",
                        "pr": 52, "expected_head": HEAD_2}),
        ]
        bindings = parse_repository_bindings(argparse.Namespace(
            repository=None, repository_id=None, pr=None, expected_head=None,
            repository_binding=raw_bindings,
        ))
        aggregate = {"schema_version": 2, "receipts": [shipyard_truth(), shipyard_truth_2()]}
        command = [sys.executable, "-c", f"import json; print(json.dumps({aggregate!r}))"]
        observed = ShipyardTruthReader(command, timeout=2).read_many(bindings)
        self.assertEqual([item["repository_key"] for item in observed], [KEY, KEY_2])

        missing = {"schema_version": 2, "receipts": [shipyard_truth()]}
        command = [sys.executable, "-c", f"import json; print(json.dumps({missing!r}))"]
        with self.assertRaisesRegex(ReconcileError, "aggregate_keyset_mismatch"):
            ShipyardTruthReader(command, timeout=2).read_many(bindings)

    def test_identical_pr_numbers_remain_repository_qualified(self):
        second_github = {**github_truth_2(), "pr_number": 41}
        second_receipt_value = {
            "schema_version": 1, "repository": "generous-corp/vellum",
            "repository_key": KEY_2, "pr_number": 41, "head": HEAD_2,
            "disposition": "merged", "receipt_id": "shipyard-receipt-vellum-41",
        }
        second_shipyard = {
            **second_receipt_value,
            "receipt_sha256": canonical_digest(second_receipt_value),
        }
        material = closure_input()
        material["required_child_ids"] = ["GEN-38", "GEN-39"]
        landed = reconcile_lifecycle(
            snapshot=multi_repository_snapshot(),
            adapter=self.adapter(FakeProjectionClient()),
            github=[github_truth(), second_github],
            shipyard=[shipyard_truth(), second_shipyard], closure_input=material,
            independent_review=None, created_at="2026-08-28T12:00:00Z",
        )
        repositories = landed["lifecycle"]["repositories"]
        self.assertEqual(
            [(item["repository_key"], item["github"]["pr_number"]) for item in repositories],
            [(KEY, 41), (KEY_2, 41)],
        )

    def test_relation_target_reader_binds_immutable_route_and_peer_edges(self):
        target_uuid = "22222222-2222-4222-8222-222222222222"
        target_authority = {
            "workspace_id": "workspace", "team_id": "team", "project_id": "project",
            "root_issue_id": target_uuid,
        }
        inverse = {"type": "blocked_by", "target": {
            "workspace_id": "workspace", "issue_id": ROOT_UUID,
            "identifier": "GEN-37",
        }}
        event = build_projection_event(
            workstream_id="GEN-50", kind="relation", key="blocked_by:GEN-37",
            value=inverse, plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-28T12:00:00Z", authority=target_authority,
        )
        comment = {
            "id": projection_slot_id("GEN-50", PLAN, 0, target_authority),
            "body": encode_projection_comment(event),
        }

        class Client:
            def execute(self, query, variables):
                if "WorkstreamRelationTarget" in query:
                    return {"issue": {
                        "id": target_uuid, "identifier": "GEN-50",
                        "description": f"Plan revision: {PLAN}",
                        "team": {"id": "team", "organization": {"id": "workspace"}},
                        "project": {"id": "project"},
                    }}
                if "WorkstreamDeltaComments" in query:
                    return {"issue": {
                        "id": target_uuid, "identifier": "GEN-50",
                        "team": {"id": "team", "organization": {"id": "workspace"}},
                        "project": {"id": "project"},
                        "comments": {"nodes": [comment], "pageInfo": {"hasNextPage": False}},
                    }}
                raise AssertionError((query, variables))

        target = {"workspace_id": "workspace", "issue_id": target_uuid,
                  "identifier": "GEN-50"}
        resolved = read_relation_targets(Client(), [{"type": "blocks", "target": target}])
        self.assertEqual(resolved[f"workspace:{target_uuid}"]["relations"], [inverse])

    def test_single_repository_flags_remain_compatibility_sugar(self):
        bindings = parse_repository_bindings(argparse.Namespace(
            repository="Generous-Corp/pulp", repository_id="R_pulp", pr=41,
            expected_head=HEAD, repository_binding=[],
        ))
        self.assertEqual(bindings, [{
            "repository": "Generous-Corp/pulp", "repository_id": "R_pulp",
            "pr": 41, "expected_head": HEAD,
        }])
        with self.assertRaisesRegex(ReconcileError, "conflicts_with_single"):
            parse_repository_bindings(argparse.Namespace(
                repository="Generous-Corp/pulp", repository_id=None, pr=None,
                expected_head=None, repository_binding=[json.dumps(bindings[0])],
            ))

    def test_shipyard_output_and_inherited_pipe_are_bounded(self):
        with self.assertRaisesRegex(ReconcileError, "shipyard_truth_too_large"):
            _bounded_command([
                sys.executable, "-c", "import sys; sys.stdout.write('x' * 1048577)",
            ], 2)
        started = time.monotonic()
        output = _bounded_command([
            sys.executable, "-c",
            "import subprocess,sys; subprocess.Popen([sys.executable,'-c','import time; time.sleep(10)']); print('{}')",
        ], 2)
        self.assertEqual(output.strip(), b"{}")
        self.assertLess(time.monotonic() - started, 1.5)

    def test_shipyard_adapter_cannot_inherit_provider_secrets_and_kills_descendant(self):
        secret_names = (
            "GITHUB_TOKEN", "SHIPYARD_GITHUB_TOKEN", "GITHUB_APP_PRIVATE_KEY",
            "LINEAR_API_KEY",
        )
        previous = {name: os.environ.get(name) for name in secret_names}
        for name in secret_names:
            os.environ[name] = "must-not-cross-adapter-boundary"
        try:
            output = _bounded_command([
                sys.executable, "-c",
                f"import os; print([os.environ.get(name) for name in {secret_names!r}])",
            ], 2)
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
        self.assertEqual(output.strip(), b"[None, None, None, None]")

        with tempfile.TemporaryDirectory() as directory:
            pid_path = Path(directory) / "descendant.pid"
            child = (
                "import os,time; "
                f"open({str(pid_path)!r},'w').write(str(os.getpid())); "
                "time.sleep(10)"
            )
            output = _bounded_command([
                sys.executable, "-c",
                "import subprocess,sys,time; "
                f"subprocess.Popen([sys.executable,'-c',{child!r}]); "
                "time.sleep(.1); print('{}')",
            ], 2)
            self.assertEqual(output.strip(), b"{}")
            descendant_pid = int(pid_path.read_text())
            deadline = time.monotonic() + 1
            while True:
                try:
                    os.kill(descendant_pid, 0)
                except ProcessLookupError:
                    break
                if time.monotonic() >= deadline:
                    self.fail(f"Shipyard adapter descendant survived: {descendant_pid}")
                time.sleep(0.01)

    def test_github_token_helper_is_bounded_and_refuses_malformed_output(self):
        self.assertEqual(github_token_from_command([
            sys.executable, "-c", "print('github-token-value')",
        ], timeout=2), "github-token-value")
        with self.assertRaisesRegex(ReconcileError, "github_auth_token_malformed"):
            github_token_from_command([
                sys.executable, "-c", "print('two tokens')",
            ], timeout=2)
        with self.assertRaisesRegex(ReconcileError, "github_auth_too_large"):
            github_token_from_command([
                sys.executable, "-c", "print('x' * 8193)",
            ], timeout=2)
        with self.assertRaisesRegex(ReconcileError, "github_auth_timeout"):
            github_token_from_command([
                sys.executable, "-c", "import time; time.sleep(2)",
            ], timeout=0.05)


if __name__ == "__main__":
    unittest.main()
