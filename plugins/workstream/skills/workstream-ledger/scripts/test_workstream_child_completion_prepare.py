#!/usr/bin/env python3

import unittest
from types import SimpleNamespace

from workstream_child_completion_prepare import (
    ChildCompletionPrepareError, prepare_child_completion,
)
from workstream_linear_projection import build_projection_event
from workstream_projection import projection_review_contract


PLAN = "a" * 64
HEAD = "b" * 40
ROOT_ID = "33333333-3333-4333-8333-333333333333"
CHILD_ID = "44444444-4444-4444-8444-444444444444"
ROUTE = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
    "root_issue_id": ROOT_ID,
}
SOURCE = {
    "identity": "https://github.com/example/plans/blob/main/PLAN.md",
    "sha256": PLAN,
}
REPOSITORY = {
    "slug": "github.com/example/repo", "provider": "github.com",
    "provider_repository_id": "R_repo", "aliases": [], "exact_head": HEAD,
    "identity_resolution": {
        "provider_repository_id": "R_repo",
        "resolved_slug": "github.com/example/repo",
        "observed_at": "2026-09-02T00:00:00Z",
        "evidence": [{
            "kind": "authenticated_provider_readback", "authenticated": True,
            "provider_repository_id": "R_repo",
            "resolved_slug": "github.com/example/repo",
        }],
    },
    "identity_updates": [], "evidence": [],
}
REPOSITORY_KEY = "github.com:id:R_repo"


def receipt(kind):
    return {
        "kind": kind, "passed": True, "proof": f"{kind} passed",
        "repository_key": REPOSITORY_KEY, "exact_head": HEAD,
    }


def evidence_contract():
    na = lambda reason: {"status": "not_applicable", "reason": reason}
    return {
        "slice_id": "child-completion", "owning_child": "GEN-92",
        "repository": "github.com/example/repo",
        "repository_key": REPOSITORY_KEY,
        "plan_revision": PLAN, "exact_head": HEAD,
        "layers": {
            "architecture": {
                "status": "required", "owned_seam": "completion",
                "trust_boundary": "Linear", "allowed_side_effects": [],
                "receipts": [receipt("review")],
            },
            "logic": {
                "status": "required", "methods": ["unit"],
                "receipts": [receipt("test")],
            },
            "component": na("Covered by logic"),
            "adapter": na("No adapter"),
            "e2e": na("No end-to-end seam"),
            "visual": na("No visual output"),
            "operational": na("No deployment"),
            "negative_control": {
                "status": "required", "failure_detected": True,
                "receipts": [receipt("negative")],
            },
        },
    }


def snapshot(*, terminal=False):
    return {
        "root": {
            "identifier": "GEN-91", "url": "https://linear.test/GEN-91",
            "plan_revision": PLAN, "revision": 0, "status": "In Progress",
            "next_action": "Complete child", "issue_revision": 0,
        },
        "children": [{
            "id": CHILD_ID, "identifier": "GEN-92", "status": (
                "Done" if terminal else "In Progress"
            ), "status_type": ("completed" if terminal else "started"),
            "state_id": ("done" if terminal else "started"),
            "parent": {"id": ROOT_ID, "identifier": "GEN-91"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project"}, "assignee": None,
            "next_action": "Finish",
        }],
        "scope": {
            "linear": dict(ROUTE), "primary_repository": REPOSITORY_KEY,
            "repositories": [dict(REPOSITORY)],
            "child_ownership": {"GEN-92": REPOSITORY_KEY},
        },
        "source": dict(SOURCE), "provenance": [],
        "disposition": {
            "disposition": "attach", "remote_head": HEAD,
            "recovered_from_checkpoint": None,
        },
        "relations": [], "relation_targets": {},
        "dependency_graph": {"relations": [], "grants": [], "sha256": "0" * 64},
        "material_events": [], "raw_material_events": [],
        "material_semantic_repairs": [], "material_event_revision": 0,
        "latest_checkpoint": None,
        "checkpoint_recovery": {"state": "not_found", "stale_plan_count": 0},
        "authenticated_route": dict(ROUTE),
        "authenticated_source": dict(SOURCE),
        "projection_revision": 3,
        "projection_recovery": {"state": "current"},
        "projection_quarantine": {"count": 0, "sha256": "0" * 64},
        "projection_unresolved_quarantine": [],
        "child_closures": [], "evidence_contracts": [],
        "choices": [], "choice_events": [],
        "pending_child_proposals": [],
    }


def state(*, include_evidence=False, include_closure=False,
          source_value=None, scope_value=None):
    authority = dict(ROUTE)
    scope_value = scope_value or snapshot()["scope"]
    source_value = source_value or SOURCE
    values = [
        ("scope", "root", scope_value),
        ("source", "root", source_value),
        ("provenance", "root", {"agent": "codex", "machine": "M5", "session_id": "s"}),
        ("disposition", "root", snapshot()["disposition"]),
    ]
    if include_evidence:
        values.append(("evidence_contract", "child-completion", evidence_contract()))
    if include_closure:
        prepared = prepare_child_completion(
            snapshot(terminal=True), state(include_evidence=True),
            root_token="GEN-91", child_token="GEN-92",
            evidence_contract=evidence_contract(), authenticated_source=SOURCE,
            authenticated_route=ROUTE,
        )
        closure = next(item["value"] for item in
                       prepared["projection_manifest"]["projection"]
                       if item["kind"] == "child_closure")
        values.append(("child_closure", "GEN-92", closure))
    events = []
    for revision, (kind, key, value) in enumerate(values):
        events.append(build_projection_event(
            workstream_id="GEN-91", kind=kind, key=key, value=value,
            plan_revision=PLAN, expected_revision=revision,
            created_at=f"2026-09-02T00:00:0{revision}Z", authority=authority,
        ))
    return SimpleNamespace(
        events=events, revision=len(events),
        snapshot={"projection_quarantined": []},
    )


class ChildCompletionPrepareTests(unittest.TestCase):
    def call(self, snap, projection):
        return prepare_child_completion(
            snap, projection, root_token="GEN-91", child_token="GEN-92",
            evidence_contract=evidence_contract(),
            authenticated_source=SOURCE, authenticated_route=ROUTE,
        )

    def test_emits_evidence_only_when_contract_is_not_active(self):
        result = self.call(snapshot(), state())
        self.assertEqual(result["operation_status"], "evidence_projection_required")
        manifest = result["projection_manifest"]
        self.assertNotIn("terminal_child_repairs", manifest)
        evidence = [
            item for item in manifest["projection"]
            if item["kind"] == "evidence_contract"
        ]
        self.assertEqual(evidence[0]["value"], evidence_contract())
        for key, value in projection_review_contract(state()).items():
            self.assertEqual(manifest[key], value)

    def test_reports_native_transition_when_evidence_active_but_child_open(self):
        result = self.call(snapshot(), state(include_evidence=True))
        self.assertEqual(result["operation_status"], "native_transition_required")
        self.assertNotIn("terminal_child_repairs", result["projection_manifest"])

    def test_emits_exact_repair_when_evidence_active_and_child_completed(self):
        result = self.call(
            snapshot(terminal=True), state(include_evidence=True),
        )
        self.assertEqual(result["operation_status"], "closure_projection_required")
        repair = result["projection_manifest"]["terminal_child_repairs"][0]
        self.assertEqual(repair["child_identifier"], "GEN-92")
        self.assertEqual(repair["child_issue_id"], CHILD_ID)
        self.assertIsNone(repair["expected_assignee_id"])
        closures = [
            item for item in result["projection_manifest"]["projection"]
            if item["kind"] == "child_closure"
        ]
        self.assertEqual(closures[0]["key"], "GEN-92")

    def test_active_exact_closure_is_complete_control_state(self):
        result = self.call(
            snapshot(terminal=True),
            state(include_evidence=True, include_closure=True),
        )
        self.assertEqual(result["operation_status"], "complete")
        self.assertNotIn("terminal_child_repairs", result["projection_manifest"])

    def test_refuses_wrong_owner(self):
        contract = evidence_contract()
        contract["repository_key"] = "github.com:id:R_other"
        for layer in contract["layers"].values():
            for item in layer.get("receipts", []):
                item["repository_key"] = "github.com:id:R_other"
        with self.assertRaisesRegex(
            ChildCompletionPrepareError, "evidence_contract_owner_mismatch",
        ):
            prepare_child_completion(
                snapshot(), state(), root_token="GEN-91",
                child_token="GEN-92", evidence_contract=contract,
                authenticated_source=SOURCE, authenticated_route=ROUTE,
            )

    def test_refuses_wrong_parent(self):
        value = snapshot()
        value["children"][0]["parent"]["id"] = "wrong"
        with self.assertRaisesRegex(
            ChildCompletionPrepareError, "child_native_route_mismatch",
        ):
            self.call(value, state())

    def test_refuses_inactive_generation_source(self):
        other_source = dict(SOURCE, sha256="c" * 64)
        with self.assertRaisesRegex(
            ChildCompletionPrepareError, "active_projection_source_mismatch",
        ):
            self.call(snapshot(), state(source_value=other_source))

    def test_refuses_active_scope_route_mismatch(self):
        other_scope = snapshot()["scope"]
        other_scope["linear"] = dict(ROUTE, project_id="other-project")
        with self.assertRaisesRegex(
            ChildCompletionPrepareError, "active_projection_route_mismatch",
        ):
            self.call(snapshot(), state(scope_value=other_scope))


if __name__ == "__main__":
    unittest.main()
