#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from workstream_checkpoint import build_checkpoint
from workstream_choices import record_choice
from workstream_delta import Delta
from workstream_linear_checkpoints import encode_checkpoint_comment
from workstream_linear import bootstrap_linear_route, LinearGraphQLTransport
from workstream_linear_events import (
    encode_event_comment, LinearCommentEventAdapter,
)
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, LinearProjectionAdapter,
    LinearProjectionError, reduce_projection_comments, TOMBSTONE,
)
from workstream_resume import add_material_history, compact_context, ResumeError
import workstream_projection
from workstream_projection import (
    projection_review_contract, reconcile_required_projection, stable_live_readback,
)
from workstream_successor import choose_disposition


PLAN = "f38baae4441485b14e5b16ea0255e3a07e42aa94a4fb0e6e04e7aa513693719d"
HEAD = "a" * 40
ROOT_UUID = "33333333-3333-4333-8333-333333333333"


def reviewed_manifest(adapter, projection, retirements=None):
    return {
        **projection_review_contract(adapter.state()),
        "projection": projection,
        "retirements": list(retirements or []),
    }


def reviewed_retirement(adapter, kind, key):
    state = adapter.state()
    event = next(
        item for item in reversed(state.events)
        if item["kind"] == kind and item["key"] == key
        and item["value"] != {"_projection_tombstone": True}
    )
    return {
        "kind": kind,
        "key": key,
        "expected_event_id": event["event_id"],
        "expected_value_sha256": workstream_projection._value_digest(event["value"]),
    }


class FakeProjectionClient:
    def __init__(self):
        self.comments: list[dict] = []
        self.calls: list[tuple[str, dict]] = []

    def execute(self, query, variables):
        self.calls.append((query, variables))
        if "query WorkstreamDeltaComments" in query:
            return {"issue": {
                "id": ROOT_UUID, "identifier": "GEN-37",
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                "comments": {"nodes": [dict(item) for item in self.comments],
                             "pageInfo": {"hasNextPage": False, "endCursor": None}},
            }}
        if "commentCreate" in query:
            comment = {"id": f"comment-{len(self.comments) + 1}",
                       "body": variables["input"]["body"],
                       "createdAt": "now", "updatedAt": "now"}
            self.comments.append(comment)
            return {"commentCreate": {"success": True, "comment": dict(comment)}}
        raise AssertionError("unexpected GraphQL operation")


class PaginatedLiveLikeClient:
    def __init__(self, comments):
        self.comments = comments
        self.issue_afters = []
        self.comment_afters = []

    def execute(self, query, variables):
        if "query WorkstreamTokenRoute" in query:
            return {"issue": {
                "id": ROOT_UUID, "identifier": "GEN-37",
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project", "teams": {"nodes": [{"id": "team"}]}},
            }}
        if "query WorkstreamRoute" in query:
            return {
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project", "teams": {"nodes": [{"id": "team"}]}},
            }
        if "query WorkstreamIssues" in query:
            after = variables["after"]
            self.issue_afters.append(after)
            if after is None:
                return {"team": {"issues": {
                    "nodes": [{
                        "id": ROOT_UUID, "identifier": "GEN-37", "title": "Continuity",
                        "description": f"Plan revision: {PLAN}\nLedger revision: 3\nCurrent next action: Resume.",
                        "url": "https://linear.app/acme/issue/GEN-37/root",
                        "updatedAt": "now", "parent": None, "project": {"id": "project"},
                        "state": {"name": "In Progress", "type": "started"},
                    }],
                    "pageInfo": {"hasNextPage": True, "endCursor": "issues-2"},
                }}}
            return {"team": {"issues": {
                "nodes": [{
                    "id": "child-38", "identifier": "GEN-38", "title": "Resume transport",
                    "description": "Current next action: Run live canary.",
                    "url": "https://linear.app/acme/issue/GEN-38/child", "updatedAt": "now",
                    "parent": {"id": ROOT_UUID, "identifier": "GEN-37"},
                    "project": {"id": "project"},
                    "state": {"name": "In Progress", "type": "started"},
                }],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }}}
        if "query WorkstreamDeltaComments" in query:
            after = variables["after"]
            self.comment_afters.append(after)
            nodes = self.comments[:1] if after is None else self.comments[1:]
            return {"issue": {
                "id": ROOT_UUID, "identifier": "GEN-37",
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                "comments": {"nodes": nodes, "pageInfo": {
                    "hasNextPage": after is None, "endCursor": "comments-2" if after is None else None,
                }},
            }}
        raise AssertionError("unexpected GraphQL operation")


def scope() -> dict:
    repository_key = "github.com:id:R_agent_workstream"
    return {
        "namespace": "agent-workstream-continuity",
        "linear": {
            "workspace_id": "workspace", "team_id": "team",
            "project_id": "project", "root_issue_id": ROOT_UUID,
            "route_verification": {
                "workspace_id": "workspace", "team_id": "team",
                "project_id": "project", "root_issue_id": ROOT_UUID,
                "observed_at": "2026-08-27T12:00:00Z",
                "evidence": [{
                    "kind": "authenticated_linear_readback", "authenticated": True,
                    "workspace_id": "workspace", "team_id": "team",
                    "project_id": "project", "root_issue_id": ROOT_UUID,
                }],
            },
        },
        "primary_repository": repository_key,
        "repositories": [{
            "slug": "github.com/generous-corp/agent-workstream",
            "provider_repository_id": "R_agent_workstream", "aliases": [],
            "exact_head": HEAD,
            "identity_resolution": {
                "provider_repository_id": "R_agent_workstream",
                "resolved_slug": "github.com/generous-corp/agent-workstream",
                "observed_at": "2026-08-27T12:00:00Z",
                "evidence": [{
                    "kind": "authenticated_provider_readback", "authenticated": True,
                    "provider_repository_id": "R_agent_workstream",
                    "resolved_slug": "github.com/generous-corp/agent-workstream",
                }],
            },
            "identity_updates": [], "evidence": [],
        }],
        "child_ownership": {"GEN-38": repository_key},
    }


def evidence_contract() -> dict:
    not_applicable = lambda reason: {"status": "not_applicable", "reason": reason}
    receipt = lambda kind: {
        "kind": kind, "passed": True,
        "repository_key": "github.com:id:R_agent_workstream",
        "exact_head": HEAD, "proof": f"{kind} passed",
    }
    return {
        "slice_id": "gen37-resume", "owning_child": "GEN-38",
        "repository": "github.com/generous-corp/agent-workstream",
        "repository_key": "github.com:id:R_agent_workstream",
        "plan_revision": PLAN, "exact_head": HEAD,
        "layers": {
            "architecture": {"status": "required", "owned_seam": "Linear projection",
                             "trust_boundary": "Linear comments to resume reducer",
                             "allowed_side_effects": ["append Linear comment"],
                             "receipts": [{**receipt("review"), "status": "accepted"}]},
            "logic": {"status": "required", "methods": ["unit"],
                      "receipts": [receipt("test")]},
            "component": {"status": "required", "uses_fakes": True,
                          "fake_scope": "external_edge_only", "receipts": [receipt("test")]},
            "adapter": {"status": "required", "mode": "contract_fake",
                        "receipts": [receipt("test")]},
            "e2e": not_applicable("Bounded live canary is a separate physical gate"),
            "visual": not_applicable("No visual output"),
            "operational": not_applicable("No deployment in this slice"),
            "negative_control": {"status": "required", "failure_detected": True,
                                 "receipts": [receipt("planted-conflict")]},
        },
    }


class ProjectionTests(unittest.TestCase):
    def event(self, kind, key, value, revision, supersedes=None):
        return build_projection_event(
            workstream_id="GEN-37", kind=kind, key=key, value=value,
            plan_revision=PLAN, expected_revision=revision,
            created_at=f"2026-08-27T12:{revision:02d}:00Z",
            supersedes_event_id=supersedes,
        )

    def test_live_like_gen37_projection_round_trips_into_token_resume(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=ROOT_UUID,
        )
        choice = record_choice(
            choice_id="choice-comment-authority", workstream_id="GEN-37",
            owning_child="GEN-38", namespace="agent-workstream-continuity",
            repository="github.com/generous-corp/agent-workstream",
            repository_key="github.com:id:R_agent_workstream",
            plan_revision=PLAN, git_head=HEAD,
            created_at="2026-08-27T12:00:00Z",
            spec_gap="Resume projection storage was unspecified",
            decision="Use immutable Linear comments", alternatives=["issue prose"],
            reach="local", irreversible=False, domains=[],
            technical_confidence="high", intent_confidence="high",
        )
        checkpoint = build_checkpoint(
            workstream_id="GEN-37", boundary_id="projected", root_revision=1,
            plan_revision=PLAN, before_status="In Progress", after_status="In Progress",
            execution={"agent": "codex", "provider": "openai", "session_id": "session-m5",
                       "machine": "M5", "worktree": {"state": "safe", "path": "/worktree",
                                                       "branch": "feature/gen37", "head": HEAD}},
            exact_head=HEAD, evidence=[], blocker=None, next_action="resume GEN-37",
        )
        values = [
            ("scope", "root", scope()),
            ("relation", "blocks:GEN-14", {"type": "blocks", "target": {
                "workspace_id": "workspace", "issue_id": "44444444-4444-4444-8444-444444444444",
                "identifier": "GEN-14"}}),
            ("choice", choice["event_id"], choice),
            ("evidence_contract", "gen37-resume", evidence_contract()),
            ("source", "root", {"kind": "markdown", "sha256": PLAN,
                                "url": "https://github.com/danielraffel/pulp-planning/blob/main/2026-08-20-workstream-continuity-consolidated-plan.md"}),
            ("provenance", "session-m5", {"agent": "codex", "machine": "M5",
                                           "session_id": "session-m5"}),
            ("disposition", "root", {"disposition": "attach",
                                      "remote_head": HEAD,
                                      "recovered_from_checkpoint": checkpoint["event_id"]}),
        ]
        receipts = [adapter.append(self.event(kind, key, value, index))
                    for index, (kind, key, value) in enumerate(values)]
        self.assertEqual(receipts[-1]["revision"], len(values))

        client.comments.append({"id": "delta-1", "body": encode_event_comment(Delta(
            "delta-1", "GEN-37", "progress", "agent", {"next_action": "resume GEN-37"},
            0, "2026-08-27T13:00:00Z"))})
        client.comments.append({"id": "checkpoint-1", "body": encode_checkpoint_comment(checkpoint)})
        snapshot = {
            "root": {"identifier": "GEN-37", "url": "https://linear.app/acme/issue/GEN-37/root",
                     "plan_revision": PLAN, "revision": 9, "status": "In Progress",
                     "next_action": "stale"},
            "children": [{"identifier": "GEN-38", "title": "Resume transport",
                          "status": "In Progress", "next_action": "finish canary"}],
            "decisions": [], "provenance": [],
        }
        context = compact_context(
            add_material_history(
                snapshot, client.comments, "GEN-37",
                authenticated_route={"workspace_id": "workspace", "team_id": "team",
                                     "project_id": "project", "root_issue_id": ROOT_UUID},
                authenticated_source={
                    "identity": "https://github.com/danielraffel/pulp-planning/blob/main/2026-08-20-workstream-continuity-consolidated-plan.md",
                    "sha256": PLAN,
                },
            ), "Resume GEN-37", require_projection_authority=True,
        )
        self.assertEqual(context["scope"]["namespace"], "agent-workstream-continuity")
        self.assertEqual(context["relations"][0]["target"]["identifier"], "GEN-14")
        self.assertEqual(context["choice_events"][0]["choice_id"], "choice-comment-authority")
        self.assertEqual(context["evidence_contracts"][0]["owning_child"], "GEN-38")
        self.assertEqual(context["source"]["sha256"], PLAN)
        self.assertEqual(context["provenance"][0]["machine"], "M5")
        self.assertEqual(context["projection_revision"], 7)
        self.assertEqual(context["resume_authority"], "full")
        disposition = choose_disposition(context, remote_head=HEAD)
        self.assertEqual(disposition["disposition"], "attach")
        self.assertEqual(disposition["recovered_from_checkpoint"], checkpoint["event_id"])
        self.assertFalse(disposition["durable_projection_required"])
        self.assertEqual(disposition["durable_disposition"], context["disposition"])

    def test_token_bootstrap_routed_graph_and_paginated_projection_end_to_end(self):
        projection_events = [
            self.event("scope", "root", scope(), 0),
            self.event("source", "root", {"sha256": PLAN,
                                           "identity": "https://example.test/plan"}, 1),
            self.event("provenance", "session-m5", {
                "agent": "codex", "machine": "M5", "session_id": "session-m5",
            }, 2),
            self.event("disposition", "root", {
                "disposition": "create_successor", "remote_head": HEAD,
                "recovered_from_checkpoint": None,
            }, 3),
        ]
        client = PaginatedLiveLikeClient([
            {"id": f"projection-{index}", "body": encode_projection_comment(event),
             "createdAt": "now", "updatedAt": "now"}
            for index, event in enumerate(projection_events)
        ])
        route = bootstrap_linear_route(client, "GEN-37")
        graph = LinearGraphQLTransport(
            client, team_id=route["team_id"], workspace_id=route["workspace_id"],
            project_id=route["project_id"],
        ).snapshot_for_root("GEN-37")
        comments = LinearCommentEventAdapter(
            client, issue_id="GEN-37", workspace_id=route["workspace_id"],
            team_id=route["team_id"], project_id=route["project_id"],
        ).comments()
        enriched = add_material_history(
            graph, comments, "GEN-37", authenticated_route=route,
        )
        context = compact_context(enriched, "GEN-37")
        self.assertEqual(context["workstream_id"], "GEN-37")
        self.assertEqual(context["scope"]["linear"]["project_id"], "project")
        self.assertEqual([child["identifier"] for child in context["children"]], ["GEN-38"])
        self.assertEqual(client.issue_afters, [None, "issues-2"])
        self.assertEqual(client.comment_afters, [None, "comments-2"])
        stale = build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="stale",
            value={"agent": "codex", "machine": "M3", "session_id": "stale"},
            plan_revision="b" * 64, expected_revision=enriched["projection_revision"],
            created_at="2026-08-27T15:00:00Z",
        )
        enriched["projection_events"].append(stale)
        enriched["projection_revision"] += 1
        with self.assertRaisesRegex(ResumeError, "projection_plan_drift"):
            compact_context(enriched, "GEN-37")

    def test_replay_is_zero_write_and_unfenced_replacement_fails_closed(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN
        )
        first = self.event("source", "root", {"sha256": PLAN, "identity": "https://example.test/plan"}, 0)
        adapter.append(first)
        adapter.append(first)
        self.assertEqual(len(client.comments), 1)
        conflicting = self.event("source", "root", {"sha256": "b" * 64,
                                                      "identity": "https://example.test/plan"}, 1)
        with self.assertRaisesRegex(LinearProjectionError, "projection_concurrent_conflict"):
            adapter.append(conflicting)
        self.assertEqual(len(client.comments), 1)

    def test_explicit_supersession_preserves_history_and_derives_current_source(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN
        )
        first = self.event("source", "root", {"sha256": PLAN,
                                               "identity": "https://example.test/old"}, 0)
        adapter.append(first)
        second = self.event(
            "source", "root", {"sha256": PLAN,
                                 "identity": "https://example.test/current"}, 1,
            supersedes=first["event_id"],
        )
        adapter.append(second)
        state = adapter.state()
        self.assertEqual(len(state.events), 2)
        self.assertEqual(state.snapshot["source"]["identity"], "https://example.test/current")

    def test_stale_only_generation_is_retained_but_not_current(self):
        event = self.event("source", "root", {"sha256": PLAN,
                                               "identity": "https://example.test/plan"}, 0)
        from workstream_linear_projection import encode_projection_comment
        reduced = reduce_projection_comments(
            [{"id": "one", "body": encode_projection_comment(event)}],
            workstream_id="GEN-37", expected_plan_revision="b" * 64,
        )
        self.assertEqual(reduced.revision, 0)
        self.assertEqual(reduced.snapshot["projection_recovery"], {
            "state": "stale_plan", "stale_plan_count": 1,
        })
        self.assertEqual(reduced.snapshot["projection_history"], [event])

    def test_stale_generation_cannot_poison_or_supersede_current(self):
        from workstream_linear_projection import encode_projection_comment
        current = self.event("source", "root", {"sha256": PLAN,
                                                 "identity": "https://example.test/plan"}, 0)
        stale = build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="old-session",
            value={"agent": "codex", "machine": "M3", "session_id": "old"},
            plan_revision="b" * 64, expected_revision=0,
            created_at="2026-08-27T13:00:00Z",
            supersedes_event_id=current["event_id"],
        )
        comments = [
            {"id": "current", "body": encode_projection_comment(current)},
            {"id": "stale", "body": encode_projection_comment(stale)},
        ]
        reduced = reduce_projection_comments(
            comments, workstream_id="GEN-37", expected_plan_revision=PLAN,
        )
        self.assertEqual(reduced.snapshot["source"]["sha256"], PLAN)
        self.assertEqual(reduced.snapshot["projection_recovery"], {
            "state": "current", "stale_plan_count": 1,
        })

    def test_new_plan_generation_starts_at_zero_and_mixed_current_conflicts(self):
        old_plan = "b" * 64
        old = build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"sha256": old_plan, "identity": "https://example.test/old"},
            plan_revision=old_plan, expected_revision=0,
            created_at="2026-08-27T10:00:00Z",
        )
        current = self.event(
            "source", "root", {"sha256": PLAN, "identity": "https://example.test/current"}, 0,
        )
        comments = [
            {"id": "old", "body": encode_projection_comment(old)},
            {"id": "current", "body": encode_projection_comment(current)},
        ]
        reduced = reduce_projection_comments(
            comments, workstream_id="GEN-37", expected_plan_revision=PLAN,
        )
        self.assertEqual(reduced.revision, 1)
        self.assertEqual(reduced.snapshot["source"]["identity"], "https://example.test/current")
        conflict = self.event(
            "source", "root", {"sha256": PLAN, "identity": "https://example.test/other"}, 0,
        )
        comments.append({"id": "conflict", "body": encode_projection_comment(conflict)})
        with self.assertRaisesRegex(LinearProjectionError, "projection_concurrent_conflict"):
            reduce_projection_comments(
                comments, workstream_id="GEN-37", expected_plan_revision=PLAN,
            )

    def test_multiple_evidence_slices_for_one_child_remain_distinct(self):
        first = evidence_contract()
        second = evidence_contract()
        second["slice_id"] = "gen37-route"
        events = [
            self.event("evidence_contract", first["slice_id"], first, 0),
            self.event("evidence_contract", second["slice_id"], second, 1),
        ]
        from workstream_linear_projection import encode_projection_comment
        reduced = reduce_projection_comments(
            [{"id": str(index), "body": encode_projection_comment(event)}
             for index, event in enumerate(events)],
            workstream_id="GEN-37", expected_plan_revision=PLAN,
        )
        self.assertEqual(
            {item["slice_id"] for item in reduced.snapshot["evidence_contracts"]},
            {"gen37-resume", "gen37-route"},
        )

    def test_source_digest_and_authenticated_route_mismatches_fail_closed(self):
        from workstream_linear_projection import encode_projection_comment
        wrong_source = build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"sha256": "b" * 64, "identity": "https://example.test/plan"},
            plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-27T14:00:00Z",
        )
        with self.assertRaisesRegex(LinearProjectionError, "source_plan_mismatch"):
            reduce_projection_comments(
                [{"id": "source", "body": encode_projection_comment(wrong_source)}],
                workstream_id="GEN-37", expected_plan_revision=PLAN,
            )
        scoped = self.event("scope", "root", scope(), 0)
        with self.assertRaisesRegex(LinearProjectionError, "route_mismatch:project_id"):
            reduce_projection_comments(
                [{"id": "scope", "body": encode_projection_comment(scoped)}],
                workstream_id="GEN-37", expected_plan_revision=PLAN,
                authenticated_route={"workspace_id": "workspace", "team_id": "team",
                                     "project_id": "wrong", "root_issue_id": ROOT_UUID},
            )

    def test_authenticated_root_and_exact_source_bytes_must_match(self):
        events = [
            self.event("scope", "root", scope(), 0),
            self.event("source", "root", {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }, 1),
        ]
        comments = [{"id": str(i), "body": encode_projection_comment(event)}
                    for i, event in enumerate(events)]
        route = {"workspace_id": "workspace", "team_id": "team",
                 "project_id": "project", "root_issue_id": ROOT_UUID}
        with self.assertRaisesRegex(LinearProjectionError, "root_issue_id"):
            reduce_projection_comments(
                comments, workstream_id="GEN-37", expected_plan_revision=PLAN,
                authenticated_route={**route, "root_issue_id": "wrong"},
            )
        with self.assertRaisesRegex(LinearProjectionError, "source_identity_mismatch"):
            reduce_projection_comments(
                comments, workstream_id="GEN-37", expected_plan_revision=PLAN,
                authenticated_route=route,
                authenticated_source={"identity": "https://example.test/other", "sha256": PLAN},
            )
        with self.assertRaisesRegex(LinearProjectionError, "source_bytes_mismatch"):
            reduce_projection_comments(
                comments, workstream_id="GEN-37", expected_plan_revision=PLAN,
                authenticated_route=route,
                authenticated_source={"identity": "https://example.test/plan", "sha256": "c" * 64},
            )

    def test_full_resume_refuses_absent_projection_authority(self):
        snapshot = {
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37",
                     "plan_revision": PLAN, "revision": 0, "status": "In Progress",
                     "next_action": "continue"},
            "children": [], "material_events": [], "material_event_revision": 0,
        }
        with self.assertRaisesRegex(ResumeError, "projection_authority_absent"):
            compact_context(snapshot, "GEN-37", require_projection_authority=True)
        inspected = compact_context(snapshot, "GEN-37")
        self.assertEqual(inspected["resume_authority"], "inspection_only")

    def test_product_reconcile_appends_disposition_reads_back_and_replays_zero_write(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=ROOT_UUID,
        )
        projection = [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }},
            {"kind": "provenance", "key": "session-m5", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session-m5",
                "worktree": {"state": "safe", "head": HEAD},
            }},
        ]
        manifest = reviewed_manifest(adapter, projection)
        snapshot = {
            "root": {"identifier": "GEN-37"},
            "latest_checkpoint": {
                "checkpoint_event_id": "wsc-live",
                "worktree": {"state": "safe", "head": HEAD},
            },
        }
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        first = reconcile_required_projection(
            adapter, snapshot, manifest, remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
        )
        self.assertEqual(first["disposition"]["disposition"], "attach")
        self.assertTrue(first["readback_verified"])
        self.assertEqual(len(first["writes"]), 4)
        manifest = reviewed_manifest(adapter, projection)
        second = reconcile_required_projection(
            adapter, snapshot, manifest, remote_head=HEAD,
            created_at="2026-08-27T18:01:00Z", authenticated_source=source,
        )
        self.assertEqual(second["writes"], [])
        self.assertEqual(len(client.comments), 4)

    def test_product_reconcile_durably_records_create_successor(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=ROOT_UUID,
        )
        manifest = reviewed_manifest(adapter, [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }},
            {"kind": "provenance", "key": "session-m3", "value": {
                "agent": "claude", "machine": "M3", "session_id": "session-m3",
                "worktree": {"state": "stale", "head": "b" * 40},
            }},
        ])
        result = reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}}, manifest,
            remote_head=HEAD, created_at="2026-08-27T18:00:00Z",
            authenticated_source={"identity": "https://example.test/plan", "sha256": PLAN},
        )
        self.assertEqual(result["disposition"], {
            "disposition": "create_successor", "remote_head": HEAD,
            "recovered_from_checkpoint": None,
        })
        self.assertEqual(adapter.state().snapshot["disposition"], result["disposition"])

    def test_product_reconcile_explicitly_retires_omitted_keyed_state(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=ROOT_UUID,
        )
        base = [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }},
        ]
        old_provenance = {"agent": "codex", "machine": "M5", "session_id": "old",
                          "worktree": {"state": "safe", "head": HEAD}}
        relation = {"type": "blocks", "target": {
            "workspace_id": "workspace", "issue_id": "target-uuid",
            "identifier": "GEN-14",
        }}
        first_manifest = reviewed_manifest(adapter, [*base,
            {"kind": "provenance", "key": "old", "value": old_provenance},
            {"kind": "relation", "key": "blocks:GEN-14", "value": relation},
            {"kind": "evidence_contract", "key": "gen37-resume",
             "value": evidence_contract()},
        ])
        snapshot = {"root": {"identifier": "GEN-37"}}
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        reconcile_required_projection(
            adapter, snapshot, first_manifest, remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
        )
        new_provenance = {"agent": "claude", "machine": "M3", "session_id": "new",
                          "worktree": {"state": "safe", "head": HEAD}}
        retirements = [
            reviewed_retirement(adapter, "relation", "blocks:GEN-14"),
            reviewed_retirement(adapter, "evidence_contract", "gen37-resume"),
            reviewed_retirement(adapter, "provenance", "old"),
        ]
        second_manifest = reviewed_manifest(adapter, [*base,
            {"kind": "provenance", "key": "new", "value": new_provenance},
        ], retirements)
        result = reconcile_required_projection(
            adapter, snapshot, second_manifest, remote_head=HEAD,
            created_at="2026-08-27T19:00:00Z", authenticated_source=source,
        )
        state = adapter.state().snapshot
        self.assertEqual(state["relations"], [])
        self.assertEqual(state["evidence_contracts"], [])
        self.assertEqual(state["provenance"], [new_provenance])
        tombstones = [event for event in state["projection_events"]
                      if event["value"] == {"_projection_tombstone": True}]
        self.assertEqual({(event["kind"], event["key"]) for event in tombstones}, {
            ("relation", "blocks:GEN-14"),
            ("evidence_contract", "gen37-resume"),
            ("provenance", "old"),
        })
        self.assertTrue(result["readback_verified"])

    def test_product_reconcile_refuses_late_key_after_review_with_zero_writes(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=ROOT_UUID,
        )
        projection = [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
            }},
        ]
        manifest = reviewed_manifest(adapter, projection)
        late = build_projection_event(
            workstream_id="GEN-37", kind="relation", key="blocks:GEN-99",
            value={"type": "blocks", "target": {
                "workspace_id": "workspace", "issue_id": "late-uuid",
                "identifier": "GEN-99",
            }},
            plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-27T18:00:01Z",
        )
        client.comments.append({
            "id": "late-comment", "body": encode_projection_comment(late),
            "createdAt": "now", "updatedAt": "now",
        })
        writes_before = len(client.comments)
        with self.assertRaisesRegex(LinearProjectionError, "stale_reload_required"):
            reconcile_required_projection(
                adapter, {"root": {"identifier": "GEN-37"}}, manifest,
                remote_head=HEAD, created_at="2026-08-27T18:01:00Z",
                authenticated_source={
                    "identity": "https://example.test/plan", "sha256": PLAN,
                },
            )
        self.assertEqual(len(client.comments), writes_before)
        self.assertFalse(any("commentCreate" in query for query, _ in client.calls))

    def test_product_reconcile_preserves_omitted_live_key_without_retirement(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=ROOT_UUID,
        )
        base = [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
            }},
        ]
        relation = {"type": "blocks", "target": {
            "workspace_id": "workspace", "issue_id": "target-uuid",
            "identifier": "GEN-14",
        }}
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, [*base, {
                "kind": "relation", "key": "blocks:GEN-14", "value": relation,
            }]), remote_head=HEAD, created_at="2026-08-27T18:00:00Z",
            authenticated_source=source,
        )
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, base), remote_head=HEAD,
            created_at="2026-08-27T19:00:00Z", authenticated_source=source,
        )
        state = adapter.state().snapshot
        self.assertEqual(state["relations"], [relation])
        self.assertFalse(any(
            event["kind"] == "relation" and event["value"] == TOMBSTONE
            for event in state["projection_events"]
        ))

    def test_product_reconcile_refuses_stale_explicit_retirement_head(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=ROOT_UUID,
        )
        base = [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
            }},
            {"kind": "relation", "key": "blocks:GEN-14", "value": {
                "type": "blocks", "target": {
                    "workspace_id": "workspace", "issue_id": "target-uuid",
                    "identifier": "GEN-14",
                },
            }},
        ]
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, base), remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
        )
        retirement = reviewed_retirement(adapter, "relation", "blocks:GEN-14")
        manifest = reviewed_manifest(adapter, base[:-1], [retirement])
        current = next(
            event for event in reversed(adapter.state().events)
            if event["kind"] == "relation" and event["key"] == "blocks:GEN-14"
        )
        changed = build_projection_event(
            workstream_id="GEN-37", kind="relation", key="blocks:GEN-14",
            value={"type": "blocks", "target": {
                "workspace_id": "workspace", "issue_id": "changed-uuid",
                "identifier": "GEN-14",
            }},
            plan_revision=PLAN, expected_revision=adapter.state().revision,
            created_at="2026-08-27T18:00:30Z",
            supersedes_event_id=current["event_id"],
        )
        adapter.append(changed)
        writes_before = len(client.comments)
        with self.assertRaisesRegex(LinearProjectionError, "stale_reload_required"):
            reconcile_required_projection(
                adapter, {"root": {"identifier": "GEN-37"}}, manifest,
                remote_head=HEAD, created_at="2026-08-27T19:00:00Z",
                authenticated_source=source,
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_product_reconcile_retirement_must_name_exact_reviewed_head(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=ROOT_UUID,
        )
        relation = {"type": "blocks", "target": {
            "workspace_id": "workspace", "issue_id": "target-uuid",
            "identifier": "GEN-14",
        }}
        base = [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
            }},
        ]
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, [*base, {
                "kind": "relation", "key": "blocks:GEN-14", "value": relation,
            }]), remote_head=HEAD, created_at="2026-08-27T18:00:00Z",
            authenticated_source=source,
        )
        retirement = reviewed_retirement(adapter, "relation", "blocks:GEN-14")
        retirement["expected_event_id"] = "wsp_stale"
        manifest = reviewed_manifest(adapter, base, [retirement])
        writes_before = len(client.comments)
        with self.assertRaisesRegex(LinearProjectionError, "retirement_stale"):
            reconcile_required_projection(
                adapter, {"root": {"identifier": "GEN-37"}}, manifest,
                remote_head=HEAD, created_at="2026-08-27T19:00:00Z",
                authenticated_source=source,
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_product_reconcile_refuses_unverified_source_bytes(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN,
        )
        manifest = reviewed_manifest(adapter, [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
            }},
        ])
        with self.assertRaisesRegex(LinearProjectionError, "source_bytes_mismatch"):
            reconcile_required_projection(
                adapter, {"root": {"identifier": "GEN-37"}}, manifest,
                remote_head=HEAD, created_at="2026-08-27T18:00:00Z",
                authenticated_source={"identity": "https://example.test/plan",
                                      "sha256": "b" * 64},
            )
        self.assertEqual(client.comments, [])

    def test_product_reconcile_preflights_root_revision_and_full_route_before_write(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=ROOT_UUID,
        )
        projected_scope = scope()
        projected_scope["linear"]["root_issue_id"] = "wrong"
        manifest = reviewed_manifest(adapter, [
            {"kind": "scope", "key": "root", "value": projected_scope},
            {"kind": "source", "key": "root", "value": {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
            }},
        ])
        with self.assertRaisesRegex(LinearProjectionError, "root_issue_id"):
            reconcile_required_projection(
                adapter, {"root": {"identifier": "GEN-37"}}, manifest,
                remote_head=HEAD, created_at="2026-08-27T18:00:00Z",
                authenticated_source={"identity": "https://example.test/plan", "sha256": PLAN},
            )
        self.assertEqual(client.comments, [])
        manifest["projection"][0]["value"] = scope()
        manifest["projection"][1]["value"]["sha256"] = "b" * 64
        with self.assertRaisesRegex(LinearProjectionError, "root_plan_revision"):
            reconcile_required_projection(
                adapter, {"root": {"identifier": "GEN-37"}}, manifest,
                remote_head=HEAD, created_at="2026-08-27T18:00:00Z",
                authenticated_source={"identity": "https://example.test/plan", "sha256": "b" * 64},
            )
        self.assertEqual(client.comments, [])

    def test_projection_cli_end_to_end_is_idempotent_and_full_resume_verified(self):
        raw = b"# Exact plan\n\n## Deliver\n"
        digest = hashlib.sha256(raw).hexdigest()
        identity = "https://example.test/commit/plan.md"
        client = FakeProjectionClient()
        scoped = scope()
        manifest = {
            "expected_projection_revision": 0,
            "expected_active_heads": [],
            "retirements": [],
            "projection": [
            {"kind": "scope", "key": "root", "value": scoped},
            {"kind": "source", "key": "root", "value": {
                "sha256": digest, "identity": identity,
            }},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
                "worktree": {"state": "safe", "head": HEAD},
            }},
        ]}
        graph = {
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37",
                     "plan_revision": digest, "revision": 0,
                     "status": "In Progress", "next_action": "continue"},
            "children": [{"identifier": "GEN-38", "title": "Resume transport",
                          "status": "In Progress", "next_action": "continue"}],
            "decisions": [],
        }
        route = {"workspace_id": "workspace", "team_id": "team",
                 "project_id": "project", "root_issue_id": ROOT_UUID}
        comments = mock.Mock()
        comments.comments.side_effect = lambda: [dict(item) for item in client.comments]
        transport = mock.Mock()
        transport.snapshot_for_root.return_value = graph
        with tempfile.TemporaryDirectory() as directory:
            plan_path = Path(directory) / "plan.md"
            manifest_path = Path(directory) / "manifest.json"
            plan_path.write_bytes(raw)
            manifest_path.write_text(json.dumps(manifest))
            argv = [
                "workstream_projection.py", "GEN-37", str(manifest_path),
                "--remote-head", HEAD, "--plan-source", str(plan_path),
                "--plan-identity", identity,
            ]
            for expected_writes in (4, 4):
                manifest_path.write_text(json.dumps(manifest))
                output = io.StringIO()
                with mock.patch.object(workstream_projection.sys, "argv", argv), \
                     mock.patch.object(workstream_projection.sys, "stdout", output), \
                     mock.patch.object(workstream_projection, "load_linear_api_key", return_value="secret"), \
                     mock.patch.object(workstream_projection, "HttpGraphQLClient", return_value=client), \
                     mock.patch.object(workstream_projection, "resolve_linear_route", return_value=(route, None)), \
                     mock.patch.object(workstream_projection, "resolve_authenticated_issue_route", return_value=route), \
                     mock.patch.object(workstream_projection, "LinearGraphQLTransport", return_value=transport), \
                     mock.patch.object(workstream_projection, "LinearCommentEventAdapter", return_value=comments):
                    self.assertEqual(workstream_projection.main(), 0)
                payload = json.loads(output.getvalue())
                self.assertTrue(payload["readback_verified"])
                self.assertEqual(len(client.comments), expected_writes)
                manifest.update(payload["projection_contract"])
        self.assertEqual(len(json.loads(output.getvalue())["writes"]), 0)

    def test_final_live_readback_refuses_concurrent_graph_or_checkpoint_change(self):
        graph = {"root": {"identifier": "GEN-37"}, "children": []}
        changed = {"root": {"identifier": "GEN-37"},
                   "children": [{"identifier": "GEN-38"}]}
        transport = mock.Mock()
        transport.snapshot_for_root.side_effect = [graph, changed, changed]
        comments = mock.Mock()
        comments.comments.return_value = []
        with self.assertRaisesRegex(LinearProjectionError, "changed_during_read"):
            stable_live_readback(transport, comments, "GEN-37")

        transport.snapshot_for_root.side_effect = [graph, graph, graph]
        comments.comments.side_effect = [[], [{"id": "new-checkpoint"}]]
        with self.assertRaisesRegex(LinearProjectionError, "changed_during_read"):
            stable_live_readback(transport, comments, "GEN-37")

        transport.snapshot_for_root.side_effect = [graph, graph, changed]
        comments.comments.side_effect = [[], []]
        with self.assertRaisesRegex(LinearProjectionError, "changed_during_read"):
            stable_live_readback(transport, comments, "GEN-37")


if __name__ == "__main__":
    unittest.main()
