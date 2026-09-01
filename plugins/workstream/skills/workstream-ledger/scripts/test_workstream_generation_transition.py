#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy
import hashlib
import io
import json
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import workstream_projection
import workstream_generation

from workstream_delta import Delta
from workstream_generation import (
    GenerationTransport, WorkstreamGenerationError, _digest,
    _gen14_legacy_split_head_prefix,
    _gen14_recorded_repair_head,
    build_retirement_proof, generation_quarantine_metadata, main, parser,
    prepare_generation_operator_contract,
    validate_activation_operator_contract,
    encode_generation_finalization, generation_finalization_slot_id,
    native_root_activation_proof,
    pending_generation_reservations, reduce_generation_checkpoint_comments,
    reduce_generation_checkpoint_custodies,
    reduce_generation_finalizations, reduce_generation_reservations,
    selected_activation_checkpoints,
    selected_generation_execution_status,
    strict_candidate_loader,
)
from workstream_checkpoint import build_checkpoint
from workstream_linear import LinearGraphQLTransport, LinearTransportError
from workstream_linear_events import (
    encode_event_comment, LinearCommentEventAdapter, ledger_boundary_slot_id,
    reduce_event_comments,
)
from workstream_linear_checkpoints import (
    encode_checkpoint_comment, LinearCheckpointAdapter, reduce_checkpoint_comments,
)
from workstream_linear_projection import (
    _decode_projection, build_projection_event, encode_projection_comment,
    LinearProjectionAdapter,
    LinearProjectionError, PROJECTION_PREFIX, PROJECTION_RE,
    projection_slot_id, reduce_projection_comments, select_plan_generation,
)
from workstream_projection import bind_projection_plan_generation
from workstream_resume import compact_context as real_compact_context
from workstream_root_transition import (
    RootTransitionError, RootTransitionTransport, validate_operator_contract,
)


WORKSTREAM = "GEN-37"
OLD, NEW, LATER, OTHER = (letter * 64 for letter in "abcd")
AUTHORITY = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
    "root_issue_id": "33333333-3333-4333-8333-333333333333",
}
STARTED_STATE = {
    "id": "44444444-4444-4444-8444-444444444444",
    "name": "In Progress", "type": "started", "team_id": "team",
}
CHILD_CONTENT = {
    "schema_version": 1,
    "title": "New child",
    "description_sha256": "1" * 64,
}


class FakeClient:
    def __init__(self):
        self.comments: list[dict] = []
        self.mutations: list[dict] = []
        self.commit_then_fail_at: set[int] = set()
        self.before_each_create = None
        self.description = f"Plan revision: {OLD}"
        self.graph_nonce = "initial"
        self.graph_title = "Generation test"
        self.graph_status = "In Progress"
        self.graph_status_type = "started"
        self.graph_state_id = "state"
        self.children: list[dict] = []
        self.before_issue_create = None
        self.allow_issue_update = False
        self.lose_issue_update_response_once = False
        self.issue_update_count = 0
        self.comment_updates_root = False
        self.comment_clock = 0
        self.expanded_resume_project = False

    def root_issue(self):
        return {
            "id": AUTHORITY["root_issue_id"], "identifier": WORKSTREAM,
            "title": self.graph_title, "description": self.description,
            "url": "https://linear.test/GEN-37", "updatedAt": self.graph_nonce,
            "parent": None,
            "project": {"id": "project"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "assignee": None,
            "state": {"id": self.graph_state_id, "name": self.graph_status,
                      "type": self.graph_status_type},
        }

    def execute(self, query, variables):
        if "query WorkstreamNativeState" in query:
            return {
                "team": {"id": variables["teamId"],
                         "organization": {"id": "workspace"}},
                "workflowState": {"id": variables["stateId"],
                                  "team": {"id": variables["teamId"]}},
            }
        if "query WorkstreamNativeAssignee" in query:
            return {"user": {
                "id": variables["assigneeId"],
                "active": True,
                "organization": {"id": "workspace"},
                "teams": {"nodes": [{"id": "team"}], "pageInfo": {
                    "hasNextPage": False, "endCursor": None,
                }},
            }}
        if "WorkstreamRootTransitionState" in query:
            return {
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "workflowState": {
                    "id": variables["stateId"], "name": "In Progress",
                    "type": "started", "team": {"id": "team"},
                },
            }
        if "WorkstreamRootTransition" in query:
            if not self.allow_issue_update:
                raise AssertionError("generation protocol must never issueUpdate")
            update = variables["input"]
            self.issue_update_count += 1
            if "stateId" in update:
                self.graph_state_id = update["stateId"]
                self.graph_status = "In Progress"
                self.graph_status_type = "started"
            self.graph_nonce = f"issue-update-{self.issue_update_count}"
            if self.lose_issue_update_response_once:
                self.lose_issue_update_response_once = False
                raise LinearTransportError("response lost after accepted issue update")
            return {"issueUpdate": {"success": True, "issue": self.root_issue()}}
        if "issueUpdate" in query:
            raise AssertionError("generation protocol must never issueUpdate")
        if "CommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}]}}
        if "query WorkstreamResumeRoot" in query:
            root = self.root_issue()
            if self.expanded_resume_project:
                root["project"] = {**root["project"], "name": "Project"}
            return {"issue": {**root, "children": {
                "nodes": [], "pageInfo": {
                    "hasNextPage": False, "endCursor": None,
                },
            }}}
        if "query WorkstreamIssues" in query:
            return {"team": {"issues": {
                "nodes": [self.root_issue(), *deepcopy(self.children)], "pageInfo": {
                    "hasNextPage": False, "endCursor": None,
                },
            }}}
        if "query WorkstreamRoute" in query:
            return {
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project", "teams": {
                    "nodes": [{"id": "team"}],
                }},
            }
        if "query WorkstreamDeltaComments" in query:
            return {"issue": {
                "id": AUTHORITY["root_issue_id"], "identifier": WORKSTREAM,
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                "comments": {"nodes": deepcopy(self.comments), "pageInfo": {
                    "hasNextPage": False, "endCursor": None,
                }},
            }}
        if (
            "query WorkstreamChildRelations" in query
            or "query WorkstreamChildInverseRelations" in query
        ):
            child = next(
                item for item in self.children
                if item["identifier"] == variables["issueId"]
            )
            field = (
                "inverseRelations"
                if "InverseRelations" in query else "relations"
            )
            return {"issue": {**deepcopy(child), field: {
                "nodes": [], "pageInfo": {
                    "hasNextPage": False, "endCursor": None,
                },
            }}}
        if "commentCreate" in query:
            item = deepcopy(variables["input"])
            if self.before_each_create is not None:
                self.before_each_create(item, self)
            if any(comment["id"] == item["id"] for comment in self.comments):
                raise LinearTransportError("duplicate comment id")
            self.mutations.append(item)
            timestamp = "2026-08-29T00:00:00Z"
            if self.comment_updates_root:
                self.comment_clock += 1
                timestamp = f"comment-{self.comment_clock}"
            comment = {
                "id": item["id"], "body": item["body"],
                "createdAt": timestamp, "updatedAt": timestamp,
            }
            self.comments.append(comment)
            if self.comment_updates_root:
                self.graph_nonce = timestamp
            if len(self.mutations) in self.commit_then_fail_at:
                raise LinearTransportError("lost response after commit")
            return {"commentCreate": {"success": True, "comment": deepcopy(comment)}}
        if "issueCreate" in query:
            if self.before_issue_create is not None:
                callback = self.before_issue_create
                self.before_issue_create = None
                callback()
            item = deepcopy(variables["input"])
            if any(child["id"] == item["id"] for child in self.children):
                raise LinearTransportError("duplicate issue id")
            child = {
                "id": item["id"], "identifier": "GEN-38",
                "title": item["title"], "description": item["description"],
                "url": "https://linear.test/GEN-38", "updatedAt": "created",
                "parent": {"id": item["parentId"], "identifier": WORKSTREAM},
                "project": {"id": item["projectId"]},
                "team": {
                    "id": item["teamId"],
                    "organization": {"id": "workspace"},
                },
                "state": {
                    "id": item["stateId"], "name": "Ready", "type": "started",
                },
                "assignee": (
                    {"id": item["assigneeId"]}
                    if item.get("assigneeId") else None
                ),
            }
            self.children.append(child)
            self.mutations.append(item)
            return {"issueCreate": {"success": True, "issue": deepcopy(child)}}
        raise AssertionError(f"unexpected operation: {query[:80]}")


def adapter(client, plan):
    return LinearProjectionAdapter(
        client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
        plan_revision=plan, **AUTHORITY,
    )


def project_full(client, plan, identity=None):
    target = adapter(client, plan)
    target.append(build_projection_event(
        workstream_id=WORKSTREAM, kind="scope", key="root",
        value={
            "namespace": "generation-tests",
            "linear": {**AUTHORITY, "route_verification": {
                **AUTHORITY, "observed_at": "2026-08-29T00:00:00Z", "evidence": [{
                    "kind": "authenticated_linear_readback", "authenticated": True,
                    **AUTHORITY,
                }],
            }},
            "primary_repository": "github.com:id:R_repo",
            "repositories": [{
                "slug": "github.com/generous-corp/agent-workstream",
                "provider_repository_id": "R_repo", "aliases": [],
                "exact_head": "e" * 40, "identity_resolution": {
                    "provider_repository_id": "R_repo",
                    "resolved_slug": "github.com/generous-corp/agent-workstream",
                    "observed_at": "2026-08-29T00:00:00Z", "evidence": [{
                        "kind": "authenticated_provider_readback", "authenticated": True,
                        "provider_repository_id": "R_repo",
                        "resolved_slug": "github.com/generous-corp/agent-workstream",
                    }],
                }, "identity_updates": [], "evidence": [],
            }], "child_ownership": {},
        }, plan_revision=plan, expected_revision=0, created_at="0",
        authority=AUTHORITY,
    ))
    target.append(build_projection_event(
        workstream_id=WORKSTREAM, kind="source", key="root",
        value={"identity": identity or f"https://example.test/{plan}",
               "sha256": plan},
        plan_revision=plan, expected_revision=1, created_at="1", authority=AUTHORITY,
    ))
    target.append(build_projection_event(
        workstream_id=WORKSTREAM, kind="provenance", key="generation",
        value={"agent": "codex", "machine": "M5", "session_id": plan[:8],
               "worktree": {"state": "safe", "head": "e" * 40}},
        plan_revision=plan, expected_revision=2, created_at="2", authority=AUTHORITY,
    ))
    target.append(build_projection_event(
        workstream_id=WORKSTREAM, kind="disposition", key="root",
        value={"disposition": "attach", "remote_head": "e" * 40,
               "recovered_from_checkpoint": None},
        plan_revision=plan, expected_revision=3, created_at="3", authority=AUTHORITY,
    ))


class Loader:
    def __init__(self, client):
        self.client = client
        self.graph = "stable"
        self.authority = "full"

    def __call__(self, plan):
        comments = deepcopy(self.client.comments)
        state = reduce_projection_comments(
            comments, workstream_id=WORKSTREAM, expected_plan_revision=plan,
            authenticated_route=AUTHORITY,
        )
        source = state.snapshot["source"]
        checkpoints = reduce_generation_checkpoint_comments(
            comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        )
        checkpoint_ids = sorted(item["event_id"] for item in checkpoints.checkpoints
                                if item["plan_revision"] == plan)
        material = reduce_event_comments(comments, workstream_id=WORKSTREAM)
        surface = {
            "plan": plan, "material": material.revision,
            "checkpoints": checkpoint_ids, "projection": state.revision,
            "events": [event["event_id"] for event in state.events],
        }
        return {
            "resume_authority": self.authority, "plan_revision": plan,
            "authenticated_route": AUTHORITY,
            "source": {"identity": source.get("identity") or source.get("url"),
                       "sha256": source["sha256"]},
            "material_revision": material.revision,
            "checkpoint_event_ids": checkpoint_ids,
            "projection_revision": state.revision,
            "graph_frontier_sha256": _digest(self.graph),
            "snapshot_sha256": _digest(surface),
            "quarantined_legacy_writes": generation_quarantine_metadata(
                comments, workstream_id=WORKSTREAM,
            ),
        }


class ActivationCheckpointLoader(Loader):
    def __init__(self, client, checkpoint):
        super().__init__(client)
        self.checkpoint = checkpoint

    def __call__(self, plan):
        receipt = super().__call__(plan)
        ids = sorted(set([
            *receipt["checkpoint_event_ids"], self.checkpoint["event_id"],
        ])) if plan == self.checkpoint["plan_revision"] else receipt[
            "checkpoint_event_ids"
        ]
        receipt["checkpoint_event_ids"] = ids
        receipt["snapshot_sha256"] = _digest({
            "base": receipt["snapshot_sha256"], "checkpoint_event_ids": ids,
        })
        return receipt


class GenerationTransitionTests(unittest.TestCase):
    def test_gen14_legacy_split_producer_accepts_only_captured_prefix(self):
        from types import SimpleNamespace
        plan = "e" * 64
        stored_frontier, frontier = "d" * 64, "f" * 64
        old_disposition_head, old_scope_head = "a" * 40, "b" * 40
        prefix_time = "2030-01-01T00:00:00Z"
        scope_value = {
            "primary_repository": "github.com:id:R_synthetic",
            "repositories": [{"provider_repository_id": "R_synthetic",
                              "slug": "github.com/acme/synthetic",
                              "exact_head": old_scope_head, "aliases": [],
                              "evidence": [], "identity_updates": [],
                              "identity_resolution": {
                                  "provider_repository_id": "R_synthetic",
                                  "resolved_slug": "github.com/acme/synthetic",
                                  "observed_at": prefix_time,
                                  "evidence": [{"authenticated": True,
                                      "kind": "authenticated_provider_readback",
                                      "provider_repository_id": "R_synthetic",
                                      "resolved_slug": "github.com/acme/synthetic"}],
                              }}],
            "child_ownership": {}, "linear": deepcopy(AUTHORITY),
            "namespace": "synthetic",
        }
        values = [
            {"slice_id": key, "owning_child": child,
             "predecessor_closure_authority": {
                "input_frontier_sha256": stored_frontier,
            }} for key, child in (("one", "GEN-1"), ("two", "GEN-2"))
        ] + [
            {"agent": "synthetic", "machine": "test", "session_id": "session"},
            {"identity": "https://example.test/plan", "sha256": plan},
            {"disposition": "create_successor",
             "remote_head": old_disposition_head,
             "recovered_from_checkpoint": None},
            scope_value,
        ]
        identities = [
            ("evidence_contract", "one"), ("evidence_contract", "two"),
            ("provenance", "synthetic"), ("source", "root"),
            ("disposition", "root"), ("scope", "root"),
        ]
        events = [build_projection_event(
            workstream_id="GEN-14", kind=kind, key=key, value=value,
            plan_revision=plan, expected_revision=index,
            created_at=prefix_time, authority=AUTHORITY,
        ) for index, ((kind, key), value) in enumerate(zip(identities, values))]
        original_digest = workstream_generation.GEN14_LEGACY_SPLIT_PREFIX_SHA256
        workstream_generation.GEN14_LEGACY_SPLIT_PREFIX_SHA256 = (
            workstream_projection.canonical_digest(events)
        )
        original_stored_frontier = (
            workstream_generation.GEN14_LEGACY_SPLIT_STORED_FRONTIER_SHA256
        )
        original_recomputed_frontier = (
            workstream_generation.GEN14_LEGACY_SPLIT_RECOMPUTED_FRONTIER_SHA256
        )
        workstream_generation.GEN14_LEGACY_SPLIT_STORED_FRONTIER_SHA256 = (
            stored_frontier
        )
        workstream_generation.GEN14_LEGACY_SPLIT_RECOMPUTED_FRONTIER_SHA256 = (
            frontier
        )
        self.addCleanup(
            setattr, workstream_generation, "GEN14_LEGACY_SPLIT_PREFIX_SHA256",
            original_digest,
        )
        self.addCleanup(
            setattr, workstream_generation,
            "GEN14_LEGACY_SPLIT_STORED_FRONTIER_SHA256",
            original_stored_frontier,
        )
        self.addCleanup(
            setattr, workstream_generation,
            "GEN14_LEGACY_SPLIT_RECOMPUTED_FRONTIER_SHA256",
            original_recomputed_frontier,
        )
        state = SimpleNamespace(revision=6, events=events)
        fresh_head = "c" * 40
        self.assertTrue(_gen14_legacy_split_head_prefix(
            state, workstream_id="GEN-14",
            target_plan=plan, input_frontier_sha256=frontier,
            remote_head=fresh_head,
        ))
        desired_scope = deepcopy(events[5]["value"])
        next(
            repository for repository in desired_scope["repositories"]
            if repository["provider_repository_id"] == "R_synthetic"
        )["exact_head"] = fresh_head
        repair_time = "2026-09-01T10:00:00Z"
        disposition = build_projection_event(
            workstream_id="GEN-14", kind="disposition", key="root",
            value={"disposition": "create_successor",
                   "remote_head": fresh_head,
                   "recovered_from_checkpoint": None},
            plan_revision=events[0]["plan_revision"], expected_revision=6,
            created_at=repair_time, supersedes_event_id=events[4]["event_id"],
            authority=events[0]["authority"],
        )
        scope = build_projection_event(
            workstream_id="GEN-14", kind="scope", key="root",
            value=desired_scope, plan_revision=events[0]["plan_revision"],
            expected_revision=7, created_at=repair_time,
            supersedes_event_id=events[5]["event_id"],
            authority=events[0]["authority"],
        )
        for tail in ([disposition], [disposition, scope]):
            self.assertTrue(_gen14_legacy_split_head_prefix(
                SimpleNamespace(revision=6 + len(tail), events=[*events, *tail]),
                workstream_id="GEN-14", target_plan=plan,
                input_frontier_sha256=frontier, remote_head=fresh_head,
            ))
        later_head = "d" * 40
        later_disposition = build_projection_event(
            workstream_id="GEN-14", kind="disposition", key="root",
            value={"disposition": "create_successor", "remote_head": later_head,
                   "recovered_from_checkpoint": None},
            plan_revision=plan, expected_revision=8,
            created_at="2030-01-01T02:00:00Z",
            supersedes_event_id=disposition["event_id"], authority=AUTHORITY,
        )
        later_scope_value = deepcopy(desired_scope)
        next(item for item in later_scope_value["repositories"]
             if item["provider_repository_id"] == "R_synthetic"
             )["exact_head"] = later_head
        later_scope = build_projection_event(
            workstream_id="GEN-14", kind="scope", key="root",
            value=later_scope_value, plan_revision=plan, expected_revision=9,
            created_at="2030-01-01T02:00:00Z",
            supersedes_event_id=scope["event_id"], authority=AUTHORITY,
        )
        for later_tail in ([later_disposition], [later_disposition, later_scope]):
            self.assertTrue(_gen14_legacy_split_head_prefix(
                SimpleNamespace(
                    revision=8 + len(later_tail),
                    events=[*events, disposition, scope, *later_tail],
                ),
                workstream_id="GEN-14", target_plan=plan,
                input_frontier_sha256=frontier, remote_head=fresh_head,
            ))
        newer_main = "d" * 40
        self.assertEqual(
            _gen14_recorded_repair_head(
                SimpleNamespace(revision=7, events=[*events, disposition]),
                newer_main, workstream_id="GEN-14", target_plan=plan,
                input_frontier_sha256=frontier,
            ),
            fresh_head,
        )
        mutations = {
            "arbitrary_pair": (5, "event_id", "wsp_" + "f" * 32),
            "plan": (3, "plan_revision", "9" * 64),
            "route": (3, "authority", {
                **AUTHORITY, "project_id": "other-project",
            }),
            "wrong_order": (4, "expected_revision", 5),
            "superseded": (4, "supersedes_event_id", "wsp_" + "e" * 32),
            "noncanonical_disposition": (
                4, "value", {"disposition": "attach", "remote_head": "a" * 40,
                             "recovered_from_checkpoint": None},
            ),
        }
        for name, (index, field, value) in mutations.items():
            with self.subTest(name=name):
                changed = deepcopy(events)
                changed[index][field] = value
                self.assertFalse(_gen14_legacy_split_head_prefix(
                    SimpleNamespace(revision=6, events=changed),
                    workstream_id="GEN-14",
                    target_plan=plan, input_frontier_sha256=frontier,
                    remote_head=fresh_head,
                ))
        self.assertEqual(_gen14_recorded_repair_head(
            SimpleNamespace(revision=7, events=[*events, disposition]),
            newer_main, workstream_id="GEN-99", target_plan=plan,
            input_frontier_sha256=frontier,
        ), newer_main)
        for workstream_id, candidate_plan, candidate_frontier in (
            ("GEN-99", plan, frontier),
            ("GEN-14", "9" * 64, frontier),
            ("GEN-14", plan, "8" * 64),
        ):
            self.assertFalse(_gen14_legacy_split_head_prefix(
                state, workstream_id=workstream_id,
                target_plan=candidate_plan,
                input_frontier_sha256=candidate_frontier,
                remote_head=fresh_head,
            ))
        for colliding_head in (old_disposition_head, old_scope_head):
            self.assertFalse(_gen14_legacy_split_head_prefix(
                state, workstream_id="GEN-14", target_plan=plan,
                input_frontier_sha256=frontier,
                remote_head=colliding_head,
            ))

        changed_stored = deepcopy(events)
        for event in changed_stored[:2]:
            event["value"]["predecessor_closure_authority"][
                "input_frontier_sha256"
            ] = "7" * 64
        workstream_generation.GEN14_LEGACY_SPLIT_PREFIX_SHA256 = (
            workstream_projection.canonical_digest(changed_stored)
        )
        self.assertFalse(_gen14_legacy_split_head_prefix(
            SimpleNamespace(revision=6, events=changed_stored),
            workstream_id="GEN-14", target_plan=plan,
            input_frontier_sha256=frontier, remote_head=fresh_head,
        ))

    def setUp(self):
        self.client = FakeClient()
        project_full(self.client, OLD)
        self.loader = Loader(self.client)
        self.transport = GenerationTransport(
            self.client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            authority=AUTHORITY, candidate_loader=self.loader,
            legacy_description_plan_revision=OLD,
        )

    def retirement(self, predecessor=OLD, epoch=0):
        state = adapter(self.client, predecessor).state()
        checkpoints = reduce_generation_checkpoint_comments(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        )
        return build_retirement_proof(
            predecessor_plan_revision=predecessor, retired_at="now",
            retired_writer_epoch=epoch,
            provenance_event_ids=[event["event_id"] for event in state.events
                                  if event["kind"] == "provenance"],
            checkpoint_event_ids=sorted(
                item["event_id"] for item in checkpoints.checkpoints
                if item["plan_revision"] == predecessor
            ),
        )

    def generation_preparation(self):
        graph = LinearGraphQLTransport(
            self.client, workspace_id="workspace", team_id="team",
            project_id="project",
        ).snapshot_for_root(
            WORKSTREAM, include_description=True, include_child_comments=True,
        )
        return prepare_generation_operator_contract(
            comments=deepcopy(self.client.comments), graph=graph,
            workstream_id=WORKSTREAM, authority=AUTHORITY,
            description_plan_revision=OLD,
            target_source={
                "identity": f"https://example.test/{NEW}", "sha256": NEW,
            },
            created_at="2026-08-31T23:00:00Z",
            remote_head="e" * 40,
            started_state=STARTED_STATE,
        )

    def test_prepare_emits_complete_zero_write_operator_contract(self):
        writes = len(self.client.mutations)
        comments = deepcopy(self.client.comments)
        first = self.generation_preparation()
        second = self.generation_preparation()
        self.assertEqual(first, second)
        self.assertEqual(len(self.client.mutations), writes)
        self.assertEqual(self.client.comments, comments)
        self.assertEqual(first["projection_preview"]["writes_performed"], 0)
        self.assertFalse(first["projection_preview"]["apply"])
        self.assertEqual(
            first["retirement_proof"]["provenance_event_ids"],
            [next(
                event["event_id"] for event in adapter(self.client, OLD).state().events
                if event["kind"] == "provenance"
            )],
        )
        self.assertEqual(first["retirement_proof"]["schema_version"], 2)
        quiescence = first["retirement_proof"]["authenticated_quiescence"]
        self.assertEqual(quiescence["authenticated_route"], AUTHORITY)
        self.assertEqual(quiescence["material_revision"], 0)
        self.assertEqual(
            quiescence["predecessor_projection"],
            first["frontiers"]["predecessor_projection"],
        )
        self.assertEqual(first["native_transition"], {
            "operation": "reopen", "target_state": STARTED_STATE,
        })
        accounting = first["projection_preview"]["active_key_accounting"]
        classified = {
            (item["kind"], item["key"])
            for category in accounting.values() for item in category
        }
        self.assertEqual(classified, {
            ("scope", "root"), ("source", "root"),
            ("provenance", "generation"), ("disposition", "root"),
        })
        items = {
            (item["kind"], item["key"]): item["value"]
            for item in first["projection_preview"]["manifest"]["projection"]
        }
        self.assertEqual(items[("source", "root")], {
            "identity": f"https://example.test/{NEW}", "sha256": NEW,
        })
        self.assertNotIn(("disposition", "root"), items)
        self.assertEqual(first["contract_sha256"], _digest({
            key: value for key, value in first.items()
            if key != "contract_sha256"
        }))

    def test_operator_gate_recomputes_prepare_and_rejects_forged_ready_phase(self):
        predecessor = adapter(self.client, OLD)
        predecessor.append(build_projection_event(
            workstream_id=WORKSTREAM, kind="choice", key="must-carry",
            value={
                "event_id": "must-carry",
                "decision": "preserve this required choice",
            },
            plan_revision=OLD, expected_revision=predecessor.state().revision,
            created_at="predecessor-choice", authority=AUTHORITY,
        ))
        graph = LinearGraphQLTransport(
            self.client, workspace_id="workspace", team_id="team",
            project_id="project",
        ).snapshot_for_root(
            WORKSTREAM, include_description=True, include_child_comments=True,
        )
        activation_graph = deepcopy(graph)
        activation_graph["root"]["state"] = {
            "id": STARTED_STATE["id"], "name": STARTED_STATE["name"],
            "type": STARTED_STATE["type"],
        }
        incomplete = self.generation_preparation()
        self.assertEqual(
            incomplete["projection_preview"]["phase"], "complete_projection",
        )
        target = adapter(self.client, NEW)
        revision = target.state().revision
        omitted_choice = None
        for index, item in enumerate(
            incomplete["projection_preview"]["manifest"]["projection"]
        ):
            if (item["kind"], item["key"]) == ("choice", "must-carry"):
                omitted_choice = item
                continue
            target.append(build_projection_event(
                workstream_id=WORKSTREAM, kind=item["kind"], key=item["key"],
                value=deepcopy(item["value"]), plan_revision=NEW,
                expected_revision=revision, created_at=f"target-{index}",
                authority=AUTHORITY,
            ))
            revision += 1
        self.assertIsNotNone(omitted_choice)
        target.append(build_projection_event(
            workstream_id=WORKSTREAM, kind="disposition", key="root",
            value={
                "disposition": "attach", "remote_head": "e" * 40,
                "recovered_from_checkpoint": None,
            },
            plan_revision=NEW, expected_revision=revision,
            created_at="target-disposition", authority=AUTHORITY,
        ))
        revision += 1
        partial = self.generation_preparation()
        self.assertEqual(
            partial["projection_preview"]["phase"], "complete_projection",
        )
        forged = deepcopy(partial)
        forged["projection_preview"]["phase"] = "activation_ready"
        forged["projection_preview"]["next_gate"] = "preview_generation_activation"
        forged["contract_sha256"] = _digest({
            key: value for key, value in forged.items()
            if key != "contract_sha256"
        })
        before_comments = deepcopy(self.client.comments)
        before_mutations = deepcopy(self.client.mutations)
        with self.assertRaisesRegex(
            RootTransitionError,
            "operator_contract_not_exact_live_prepare_output",
        ):
            validate_activation_operator_contract(
                forged, source=forged["source"], workstream_id=WORKSTREAM,
                authority=AUTHORITY, comments=deepcopy(self.client.comments),
                graph=activation_graph,
                description_plan_revision=OLD,
                created_at=forged["created_at"], remote_head=forged["remote_head"],
            )
        self.assertEqual(self.client.comments, before_comments)
        self.assertEqual(self.client.mutations, before_mutations)

        target.append(build_projection_event(
            workstream_id=WORKSTREAM,
            kind=omitted_choice["kind"], key=omitted_choice["key"],
            value=deepcopy(omitted_choice["value"]), plan_revision=NEW,
            expected_revision=revision, created_at="target-required-choice",
            authority=AUTHORITY,
        ))
        ready = self.generation_preparation()
        self.assertEqual(ready["projection_preview"]["phase"], "activation_ready")
        activation = validate_activation_operator_contract(
            ready, source=ready["source"], workstream_id=WORKSTREAM,
            authority=AUTHORITY, comments=deepcopy(self.client.comments),
            graph=activation_graph,
            description_plan_revision=OLD,
            created_at=ready["created_at"], remote_head=ready["remote_head"],
        )
        self.assertEqual(
            activation["authorization"]["contract_sha256"],
            ready["contract_sha256"],
        )

    def test_prepare_refuses_nonempty_inactive_candidate_without_mutation(self):
        project_full(self.client, NEW)
        writes = len(self.client.mutations)
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "noncanonical_target_prefix",
        ):
            self.generation_preparation()
        self.assertEqual(len(self.client.mutations), writes)

    def test_prepare_requires_exact_source_and_timestamps(self):
        graph = LinearGraphQLTransport(
            self.client, workspace_id="workspace", team_id="team",
            project_id="project",
        ).snapshot_for_root(WORKSTREAM)
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "exact_source_and_timestamps_required",
        ):
            prepare_generation_operator_contract(
                comments=deepcopy(self.client.comments), graph=graph,
                workstream_id=WORKSTREAM, authority=AUTHORITY,
                description_plan_revision=OLD,
                target_source={"identity": "target", "sha256": "not-a-digest"},
                created_at="",
                remote_head="e" * 40,
                started_state=STARTED_STATE,
            )

    def test_transport_invokes_operator_gate_before_activation_preview(self):
        project_full(self.client, NEW)

        def planted_gate():
            raise WorkstreamGenerationError("planted_operator_gate")

        self.transport.operator_validator = planted_gate
        writes = len(self.client.mutations)
        with self.assertRaisesRegex(
            WorkstreamGenerationError, "planted_operator_gate",
        ):
            self.transport.preview_activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(),
            )
        self.assertEqual(len(self.client.mutations), writes)

    def test_prepare_cli_is_zero_write_and_has_no_apply_flag(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest_path = f"{directory}/target-projection.json"
            arguments = [
                "prepare", WORKSTREAM, "--plan-source", "PLAN.md",
                "--created-at", "2026-08-31T23:00:00Z",
                "--remote-head", "e" * 40,
                "--started-state-id", STARTED_STATE["id"],
                "--manifest-output", manifest_path,
            ]
            args = parser().parse_args(arguments)
            self.assertEqual(args.command, "prepare")
            self.assertEqual(args.manifest_output, manifest_path)
            self.assertFalse(hasattr(args, "apply"))

            original_execute = self.client.execute

            def execute(query, variables):
                if "query WorkstreamGenerationPrepareState" in query:
                    return {
                        "team": {"id": "team", "organization": {"id": "workspace"}},
                        "workflowState": {
                            "id": STARTED_STATE["id"], "name": STARTED_STATE["name"],
                            "type": STARTED_STATE["type"], "team": {"id": "team"},
                        },
                    }
                return original_execute(query, variables)

            self.client.execute = execute
            stdout = io.StringIO()
            writes = len(self.client.mutations)
            with patch.object(sys, "argv", ["workstream_generation.py", *arguments]), \
                 patch("workstream_generation._route_and_client", return_value=(self.client, AUTHORITY)), \
                 patch("workstream_generation.plan_payload", return_value={"source": {
                     "identity": f"https://example.test/{NEW}", "sha256": NEW,
                 }}), patch.object(sys, "stdout", stdout):
                self.assertEqual(main(), 0)
            output = json.loads(stdout.getvalue())
            with open(manifest_path, encoding="utf-8") as handle:
                emitted_manifest = json.load(handle)
            self.assertEqual(emitted_manifest, output["projection_preview"]["manifest"])
            self.assertEqual(len(self.client.mutations), writes)
        with self.assertRaises(SystemExit):
            parser().parse_args([
                "prepare", WORKSTREAM, "--plan-source", "PLAN.md",
                "--created-at", "2026-08-31T23:00:00Z",
                "--remote-head", "e" * 40, "--apply",
                "--started-state-id", STARTED_STATE["id"],
            ])

    def test_prepare_stages_terminal_evidence_and_closure_without_copying_closure(self):
        from types import SimpleNamespace
        predecessor = adapter(self.client, OLD).state()
        base_events = deepcopy(list(predecessor.events))
        next(
            event for event in base_events
            if (event["kind"], event["key"]) == ("scope", "root")
        )["value"]["child_ownership"] = {"GEN-72": "github.com:id:R_repo"}
        predecessor_events = [*base_events, {
            "schema_version": 2, "kind": "evidence_contract", "key": "terminal",
            "event_id": "wsp_" + "7" * 32,
            "value": {
                "owning_child": "GEN-72", "plan_revision": OLD,
                "repository_key": "github.com:id:R_repo", "exact_head": "e" * 40,
            },
        }, {
            "schema_version": 2, "kind": "child_closure", "key": "GEN-72",
            "event_id": "wsp_" + "8" * 32,
            "value": {"child_identifier": "GEN-72", "plan_revision": OLD},
        }]
        predecessor_state = SimpleNamespace(
            revision=len(predecessor_events), events=predecessor_events,
            snapshot=deepcopy(predecessor.snapshot),
        )
        target_state = SimpleNamespace(
            revision=0, events=[], snapshot={"projection_history": predecessor_events},
        )
        self.client.children = [{"identifier": "GEN-72"}]
        readback = {
            "child_identifier": "GEN-72", "child_issue_id": "child-72",
            "assignee_id": "agent", "workspace_id": "workspace",
            "team_id": "team", "project_id": "project",
            "parent_issue_id": AUTHORITY["root_issue_id"],
        }
        binding = {
            "schema_version": 1, "plan_revision": OLD,
            "projection_revision": 6, "projection_events_sha256": "1" * 64,
            "projection_frontier_event_id": "frontier",
            "projection_frontier_sha256": "2" * 64,
            "projection_history_sha256": "3" * 64,
            "material_revision": 0, "material_events_sha256": "4" * 64,
            "checkpoint_event_id": None, "checkpoint_events_sha256": "5" * 64,
            "input_frontier_sha256": "6" * 64,
            "evidence_heads": [{
                "child_identifier": "GEN-72", "key": "terminal",
                "evidence_event_id": "wsp_" + "7" * 32,
                "evidence_value_sha256": "7" * 64,
                "closure_event_id": "wsp_" + "8" * 32,
                "closure_value_sha256": "8" * 64,
            }],
        }
        with patch(
            "workstream_generation.terminal_child_readback",
            return_value=readback,
        ), patch(
            "workstream_projection.terminal_child_readback",
            return_value=readback,
        ), patch(
            "workstream_projection.evidence_errors", return_value=[],
        ), patch(
            "workstream_generation.reduce_projection_comments",
            side_effect=lambda _comments, **kwargs: (
                predecessor_state
                if kwargs["expected_plan_revision"] == OLD else target_state
            ),
        ), patch(
            "workstream_projection.terminal_child_evidence_seed_predecessor_contract",
            return_value=(binding, {"terminal": {
                "evidence_event_id": "wsp_" + "7" * 32,
                "evidence_value_sha256": "7" * 64,
                "closure_event_id": "wsp_" + "8" * 32,
                "closure_value_sha256": "8" * 64,
            }}),
        ):
            prepared = prepare_generation_operator_contract(
                comments=deepcopy(self.client.comments),
                graph={"root": {}, "children": [{"identifier": "GEN-72"}]},
                workstream_id=WORKSTREAM, authority=AUTHORITY,
                description_plan_revision=OLD,
                target_source={
                    "identity": f"https://example.test/{NEW}", "sha256": NEW,
                },
                created_at="2026-08-31T23:00:00Z",
                remote_head="e" * 40,
                started_state=STARTED_STATE,
            )
        preview = prepared["projection_preview"]
        self.assertEqual(preview["terminal_child_stage"], {
            "state": "terminal_evidence_seed_required",
            "children": ["GEN-72"],
        })
        self.assertEqual(preview["phase"], "terminal_evidence_seed")
        manifest = preview["manifest"]
        self.assertEqual(
            manifest["terminal_child_evidence_seeds"][0]["evidence_keys"],
            ["terminal"],
        )
        self.assertEqual(
            manifest["terminal_child_evidence_seed_predecessor"], binding,
        )
        identities = {
            (item["kind"], item["key"]) for item in manifest["projection"]
        }
        self.assertIn(("evidence_contract", "terminal"), identities)
        self.assertNotIn(("child_closure", "GEN-72"), identities)
        closure_stage = next(
            item for item in preview["active_key_accounting"]["staged"]
            if item["kind"] == "child_closure"
        )
        self.assertEqual(closure_stage["phase"], "terminal_child_closure_repair")

    def test_prepare_continues_canonical_seed_through_activation_readiness(self):
        from types import SimpleNamespace
        predecessor_head = "b" * 40
        target_head = "e" * 40
        secondary_head = "7" * 40
        old_source = {
            "identity": (
                "https://github.com/acme/plans/blob/"
                + "1" * 40 + "/plan.md"
            ),
            "sha256": NEW,
        }
        target_source = {
            "identity": (
                "https://github.com/acme/plans/blob/"
                + "2" * 40 + "/plan.md"
            ),
            "sha256": NEW,
        }
        predecessor = adapter(self.client, OLD).state()
        base = deepcopy(list(predecessor.events))
        scope = next(
            event for event in base
            if (event["kind"], event["key"]) == ("scope", "root")
        )["value"]
        scope["child_ownership"] = {"GEN-72": "github.com:id:R_repo"}
        primary_repository = next(
            repository for repository in scope["repositories"]
            if repository["provider_repository_id"] == "R_repo"
        )
        primary_repository["exact_head"] = predecessor_head
        secondary_repository = deepcopy(primary_repository)
        secondary_repository.update({
            "slug": "github.com/generous-corp/pulp",
            "provider_repository_id": "R_pulp",
            "exact_head": secondary_head,
        })
        secondary_repository["identity_resolution"].update({
            "provider_repository_id": "R_pulp",
            "resolved_slug": "github.com/generous-corp/pulp",
        })
        secondary_repository["identity_resolution"]["evidence"][0].update({
            "provider_repository_id": "R_pulp",
            "resolved_slug": "github.com/generous-corp/pulp",
        })
        scope["repositories"].append(secondary_repository)
        scope["child_ownership"] = {"GEN-72": "github.com:id:R_pulp"}
        evidence_old = {
            "owning_child": "GEN-72", "plan_revision": OLD,
            "repository_key": "github.com:id:R_pulp",
            "exact_head": secondary_head,
        }
        predecessor_events = [*base, {
            "schema_version": 2, "kind": "evidence_contract", "key": "terminal",
            "event_id": "wsp_" + "7" * 32, "value": evidence_old,
        }, {
            "schema_version": 2, "kind": "child_closure", "key": "GEN-72",
            "event_id": "wsp_" + "8" * 32,
            "value": {"child_identifier": "GEN-72", "plan_revision": OLD},
        }, {
            "schema_version": 2, "kind": "choice", "key": "keep-me",
            "event_id": "wsp_" + "9" * 32, "value": {"decision": "preserve"},
        }]
        predecessor_state = SimpleNamespace(
            revision=len(predecessor_events), events=predecessor_events,
            snapshot={**deepcopy(predecessor.snapshot),
                      "projection_history": predecessor_events},
        )
        evidence_new = {**evidence_old, "plan_revision": NEW}
        common = [
            {**deepcopy(predecessor_events[0]), "plan_revision": NEW,
             "event_id": "wsp_" + "a" * 32},
            {**deepcopy(predecessor_events[1]), "plan_revision": NEW,
             "event_id": "wsp_" + "b" * 32,
             "value": old_source},
            {**deepcopy(predecessor_events[2]), "plan_revision": NEW,
             "event_id": "wsp_" + "c" * 32},
            {"schema_version": 2, "kind": "evidence_contract", "key": "terminal",
             "event_id": "wsp_" + "d" * 32, "value": evidence_new},
            {"schema_version": 2, "kind": "disposition", "key": "root",
             "event_id": "wsp_" + "e" * 32,
             "value": {"disposition": "attach", "remote_head": predecessor_head,
                       "recovered_from_checkpoint": None}},
        ]
        closure = {
            "schema_version": 2, "kind": "child_closure", "key": "GEN-72",
            "event_id": "wsp_" + "f" * 32,
            "value": {"child_identifier": "GEN-72", "plan_revision": NEW},
        }
        choice = {
            "schema_version": 2, "kind": "choice", "key": "keep-me",
            "event_id": "wsp_" + "1" * 32, "value": {"decision": "preserve"},
        }
        target_events = list(common)

        def state(events):
            return SimpleNamespace(
                revision=len(events), events=deepcopy(events),
                snapshot={"projection_history": [*predecessor_events, *events]},
            )

        readback = {
            "child_identifier": "GEN-72", "child_issue_id": "child-72",
            "assignee_id": "agent", "workspace_id": "workspace",
            "team_id": "team", "project_id": "project",
            "parent_issue_id": AUTHORITY["root_issue_id"],
        }
        binding = {
            "schema_version": 1, "plan_revision": OLD,
            "projection_revision": len(predecessor_events),
            "projection_events_sha256": "1" * 64,
            "projection_frontier_event_id": "frontier",
            "projection_frontier_sha256": "2" * 64,
            "projection_history_sha256": "3" * 64,
            "material_revision": 0, "material_events_sha256": "4" * 64,
            "checkpoint_event_id": None, "checkpoint_events_sha256": "5" * 64,
            "input_frontier_sha256": "6" * 64,
            "evidence_heads": [{
                "child_identifier": "GEN-72", "key": "terminal",
                "evidence_event_id": "wsp_" + "7" * 32,
                "evidence_value_sha256": "7" * 64,
                "closure_event_id": "wsp_" + "8" * 32,
                "closure_value_sha256": "8" * 64,
            }],
        }
        common_authority = {
            "schema_version": 1,
            "predecessor_plan_revision": binding["plan_revision"],
            "predecessor_projection_revision": binding["projection_revision"],
            "predecessor_projection_events_sha256": binding[
                "projection_events_sha256"
            ],
            "predecessor_projection_frontier_event_id": binding[
                "projection_frontier_event_id"
            ],
            "predecessor_projection_frontier_sha256": binding[
                "projection_frontier_sha256"
            ],
            "projection_history_sha256": binding["projection_history_sha256"],
            "material_revision": binding["material_revision"],
            "material_events_sha256": binding["material_events_sha256"],
            "checkpoint_event_id": binding["checkpoint_event_id"],
            "checkpoint_events_sha256": binding["checkpoint_events_sha256"],
            "input_frontier_sha256": binding["input_frontier_sha256"],
            "predecessor_evidence_event_id": "wsp_" + "7" * 32,
            "predecessor_evidence_value_sha256": "7" * 64,
            "predecessor_closure_event_id": "wsp_" + "8" * 32,
            "predecessor_closure_value_sha256": "8" * 64,
        }
        common[3]["value"]["predecessor_closure_authority"] = common_authority
        real_prepare_seed = workstream_projection.prepare_terminal_child_evidence_seeds

        def normalize_seed(manifest, _graph, target, **_kwargs):
            target_contract = workstream_projection.projection_review_contract(target)
            if len(target.events) == len(common):
                result = real_prepare_seed(manifest, _graph, target, **_kwargs)
            else:
                result = deepcopy(manifest)
                for item in result["projection"]:
                    if item["kind"] == "evidence_contract":
                        item["value"]["predecessor_closure_authority"] = (
                            deepcopy(common_authority)
                        )
                result.update(target_contract)
            desired_scope = next(
                item["value"] for item in result["projection"]
                if (item["kind"], item["key"]) == ("scope", "root")
            )
            desired_primary = next(
                repository for repository in desired_scope["repositories"]
                if repository["provider_repository_id"] == "R_repo"
            )
            current_scope_event = workstream_projection._active_heads(target).get(
                ("scope", "root")
            )
            if current_scope_event is not None:
                current_primary = next(
                    repository
                    for repository in current_scope_event["value"]["repositories"]
                    if repository["provider_repository_id"] == "R_repo"
                )
                if current_primary["exact_head"] != target_head:
                    transition = result[
                        "terminal_child_evidence_seed_head_transition"
                    ]
                    self.assertEqual(desired_primary["exact_head"], target_head)
                    self.assertEqual(transition["from_exact_head"], predecessor_head)
                    self.assertEqual(transition["to_exact_head"], target_head)
                    self.assertEqual(
                        transition["from_scope_event_id"],
                        current_scope_event["event_id"],
                    )
                    self.assertEqual(
                        transition["from_disposition_event_id"],
                        common[4]["event_id"],
                    )
                    self.assertEqual(
                        transition["from_disposition_value_sha256"],
                        workstream_projection.canonical_digest(common[4]["value"]),
                    )
                    self.assertEqual(
                        transition["disposition"]["remote_head"], target_head,
                    )
                    self.assertEqual(
                        result["expected_projection_revision"],
                        target_contract["expected_projection_revision"],
                    )
            return result

        def normalize_repair(manifest, _graph, _target):
            result = deepcopy(manifest)
            if not any(item["kind"] == "child_closure"
                       for item in result["projection"]):
                result["projection"].append({
                    "kind": "child_closure", "key": "GEN-72",
                    "value": deepcopy(closure["value"]),
                })
            return result

        def run(events):
            target = state(events)
            with patch(
                "workstream_generation.reduce_projection_comments",
                side_effect=lambda _comments, **kwargs: (
                    predecessor_state
                    if kwargs["expected_plan_revision"] == OLD else target
                ),
            ), patch(
                "workstream_generation.terminal_child_readback",
                return_value=readback,
            ), patch(
                "workstream_projection.terminal_child_readback",
                return_value=readback,
            ), patch(
                "workstream_projection.evidence_errors", return_value=[],
            ), patch(
                "workstream_projection.terminal_child_evidence_seed_predecessor_contract",
                return_value=(binding, {"terminal": {
                    "evidence_event_id": "wsp_" + "7" * 32,
                    "evidence_value_sha256": "7" * 64,
                    "closure_event_id": "wsp_" + "8" * 32,
                    "closure_value_sha256": "8" * 64,
                }}),
            ), patch(
                "workstream_projection.prepare_terminal_child_evidence_seeds",
                side_effect=normalize_seed,
            ), patch(
                "workstream_projection.prepare_terminal_child_repairs",
                side_effect=normalize_repair,
            ):
                return prepare_generation_operator_contract(
                    comments=deepcopy(self.client.comments),
                    graph={
                        "root": {},
                        "children": [{
                            "identifier": "GEN-72",
                            "status_type": "completed",
                        }],
                    },
                    workstream_id=WORKSTREAM, authority=AUTHORITY,
                    description_plan_revision=OLD,
                    target_source=target_source,
                    created_at="2026-08-31T23:00:00Z",
                    remote_head=target_head,
                    started_state=STARTED_STATE,
                )

        noncanonical_source_candidate = [*target_events, {
            **deepcopy(common[2]),
            "kind": "provenance", "key": "unreviewed",
            "event_id": "wsp_" + "9" * 32,
            "expected_revision": len(target_events),
            "value": {
                "agent": "forged", "machine": "forged",
                "session_id": "forged",
            },
        }]
        comments_before_refusal = len(self.client.comments)
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_prepare_noncanonical_target_prefix",
        ):
            run(noncanonical_source_candidate)
        self.assertEqual(len(self.client.comments), comments_before_refusal)

        without_disposition = target_events[:4]
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_prepare_target_disposition_missing_for_head_transition",
        ):
            run(without_disposition)
        unbound_progress = [*without_disposition, {
            **deepcopy(common[4]), "event_id": "wsp_" + "4" * 32,
            "value": {
                "disposition": "attach", "remote_head": target_head,
                "recovered_from_checkpoint": None,
            },
        }]
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_prepare_target_disposition_missing_for_head_transition",
        ):
            run(unbound_progress)

        seed_repair = run(target_events)
        self.assertEqual(
            seed_repair["projection_preview"]["phase"], "terminal_evidence_seed",
        )
        seed_manifest = seed_repair["projection_preview"]["manifest"]
        desired_scope = next(
            item["value"] for item in seed_manifest["projection"]
            if (item["kind"], item["key"]) == ("scope", "root")
        )
        self.assertEqual(
            next(
                repository for repository in desired_scope["repositories"]
                if repository["provider_repository_id"] == "R_pulp"
            ),
            secondary_repository,
        )
        self.assertEqual(
            next(
                item["value"] for item in seed_manifest["projection"]
                if item["kind"] == "evidence_contract"
            )["exact_head"],
            secondary_head,
        )
        target_events.append({
            **deepcopy(common[4]), "event_id": "wsp_" + "2" * 32,
            "supersedes_event_id": common[4]["event_id"],
            "value": deepcopy(
                seed_manifest[
                    "terminal_child_evidence_seed_head_transition"
                ]["disposition"]
            ),
        })
        target_events.append({
            **deepcopy(common[0]), "event_id": "wsp_" + "3" * 32,
            "value": desired_scope,
        })

        source_repair = run(target_events)
        self.assertEqual(
            source_repair["projection_preview"]["phase"],
            "terminal_source_transition",
        )
        self.assertEqual(
            source_repair["projection_preview"]["invocation"]["source"],
            target_source,
        )
        self.assertEqual(
            source_repair["projection_preview"]["manifest"]
            ["terminal_child_source_transition"],
            {
                "from_identity": old_source["identity"],
                "to_identity": target_source["identity"],
                "sha256": NEW,
                "created_at": "2026-08-31T23:00:00Z",
                "expected_revision": len(target_events),
                "from_event_id": next(
                    event["event_id"] for event in reversed(target_events)
                    if event["kind"] == "source"
                ),
                "from_value_sha256": (
                    workstream_projection.canonical_digest(old_source)
                ),
                "pending_children": [{
                    "child_identifier": "GEN-72",
                    "child_issue_id": "child-72",
                    "expected_child_readback_sha256": (
                        workstream_projection.canonical_digest(readback)
                    ),
                    "expected_assignee_id": "agent",
                }],
            },
        )
        target_events.append({
            **deepcopy(common[1]), "event_id": "wsp_" + "6" * 32,
            "value": target_source,
            "supersedes_event_id": common[1]["event_id"],
        })

        repair = run(target_events)
        self.assertEqual(
            repair["projection_preview"]["phase"], "terminal_closure_repair",
        )
        self.assertIn(
            "terminal_child_repairs",
            repair["projection_preview"]["manifest"],
        )
        target_events.append(closure)
        post = run(target_events)
        self.assertEqual(post["projection_preview"]["phase"], "complete_projection")
        self.assertNotIn(
            "terminal_child_repairs", post["projection_preview"]["manifest"],
        )
        self.assertIn(
            ("choice", "keep-me"), {
                (item["kind"], item["key"])
                for item in post["projection_preview"]["manifest"]["projection"]
            },
        )
        target_events.append(choice)
        ready = run(target_events)
        self.assertEqual(ready["projection_preview"]["phase"], "activation_ready")
        self.assertEqual(
            ready["projection_preview"]["next_gate"],
            "preview_generation_activation",
        )

    def native_root_sha(self, client=None):
        client = client or self.client
        snapshot = LinearGraphQLTransport(
            client, workspace_id="workspace", team_id="team", project_id="project",
        ).snapshot_for_root(WORKSTREAM)
        return native_root_activation_proof(
            snapshot, workstream_id=WORKSTREAM, issue_id=WORKSTREAM,
            authority=AUTHORITY,
        )["sha256"]

    def native_fenced_transport(self):
        linear = LinearGraphQLTransport(
            self.client, workspace_id="workspace", team_id="team",
            project_id="project",
        )
        return GenerationTransport(
            self.client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            authority=AUTHORITY, candidate_loader=self.loader,
            legacy_description_plan_revision=OLD,
            native_root_loader=lambda: linear.snapshot_for_root(WORKSTREAM),
        )

    def native_and_source_fenced_transport(self, source_state):
        linear = LinearGraphQLTransport(
            self.client, workspace_id="workspace", team_id="team",
            project_id="project",
        )
        return GenerationTransport(
            self.client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            authority=AUTHORITY, candidate_loader=self.loader,
            legacy_description_plan_revision=OLD,
            native_root_loader=lambda: linear.snapshot_for_root(WORKSTREAM),
            source_loader=lambda: deepcopy(source_state),
        )

    def test_terminal_reopen_response_loss_drives_schema6_activation_replays(self):
        with patch(__name__ + ".WORKSTREAM", "GEN-37"):
            client = FakeClient()
            client.allow_issue_update = True
            client.comment_updates_root = True
            # The bounded root query and full resume query legitimately have
            # different project selections. Native custody must replay the
            # exact full operator-snapshot shape that was originally bound.
            client.expanded_resume_project = True
            client.graph_state_id = "done-state"
            client.graph_status = "Done"
            client.graph_status_type = "completed"
            project_full(client, OLD)
            source = {
                "identity": f"https://example.test/{NEW}", "sha256": NEW,
            }
            created_at = "2026-09-01T12:00:00Z"

            def graph_for(current):
                return LinearGraphQLTransport(
                    current, workspace_id="workspace", team_id="team",
                    project_id="project",
                ).snapshot_for_root(
                    WORKSTREAM, include_description=True,
                    include_child_comments=True,
                )

            def prepare():
                return prepare_generation_operator_contract(
                    comments=deepcopy(client.comments), graph=graph_for(client),
                    workstream_id=WORKSTREAM, authority=AUTHORITY,
                    description_plan_revision=OLD, target_source=source,
                    created_at=created_at, remote_head="e" * 40,
                    started_state=STARTED_STATE,
                )

            contract = prepare()
            target = adapter(client, NEW)
            for phase in range(4):
                if contract["projection_preview"]["phase"] == "activation_ready":
                    break
                state = target.state()
                active = {
                    (event["kind"], event["key"]): event
                    for event in state.events
                }
                for index, item in enumerate(
                    contract["projection_preview"]["manifest"]["projection"]
                ):
                    prior = active.get((item["kind"], item["key"]))
                    if prior is not None and prior["value"] == item["value"]:
                        continue
                    target.append(build_projection_event(
                        workstream_id=WORKSTREAM, kind=item["kind"],
                        key=item["key"], value=deepcopy(item["value"]),
                        plan_revision=NEW,
                        expected_revision=target.state().revision,
                        created_at=f"target-{phase}-{index}",
                        supersedes_event_id=(
                            prior["event_id"] if prior is not None else None
                        ), authority=AUTHORITY,
                    ))
                if contract["projection_preview"]["phase"] == "complete_projection":
                    state = target.state()
                    prior = next((
                        event for event in reversed(state.events)
                        if (event["kind"], event["key"])
                        == ("disposition", "root")
                    ), None)
                    disposition = {
                        "disposition": "attach", "remote_head": "e" * 40,
                        "recovered_from_checkpoint": None,
                    }
                    if prior is None or prior["value"] != disposition:
                        target.append(build_projection_event(
                            workstream_id=WORKSTREAM, kind="disposition",
                            key="root", value=disposition,
                            plan_revision=NEW,
                            expected_revision=target.state().revision,
                            created_at=f"target-{phase}-disposition",
                            supersedes_event_id=(
                                prior["event_id"] if prior is not None else None
                            ), authority=AUTHORITY,
                        ))
                contract = prepare()
            self.assertEqual(
                contract["projection_preview"]["phase"], "activation_ready",
            )

            def root_operator(graph, comments):
                return validate_operator_contract(
                    contract, source=source, token=WORKSTREAM,
                    authority=AUTHORITY, comments=comments, graph=graph,
                    started_state=STARTED_STATE,
                    description_plan_revision=OLD,
                )

            root_transport = RootTransitionTransport(
                client, token=WORKSTREAM, authority=AUTHORITY,
                operator_validator=root_operator,
                operator_contract_sha256=_digest(contract),
            )
            root_preview = root_transport.preview(
                operation="reopen", target=STARTED_STATE["id"],
            )
            root_args = {
                "operation": "reopen", "target": STARTED_STATE["id"],
                "expected_snapshot_sha256": root_preview[
                    "expected_snapshot_sha256"
                ],
                "expected_frontier_sha256": root_preview[
                    "expected_frontier_sha256"
                ],
                "expected_intent_sha256": root_preview["intent_sha256"],
            }
            client.lose_issue_update_response_once = True
            with self.assertRaisesRegex(LinearTransportError, "response lost"):
                root_transport.apply(**root_args)
            root_replay = root_transport.apply(**root_args)
            self.assertEqual(client.issue_update_count, 1)
            root_receipt = root_replay["root_transition_recovery_receipt"]

            def operator_validation(current):
                return validate_activation_operator_contract(
                    contract, source=source, workstream_id=WORKSTREAM,
                    authority=AUTHORITY, comments=deepcopy(current.comments),
                    graph=graph_for(current),
                    description_plan_revision=OLD,
                    created_at=created_at, remote_head="e" * 40,
                )

            def operator_snapshot_validation(comments, graph):
                return validate_activation_operator_contract(
                    contract, source=source, workstream_id=WORKSTREAM,
                    authority=AUTHORITY, comments=deepcopy(comments),
                    graph=deepcopy(graph),
                    description_plan_revision=OLD,
                    created_at=created_at, remote_head="e" * 40,
                )

            validated = operator_validation(client)
            self.assertEqual(
                validated["root_transition_recovery_receipt"], root_receipt,
            )
            base = deepcopy(client)
            retirement = contract["retirement_proof"]

            def activation_transport(current, candidate_loader=None):
                native_transport = LinearGraphQLTransport(
                    current, workspace_id="workspace", team_id="team",
                    project_id="project",
                )
                return GenerationTransport(
                    current, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
                    authority=AUTHORITY,
                    candidate_loader=candidate_loader or Loader(current),
                    legacy_description_plan_revision=OLD,
                    # Match the production CLI's full operator-snapshot query
                    # shape. Durable reopen custody currently binds fields such
                    # as project.name that the bounded root query omits.
                    native_root_loader=lambda: (
                        workstream_generation._activation_native_root_snapshot(
                            native_transport, WORKSTREAM,
                        )
                    ),
                    source_loader=lambda: deepcopy(source),
                    operator_validator=lambda: operator_validation(current),
                    operator_snapshot_validator=operator_snapshot_validation,
                    operator_contract_sha256=_digest(contract),
                    operator_remote_head="e" * 40,
                )

            def reviewed_sha(current):
                return native_root_activation_proof(
                    graph_for(current), workstream_id=WORKSTREAM,
                    issue_id=WORKSTREAM, authority=AUTHORITY,
                )["sha256"]

            def retry_and_assert(current, expected_sha):
                recovered = activation_transport(current).activate(
                    target_plan_revision=NEW, created_at=created_at,
                    retirement=retirement,
                    remote_head="e" * 40,
                    expected_native_root_sha256=expected_sha,
                )
                replay_writes = len(current.mutations)
                replayed = activation_transport(current).activate(
                    target_plan_revision=NEW, created_at=created_at,
                    retirement=retirement,
                    remote_head="e" * 40,
                    expected_native_root_sha256=expected_sha,
                )
                self.assertEqual(replayed["event_id"], recovered["event_id"])
                self.assertEqual(len(current.mutations), replay_writes)
                self.assertEqual(current.issue_update_count, 1)
                ids = [item["id"] for item in current.comments]
                self.assertEqual(len(ids), len(set(ids)))
                reservations = reduce_generation_reservations(
                    current.comments, workstream_id=WORKSTREAM,
                    authenticated_route=AUTHORITY,
                )
                self.assertEqual(len(reservations), 1)
                self.assertEqual(reservations[0]["schema_version"], 6)
                self.assertEqual(
                    reservations[0]["root_transition_receipt_sha256"],
                    root_receipt["sha256"],
                )
                self.assertIsNone(reservations[0]["activation_checkpoint"])
                self.assertIsNone(reservations[0]["remote_head"])
                refused_writes = len(current.mutations)
                refused_comments = deepcopy(current.comments)
                with self.assertRaisesRegex(
                    WorkstreamGenerationError,
                    "generation_operator_remote_head_mismatch",
                ):
                    activation_transport(current).activate(
                        target_plan_revision=NEW, created_at=created_at,
                        retirement=retirement, remote_head="f" * 40,
                        expected_native_root_sha256=expected_sha,
                    )
                self.assertEqual(len(current.mutations), refused_writes)
                self.assertEqual(current.comments, refused_comments)
                return recovered

            checkpoint = self.activation_checkpoint()
            custody_only = deepcopy(base)
            custody_loader = ActivationCheckpointLoader(
                custody_only, checkpoint,
            )
            custody_transport = activation_transport(
                custody_only, custody_loader,
            )
            custody_sha = reviewed_sha(custody_only)
            append_custody = custody_transport._append_checkpoint_custody

            def crash_after_custody(value):
                append_custody(value)
                raise WorkstreamGenerationError(
                    "crash_after_checkpoint_custody"
                )

            custody_transport._append_checkpoint_custody = crash_after_custody
            with self.assertRaisesRegex(
                WorkstreamGenerationError, "crash_after_checkpoint_custody",
            ):
                custody_transport.activate(
                    target_plan_revision=NEW, created_at=created_at,
                    retirement=retirement,
                    activation_checkpoint=checkpoint,
                    remote_head="e" * 40,
                    expected_native_root_sha256=custody_sha,
                )
            custodies = reduce_generation_checkpoint_custodies(
                custody_only.comments, workstream_id=WORKSTREAM,
                authenticated_route=AUTHORITY,
            )
            self.assertEqual(len(custodies), 1)
            self.assertFalse(reduce_generation_reservations(
                custody_only.comments, workstream_id=WORKSTREAM,
                authenticated_route=AUTHORITY,
            ))
            self.assertFalse(any(
                event["kind"] == "disposition"
                and event["value"].get("recovered_from_checkpoint")
                == checkpoint["event_id"]
                for event in adapter(custody_only, NEW).state().events
            ))
            changed_custody = deepcopy(custody_only)
            changed_writes = len(changed_custody.mutations)
            with self.assertRaisesRegex(
                WorkstreamGenerationError,
                "generation_checkpoint_custody_native_root_mismatch",
            ):
                activation_transport(
                    changed_custody,
                    ActivationCheckpointLoader(changed_custody, checkpoint),
                ).activate(
                    target_plan_revision=NEW, created_at=created_at,
                    retirement=retirement, activation_checkpoint=checkpoint,
                    remote_head="e" * 40,
                    expected_native_root_sha256="9" * 64,
                )
            self.assertEqual(len(changed_custody.mutations), changed_writes)
            custody_result = activation_transport(
                custody_only,
                ActivationCheckpointLoader(custody_only, checkpoint),
            ).activate(
                target_plan_revision=NEW, created_at=created_at,
                retirement=retirement, activation_checkpoint=checkpoint,
                remote_head="e" * 40,
                expected_native_root_sha256=custody_sha,
            )
            self.assertEqual(custody_result["activated_plan_revision"], NEW)
            self.assertEqual(len(reduce_generation_checkpoint_custodies(
                custody_only.comments, workstream_id=WORKSTREAM,
                authenticated_route=AUTHORITY,
            )), 1)
            self.assertEqual(len(reduce_generation_reservations(
                custody_only.comments, workstream_id=WORKSTREAM,
                authenticated_route=AUTHORITY,
            )), 1)
            self.assertEqual(sum(
                event["kind"] == "disposition"
                and event["value"].get("recovered_from_checkpoint")
                == checkpoint["event_id"]
                for event in adapter(custody_only, NEW).state().events
            ), 1)
            self.assertEqual(custody_only.issue_update_count, 1)
            custody_ids = [item["id"] for item in custody_only.comments]
            self.assertEqual(len(custody_ids), len(set(custody_ids)))

            checkpoint_client = deepcopy(base)
            checkpoint_loader = ActivationCheckpointLoader(
                checkpoint_client, checkpoint,
            )
            checkpoint_transport = activation_transport(
                checkpoint_client, checkpoint_loader,
            )
            checkpoint_sha = reviewed_sha(checkpoint_client)
            checkpoint_transport._append_reservation = (
                lambda _value: (_ for _ in ()).throw(
                    WorkstreamGenerationError(
                        "crash_after_prospective_checkpoint_append"
                    )
                )
            )
            with self.assertRaisesRegex(
                WorkstreamGenerationError,
                "crash_after_prospective_checkpoint_append",
            ):
                checkpoint_transport.activate(
                    target_plan_revision=NEW, created_at=created_at,
                    retirement=retirement,
                    activation_checkpoint=checkpoint,
                    remote_head="e" * 40,
                    expected_native_root_sha256=checkpoint_sha,
                )
            self.assertFalse(reduce_generation_reservations(
                checkpoint_client.comments, workstream_id=WORKSTREAM,
                authenticated_route=AUTHORITY,
            ))
            self.assertEqual(sum(
                event["kind"] == "disposition"
                and event["value"].get("recovered_from_checkpoint")
                == checkpoint["event_id"]
                for event in adapter(checkpoint_client, NEW).state().events
            ), 1)
            substituted = deepcopy(checkpoint_client)
            substituted_writes = len(substituted.mutations)
            with self.assertRaisesRegex(
                WorkstreamGenerationError,
                "generation_checkpoint_custody_native_root_mismatch",
            ):
                activation_transport(
                    substituted,
                    ActivationCheckpointLoader(substituted, checkpoint),
                ).activate(
                    target_plan_revision=NEW, created_at=created_at,
                    retirement=retirement, activation_checkpoint=checkpoint,
                    remote_head="e" * 40,
                    expected_native_root_sha256="9" * 64,
                )
            self.assertEqual(len(substituted.mutations), substituted_writes)
            checkpoint_result = activation_transport(
                checkpoint_client,
                ActivationCheckpointLoader(checkpoint_client, checkpoint),
            ).activate(
                target_plan_revision=NEW, created_at=created_at,
                retirement=retirement, activation_checkpoint=checkpoint,
                remote_head="e" * 40,
                expected_native_root_sha256=checkpoint_sha,
            )
            self.assertEqual(checkpoint_result["activated_plan_revision"], NEW)
            self.assertEqual(checkpoint_client.issue_update_count, 1)
            checkpoint_reservations = reduce_generation_reservations(
                checkpoint_client.comments, workstream_id=WORKSTREAM,
                authenticated_route=AUTHORITY,
            )
            self.assertEqual(len(checkpoint_reservations), 1)
            self.assertEqual(
                checkpoint_reservations[0]["native_root_sha256"],
                checkpoint_sha,
            )
            self.assertEqual(sum(
                event["kind"] == "disposition"
                and event["value"].get("recovered_from_checkpoint")
                == checkpoint["event_id"]
                for event in adapter(checkpoint_client, NEW).state().events
            ), 1)

            # Response loss immediately after the reservation must reuse its
            # authenticated custody without a fresh terminal-state prepare.
            after_reservation = deepcopy(base)
            reservation_transport = activation_transport(after_reservation)
            reservation_sha = reviewed_sha(after_reservation)
            append_reservation = reservation_transport._append_reservation

            def crash_after_reservation(value):
                append_reservation(value)
                raise WorkstreamGenerationError("crash_after_v6_reservation")

            reservation_transport._append_reservation = crash_after_reservation
            with self.assertRaisesRegex(
                WorkstreamGenerationError, "crash_after_v6_reservation",
            ):
                reservation_transport.activate(
                    target_plan_revision=NEW, created_at=created_at,
                    retirement=retirement,
                    remote_head="e" * 40,
                    expected_native_root_sha256=reservation_sha,
                )
            for field, value in (
                ("graph_title", "unrelated root drift"),
                ("graph_status", "Blocked"),
            ):
                drifted = deepcopy(after_reservation)
                setattr(drifted, field, value)
                writes = len(drifted.mutations)
                with self.subTest(material_drift=field), self.assertRaisesRegex(
                    (WorkstreamGenerationError, RootTransitionError),
                    "generation_pending_native_root_material_mismatch|"
                    "generation_activation_requires_reviewed_in_progress_root|"
                    "root_transition_reopen_witness_mismatch",
                ):
                    activation_transport(drifted).activate(
                        target_plan_revision=NEW, created_at=created_at,
                        retirement=retirement,
                        remote_head="e" * 40,
                        expected_native_root_sha256=reservation_sha,
                    )
                self.assertEqual(len(drifted.mutations), writes)
                self.assertEqual(drifted.issue_update_count, 1)
            retry_and_assert(after_reservation, reservation_sha)

            for crash_call, error in (
                (2, "crash_after_v6_candidate_seal"),
                (4, "crash_after_v6_prepared_transition"),
            ):
                current = deepcopy(base)
                stable = Loader(current)
                calls = {"count": 0}

                def crashing_loader(plan, *, _at=crash_call, _error=error):
                    calls["count"] += 1
                    receipt = stable(plan)
                    if calls["count"] == _at:
                        raise WorkstreamGenerationError(_error)
                    return receipt

                transport = activation_transport(current, crashing_loader)
                expected_sha = reviewed_sha(current)
                with self.assertRaisesRegex(WorkstreamGenerationError, error):
                    transport.activate(
                        target_plan_revision=NEW, created_at=created_at,
                        retirement=retirement,
                        remote_head="e" * 40,
                        expected_native_root_sha256=expected_sha,
                    )
                retry_and_assert(current, expected_sha)

            after_finalization = deepcopy(base)
            final_transport = activation_transport(after_finalization)
            final_sha = reviewed_sha(after_finalization)
            append_finalization = final_transport._append_finalization

            def crash_after_finalization(**kwargs):
                append_finalization(**kwargs)
                raise WorkstreamGenerationError("crash_after_v6_finalization")

            final_transport._append_finalization = crash_after_finalization
            with self.assertRaisesRegex(
                WorkstreamGenerationError, "crash_after_v6_finalization",
            ):
                final_transport.activate(
                    target_plan_revision=NEW, created_at=created_at,
                    retirement=retirement,
                    remote_head="e" * 40,
                    expected_native_root_sha256=final_sha,
                )
            retry_and_assert(after_finalization, final_sha)

    def test_activation_accepts_exact_prepared_v2_retirement_and_refuses_tamper(self):
        target = adapter(self.client, NEW)
        revision = target.state().revision
        contract = self.generation_preparation()
        for phase in range(3):
            if contract["projection_preview"]["phase"] == "activation_ready":
                break
            current_values = {
                (event["kind"], event["key"]): event["value"]
                for event in target.state().events
            }
            for index, item in enumerate(
                contract["projection_preview"]["manifest"]["projection"]
            ):
                if current_values.get((item["kind"], item["key"])) == item["value"]:
                    continue
                target.append(build_projection_event(
                    workstream_id=WORKSTREAM, kind=item["kind"], key=item["key"],
                    value=deepcopy(item["value"]), plan_revision=NEW,
                    expected_revision=revision,
                    created_at=f"target-{phase}-{index}", authority=AUTHORITY,
                ))
                revision += 1
            if contract["projection_preview"]["phase"] == "complete_projection":
                disposition = {
                    "disposition": "attach", "remote_head": contract["remote_head"],
                    "recovered_from_checkpoint": None,
                }
                if current_values.get(("disposition", "root")) != disposition:
                    target.append(build_projection_event(
                        workstream_id=WORKSTREAM, kind="disposition", key="root",
                        value=disposition, plan_revision=NEW,
                        expected_revision=revision,
                        created_at=f"target-{phase}-disposition", authority=AUTHORITY,
                    ))
                    revision += 1
            contract = self.generation_preparation()
        self.assertEqual(contract["projection_preview"]["phase"], "activation_ready")
        retirement = contract["retirement_proof"]

        race = deepcopy(self.client)
        race_linear = LinearGraphQLTransport(
            race, workspace_id="workspace", team_id="team", project_id="project",
        )

        def validate_race_operator():
            graph = race_linear.snapshot_for_root(
                WORKSTREAM, include_description=True, include_child_comments=True,
            )
            observed = prepare_generation_operator_contract(
                comments=deepcopy(race.comments), graph=graph,
                workstream_id=WORKSTREAM, authority=AUTHORITY,
                description_plan_revision=OLD,
                target_source={
                    "identity": f"https://example.test/{NEW}", "sha256": NEW,
                }, created_at=contract["created_at"],
                remote_head=contract["remote_head"], started_state=STARTED_STATE,
            )
            return {"retirement_proof": observed["retirement_proof"]}

        injected = {"done": False}

        def drift_after_reservation(item, _client):
            if injected["done"] or "workstream-generation-reservation" not in item["body"]:
                return
            injected["done"] = True
            state = adapter(race, NEW).state()
            event = build_projection_event(
                workstream_id=WORKSTREAM, kind="choice", key="unexpected-drift",
                value={"event_id": "unexpected-drift", "decision": "late"},
                plan_revision=NEW, expected_revision=state.revision,
                created_at="late-drift", authority=AUTHORITY,
            )
            race.comments.append({
                "id": projection_slot_id(
                    WORKSTREAM, NEW, state.revision, AUTHORITY,
                ), "body": encode_projection_comment(event),
                "createdAt": "late-drift", "updatedAt": "late-drift",
            })

        race.before_each_create = drift_after_reservation
        race_transport = GenerationTransport(
            race, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            authority=AUTHORITY, candidate_loader=Loader(race),
            legacy_description_plan_revision=OLD,
            native_root_loader=lambda: race_linear.snapshot_for_root(WORKSTREAM),
            operator_validator=validate_race_operator,
            operator_contract_sha256=_digest(contract),
        )
        race_preview = race_transport.preview_activate(
            target_plan_revision=NEW, created_at=contract["created_at"],
            retirement=retirement,
        )
        with self.assertRaisesRegex(
            WorkstreamGenerationError, "generation_prepare_noncanonical_target_prefix",
        ):
            race_transport.activate(
                target_plan_revision=NEW, created_at=contract["created_at"],
                retirement=retirement,
                expected_native_root_sha256=(
                    race_preview["native_root_activation_proof"]["sha256"]
                ),
            )
        self.assertFalse(any(
            event["kind"] == "generation_candidate_seal"
            for event in adapter(race, NEW).state().events
        ))

        crash = deepcopy(self.client)
        crash_linear = LinearGraphQLTransport(
            crash, workspace_id="workspace", team_id="team", project_id="project",
        )
        stable_loader = Loader(crash)
        loader_calls = {"count": 0}

        def crash_after_seal(plan):
            loader_calls["count"] += 1
            result = stable_loader(plan)
            if loader_calls["count"] == 2:
                raise WorkstreamGenerationError("simulated_operator_crash_after_seal")
            return result

        def validate_crash_operator():
            graph = crash_linear.snapshot_for_root(
                WORKSTREAM, include_description=True, include_child_comments=True,
            )
            observed = prepare_generation_operator_contract(
                comments=deepcopy(crash.comments), graph=graph,
                workstream_id=WORKSTREAM, authority=AUTHORITY,
                description_plan_revision=OLD,
                target_source={
                    "identity": f"https://example.test/{NEW}", "sha256": NEW,
                }, created_at=contract["created_at"],
                remote_head=contract["remote_head"], started_state=STARTED_STATE,
            )
            self.assertEqual(observed, contract)
            return {"retirement_proof": observed["retirement_proof"]}

        crash_transport = GenerationTransport(
            crash, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            authority=AUTHORITY, candidate_loader=crash_after_seal,
            legacy_description_plan_revision=OLD,
            native_root_loader=lambda: crash_linear.snapshot_for_root(WORKSTREAM),
            operator_validator=validate_crash_operator,
            operator_contract_sha256=_digest(contract),
        )
        crash_native = native_root_activation_proof(
            crash_linear.snapshot_for_root(WORKSTREAM),
            workstream_id=WORKSTREAM, issue_id=WORKSTREAM, authority=AUTHORITY,
        )["sha256"]
        with self.assertRaisesRegex(
            WorkstreamGenerationError, "operator_crash_after_seal",
        ):
            crash_transport.activate(
                target_plan_revision=NEW, created_at=contract["created_at"],
                retirement=retirement,
                expected_native_root_sha256=crash_native,
            )
        seals_after_crash = sum(
            event["kind"] == "generation_candidate_seal"
            for event in adapter(crash, NEW).state().events
        )
        self.assertEqual(seals_after_crash, 1)
        crash_transport.candidate_loader = stable_loader

        def obsolete_prepare():
            raise WorkstreamGenerationError("fresh_prepare_must_not_gate_pending_seal")

        crash_transport.operator_validator = obsolete_prepare
        recovered = crash_transport.activate(
            target_plan_revision=NEW, created_at=contract["created_at"],
            retirement=retirement, expected_native_root_sha256=crash_native,
        )
        self.assertEqual(recovered["activated_plan_revision"], NEW)
        self.assertEqual(sum(
            event["kind"] == "generation_candidate_seal"
            for event in adapter(crash, NEW).state().events
        ), 1)

        post_transition = deepcopy(self.client)
        post_linear = LinearGraphQLTransport(
            post_transition, workspace_id="workspace", team_id="team",
            project_id="project",
        )
        post_stable_loader = Loader(post_transition)
        post_calls = {"count": 0}

        def crash_before_finalization(plan):
            post_calls["count"] += 1
            result = post_stable_loader(plan)
            if post_calls["count"] == 4:
                raise WorkstreamGenerationError("simulated_post_transition_crash")
            return result

        def validate_post_operator():
            graph = post_linear.snapshot_for_root(
                WORKSTREAM, include_description=True, include_child_comments=True,
            )
            observed = prepare_generation_operator_contract(
                comments=deepcopy(post_transition.comments), graph=graph,
                workstream_id=WORKSTREAM, authority=AUTHORITY,
                description_plan_revision=OLD,
                target_source={
                    "identity": f"https://example.test/{NEW}", "sha256": NEW,
                }, created_at=contract["created_at"],
                remote_head=contract["remote_head"], started_state=STARTED_STATE,
            )
            return {"retirement_proof": observed["retirement_proof"]}

        post_transport = GenerationTransport(
            post_transition, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            authority=AUTHORITY, candidate_loader=crash_before_finalization,
            legacy_description_plan_revision=OLD,
            native_root_loader=lambda: post_linear.snapshot_for_root(WORKSTREAM),
            operator_validator=validate_post_operator,
            operator_contract_sha256=_digest(contract),
        )
        post_native = native_root_activation_proof(
            post_linear.snapshot_for_root(WORKSTREAM),
            workstream_id=WORKSTREAM, issue_id=WORKSTREAM, authority=AUTHORITY,
        )["sha256"]
        with self.assertRaisesRegex(
            WorkstreamGenerationError, "post_transition_crash",
        ):
            post_transport.activate(
                target_plan_revision=NEW, created_at=contract["created_at"],
                retirement=retirement, expected_native_root_sha256=post_native,
            )
        self.assertEqual(len(pending_generation_reservations(
            post_transition.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        )), 1)
        writes_before_mismatch = len(post_transition.mutations)
        mismatched_post_transport = GenerationTransport(
            post_transition, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            authority=AUTHORITY, candidate_loader=post_stable_loader,
            legacy_description_plan_revision=OLD,
            native_root_loader=lambda: post_linear.snapshot_for_root(WORKSTREAM),
            operator_validator=obsolete_prepare,
            operator_contract_sha256="0" * 64,
        )
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_historical_operator_contract_mismatch",
        ):
            mismatched_post_transport.activate(
                target_plan_revision=NEW, created_at=contract["created_at"],
                retirement=retirement, expected_native_root_sha256=post_native,
            )
        self.assertEqual(len(post_transition.mutations), writes_before_mismatch)
        post_transport.candidate_loader = post_stable_loader
        post_transport.operator_validator = obsolete_prepare
        finalized = post_transport.activate(
            target_plan_revision=NEW, created_at=contract["created_at"],
            retirement=retirement, expected_native_root_sha256=post_native,
        )
        self.assertEqual(finalized["activated_plan_revision"], NEW)
        self.assertFalse(pending_generation_reservations(
            post_transition.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        ))

        cli_client = deepcopy(self.client)
        cli_client.graph_state_id = STARTED_STATE["id"]
        cli_linear = LinearGraphQLTransport(
            cli_client, workspace_id="workspace", team_id="team", project_id="project",
        )
        cli_stable_loader = Loader(cli_client)
        cli_calls = {"count": 0}

        def cli_crash_after_seal(plan):
            cli_calls["count"] += 1
            result = cli_stable_loader(plan)
            if cli_calls["count"] == 2:
                raise WorkstreamGenerationError("simulated_cli_crash_after_seal")
            return result

        cli_native = native_root_activation_proof(
            cli_linear.snapshot_for_root(WORKSTREAM),
            workstream_id=WORKSTREAM, issue_id=WORKSTREAM, authority=AUTHORITY,
        )["sha256"]
        with tempfile.NamedTemporaryFile("w", suffix=".json") as contract_file:
            json.dump(contract, contract_file)
            contract_file.flush()
            argv = [
                "workstream_generation.py", "activate", WORKSTREAM,
                "--plan-source", "plan", "--plan-identity", "plan",
                "--operator-contract", contract_file.name,
                "--created-at", contract["created_at"], "--apply",
                "--expected-native-root-sha256", cli_native,
            ]

            def invoke_cli(loader):
                stdout, stderr = io.StringIO(), io.StringIO()
                with patch.object(sys, "argv", argv), patch(
                    "workstream_generation._route_and_client",
                    return_value=(cli_client, AUTHORITY),
                ), patch("workstream_generation.plan_payload", return_value={
                    "source": {
                        "identity": f"https://example.test/{NEW}", "sha256": NEW,
                    },
                }), patch(
                    "workstream_generation.strict_candidate_loader",
                    return_value=loader,
                ), patch.object(sys, "stdout", stdout), patch.object(
                    sys, "stderr", stderr,
                ):
                    return main(), stdout.getvalue(), stderr.getvalue()

            code, _stdout, error = invoke_cli(cli_crash_after_seal)
            self.assertEqual(code, 2)
            self.assertIn("simulated_cli_crash_after_seal", error)
            seals = sum(
                event["kind"] == "generation_candidate_seal"
                for event in adapter(cli_client, NEW).state().events
            )
            self.assertEqual(seals, 1)
            code, stdout, error = invoke_cli(cli_stable_loader)
            self.assertEqual((code, error), (0, ""))
            self.assertEqual(json.loads(stdout)["activated_plan_revision"], NEW)
            self.assertEqual(sum(
                event["kind"] == "generation_candidate_seal"
                for event in adapter(cli_client, NEW).state().events
            ), 1)
            writes_before_mismatch = len(cli_client.mutations)
            mismatched = deepcopy(contract)
            mismatched["native_transition"]["target_state"]["name"] = "Different"
            contract_file.seek(0)
            contract_file.truncate()
            json.dump(mismatched, contract_file)
            contract_file.flush()
            code, _stdout, error = invoke_cli(cli_stable_loader)
            self.assertEqual(code, 2)
            self.assertIn(
                "generation_historical_operator_contract_mismatch", error,
            )
            self.assertEqual(len(cli_client.mutations), writes_before_mismatch)
            contract_file.seek(0)
            contract_file.truncate()
            json.dump(contract, contract_file)
            contract_file.flush()
            code, stdout, error = invoke_cli(cli_stable_loader)
            self.assertEqual((code, error), (0, ""))
            self.assertEqual(json.loads(stdout)["activated_plan_revision"], NEW)
            self.assertEqual(len(cli_client.mutations), writes_before_mismatch)

        transport = self.native_fenced_transport()
        for field, value in (
            ("ordering", "caller assertion"),
            ("schema_version", 1),
        ):
            tampered = deepcopy(retirement)
            if field == "schema_version":
                tampered[field] = value
            else:
                tampered["authenticated_quiescence"][field] = value
            writes = len(self.client.mutations)
            with self.assertRaisesRegex(
                (WorkstreamGenerationError, LinearProjectionError),
                "retirement|candidate_seal",
            ):
                transport.preview_activate(
                    target_plan_revision=NEW, created_at=contract["created_at"],
                    retirement=tampered,
                )
            self.assertEqual(len(self.client.mutations), writes)
        preview = transport.preview_activate(
            target_plan_revision=NEW, created_at=contract["created_at"],
            retirement=retirement,
        )
        receipt = transport.activate(
            target_plan_revision=NEW, created_at=contract["created_at"],
            retirement=retirement,
            expected_native_root_sha256=preview["native_root_activation_proof"]["sha256"],
        )
        self.assertEqual(receipt["activated_plan_revision"], NEW)
        writes = len(self.client.mutations)

        def stale_fresh_prepare():
            raise WorkstreamGenerationError("fresh_prepare_must_not_gate_exact_replay")

        transport.operator_validator = stale_fresh_prepare
        replay = transport.activate(
            target_plan_revision=NEW, created_at=contract["created_at"],
            retirement=retirement,
            expected_native_root_sha256=preview["native_root_activation_proof"]["sha256"],
        )
        self.assertEqual(replay["event_id"], receipt["event_id"])
        self.assertEqual(len(self.client.mutations), writes)

    def test_terminal_native_root_refuses_preview_and_apply_before_first_write(self):
        project_full(self.client, NEW)
        self.client.graph_status = "Done"
        transport = self.native_fenced_transport()
        writes = len(self.client.mutations)

        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_activation_requires_reviewed_nonterminal_root",
        ):
            transport.preview_activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(),
            )
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_activation_requires_reviewed_nonterminal_root",
        ):
            transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(),
                expected_native_root_sha256="0" * 64,
            )
        self.assertEqual(len(self.client.mutations), writes)
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], OLD)

    def test_todo_native_root_is_not_accepted_as_in_progress(self):
        project_full(self.client, NEW)
        self.client.graph_status = "Todo"
        self.client.graph_status_type = "unstarted"
        transport = self.native_fenced_transport()
        writes = len(self.client.mutations)
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_activation_requires_reviewed_in_progress_root",
        ):
            transport.preview_activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(),
            )
        self.assertEqual(len(self.client.mutations), writes)

    def test_bootstrap_terminal_native_cache_has_no_late_activation_gate(self):
        client = FakeClient()
        client.description = "Next action: Continue."
        client.graph_status = "Done"
        client.graph_status_type = "completed"
        project_full(client, OLD)
        transport = GenerationTransport(
            client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            authority=AUTHORITY, candidate_loader=Loader(client),
            legacy_description_plan_revision=None,
        )
        receipt = transport.bootstrap(
            target_plan_revision=OLD, created_at="bootstrap",
        )
        self.assertEqual(receipt["activated_plan_revision"], OLD)
        self.assertEqual(select_plan_generation(
            client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=None, authenticated_route=AUTHORITY,
        )["plan_revision"], OLD)

    def test_reviewed_native_root_proof_replays_idempotently(self):
        project_full(self.client, NEW)
        transport = self.native_fenced_transport()
        preview = transport.preview_activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
        )
        proof = preview["native_root_activation_proof"]
        first = transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
            expected_native_root_sha256=proof["sha256"],
        )
        writes = len(self.client.mutations)
        replay = transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
            expected_native_root_sha256=proof["sha256"],
        )

        self.assertFalse(first["replay"])
        self.assertTrue(replay["replay"])
        self.assertEqual(len(self.client.mutations), writes)
        self.assertEqual(replay["native_root_activation_proof"], proof)

    def test_native_status_race_leaves_recoverable_reservation_not_authority(self):
        project_full(self.client, NEW)
        transport = self.native_fenced_transport()
        proof = transport.preview_activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
        )["native_root_activation_proof"]

        def close_after_first_write(_item, client):
            client.before_each_create = None
            client.graph_status = "Done"

        self.client.before_each_create = close_after_first_write
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_activation_requires_reviewed_nonterminal_root",
        ):
            transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(),
                expected_native_root_sha256=proof["sha256"],
            )
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], OLD)
        pending = pending_generation_reservations(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        )
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["to_plan_revision"], NEW)
        self.assertEqual(pending[0]["native_root_sha256"], proof["sha256"])

    def test_native_status_race_after_preparation_keeps_old_authority(self):
        project_full(self.client, NEW)
        source = {"identity": f"https://example.test/{NEW}", "sha256": NEW}
        transport = self.native_and_source_fenced_transport(source)
        proof = transport.preview_activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
        )["native_root_activation_proof"]

        def close_when_transition_is_written(item, client):
            body = item.get("body") or ""
            if PROJECTION_PREFIX not in body:
                return
            match = PROJECTION_RE.findall(body)
            if len(match) == 1 and _decode_projection(match[0])["kind"] == \
                    "generation_transition":
                client.graph_status = "Done"
                client.graph_status_type = "completed"

        self.client.before_each_create = close_when_transition_is_written
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_activation_requires_reviewed_nonterminal_root",
        ):
            transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(),
                expected_native_root_sha256=proof["sha256"],
            )
        selected = select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )
        self.assertEqual(selected["plan_revision"], OLD)
        pending = pending_generation_reservations(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        )
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["source"], source)
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_abort_after_preparation_replay_required",
        ):
            transport.abort(
                reservation_id=pending[0]["reservation_id"],
                reservation_sha256=pending[0]["reservation_sha256"],
                reason="writer stopped", created_at="later",
            )
        self.client.before_each_create = None
        self.client.graph_status = "In Progress"
        self.client.graph_status_type = "started"
        replay = transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
            expected_native_root_sha256=proof["sha256"],
        )
        self.assertTrue(replay["replay"])
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], NEW)
        self.assertEqual(pending_generation_reservations(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        ), [])

    def test_canonical_source_changes_at_first_write_never_activates(self):
        project_full(self.client, NEW)
        source = {"identity": f"https://example.test/{NEW}", "sha256": NEW}
        transport = self.native_and_source_fenced_transport(source)
        proof = transport.preview_activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
        )["native_root_activation_proof"]

        def drift_source_at_first_write(_item, _client):
            self.client.before_each_create = None
            source["sha256"] = OTHER

        self.client.before_each_create = drift_source_at_first_write
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_canonical_source_changed_during_activation",
        ):
            transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(),
                expected_native_root_sha256=proof["sha256"],
            )
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], OLD)

    def test_canonical_source_changes_during_finalization_is_not_success(self):
        project_full(self.client, NEW)
        source = {"identity": f"https://example.test/{NEW}", "sha256": NEW}
        transport = self.native_and_source_fenced_transport(source)
        proof = transport.preview_activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
        )["native_root_activation_proof"]

        def drift_source_at_finalization(item, _client):
            if "workstream-generation-finalization:v1" in (item.get("body") or ""):
                source["sha256"] = OTHER

        self.client.before_each_create = drift_source_at_finalization
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_canonical_source_changed_during_activation",
        ):
            transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(),
                expected_native_root_sha256=proof["sha256"],
            )
        # The finalization is immutable, but stale canonical bytes can never be
        # reported as success; ordinary resume observes this exact digest drift.
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], NEW)

    def test_schema_v4_finalization_lost_response_replays_without_writes(self):
        project_full(self.client, NEW)
        source = {"identity": f"https://example.test/{NEW}", "sha256": NEW}
        transport = self.native_and_source_fenced_transport(source)
        proof = transport.preview_activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
        )["native_root_activation_proof"]
        self.client.commit_then_fail_at.add(len(self.client.mutations) + 4)
        first = transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
            expected_native_root_sha256=proof["sha256"],
        )
        writes = len(self.client.mutations)
        replay = transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
            expected_native_root_sha256=proof["sha256"],
        )
        self.assertFalse(first["replay"])
        self.assertTrue(replay["replay"])
        self.assertEqual(len(self.client.mutations), writes)
        self.assertEqual(
            selected_generation_execution_status(
                self.client.comments, workstream_id=WORKSTREAM,
                transition_event_id=replay["event_id"],
                authenticated_route=AUTHORITY,
            )["name"],
            "In Progress",
        )

    def test_generation_local_status_prevents_native_done_after_final_fence(self):
        project_full(self.client, NEW)
        source = {"identity": f"https://example.test/{NEW}", "sha256": NEW}
        transport = self.native_and_source_fenced_transport(source)
        proof = transport.preview_activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
        )["native_root_activation_proof"]

        def close_during_finalization(item, client):
            if "workstream-generation-finalization:v1" in (item.get("body") or ""):
                client.graph_status = "Done"
                client.graph_status_type = "completed"

        self.client.before_each_create = close_during_finalization
        receipt = transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
            expected_native_root_sha256=proof["sha256"],
        )
        status = selected_generation_execution_status(
            self.client.comments, workstream_id=WORKSTREAM,
            transition_event_id=receipt["event_id"],
            authenticated_route=AUTHORITY,
        )
        self.assertEqual(status, {
            "authority": "generation_local", "name": "In Progress",
            "type": "started",
        })
        self.assertEqual(self.client.graph_status, "Done")

    def test_forged_finalization_binding_is_a_planted_contradiction(self):
        project_full(self.client, NEW)
        source = {"identity": f"https://example.test/{NEW}", "sha256": NEW}
        transport = self.native_and_source_fenced_transport(source)
        proof = transport.preview_activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
        )["native_root_activation_proof"]
        receipt = transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
            expected_native_root_sha256=proof["sha256"],
        )
        forged = deepcopy(receipt["two_phase_finalization"])
        forged.pop("remote_id", None)
        forged["source"]["sha256"] = OTHER
        unsigned = {key: deepcopy(value) for key, value in forged.items()
                    if key != "finalization_id"}
        forged["finalization_id"] = "wsgf_" + _digest(unsigned)[:32]
        self.client.comments.append({
            "id": generation_finalization_slot_id(forged),
            "body": encode_generation_finalization(forged),
            "createdAt": "2026-08-31T00:00:00Z",
            "updatedAt": "2026-08-31T00:00:00Z",
        })
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_finalization_binding_mismatch",
        ):
            reduce_generation_finalizations(
                self.client.comments, workstream_id=WORKSTREAM,
                authenticated_route=AUTHORITY,
            )

    def activate(self, target=NEW, predecessor=OLD, epoch=0):
        if not adapter(self.client, target).state().events:
            project_full(self.client, target)
        return self.transport.activate(
            target_plan_revision=target, created_at="now",
            retirement=self.retirement(predecessor, epoch),
        )

    def activation_checkpoint(self, **changes):
        values = {
            "workstream_id": WORKSTREAM, "boundary_id": "activate-new",
            "root_revision": 0, "plan_revision": NEW,
            "before_status": "In Progress", "after_status": "In Progress",
            "execution": {
                "agent": "codex", "provider": "openai", "session_id": "new",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/tmp/new", "branch": "new",
                    "head": "e" * 40,
                },
            },
            "exact_head": "e" * 40, "evidence": [], "blocker": None,
            "next_action": "Continue the activated target generation.",
        }
        values.update(changes)
        return build_checkpoint(**values)

    def test_child_extension_selection_preserves_legacy_description_authority(self):
        selected = adapter(self.client, OLD).select_child_extension_generation(
            description_plan_revision=OLD,
            source={
                "identity": f"https://example.test/{OLD}", "sha256": OLD,
            },
        )

        self.assertEqual(selected["plan_revision"], OLD)
        self.assertEqual(selected["authority_origin"], "legacy_description")
        self.assertEqual(selected["authority"], AUTHORITY)

    def test_child_extension_selection_uses_active_generation_not_description(self):
        self.activate()
        selected = adapter(self.client, NEW).select_child_extension_generation(
            description_plan_revision=OLD,
            source={
                "identity": f"https://example.test/{NEW}", "sha256": NEW,
            },
        )

        self.assertEqual(selected["plan_revision"], NEW)
        self.assertEqual(selected["description_plan_revision"], OLD)
        self.assertEqual(selected["authority_origin"], "generation_transition")

    def test_child_extension_selection_refuses_inactive_or_wrong_source(self):
        self.activate()
        with self.assertRaisesRegex(
            LinearProjectionError, "plan_generation_not_selected",
        ):
            adapter(self.client, OLD).select_child_extension_generation(
                description_plan_revision=OLD,
                source={
                    "identity": f"https://example.test/{OLD}", "sha256": OLD,
                },
            )
        with self.assertRaisesRegex(
            LinearProjectionError, "generation_source_mismatch",
        ):
            adapter(self.client, NEW).select_child_extension_generation(
                description_plan_revision=OLD,
                source={"identity": "https://wrong.test/plan", "sha256": NEW},
            )

    def test_active_generation_child_authorization_replays_exactly(self):
        self.activate()
        target = adapter(self.client, NEW)
        source = {
            "identity": f"https://example.test/{NEW}", "sha256": NEW,
        }
        material = reduce_event_comments(
            self.client.comments, workstream_id=WORKSTREAM,
        ).revision
        generation_authority = target.select_child_extension_generation(
            description_plan_revision=OLD, source=source,
        )
        first = target.reserve_child_extension(
            source=source, reviewed_candidate_key="new-child",
            child_issue_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            expected_material_revision=material,
            expected_projection_revision=target.state().revision,
            native_initialization={
                "state_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "assignee_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            },
            generation_authority=generation_authority,
            native_validation_sha256="0" * 64,
            child_content=CHILD_CONTENT,
        )
        mutation_count = len(self.client.mutations)

        second = target.reserve_child_extension(
            source=source, reviewed_candidate_key="new-child",
            child_issue_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            expected_material_revision=material,
            expected_projection_revision=target.state().revision,
            native_initialization={
                "state_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "assignee_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            },
            generation_authority=generation_authority,
            native_validation_sha256="0" * 64,
            child_content=CHILD_CONTENT,
        )

        self.assertEqual(first["event"], second["event"])
        self.assertEqual(first["disposition"], "created")
        self.assertEqual(second["disposition"], "existing")
        self.assertEqual(len(self.client.mutations), mutation_count)

    def test_retired_generation_exact_child_grant_remains_authoritative(self):
        self.activate()
        target = adapter(self.client, NEW)
        source = {
            "identity": f"https://example.test/{NEW}", "sha256": NEW,
        }
        native = {"state_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "assignee_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}
        authority = target.select_child_extension_generation(
            description_plan_revision=OLD, source=source,
        )
        material = reduce_event_comments(
            self.client.comments, workstream_id=WORKSTREAM,
        ).revision
        grant = target.reserve_child_extension(
            source=source, reviewed_candidate_key="new-child",
            child_issue_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            expected_material_revision=material,
            expected_projection_revision=target.state().revision,
            native_initialization=native, generation_authority=authority,
            native_validation_sha256="0" * 64,
            child_content=CHILD_CONTENT,
        )
        self.activate(target=LATER, predecessor=NEW, epoch=1)
        mutation_count = len(self.client.mutations)

        replay = target.reserve_child_extension(
            source=source, reviewed_candidate_key="new-child",
            child_issue_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            expected_material_revision=material,
            expected_projection_revision=target.state().revision,
            native_initialization=native, generation_authority=authority,
            native_validation_sha256="0" * 64,
            child_content=CHILD_CONTENT,
        )
        target.assert_child_extension_authorized(grant["event"])

        self.assertEqual(replay["event"], grant["event"])
        self.assertEqual(replay["disposition"], "existing")
        self.assertEqual(len(self.client.mutations), mutation_count)
        with self.assertRaisesRegex(
            LinearProjectionError, "plan_generation_not_selected",
        ):
            target.reserve_child_extension(
                source=source, reviewed_candidate_key="different-child",
                child_issue_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                expected_material_revision=material,
                expected_projection_revision=target.state().revision,
                native_initialization=native, generation_authority=authority,
                native_validation_sha256="0" * 64,
                child_content=CHILD_CONTENT,
            )

    def test_preactivation_old_grant_cannot_be_laundered_after_activation(self):
        project_full(self.client, NEW)
        target = adapter(self.client, NEW)
        source = {
            "identity": f"https://example.test/{NEW}", "sha256": NEW,
        }
        child_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        old_value = {
            "root_issue_id": AUTHORITY["root_issue_id"],
            "route": AUTHORITY, "source": source, "plan_revision": NEW,
            "reviewed_candidate_key": "new-child", "child_issue_id": child_id,
            "expected_material_revision": 0,
            "expected_projection_revision": target.state().revision,
            "initial_state": "planned_pending_projection",
        }
        target.append(build_projection_event(
            workstream_id=WORKSTREAM, kind="child_extension_authorization",
            key=child_id, value=old_value, plan_revision=NEW,
            expected_revision=target.state().revision, created_at="before-activation",
            authority=AUTHORITY,
        ))
        self.activate()
        authority = target.select_child_extension_generation(
            description_plan_revision=OLD, source=source,
        )

        with self.assertRaisesRegex(
            LinearProjectionError,
            "legacy_authorization_requires_existing_child",
        ):
            target.reserve_child_extension(
                source=source, reviewed_candidate_key="new-child",
                child_issue_id=child_id, expected_material_revision=0,
                expected_projection_revision=target.state().revision,
                native_initialization={
                    "state_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "assignee_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                },
                generation_authority=authority,
                native_validation_sha256="0" * 64,
                child_content=CHILD_CONTENT,
            )

    def test_child_create_linearizes_before_planted_later_generation(self):
        self.activate()
        target = adapter(self.client, NEW)
        plan = {
            "graph_review_required": True,
            "source": {
                "identity": f"https://example.test/{NEW}",
                "sha256": NEW, "bytes": 10,
            },
            "root": {"plan_revision": NEW},
            "children": [{
                "key": "new-child", "title": "Ready child",
                "next_action": "Write the child-local checkpoint.",
                "description": "**Ready child.** Write the child-local checkpoint.",
                "content_schema_version": 1,
            }],
        }
        material = reduce_event_comments(
            self.client.comments, workstream_id=WORKSTREAM,
        ).revision
        projection = target.state().revision
        self.client.before_issue_create = lambda: self.activate(
            target=LATER, predecessor=NEW, epoch=1,
        )
        transport = LinearGraphQLTransport(
            self.client, workspace_id="workspace", team_id="team",
            project_id="project",
        )

        created = transport.extend_existing_root_reviewed_child(
            plan, root_issue_id=AUTHORITY["root_issue_id"],
            reviewed_candidate_key="new-child", source_revision=NEW,
            plan_revision=NEW, expected_frontier={
                "material_revision": material,
                "projection_revision": projection,
            }, state_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", assignee_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            unassigned=False, authorization_adapter=target,
        )

        self.assertEqual(created["receipt"]["disposition"], "created")
        self.assertEqual(created["receipt"]["state_id"], "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
        self.assertEqual(created["receipt"]["assignee_id"], "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], LATER)
        self.assertFalse(any("issueUpdate" in str(item) for item in self.client.mutations))

        mutation_count = len(self.client.mutations)
        replay = transport.extend_existing_root_reviewed_child(
            plan, root_issue_id=AUTHORITY["root_issue_id"],
            reviewed_candidate_key="new-child", source_revision=NEW,
            plan_revision=NEW, expected_frontier={
                "material_revision": material,
                "projection_revision": target.state().revision,
            }, state_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", assignee_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            unassigned=False, authorization_adapter=target,
        )
        self.assertEqual(replay["receipt"]["disposition"], "existing")
        self.assertEqual(len(self.client.mutations), mutation_count)

    def test_activation_checkpoint_is_inert_until_transition_and_replays(self):
        project_full(self.client, NEW)
        checkpoint = self.activation_checkpoint()
        self.loader = ActivationCheckpointLoader(self.client, checkpoint)
        self.transport.candidate_loader = self.loader
        receipt = self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(), activation_checkpoint=checkpoint,
            remote_head="e" * 40,
        )
        transition = adapter(self.client, OLD).state().events[-1]
        self.assertEqual(transition["kind"], "generation_transition")
        self.assertEqual(transition["value"]["schema_version"], 3)
        self.assertEqual(
            transition["value"]["activation_checkpoint"]["event_id"],
            checkpoint["event_id"],
        )
        selected = select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )
        carried = selected_activation_checkpoints(
            self.client.comments, workstream_id=WORKSTREAM,
            transition_event_id=selected["transition_tip_event_id"],
            active_plan_revision=NEW, authenticated_route=AUTHORITY,
        )
        recovered = reduce_checkpoint_comments(
            self.client.comments, workstream_id=WORKSTREAM,
            selected_activation_checkpoints=carried,
        )
        self.assertIn(checkpoint["event_id"], {
            item["event_id"] for item in recovered.checkpoints
        })
        target = adapter(self.client, NEW).state()
        self.assertEqual(target.snapshot["disposition"], {
            "disposition": "attach", "remote_head": "e" * 40,
            "recovered_from_checkpoint": checkpoint["event_id"],
        })
        count = len(self.client.mutations)
        replay = self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(), activation_checkpoint=checkpoint,
            remote_head="e" * 40,
        )
        self.assertEqual(replay["event_id"], receipt["event_id"])
        self.assertEqual(len(self.client.mutations), count)

    def test_activation_preview_and_apply_reject_historical_provenance_frontier(self):
        project_full(self.client, NEW)
        predecessor = adapter(self.client, OLD)
        state = predecessor.state()
        historical = next(
            event for event in state.events
            if event["kind"] == "provenance" and event["key"] == "generation"
        )
        replacement = build_projection_event(
            workstream_id=WORKSTREAM, kind="provenance", key="generation",
            value={
                "agent": "codex", "machine": "M3", "session_id": "current",
                "worktree": {"state": "safe", "head": "e" * 40},
            },
            plan_revision=OLD, expected_revision=state.revision,
            created_at="replacement",
            supersedes_event_id=historical["event_id"], authority=AUTHORITY,
        )
        predecessor.append(replacement)
        current = predecessor.state().events[-1]
        for provenance_ids in (
            [historical["event_id"]],
            sorted([historical["event_id"], current["event_id"]]),
        ):
            with self.subTest(provenance_ids=provenance_ids):
                stale = build_retirement_proof(
                    predecessor_plan_revision=OLD, retired_at="now",
                    retired_writer_epoch=0,
                    provenance_event_ids=provenance_ids,
                    checkpoint_event_ids=[],
                )
                writes = len(self.client.mutations)
                with self.assertRaisesRegex(
                    WorkstreamGenerationError,
                    "generation_retirement_frontier_mismatch",
                ):
                    self.transport.preview_activate(
                        target_plan_revision=NEW, created_at="now",
                        retirement=stale,
                    )
                with self.assertRaisesRegex(
                    WorkstreamGenerationError,
                    "generation_retirement_frontier_mismatch",
                ):
                    self.transport.activate(
                        target_plan_revision=NEW, created_at="now",
                        retirement=stale,
                    )
                self.assertEqual(len(self.client.mutations), writes)

    def test_activation_retirement_ignores_child_checkpoint_but_refuses_contamination(self):
        project_full(self.client, NEW)
        child_checkpoint = build_checkpoint(
            workstream_id="GEN-38", boundary_id="child-only",
            root_revision=0, plan_revision=OLD,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "child", "machine": "M5",
                "worktree": {"state": "unknown"},
            }, exact_head=None, evidence=[], blocker=None,
            next_action="Continue child work.",
        )
        # This checkpoint belongs to a child-local comment collection, not the
        # root stream used to derive generation retirement authority.
        child_comments = [{
            "id": "child-checkpoint",
            "body": encode_checkpoint_comment(child_checkpoint),
        }]
        self.assertEqual(
            reduce_checkpoint_comments(
                child_comments, workstream_id="GEN-38",
            ).checkpoints[0]["event_id"],
            child_checkpoint["event_id"],
        )
        valid = self.retirement()
        writes = len(self.client.mutations)
        preview = self.transport.preview_activate(
            target_plan_revision=NEW, created_at="now", retirement=valid,
        )
        self.assertEqual(preview["retirement_frontier"]["checkpoint_event_ids"], [])
        self.assertEqual(len(self.client.mutations), writes)

        contaminated = build_retirement_proof(
            predecessor_plan_revision=OLD, retired_at="now",
            retired_writer_epoch=0,
            provenance_event_ids=valid["provenance_event_ids"],
            checkpoint_event_ids=[child_checkpoint["event_id"]],
        )
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_retirement_frontier_mismatch",
        ):
            self.transport.preview_activate(
                target_plan_revision=NEW, created_at="now",
                retirement=contaminated,
            )
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_retirement_frontier_mismatch",
        ):
            self.transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=contaminated,
            )
        self.assertEqual(len(self.client.mutations), writes)

    def test_activation_checkpoint_preview_simulates_exact_apply_candidate(self):
        project_full(self.client, NEW)
        checkpoint = self.activation_checkpoint()
        retirement = self.retirement()
        with patch("workstream_generation.plan_payload", return_value={
            "source": {
                "identity": f"https://example.test/{NEW}", "sha256": NEW,
            },
        }):
            strict = strict_candidate_loader(
                self.client, token=WORKSTREAM, authority=AUTHORITY,
                plan_source="plan", plan_identity=None,
                activation_checkpoint=checkpoint,
                activation_remote_head="e" * 40,
                activation_created_at="checkpoint-preview",
            )
            observed = []

            def recording_loader(plan):
                receipt = strict(plan)
                observed.append(deepcopy(receipt))
                return receipt

            transport = GenerationTransport(
                self.client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
                authority=AUTHORITY, candidate_loader=recording_loader,
                legacy_description_plan_revision=OLD,
            )
            writes = len(self.client.mutations)
            preview = transport.preview_activate(
                target_plan_revision=NEW, created_at="checkpoint-preview",
                retirement=retirement, activation_checkpoint=checkpoint,
                remote_head="e" * 40,
            )
            self.assertEqual(len(self.client.mutations), writes)
            self.assertEqual(preview["candidate"], observed[-1])
            self.assertEqual(preview["prospective_target_disposition"], {
                "disposition": "attach", "remote_head": "e" * 40,
                "recovered_from_checkpoint": checkpoint["event_id"],
            })
            prospective = preview["prospective_target_disposition_event"]
            self.assertEqual(prospective["expected_revision"], 4)
            observed.clear()
            transport.activate(
                target_plan_revision=NEW, created_at="checkpoint-preview",
                retirement=retirement, activation_checkpoint=checkpoint,
                remote_head="e" * 40,
            )

        self.assertEqual(observed[0], preview["candidate"])
        actual = next(
            event for event in adapter(self.client, NEW).state().events
            if event["event_id"] == prospective["event_id"]
        )
        self.assertEqual(actual, prospective)

    def test_production_shaped_stale_child_compaction_preserves_preview_apply_parity(self):
        project_full(self.client, NEW)
        target = adapter(self.client, NEW)
        state = target.state()
        prior_scope = next(
            event for event in state.events
            if event["kind"] == "scope" and event["key"] == "root"
        )
        owned_scope = deepcopy(prior_scope["value"])
        owned_scope["child_ownership"]["GEN-43"] = "github.com:id:R_repo"
        target.append(build_projection_event(
            workstream_id=WORKSTREAM, kind="scope", key="root",
            value=owned_scope, plan_revision=NEW,
            expected_revision=state.revision, created_at="owned-scope",
            supersedes_event_id=prior_scope["event_id"], authority=AUTHORITY,
        ))
        checkpoint = self.activation_checkpoint(root_revision=0)
        kinds = ("blocker", "decision", "decision_required", "followup", "requirement")
        child_events = [
            Delta(
                f"production-obligation-{index}", "GEN-43",
                kinds[index % len(kinds)], "agent",
                {kinds[index % len(kinds)]: f"obligation {index}: " + "x" * 420},
                index, f"2026-08-29T19:{index:02d}:00Z",
            )
            for index in range(30)
        ]
        stale_checkpoint = build_checkpoint(
            workstream_id="GEN-43", boundary_id="predecessor-child",
            root_revision=len(child_events), plan_revision=OLD,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "old",
                "machine": "M3", "worktree": {"state": "unavailable"},
            }, exact_head=None, evidence=[], blocker=None,
            next_action="Historical predecessor action.",
        )
        graph = {
            "root": {
                "id": AUTHORITY["root_issue_id"], "identifier": WORKSTREAM,
                "description": f"Plan revision: {OLD}",
                "url": "https://linear.test/GEN-37", "plan_revision": OLD,
                "revision": 0, "status": "In Progress",
                "next_action": "Activate the reviewed generation.",
                "team": {"id": AUTHORITY["team_id"], "organization": {
                    "id": AUTHORITY["workspace_id"],
                }},
                "project": {"id": AUTHORITY["project_id"]},
            },
            "children": [{
                "id": "44444444-4444-4444-8444-444444444444",
                "identifier": "GEN-43", "url": "https://linear.test/GEN-43",
                "title": "Physical successor canary", "status": "In Progress",
                "status_type": "started", "state_id": "started",
                "next_action": "Run the current physical canary.",
                "parent": {"id": AUTHORITY["root_issue_id"],
                           "identifier": WORKSTREAM},
                "team": {"id": AUTHORITY["team_id"], "organization": {
                    "id": AUTHORITY["workspace_id"],
                }},
                "project": {"id": AUTHORITY["project_id"]},
            }],
            "decisions": [],
            "child_comments": {"GEN-43": [
                *[
                    {"id": f"child-event-{index}",
                     "body": encode_event_comment(event)}
                    for index, event in enumerate(child_events)
                ],
                {"id": "stale-checkpoint",
                 "body": encode_checkpoint_comment(stale_checkpoint)},
            ]},
        }
        snapshot_transport = MagicMock()
        snapshot_transport.snapshot_for_root.side_effect = (
            lambda *_args, **_kwargs: deepcopy(graph)
        )
        retirement = self.retirement()
        contexts = []

        def capture_compact(snapshot, token, **kwargs):
            result = real_compact_context(snapshot, token, **kwargs)
            contexts.append(deepcopy(result))
            # Generation receipts must use the already-validated joined state,
            # not fields that a bounded presentation envelope may omit.
            return {"resume_authority": result["resume_authority"]}

        with patch("workstream_generation.plan_payload", return_value={
            "source": {
                "identity": f"https://example.test/{NEW}", "sha256": NEW,
            },
        }), patch("workstream_generation.LinearGraphQLTransport",
                  return_value=snapshot_transport), patch(
            "workstream_generation.compact_context", side_effect=capture_compact,
        ):
            strict = strict_candidate_loader(
                self.client, token=WORKSTREAM, authority=AUTHORITY,
                plan_source="plan", plan_identity=None,
                activation_checkpoint=checkpoint,
                activation_remote_head="e" * 40,
                activation_created_at="production-preview",
            )
            candidates = []

            def recording_loader(plan):
                receipt = strict(plan)
                candidates.append(deepcopy(receipt))
                return receipt

            transport = GenerationTransport(
                self.client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
                authority=AUTHORITY, candidate_loader=recording_loader,
                legacy_description_plan_revision=OLD,
            )
            writes = len(self.client.mutations)
            preview = transport.preview_activate(
                target_plan_revision=NEW, created_at="production-preview",
                retirement=retirement, activation_checkpoint=checkpoint,
                remote_head="e" * 40,
            )
            self.assertEqual(len(self.client.mutations), writes)
            preview_candidate = deepcopy(preview["candidate"])
            candidates.clear()
            transport.activate(
                target_plan_revision=NEW, created_at="production-preview",
                retirement=retirement, activation_checkpoint=checkpoint,
                remote_head="e" * 40,
            )

        self.assertEqual(candidates[0], preview_candidate)
        self.assertTrue(contexts)
        for context in contexts:
            encoded = json.dumps(
                context, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode()
            self.assertLessEqual(len(encoded), 24 * 1024)
            child = context["children"][0]
            self.assertEqual(child["next_action"], "Run the current physical canary.")
            self.assertEqual(
                child["stale_plan_material_obligations"][
                    "checkpoint_root_revision"
                ], 30,
            )
            self.assertEqual(
                child["stale_plan_material_obligations"]["acknowledged_count"], 30,
            )
            self.assertEqual(child["uncheckpointed_material_obligations"], [])

    def test_activation_cli_refuses_legacy_retirement_file_without_writes(self):
        project_full(self.client, NEW)
        checkpoint = self.activation_checkpoint()
        retirement = self.retirement()
        with tempfile.NamedTemporaryFile("w", suffix=".json") as proof, \
                tempfile.NamedTemporaryFile("w", suffix=".json") as checkpoint_file:
            json.dump(retirement, proof)
            proof.flush()
            json.dump(checkpoint, checkpoint_file)
            checkpoint_file.flush()
            argv = [
                "workstream_generation.py", "activate", WORKSTREAM,
                "--plan-source", "plan", "--plan-identity", "plan",
                "--retirement-proof", proof.name,
                "--activation-checkpoint", checkpoint_file.name,
                "--remote-head", "e" * 40,
                "--created-at", "checkpoint-preview",
            ]
            stdout, stderr = io.StringIO(), io.StringIO()
            writes = len(self.client.mutations)
            with patch.object(sys, "argv", argv), patch(
                "workstream_generation._route_and_client",
                side_effect=AssertionError("invalid local CLI reached auth/network"),
            ), patch("workstream_generation.plan_payload", return_value={
                "source": {
                    "identity": f"https://example.test/{NEW}",
                    "sha256": NEW,
                },
            }), patch.object(sys, "stdout", stdout), patch.object(
                sys, "stderr", stderr,
            ):
                self.assertEqual(main(), 2)
        self.assertIn(
            "generation_legacy_retirement_proof_cannot_authorize_operator",
            stderr.getvalue(),
        )
        self.assertEqual(len(self.client.mutations), writes)
        self.assertEqual(stdout.getvalue(), "")

        argv = [
            "workstream_generation.py", "activate", WORKSTREAM,
            "--plan-source", "plan", "--created-at", "now",
        ]
        stderr = io.StringIO()
        with patch.object(sys, "argv", argv), patch(
            "workstream_generation._route_and_client",
            side_effect=AssertionError("invalid local CLI reached auth/network"),
        ), patch.object(sys, "stderr", stderr):
            self.assertEqual(main(), 2)
        self.assertIn("generation_candidate_cli_arguments_incomplete", stderr.getvalue())

    def test_activation_preview_refuses_unrelated_pending_boundary(self):
        project_full(self.client, NEW)
        project_full(self.client, OTHER)
        retirement = self.retirement()
        candidate = self.transport._candidate(NEW, self.client.comments)
        reservation = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None,
            candidate=candidate, retirement=retirement,
            created_at="competing",
        )
        self.transport._append_reservation(reservation)
        writes = len(self.client.mutations)
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_boundary_reserved|ledger_boundary_reserved",
        ):
            self.transport.preview_activate(
                target_plan_revision=OTHER, created_at="now",
                retirement=retirement,
            )
        self.assertEqual(len(self.client.mutations), writes)

    def test_activation_preview_rejects_exact_aborted_reservation_like_apply(self):
        project_full(self.client, NEW)
        retirement = self.retirement()
        candidate = self.transport._candidate(NEW, self.client.comments)
        reservation = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None,
            candidate=candidate, retirement=retirement, created_at="now",
        )
        stored = self.transport._append_reservation(reservation)
        self.transport.abort(
            reservation_id=stored["reservation_id"],
            reservation_sha256=stored["reservation_sha256"],
            reason="reviewed cancellation", created_at="abort",
        )
        writes = len(self.client.mutations)
        for operation in (
            self.transport.preview_activate,
            self.transport.activate,
        ):
            with self.subTest(operation=operation.__name__):
                with self.assertRaisesRegex(
                    WorkstreamGenerationError,
                    "generation_reservation_aborted_or_completed",
                ):
                    operation(
                        target_plan_revision=NEW, created_at="now",
                        retirement=retirement,
                    )
                self.assertEqual(len(self.client.mutations), writes)

    def test_checkpoint_retry_rejects_aborted_plain_reservation_before_disposition(self):
        project_full(self.client, NEW)
        retirement = self.retirement()
        candidate = self.transport._candidate(NEW, self.client.comments)
        plain = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None,
            candidate=candidate, retirement=retirement, created_at="now",
        )
        stored = self.transport._append_reservation(plain)
        self.transport.abort(
            reservation_id=stored["reservation_id"],
            reservation_sha256=stored["reservation_sha256"],
            reason="replace with checkpoint activation", created_at="abort",
        )
        checkpoint = self.activation_checkpoint()
        writes = len(self.client.mutations)
        with patch("workstream_generation.plan_payload", return_value={
            "source": {
                "identity": f"https://example.test/{NEW}", "sha256": NEW,
            },
        }):
            self.transport.candidate_loader = strict_candidate_loader(
                self.client, token=WORKSTREAM, authority=AUTHORITY,
                plan_source="plan", plan_identity=None,
                activation_checkpoint=checkpoint,
                activation_remote_head="e" * 40,
                activation_created_at="now",
            )
            for operation in (
                self.transport.preview_activate,
                self.transport.activate,
            ):
                with self.subTest(operation=operation.__name__):
                    with self.assertRaisesRegex(
                        WorkstreamGenerationError,
                        "generation_reservation_aborted_or_completed",
                    ):
                        operation(
                            target_plan_revision=NEW, created_at="now",
                            retirement=retirement,
                            activation_checkpoint=checkpoint,
                            remote_head="e" * 40,
                        )
                    self.assertEqual(len(self.client.mutations), writes)
                    self.assertEqual(
                        adapter(self.client, NEW).state().snapshot["disposition"],
                        {
                            "disposition": "attach", "remote_head": "e" * 40,
                            "recovered_from_checkpoint": None,
                        },
                    )

    def test_activation_preview_returns_exact_completed_historical_replay(self):
        project_full(self.client, NEW)
        retirement = self.retirement()
        applied = self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=retirement,
        )
        writes = len(self.client.mutations)
        preview = self.transport.preview_activate(
            target_plan_revision=NEW, created_at="now",
            retirement=retirement,
        )
        replay = self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=retirement,
        )
        self.assertEqual(preview, replay)
        self.assertEqual(preview["event_id"], applied["event_id"])
        self.assertTrue(preview["replay"])
        self.assertEqual(len(self.client.mutations), writes)

    def test_prepared_disposition_rejects_checkpoint_from_predecessor_generation(self):
        project_full(self.client, NEW)
        predecessor_checkpoint = build_checkpoint(
            workstream_id=WORKSTREAM, boundary_id="predecessor-only",
            root_revision=0, plan_revision=OLD,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "old", "machine": "M5",
                "worktree": {"state": "unknown"},
            }, exact_head=None, evidence=[], blocker=None,
            next_action="Retire the predecessor.",
        )
        LinearCheckpointAdapter(
            self.client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            workspace_id="workspace", team_id="team", project_id="project",
        ).persist(predecessor_checkpoint)
        target = adapter(self.client, NEW)
        state = target.state()
        disposition = next(
            event for event in state.events
            if event["kind"] == "disposition" and event["key"] == "root"
        )
        target.append(build_projection_event(
            workstream_id=WORKSTREAM, kind="disposition", key="root",
            value={
                "disposition": "attach", "remote_head": "e" * 40,
                "recovered_from_checkpoint": predecessor_checkpoint["event_id"],
            },
            plan_revision=NEW, expected_revision=state.revision,
            created_at="prepared",
            supersedes_event_id=disposition["event_id"], authority=AUTHORITY,
        ))
        retirement = self.retirement()
        writes = len(self.client.mutations)
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_prepared_activation_checkpoint_required",
        ):
            self.transport.preview_activate(
                target_plan_revision=NEW, created_at="now",
                retirement=retirement,
            )
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_prepared_activation_checkpoint_required",
        ):
            self.transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=retirement,
            )
        self.assertEqual(len(self.client.mutations), writes)

    def test_activation_checkpoint_crash_after_disposition_keeps_old_active(self):
        project_full(self.client, NEW)
        checkpoint = self.activation_checkpoint()
        self.loader = ActivationCheckpointLoader(self.client, checkpoint)
        self.transport.candidate_loader = self.loader
        original = self.transport._append_reservation
        self.transport._append_reservation = lambda _value: (_ for _ in ()).throw(
            WorkstreamGenerationError("crash after disposition")
        )
        with self.assertRaisesRegex(WorkstreamGenerationError, "crash after"):
            self.transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(), activation_checkpoint=checkpoint,
                remote_head="e" * 40,
            )
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], OLD)
        self.assertFalse(any(
            item["event_id"] == checkpoint["event_id"]
            for item in reduce_checkpoint_comments(
                self.client.comments, workstream_id=WORKSTREAM,
            ).checkpoints
        ))
        self.transport._append_reservation = original
        self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(), activation_checkpoint=checkpoint,
            remote_head="e" * 40,
        )

    def test_activation_checkpoint_crash_then_plain_retry_refuses(self):
        project_full(self.client, NEW)
        checkpoint = self.activation_checkpoint()
        self.transport.candidate_loader = ActivationCheckpointLoader(
            self.client, checkpoint,
        )
        original = self.transport._append_reservation
        self.transport._append_reservation = lambda _value: (_ for _ in ()).throw(
            WorkstreamGenerationError("crash after disposition")
        )
        with self.assertRaisesRegex(WorkstreamGenerationError, "crash after"):
            self.transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(), activation_checkpoint=checkpoint,
                remote_head="e" * 40,
            )
        self.transport._append_reservation = original
        self.transport.candidate_loader = Loader(self.client)
        count = len(self.client.mutations)
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_prepared_activation_checkpoint_required",
        ):
            self.transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(),
            )
        self.assertEqual(len(self.client.mutations), count)
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], OLD)

    def test_activation_checkpoint_historical_replay_requires_exact_inputs(self):
        project_full(self.client, NEW)
        checkpoint = self.activation_checkpoint()
        self.transport.candidate_loader = ActivationCheckpointLoader(
            self.client, checkpoint,
        )
        retirement = self.retirement()
        self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=retirement, activation_checkpoint=checkpoint,
            remote_head="e" * 40,
        )
        count = len(self.client.mutations)
        different = self.activation_checkpoint(boundary_id="different")
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_historical_replay_checkpoint_mismatch",
        ):
            self.transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=retirement, activation_checkpoint=different,
                remote_head="f" * 40,
            )
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_historical_replay_remote_head_mismatch",
        ):
            self.transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=retirement, activation_checkpoint=checkpoint,
                remote_head="f" * 40,
            )
        with self.assertRaisesRegex(
            WorkstreamGenerationError,
            "generation_historical_replay_checkpoint_mismatch",
        ):
            self.transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=retirement,
            )
        self.assertEqual(len(self.client.mutations), count)
        target = adapter(self.client, NEW)
        target_state = target.state()
        prior_disposition = next(
            event for event in reversed(target_state.events)
            if event["kind"] == "disposition" and event["key"] == "root"
        )
        target.append(build_projection_event(
            workstream_id=WORKSTREAM, kind="disposition", key="root",
            value={
                "disposition": "create_successor", "remote_head": "a" * 40,
                "recovered_from_checkpoint": checkpoint["event_id"],
            },
            plan_revision=NEW, expected_revision=target_state.revision,
            created_at="later", supersedes_event_id=prior_disposition["event_id"],
            authority=AUTHORITY,
        ))
        count = len(self.client.mutations)
        replay = self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=retirement, activation_checkpoint=checkpoint,
            remote_head="e" * 40,
        )
        self.assertTrue(replay["replay"])
        self.assertEqual(len(self.client.mutations), count)

    def test_activation_checkpoint_contradiction_refuses_without_mutation(self):
        project_full(self.client, NEW)
        checkpoint = self.activation_checkpoint(plan_revision=OTHER)
        count = len(self.client.mutations)
        with self.assertRaisesRegex(
            WorkstreamGenerationError, "activation_checkpoint_mismatch",
        ):
            self.transport.activate(
                target_plan_revision=NEW, created_at="now",
                retirement=self.retirement(), activation_checkpoint=checkpoint,
                remote_head="e" * 40,
            )
        self.assertEqual(len(self.client.mutations), count)

    def test_activation_checkpoint_requires_authenticated_selected_chain(self):
        project_full(self.client, NEW)
        checkpoint = self.activation_checkpoint()
        self.transport.candidate_loader = ActivationCheckpointLoader(
            self.client, checkpoint,
        )
        self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(), activation_checkpoint=checkpoint,
            remote_head="e" * 40,
        )
        selected = select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )
        transition = adapter(self.client, OLD).state().events[-1]

        # A syntactically valid sibling is not a second checkpoint authority.
        fork = build_projection_event(
            workstream_id=WORKSTREAM, kind="generation_transition", key="root",
            value=transition["value"], plan_revision=OLD,
            expected_revision=transition["expected_revision"],
            created_at="fork", authority=AUTHORITY,
        )
        self.client.comments.append({
            "id": "fork", "body": encode_projection_comment(fork),
        })
        with self.assertRaisesRegex(
            LinearProjectionError, "fork|conflict|slot_identity_mismatch",
        ):
            selected_activation_checkpoints(
                self.client.comments, workstream_id=WORKSTREAM,
                transition_event_id=selected["transition_tip_event_id"],
                active_plan_revision=NEW, authenticated_route=AUTHORITY,
            )
        self.client.comments.pop()

        # A route-forged transition also closes authority rather than injecting.
        forged_authority = {**AUTHORITY, "project_id": "forged"}
        forged = build_projection_event(
            workstream_id=WORKSTREAM, kind="generation_transition", key="root",
            value=transition["value"], plan_revision=OLD,
            expected_revision=transition["expected_revision"],
            created_at="forged", authority=forged_authority,
        )
        self.client.comments.append({
            "id": "forged", "body": encode_projection_comment(forged),
        })
        with self.assertRaisesRegex(LinearProjectionError, "route_mismatch"):
            selected_activation_checkpoints(
                self.client.comments, workstream_id=WORKSTREAM,
                transition_event_id=selected["transition_tip_event_id"],
                active_plan_revision=NEW, authenticated_route=AUTHORITY,
            )

    def test_activation_checkpoint_duplicate_physical_copy_refuses(self):
        project_full(self.client, NEW)
        checkpoint = self.activation_checkpoint()
        self.transport.candidate_loader = ActivationCheckpointLoader(
            self.client, checkpoint,
        )
        self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(), activation_checkpoint=checkpoint,
            remote_head="e" * 40,
        )
        selected = select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )
        carried = selected_activation_checkpoints(
            self.client.comments, workstream_id=WORKSTREAM,
            transition_event_id=selected["transition_tip_event_id"],
            active_plan_revision=NEW, authenticated_route=AUTHORITY,
        )
        duplicate = [*self.client.comments, {
            "id": "physical-copy", "body": encode_checkpoint_comment(checkpoint),
        }]
        with self.assertRaisesRegex(Exception, "duplicate_checkpoint_event_id"):
            reduce_checkpoint_comments(
                duplicate, workstream_id=WORKSTREAM,
                selected_activation_checkpoints=carried,
            )

    def test_superseded_activation_checkpoint_remains_predecessor_history(self):
        project_full(self.client, NEW)
        first = self.activation_checkpoint()
        self.transport.candidate_loader = ActivationCheckpointLoader(
            self.client, first,
        )
        self.transport.activate(
            target_plan_revision=NEW, created_at="first",
            retirement=self.retirement(), activation_checkpoint=first,
            remote_head="e" * 40,
        )
        project_full(self.client, LATER)
        second = self.activation_checkpoint(
            plan_revision=LATER, boundary_id="activate-later",
            predecessor_event_id=None,
        )
        self.transport.candidate_loader = ActivationCheckpointLoader(
            self.client, second,
        )
        self.transport.activate(
            target_plan_revision=LATER, created_at="second",
            retirement=self.retirement(NEW, 1), activation_checkpoint=second,
            remote_head="e" * 40,
        )
        selected = select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )
        carried = selected_activation_checkpoints(
            self.client.comments, workstream_id=WORKSTREAM,
            transition_event_id=selected["transition_tip_event_id"],
            active_plan_revision=LATER, authenticated_route=AUTHORITY,
        )
        self.assertEqual(
            {item[0]["event_id"] for item in carried},
            {first["event_id"], second["event_id"]},
        )

    def test_ordinary_checkpoint_can_follow_activation_checkpoint(self):
        project_full(self.client, NEW)
        activation = self.activation_checkpoint()
        self.transport.candidate_loader = ActivationCheckpointLoader(
            self.client, activation,
        )
        self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(), activation_checkpoint=activation,
            remote_head="e" * 40,
        )
        LinearCommentEventAdapter(
            self.client, issue_id=WORKSTREAM, plan_revision=NEW, **AUTHORITY,
        ).apply(Delta(
            "after-activation", WORKSTREAM, "requirement", "successor",
            {"requirement": "successor checkpoint material"}, 0, "after",
        ))
        successor = self.activation_checkpoint(
            boundary_id="ordinary-successor",
            root_revision=1,
            predecessor_event_id=activation["event_id"],
        )
        persisted = LinearCheckpointAdapter(
            self.client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            workspace_id="workspace", team_id="team", project_id="project",
        ).persist(successor)
        self.assertEqual(persisted["event_id"], successor["event_id"])

    def test_legacy_description_compatibility_and_missing_description_bootstrap_gate(self):
        selected = select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )
        self.assertEqual(selected["plan_revision"], OLD)
        with self.assertRaisesRegex(LinearProjectionError, "bootstrap_required"):
            select_plan_generation(
                self.client.comments, workstream_id=WORKSTREAM,
                description_plan_revision=None, authenticated_route=AUTHORITY,
            )

    def test_bootstrap_genesis_is_append_only_and_description_becomes_diagnostic(self):
        bootstrap = GenerationTransport(
            self.client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            authority=AUTHORITY, candidate_loader=self.loader,
            legacy_description_plan_revision=None,
        )
        receipt = bootstrap.bootstrap(target_plan_revision=OLD, created_at="now")
        selected = select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=None, authenticated_route=AUTHORITY,
        )
        self.assertEqual(selected["plan_revision"], OLD)
        self.assertEqual(selected["authority_origin"], "generation_genesis")
        self.assertEqual(selected["activation_epoch"], 0)
        self.assertEqual(receipt["event_id"], selected["transition_tip_event_id"])
        count = len(self.client.mutations)
        replay = bootstrap.bootstrap(target_plan_revision=OLD, created_at="ignored")
        self.assertTrue(replay["replay"])
        self.assertEqual(len(self.client.mutations), count)

    def test_genesis_target_can_evolve_after_authority_append(self):
        bootstrap = GenerationTransport(
            self.client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            authority=AUTHORITY, candidate_loader=self.loader,
            legacy_description_plan_revision=None,
        )
        bootstrap.bootstrap(target_plan_revision=OLD, created_at="now")
        checkpoint = build_checkpoint(
            workstream_id=WORKSTREAM, boundary_id="active-genesis", root_revision=0,
            plan_revision=OLD, before_status="In Progress",
            after_status="In Progress", execution={
                "agent": "codex", "provider": "openai", "session_id": "active",
                "machine": "M5", "worktree": {"state": "unknown"},
            }, exact_head=None, evidence=[], blocker=None, next_action="Continue.",
        )
        LinearCheckpointAdapter(
            self.client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            workspace_id="workspace", team_id="team", project_id="project",
        ).persist(checkpoint)
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=None, authenticated_route=AUTHORITY,
        )["plan_revision"], OLD)

    def test_activation_order_is_reservation_seal_then_last_activation(self):
        project_full(self.client, NEW)
        before = len(self.client.mutations)
        receipt = self.activate()
        writes = self.client.mutations[before:]
        self.assertEqual(len(writes), 3)
        self.assertIn("generation-reservation", writes[0]["body"])
        target = adapter(self.client, NEW).state()
        predecessor = adapter(self.client, OLD).state()
        seal, activation = target.events[-1], predecessor.events[-1]
        self.assertEqual(seal["kind"], "generation_candidate_seal")
        self.assertEqual(activation["kind"], "generation_transition")
        self.assertEqual(receipt["event_id"], activation["event_id"])
        self.assertEqual(activation["value"]["candidate_seal_event_id"], seal["event_id"])

    def test_naked_retirement_boolean_or_tampered_proof_cannot_activate(self):
        project_full(self.client, NEW)
        with self.assertRaisesRegex(WorkstreamGenerationError, "retirement_proof"):
            self.transport.activate(target_plan_revision=NEW, created_at="now", retirement=True)
        bad = build_retirement_proof(
            predecessor_plan_revision=OLD, retired_at="now", retired_writer_epoch=0,
            provenance_event_ids=[], checkpoint_event_ids=[],
        )
        with self.assertRaisesRegex(LinearTransportError, "retirement_frontier"):
            self.transport.activate(target_plan_revision=NEW, created_at="now", retirement=bad)

    def test_candidate_must_be_standard_strict_full_authority(self):
        project_full(self.client, NEW)
        self.loader.authority = "partial"
        before = len(self.client.mutations)
        with self.assertRaisesRegex(WorkstreamGenerationError, "strict_full"):
            self.activate()
        self.assertEqual(len(self.client.mutations), before)

    def test_active_target_can_evolve_but_retired_projection_writer_refuses(self):
        self.activate()
        target = adapter(self.client, NEW)
        event = build_projection_event(
            workstream_id=WORKSTREAM, kind="provenance", key="active-later",
            value={"agent": "codex", "machine": "M5", "session_id": "active",
                   "worktree": {"state": "safe", "head": "e" * 40}},
            plan_revision=NEW, expected_revision=target.state().revision,
            created_at="later", authority=AUTHORITY,
        )
        target.append(event)
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OTHER, authenticated_route=AUTHORITY,
        )["plan_revision"], NEW)
        old = adapter(self.client, OLD)
        late = build_projection_event(
            workstream_id=WORKSTREAM, kind="provenance", key="retired-late",
            value={"agent": "codex", "machine": "M5", "session_id": "retired",
                   "worktree": {"state": "safe", "head": "e" * 40}},
            plan_revision=OLD, expected_revision=old.state().revision,
            created_at="late", authority=AUTHORITY,
        )
        count = len(self.client.mutations)
        with self.assertRaisesRegex(LinearProjectionError, "writer_retired"):
            old.append(late)
        self.assertEqual(len(self.client.mutations), count)

    def test_retired_material_writer_refuses_before_mutation(self):
        self.activate()
        count = len(self.client.mutations)
        with self.assertRaisesRegex(LinearTransportError, "writer_retired"):
            LinearCommentEventAdapter(
                self.client, issue_id=WORKSTREAM, plan_revision=OLD, **AUTHORITY,
            ).apply(Delta("late", WORKSTREAM, "requirement", "retired",
                          {"requirement": "must not append"}, 0, "later"))
        self.assertEqual(len(self.client.mutations), count)

    def test_exact_historical_replay_after_later_successor_is_zero_write(self):
        original_retirement = self.retirement()
        project_full(self.client, NEW)
        first = self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=original_retirement,
        )
        project_full(self.client, LATER)
        self.transport.activate(
            target_plan_revision=LATER, created_at="later",
            retirement=self.retirement(NEW, 1),
        )
        count = len(self.client.mutations)
        replay = self.transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=original_retirement,
        )
        self.assertEqual(replay["event_id"], first["event_id"])
        self.assertEqual(len(self.client.mutations), count)

    def test_lost_activation_response_converges_without_second_append(self):
        project_full(self.client, NEW)
        self.client.commit_then_fail_at.add(len(self.client.mutations) + 3)
        receipt = self.activate()
        self.assertEqual(receipt["event_id"], adapter(self.client, OLD).state().events[-1]["event_id"])
        self.assertEqual(sum(event["kind"] == "generation_transition"
                             for event in adapter(self.client, OLD).state().events), 1)

    def test_pending_reservation_blocks_material_and_is_recoverable(self):
        project_full(self.client, NEW)
        candidate = self.transport._candidate(NEW, self.client.comments)
        reservation = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None, candidate=candidate,
            retirement=self.retirement(), created_at="now",
        )
        stored = self.transport._append_reservation(reservation)
        self.assertEqual(len(pending_generation_reservations(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        )), 1)
        count = len(self.client.mutations)
        with self.assertRaisesRegex(LinearTransportError, "generation_boundary_reserved"):
            LinearCommentEventAdapter(
                self.client, issue_id=WORKSTREAM, plan_revision=OLD, **AUTHORITY,
            ).apply(Delta("blocked", WORKSTREAM, "requirement", "active",
                          {"requirement": "blocked"}, 0, "now"))
        self.assertEqual(len(self.client.mutations), count)
        self.assertEqual(self.transport._append_reservation(reservation)["remote_id"],
                         stored["remote_id"])

    def test_competing_candidates_share_one_route_root_slot(self):
        project_full(self.client, NEW)
        project_full(self.client, OTHER)
        retirement = self.retirement()
        new = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None,
            candidate=self.transport._candidate(NEW, self.client.comments),
            retirement=retirement, created_at="now",
        )
        other = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=OTHER, epoch=0, previous_control=None,
            candidate=self.transport._candidate(OTHER, self.client.comments),
            retirement=retirement, created_at="now",
        )
        self.assertNotEqual(new["reservation_id"], other["reservation_id"])
        self.assertEqual(
            ledger_boundary_slot_id(WORKSTREAM, new["material_revision"], new["ledger_frontier"], AUTHORITY),
            ledger_boundary_slot_id(WORKSTREAM, other["material_revision"], other["ledger_frontier"], AUTHORITY),
        )
        self.transport._append_reservation(new)
        with self.assertRaisesRegex(WorkstreamGenerationError, "slot_lost"):
            self.transport._append_reservation(other)

    def test_graph_mutation_after_reservation_refuses_before_seal(self):
        project_full(self.client, NEW)
        original = self.transport._append_reservation

        def mutate(value):
            result = original(value)
            self.loader.graph = "changed"
            return result

        self.transport._append_reservation = mutate
        with self.assertRaisesRegex(WorkstreamGenerationError, "changed_after_reservation"):
            self.activate()
        self.assertFalse(any(event["kind"] == "generation_transition"
                             for event in adapter(self.client, OLD).state().events))

    def test_durable_abort_releases_exact_reservation_and_replays(self):
        project_full(self.client, NEW)
        candidate = self.transport._candidate(NEW, self.client.comments)
        reservation = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None, candidate=candidate,
            retirement=self.retirement(), created_at="now",
        )
        stored = self.transport._append_reservation(reservation)
        receipt = self.transport.abort(
            reservation_id=reservation["reservation_id"],
            reservation_sha256=stored["reservation_sha256"],
            reason="candidate graph changed", created_at="later",
        )
        self.assertFalse(pending_generation_reservations(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        ))
        count = len(self.client.mutations)
        replay = self.transport.abort(
            reservation_id=reservation["reservation_id"],
            reservation_sha256=stored["reservation_sha256"],
            reason="candidate graph changed", created_at="later",
        )
        self.assertTrue(replay["replay"])
        self.assertEqual(receipt["remote_id"], replay["remote_id"])
        self.assertEqual(len(self.client.mutations), count)
        self.assertEqual(adapter(self.client, OLD).state().events[-1]["kind"],
                         "generation_abort")
        project_full(self.client, OTHER)
        replacement = self.transport.activate(
            target_plan_revision=OTHER, created_at="replacement",
            retirement=self.retirement(),
        )
        self.assertFalse(replacement["replay"])
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], OTHER)
        self.assertFalse(pending_generation_reservations(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        ))

    def test_tampered_reservation_refuses_instead_of_laundering_material(self):
        project_full(self.client, NEW)
        candidate = self.transport._candidate(NEW, self.client.comments)
        reservation = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None, candidate=candidate,
            retirement=self.retirement(), created_at="now",
        )
        self.transport._append_reservation(reservation)
        body = self.client.comments[-1]["body"]
        self.client.comments[-1]["body"] = body[:-5] + "A -->"
        with self.assertRaisesRegex(WorkstreamGenerationError, "malformed"):
            pending_generation_reservations(
                self.client.comments, workstream_id=WORKSTREAM,
                authenticated_route=AUTHORITY,
            )

    def test_predecessor_projection_race_wins_slot_and_activation_refuses(self):
        project_full(self.client, NEW)

        def race(item, client):
            body = item["body"]
            if PROJECTION_PREFIX not in body:
                return
            matches = PROJECTION_RE.findall(body)
            if len(matches) != 1:
                return
            event = _decode_projection(matches[0])
            if event["kind"] != "generation_transition":
                return
            state = adapter(client, OLD).state()
            winner = build_projection_event(
                workstream_id=WORKSTREAM, kind="provenance", key="race-winner",
                value={"agent": "codex", "machine": "M5", "session_id": "race",
                       "worktree": {"state": "safe", "head": "e" * 40}},
                plan_revision=OLD, expected_revision=state.revision,
                created_at="race", authority=AUTHORITY,
            )
            client.comments.append({
                "id": item["id"], "body": __import__(
                    "workstream_linear_projection"
                ).encode_projection_comment(winner),
                "createdAt": "race", "updatedAt": "race",
            })

        self.client.before_each_create = race
        with self.assertRaisesRegex(LinearProjectionError, "slot_lost"):
            self.activate()
        self.assertNotEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], NEW)
        self.client.before_each_create = None
        pending = pending_generation_reservations(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        )
        self.assertEqual(len(pending), 1)
        abort = self.transport.abort(
            reservation_id=pending[0]["reservation_id"],
            reservation_sha256=pending[0]["reservation_sha256"],
            reason="predecessor writer won original slot", created_at="abort",
        )
        self.assertFalse(abort["replay"])
        old_state = adapter(self.client, OLD).state()
        self.assertIn("race-winner", [event["key"] for event in old_state.events])
        self.assertEqual(old_state.events[-1]["kind"], "generation_abort")
        self.assertFalse(pending_generation_reservations(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        ))
        project_full(self.client, OTHER)
        replacement = self.transport.activate(
            target_plan_revision=OTHER, created_at="replacement",
            retirement=self.retirement(),
        )
        self.assertFalse(replacement["replay"])
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], OTHER)

    def test_retired_checkpoint_writer_refuses_before_mutation(self):
        self.activate()
        checkpoint = build_checkpoint(
            workstream_id=WORKSTREAM, boundary_id="retired", root_revision=0,
            plan_revision=OLD, before_status="In Progress",
            after_status="In Progress", execution={
                "agent": "codex", "provider": "openai", "session_id": "old",
                "machine": "M5", "worktree": {"state": "unknown"},
            }, exact_head=None, evidence=[], blocker=None, next_action="Must refuse.",
        )
        count = len(self.client.mutations)
        with self.assertRaisesRegex(LinearTransportError, "writer_retired"):
            LinearCheckpointAdapter(
                self.client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
                workspace_id="workspace", team_id="team", project_id="project",
            ).persist(checkpoint)
        self.assertEqual(len(self.client.mutations), count)

    def test_rebased_abort_binds_multiple_intervening_predecessor_events(self):
        project_full(self.client, NEW)
        retirement = self.retirement()
        reservation = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None,
            candidate=self.transport._candidate(NEW, self.client.comments),
            retirement=retirement, created_at="now",
        )
        stored = self.transport._append_reservation(reservation)
        intervening = []
        for index in range(2):
            state = adapter(self.client, OLD).state()
            event = build_projection_event(
                workstream_id=WORKSTREAM, kind="provenance",
                key=f"intervening-{index}", value={
                    "agent": "old", "machine": "M5",
                    "session_id": f"old-{index}",
                    "worktree": {"state": "safe", "head": "e" * 40},
                }, plan_revision=OLD, expected_revision=state.revision,
                created_at=f"race-{index}", authority=AUTHORITY,
            )
            intervening.append(event)
            self.client.execute("commentCreate", {"input": {
                "id": projection_slot_id(
                    WORKSTREAM, OLD, state.revision, AUTHORITY,
                ),
                "issueId": WORKSTREAM, "body": encode_projection_comment(event),
            }})
        self.transport.abort(
            reservation_id=reservation["reservation_id"],
            reservation_sha256=stored["reservation_sha256"],
            reason="two old writers won", created_at="abort",
        )
        abort = adapter(self.client, OLD).state().events[-1]
        self.assertEqual(abort["kind"], "generation_abort")
        self.assertEqual(abort["value"]["intervening_event_ids"],
                         [event["event_id"] for event in intervening])
        self.assertEqual(abort["value"]["intervening_events_sha256"],
                         _digest(intervening))
        self.assertEqual(abort["value"]["original_occupant_event_id"],
                         intervening[0]["event_id"])

    def test_rebased_abort_race_is_bounded_fail_closed_then_retryable(self):
        project_full(self.client, NEW)
        reservation = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None,
            candidate=self.transport._candidate(NEW, self.client.comments),
            retirement=self.retirement(), created_at="now",
        )
        stored = self.transport._append_reservation(reservation)
        race_count = 0

        def always_win_abort_slot(item, client):
            nonlocal race_count
            if PROJECTION_PREFIX not in item["body"]:
                return
            attempted = _decode_projection(PROJECTION_RE.findall(item["body"])[0])
            if attempted["kind"] != "generation_abort":
                return
            winner = build_projection_event(
                workstream_id=WORKSTREAM, kind="provenance",
                key=f"abort-race-{race_count}", value={
                    "agent": "old", "machine": "M5",
                    "session_id": f"race-{race_count}",
                    "worktree": {"state": "safe", "head": "e" * 40},
                }, plan_revision=OLD,
                expected_revision=attempted["expected_revision"],
                created_at=f"abort-race-{race_count}", authority=AUTHORITY,
            )
            race_count += 1
            client.comments.append({
                "id": item["id"], "body": encode_projection_comment(winner),
                "createdAt": "race", "updatedAt": "race",
            })

        self.client.before_each_create = always_win_abort_slot
        with self.assertRaisesRegex(WorkstreamGenerationError, "rebase_limit"):
            self.transport.abort(
                reservation_id=reservation["reservation_id"],
                reservation_sha256=stored["reservation_sha256"],
                reason="bounded", created_at="abort",
            )
        self.assertEqual(race_count, 8)
        self.assertEqual(len(pending_generation_reservations(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        )), 1)
        self.client.before_each_create = None
        self.transport.abort(
            reservation_id=reservation["reservation_id"],
            reservation_sha256=stored["reservation_sha256"],
            reason="bounded", created_at="abort",
        )
        self.assertFalse(pending_generation_reservations(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        ))

    def test_malformed_rebased_abort_frontier_fails_closed(self):
        project_full(self.client, NEW)
        reservation = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None,
            candidate=self.transport._candidate(NEW, self.client.comments),
            retirement=self.retirement(), created_at="now",
        )
        stored = self.transport._append_reservation(reservation)
        state = adapter(self.client, OLD).state()
        winner = build_projection_event(
            workstream_id=WORKSTREAM, kind="provenance", key="malformed-winner",
            value={"agent": "old", "machine": "M5", "session_id": "bad",
                   "worktree": {"state": "safe", "head": "e" * 40}},
            plan_revision=OLD, expected_revision=state.revision,
            created_at="race", authority=AUTHORITY,
        )
        self.client.execute("commentCreate", {"input": {
            "id": projection_slot_id(WORKSTREAM, OLD, state.revision, AUTHORITY),
            "issueId": WORKSTREAM, "body": encode_projection_comment(winner),
        }})
        abort = build_projection_event(
            workstream_id=WORKSTREAM, kind="generation_abort",
            key=reservation["reservation_id"], value={
                "schema_version": 2,
                "reservation_id": reservation["reservation_id"],
                "reservation_sha256": stored["reservation_sha256"],
                "reason": "tampered", "original_projection_revision": state.revision,
                "intervening_event_ids": [winner["event_id"]],
                "intervening_events_sha256": "0" * 64,
                "original_occupant_event_id": winner["event_id"],
            }, plan_revision=OLD, expected_revision=state.revision + 1,
            created_at="abort", authority=AUTHORITY,
        )
        self.client.execute("commentCreate", {"input": {
            "id": projection_slot_id(
                WORKSTREAM, OLD, state.revision + 1, AUTHORITY,
            ), "issueId": WORKSTREAM, "body": encode_projection_comment(abort),
        }})
        with self.assertRaisesRegex(WorkstreamGenerationError,
                                    "abort_frontier_mismatch"):
            pending_generation_reservations(
                self.client.comments, workstream_id=WORKSTREAM,
                authenticated_route=AUTHORITY,
            )

    def test_old_runtime_collision_successors_are_quarantined_without_wedging(self):
        project_full(self.client, NEW)
        candidate = self.transport._candidate(NEW, self.client.comments)
        retirement = self.retirement()
        reservation = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None, candidate=candidate,
            retirement=retirement, created_at="now",
        )
        stored = self.transport._append_reservation(reservation)

        def legacy_successor_slot():
            frontier = list(reservation["ledger_frontier"])
            by_id = {item["id"]: item for item in self.client.comments}
            while True:
                slot = ledger_boundary_slot_id(
                    WORKSTREAM, reservation["material_revision"], frontier,
                    AUTHORITY,
                )
                occupant = by_id.get(slot)
                if occupant is None:
                    return slot
                collision = "collision:" + hashlib.sha256(json.dumps(
                    [occupant.get("id"), occupant.get("body")],
                    sort_keys=True, separators=(",", ":"),
                ).encode()).hexdigest()
                frontier = sorted([*frontier, collision])

        late_delta = Delta(
            "legacy-late", WORKSTREAM, "requirement", "old-runtime",
            {"requirement": "must be quarantined"}, 0, "late",
        )
        self.client.execute("commentCreate", {"input": {
            "id": legacy_successor_slot(), "issueId": WORKSTREAM,
            "body": encode_event_comment(late_delta),
        }})
        checkpoint = build_checkpoint(
            workstream_id=WORKSTREAM, boundary_id="legacy-late",
            root_revision=0, plan_revision=OLD, before_status="In Progress",
            after_status="In Progress", execution={
                "agent": "old", "provider": "old", "session_id": "old",
                "machine": "old", "worktree": {"state": "unknown"},
            }, exact_head=None, evidence=[], blocker=None,
            next_action="Must be quarantined.",
        )
        self.client.execute("commentCreate", {"input": {
            "id": legacy_successor_slot(), "issueId": WORKSTREAM,
            "body": encode_checkpoint_comment(checkpoint),
        }})
        self.assertEqual(reduce_event_comments(
            self.client.comments, workstream_id=WORKSTREAM,
        ).revision, 0)
        self.assertFalse(reduce_checkpoint_comments(
            self.client.comments, workstream_id=WORKSTREAM,
        ).checkpoints)
        quarantine = generation_quarantine_metadata(
            self.client.comments, workstream_id=WORKSTREAM,
        )
        self.assertEqual(quarantine["count"], 2)
        receipt = self.transport.activate(
            target_plan_revision=NEW, created_at="now", retirement=retirement,
        )
        self.assertFalse(receipt["replay"])
        self.assertEqual(receipt["quarantined_legacy_writes"], quarantine)
        self.assertEqual(stored["reservation_id"], reservation["reservation_id"])
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], NEW)
        LinearCommentEventAdapter(
            self.client, issue_id=WORKSTREAM, plan_revision=NEW, **AUTHORITY,
        ).apply(Delta(
            "upgraded-active", WORKSTREAM, "requirement", "active-runtime",
            {"requirement": "authoritative"}, 0, "after-activation",
        ))
        self.assertEqual(reduce_event_comments(
            self.client.comments, workstream_id=WORKSTREAM,
        ).revision, 1)
        self.assertEqual(generation_quarantine_metadata(
            self.client.comments, workstream_id=WORKSTREAM,
        ), quarantine)

    def test_abort_wins_the_exact_final_authority_slot(self):
        project_full(self.client, NEW)

        def abort_at_final_create(item, client):
            if PROJECTION_PREFIX not in item["body"]:
                return
            event = _decode_projection(PROJECTION_RE.findall(item["body"])[0])
            if event["kind"] != "generation_transition":
                return
            value = {
                "schema_version": 2,
                "reservation_id": event["value"]["reservation_id"],
                "reservation_sha256": event["value"]["reservation_sha256"],
                "reason": "abort won final CAS",
                "original_projection_revision": event["expected_revision"],
                "intervening_event_ids": [],
                "intervening_events_sha256": _digest([]),
                "original_occupant_event_id": None,
            }
            abort = build_projection_event(
                workstream_id=WORKSTREAM, kind="generation_abort",
                key=value["reservation_id"], value=value,
                plan_revision=OLD, expected_revision=event["expected_revision"],
                created_at="race", authority=AUTHORITY,
            )
            client.comments.append({
                "id": item["id"],
                "body": __import__(
                    "workstream_linear_projection"
                ).encode_projection_comment(abort),
                "createdAt": "race", "updatedAt": "race",
            })

        self.client.before_each_create = abort_at_final_create
        with self.assertRaises(LinearTransportError):
            self.activate()
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], OLD)
        self.assertFalse(pending_generation_reservations(
            self.client.comments, workstream_id=WORKSTREAM,
            authenticated_route=AUTHORITY,
        ))

    def test_completed_activation_cannot_be_overridden_by_abort(self):
        self.activate()
        activation = adapter(self.client, OLD).state().events[-1]
        self.assertEqual(activation["kind"], "generation_transition")
        count = len(self.client.mutations)
        with self.assertRaisesRegex(WorkstreamGenerationError,
                                    "abort_after_activation"):
            self.transport.abort(
                reservation_id=activation["value"]["reservation_id"],
                reservation_sha256=activation["value"]["reservation_sha256"],
                reason="must not override", created_at="later",
            )
        self.assertEqual(len(self.client.mutations), count)
        self.assertEqual(select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )["plan_revision"], NEW)

    def test_exact_retry_resumes_after_reservation_and_after_seal(self):
        project_full(self.client, NEW)
        retirement = self.retirement()
        candidate = self.transport._candidate(NEW, self.client.comments)
        reservation = self.transport._reservation(
            comments=self.client.comments, mode="activate", from_plan=OLD,
            to_plan=NEW, epoch=0, previous_control=None, candidate=candidate,
            retirement=retirement, created_at="now",
        )
        self.transport._append_reservation(reservation)
        count = len(self.client.mutations)
        receipt = self.transport.activate(
            target_plan_revision=NEW, created_at="now", retirement=retirement,
        )
        self.assertEqual(len(self.client.mutations), count + 2)
        self.assertFalse(receipt["replay"])

        other_client = FakeClient()
        project_full(other_client, OLD)
        project_full(other_client, NEW)
        stable_loader = Loader(other_client)
        calls = 0

        def crash_after_seal(plan):
            nonlocal calls
            calls += 1
            result = stable_loader(plan)
            if calls == 2:
                raise WorkstreamGenerationError("simulated_crash_after_seal")
            return result

        transport = GenerationTransport(
            other_client, issue_id=WORKSTREAM, workstream_id=WORKSTREAM,
            authority=AUTHORITY, candidate_loader=crash_after_seal,
            legacy_description_plan_revision=OLD,
        )
        retirement = build_retirement_proof(
            predecessor_plan_revision=OLD, retired_at="now",
            retired_writer_epoch=0,
            provenance_event_ids=[event["event_id"] for event in adapter(
                other_client, OLD,
            ).state().events if event["kind"] == "provenance"],
            checkpoint_event_ids=[],
        )
        with self.assertRaisesRegex(WorkstreamGenerationError, "crash_after_seal"):
            transport.activate(
                target_plan_revision=NEW, created_at="now", retirement=retirement,
            )
        writes_after_crash = len(other_client.mutations)
        transport.candidate_loader = stable_loader
        transport.activate(
            target_plan_revision=NEW, created_at="now", retirement=retirement,
        )
        self.assertEqual(len(other_client.mutations), writes_after_crash + 1)

    def test_final_material_race_is_quarantined_at_authority_create(self):
        project_full(self.client, NEW)
        reservation_seen = None

        def race(item, client):
            nonlocal reservation_seen
            if "generation-reservation" in item["body"]:
                reservation_seen = item
                return
            if PROJECTION_PREFIX not in item["body"]:
                return
            event = _decode_projection(PROJECTION_RE.findall(item["body"])[0])
            if event["kind"] != "generation_transition":
                return
            reservation = next(iter(pending_generation_reservations(
                client.comments, workstream_id=WORKSTREAM,
                authenticated_route=AUTHORITY,
            )))
            frontier = list(reservation["ledger_frontier"])
            by_id = {entry["id"]: entry for entry in client.comments}
            while True:
                slot = ledger_boundary_slot_id(
                    WORKSTREAM, reservation["material_revision"], frontier,
                    AUTHORITY,
                )
                occupant = by_id.get(slot)
                if occupant is None:
                    break
                frontier = sorted([*frontier, "collision:" + hashlib.sha256(
                    json.dumps([occupant["id"], occupant["body"]],
                               sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()])
            client.comments.append({
                "id": slot, "body": encode_event_comment(Delta(
                    "final-race", WORKSTREAM, "requirement", "old-runtime",
                    {"requirement": "quarantine"}, 0, "race",
                )), "createdAt": "race", "updatedAt": "race",
            })

        self.client.before_each_create = race
        receipt = self.activate()
        self.assertIsNotNone(reservation_seen)
        self.assertFalse(receipt["replay"])
        self.assertEqual(reduce_event_comments(
            self.client.comments, workstream_id=WORKSTREAM,
        ).revision, 0)

    def test_strict_loader_uses_standard_budgets_and_main_revalidates(self):
        graph = {"root": {
                    "id": AUTHORITY["root_issue_id"], "identifier": WORKSTREAM,
                    "description": f"Plan revision: {OLD}",
                    "team": {"id": AUTHORITY["team_id"], "organization": {
                        "id": AUTHORITY["workspace_id"],
                    }},
                    "project": {"id": AUTHORITY["project_id"]},
                }, "children": [],
                 "decisions": [], "child_comments": {}}
        compact = MagicMock(return_value={
            "resume_authority": "full", "material_event_revision": 0,
            "material_events": [],
            "quarantined_legacy_writes": {"count": 0, "sha256": _digest([])},
        })
        snapshot_transport = MagicMock()
        snapshot_transport.snapshot_for_root.return_value = deepcopy(graph)
        snapshot_transport.recover_authorized_children.return_value = deepcopy(graph)
        child_history = MagicMock(
            side_effect=lambda value, *_args, **_kwargs: value,
        )
        activated_child = {"value": {
            "child_issue_id": "child-id", "child_workstream_id": "GEN-43",
        }}
        with patch("workstream_generation.plan_payload", return_value={
            "source": {"identity": f"https://example.test/{OLD}",
                       "sha256": OLD},
        }), patch("workstream_generation.LinearGraphQLTransport",
                  return_value=snapshot_transport), patch(
            "workstream_linear_projection.child_mutation_authorizations_from_comments",
            return_value=[activated_child],
        ), patch(
            "workstream_generation.add_child_material_history",
            child_history,
        ), patch("workstream_generation.add_material_history",
                 side_effect=lambda value, *_args, **_kwargs: value), patch(
            "workstream_generation.compact_context", compact,
        ):
            receipt = strict_candidate_loader(
                self.client, token=WORKSTREAM, authority=AUTHORITY,
                plan_source="plan", plan_identity=None,
            )(OLD)
        self.assertEqual(receipt["resume_authority"], "full")
        self.assertEqual(compact.call_args.kwargs["max_bytes"], 24 * 1024)
        self.assertEqual(compact.call_args.kwargs["max_items"], 100)
        self.assertEqual(
            child_history.call_args.kwargs["root_comments"], self.client.comments,
        )
        self.assertEqual(
            child_history.call_args.args[0]["root"]["description_plan_revision"],
            OLD,
        )
        snapshot_transport.recover_authorized_children.assert_called_once_with(
            snapshot_transport.snapshot_for_root.return_value, [activated_child],
        )

        with tempfile.NamedTemporaryFile("w", suffix=".md") as plan_file, \
                tempfile.NamedTemporaryFile("w", suffix=".json") as proof_file:
            plan_file.write("# plan\n")
            plan_file.flush()
            json.dump(self.retirement(), proof_file)
            proof_file.flush()
            loader = MagicMock(return_value={"resume_authority": "full"})
            generation = MagicMock()
            generation.activate.return_value = {
                "event_id": "activation", "activated_plan_revision": OLD,
                "bound_graph_frontier_sha256": "1" * 64,
                "bound_candidate_resume_sha256": "2" * 64,
            }
            generation._native_root_proof.return_value = None
            root_transport = MagicMock()
            root_transport.snapshot_for_root.return_value = {
                "root": {"plan_revision": OLD},
            }
            argv = [
                "workstream_generation.py", "activate", WORKSTREAM,
                "--plan-source", plan_file.name, "--created-at", "now",
                "--operator-contract", proof_file.name, "--apply",
                "--expected-native-root-sha256", "0" * 64,
            ]
            final_candidate = {
                "graph_frontier_sha256": "1" * 64,
                "snapshot_sha256": "2" * 64,
            }
            with patch.object(sys, "argv", argv), patch(
                "workstream_generation._route_and_client",
                return_value=(self.client, AUTHORITY),
            ), patch("workstream_generation.strict_candidate_loader",
                     return_value=loader) as loader_factory, patch(
                "workstream_generation.LinearGraphQLTransport",
                return_value=root_transport,
            ), patch("workstream_generation.GenerationTransport",
                     return_value=generation), patch(
                "workstream_generation.validate_activation_operator_contract",
                return_value={
                    "authorization": {}, "retirement_proof": self.retirement(),
                    "remote_head": "e" * 40,
                },
            ), patch(
                "workstream_generation.strict_active_generation_receipt",
                return_value=({"plan_revision": OLD}, final_candidate),
            ), patch.object(
                sys, "stdout", io.StringIO(),
            ):
                self.assertEqual(main(), 0)
            self.assertEqual(loader.call_count, 0)
            self.assertEqual(loader_factory.call_args.kwargs["max_bytes"], 24 * 1024)
            self.assertEqual(loader_factory.call_args.kwargs["max_items"], 100)

    def test_strict_activation_checkpoint_keeps_child_obligations_under_default_budget(self):
        project_full(self.client, NEW)
        target = adapter(self.client, NEW)
        state = target.state()
        prior_scope = next(
            event for event in state.events
            if event["kind"] == "scope" and event["key"] == "root"
        )
        owned_scope = deepcopy(prior_scope["value"])
        owned_scope["child_ownership"]["GEN-43"] = "github.com:id:R_repo"
        target.append(build_projection_event(
            workstream_id=WORKSTREAM, kind="scope", key="root",
            value=owned_scope, plan_revision=NEW,
            expected_revision=state.revision, created_at="4",
            supersedes_event_id=prior_scope["event_id"], authority=AUTHORITY,
        ))
        checkpoint = self.activation_checkpoint()
        state = target.state()
        prior_disposition = next(
            event for event in state.events
            if event["kind"] == "disposition" and event["key"] == "root"
        )
        target.append(build_projection_event(
            workstream_id=WORKSTREAM, kind="disposition", key="root",
            value={
                "disposition": "attach", "remote_head": "e" * 40,
                "recovered_from_checkpoint": checkpoint["event_id"],
            },
            plan_revision=NEW, expected_revision=state.revision,
            created_at="5", supersedes_event_id=prior_disposition["event_id"],
            authority=AUTHORITY,
        ))
        child_event = Delta(
            "child-obligation", "GEN-43", "requirement", "agent",
            {"requirement": "Finish the physical successor canary."},
            0, "2026-08-29T00:00:00Z",
        )
        graph = {
            "root": {
                "id": AUTHORITY["root_issue_id"], "identifier": WORKSTREAM,
                "description": f"Plan revision: {OLD}",
                "url": "https://linear.test/GEN-37", "plan_revision": OLD,
                "revision": 0, "status": "In Progress",
                "next_action": "Activate the reviewed generation.",
                "team": {"id": AUTHORITY["team_id"], "organization": {
                    "id": AUTHORITY["workspace_id"],
                }},
                "project": {"id": AUTHORITY["project_id"]},
            },
            "children": [{
                "id": "44444444-4444-4444-8444-444444444444",
                "identifier": "GEN-43", "url": "https://linear.test/GEN-43",
                "title": "Physical successor canary", "status": "In Progress",
                "status_type": "started", "next_action": "Run the canary.",
                "parent": {"id": AUTHORITY["root_issue_id"],
                           "identifier": WORKSTREAM},
                "team": {"id": AUTHORITY["team_id"], "organization": {
                    "id": AUTHORITY["workspace_id"],
                }},
                "project": {"id": AUTHORITY["project_id"]},
            }],
            "decisions": [],
            "child_comments": {"GEN-43": [{
                "id": "child-obligation-comment",
                "body": encode_event_comment(child_event),
            }]},
        }
        snapshot_transport = MagicMock()
        snapshot_transport.snapshot_for_root.return_value = deepcopy(graph)
        observed = {}

        def capture_compact(snapshot, token, **kwargs):
            result = real_compact_context(snapshot, token, **kwargs)
            observed["snapshot"] = result
            return result

        with patch("workstream_generation.plan_payload", return_value={
            "source": {"identity": f"https://example.test/{NEW}",
                       "sha256": NEW},
        }), patch("workstream_generation.LinearGraphQLTransport",
                  return_value=snapshot_transport), patch(
            "workstream_generation.compact_context", side_effect=capture_compact,
        ):
            receipt = strict_candidate_loader(
                self.client, token=WORKSTREAM, authority=AUTHORITY,
                plan_source="plan", plan_identity=None,
                activation_checkpoint=checkpoint,
                activation_remote_head="e" * 40,
                activation_created_at="checkpoint-preview",
            )(NEW)

        self.assertEqual(receipt["resume_authority"], "full")
        self.assertIn(checkpoint["event_id"], receipt["checkpoint_event_ids"])
        self.assertEqual(
            observed["snapshot"]["children"][0][
                "uncheckpointed_material_obligations"
            ][0]["event_id"],
            child_event.event_id,
        )
        self.assertLess(
            len(json.dumps(observed["snapshot"], sort_keys=True).encode()),
            24 * 1024,
        )

    def test_activation_checkpoint_cli_refuses_custom_item_budget(self):
        checkpoint = self.activation_checkpoint()
        with tempfile.NamedTemporaryFile("w", suffix=".md") as plan_file, \
                tempfile.NamedTemporaryFile("w", suffix=".json") as proof_file, \
                tempfile.NamedTemporaryFile("w", suffix=".json") as checkpoint_file:
            plan_file.write("# plan\n")
            plan_file.flush()
            json.dump(self.retirement(), proof_file)
            proof_file.flush()
            json.dump(checkpoint, checkpoint_file)
            checkpoint_file.flush()
            argv = [
                "workstream_generation.py", "activate", WORKSTREAM,
                "--plan-source", plan_file.name,
                "--operator-contract", proof_file.name,
                "--activation-checkpoint", checkpoint_file.name,
                "--remote-head", "e" * 40,
                "--expected-native-root-sha256", "f" * 64,
                "--max-items", "101", "--created-at", "now", "--apply",
            ]
            stderr = io.StringIO()
            count = len(self.client.mutations)
            with patch.object(sys, "argv", argv), patch(
                "workstream_generation._route_and_client",
                return_value=(self.client, AUTHORITY),
            ), patch.object(sys, "stderr", stderr):
                self.assertEqual(main(), 2)
        self.assertIn(
            "generation_activation_checkpoint_requires_default_resume_budget",
            stderr.getvalue(),
        )
        self.assertEqual(len(self.client.mutations), count)

    def test_production_main_composes_real_bootstrap_activate_and_strict_reread(self):
        def plan_file(text):
            handle = tempfile.NamedTemporaryFile("w", suffix=".md")
            handle.write(text)
            handle.flush()
            digest = hashlib.sha256(text.encode()).hexdigest()
            return handle, digest

        def invoke(client, argv):
            stdout, stderr = io.StringIO(), io.StringIO()
            with patch.object(sys, "argv", ["workstream_generation.py", *argv]), \
                    patch("workstream_generation._route_and_client",
                          return_value=(client, AUTHORITY)), patch(
                    "workstream_generation.compact_context",
                    wraps=real_compact_context) as compact, patch.object(
                    sys, "stdout", stdout), patch.object(sys, "stderr", stderr):
                code = main()
            return code, stdout.getvalue(), stderr.getvalue(), compact

        def ready_operator_contract(client, digest, identity, created_at):
            client.graph_state_id = STARTED_STATE["id"]
            linear = LinearGraphQLTransport(
                client, workspace_id="workspace", team_id="team",
                project_id="project",
            )
            graph = linear.snapshot_for_root(
                WORKSTREAM, include_description=True,
                include_child_comments=True,
            )

            def prepare():
                return prepare_generation_operator_contract(
                    comments=deepcopy(client.comments), graph=graph,
                    workstream_id=WORKSTREAM, authority=AUTHORITY,
                    description_plan_revision=graph["root"].get(
                        "plan_revision"
                    ),
                    target_source={"identity": identity, "sha256": digest},
                    created_at=created_at, remote_head="e" * 40,
                    started_state=STARTED_STATE,
                )

            contract = prepare()
            target = adapter(client, digest)
            revision = target.state().revision
            for index, item in enumerate(
                contract["projection_preview"]["manifest"]["projection"]
            ):
                target.append(build_projection_event(
                    workstream_id=WORKSTREAM, kind=item["kind"],
                    key=item["key"], value=deepcopy(item["value"]),
                    plan_revision=digest, expected_revision=revision,
                    created_at=f"{created_at}-projection-{index}",
                    authority=AUTHORITY,
                ))
                revision += 1
            target.append(build_projection_event(
                workstream_id=WORKSTREAM, kind="disposition", key="root",
                value={
                    "disposition": "attach", "remote_head": "e" * 40,
                    "recovered_from_checkpoint": None,
                },
                plan_revision=digest, expected_revision=revision,
                created_at=f"{created_at}-disposition", authority=AUTHORITY,
            ))
            contract = prepare()
            self.assertEqual(
                contract["projection_preview"]["phase"], "activation_ready",
            )
            authorization = validate_activation_operator_contract(
                contract,
                source={"identity": identity, "sha256": digest},
                workstream_id=WORKSTREAM, authority=AUTHORITY,
                comments=deepcopy(client.comments),
                graph=linear.snapshot_for_root(
                    WORKSTREAM, include_description=True,
                    include_child_comments=True,
                ),
                description_plan_revision=graph["root"].get("plan_revision"),
                created_at=created_at, remote_head="e" * 40,
            )
            self.assertEqual(
                authorization["authorization"]["contract_sha256"],
                contract["contract_sha256"],
            )
            return contract

        bootstrap_plan, bootstrap_digest = plan_file("# Bootstrap plan\n")
        self.addCleanup(bootstrap_plan.close)
        bootstrap_client = FakeClient()
        bootstrap_client.description = "Next action: Continue."
        project_full(
            bootstrap_client, bootstrap_digest, identity=bootstrap_plan.name,
        )
        code, raw, error, compact = invoke(bootstrap_client, [
            "bootstrap", WORKSTREAM, "--plan-source", bootstrap_plan.name,
            "--plan-identity", bootstrap_plan.name,
            "--created-at", "now", "--apply",
        ])
        self.assertEqual((code, error), (0, ""))
        output = json.loads(raw)
        self.assertEqual(output["final_active_plan_revision"], bootstrap_digest)
        self.assertEqual(output["post_read_status"],
                         "authority_bound_post_read_match")
        self.assertTrue(any("generation-reservation" in item["body"]
                            for item in bootstrap_client.mutations))
        self.assertTrue(compact.called)
        self.assertTrue(all(call.kwargs["max_bytes"] == 24 * 1024
                            and call.kwargs["max_items"] == 100
                            and call.kwargs["include_history"] is False
                            for call in compact.call_args_list))

        old_plan, old_digest = plan_file("# Old plan\n")
        new_plan, new_digest = plan_file("# New plan\n")
        self.addCleanup(old_plan.close)
        self.addCleanup(new_plan.close)
        activate_client = FakeClient()
        activate_client.description = (
            f"Plan revision: {old_digest}\nNext action: Continue."
        )
        project_full(activate_client, old_digest, identity=old_plan.name)
        operator_contract = ready_operator_contract(
            activate_client, new_digest, new_plan.name, "now",
        )
        with tempfile.NamedTemporaryFile("w", suffix=".json") as proof:
            json.dump(operator_contract, proof)
            proof.flush()
            code, raw, error, compact = invoke(activate_client, [
                "activate", WORKSTREAM, "--plan-source", new_plan.name,
                "--plan-identity", new_plan.name,
                "--operator-contract", proof.name,
                "--created-at", "now", "--apply",
                "--expected-native-root-sha256",
                self.native_root_sha(activate_client),
            ])
        self.assertEqual((code, error), (0, ""))
        output = json.loads(raw)
        self.assertEqual(output["final_active_plan_revision"], new_digest)
        self.assertEqual(select_plan_generation(
            activate_client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=old_digest, authenticated_route=AUTHORITY,
        )["plan_revision"], new_digest)
        self.assertTrue(compact.called)

        checkpoint_client = FakeClient()
        checkpoint_client.description = (
            f"Plan revision: {old_digest}\nNext action: Continue."
        )
        project_full(checkpoint_client, old_digest, identity=old_plan.name)
        checkpoint_contract = ready_operator_contract(
            checkpoint_client, new_digest, new_plan.name, "checkpoint",
        )
        activation_checkpoint = build_checkpoint(
            workstream_id=WORKSTREAM, boundary_id="production-main-activation",
            root_revision=0, plan_revision=new_digest,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai",
                "session_id": "production-main", "machine": "M5",
                "worktree": {
                    "state": "safe", "path": "/tmp/production-main",
                    "branch": "activation", "head": "e" * 40,
                },
            },
            exact_head="e" * 40, evidence=[], blocker=None,
            next_action="Continue after atomic activation.",
        )
        with tempfile.NamedTemporaryFile("w", suffix=".json") as proof, \
                tempfile.NamedTemporaryFile("w", suffix=".json") as checkpoint_file:
            json.dump(checkpoint_contract, proof)
            proof.flush()
            json.dump(activation_checkpoint, checkpoint_file)
            checkpoint_file.flush()
            code, raw, error, compact = invoke(checkpoint_client, [
                "activate", WORKSTREAM, "--plan-source", new_plan.name,
                "--plan-identity", new_plan.name,
                "--operator-contract", proof.name,
                "--activation-checkpoint", checkpoint_file.name,
                "--remote-head", "e" * 40,
                "--created-at", "checkpoint", "--apply",
                "--expected-native-root-sha256",
                self.native_root_sha(checkpoint_client),
            ])
        self.assertEqual((code, error), (0, ""))
        checkpoint_output = json.loads(raw)
        self.assertEqual(checkpoint_output["final_active_plan_revision"], new_digest)
        transition = adapter(checkpoint_client, old_digest).state().events[-1]
        self.assertEqual(transition["value"]["schema_version"], 4)
        self.assertEqual(
            checkpoint_output["two_phase_finalization"]["execution_status"],
            {"authority": "generation_local", "name": "In Progress",
             "type": "started"},
        )
        selected = select_plan_generation(
            checkpoint_client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=old_digest, authenticated_route=AUTHORITY,
        )
        carried = selected_activation_checkpoints(
            checkpoint_client.comments, workstream_id=WORKSTREAM,
            transition_event_id=selected["transition_tip_event_id"],
            active_plan_revision=new_digest, authenticated_route=AUTHORITY,
        )
        self.assertEqual(carried[0][0]["event_id"], activation_checkpoint["event_id"])
        self.assertTrue(all(
            call.kwargs["max_bytes"] == 24 * 1024
            for call in compact.call_args_list
        ))

        later_plan, later_digest = plan_file("# Later plan\n")
        self.addCleanup(later_plan.close)
        later_contract = ready_operator_contract(
            activate_client, later_digest, later_plan.name, "later",
        )
        with tempfile.NamedTemporaryFile("w", suffix=".json") as proof:
            json.dump(later_contract, proof)
            proof.flush()
            code, _raw, error, _compact = invoke(activate_client, [
                "activate", WORKSTREAM, "--plan-source", later_plan.name,
                "--plan-identity", later_plan.name,
                "--operator-contract", proof.name,
                "--created-at", "later", "--apply",
                "--expected-native-root-sha256",
                self.native_root_sha(activate_client),
            ])
        self.assertEqual((code, error), (0, ""))
        with tempfile.NamedTemporaryFile("w", suffix=".json") as proof:
            json.dump(operator_contract, proof)
            proof.flush()
            code, raw, error, _compact = invoke(activate_client, [
                "activate", WORKSTREAM, "--plan-source", new_plan.name,
                "--plan-identity", new_plan.name,
                "--operator-contract", proof.name,
                "--created-at", "now", "--apply",
                "--expected-native-root-sha256",
                self.native_root_sha(activate_client),
            ])
        self.assertEqual((code, error), (0, ""))
        replay_output = json.loads(raw)
        self.assertEqual(replay_output["final_active_plan_revision"], later_digest)
        self.assertEqual(replay_output["post_read_status"],
                         "historical_replay_active_generation_advanced")

        drift_client = FakeClient()
        drift_client.description = (
            f"Plan revision: {old_digest}\nNext action: Continue."
        )
        project_full(drift_client, old_digest, identity=old_plan.name)
        drift_contract = ready_operator_contract(
            drift_client, new_digest, new_plan.name, "now",
        )

        def drift_at_authority_create(item, client):
            if PROJECTION_PREFIX not in item["body"]:
                return
            event = _decode_projection(PROJECTION_RE.findall(item["body"])[0])
            if event["kind"] == "generation_transition":
                client.graph_status = "Drifted after bind"

        drift_client.before_each_create = drift_at_authority_create
        with tempfile.NamedTemporaryFile("w", suffix=".json") as proof:
            json.dump(drift_contract, proof)
            proof.flush()
            code, _raw, error, _compact = invoke(drift_client, [
                "activate", WORKSTREAM, "--plan-source", new_plan.name,
                "--plan-identity", new_plan.name,
                "--operator-contract", proof.name,
                "--created-at", "now", "--apply",
                "--expected-native-root-sha256",
                self.native_root_sha(drift_client),
            ])
        self.assertEqual(code, 2)
        self.assertRegex(
            error,
            "authority_changed_with_post_read_drift|"
            "generation_native_root_review_proof_mismatch|"
            "generation_prepared_candidate_changed",
        )

    def test_production_cli_surface_has_bootstrap_activate_apply_and_abort(self):
        parsed = parser().parse_args([
            "bootstrap", WORKSTREAM, "--plan-source", "plan.md",
            "--created-at", "now", "--apply",
        ])
        self.assertEqual((parsed.command, parsed.apply), ("bootstrap", True))
        parsed = parser().parse_args([
            "activate", WORKSTREAM, "--created-at", "now", "--apply",
            "--abort-reservation-id", "wsgr_" + "1" * 32,
            "--abort-reservation-sha256", "2" * 64, "--abort-reason", "reviewed",
        ])
        self.assertEqual((parsed.command, parsed.abort_reason), ("activate", "reviewed"))

    def test_projection_cli_binding_treats_description_as_diagnostic(self):
        graph = {"root": {
            "plan_revision": OLD, "identifier": WORKSTREAM,
            "status": "Done", "status_type": "completed",
        }}
        candidate = bind_projection_plan_generation(
            graph, self.client.comments, workstream_id=WORKSTREAM,
            requested_plan_revision=NEW, authenticated_route=AUTHORITY,
        )
        self.assertEqual(candidate["root"]["plan_revision"], NEW)
        self.assertEqual(candidate["root"]["description_plan_revision"], OLD)
        project_full(self.client, NEW)
        transport = self.native_and_source_fenced_transport({
            "identity": f"https://example.test/{NEW}", "sha256": NEW,
        })
        proof = transport.preview_activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
        )["native_root_activation_proof"]
        transport.activate(
            target_plan_revision=NEW, created_at="now",
            retirement=self.retirement(),
            expected_native_root_sha256=proof["sha256"],
        )
        active = bind_projection_plan_generation(
            graph, self.client.comments, workstream_id=WORKSTREAM,
            requested_plan_revision=NEW, authenticated_route=AUTHORITY,
        )
        self.assertEqual(active["root"]["plan_revision"], NEW)
        self.assertEqual(active["root"]["status"], "In Progress")
        self.assertEqual(active["root"]["status_type"], "started")
        self.assertEqual(active["root"]["issue_status"], "Done")
        self.assertEqual(active["root"]["issue_status_type"], "completed")
        with self.assertRaisesRegex(LinearProjectionError, "plan_retired"):
            bind_projection_plan_generation(
                graph, self.client.comments, workstream_id=WORKSTREAM,
                requested_plan_revision=OLD, authenticated_route=AUTHORITY,
            )

    def test_descriptionless_projection_candidate_can_be_built_before_genesis(self):
        graph = {"root": {"plan_revision": None, "identifier": WORKSTREAM}}
        candidate = bind_projection_plan_generation(
            graph, self.client.comments, workstream_id=WORKSTREAM,
            requested_plan_revision=OLD, authenticated_route=AUTHORITY,
        )
        self.assertEqual(candidate["root"]["plan_revision"], OLD)
        self.assertIsNone(candidate["root"]["description_plan_revision"])


if __name__ == "__main__":
    unittest.main()
