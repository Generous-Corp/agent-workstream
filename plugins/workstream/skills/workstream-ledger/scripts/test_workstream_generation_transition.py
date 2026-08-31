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

from workstream_delta import Delta
from workstream_generation import (
    GenerationTransport, WorkstreamGenerationError, _digest,
    build_retirement_proof, generation_quarantine_metadata, main, parser,
    pending_generation_reservations, reduce_generation_checkpoint_comments,
    selected_activation_checkpoints,
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


WORKSTREAM = "GEN-37"
OLD, NEW, LATER, OTHER = (letter * 64 for letter in "abcd")
AUTHORITY = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
    "root_issue_id": "33333333-3333-4333-8333-333333333333",
}


class FakeClient:
    def __init__(self):
        self.comments: list[dict] = []
        self.mutations: list[dict] = []
        self.commit_then_fail_at: set[int] = set()
        self.before_each_create = None
        self.description = f"Plan revision: {OLD}"
        self.graph_nonce = "initial"
        self.graph_status = "In Progress"
        self.children: list[dict] = []
        self.before_issue_create = None

    def root_issue(self):
        return {
            "id": AUTHORITY["root_issue_id"], "identifier": WORKSTREAM,
            "title": "Generation test", "description": self.description,
            "url": "https://linear.test/GEN-37", "updatedAt": self.graph_nonce,
            "parent": None,
            "project": {"id": "project"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "assignee": None,
            "state": {"id": "state", "name": self.graph_status,
                      "type": "started"},
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
        if "issueUpdate" in query:
            raise AssertionError("generation protocol must never issueUpdate")
        if "CommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}]}}
        if "query WorkstreamResumeRoot" in query:
            return {"issue": {**self.root_issue(), "children": {
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
        if "commentCreate" in query:
            item = deepcopy(variables["input"])
            if self.before_each_create is not None:
                self.before_each_create(item, self)
            if any(comment["id"] == item["id"] for comment in self.comments):
                raise LinearTransportError("duplicate comment id")
            self.mutations.append(item)
            comment = {
                "id": item["id"], "body": item["body"],
                "createdAt": "2026-08-29T00:00:00Z",
                "updatedAt": "2026-08-29T00:00:00Z",
            }
            self.comments.append(comment)
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
        graph = {"root": {"identifier": WORKSTREAM}, "children": [],
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
            root_transport = MagicMock()
            root_transport.snapshot_for_root.return_value = {
                "root": {"plan_revision": OLD},
            }
            argv = [
                "workstream_generation.py", "activate", WORKSTREAM,
                "--plan-source", plan_file.name, "--created-at", "now",
                "--retirement-proof", proof_file.name, "--apply",
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
                "url": "https://linear.test/GEN-37", "plan_revision": OLD,
                "revision": 0, "status": "In Progress",
                "next_action": "Activate the reviewed generation.",
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
                "--retirement-proof", proof_file.name,
                "--activation-checkpoint", checkpoint_file.name,
                "--remote-head", "e" * 40,
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
        project_full(activate_client, new_digest, identity=new_plan.name)
        old_state = adapter(activate_client, old_digest).state()
        retirement = build_retirement_proof(
            predecessor_plan_revision=old_digest, retired_at="now",
            retired_writer_epoch=0,
            provenance_event_ids=[event["event_id"] for event in old_state.events
                                  if event["kind"] == "provenance"],
            checkpoint_event_ids=[],
        )
        with tempfile.NamedTemporaryFile("w", suffix=".json") as proof:
            json.dump(retirement, proof)
            proof.flush()
            code, raw, error, compact = invoke(activate_client, [
                "activate", WORKSTREAM, "--plan-source", new_plan.name,
                "--plan-identity", new_plan.name,
                "--retirement-proof", proof.name,
                "--created-at", "now", "--apply",
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
        project_full(checkpoint_client, new_digest, identity=new_plan.name)
        checkpoint_old = adapter(checkpoint_client, old_digest).state()
        checkpoint_retirement = build_retirement_proof(
            predecessor_plan_revision=old_digest, retired_at="checkpoint",
            retired_writer_epoch=0,
            provenance_event_ids=[
                event["event_id"] for event in checkpoint_old.events
                if event["kind"] == "provenance"
            ],
            checkpoint_event_ids=[],
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
            json.dump(checkpoint_retirement, proof)
            proof.flush()
            json.dump(activation_checkpoint, checkpoint_file)
            checkpoint_file.flush()
            code, raw, error, compact = invoke(checkpoint_client, [
                "activate", WORKSTREAM, "--plan-source", new_plan.name,
                "--plan-identity", new_plan.name,
                "--retirement-proof", proof.name,
                "--activation-checkpoint", checkpoint_file.name,
                "--remote-head", "e" * 40,
                "--created-at", "checkpoint", "--apply",
            ])
        self.assertEqual((code, error), (0, ""))
        checkpoint_output = json.loads(raw)
        self.assertEqual(checkpoint_output["final_active_plan_revision"], new_digest)
        transition = adapter(checkpoint_client, old_digest).state().events[-1]
        self.assertEqual(transition["value"]["schema_version"], 3)
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
        project_full(activate_client, later_digest, identity=later_plan.name)
        new_state = adapter(activate_client, new_digest).state()
        later_retirement = build_retirement_proof(
            predecessor_plan_revision=new_digest, retired_at="later",
            retired_writer_epoch=1,
            provenance_event_ids=[event["event_id"] for event in new_state.events
                                  if event["kind"] == "provenance"],
            checkpoint_event_ids=[],
        )
        with tempfile.NamedTemporaryFile("w", suffix=".json") as proof:
            json.dump(later_retirement, proof)
            proof.flush()
            code, _raw, error, _compact = invoke(activate_client, [
                "activate", WORKSTREAM, "--plan-source", later_plan.name,
                "--plan-identity", later_plan.name,
                "--retirement-proof", proof.name,
                "--created-at", "later", "--apply",
            ])
        self.assertEqual((code, error), (0, ""))
        with tempfile.NamedTemporaryFile("w", suffix=".json") as proof:
            json.dump(retirement, proof)
            proof.flush()
            code, raw, error, _compact = invoke(activate_client, [
                "activate", WORKSTREAM, "--plan-source", new_plan.name,
                "--plan-identity", new_plan.name,
                "--retirement-proof", proof.name,
                "--created-at", "now", "--apply",
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
        project_full(drift_client, new_digest, identity=new_plan.name)

        def drift_at_authority_create(item, client):
            if PROJECTION_PREFIX not in item["body"]:
                return
            event = _decode_projection(PROJECTION_RE.findall(item["body"])[0])
            if event["kind"] == "generation_transition":
                client.graph_status = "Drifted after bind"

        drift_client.before_each_create = drift_at_authority_create
        drift_state = adapter(drift_client, old_digest).state()
        drift_retirement = build_retirement_proof(
            predecessor_plan_revision=old_digest, retired_at="now",
            retired_writer_epoch=0,
            provenance_event_ids=[event["event_id"] for event in drift_state.events
                                  if event["kind"] == "provenance"],
            checkpoint_event_ids=[],
        )
        with tempfile.NamedTemporaryFile("w", suffix=".json") as proof:
            json.dump(drift_retirement, proof)
            proof.flush()
            code, _raw, error, _compact = invoke(drift_client, [
                "activate", WORKSTREAM, "--plan-source", new_plan.name,
                "--plan-identity", new_plan.name,
                "--retirement-proof", proof.name,
                "--created-at", "now", "--apply",
            ])
        self.assertEqual(code, 2)
        self.assertIn("authority_changed_with_post_read_drift", error)

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
        graph = {"root": {"plan_revision": OLD, "identifier": WORKSTREAM}}
        candidate = bind_projection_plan_generation(
            graph, self.client.comments, workstream_id=WORKSTREAM,
            requested_plan_revision=NEW, authenticated_route=AUTHORITY,
        )
        self.assertEqual(candidate["root"]["plan_revision"], NEW)
        self.assertEqual(candidate["root"]["description_plan_revision"], OLD)
        self.activate()
        active = bind_projection_plan_generation(
            graph, self.client.comments, workstream_id=WORKSTREAM,
            requested_plan_revision=NEW, authenticated_route=AUTHORITY,
        )
        self.assertEqual(active["root"]["plan_revision"], NEW)
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
