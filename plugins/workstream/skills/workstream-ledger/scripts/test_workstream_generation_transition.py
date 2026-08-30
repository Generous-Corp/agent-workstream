#!/usr/bin/env python3
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import json
import unittest
from unittest import mock

from workstream_linear import LinearTransportError
from workstream_checkpoint import build_checkpoint
from workstream_delta import Delta
from workstream_linear_checkpoints import encode_checkpoint_comment
from workstream_linear_events import encode_event_comment
from workstream_linear_projection import (
    _canonical, _generation_frontier, build_projection_event,
    encode_projection_comment, LinearProjectionAdapter, LinearProjectionError,
    projection_slot_id, reduce_projection_comments, select_plan_generation,
)
from workstream_resume import add_material_history, compact_context
import workstream_linear_projection as projection_module


WORKSTREAM = "GEN-37"
OLD = "a" * 64
NEW = "b" * 64
LATER = "c" * 64
OTHER = "d" * 64
AUTHORITY = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
    "root_issue_id": "33333333-3333-4333-8333-333333333333",
}


class FakeClient:
    def __init__(self):
        self.comments: list[dict] = []

    def execute(self, query, variables):
        if "CommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}]}}
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
            item = variables["input"]
            if any(comment["id"] == item["id"] for comment in self.comments):
                raise LinearTransportError("duplicate comment id")
            comment = {
                "id": item["id"], "body": item["body"],
                "createdAt": "2026-08-29T00:00:00Z",
                "updatedAt": "2026-08-29T00:00:00Z",
            }
            self.comments.append(comment)
            return {"commentCreate": {"success": True, "comment": deepcopy(comment)}}
        raise AssertionError("unexpected operation")


class RacingClient(FakeClient):
    def __init__(self, comments, winner):
        super().__init__()
        self.comments = deepcopy(comments)
        self.winner = winner
        self.injected = False

    def execute(self, query, variables):
        if "commentCreate" in query and not self.injected:
            self.injected = True
            self.comments.append({
                "id": variables["input"]["id"],
                "body": encode_projection_comment(self.winner),
                "createdAt": "2026-08-29T00:01:00Z",
                "updatedAt": "2026-08-29T00:01:00Z",
            })
            raise LinearTransportError("duplicate comment id")
        return super().execute(query, variables)


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
            "namespace": "generation-transition-tests",
            "linear": {
                **AUTHORITY,
                "route_verification": {
                    **AUTHORITY, "observed_at": "2026-08-29T00:00:00Z",
                    "evidence": [{
                        "kind": "authenticated_linear_readback", "authenticated": True,
                        **AUTHORITY,
                    }],
                },
            },
            "primary_repository": "github.com:id:R_agent_workstream",
            "repositories": [{
                "slug": "github.com/generous-corp/agent-workstream",
                "provider_repository_id": "R_agent_workstream", "aliases": [],
                "exact_head": "e" * 40,
                "identity_resolution": {
                    "provider_repository_id": "R_agent_workstream",
                    "resolved_slug": "github.com/generous-corp/agent-workstream",
                    "observed_at": "2026-08-29T00:00:00Z",
                    "evidence": [{
                        "kind": "authenticated_provider_readback", "authenticated": True,
                        "provider_repository_id": "R_agent_workstream",
                        "resolved_slug": "github.com/generous-corp/agent-workstream",
                    }],
                },
                "identity_updates": [], "evidence": [],
            }],
            "child_ownership": {},
        }, plan_revision=plan,
        expected_revision=0, created_at="2026-08-29T00:00:00Z",
        authority=AUTHORITY,
    ))
    target.append(build_projection_event(
        workstream_id=WORKSTREAM, kind="source", key="root",
        value={"identity": identity or f"https://example.test/{plan}", "sha256": plan},
        plan_revision=plan, expected_revision=1,
        created_at="2026-08-29T00:00:01Z", authority=AUTHORITY,
    ))
    target.append(build_projection_event(
        workstream_id=WORKSTREAM, kind="provenance", key="generation",
        value={
            "agent": "codex", "machine": "M5", "session_id": plan[:8],
            "worktree": {"state": "safe", "head": "e" * 40},
        },
        plan_revision=plan, expected_revision=2,
        created_at="2026-08-29T00:00:02Z", authority=AUTHORITY,
    ))
    target.append(build_projection_event(
        workstream_id=WORKSTREAM, kind="disposition", key="root",
        value={
            "disposition": "attach", "remote_head": "e" * 40,
            "recovered_from_checkpoint": None,
        },
        plan_revision=plan, expected_revision=3,
        created_at="2026-08-29T00:00:03Z", authority=AUTHORITY,
    ))
    return target


class GenerationTransitionTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        project_full(self.client, OLD)

    def activate(self, target=NEW, predecessor=OLD):
        project_full(self.client, target)
        return adapter(self.client, predecessor).activate_generation(
            target_plan_revision=target,
            created_at="2026-08-29T00:01:00Z",
            predecessor_sessions_retired=True,
        )

    def select(self, description=OLD):
        return select_plan_generation(
            self.client.comments, workstream_id=WORKSTREAM,
            description_plan_revision=description,
            authenticated_route=AUTHORITY,
        )

    def test_legacy_and_candidate_before_activation_remain_description_selected(self):
        self.assertEqual(self.select()["plan_revision"], OLD)
        project_full(self.client, NEW)
        self.assertEqual(self.select()["plan_revision"], OLD)
        self.assertEqual(self.select(NEW)["plan_revision"], NEW)

    def test_activation_ignores_stale_or_edited_description_and_replays(self):
        receipt = self.activate()
        self.assertEqual(self.select(OLD)["plan_revision"], NEW)
        self.assertEqual(self.select(OTHER)["plan_revision"], NEW)
        replay = adapter(self.client, OLD).activate_generation(
            target_plan_revision=NEW, created_at="different-but-ignored",
            predecessor_sessions_retired=True,
        )
        self.assertEqual(replay["event_id"], receipt["event_id"])
        self.assertEqual(len([
            event for event in reduce_projection_comments(
                self.client.comments, workstream_id=WORKSTREAM,
                expected_plan_revision=OLD, authenticated_route=AUTHORITY,
            ).events if event["kind"] == "generation_transition"
        ]), 1)

    def test_pre_transition_reducer_fails_closed_on_new_control_kind(self):
        self.activate()
        body = next(
            comment["body"] for comment in self.client.comments
            if adapter(self.client, OLD).state().remote_ids.get(
                adapter(self.client, OLD).state().events[-1]["event_id"]
            ) == comment["id"]
        )
        encoded = body.split(":v1:", 1)[1].split(" -->", 1)[0]
        with mock.patch.object(
            projection_module, "KINDS",
            projection_module.KINDS - {"generation_transition"},
        ):
            with self.assertRaisesRegex(
                LinearProjectionError, "malformed_projection_marker",
            ):
                projection_module._decode_projection(encoded)

    def test_activation_then_full_resume_uses_transition_tip(self):
        self.activate()
        late_material = Delta(
            "late-material", WORKSTREAM, "requirement", "old-or-new-session",
            {"requirement": "This append must remain visible.",
             "next_action": "Handle the visible late append."},
            0, "2026-08-29T00:01:01Z",
        )
        self.client.comments.append({
            "id": "late-material-comment", "body": encode_event_comment(late_material),
        })
        generation = self.select(OTHER)
        snapshot = {
            "root": {
                "identifier": WORKSTREAM, "url": "https://linear/GEN-37",
                "plan_revision": generation["plan_revision"], "revision": 0,
                "status": "In Progress", "next_action": "Continue.",
                "description_plan_revision": generation["description_plan_revision"],
                "generation_transition_tip_event_id": generation[
                    "transition_tip_event_id"
                ],
            },
            "children": [], "decisions": [], "provenance": [],
        }
        source = {"identity": f"https://example.test/{NEW}", "sha256": NEW}
        enriched = add_material_history(
            snapshot, self.client.comments, WORKSTREAM,
            authenticated_route=AUTHORITY, authenticated_source=source,
        )
        context = compact_context(
            enriched, WORKSTREAM, require_projection_authority=True,
        )
        self.assertEqual(context["plan_revision"], NEW)
        self.assertEqual(context["description_plan_revision"], OTHER)
        self.assertEqual(context["material_event_revision"], 1)
        self.assertEqual(context["next_action"], "Handle the visible late append.")
        self.assertEqual(len(context["uncheckpointed_material_obligations"]), 1)
        self.assertEqual(
            context["generation_transition_tip_event_id"],
            generation["transition_tip_event_id"],
        )

    def test_late_predecessor_checkpoint_refuses_instead_of_being_omitted(self):
        self.activate()
        checkpoint = build_checkpoint(
            workstream_id=WORKSTREAM, boundary_id="late-old", root_revision=0,
            plan_revision=OLD, before_status="In Progress",
            after_status="In Progress", execution={
                "agent": "codex", "provider": "openai", "session_id": "old",
                "machine": "M5", "worktree": {"state": "unknown"},
            }, exact_head=None, evidence=[], blocker=None,
            next_action="Old predecessor action.",
        )
        self.client.comments.append({
            "id": "late-old-checkpoint", "body": encode_checkpoint_comment(checkpoint),
        })
        with self.assertRaisesRegex(
            LinearProjectionError, "frontier_mismatch|checkpoint_changed",
        ):
            self.select()

    def test_late_target_projection_refuses_exact_frontier(self):
        self.activate()
        target = adapter(self.client, NEW)
        target.append(build_projection_event(
            workstream_id=WORKSTREAM, kind="provenance", key="late-target",
            value={
                "agent": "codex", "machine": "M5", "session_id": "late-target",
                "worktree": {"state": "safe", "head": "e" * 40},
            }, plan_revision=NEW, expected_revision=target.state().revision,
            created_at="late", authority=AUTHORITY,
        ))
        with self.assertRaisesRegex(
            LinearProjectionError, "tip_projection_changed",
        ):
            self.select()

    def test_concurrent_candidate_has_one_winner_and_retirement_is_required(self):
        project_full(self.client, NEW)
        project_full(self.client, OTHER)
        with self.assertRaisesRegex(
            LinearProjectionError, "predecessor_sessions_not_retired",
        ):
            adapter(self.client, OLD).activate_generation(
                target_plan_revision=NEW, created_at="now",
                predecessor_sessions_retired=False,
            )
        adapter(self.client, OLD).activate_generation(
            target_plan_revision=NEW, created_at="now",
            predecessor_sessions_retired=True,
        )
        with self.assertRaisesRegex(LinearProjectionError, "already_activated"):
            adapter(self.client, OLD).activate_generation(
                target_plan_revision=OTHER, created_at="now",
                predecessor_sessions_retired=True,
            )
        self.assertEqual(self.select()["plan_revision"], NEW)

    def test_concurrent_projection_slot_race_has_exactly_one_winner(self):
        project_full(self.client, NEW)
        project_full(self.client, OTHER)
        old_state = adapter(self.client, OLD).state()
        new_state = adapter(self.client, NEW).state()
        winner = build_projection_event(
            workstream_id=WORKSTREAM, kind="generation_transition", key="root",
            value={
                "from": _generation_frontier(
                    old_state, self.client.comments, plan_revision=OLD,
                    material_revision=0,
                ),
                "to": _generation_frontier(
                    new_state, self.client.comments, plan_revision=NEW,
                    material_revision=0,
                ),
                "previous_transition_event_id": None,
            },
            plan_revision=OLD, expected_revision=old_state.revision,
            created_at="winner", authority=AUTHORITY,
        )
        racing = RacingClient(self.client.comments, winner)
        with self.assertRaisesRegex(
            LinearProjectionError, "projection_slot_lost_reload_required",
        ):
            adapter(racing, OLD).activate_generation(
                target_plan_revision=OTHER, created_at="loser",
                predecessor_sessions_retired=True,
            )
        selected = select_plan_generation(
            racing.comments, workstream_id=WORKSTREAM,
            description_plan_revision=OLD, authenticated_route=AUTHORITY,
        )
        self.assertEqual(selected["plan_revision"], NEW)
        self.assertEqual(selected["transition_tip_event_id"], winner["event_id"])

    def test_incomplete_target_and_tampered_marker_refuse(self):
        incomplete = adapter(self.client, NEW)
        incomplete.append(build_projection_event(
            workstream_id=WORKSTREAM, kind="source", key="root",
            value={"identity": "new", "sha256": NEW}, plan_revision=NEW,
            expected_revision=0, created_at="now", authority=AUTHORITY,
        ))
        with self.assertRaisesRegex(LinearProjectionError, "target_incomplete"):
            adapter(self.client, OLD).activate_generation(
                target_plan_revision=NEW, created_at="now",
                predecessor_sessions_retired=True,
            )
        body = self.client.comments[-1]["body"]
        encoded = body.split(":v1:", 1)[1].split(" -->", 1)[0]
        envelope = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        envelope["event"]["value"]["sha256"] = OTHER
        changed = base64.urlsafe_b64encode(json.dumps(
            envelope, sort_keys=True, separators=(",", ":"),
        ).encode()).decode().rstrip("=")
        self.client.comments[-1]["body"] = f"<!-- workstream-projection:v1:{changed} -->"
        with self.assertRaisesRegex(LinearProjectionError, "malformed_projection_marker"):
            self.select()

    def test_fork_orphan_cycle_shape_refuses(self):
        self.activate()
        transition_comment = next(
            comment for comment in self.client.comments
            if "generation_transition" in str(
                json.loads(base64.urlsafe_b64decode(
                    comment["body"].split(":v1:", 1)[1].split(" -->", 1)[0]
                    + "=" * (-len(comment["body"].split(":v1:", 1)[1].split(" -->", 1)[0]) % 4)
                )).get("event", {}).get("kind", "")
            )
        )
        # A second root is an explicit fork/ambiguous-root refusal.
        original = adapter(self.client, OLD).state().events[-1]
        fork = deepcopy(original)
        fork["value"]["to"]["plan_revision"] = OTHER
        fork["value"]["to"]["source_sha256"] = OTHER
        fork["event_id"] = "wsp_" + hashlib.sha256(_canonical(
            {key: value for key, value in fork.items() if key != "event_id"}
        )).hexdigest()[:32]
        self.client.comments.append({
            "id": "synthetic-fork", "body": encode_projection_comment(fork),
        })
        with self.assertRaises(LinearProjectionError):
            self.select()
        self.client.comments.pop()
        # Missing/self previous links cover orphan/cycle-shaped chains and fail closed.
        for previous in ("wsp_" + "1" * 32, original["event_id"]):
            changed = deepcopy(original)
            changed["value"]["previous_transition_event_id"] = previous
            changed["event_id"] = "wsp_" + hashlib.sha256(_canonical(
                {key: value for key, value in changed.items() if key != "event_id"}
            )).hexdigest()[:32]
            self.client.comments[-1] = {
                "id": transition_comment["id"],
                "body": encode_projection_comment(changed),
            }
            with self.assertRaises(LinearProjectionError):
                self.select()
        self.client.comments[-1] = transition_comment

    def test_later_transition_forms_one_chain_and_late_predecessor_append_refuses(self):
        self.activate()
        project_full(self.client, LATER)
        adapter(self.client, NEW).activate_generation(
            target_plan_revision=LATER, created_at="2026-08-29T00:02:00Z",
            predecessor_sessions_retired=True,
        )
        self.assertEqual(self.select(OTHER)["plan_revision"], LATER)
        comment_count = len(self.client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError, "target_not_candidate",
        ):
            adapter(self.client, LATER).activate_generation(
                target_plan_revision=OLD, created_at="cycle",
                predecessor_sessions_retired=True,
            )
        self.assertEqual(len(self.client.comments), comment_count)

        old = adapter(self.client, OLD).state()
        late = build_projection_event(
            workstream_id=WORKSTREAM, kind="provenance", key="late-old-session",
            value={
                "agent": "codex", "machine": "M5", "session_id": "old",
                "worktree": {"state": "safe", "head": "e" * 40},
            }, plan_revision=OLD,
            expected_revision=old.revision, created_at="late", authority=AUTHORITY,
        )
        self.client.comments.append({
            "id": projection_slot_id(WORKSTREAM, OLD, old.revision, AUTHORITY),
            "body": encode_projection_comment(late),
        })
        with self.assertRaisesRegex(LinearProjectionError, "not_last"):
            self.select()


if __name__ == "__main__":
    unittest.main()
