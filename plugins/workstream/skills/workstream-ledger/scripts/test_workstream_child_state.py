#!/usr/bin/env python3
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest import mock

from workstream_checkpoint import build_checkpoint
from workstream_child_proposal import (
    _append_proposal, build_proposal, encode_proposal, proposal_slot_id,
)
from workstream_delta import Delta
import workstream_child_checkpoint
import workstream_child_event
import workstream_child_proposal_activate
from workstream_linear import LinearTransportError
from workstream_linear_events import (
    assert_no_pending_ledger_reservation,
    encode_event_comment, encode_ledger_reservation, ledger_boundary_slot_id,
    LinearEventError, reduce_event_comments, reduce_ledger_reservations,
    semantic_ledger_reservations, SERIALIZATION_PREFIX, SERIALIZATION_RE,
)
from workstream_generation import (
    _digest, build_retirement_proof, encode_generation_reservation,
    generation_ledger_frontier_tokens,
)
from workstream_linear_projection import (
    _generation_frontier, build_projection_event, encode_projection_comment,
    LinearProjectionAdapter, projection_slot_id, reduce_projection_comments,
    TOMBSTONE,
)
from workstream_resume import add_child_material_history, compact_context


PLAN = "a" * 64
ROOT_ID = "11111111-1111-4111-8111-111111111111"
CHILD_ID = "07104fd8-924f-40d8-b7e2-fe2f87f76657"
ROUTE = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
}


def decode_reservation(body):
    encoded = SERIALIZATION_RE.findall(body)[0]
    return json.loads(base64.urlsafe_b64decode(
        encoded + "=" * (-len(encoded) % 4)
    ))["reservation"]


def encode_forged_reservation_digest(body):
    encoded = SERIALIZATION_RE.findall(body)[0]
    envelope = json.loads(base64.urlsafe_b64decode(
        encoded + "=" * (-len(encoded) % 4)
    ))
    envelope["sha256"] = "0" * 64
    forged = base64.urlsafe_b64encode(json.dumps(
        envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()).decode().rstrip("=")
    return f"{SERIALIZATION_PREFIX}{forged} -->"


def install_active_generation(client):
    """Install a real reservation-backed genesis like the live GEN-37 root."""
    authority = {**ROUTE, "root_issue_id": ROOT_ID}
    provenance = build_projection_event(
        workstream_id="GEN-37", kind="provenance", key="generation",
        value={"agent": "codex", "machine": "M5", "session_id": "fixture"},
        plan_revision=PLAN, expected_revision=3,
        created_at="2026-08-30T00:00:03Z", authority=authority,
    )
    disposition = build_projection_event(
        workstream_id="GEN-37", kind="disposition", key="root",
        value={"disposition": "attach", "remote_head": "e" * 40,
               "recovered_from_checkpoint": None},
        plan_revision=PLAN, expected_revision=4,
        created_at="2026-08-30T00:00:04Z", authority=authority,
    )
    for event in (provenance, disposition):
        client.root_comments.append({
            "id": projection_slot_id(
                event["workstream_id"], event["plan_revision"],
                event["expected_revision"], event["authority"],
            ),
            "body": encode_projection_comment(event),
            "createdAt": event["created_at"], "updatedAt": event["created_at"],
        })
    state = reduce_projection_comments(
        client.root_comments, workstream_id="GEN-37",
        expected_plan_revision=PLAN, authenticated_route=authority,
    )
    frontier = _generation_frontier(
        state, client.root_comments, plan_revision=PLAN,
        projection_revision=5, material_revision=0,
    )
    retirement = build_retirement_proof(
        predecessor_plan_revision=PLAN,
        retired_at="2026-08-30T00:00:05Z", retired_writer_epoch=0,
        provenance_event_ids=[provenance["event_id"]], checkpoint_event_ids=[],
    )
    unsigned = {
        "schema_version": 2, "workstream_id": "GEN-37",
        "authority": authority, "mode": "bootstrap",
        "from_plan_revision": PLAN, "to_plan_revision": PLAN,
        "activation_epoch": 0, "previous_control_event_id": None,
        "source": {"identity": "https://example.test/plan", "sha256": PLAN},
        "material_revision": 0, "checkpoint_event_ids": [],
        "ledger_frontier": [], "from_projection_revision": 5,
        "to_projection_revision": 5, "graph_frontier_sha256": "b" * 64,
        "candidate_resume_sha256": "c" * 64,
        "retirement": retirement, "created_at": "2026-08-30T00:00:05Z",
    }
    reservation = {
        **unsigned, "reservation_id": "wsgr_" + _digest(unsigned)[:32],
    }
    reservation_sha256 = _digest(reservation)
    client.root_comments.append({
        "id": ledger_boundary_slot_id("GEN-37", 0, [], authority),
        "body": encode_generation_reservation(reservation),
        "createdAt": reservation["created_at"],
        "updatedAt": reservation["created_at"],
    })
    genesis = build_projection_event(
        workstream_id="GEN-37", kind="generation_genesis", key="root",
        value={
            "schema_version": 2,
            "reservation_id": reservation["reservation_id"],
            "reservation_sha256": reservation_sha256,
            "from": frontier, "to": frontier,
            "source": reservation["source"],
            "graph_frontier_sha256": reservation["graph_frontier_sha256"],
            "candidate_resume_sha256": reservation["candidate_resume_sha256"],
            "retirement": retirement, "previous_control_event_id": None,
            "activation_epoch": 0, "candidate_seal_event_id": None,
            "candidate_seal_sha256": None,
        },
        plan_revision=PLAN, expected_revision=5,
        created_at=reservation["created_at"], authority=authority,
    )
    client.root_comments.append({
        "id": projection_slot_id(
            "GEN-37", PLAN, genesis["expected_revision"], authority,
        ),
        "body": encode_projection_comment(genesis),
        "createdAt": genesis["created_at"], "updatedAt": genesis["created_at"],
    })
    token = f"generation:{reservation['reservation_id']}:{reservation_sha256}"
    assert generation_ledger_frontier_tokens(
        client.root_comments, workstream_id="GEN-37",
        authenticated_route=authority, current_plan_revision=PLAN,
    ) == [token]
    return token


class FakeChildStateClient:
    def __init__(self):
        scope = self.scope_event({"GEN-38": "github.com:id:R_repo"})
        source = build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"identity": "https://example.test/plan", "sha256": PLAN},
            plan_revision=PLAN, expected_revision=1,
            created_at="2026-08-30T00:00:01Z",
            authority={**ROUTE, "root_issue_id": ROOT_ID},
        )
        origin = build_projection_event(
            workstream_id="GEN-37", kind="child_extension_authorization",
            key=CHILD_ID, value={
                "root_issue_id": ROOT_ID,
                "route": {**ROUTE, "root_issue_id": ROOT_ID},
                "source": {"identity": "https://example.test/plan", "sha256": PLAN},
                "plan_revision": PLAN, "reviewed_candidate_key": "a",
                "child_issue_id": CHILD_ID, "expected_material_revision": 0,
                "expected_projection_revision": 2,
                "initial_state": "planned_pending_projection",
            }, plan_revision=PLAN, expected_revision=2,
            created_at="2026-08-30T00:00:02Z",
            authority={**ROUTE, "root_issue_id": ROOT_ID},
        )
        self.root_comments: list[dict] = [{
            "id": projection_slot_id(
                scope["workstream_id"], scope["plan_revision"],
                scope["expected_revision"], scope["authority"],
            ),
            "body": encode_projection_comment(scope),
            "createdAt": "2026-08-30T00:00:00Z",
            "updatedAt": "2026-08-30T00:00:00Z",
        }, {
            "id": projection_slot_id(
                source["workstream_id"], source["plan_revision"],
                source["expected_revision"], source["authority"],
            ), "body": encode_projection_comment(source),
            "createdAt": "2026-08-30T00:00:01Z",
            "updatedAt": "2026-08-30T00:00:01Z",
        }, {
            "id": projection_slot_id(
                origin["workstream_id"], origin["plan_revision"],
                origin["expected_revision"], origin["authority"],
            ), "body": encode_projection_comment(origin),
            "createdAt": "2026-08-30T00:00:02Z",
            "updatedAt": "2026-08-30T00:00:02Z",
        }]
        self.child_comments: list[dict] = []
        self.calls: list[tuple[str, dict]] = []
        self.crash_after_root_append = False
        self.transport_error_after_root_append = False
        self.child_parent_id = ROOT_ID

    @staticmethod
    def scope_event(child_ownership):
        return build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root",
            value={
                "namespace": "child-state-tests",
                "linear": {**ROUTE, "root_issue_id": ROOT_ID},
                "primary_repository": "github.com:id:R_repo",
                "repositories": [], "child_ownership": child_ownership,
            },
            plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-30T00:00:00Z",
            authority={**ROUTE, "root_issue_id": ROOT_ID},
        )

    @staticmethod
    def issue(identifier, issue_id, comments):
        return {
            "id": issue_id, "identifier": identifier,
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project"},
            "comments": {
                "nodes": deepcopy(comments),
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            },
        }

    def execute(self, query, variables):
        self.calls.append((query, deepcopy(variables)))
        if (
            "query WorkstreamChildTarget" in query
            or "query WorkstreamChildOriginRepairTarget" in query
        ):
            return {
                "root": {
                    **self.issue("GEN-37", ROOT_ID, self.root_comments),
                    "description": f"Plan revision: {PLAN}", "parent": None,
                    "createdAt": "2026-08-01T00:00:00Z",
                    "state": {
                        "id": "state-started", "name": "In Progress",
                        "type": "started",
                    },
                    "assignee": {"id": "assignee"},
                },
                "child": {
                    **self.issue("GEN-38", CHILD_ID, self.child_comments),
                    "description": "Legacy child description",
                    "createdAt": "2026-08-01T00:00:00Z",
                    "state": {
                        "id": "state-started", "name": "In Progress",
                        "type": "started",
                    },
                    "assignee": {"id": "assignee"},
                    "parent": {
                        "id": self.child_parent_id,
                        "identifier": (
                            "GEN-37" if self.child_parent_id == ROOT_ID else "GEN-99"
                        ),
                    },
                },
            }
        if "query WorkstreamChildOriginNativeReadback" in query:
            return {"issue": {
                **self.issue("GEN-38", CHILD_ID, self.child_comments),
                "description": "Legacy child description",
                "createdAt": "2026-08-01T00:00:00Z",
                "state": {
                    "id": "state-started", "name": "In Progress",
                    "type": "started",
                },
                "assignee": {"id": "assignee"},
                "parent": {"id": self.child_parent_id, "identifier": (
                    "GEN-37" if self.child_parent_id == ROOT_ID else "GEN-99"
                )},
            }}
        if "query WorkstreamRootOriginNativeReadback" in query:
            return {"issue": {
                **self.issue("GEN-37", ROOT_ID, self.root_comments),
                "description": f"Plan revision: {PLAN}", "parent": None,
                "createdAt": "2026-08-01T00:00:00Z",
                "state": {
                    "id": "state-started", "name": "In Progress",
                    "type": "started",
                },
                "assignee": {"id": "assignee"},
            }}
        if "query WorkstreamChildMutationTarget" in query:
            return {"issue": {
                **self.issue("GEN-38", CHILD_ID, self.child_comments),
                "parent": {"id": ROOT_ID, "identifier": "GEN-37"},
            }}
        if "query WorkstreamDeltaComments" in query:
            identifier = variables["issueId"]
            if identifier == "GEN-37":
                return {"issue": self.issue("GEN-37", ROOT_ID, self.root_comments)}
            if identifier == "GEN-38":
                return {"issue": self.issue("GEN-38", CHILD_ID, self.child_comments)}
            raise AssertionError(f"unexpected issue: {identifier}")
        if "CommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}]}}
        if "commentCreate" in query:
            item = variables["input"]
            target = (
                self.child_comments if item["issueId"] == "GEN-38"
                else self.root_comments if item["issueId"] == "GEN-37"
                else None
            )
            if target is None:
                raise AssertionError("unexpected comment target")
            if any(comment["id"] == item["id"] for comment in target):
                raise LinearTransportError("duplicate comment id")
            comment = {
                "id": item["id"], "body": item["body"],
                "createdAt": "2026-08-30T00:00:00Z",
                "updatedAt": "2026-08-30T00:00:00Z",
            }
            target.append(comment)
            if (
                item["issueId"] == "GEN-37"
                and "workstream-projection:v1" in item["body"]
                and self.transport_error_after_root_append
            ):
                self.transport_error_after_root_append = False
                raise LinearTransportError("lost response after durable append")
            if (
                item["issueId"] == "GEN-37"
                and "workstream-projection:v1" in item["body"]
                and self.crash_after_root_append
            ):
                self.crash_after_root_append = False
                raise SystemExit("death after root activation")
            return {"commentCreate": {"success": True, "comment": deepcopy(comment)}}
        raise AssertionError(f"unexpected GraphQL operation: {query[:80]}")


class WorkstreamChildStateTests(unittest.TestCase):
    def common(self):
        return [
            "GEN-37", "--root-issue-id", ROOT_ID,
            "--child-workstream-id", "GEN-38", "--child-issue-id", CHILD_ID,
            "--plan-revision", PLAN, "--workspace-id", "workspace",
            "--team-id", "team", "--project-id", "project", "--apply",
        ]

    def patches(self):
        return (
            mock.patch(
                "workstream_child_target.resolve_linear_route",
                return_value=(ROUTE, None),
            ),
            mock.patch(
                "workstream_child_target.load_linear_api_key",
                return_value="secret",
            ),
        )

    def generated_reservation_fixture(self):
        """Reproduce GEN-37's three generated-ID reservations and no proposal."""
        client = FakeChildStateClient()
        install_active_generation(client)
        authority = {**ROUTE, "root_issue_id": ROOT_ID}
        for revision in range(6, 30):
            event = build_projection_event(
                workstream_id="GEN-37", kind="provenance",
                key=f"production-padding-{revision}",
                value={
                    "agent": "codex", "machine": "M3",
                    "session_id": f"production-padding-{revision}",
                },
                plan_revision=PLAN, expected_revision=revision,
                created_at=f"2026-08-30T00:00:{revision:02d}Z",
                authority=authority,
            )
            client.root_comments.append({
                "id": projection_slot_id(
                    "GEN-37", PLAN, event["expected_revision"], authority,
                ),
                "body": encode_projection_comment(event),
                "createdAt": event["created_at"],
                "updatedAt": event["created_at"],
            })
        for revision in range(63):
            created_at = (
                f"2026-08-31T{revision // 60:02d}:{revision % 60:02d}:00Z"
            )
            delta = Delta(
                event_id=f"wsd_{revision:032x}", workstream_id="GEN-37",
                kind="progress", source="system",
                payload={"next_action": f"production fixture {revision}"},
                expected_revision=revision, created_at=created_at,
            )
            client.root_comments.append({
                "id": str(uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"agent-workstream-material-{revision}",
                )),
                "body": encode_event_comment(delta),
                "createdAt": created_at, "updatedAt": created_at,
            })
        self.assertEqual(reduce_event_comments(
            client.root_comments, workstream_id="GEN-37",
        ).revision, 63)
        self.assertEqual(reduce_projection_comments(
            client.root_comments, workstream_id="GEN-37",
            expected_plan_revision=PLAN, authenticated_route=authority,
        ).revision, 30)
        args = [
            *self.common(), "--kind", "progress", "--source", "system",
            "--expected-revision", "0", "--created-at",
            "2026-09-02T03:48:00Z", "--event-id",
            "wsc_gen94_m3_reverse_canary_20260901", "--payload-json",
            '{"next_action":"GEN-94 M3 reverse canary verified"}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, mock.patch(
            "workstream_linear_events._proven_ledger_reservations",
            return_value=[],
        ), mock.patch(
            "workstream_linear_events.semantic_ledger_reservations",
            return_value=[],
        ), self.assertRaisesRegex(
            LinearTransportError,
            "child_mutation_serialization_slot_lost_reload_required",
        ):
            workstream_child_event.run(args, client_factory=lambda _token: client)
        original = next(
            item for item in client.root_comments
            if SERIALIZATION_PREFIX in item["body"]
        )
        reservation = decode_reservation(original["body"])
        self.assertEqual(reservation["material_revision"], 63)
        self.assertEqual(reservation["projection_revision"], 30)
        client.root_comments.remove(original)
        generated = []
        for index, length in enumerate((26, 28, 27)):
            retry = deepcopy(reservation)
            retry["frontier_ids"] = [
                f"opaque-frontier-{index}-{item:02d}" for item in range(length)
            ]
            comment = {
                "id": str(uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"agent-workstream-generated-reservation-{index}",
                )),
                "body": encode_ledger_reservation(retry),
                "createdAt": f"2026-09-02T03:48:0{index}Z",
                "updatedAt": f"2026-09-02T03:48:0{index}Z",
            }
            generated.append(comment)
            client.root_comments.append(comment)
        self.assertEqual(client.child_comments, [])
        return client, args, reservation, generated

    @staticmethod
    def semantic_kwargs(reservation):
        return {
            "workstream_id": reservation["workstream_id"],
            "authenticated_route": reservation["authority"],
            "current_plan_revision": reservation["plan_revision"],
            "intent_event": reservation["intent_event"],
            "expected_material_revision": reservation["material_revision"],
            "expected_projection_revision": reservation["projection_revision"],
            "expected_projection_frontier_ids": reservation[
                "projection_frontier_ids"
            ],
        }

    @staticmethod
    def reservation_with_event(reservation, event):
        changed = deepcopy(reservation)
        changed.update({
            "workstream_id": event["workstream_id"],
            "authority": deepcopy(event["authority"]),
            "plan_revision": event["plan_revision"],
            "projection_revision": event["expected_revision"],
            "intent_event": event,
            "intent_sha256": hashlib.sha256(json.dumps(
                event, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ).encode()).hexdigest(),
        })
        changed["projection_frontier_ids"] = changed[
            "projection_frontier_ids"
        ][:event["expected_revision"]]
        return changed

    def test_real_projection_adapter_recognizes_legacy_origin_replay(self):
        client = FakeChildStateClient()
        receipt = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **ROUTE, root_issue_id=ROOT_ID,
        ).replay_legacy_child_extension(
            source={"identity": "https://example.test/plan", "sha256": PLAN},
            reviewed_candidate_key="a", child_issue_id=CHILD_ID,
            require_existing=True,
        )
        self.assertEqual(receipt["disposition"], "legacy_existing")
        self.assertEqual(receipt["event"]["key"], CHILD_ID)

    def test_child_event_and_checkpoint_leave_root_unchanged_and_resume(self):
        client = FakeChildStateClient()
        root_comments_before = deepcopy(client.root_comments)
        event_args = [
            *self.common(), "--kind", "material_boundary",
            "--source", "agent_discovery", "--expected-revision", "0",
            "--created-at", "2026-08-30T00:00:00Z", "--payload-json",
            json.dumps({
                "boundary_id": "child-ready",
                "changes": [{
                    "kind": "progress", "payload": {
                        "next_action": "Run the child acceptance proof.",
                        "blocker": {"kind": "review", "owner": "maintainer"},
                    },
                }],
            }),
        ]
        checkpoint = build_checkpoint(
            workstream_id="GEN-38", boundary_id="child-checkpoint",
            root_revision=1, plan_revision=PLAN,
            before_status="In Progress", after_status="In Progress",
            execution={
                "agent": "codex", "provider": "openai", "session_id": "child",
                "machine": "M5", "worktree": {
                    "state": "safe", "path": "/repo/child", "branch": "child",
                    "head": "b" * 40,
                },
            }, exact_head="b" * 40, evidence=[{"kind": "test", "id": "focused"}],
            blocker={"kind": "review", "owner": "maintainer"},
            next_action="Run the child acceptance proof.",
        )
        with tempfile.TemporaryDirectory() as directory:
            checkpoint_path = Path(directory) / "checkpoint.json"
            checkpoint_path.write_text(json.dumps(checkpoint))
            checkpoint_args = [
                *self.common(), "--checkpoint", str(checkpoint_path),
                "--material-revision", "1", "--no-predecessor",
            ]
            route_patch, auth_patch = self.patches()
            with route_patch, auth_patch:
                first_event = workstream_child_event.run(
                    event_args, client_factory=lambda _token: client,
                )
                first_checkpoint = workstream_child_checkpoint.run(
                    checkpoint_args, client_factory=lambda _token: client,
                )
                writes = len(client.child_comments)
                replay_event = workstream_child_event.run(
                    event_args, client_factory=lambda _token: client,
                )
                replay_checkpoint = workstream_child_checkpoint.run(
                    checkpoint_args, client_factory=lambda _token: client,
                )

        self.assertEqual(
            client.root_comments[:len(root_comments_before)], root_comments_before,
        )
        # Each child record now has one root serialization intent followed by
        # its projection authorization.
        self.assertEqual(len(client.root_comments), len(root_comments_before) + 4)
        self.assertTrue(all(
            "Run the child acceptance proof." not in comment["body"]
            for comment in client.root_comments
        ))
        self.assertEqual(len(client.child_comments), writes)
        self.assertEqual(first_event["receipt"], replay_event["receipt"])
        self.assertEqual(
            first_checkpoint["receipt"]["event_id"],
            replay_checkpoint["receipt"]["event_id"],
        )
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 7,
                "status": "In Progress", "status_type": "started",
                "next_action": "Continue the root.",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child work",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "next_action": "stale issue prose",
                "parent": {"id": ROOT_ID}, "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        enriched = add_child_material_history(
            snapshot, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=deepcopy(client.root_comments),
        )
        child = compact_context(enriched, "GEN-37")["children"][0]
        self.assertEqual(child["next_action"], "Run the child acceptance proof.")
        self.assertEqual(child["blocker"], {
            "kind": "review", "owner": "maintainer",
        })
        self.assertEqual(child["latest_checkpoint"]["exact_head"], "b" * 40)
        self.assertEqual(
            child["latest_checkpoint"]["checkpoint_event_id"],
            checkpoint["event_id"],
        )
        inactive_target = deepcopy(snapshot)
        inactive_target["root"].update({
            "plan_revision": "c" * 64,
            "description_plan_revision": PLAN,
        })
        target_child = add_child_material_history(
            inactive_target, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=deepcopy(client.root_comments),
            proposal_plan_revision=PLAN,
        )["children"][0]
        self.assertEqual(target_child["material_event_revision"], 1)
        self.assertEqual(
            target_child["next_action"], "Run the child acceptance proof.",
        )
        self.assertNotIn("pending_child_proposals", target_child)

    def test_child_target_identity_mismatch_refuses_before_comment_write(self):
        client = FakeChildStateClient()
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", "{}",
        ]
        route_patch, auth_patch = self.patches()
        original = client.execute
        with route_patch, auth_patch, mock.patch.object(
            client, "execute", wraps=original,
        ) as execute:
            def wrong_child(query, variables):
                result = original(query, variables)
                if "query WorkstreamChildTarget" in query:
                    result["child"]["id"] = "33333333-3333-4333-8333-333333333333"
                return result

            execute.side_effect = wrong_child
            with self.assertRaisesRegex(
                LinearTransportError, "child_target_identity_mismatch",
            ):
                workstream_child_event.run(
                    args, client_factory=lambda _token: client,
                )
        self.assertEqual(len(client.root_comments), 3)
        self.assertEqual(client.child_comments, [])

    def test_proposal_wrapper_target_mismatch_cannot_append_root_authority(self):
        client = FakeChildStateClient()
        wrong_child = "22222222-2222-4222-8222-222222222222"
        proposal = build_proposal(
            "event", {
                "event_id": "wrong-wrapper-child", "workstream_id": "GEN-38",
                "kind": "progress", "source": "user_turn", "payload": {},
                "expected_revision": 0, "created_at": "now",
            }, child_workstream_id="GEN-38", child_issue_id=wrong_child,
            plan_revision=PLAN,
        )
        receipt = _append_proposal(client, proposal)
        projection = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **ROUTE, root_issue_id=ROOT_ID,
        )
        selected = projection.select_owned_child_generation(
            description_plan_revision=PLAN, child_workstream_id="GEN-38",
            child_issue_id=CHILD_ID,
        )
        generation = {key: selected[key] for key in (
            "plan_revision", "description_plan_revision",
            "transition_tip_event_id", "activation_epoch", "authority_origin",
            "workstream_id", "authority", "source",
        )}
        root_count = len(client.root_comments)
        with self.assertRaisesRegex(
            Exception, "child_mutation_proposal_identity_mismatch",
        ):
            projection.reserve_child_mutation(
                proposal=proposal, proposal_remote_id=receipt["remote_id"],
                child_identity={
                    "identifier": "GEN-38", "id": CHILD_ID,
                    "parent_issue_id": ROOT_ID, "route": ROUTE,
                }, generation_authority=generation,
                scope_event_id=selected["scope_event_id"],
                scope_value_sha256=selected["scope_value_sha256"],
                repository_owner=selected["child_repository_owner"],
                child_origin=selected["child_origin"],
                expected_projection_revision=selected["projection_revision"],
            )
        self.assertEqual(len(client.root_comments), root_count)
        self.assertEqual(len(client.child_comments), 1)

    def test_unowned_child_refuses_before_comment_write(self):
        client = FakeChildStateClient()
        scope = client.scope_event({})
        client.root_comments[0].update({
            "id": projection_slot_id(
                scope["workstream_id"], scope["plan_revision"],
                scope["expected_revision"], scope["authority"],
            ),
            "body": encode_projection_comment(scope),
        })
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", "{}",
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, self.assertRaisesRegex(
            Exception, "child_target_not_owned:GEN-38",
        ):
            workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        self.assertEqual(len(client.root_comments), 3)
        self.assertEqual(client.child_comments, [])

    def test_death_before_root_activation_leaves_inert_recoverable_proposal(self):
        client = FakeChildStateClient()
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", '{"next_action":"recover"}',
        ]
        route_patch, auth_patch = self.patches()
        original_reserve = LinearProjectionAdapter.reserve_child_mutation
        def die_after_proposal(adapter, **kwargs):
            if kwargs.get("publish_intent"):
                return original_reserve(adapter, **kwargs)
            raise OSError("death before activation")
        with route_patch, auth_patch, mock.patch(
            "workstream_linear_projection.LinearProjectionAdapter.reserve_child_mutation",
            new=die_after_proposal,
        ):
            with self.assertRaisesRegex(OSError, "death before activation"):
                workstream_child_event.run(args, client_factory=lambda _token: client)
        self.assertEqual(len(client.root_comments), 4)
        self.assertEqual(len(client.child_comments), 1)
        self.assertNotIn("workstream-delta:v1", client.child_comments[0]["body"])
        with self.assertRaisesRegex(Exception, "ledger_boundary_reserved"):
            assert_no_pending_ledger_reservation(
                client.root_comments, workstream_id="GEN-37",
                authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
                current_plan_revision=PLAN,
            )
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 0,
                "status": "In Progress", "status_type": "started",
                "next_action": "root action",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "next_action": "issue action",
                "parent": {"id": ROOT_ID}, "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        pending_context = compact_context(add_child_material_history(
            snapshot, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=deepcopy(client.root_comments),
        ), "GEN-37")
        pending = pending_context["children"][0]["pending_child_proposals"]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending_context["children"][0]["next_action"], "issue action")
        self.assertNotIn("record", pending[0])
        activation_args = [
            *self.common(), "--proposal-id", pending[0]["proposal_id"],
            "--proposal-remote-id", pending[0]["proposal_remote_id"],
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            result = workstream_child_proposal_activate.run(
                activation_args, client_factory=lambda _token: client,
            )
            root_writes = len(client.root_comments)
            child_writes = len(client.child_comments)
            replay = workstream_child_proposal_activate.run(
                activation_args, client_factory=lambda _token: client,
            )
        self.assertEqual(result["authorization"]["disposition"], "created")
        self.assertEqual(replay["authorization"]["disposition"], "existing")
        self.assertEqual(len(client.root_comments), root_writes)
        self.assertEqual(len(client.child_comments), child_writes)

    def test_generation_frontier_duplicate_reservations_recover_exact_child_event(self):
        client = FakeChildStateClient()
        install_active_generation(client)
        generation_fixture_comments = len(client.root_comments)
        args = [
            *self.common(), "--kind", "progress", "--source", "system",
            "--expected-revision", "0", "--created-at",
            "2026-09-02T03:48:00Z", "--event-id",
            "wsc_gen94_m3_reverse_canary_20260901", "--payload-json",
            '{"next_action":"GEN-94 M3 reverse canary verified"}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, mock.patch(
            "workstream_linear_events._proven_ledger_reservations",
            return_value=[],
        ), mock.patch(
            "workstream_linear_events.semantic_ledger_reservations",
            return_value=[],
        ):
            for _attempt in range(2):
                with self.assertRaisesRegex(
                    LinearTransportError,
                    "child_mutation_serialization_slot_lost_reload_required",
                ):
                    workstream_child_event.run(
                        args, client_factory=lambda _token: client,
                    )

        reservations = [
            comment for comment in client.root_comments
            if SERIALIZATION_PREFIX in comment["body"]
        ]
        self.assertEqual(len(reservations), 2)
        self.assertEqual(client.child_comments, [])
        decoded = [decode_reservation(item["body"]) for item in reservations]
        self.assertEqual(len({
            item["intent_event"]["event_id"] for item in decoded
        }), 1)
        self.assertEqual(len({
            item["intent_event"]["value"]["proposal_id"] for item in decoded
        }), 1)
        self.assertEqual(
            [sum(token.startswith("collision:") for token in item["frontier_ids"])
             for item in decoded],
            [0, 1],
        )
        self.assertEqual(len(reduce_ledger_reservations(
            client.root_comments, workstream_id="GEN-37",
        )), 1)
        without_predecessor = [
            comment for comment in client.root_comments
            if comment["id"] != reservations[0]["id"]
        ]
        self.assertEqual(reduce_ledger_reservations(
            without_predecessor, workstream_id="GEN-37",
        ), [])
        random_collision = deepcopy(decoded[1])
        random_collision["frontier_ids"] = sorted([
            value if not value.startswith("collision:")
            else "collision:" + "f" * 64
            for value in random_collision["frontier_ids"]
        ])
        random_collision_comment = {
            "id": ledger_boundary_slot_id(
                random_collision["workstream_id"],
                random_collision["material_revision"],
                random_collision["frontier_ids"],
                random_collision["authority"],
            ),
            "body": encode_ledger_reservation(random_collision),
        }
        random_collision_comments = [
            comment for comment in client.root_comments
            if comment["id"] not in {item["id"] for item in reservations}
        ] + [reservations[0], random_collision_comment]
        self.assertEqual(reduce_ledger_reservations(
            random_collision_comments, workstream_id="GEN-37",
        ), [])

        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            recovered = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
            root_writes = len(client.root_comments)
            child_writes = len(client.child_comments)
            replay = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )

        self.assertEqual(len([
            comment for comment in client.root_comments
            if SERIALIZATION_PREFIX in comment["body"]
        ]), 2)
        self.assertEqual(
            root_writes,
            generation_fixture_comments + len(reservations) + 1,
        )
        self.assertEqual(child_writes, 1)
        self.assertEqual(len(client.root_comments), root_writes)
        self.assertEqual(len(client.child_comments), child_writes)
        self.assertEqual(recovered["receipt"], replay["receipt"])
        self.assertEqual(recovered["receipt"]["revision"], 1)
        self.assertEqual(recovered["proposal"]["disposition"], "created")
        self.assertEqual(recovered["authorization"]["disposition"], "created")
        self.assertEqual(replay["authorization"]["disposition"], "existing")
        assert_no_pending_ledger_reservation(
            client.root_comments, workstream_id="GEN-37",
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            current_plan_revision=PLAN,
        )

        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 0,
                "status": "In Progress", "status_type": "started",
                "next_action": "root action",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "next_action": "issue action",
                "parent": {"id": ROOT_ID}, "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        enriched = add_child_material_history(
            snapshot, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=deepcopy(client.root_comments),
        )
        resumed = compact_context(enriched, "GEN-37")
        self.assertEqual(resumed["children"][0]["material_event_revision"], 1)
        self.assertEqual(
            resumed["children"][0]["next_action"],
            "GEN-94 M3 reverse canary verified",
        )
        self.assertNotIn("pending_child_proposals", resumed["children"][0])
        from test_workstream_resume import ResumeTests
        authority_snapshot = ResumeTests().snapshot()
        authority_snapshot["children"] = [deepcopy(enriched["children"][0])]
        authority_snapshot["decisions"] = []
        authority_snapshot["provenance"] = []
        authority_snapshot = ResumeTests().full_authority_snapshot(
            authority_snapshot
        )
        authority_snapshot["children"][0]["parent"] = {
            "id": authority_snapshot["authenticated_route"]["root_issue_id"],
            "identifier": "GEN-37",
        }
        authority_snapshot["children"][0]["team"] = {
            "id": "team", "organization": {"id": "workspace"},
        }
        authority_snapshot["children"][0]["project"] = {"id": "project"}
        authority_context = compact_context(
            authority_snapshot, "GEN-37", require_projection_authority=True,
        )
        self.assertEqual(authority_context["resume_authority"], "full")
        self.assertEqual(
            authority_context["children"][0]["next_action"],
            "GEN-94 M3 reverse canary verified",
        )

    def test_three_generated_reservations_without_proposal_recover_no_fourth(self):
        client, args, _reservation, generated = self.generated_reservation_fixture()
        before_root = len(client.root_comments)
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            recovered = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
            root_writes = len(client.root_comments)
            child_writes = len(client.child_comments)
            replay = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )

        reservations = [
            comment for comment in client.root_comments
            if SERIALIZATION_PREFIX in comment["body"]
        ]
        self.assertEqual(reservations, generated)
        self.assertEqual(root_writes, before_root + 1)
        self.assertEqual(len(client.root_comments), root_writes)
        self.assertEqual(child_writes, 1)
        self.assertEqual(len(client.child_comments), child_writes)
        self.assertEqual(recovered["proposal"]["disposition"], "created")
        self.assertEqual(recovered["authorization"]["disposition"], "created")
        self.assertEqual(replay["authorization"]["disposition"], "existing")
        self.assertEqual(recovered["receipt"], replay["receipt"])
        state = reduce_projection_comments(
            client.root_comments, workstream_id="GEN-37",
            expected_plan_revision=PLAN,
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
        )
        authorizations = [
            item for item in state.events
            if item["kind"] == "child_mutation_authorization"
            and item["value"]["child_workstream_id"] == "GEN-38"
        ]
        self.assertEqual(len(authorizations), 1)

        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 0,
                "status": "In Progress", "status_type": "started",
                "next_action": "root action",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "next_action": "issue action",
                "parent": {"id": ROOT_ID}, "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        enriched = add_child_material_history(
            snapshot, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=deepcopy(client.root_comments),
        )
        from test_workstream_resume import ResumeTests
        authority_snapshot = ResumeTests().snapshot()
        authority_snapshot.update({
            "children": [deepcopy(enriched["children"][0])],
            "decisions": [], "provenance": [],
        })
        authority_snapshot = ResumeTests().full_authority_snapshot(
            authority_snapshot
        )
        authority_snapshot["children"][0].update({
            "parent": {
                "id": authority_snapshot["authenticated_route"]["root_issue_id"],
                "identifier": "GEN-37",
            },
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project"},
        })
        authority_context = compact_context(
            authority_snapshot, "GEN-37", require_projection_authority=True,
        )
        self.assertEqual(authority_context["resume_authority"], "full")
        self.assertEqual(
            authority_context["children"][0]["next_action"],
            "GEN-94 M3 reverse canary verified",
        )

    def test_generated_reservation_lost_response_converges_without_retry(self):
        client = FakeChildStateClient()
        install_active_generation(client)
        args = [
            *self.common(), "--kind", "progress", "--source", "system",
            "--expected-revision", "0", "--created-at",
            "2026-09-02T03:48:00Z", "--event-id",
            "wsc_gen94_m3_reverse_canary_20260901", "--payload-json",
            '{"next_action":"GEN-94 M3 reverse canary verified"}',
        ]
        original_execute = client.execute
        generated_id = "9c60b3e5-0918-4ab3-8caa-e7c2bc58f80d"

        def lose_generated_response(query, variables):
            item = variables.get("input") or {}
            if (
                "commentCreate" in query
                and SERIALIZATION_PREFIX in str(item.get("body", ""))
            ):
                client.root_comments.append({
                    "id": generated_id, "body": item["body"],
                    "createdAt": "2026-09-02T03:48:00Z",
                    "updatedAt": "2026-09-02T03:48:00Z",
                })
                raise LinearTransportError("lost generated-id response")
            return original_execute(query, variables)

        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, mock.patch.object(
            client, "execute", side_effect=lose_generated_response,
        ):
            result = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        reservations = [
            comment for comment in client.root_comments
            if SERIALIZATION_PREFIX in comment["body"]
        ]
        self.assertEqual([item["id"] for item in reservations], [generated_id])
        self.assertEqual(len(client.child_comments), 1)
        self.assertEqual(result["authorization"]["disposition"], "created")

    def test_semantic_reservation_refuses_every_core_or_authority_drift(self):
        _client, _args, reservation, generated = self.generated_reservation_fixture()
        kwargs = self.semantic_kwargs(reservation)
        matches = semantic_ledger_reservations(generated, **kwargs)
        self.assertEqual(len(matches), 3)
        self.assertEqual(
            sorted(len(item[0]["frontier_ids"]) for item in matches),
            [26, 27, 28],
        )

        def rebuild(*, value=None, authority=None, plan_revision=None,
                    expected_revision=None, workstream_id=None, key=None):
            original = reservation["intent_event"]
            return build_projection_event(
                workstream_id=original["workstream_id"]
                if workstream_id is None else workstream_id,
                kind=original["kind"],
                key=original["key"] if key is None else key,
                value=deepcopy(original["value"] if value is None else value),
                plan_revision=original["plan_revision"]
                if plan_revision is None else plan_revision,
                expected_revision=original["expected_revision"]
                if expected_revision is None else expected_revision,
                created_at=original["created_at"],
                supersedes_event_id=original["supersedes_event_id"],
                authority=deepcopy(
                    original["authority"] if authority is None else authority
                ),
            )

        projection_frontier = deepcopy(reservation)
        projection_frontier["projection_frontier_ids"][0] = "forged-projection"

        material = deepcopy(reservation)
        material["material_revision"] += 1

        stale_event = rebuild(
            expected_revision=reservation["projection_revision"] - 1,
        )
        projection = self.reservation_with_event(reservation, stale_event)

        generation_value = deepcopy(reservation["intent_event"]["value"])
        generation_value["generation_authority"]["activation_epoch"] += 1
        generation = self.reservation_with_event(
            reservation, rebuild(value=generation_value),
        )

        record_value = deepcopy(reservation["intent_event"]["value"])
        record_value["record_sha256"] = "f" * 64
        record = self.reservation_with_event(
            reservation, rebuild(value=record_value),
        )

        child_value = deepcopy(reservation["intent_event"]["value"])
        child_value["child_workstream_id"] = "GEN-999"
        child = self.reservation_with_event(
            reservation, rebuild(value=child_value),
        )

        kind_value = deepcopy(reservation["intent_event"]["value"])
        kind_value["mutation_kind"] = "checkpoint"
        kind = self.reservation_with_event(
            reservation, rebuild(value=kind_value),
        )

        proposal_value = deepcopy(reservation["intent_event"]["value"])
        proposal_value["proposal_id"] = "wscp_" + "f" * 32
        proposal = self.reservation_with_event(
            reservation,
            rebuild(value=proposal_value, key=proposal_value["proposal_id"]),
        )

        foreign_workstream_value = deepcopy(
            reservation["intent_event"]["value"]
        )
        foreign_workstream_value["generation_authority"][
            "workstream_id"
        ] = "GEN-999"
        foreign_workstream = self.reservation_with_event(
            reservation, rebuild(
                value=foreign_workstream_value, workstream_id="GEN-999",
            ),
        )

        foreign_authority = deepcopy(reservation["authority"])
        foreign_authority["root_issue_id"] = (
            "22222222-2222-4222-8222-222222222222"
        )
        route_value = deepcopy(reservation["intent_event"]["value"])
        route_value.update({
            "root_issue_id": foreign_authority["root_issue_id"],
            "route": deepcopy(foreign_authority),
            "child_parent_issue_id": foreign_authority["root_issue_id"],
            "child_route": {
                key: foreign_authority[key]
                for key in ("workspace_id", "team_id", "project_id")
            },
        })
        route_value["generation_authority"]["authority"] = deepcopy(
            foreign_authority
        )
        route = self.reservation_with_event(
            reservation,
            rebuild(value=route_value, authority=foreign_authority),
        )

        for name, changed in (
            ("projection_frontier", projection_frontier),
            ("material_revision", material),
            ("projection_revision", projection),
            ("generation", generation),
            ("record", record),
            ("child", child),
            ("kind", kind),
            ("proposal", proposal),
            ("workstream", foreign_workstream),
            ("route", route),
        ):
            comment = {
                "id": f"generated-{name}",
                "body": encode_ledger_reservation(changed),
            }
            with self.subTest(name=name), self.assertRaisesRegex(
                LinearEventError, "ledger_reservation_intent_conflict",
            ):
                semantic_ledger_reservations([comment], **kwargs)

        forged = {
            "id": "generated-forged-digest",
            "body": encode_forged_reservation_digest(generated[0]["body"]),
        }
        with self.assertRaisesRegex(
            LinearEventError, "ledger_reservation_authentication_failed",
        ):
            semantic_ledger_reservations([forged], **kwargs)

        conflicting = {
            "id": "generated-conflicting-duplicate",
            "body": encode_ledger_reservation(projection_frontier),
        }
        with self.assertRaisesRegex(
            LinearEventError, "ledger_reservation_intent_conflict",
        ):
            semantic_ledger_reservations([generated[0], conflicting], **kwargs)

    def test_semantic_reservation_reuse_refuses_live_proposal_body_drift(self):
        client, _args, reservation, _generated = self.generated_reservation_fixture()
        record = {
            "event_id": "wsc_gen94_m3_reverse_canary_20260901",
            "workstream_id": "GEN-38", "kind": "progress", "source": "system",
            "payload": {"next_action": "GEN-94 M3 reverse canary verified"},
            "expected_revision": 0, "created_at": "2026-09-02T03:48:00Z",
        }
        proposal = build_proposal(
            "event", record, child_workstream_id="GEN-38",
            child_issue_id=CHILD_ID, plan_revision=PLAN,
        )
        value = reservation["intent_event"]["value"]
        self.assertEqual(proposal["proposal_id"], value["proposal_id"])
        self.assertEqual(proposal["record_sha256"], value["record_sha256"])
        drifted = deepcopy(proposal)
        drifted["child_issue_id"] = "33333333-3333-4333-8333-333333333333"
        client.child_comments.append({
            "id": value["proposal_remote_id"], "body": encode_proposal(drifted),
            "createdAt": "2026-09-02T03:48:10Z",
            "updatedAt": "2026-09-02T03:48:10Z",
        })
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **ROUTE, root_issue_id=ROOT_ID,
        )
        root_count = len(client.root_comments)
        with self.assertRaisesRegex(
            LinearTransportError,
            "child_mutation_proposal_missing_or_mismatch",
        ):
            adapter._reserve_child_mutation_intent(reservation["intent_event"])
        self.assertEqual(len(client.root_comments), root_count)

    def test_duplicate_reservation_material_divergence_is_quarantined(self):
        client = FakeChildStateClient()
        args = [
            *self.common(), "--kind", "progress", "--source", "system",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", '{"next_action":"recover"}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, mock.patch(
            "workstream_linear_events._proven_ledger_reservations",
            return_value=[],
        ), mock.patch(
            "workstream_linear_events.semantic_ledger_reservations",
            return_value=[],
        ), self.assertRaisesRegex(
            LinearTransportError,
            "child_mutation_serialization_slot_lost_reload_required",
        ):
            workstream_child_event.run(args, client_factory=lambda _token: client)
        first_comment = next(
            item for item in client.root_comments
            if SERIALIZATION_PREFIX in item["body"]
        )
        first = decode_reservation(first_comment["body"])
        divergent = {**deepcopy(first), "material_revision": 1}
        divergent_id = ledger_boundary_slot_id(
            divergent["workstream_id"], divergent["material_revision"],
            divergent["frontier_ids"], divergent["authority"],
        )
        comments = [*client.root_comments, {
            "id": divergent_id, "body": encode_ledger_reservation(divergent),
            "createdAt": "later", "updatedAt": "later",
        }]
        self.assertEqual(
            reduce_ledger_reservations(comments, workstream_id="GEN-37"), [],
        )

    def test_inactive_target_projection_classifies_predecessor_proposal(self):
        client = FakeChildStateClient()
        proposal = build_proposal(
            "event", {
                "event_id": "predecessor-proposal", "workstream_id": "GEN-38",
                "kind": "progress", "source": "agent",
                "payload": {"next_action": "recover predecessor proposal"},
                "expected_revision": 0, "created_at": "now",
            }, child_workstream_id="GEN-38", child_issue_id=CHILD_ID,
            plan_revision=PLAN,
        )
        _append_proposal(client, proposal)
        target_plan = "b" * 64
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": target_plan,
                "description_plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 0,
                "status": "In Progress", "status_type": "started",
                "next_action": "prepare inactive target",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "next_action": "issue action",
                "parent": {"id": ROOT_ID}, "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }

        child = add_child_material_history(
            snapshot, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=deepcopy(client.root_comments),
            proposal_plan_revision=PLAN,
        )["children"][0]

        self.assertEqual(
            [item["proposal_id"] for item in child["pending_child_proposals"]],
            [proposal["proposal_id"]],
        )
        genesis = deepcopy(snapshot)
        genesis["root"].update({
            "plan_revision": PLAN,
            "description_plan_revision": None,
        })
        genesis_child = add_child_material_history(
            genesis, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=None,
        )["children"][0]
        self.assertEqual(
            [item["proposal_id"] for item in genesis_child["pending_child_proposals"]],
            [proposal["proposal_id"]],
        )
        transitioned = deepcopy(snapshot)
        transitioned["root"].update({
            "plan_revision": PLAN,
            "description_plan_revision": "d" * 64,
        })
        transitioned_child = add_child_material_history(
            transitioned, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=None,
        )["children"][0]
        self.assertEqual(
            [
                item["proposal_id"]
                for item in transitioned_child["pending_child_proposals"]
            ],
            [proposal["proposal_id"]],
        )

    def test_reparent_race_cannot_transfer_root_authority_and_resume_reports_drift(self):
        client = FakeChildStateClient()
        other_root = "33333333-3333-4333-8333-333333333333"
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", '{"next_action":"original root action"}',
        ]
        original_select = LinearProjectionAdapter.select_owned_child_generation
        def select_then_reparent(adapter, **values):
            result = original_select(adapter, **values)
            client.child_parent_id = other_root
            return result
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, mock.patch.object(
            LinearProjectionAdapter, "select_owned_child_generation",
            new=select_then_reparent,
        ):
            result = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        grant = result["authorization"]["event"]
        self.assertEqual(grant["authority"]["root_issue_id"], ROOT_ID)
        self.assertEqual(grant["value"]["child_origin"]["kind"],
                         "child_extension_authorization")
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 0,
                "status": "In Progress", "status_type": "started",
                "next_action": "root action",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "next_action": "issue action",
                "parent": {"id": other_root}, "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        child = compact_context(add_child_material_history(
            snapshot, {"GEN-38": deepcopy(client.child_comments)},
            authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
            root_comments=deepcopy(client.root_comments),
        ), "GEN-37")["children"][0]
        self.assertEqual(child["next_action"], "original root action")
        self.assertEqual(child["reconciliation_blockers"][0]["field"],
                         "parent_issue_id")
        with self.assertRaises(Exception):
            LinearProjectionAdapter(
                client, issue_id="GEN-37", workstream_id="GEN-37",
                plan_revision=PLAN, **ROUTE, root_issue_id=other_root,
            ).state()

    def test_death_after_root_activation_replays_without_second_write(self):
        client = FakeChildStateClient(); client.crash_after_root_append = True
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", '{"next_action":"recover"}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, self.assertRaisesRegex(
            SystemExit, "death after root activation",
        ):
            workstream_child_event.run(args, client_factory=lambda _token: client)
        root_writes = len(client.root_comments)
        child_writes = len(client.child_comments)
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            result = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        self.assertEqual(result["authorization"]["disposition"], "existing")
        self.assertEqual(len(client.root_comments), root_writes)
        self.assertEqual(len(client.child_comments), child_writes)

    def test_exact_activation_replays_after_scope_removes_child(self):
        client = FakeChildStateClient()
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", '{"next_action":"historical"}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            first = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        initial_scope = client.scope_event({"GEN-38": "github.com:id:R_repo"})
        removed = build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root",
            value=TOMBSTONE, plan_revision=PLAN, expected_revision=4,
            created_at="later",
            supersedes_event_id=initial_scope["event_id"],
            authority={**ROUTE, "root_issue_id": ROOT_ID},
        )
        client.root_comments.append({
            "id": projection_slot_id(
                removed["workstream_id"], removed["plan_revision"],
                removed["expected_revision"], removed["authority"],
            ), "body": encode_projection_comment(removed),
            "createdAt": "later", "updatedAt": "later",
        })
        root_count = len(client.root_comments); child_count = len(client.child_comments)
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            replay = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        self.assertEqual(first["receipt"], replay["receipt"])
        self.assertEqual(len(client.root_comments), root_count)
        self.assertEqual(len(client.child_comments), child_count)

    def test_different_payload_at_authorized_child_frontier_stays_inert(self):
        client = FakeChildStateClient()
        base = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            workstream_child_event.run(
                [*base, "--payload-json", '{"next_action":"first"}'],
                client_factory=lambda _token: client,
            )
        root_count = len(client.root_comments)
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, self.assertRaisesRegex(
            LinearTransportError, "material_frontier_stale",
        ):
            workstream_child_event.run(
                [*base, "--payload-json", '{"next_action":"different"}'],
                client_factory=lambda _token: client,
            )
        self.assertEqual(len(client.root_comments), root_count)
        self.assertEqual(len(client.child_comments), 1)

    def test_conflicting_explicit_event_id_refuses_before_second_grant(self):
        client = FakeChildStateClient()
        first = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--event-id", "explicit-child-event", "--payload-json", '{"v":1}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            workstream_child_event.run(first, client_factory=lambda _token: client)
        root_count = len(client.root_comments)
        second = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "1", "--created-at", "later",
            "--event-id", "explicit-child-event", "--payload-json", '{"v":2}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch, self.assertRaisesRegex(
            Exception, "event_id_already_authorized",
        ):
            workstream_child_event.run(second, client_factory=lambda _token: client)
        self.assertEqual(len(client.root_comments), root_count)

    def test_second_child_event_receipt_reduces_full_authoritative_history(self):
        client = FakeChildStateClient()
        def event(revision, value):
            return [
                *self.common(), "--kind", "progress", "--source", "user_turn",
                "--expected-revision", str(revision), "--created-at", "now",
                "--payload-json", json.dumps({"value": value}),
            ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            first = workstream_child_event.run(
                event(0, "first"), client_factory=lambda _token: client,
            )
            second = workstream_child_event.run(
                event(1, "second"), client_factory=lambda _token: client,
            )
            root_writes = len(client.root_comments)
            child_writes = len(client.child_comments)
            replay = workstream_child_event.run(
                event(1, "second"), client_factory=lambda _token: client,
            )
        self.assertEqual(first["receipt"]["revision"], 1)
        self.assertEqual(second["receipt"]["revision"], 2)
        self.assertEqual(replay["receipt"], second["receipt"])
        self.assertEqual(len(client.root_comments), root_writes)
        self.assertEqual(len(client.child_comments), child_writes)

    def test_resume_refuses_activated_grant_with_missing_child_origin(self):
        client = FakeChildStateClient()
        args = [
            *self.common(), "--kind", "progress", "--source", "user_turn",
            "--expected-revision", "0", "--created-at", "now",
            "--payload-json", '{"next_action":"must stay authenticated"}',
        ]
        route_patch, auth_patch = self.patches()
        with route_patch, auth_patch:
            result = workstream_child_event.run(
                args, client_factory=lambda _token: client,
            )
        grant = result["authorization"]["event"]
        bad = build_projection_event(
            workstream_id=grant["workstream_id"], kind=grant["kind"],
            key=grant["key"], value={**grant["value"], "child_origin": {}},
            plan_revision=grant["plan_revision"],
            expected_revision=grant["expected_revision"],
            created_at=grant["created_at"], authority=grant["authority"],
        )
        grant_comment = client.root_comments[-1]
        grant_comment["body"] = encode_projection_comment(bad)
        snapshot = {
            "root": {
                "identifier": "GEN-37", "plan_revision": PLAN,
                "url": "https://linear.test/GEN-37", "revision": 0,
                "status": "In Progress", "status_type": "started",
            },
            "children": [{
                "id": CHILD_ID, "identifier": "GEN-38", "title": "Child",
                "url": "https://linear.test/GEN-38", "status": "In Progress",
                "status_type": "started", "parent": {"id": ROOT_ID},
                "project": {"id": "project"},
                "team": {"id": "team", "organization": {"id": "workspace"}},
            }],
        }
        with self.assertRaisesRegex(Exception, "child_origin_provenance"):
            add_child_material_history(
                snapshot, {"GEN-38": deepcopy(client.child_comments)},
                authenticated_route={**ROUTE, "root_issue_id": ROOT_ID},
                root_comments=deepcopy(client.root_comments),
            )

    def test_nonmonotonic_checkpoint_refuses_before_second_grant(self):
        client = FakeChildStateClient()
        def checkpoint(boundary, predecessor):
            return build_checkpoint(
                workstream_id="GEN-38", boundary_id=boundary,
                root_revision=0, plan_revision=PLAN,
                before_status="In Progress", after_status="In Progress",
                execution={
                    "agent": "codex", "provider": "openai",
                    "session_id": boundary, "machine": "M5", "worktree": {
                        "state": "safe", "path": "/repo/child",
                        "branch": "child", "head": "b" * 40,
                    },
                }, exact_head="b" * 40, evidence=[], blocker=None,
                next_action="continue", predecessor_event_id=predecessor,
            )
        with tempfile.TemporaryDirectory() as directory:
            first = checkpoint("first", None)
            first_path = Path(directory) / "first.json"
            first_path.write_text(json.dumps(first))
            route_patch, auth_patch = self.patches()
            with route_patch, auth_patch:
                workstream_child_checkpoint.run([
                    *self.common(), "--checkpoint", str(first_path),
                    "--material-revision", "0", "--no-predecessor",
                ], client_factory=lambda _token: client)
            root_count = len(client.root_comments)
            second = checkpoint("second", first["event_id"])
            second_path = Path(directory) / "second.json"
            second_path.write_text(json.dumps(second))
            route_patch, auth_patch = self.patches()
            with route_patch, auth_patch, self.assertRaisesRegex(
                Exception, "checkpoint_frontier_stale",
            ):
                workstream_child_checkpoint.run([
                    *self.common(), "--checkpoint", str(second_path),
                    "--material-revision", "0",
                    "--predecessor-event-id", first["event_id"],
                ], client_factory=lambda _token: client)
        self.assertEqual(len(client.root_comments), root_count)


if __name__ == "__main__":
    unittest.main()
