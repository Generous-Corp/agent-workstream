#!/usr/bin/env python3
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from workstream_delta import Delta
from workstream_linear import LinearTransportError
from workstream_linear_events import (
    encode_event_comment, encode_ledger_reservation, ledger_boundary_slot_id,
    LinearCommentEventAdapter, reduce_ledger_reservations,
)
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, LinearProjectionAdapter,
    projection_slot_id, reduce_projection_comments,
)
from workstream_repository_identity import (
    MAX_REQUEST_BYTES, _MutationTrackingClient, _reserve_material_frontier,
    _value_digest, GitHubRepositoryResolver,
    main, reconcile_repository_identity, RepositoryIdentityError,
)


PLAN = "f" * 64
HEAD = "a" * 40
ROOT_UUID = "33333333-3333-4333-8333-333333333333"
AUTHORITY = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
    "root_issue_id": ROOT_UUID,
}
OLD = "github.com/danielraffel/pulp"
NEW = "github.com/generous-corp/pulp"
KEY = "github.com:id:R_pulp"


def repository(slug=OLD, provider_id="R_pulp"):
    return {
        "slug": slug, "provider_repository_id": provider_id,
        "aliases": [], "exact_head": HEAD,
        "identity_resolution": {
            "provider_repository_id": provider_id, "resolved_slug": slug,
            "observed_at": "2026-08-20T00:00:00Z",
            "evidence": [{
                "kind": "authenticated_provider_readback", "authenticated": True,
                "provider_repository_id": provider_id, "resolved_slug": slug,
            }],
        },
        "identity_updates": [], "evidence": [],
    }


def scope(*, collision=False):
    repositories = [repository()]
    if collision:
        repositories.append(repository(NEW, "R_other"))
    return {
        "namespace": "pulp-continuity",
        "linear": {
            **AUTHORITY,
            "route_verification": {
                **AUTHORITY, "observed_at": "2026-08-20T00:00:00Z",
                "evidence": [{
                    "kind": "authenticated_linear_readback", "authenticated": True,
                    **AUTHORITY,
                }],
            },
        },
        "primary_repository": KEY,
        "repositories": repositories,
        "child_ownership": {"GEN-38": KEY},
    }


def resolution(**changes):
    value = {
        "provider": "github.com", "provider_repository_id": "R_pulp",
        "repository_key": KEY, "requested_slug": OLD, "resolved_slug": NEW,
        "observed_at": "2026-08-29T12:00:00Z", "redirect_count": 1,
        "requested_response_url": "https://api.github.com/repositories/123",
        "canonical_response_url": "https://api.github.com/repos/generous-corp/pulp",
        "authenticated": True,
    }
    value.update(changes)
    return value


class FakeProjectionClient:
    def __init__(self):
        self.comments = []
        self.write_count = 0
        self.reservation_count = 0
        self.comment_create_count = 0

    def execute(self, query, variables):
        if "CommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}, {"name": "body"}]}}
        if "query WorkstreamDeltaComments" in query:
            return {"issue": {
                "id": ROOT_UUID, "identifier": "GEN-37",
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                "comments": {
                    "nodes": [dict(item) for item in self.comments],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
            }}
        if "commentCreate" in query:
            comment_id = variables["input"]["id"]
            if any(item["id"] == comment_id for item in self.comments):
                raise LinearTransportError("duplicate comment id")
            self.comment_create_count += 1
            if "workstream-ledger-reservation:v1" in variables["input"]["body"]:
                self.reservation_count += 1
            else:
                self.write_count += 1
            comment = {
                "id": comment_id, "body": variables["input"]["body"],
                "createdAt": f"2026-08-29T12:00:{self.comment_create_count:02d}Z",
                "updatedAt": f"2026-08-29T12:00:{self.comment_create_count:02d}Z",
            }
            self.comments.append(comment)
            return {"commentCreate": {"success": True, "comment": dict(comment)}}
        raise AssertionError("unexpected GraphQL operation")


class RacingProjectionClient(FakeProjectionClient):
    def __init__(self):
        super().__init__()
        self.comment_reads = 0

    def execute(self, query, variables):
        if "query WorkstreamDeltaComments" in query:
            self.comment_reads += 1
            if self.comment_reads == 2:
                winner = build_projection_event(
                    workstream_id="GEN-37", kind="provenance", key="race",
                    value={"agent": "codex", "machine": "M5", "session_id": "race"},
                    plan_revision=PLAN, expected_revision=1,
                    created_at="2026-08-29T12:00:00Z", authority=AUTHORITY,
                )
                self.comments.append({
                    "id": projection_slot_id("GEN-37", PLAN, 1, AUTHORITY),
                    "body": encode_projection_comment(winner),
                    "createdAt": "2026-08-29T12:00:00Z",
                    "updatedAt": "2026-08-29T12:00:00Z",
                })
        return super().execute(query, variables)


class MaterialRacingProjectionClient(FakeProjectionClient):
    def __init__(self):
        super().__init__()
        self.comment_reads = 0

    def execute(self, query, variables):
        if "query WorkstreamDeltaComments" in query:
            self.comment_reads += 1
            if self.comment_reads == 3:
                from workstream_delta import Delta
                from workstream_linear_events import encode_event_comment

                delta = Delta(
                    "wsd_race", "GEN-37", "requirement", "agent",
                    {"text": "concurrent material"}, 0, "2026-08-29T12:00:00Z",
                )
                self.comments.append({
                    "id": "material-race", "body": encode_event_comment(delta),
                    "createdAt": "2026-08-29T12:00:00Z",
                    "updatedAt": "2026-08-29T12:00:00Z",
                })
        return super().execute(query, variables)


class LostPostWriteReadbackClient(FakeProjectionClient):
    def __init__(self):
        super().__init__()
        self.fail_readback = True

    def execute(self, query, variables):
        if (
            "query WorkstreamDeltaComments" in query
            and self.write_count == 1 and self.fail_readback
        ):
            raise LinearTransportError("lost post-write readback")
        return super().execute(query, variables)


class SerializationEntryMaterialWinnerClient(FakeProjectionClient):
    """Plant a material winner in the exact shared slot at reservation entry."""

    def __init__(self):
        super().__init__()
        self.injected = False

    def execute(self, query, variables):
        body = ((variables.get("input") or {}).get("body") or "")
        if (
            "commentCreate" in query
            and "workstream-ledger-reservation:v1" in body
            and not self.injected
        ):
            self.injected = True
            material = Delta(
                "wsd_serialized_winner", "GEN-37", "requirement", "agent",
                {"text": "wins shared slot"}, 0, "2026-08-29T12:00:00Z",
            )
            self.comments.append({
                "id": variables["input"]["id"],
                "body": encode_event_comment(material),
                "createdAt": "2026-08-29T12:00:00Z",
                "updatedAt": "2026-08-29T12:00:00Z",
            })
        return super().execute(query, variables)


class SuccessorAfterIdentityReadbackClient(FakeProjectionClient):
    def __init__(self):
        super().__init__()
        self.post_identity_reads = 0

    def execute(self, query, variables):
        if "query WorkstreamDeltaComments" in query and self.write_count == 1:
            self.post_identity_reads += 1
            if self.post_identity_reads == 2:
                state = reduce_projection_comments(
                    self.comments, workstream_id="GEN-37",
                    expected_plan_revision=PLAN, authenticated_route=AUTHORITY,
                )
                head = next(item for item in reversed(state.events)
                            if (item["kind"], item["key"]) == ("scope", "root"))
                successor_value = deepcopy(head["value"])
                successor_value["repositories"][0]["exact_head"] = "b" * 40
                successor = build_projection_event(
                    workstream_id="GEN-37", kind="scope", key="root",
                    value=successor_value, plan_revision=PLAN,
                    expected_revision=state.revision,
                    created_at="2026-08-29T12:01:00Z",
                    supersedes_event_id=head["event_id"], authority=AUTHORITY,
                )
                self.comments.append({
                    "id": projection_slot_id(
                        "GEN-37", PLAN, state.revision, AUTHORITY,
                    ),
                    "body": encode_projection_comment(successor),
                    "createdAt": "2026-08-29T12:01:00Z",
                    "updatedAt": "2026-08-29T12:01:00Z",
                })
        return super().execute(query, variables)


class LostProjectionCreateAfterReservationClient(FakeProjectionClient):
    def __init__(self):
        super().__init__()
        self.lose_projection_once = True

    def execute(self, query, variables):
        body = ((variables.get("input") or {}).get("body") or "")
        if (
            "commentCreate" in query
            and "workstream-projection:v1" in body
            and self.lose_projection_once
        ):
            self.lose_projection_once = False
            raise LinearTransportError("projection outcome unknown")
        return super().execute(query, variables)


def adapter_with_scope(value=None):
    client = FakeProjectionClient()
    adapter = LinearProjectionAdapter(
        client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN,
        **AUTHORITY,
    )
    event = build_projection_event(
        workstream_id="GEN-37", kind="scope", key="root", value=value or scope(),
        plan_revision=PLAN, expected_revision=0,
        created_at="2026-08-20T00:00:00Z", authority=AUTHORITY,
    )
    client.comments.append({
        "id": projection_slot_id("GEN-37", PLAN, 0, AUTHORITY),
        "body": encode_projection_comment(event),
        "createdAt": "2026-08-20T00:00:00Z", "updatedAt": "2026-08-20T00:00:00Z",
    })
    return adapter, client, event


def racing_adapter_with_scope():
    _adapter, initial, event = adapter_with_scope()
    client = RacingProjectionClient()
    client.comments = deepcopy(initial.comments)
    adapter = LinearProjectionAdapter(
        client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN,
        **AUTHORITY,
    )
    return adapter, client, event


def special_adapter_with_scope(client):
    _adapter, initial, event = adapter_with_scope()
    client.comments = deepcopy(initial.comments)
    tracked = _MutationTrackingClient(client)
    adapter = LinearProjectionAdapter(
        tracked, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN,
        **AUTHORITY,
    )
    return adapter, client, event


def apply(adapter, event, **changes):
    values = {
        "resolution": resolution(), "expected_material_revision": 0,
        "expected_projection_revision": 1,
        "expected_scope_event_id": event["event_id"],
        "expected_scope_sha256": _value_digest(event["value"]),
    }
    values.update(changes)
    return reconcile_repository_identity(adapter, **values)


class RepositoryIdentityWriterTests(unittest.TestCase):
    def test_single_redirect_appends_one_identity_update_and_replay_is_noop(self):
        adapter, client, event = adapter_with_scope()
        first = apply(adapter, event)
        self.assertEqual(first["disposition"], "created")
        self.assertEqual(client.write_count, 1)
        projected = adapter.state().snapshot["scope"]
        repo = projected["repositories"][0]
        self.assertEqual(repo["slug"], NEW)
        self.assertEqual(repo["aliases"], [OLD])
        self.assertEqual(len(repo["identity_updates"]), 1)
        self.assertRegex(repo["identity_updates"][0]["event_id"], r"^wsri_[0-9a-f]{32}$")

        replay = apply(adapter, event)
        self.assertEqual(replay["disposition"], "existing")
        self.assertEqual(replay["write_count"], 0)
        self.assertEqual(client.write_count, 1)
        self.assertEqual(
            len(adapter.state().snapshot["scope"]["repositories"][0]["identity_updates"]),
            1,
        )

    def test_stale_material_projection_and_scope_frontiers_refuse_before_write(self):
        cases = (
            {"expected_material_revision": 1},
            {"expected_projection_revision": 0},
            {"expected_scope_event_id": "wsp_" + ("0" * 32)},
            {"expected_scope_sha256": "0" * 64},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                adapter, client, event = adapter_with_scope()
                with self.assertRaisesRegex(RepositoryIdentityError, "stale_reload_required"):
                    apply(adapter, event, **changes)
                self.assertEqual(client.write_count, 0)
                self.assertEqual(len(client.comments), 1)

    def test_multi_hop_and_owner_collision_refuse_without_write(self):
        adapter, client, event = adapter_with_scope()
        with self.assertRaisesRegex(RepositoryIdentityError, "not_single_hop"):
            apply(adapter, event, resolution=resolution(redirect_count=2))
        self.assertEqual(client.write_count, 0)

        adapter, client, event = adapter_with_scope(scope(collision=True))
        with self.assertRaisesRegex(RepositoryIdentityError, "coordinate_collision"):
            apply(adapter, event)
        self.assertEqual(client.write_count, 0)

    def test_projection_race_at_final_prewrite_fence_refuses_without_writer_mutation(self):
        adapter, client, event = racing_adapter_with_scope()
        with self.assertRaisesRegex(RepositoryIdentityError, "projection_frontier_stale"):
            apply(adapter, event)
        self.assertEqual(client.write_count, 0)
        self.assertEqual(len(client.comments), 2)  # initial scope plus planted competitor

    def test_material_race_inside_append_final_read_refuses_before_writer_mutation(self):
        adapter, client, event = special_adapter_with_scope(
            MaterialRacingProjectionClient(),
        )
        with self.assertRaisesRegex(LinearTransportError, "material_frontier_stale"):
            apply(adapter, event)
        self.assertEqual(client.write_count, 0)
        self.assertEqual(len(client.comments), 3)  # scope, reservation, material

    def test_material_writer_winning_shared_slot_at_create_entry_blocks_identity_write(self):
        adapter, client, event = special_adapter_with_scope(
            SerializationEntryMaterialWinnerClient(),
        )
        with self.assertRaisesRegex(RepositoryIdentityError, "serialization_slot_lost"):
            apply(adapter, event)
        self.assertTrue(client.injected)
        self.assertEqual(client.write_count, 0)
        self.assertEqual(client.reservation_count, 0)
        self.assertEqual(len(client.comments), 2)  # initial scope plus material winner

    def test_material_writer_refuses_while_identity_reservation_is_pending(self):
        _adapter, client, initial = adapter_with_scope()
        intended = build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root", value=scope(),
            plan_revision=PLAN, expected_revision=1,
            created_at="2026-08-29T12:00:00Z",
            supersedes_event_id=initial["event_id"], authority=AUTHORITY,
        )
        reservation = {
            "schema_version": 1, "workstream_id": "GEN-37",
            "material_revision": 0,
            "plan_revision": PLAN,
            "projection_revision": 1,
            "projection_frontier_ids": [client.comments[0]["id"]],
            "frontier_ids": [], "authority": AUTHORITY,
            "intent_kind": "repository_identity_projection",
            "intent_event": intended,
            "intent_sha256": _value_digest(intended),
        }
        client.comments.append({
            "id": ledger_boundary_slot_id("GEN-37", 0, [], AUTHORITY),
            "body": encode_ledger_reservation(reservation),
            "createdAt": "2026-08-29T12:00:00Z",
            "updatedAt": "2026-08-29T12:00:00Z",
        })
        material_adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
        delta = Delta(
            "wsd_waiting", "GEN-37", "requirement", "agent",
            {"text": "must wait"}, 0, "2026-08-29T12:00:00Z",
        )
        with self.assertRaisesRegex(LinearTransportError, "ledger_boundary_reserved"):
            material_adapter.apply(delta)
        self.assertEqual(client.write_count, 0)

        successor = build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="successor",
            value={"agent": "codex", "machine": "M5", "session_id": "next"},
            plan_revision=PLAN, expected_revision=1,
            created_at="2026-08-29T12:01:00Z", authority=AUTHORITY,
        )
        client.comments.append({
            "id": projection_slot_id("GEN-37", PLAN, 1, AUTHORITY),
            "body": encode_projection_comment(successor),
            "createdAt": "2026-08-29T12:01:00Z",
            "updatedAt": "2026-08-29T12:01:00Z",
        })
        receipt = material_adapter.apply(delta)
        self.assertEqual(receipt.event_id, "wsd_waiting")
        self.assertEqual(client.write_count, 1)

    def test_unproven_arbitrary_high_revision_and_old_plan_reservations_do_not_block(self):
        def material_result(extra_comments):
            _adapter, client, _initial = adapter_with_scope()
            client.comments.extend(extra_comments(client))
            material_adapter = LinearCommentEventAdapter(client, issue_id="GEN-37")
            receipt = material_adapter.apply(Delta(
                "wsd_safe", "GEN-37", "requirement", "agent",
                {"text": "not poisoned"}, 0, "2026-08-29T12:02:00Z",
            ))
            self.assertEqual(receipt.event_id, "wsd_safe")
            self.assertEqual(client.write_count, 1)

        def arbitrary_id(client):
            initial = reduce_projection_comments(
                client.comments, workstream_id="GEN-37",
                expected_plan_revision=PLAN, authenticated_route=AUTHORITY,
            ).events[0]
            intended = build_projection_event(
                workstream_id="GEN-37", kind="scope", key="root", value=scope(),
                plan_revision=PLAN, expected_revision=1,
                created_at="2026-08-29T12:01:00Z",
                supersedes_event_id=initial["event_id"], authority=AUTHORITY,
            )
            reservation = {
                "schema_version": 1, "workstream_id": "GEN-37",
                "material_revision": 0, "plan_revision": PLAN,
                "projection_revision": 1,
                "projection_frontier_ids": [client.comments[0]["id"]],
                "frontier_ids": [], "authority": AUTHORITY,
                "intent_kind": "repository_identity_projection",
                "intent_event": intended, "intent_sha256": _value_digest(intended),
            }
            return [{
                "id": "attacker-chosen-id", "body": encode_ledger_reservation(reservation),
                "createdAt": "2026-08-29T12:01:00Z",
                "updatedAt": "2026-08-29T12:01:00Z",
            }]

        def high_revision(_client):
            intended = build_projection_event(
                workstream_id="GEN-37", kind="scope", key="root", value=scope(),
                plan_revision=PLAN, expected_revision=999999,
                created_at="2026-08-29T12:01:00Z", authority=AUTHORITY,
            )
            malformed = {
                "schema_version": 1, "workstream_id": "GEN-37",
                "material_revision": 0, "plan_revision": PLAN,
                "projection_revision": 999999, "projection_frontier_ids": [],
                "frontier_ids": [], "authority": AUTHORITY,
                "intent_kind": "repository_identity_projection",
                "intent_event": intended, "intent_sha256": _value_digest(intended),
            }
            canonical = json.dumps(
                malformed, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode()
            envelope = {"reservation": malformed, "sha256": hashlib.sha256(canonical).hexdigest()}
            encoded = base64.urlsafe_b64encode(json.dumps(
                envelope, sort_keys=True, separators=(",", ":"),
            ).encode()).decode().rstrip("=")
            return [{
                "id": ledger_boundary_slot_id("GEN-37", 0, [], AUTHORITY),
                "body": f"<!-- workstream-ledger-reservation:v1:{encoded} -->",
                "createdAt": "2026-08-29T12:01:00Z",
                "updatedAt": "2026-08-29T12:01:00Z",
            }]

        def old_plan(client):
            old_plan = "e" * 64
            old = build_projection_event(
                workstream_id="GEN-37", kind="scope", key="root", value=scope(),
                plan_revision=old_plan, expected_revision=0,
                created_at="2026-08-19T12:00:00Z", authority=AUTHORITY,
            )
            old_id = projection_slot_id("GEN-37", old_plan, 0, AUTHORITY)
            intended = build_projection_event(
                workstream_id="GEN-37", kind="scope", key="root", value=scope(),
                plan_revision=old_plan, expected_revision=1,
                created_at="2026-08-19T12:01:00Z",
                supersedes_event_id=old["event_id"], authority=AUTHORITY,
            )
            reservation = {
                "schema_version": 1, "workstream_id": "GEN-37",
                "material_revision": 0, "plan_revision": old_plan,
                "projection_revision": 1, "projection_frontier_ids": [old_id],
                "frontier_ids": [], "authority": AUTHORITY,
                "intent_kind": "repository_identity_projection",
                "intent_event": intended, "intent_sha256": _value_digest(intended),
            }
            return [
                {"id": old_id, "body": encode_projection_comment(old),
                 "createdAt": "2026-08-19T12:00:00Z", "updatedAt": "2026-08-19T12:00:00Z"},
                {"id": ledger_boundary_slot_id("GEN-37", 0, [], AUTHORITY),
                 "body": encode_ledger_reservation(reservation),
                 "createdAt": "2026-08-19T12:01:00Z", "updatedAt": "2026-08-19T12:01:00Z"},
            ]

        for name, factory in (
            ("arbitrary_id", arbitrary_id),
            ("high_revision", high_revision),
            ("old_plan", old_plan),
        ):
            with self.subTest(name=name):
                material_result(factory)

    def test_oversized_reservation_intent_refuses_before_comment_create(self):
        value = scope()
        value["padding"] = "x" * (93 * 1024)
        adapter, client, event = adapter_with_scope(value)
        intent = build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root", value=value,
            plan_revision=PLAN, expected_revision=1,
            created_at="2026-08-29T12:00:00Z",
            supersedes_event_id=event["event_id"], authority=AUTHORITY,
        )
        no_network = mock.Mock()
        adapter.client = no_network
        with self.assertRaisesRegex(LinearTransportError, "reservation_too_large"):
            _reserve_material_frontier(
                adapter, comments=client.comments, material_revision=0,
                intent_event=intent,
            )
        self.assertEqual(client.comment_create_count, 0)
        no_network.execute.assert_not_called()

    def test_lost_post_write_readback_is_unconfirmed_then_replay_converges(self):
        adapter, client, event = special_adapter_with_scope(
            LostPostWriteReadbackClient(),
        )
        uncertain = apply(adapter, event)
        self.assertEqual(uncertain["disposition"], "landed_unconfirmed")
        self.assertTrue(uncertain["reconcile_required"])
        self.assertEqual(client.write_count, 1)

        client.fail_readback = False
        replay = apply(adapter, event)
        self.assertEqual(replay["disposition"], "existing")
        self.assertEqual(client.write_count, 1)

    def test_replay_reuses_exact_pending_reservation_without_duplicate(self):
        adapter, client, event = special_adapter_with_scope(
            LostProjectionCreateAfterReservationClient(),
        )
        uncertain = apply(adapter, event)
        self.assertEqual(uncertain["disposition"], "landed_unconfirmed")
        self.assertEqual(client.reservation_count, 1)
        self.assertEqual(client.write_count, 0)
        stored = reduce_ledger_reservations(
            client.comments, workstream_id="GEN-37",
        )
        self.assertEqual(len(stored), 1)
        self.assertEqual(
            stored[0][0]["intent_event"]["event_id"],
            uncertain["scope_event_id"],
        )

        completed = apply(adapter, event)
        self.assertEqual(completed["disposition"], "created")
        self.assertEqual(client.reservation_count, 1)
        self.assertEqual(client.write_count, 1)

    def test_successor_after_acknowledged_identity_readback_is_not_false_refusal(self):
        adapter, client, event = special_adapter_with_scope(
            SuccessorAfterIdentityReadbackClient(),
        )
        result = apply(adapter, event)
        self.assertEqual(result["disposition"], "created_superseded")
        self.assertEqual(result["write_count"], 1)
        self.assertEqual(client.write_count, 1)
        self.assertRegex(result["superseded_by_scope_event_id"], r"^wsp_[0-9a-f]{32}$")

    def test_later_scope_replacement_cannot_erase_or_alter_identity_history(self):
        adapter, client, event = adapter_with_scope()
        apply(adapter, event)
        state = adapter.state()
        current = next(item for item in reversed(state.events)
                       if (item["kind"], item["key"]) == ("scope", "root"))
        for mutate in (
            "erase", "alter", "remove_repository", "resolution", "provider_id",
        ):
            with self.subTest(mutate=mutate):
                changed = deepcopy(current["value"])
                repo = changed["repositories"][0]
                if mutate == "remove_repository":
                    changed["repositories"] = []
                    changed["primary_repository"] = None
                    changed["child_ownership"] = {}
                elif mutate == "erase":
                    repo["aliases"] = []
                    repo["identity_updates"] = []
                elif mutate == "alter":
                    repo["identity_updates"][0]["effective_at"] = "2026-08-30T00:00:00Z"
                elif mutate == "resolution":
                    repo["identity_resolution"]["observed_at"] = "2026-08-30T00:00:00Z"
                else:
                    repo["provider_repository_id"] = "R_evil"
                    repo["identity_resolution"]["provider_repository_id"] = "R_evil"
                    repo["identity_resolution"]["evidence"][0][
                        "provider_repository_id"
                    ] = "R_evil"
                replacement = build_projection_event(
                    workstream_id="GEN-37", kind="scope", key="root", value=changed,
                    plan_revision=PLAN, expected_revision=state.revision,
                    created_at="2026-08-30T00:00:00Z",
                    supersedes_event_id=current["event_id"], authority=AUTHORITY,
                )
                with self.assertRaisesRegex(LinearTransportError, "identity_history_regressed"):
                    adapter.append(replacement)
                self.assertEqual(client.write_count, 1)

    def test_recycled_alias_with_different_provider_id_refuses_without_write(self):
        adapter, client, event = adapter_with_scope()
        with self.assertRaisesRegex(RepositoryIdentityError, "selection_ambiguous"):
            apply(adapter, event, resolution=resolution(
                provider_repository_id="R_recycled",
                repository_key="github.com:id:R_recycled",
            ))
        self.assertEqual(client.write_count, 0)

    def test_forged_legacy_replay_without_full_deterministic_proof_refuses(self):
        value = scope()
        repo = value["repositories"][0]
        repo["slug"] = NEW
        repo["aliases"] = [OLD]
        repo["identity_updates"] = [{
            "from": OLD, "to": NEW, "repository_key": KEY,
            "provider_repository_id": "R_pulp",
            "observed_at": "2026-08-29T12:00:00Z",
            "evidence": [{
                "kind": "authenticated_provider_readback", "authenticated": True,
                "repository_key": KEY, "provider_repository_id": "R_pulp",
                "requested_slug": OLD, "resolved_slug": NEW,
            }],
        }]
        repo["identity_resolution"] = {
            "provider_repository_id": "R_pulp", "resolved_slug": NEW,
            "observed_at": "2026-08-29T12:00:00Z",
            "evidence": [{
                "kind": "authenticated_provider_readback", "authenticated": True,
                "provider_repository_id": "R_pulp", "resolved_slug": NEW,
            }],
        }
        adapter, client, event = adapter_with_scope(value)
        with self.assertRaisesRegex(RepositoryIdentityError, "replay_mismatch"):
            apply(adapter, event)
        self.assertEqual(client.write_count, 0)


class FakeResponse:
    def __init__(self, payload, url):
        self.payload = json.dumps(payload).encode()
        self.url = url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _limit):
        return self.payload

    def geturl(self):
        return self.url


class FakeOpener:
    def __init__(self, handler, payloads):
        self.handler = handler
        self.payloads = payloads

    def open(self, request, timeout):
        payload, final_url, redirected = self.payloads.pop(0)
        if redirected:
            self.handler.locations.append(final_url)
        return FakeResponse(payload, final_url)


class GitHubResolverTests(unittest.TestCase):
    def resolver(self, first_id="123", second_id="123"):
        payloads = [
            ({"id": int(first_id), "node_id": "R_pulp", "full_name": "Generous-Corp/pulp"},
             "https://api.github.com/repositories/123", True),
            ({"id": int(second_id), "node_id": "R_pulp", "full_name": "Generous-Corp/pulp"},
             "https://api.github.com/repos/generous-corp/pulp", False),
        ]
        return GitHubRepositoryResolver(
            "token", opener_factory=lambda handler, _https: FakeOpener(handler, payloads),
        )

    def test_authenticated_old_and_canonical_reads_bind_same_immutable_id(self):
        result = self.resolver().resolve(
            requested_slug=OLD, provider_repository_id="R_pulp",
            observed_at="2026-08-29T12:00:00Z",
        )
        self.assertEqual(result["resolved_slug"], NEW)
        self.assertEqual(result["repository_key"], KEY)

    def test_provider_id_change_between_old_and_canonical_reads_refuses(self):
        with self.assertRaisesRegex(RepositoryIdentityError, "repository_id_mismatch"):
            self.resolver(second_id="456").resolve(
                requested_slug=OLD, provider_repository_id="R_pulp",
            )


class RepositoryIdentityCliTests(unittest.TestCase):
    def request(self):
        _adapter, _client, event = adapter_with_scope()
        return {
            "schema_version": 1, "workstream_id": "GEN-37",
            "authority": deepcopy(AUTHORITY),
            "plan_revision": PLAN,
            "repository": {
                "requested_slug": OLD, "provider_repository_id": "R_pulp",
            },
            "expected_frontier": {
                "material_revision": 0, "projection_revision": 1,
                "scope_event_id": event["event_id"],
                "scope_sha256": _value_digest(event["value"]),
            },
        }

    def write_request(self, raw):
        temp = tempfile.TemporaryDirectory()
        path = Path(temp.name) / "request.json"
        path.write_text(raw if isinstance(raw, str) else json.dumps(raw))
        self.addCleanup(temp.cleanup)
        return path

    def test_duplicate_json_key_refuses_before_credentials_or_provider(self):
        raw = json.dumps(self.request()).replace(
            '"schema_version": 1,', '"schema_version": 1, "schema_version": 1,', 1,
        )
        path = self.write_request(raw)
        with mock.patch("workstream_repository_identity.load_linear_api_key") as token, \
             mock.patch("workstream_repository_identity.GitHubRepositoryResolver") as provider, \
             mock.patch("workstream_repository_identity.sys.stderr", io.StringIO()):
            self.assertEqual(main(["--request", str(path), "--apply"]), 2)
        token.assert_not_called()
        provider.assert_not_called()

    def test_invalid_static_frontiers_refuse_before_credentials_or_provider(self):
        for field, value in (("material_revision", False), ("projection_revision", -1)):
            with self.subTest(field=field, value=value):
                request = self.request()
                request["expected_frontier"][field] = value
                path = self.write_request(request)
                with mock.patch("workstream_repository_identity.load_linear_api_key") as token, \
                     mock.patch("workstream_repository_identity.GitHubRepositoryResolver") as provider, \
                     mock.patch("workstream_repository_identity.sys.stderr", io.StringIO()):
                    self.assertEqual(main(["--request", str(path), "--apply"]), 2)
                token.assert_not_called()
                provider.assert_not_called()

    def test_invalid_schema_authority_and_scope_id_refuse_before_external_access(self):
        cases = (
            ("schema_bool", lambda request: request.update(schema_version=True)),
            ("root_uuid", lambda request: request["authority"].update(root_issue_id="GEN-37")),
            ("scope_id", lambda request: request["expected_frontier"].update(scope_event_id="wsp_bad")),
            ("authority_bound", lambda request: request["authority"].update(workspace_id="x" * 257)),
            ("workstream_bound", lambda request: request.update(workstream_id=("G" * 65) + "-1")),
        )
        for name, mutate in cases:
            with self.subTest(name=name):
                request = self.request()
                mutate(request)
                path = self.write_request(request)
                with mock.patch("workstream_repository_identity.load_linear_api_key") as token, \
                     mock.patch("workstream_repository_identity.GitHubRepositoryResolver") as provider, \
                     mock.patch("workstream_repository_identity.sys.stderr", io.StringIO()):
                    self.assertEqual(main(["--request", str(path), "--apply"]), 2)
                token.assert_not_called()
                provider.assert_not_called()

    def test_public_malicious_linear_endpoint_option_is_rejected_without_token_access(self):
        path = self.write_request(self.request())
        with mock.patch("workstream_repository_identity.load_linear_api_key") as token, \
             mock.patch("workstream_repository_identity.sys.stderr", io.StringIO()):
            with self.assertRaises(SystemExit):
                main([
                    "--request", str(path), "--apply", "--linear-endpoint",
                    "https://attacker.example/graphql",
                ])
        token.assert_not_called()

    def test_oversized_request_refuses_before_credentials_or_provider(self):
        path = self.write_request("{" + (" " * (93 * 1024)) + "}")
        with mock.patch("workstream_repository_identity.load_linear_api_key") as token, \
             mock.patch("workstream_repository_identity.GitHubRepositoryResolver") as provider, \
             mock.patch("workstream_repository_identity.sys.stderr", io.StringIO()):
            self.assertEqual(main(["--request", str(path), "--apply"]), 2)
        token.assert_not_called()
        provider.assert_not_called()

    def test_request_reader_physically_caps_read_and_deep_json_refuses_safely(self):
        bounded = mock.mock_open(read_data=b"{}")
        with mock.patch.object(Path, "open", bounded), \
             mock.patch("workstream_repository_identity.load_linear_api_key") as token, \
             mock.patch("workstream_repository_identity.sys.stderr", io.StringIO()):
            self.assertEqual(main(["--request", "ignored", "--apply"]), 2)
        bounded().read.assert_called_once_with(MAX_REQUEST_BYTES + 1)
        token.assert_not_called()

        path = self.write_request(("[" * 2000) + ("0") + ("]" * 2000))
        with mock.patch("workstream_repository_identity.load_linear_api_key") as token, \
             mock.patch("workstream_repository_identity.sys.stderr", io.StringIO()):
            self.assertEqual(main(["--request", str(path), "--apply"]), 2)
        token.assert_not_called()

    def test_linear_mutation_uses_immutable_root_issue_id_and_official_authority(self):
        path = self.write_request(self.request())
        fake_http = mock.Mock()
        fake_resolver = mock.Mock()
        fake_resolver.resolve.return_value = resolution()
        with mock.patch("workstream_repository_identity.load_linear_api_key", return_value="secret"), \
             mock.patch("workstream_repository_identity.os.environ.get", return_value="github"), \
             mock.patch("workstream_repository_identity.HttpGraphQLClient", return_value=fake_http) as http, \
             mock.patch("workstream_repository_identity.GitHubRepositoryResolver", return_value=fake_resolver), \
             mock.patch("workstream_repository_identity.reconcile_repository_identity") as reconcile, \
             mock.patch("workstream_repository_identity.sys.stdout", io.StringIO()):
            reconcile.return_value = {"disposition": "existing", "write_count": 0}
            self.assertEqual(main(["--request", str(path), "--apply"]), 0)
        http.assert_called_once_with("secret", "https://api.linear.app/graphql")
        adapter = reconcile.call_args.args[0]
        self.assertEqual(adapter.issue_id, ROOT_UUID)
        self.assertEqual(adapter.workstream_id, "GEN-37")


if __name__ == "__main__":
    unittest.main()
