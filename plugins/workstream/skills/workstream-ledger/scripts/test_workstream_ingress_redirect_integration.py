#!/usr/bin/env python3
"""Focused cross-feature proof for ingress promotion and identity fencing."""

from __future__ import annotations

import hashlib
import json
import unittest
from unittest import mock

import workstream_config
import workstream_ingress as ingress
import workstream_linear
from workstream_delta import Delta
from workstream_linear import LinearTransportError
from workstream_linear_events import (
    LinearCommentEventAdapter,
    encode_ledger_reservation,
    ledger_boundary_slot_id,
)
from workstream_linear_projection import (
    build_projection_event,
    encode_projection_comment,
    projection_slot_id,
)


WORKSTREAM = "GEN-37"
PLAN = "b" * 64
OTHER_PLAN = "c" * 64
AUTHORITY = {
    "workspace_id": "11111111-1111-4111-8111-111111111111",
    "team_id": "22222222-2222-4222-8222-222222222222",
    "project_id": "33333333-3333-4333-8333-333333333333",
    "root_issue_id": "44444444-4444-4444-8444-444444444444",
}


class FakeLinearClient:
    def __init__(self) -> None:
        self.comments: list[dict] = []
        self.comment_creates = 0

    def execute(self, query, variables):
        if "WorkstreamEventCommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}, {"name": "body"}]}}
        if "query WorkstreamDeltaComments" in query:
            return {"issue": {
                "id": AUTHORITY["root_issue_id"],
                "identifier": WORKSTREAM,
                "team": {
                    "id": AUTHORITY["team_id"],
                    "organization": {"id": AUTHORITY["workspace_id"]},
                },
                "project": {"id": AUTHORITY["project_id"]},
                "comments": {
                    "nodes": [dict(item) for item in self.comments],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
            }}
        if "commentCreate" in query:
            self.comment_creates += 1
            comment_id = variables["input"]["id"]
            if any(item["id"] == comment_id for item in self.comments):
                raise LinearTransportError("duplicate comment id")
            comment = {
                "id": comment_id,
                "body": variables["input"]["body"],
                "createdAt": "2026-08-29T12:00:02Z",
                "updatedAt": "2026-08-29T12:00:02Z",
            }
            self.comments.append(comment)
            return {"commentCreate": {"success": True, "comment": dict(comment)}}
        raise AssertionError("unexpected GraphQL operation")


def capture() -> dict:
    return {
        "schema_version": 1,
        "event_id": "wsi_raw",
        "captured_at": "2026-08-29T01:00:00Z",
        "provider": "codex",
        "session_id": "expired",
        "workstream_id": WORKSTREAM,
        "context_url": "https://linear.app/generous/issue/GEN-37/x",
        "prompt": "Preserve this material boundary",
        "prompt_sha256": "a" * 64,
    }


def request(plan_revision: str = PLAN) -> dict:
    return {
        "schema_version": 1,
        "ingress": {
            "repo": "private/ingress",
            "remote_issue": 7,
            "event_id": "wsi_raw",
            "prompt_sha256": "a" * 64,
        },
        "authority": dict(AUTHORITY),
        "workstream_id": WORKSTREAM,
        "plan_revision": plan_revision,
        "expected_material_revision": 0,
        "changes": [{
            "kind": "requirement",
            "payload": {"text": "Preserve this material boundary"},
        }],
    }


def scope_value() -> dict:
    return {
        "namespace": "pulp-continuity",
        "linear": {
            **AUTHORITY,
            "route_verification": {
                **AUTHORITY,
                "observed_at": "2026-08-29T12:00:00Z",
                "evidence": [{
                    "kind": "authenticated_linear_readback",
                    "authenticated": True,
                    **AUTHORITY,
                }],
            },
        },
        "primary_repository": "github.com:id:R_pulp",
        "repositories": [{
            "slug": "github.com/generous-corp/pulp",
            "provider_repository_id": "R_pulp",
            "aliases": ["github.com/danielraffel/pulp"],
            "exact_head": "d" * 40,
            "identity_resolution": {
                "provider_repository_id": "R_pulp",
                "resolved_slug": "github.com/generous-corp/pulp",
                "observed_at": "2026-08-29T12:00:00Z",
                "evidence": [{
                    "kind": "authenticated_provider_readback",
                    "authenticated": True,
                    "provider_repository_id": "R_pulp",
                    "resolved_slug": "github.com/generous-corp/pulp",
                }],
            },
            "identity_updates": [],
            "evidence": [],
        }],
        "child_ownership": {},
    }


def plant_pending_reservation(client: FakeLinearClient, plan_revision: str) -> None:
    initial = build_projection_event(
        workstream_id=WORKSTREAM,
        kind="scope",
        key="root",
        value=scope_value(),
        plan_revision=plan_revision,
        expected_revision=0,
        created_at="2026-08-29T12:00:00Z",
        authority=AUTHORITY,
    )
    initial_id = projection_slot_id(WORKSTREAM, plan_revision, 0, AUTHORITY)
    client.comments.append({
        "id": initial_id,
        "body": encode_projection_comment(initial),
        "createdAt": "2026-08-29T12:00:00Z",
        "updatedAt": "2026-08-29T12:00:00Z",
    })
    intended = build_projection_event(
        workstream_id=WORKSTREAM,
        kind="scope",
        key="root",
        value=scope_value(),
        plan_revision=plan_revision,
        expected_revision=1,
        created_at="2026-08-29T12:00:01Z",
        supersedes_event_id=initial["event_id"],
        authority=AUTHORITY,
    )
    reservation = {
        "schema_version": 1,
        "workstream_id": WORKSTREAM,
        "material_revision": 0,
        "intent_kind": "repository_identity_projection",
        "plan_revision": plan_revision,
        "projection_revision": 1,
        "projection_frontier_ids": [initial_id],
        "frontier_ids": [],
        "authority": dict(AUTHORITY),
        "intent_event": intended,
        "intent_sha256": hashlib.sha256(json.dumps(
            intended, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest(),
    }
    client.comments.append({
        "id": ledger_boundary_slot_id(WORKSTREAM, 0, [], AUTHORITY),
        "body": encode_ledger_reservation(reservation),
        "createdAt": "2026-08-29T12:00:01Z",
        "updatedAt": "2026-08-29T12:00:01Z",
    })


def material_adapter(client: FakeLinearClient, plan_revision: str | None) -> LinearCommentEventAdapter:
    return LinearCommentEventAdapter(
        client,
        issue_id=WORKSTREAM,
        workspace_id=AUTHORITY["workspace_id"],
        team_id=AUTHORITY["team_id"],
        project_id=AUTHORITY["project_id"],
        root_issue_id=AUTHORITY["root_issue_id"],
        plan_revision=plan_revision,
    )


class IngressRedirectIntegrationTests(unittest.TestCase):
    def test_promotion_carries_authenticated_root_and_plan_into_adapter(self):
        client = FakeLinearClient()
        promotion = ingress.promotion_payload(request(), capture())
        with (
            mock.patch.object(workstream_config, "load_linear_api_key", return_value="token"),
            mock.patch.object(
                workstream_config,
                "resolve_linear_route",
                return_value=(dict(AUTHORITY), "test"),
            ),
            mock.patch.object(workstream_linear, "HttpGraphQLClient", return_value=client),
        ):
            adapter = ingress.linear_adapter_for_promotion(promotion)

        self.assertEqual(adapter.root_issue_id, AUTHORITY["root_issue_id"])
        self.assertEqual(adapter.plan_revision, PLAN)
        self.assertRegex(adapter.plan_revision, r"^[0-9a-f]{64}$")
        self.assertEqual(adapter.comments(), [])
        self.assertEqual(client.comment_creates, 0)

    def test_current_plan_reservation_blocks_promotion_material_with_zero_write(self):
        client = FakeLinearClient()
        plant_pending_reservation(client, PLAN)
        adapter = material_adapter(client, PLAN)

        with self.assertRaisesRegex(LinearTransportError, "ledger_boundary_reserved"):
            adapter.apply(ingress.promotion_delta(ingress.promotion_payload(request(), capture())))

        self.assertEqual(client.comment_creates, 0)

    def test_stale_plan_reservation_does_not_block_current_promotion(self):
        client = FakeLinearClient()
        plant_pending_reservation(client, OTHER_PLAN)
        adapter = material_adapter(client, PLAN)

        receipt = adapter.apply(
            ingress.promotion_delta(ingress.promotion_payload(request(), capture()))
        )

        self.assertEqual(receipt.event_id, ingress.promotion_delta(
            ingress.promotion_payload(request(), capture())
        ).event_id)
        self.assertEqual(client.comment_creates, 1)

    def test_missing_or_invalid_plan_authority_refuses_before_mutation(self):
        client = FakeLinearClient()
        plant_pending_reservation(client, PLAN)
        delta = ingress.promotion_delta(ingress.promotion_payload(request(), capture()))

        with self.assertRaisesRegex(
            LinearTransportError, "ledger_reservation_plan_authority_required"
        ):
            material_adapter(client, None).apply(delta)
        self.assertEqual(client.comment_creates, 0)

        with self.assertRaisesRegex(ValueError, "invalid plan revision"):
            material_adapter(client, "not-a-64-hex-revision")
        self.assertEqual(client.comment_creates, 0)

    def test_plan_revision_changes_promotion_and_material_identity(self):
        first_promotion = ingress.promotion_payload(request(PLAN), capture())
        second_promotion = ingress.promotion_payload(request(OTHER_PLAN), capture())
        first_delta = ingress.promotion_delta(first_promotion)
        second_delta = ingress.promotion_delta(second_promotion)

        self.assertNotEqual(first_promotion["promotion_id"], second_promotion["promotion_id"])
        self.assertNotEqual(first_delta.event_id, second_delta.event_id)
        self.assertEqual(first_delta.payload["ingress"]["plan_revision"], PLAN)
        self.assertEqual(second_delta.payload["ingress"]["plan_revision"], OTHER_PLAN)


if __name__ == "__main__":
    unittest.main()
