#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy
import hashlib
import unittest
from unittest import mock

from workstream_linear import LinearTransportError
from workstream_linear_events import encode_ledger_reservation, ledger_boundary_slot_id
from workstream_linear_projection import (
    build_projection_event, encode_projection_comment, projection_prefix_sha256,
    projection_slot_id,
)
import workstream_repository_identity_seal as seal_module


PLAN = "f" * 64
ROOT = "33333333-3333-4333-8333-333333333333"
AUTHORITY = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
    "root_issue_id": ROOT,
}
OLD = "github.com/danielraffel/agent-workstream"
NEW = "github.com/generous-corp/agent-workstream"
PROVIDER = "R_agent_workstream"
KEY = f"github.com:id:{PROVIDER}"


def scope():
    return {
        "namespace": "test",
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
        "repositories": [{
            "slug": NEW, "provider_repository_id": PROVIDER, "aliases": [],
            "exact_head": "a" * 40,
            "identity_resolution": {
                "provider_repository_id": PROVIDER, "resolved_slug": NEW,
                "observed_at": "2026-08-20T00:00:00Z",
                "evidence": [{
                    "kind": "authenticated_provider_readback", "authenticated": True,
                    "provider_repository_id": PROVIDER, "resolved_slug": NEW,
                }],
            },
            "identity_updates": [], "evidence": [],
        }],
        "child_ownership": {},
    }


def comment(event, index):
    return {
        "id": projection_slot_id("GEN-37", PLAN, index, AUTHORITY),
        "body": encode_projection_comment(event),
        "createdAt": f"2026-08-28T03:{index:02d}:00.000Z",
        "updatedAt": f"2026-08-28T03:{index:02d}:00.000Z",
    }


class FakeClient:
    def __init__(self, comments):
        self.comments = comments
        self.writes = 0
        self.lose_seal_response = False
        self.fail_seal_before_write = False

    def execute(self, query, variables):
        if "CommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}, {"name": "body"}]}}
        if "query WorkstreamDeltaComments" in query:
            return {"issue": {
                "id": ROOT, "identifier": "GEN-37",
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                "comments": {"nodes": deepcopy(self.comments), "pageInfo": {
                    "hasNextPage": False, "endCursor": None,
                }},
            }}
        if "commentCreate" in query:
            value = variables["input"]
            if any(item["id"] == value["id"] for item in self.comments):
                raise LinearTransportError("duplicate")
            if (
                self.fail_seal_before_write
                and "workstream-projection:v1" in value["body"]
            ):
                self.fail_seal_before_write = False
                raise LinearTransportError("provider unavailable before seal write")
            self.writes += 1
            stored = {
                "id": value["id"], "body": value["body"],
                "createdAt": f"2026-08-29T12:00:{self.writes:02d}.000Z",
                "updatedAt": f"2026-08-29T12:00:{self.writes:02d}.000Z",
            }
            self.comments.append(stored)
            if self.lose_seal_response and "workstream-projection:v1" in value["body"]:
                self.lose_seal_response = False
                raise LinearTransportError("lost response")
            return {"commentCreate": {"success": True, "comment": deepcopy(stored)}}
        raise AssertionError(query)


class Resolver:
    def __init__(self, _token):
        pass

    def resolve_route(self, *, requested_slug, provider_repository_id, canonical_slug):
        if provider_repository_id != PROVIDER or canonical_slug != NEW:
            raise AssertionError("unexpected provider binding")
        return {
            "requested_slug": requested_slug, "resolved_slug": NEW,
            "provider_repository_id": PROVIDER,
            "requested_response_url": f"https://api.github.test/{requested_slug}",
            "canonical_response_url": "https://api.github.test/canonical",
            "redirect_count": 0 if requested_slug == NEW else 1,
            "authenticated": True,
        }


class IdentityHistorySealTests(unittest.TestCase):
    def fixture(self):
        initial = scope()
        backfilled = deepcopy(initial)
        repository = backfilled["repositories"][0]
        repository["aliases"] = [OLD]
        repository["identity_resolution"]["observed_at"] = "2026-08-28T03:35:30Z"
        repository["identity_updates"] = [{
            "from": OLD, "to": NEW, "repository_key": KEY,
            "provider_repository_id": PROVIDER,
            "observed_at": "2026-08-28T03:35:30Z",
            "evidence": [{
                "kind": "authenticated_provider_readback", "authenticated": True,
                "repository_key": KEY, "provider_repository_id": PROVIDER,
                "requested_slug": OLD, "resolved_slug": NEW,
            }],
        }]
        first = build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root", value=initial,
            plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-28T03:00:00Z", authority=AUTHORITY,
        )
        source = build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"identity": "plan:test", "sha256": PLAN},
            plan_revision=PLAN, expected_revision=1,
            created_at="2026-08-28T03:01:00Z", authority=AUTHORITY,
        )
        second = build_projection_event(
            workstream_id="GEN-37", kind="scope", key="root", value=backfilled,
            plan_revision=PLAN, expected_revision=2,
            created_at="2026-08-28T03:02:00Z", authority=AUTHORITY,
            supersedes_event_id=first["event_id"],
        )
        comments = [comment(first, 0), comment(source, 1), comment(second, 2)]
        candidate = {
            "sealed_scope_event_id": second["event_id"],
            "sealed_scope_value_sha256": hashlib.sha256(
                seal_module._canonical(backfilled)
            ).hexdigest(),
            "legacy_transitions": [{
                "predecessor_scope_event_id": first["event_id"],
                "predecessor_scope_value_sha256": hashlib.sha256(
                    seal_module._canonical(initial)
                ).hexdigest(),
                "transition_scope_event_id": second["event_id"],
                "transition_scope_value_sha256": hashlib.sha256(
                    seal_module._canonical(backfilled)
                ).hexdigest(),
            }],
            "sealed_projection_frontier_event_id": second["event_id"],
            "sealed_projection_frontier_event_sha256": hashlib.sha256(
                seal_module._canonical(second)
            ).hexdigest(),
            "legacy_projection_prefix_sha256": projection_prefix_sha256(
                [first, source, second], {
                    first["event_id"]: comments[0], source["event_id"]: comments[1],
                    second["event_id"]: comments[2],
                }, second["event_id"],
            ),
        }
        request = {
            "schema_version": 1, "workstream_id": "GEN-37",
            "authority": AUTHORITY, "plan_revision": PLAN,
            "plan_source": "plan:test", "observed_at": "2026-08-29T12:00:00Z",
            "expected_frontier": {
                "material_revision": 0, "projection_revision": 3, **candidate,
            },
        }
        return FakeClient(comments), request

    def patches(self, client):
        return (
            mock.patch.object(seal_module, "load_linear_api_key", return_value="linear"),
            mock.patch.object(seal_module, "HttpGraphQLClient", return_value=client),
            mock.patch.object(seal_module, "GitHubRepositoryResolver", Resolver),
            mock.patch.object(seal_module, "plan_payload", return_value={
                "source": {"identity": "plan:test", "sha256": PLAN},
            }),
        )

    def invoke(self, client, request, *, apply):
        patches = self.patches(client)
        with patches[0], patches[1], patches[2], patches[3]:
            return seal_module.run(request, apply=apply)

    def test_preview_is_zero_write_and_apply_is_idempotent(self):
        client, request = self.fixture()
        preview = self.invoke(client, request, apply=False)
        self.assertEqual(preview["disposition"], "preview")
        self.assertEqual(client.writes, 0)
        created = self.invoke(client, request, apply=True)
        self.assertEqual(created["disposition"], "created")
        self.assertEqual(client.writes, 2)  # durable reservation plus exact seal
        repeated = self.invoke(client, request, apply=True)
        self.assertEqual(repeated["disposition"], "existing")
        self.assertEqual(client.writes, 2)

    def test_lost_seal_response_converges_by_exact_readback(self):
        client, request = self.fixture()
        client.lose_seal_response = True
        result = self.invoke(client, request, apply=True)
        self.assertEqual(result["disposition"], "created")
        self.assertEqual(client.writes, 2)

    def test_provider_unavailable_crash_replays_durable_intent(self):
        client, request = self.fixture()
        client.fail_seal_before_write = True
        with self.assertRaises(LinearTransportError):
            self.invoke(client, request, apply=True)
        self.assertEqual(client.writes, 1)  # only the durable reservation

        class UnavailableResolver:
            def __init__(self, _token):
                raise AssertionError("provider must not be called during replay")

        with mock.patch.object(
            seal_module, "load_linear_api_key", return_value="linear",
        ), mock.patch.object(
            seal_module, "HttpGraphQLClient", return_value=client,
        ), mock.patch.object(
            seal_module, "GitHubRepositoryResolver", UnavailableResolver,
        ), mock.patch.object(
            seal_module, "plan_payload",
            side_effect=AssertionError("source must not be called during replay"),
        ):
            result = seal_module.run(request, apply=True)
        self.assertEqual(result["disposition"], "created")
        self.assertEqual(client.writes, 2)

    def test_crash_replay_refuses_if_sealed_prefix_receipt_changed(self):
        client, request = self.fixture()
        client.fail_seal_before_write = True
        with self.assertRaises(LinearTransportError):
            self.invoke(client, request, apply=True)
        client.comments[0]["updatedAt"] = "2026-08-29T13:00:00.000Z"
        before = client.writes
        with self.assertRaisesRegex(
            seal_module.IdentityHistorySealError, "candidate_mismatch",
        ):
            self.invoke(client, request, apply=True)
        self.assertEqual(client.writes, before)

    def test_wrong_provider_attestation_refuses_without_writes(self):
        client, request = self.fixture()

        class WrongResolver(Resolver):
            def resolve_route(self, **kwargs):
                value = super().resolve_route(**kwargs)
                value["provider_repository_id"] = "R_attacker"
                return value

        patches = self.patches(client)
        with patches[0], patches[1], mock.patch.object(
            seal_module, "GitHubRepositoryResolver", WrongResolver,
        ), patches[3]:
            with self.assertRaisesRegex(
                (seal_module.LinearProjectionError, seal_module.RepositoryIdentityError),
                "projection|provider|repository",
            ):
                seal_module.run(request, apply=True)
        self.assertEqual(client.writes, 0)

    def test_arbitrary_id_forged_reservation_never_bypasses_provider(self):
        client, request = self.fixture()
        event = self.invoke(client, request, apply=False)["event"]
        reservation = {
            "schema_version": 1, "workstream_id": "GEN-37",
            "material_revision": 0, "plan_revision": PLAN,
            "projection_revision": 3,
            "projection_frontier_ids": [item["id"] for item in client.comments],
            "frontier_ids": [], "authority": AUTHORITY,
            "intent_kind": "repository_identity_history_seal",
            "intent_event": event,
            "intent_sha256": seal_module._value_digest(event),
        }
        client.comments.append({
            "id": "arbitrary-attacker-id",
            "body": encode_ledger_reservation(reservation),
            "createdAt": "2026-08-29T12:00:00.000Z",
            "updatedAt": "2026-08-29T12:00:00.000Z",
        })

        class ProviderMustBeConsulted:
            def __init__(self, _token):
                raise seal_module.RepositoryIdentityError("provider_reauthentication_required")

        with mock.patch.object(
            seal_module, "load_linear_api_key", return_value="linear",
        ), mock.patch.object(
            seal_module, "HttpGraphQLClient", return_value=client,
        ), mock.patch.object(
            seal_module, "GitHubRepositoryResolver", ProviderMustBeConsulted,
        ), mock.patch.object(seal_module, "plan_payload", return_value={
            "source": {"identity": "plan:test", "sha256": PLAN},
        }):
            with self.assertRaisesRegex(
                seal_module.RepositoryIdentityError,
                "provider_reauthentication_required",
            ):
                seal_module.run(request, apply=True)
        self.assertEqual(client.writes, 0)

    def test_claimed_slot_with_forged_collision_frontier_is_not_replay_authority(self):
        client, request = self.fixture()
        event = self.invoke(client, request, apply=False)["event"]
        forged_frontier = ["collision:" + "0" * 64]
        reservation = {
            "schema_version": 1, "workstream_id": "GEN-37",
            "material_revision": 0, "plan_revision": PLAN,
            "projection_revision": 3,
            "projection_frontier_ids": [item["id"] for item in client.comments],
            "frontier_ids": forged_frontier, "authority": AUTHORITY,
            "intent_kind": "repository_identity_history_seal",
            "intent_event": event,
            "intent_sha256": seal_module._value_digest(event),
        }
        client.comments.append({
            "id": ledger_boundary_slot_id(
                "GEN-37", 0, forged_frontier, AUTHORITY,
            ),
            "body": encode_ledger_reservation(reservation),
            "createdAt": "2026-08-29T12:00:00.000Z",
            "updatedAt": "2026-08-29T12:00:00.000Z",
        })
        before = client.writes
        with self.assertRaisesRegex(
            seal_module.RepositoryIdentityError,
            "reservation_frontier_unproven",
        ):
            self.invoke(client, request, apply=True)
        self.assertEqual(client.writes, before)

    def test_wrong_root_plan_and_stale_prefix_refuse_without_writes(self):
        for mutation in ("root", "plan", "prefix"):
            with self.subTest(mutation=mutation):
                client, request = self.fixture()
                if mutation == "root":
                    request["authority"]["root_issue_id"] = (
                        "44444444-4444-4444-8444-444444444444"
                    )
                elif mutation == "plan":
                    request["plan_revision"] = "e" * 64
                else:
                    request["expected_frontier"][
                        "legacy_projection_prefix_sha256"
                    ] = "0" * 64
                with self.assertRaises(Exception):
                    self.invoke(client, request, apply=True)
                self.assertEqual(client.writes, 0)


if __name__ == "__main__":
    unittest.main()
