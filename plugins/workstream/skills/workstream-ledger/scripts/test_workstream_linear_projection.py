#!/usr/bin/env python3
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest import mock
import zlib

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
    LinearProjectionError, projection_slot_id, reduce_projection_comments, TOMBSTONE,
)
from workstream_resume import add_material_history, compact_context, ResumeError
from workstream_relation_readback import RelationReadbackError
import workstream_projection
import workstream_linear_projection as projection_module
from workstream_projection import (
    load_material_history_for_projection_reconcile, projection_review_contract,
    prepare_terminal_child_repairs, reconcile_required_projection,
    stable_live_readback,
)
from workstream_successor import choose_disposition
from workstream_child_closure import canonical_digest, terminal_child_readback


PLAN = "f38baae4441485b14e5b16ea0255e3a07e42aa94a4fb0e6e04e7aa513693719d"
HEAD = "a" * 40
ROOT_UUID = "33333333-3333-4333-8333-333333333333"
TARGET_UUID = "22222222-2222-4222-8222-222222222222"
CHANGED_TARGET_UUID = "44444444-4444-4444-8444-444444444444"

# Literal read-only capture of GEN-37's 0.4.0 activation comment. The compressed
# predecessor JSON is the exact ordered event array named by that marker; it is
# deliberately not constructed with the current activation writer.
GEN37_040_ACTIVATION_COMMENT_BODY = "<!-- workstream-projection:v1:eyJldmVudCI6eyJhdXRob3JpdHkiOnsicHJvamVjdF9pZCI6ImVlYTI1MjJiLTE4N2QtNGY1Yi1hZjI3LWZjODMzZDRmZDFjYiIsInJvb3RfaXNzdWVfaWQiOiI0MDljMTQyMy1mOTQ5LTQ2NTUtOWY1Zi1kMzIxM2Q3YjQzNGYiLCJ0ZWFtX2lkIjoiZDU5YzU1MDktOGQ5Ni00MDkzLWI3ZDUtMDQzN2NlZDVjNjc5Iiwid29ya3NwYWNlX2lkIjoiZDgzMGI1YWUtNTYxNi00OTJkLWE3MWQtMzMxMzM4N2U4YjZmIn0sImNyZWF0ZWRfYXQiOiIyMDI2LTA4LTI4VDE4OjEzOjI1LjA0MjAyNFoiLCJldmVudF9pZCI6IndzcF80MDFmODQ0ODUyNGNmODkwNTU3YzlhMmE4NGJiNjc0YSIsImV4cGVjdGVkX3JldmlzaW9uIjoxNSwia2V5Ijoicm9vdCIsImtpbmQiOiJjYXNfYWN0aXZhdGlvbiIsInBsYW5fcmV2aXNpb24iOiJmMzhiYWFlNDQ0MTQ4NWIxNGU1YjE2ZWEwMjU1ZTNhMDdlNDJhYTk0YTRmYjBlNmUwNGU3YWE1MTM2OTM3MTlkIiwic2NoZW1hX3ZlcnNpb24iOjIsInN1cGVyc2VkZXNfZXZlbnRfaWQiOm51bGwsInZhbHVlIjp7ImxlZ2FjeV9ldmVudF9pZHMiOlsid3NwX2JhMGU5ZDk3NTZmYjU2ZTQ0ODM4YThhNDc3YjM2NjUxIiwid3NwXzRjMWNlNjNjODZkOTk5MzJiYjg5NDI5MjQ4OTU2YjUyIiwid3NwXzM2MDE1MjFjOGE1MGFkOGRhMzNlYjU2YWUxZmEzMzllIiwid3NwXzhjZDgyODRhZTViMmU5MTVkZmRlMjYxMTAwNmJiMDIzIiwid3NwXzNkMjQ3NDM5OTE1MDI3YjJjNGNkNmI0NzVlZDY5YWUxIiwid3NwXzhhOWFiZWViYmYzMTg5ZGQ5Yzg3OWZmYTYwZDRhYzZiIiwid3NwX2ZiMWNlYmFiNjZjNTQ0NjBlYTdjZGUxODAyNjkxZTA3Iiwid3NwX2JlYzI2ZDM1MDhjZDUxYWExOTIzNWRhZTAwZjhhMDc5Iiwid3NwXzdiMTc5OWYwZGJiODA5NTk2ZGE1OTkyNTMyZmJlNzQ1Iiwid3NwX2M2ZGVjYzk5YWYzZDRjYmNkZmUwYWFjMGUwM2ZhMDIxIiwid3NwXzA3N2NmNzhlZTk1MzMxZDk3NzRjOGRjOWNkMWE0OWY0Iiwid3NwXzI4YzUzNmMxZDdlMzA0NDE0MGVlMzUxZmY1ODA2YWJkIiwid3NwX2ZmMWEzZTU4OThlNmM5Mzg3YzI3ZjI3MDUzNTk2OTVhIiwid3NwX2I2YTRkYzBlMjFhZTY1ZTJjMDMwYTU4ZDlkYTRmZTE2Iiwid3NwXzhhMWViMjY0YzU4NTVhYTVkMDRjYjdkY2NjOWVhNzA3Il0sImxlZ2FjeV9ldmVudHNfc2hhMjU2IjoiYmVlYTZhYmFmZTAwOTM5NmEwYzhmYzgzMWU4Mzk4MTY1N2IxYTBhZTJkYmU1OTgwNTBiNDdmYmI0NmZjMjUwZSJ9LCJ3b3Jrc3RyZWFtX2lkIjoiR0VOLTM3In0sInNoYTI1NiI6IjFiYTcyOTgxMGE5NTAwZTJkYjdlNjE4NzgwNTRlN2JkYzkyMDQyODA4ZTM0MDUzNjc0MDQ0NTMwMmU4NWExMDkifQ -->"
GEN37_040_PREDECESSORS_ZLIB_B64 = "eNrtmktvGzkSx7+LzmbM90PnXeS0u8BgLjuDQCgWi1avJbXQ3bJjBPnuWy3Fj3ikRHacQN71RZBa7CJZv3+RVd3889MEO4KBygyGyXSipfZCRqHj7zJMTZxq/05Kp736Y3I2oStaDbOmcMPrfj3LICmVFJyv2XmyNpoIEWwI2Xjv1HjHxzXhaL2jq6Zv2tVkKs8ml3TDJrq2HbjJZbMaDfbYrol/rhewetB6Uk3MAGzcKhtdVpb4wxNI7RwZkIGsBkgWbM2SPElLAcAp45MJKhU22eOcljC7om5nU/GlzZp/UaF+dj+p1WaxOJtcwWJDkyk7Zt4syqy9XnHLebMeL73/+z+FiTyqi2aYb/I7bJfTpkx/m11e/O1fv8/CjC64v22rdEwrK49qpY5qpQ+0+i3Nu/fXt63Mt1t9PpssmhVBN0533bX/YXw74kSgndZZqBiKsNVlAVUHUTEaU2wtCjN3MlKdNX2/od1tViZUVhtRk03CeudEqq6KYrQyJWRrbN3ethloRNTUBmHYcvrEgmsKrZBx/PlpApthzqjGv4ktD92G7tTz1X+z3QxYRVAy4OWoql83k4FgubuhuITOySRiSZ5ZJyNyKE5IawJScehD4huu2+6yXwN+6aZEI7MDEs4rvivpIiCoIoxRxsRAMfs6+fzhbNLmnrqrA5Frp1r+8b848V/XzwqWtL1t1NcFq0tsDQ0sq6XAltW22jTDzdbJzRK6Gxbcuu2boe1uvhewdy0b6nfiXjTQb79/OHso+/HXR2CAcxbzOHjwykHwYCL5XBVok7Q2UmOuqUQvqaZANC57o42BB8jD6tvF5gdjioU03vg4qm6v3U585+K/zJYHMCq1X2wuvnLNOfuVOPZ7dmi3Pn/s5ifp/GcP5YFHN+vCTvlC69iun9Hj8bqgxPJ3NUqQ3libXCxQqsJCADmpYEqWzuVwCrq425C+AaPAqqFFB7XS4nzcfm+gKy8mhx8YwTNVcNfj8R0dD1+XlIzWsWKOSucEKaVaGbcLKheXIy/UaDlxOQX44d/vh+unROJ6s1i/HPlndv9c7LfdHTnJz192q+0CsDO0zSbDVg/PT9UtKiRvMHqWCmsl55isTtrG5Hx2en+qrg6k6u2mw9PK1W/h8CDmw7Dup+fnhyJs9LQYR75qVhfnedHmc/RKB580Su+ksk5WCBRskPzLYFABvC8l4fmdz+X+XGD8yrJqRoGUbSfvlttZzTnx8i/hoZ8lEOOlcrxKRHASCm8exhAXdUCq8tdE+wWibwUiFUhVrBcGLIhQohRRKhQqG16TtI+I5V5AY9DQClYnJqLtBswjwLbQRzazBC4BV2P69w83WqV+NLfz2ZETHlkxqq35p2dw/cA0x4CDSpPPPwt9xBJ1tMB+1pSUK7UQt1RS+pylNvvRm7u1gRZjx9NxLMreQ95eH5ueEuIBugsa7leM2lD3xY/bsT+semJgSlxXJJczV9dFiZjJCUWO6yCZq7T+B4qYmzXdO+/nhXXRNliTGKvUIWu0WHy2wVHxiYN7P1u7j60Or5ftduwP2HpSPpORojArrmgzs7UQGLXS3vIYo6WTZxshQSbKuRoV07g/xcBpH3hZLKDP+9m6vXH7itmqR2xlImcqWCFt1EwpGMHckrDGx2qyVBzEJ8+2Zs7XMmTv0VnLewIELuVUZBtJkQz72fq9bPUrZqu/ZgsmJ1LSCyRAYSEGkQkYMDI9bbFmU06ebSbUvhgneeN1CkAlbVwBkrJG9nPazzbsY/uKw/ZR1FrvE2oVRGVWzKg4kZ01nE9JdBE8aDInTzZkFbjyloXLK5lc8gVcStoZXTPXE24/2bg3auUrjlr5NVtONTQgSCGrt8Jy9iuAMgqusGw1EEC5evJs0RdCTAmqKRYzlkoSACVJ3mykPpBJpX1szStekc2jFdkzJq1DEOA1r8jkx3w5RYZmonc+uuLVybOVIWANkSg5HkFJIViMBRMWBTZVe+DpiNwH177iEsg+KoGyHyvBXEQ2geFarXm7TUm4wvkIxRpMOf1USkd0xqMqgRP+0d2SyDhVq4vSQy4H4B549lWafvuc79SoPhwX50nDADjfPuXElk3x1GrXLmdsGy/XbTM+5tgZ6GjZDvTkt0vPhmX1VEuGZVx0+/Je7tOQi4n7x8RqQR2qDtIZ3k2TgwOw9MmeKTj2oMTbqYO3Uwdvpw7eTh2c5qkD7ZO3piqMvEAXVGhSBsfbp7Qy5sx5PGmjUng7dfB26uDt1MHbqYO3Uwf/H6cOvpPNZw+2oCStgLwjjdJI4DUiFc6sSfkD2bx53W+Vj32f/ovfOz9hD3+h987fUUcERVl7i/y3Y18XaZETKURMBOHQOw5lT7wwP/ahwwuX7kfj/QbOD/8Fek0stw=="
GEN37_040_PREDECESSORS_SHA256 = "a15eb5261aa9da3a1afd81cf88e053453ca29758c616485aa7174eebe1165da2"
AUTHORITY = {
    "workspace_id": "workspace", "team_id": "team", "project_id": "project",
    "root_issue_id": ROOT_UUID,
}


def projection_comment(event):
    return {
        "id": projection_slot_id(
            event["workstream_id"], event["plan_revision"], event["expected_revision"],
            event["authority"],
        ),
        "body": encode_projection_comment(event),
    }


def legacy_event(kind, key, value, revision, created_at):
    event = build_projection_event(
        workstream_id="GEN-37", kind=kind, key=key, value=value,
        plan_revision=PLAN, expected_revision=revision, created_at=created_at,
        authority=AUTHORITY,
    )
    event["schema_version"] = 1
    event.pop("authority")
    event["event_id"] = projection_module._event_id(event)
    return event


def legacy_comment(event, remote_id):
    return {"id": remote_id, "body": encode_projection_comment(event)}


def gen37_040_activation_fixture():
    predecessor_bytes = zlib.decompress(base64.b64decode(
        GEN37_040_PREDECESSORS_ZLIB_B64
    ))
    assert hashlib.sha256(predecessor_bytes).hexdigest() == (
        GEN37_040_PREDECESSORS_SHA256
    )
    predecessors = json.loads(predecessor_bytes)
    return [
        *[
            legacy_comment(event, f"captured-gen37-predecessor-{index}")
            for index, event in enumerate(predecessors)
        ],
        {
            "id": "91886409-e4dd-4777-a462-20f9ebb20513",
            "body": GEN37_040_ACTIVATION_COMMENT_BODY,
        },
    ]


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
        if "WorkstreamProjectionCommentCreateCapability" in query:
            return {"__type": {"inputFields": [{"name": "id"}, {"name": "body"}]}}
        if "query WorkstreamDeltaComments" in query:
            return {"issue": {
                "id": ROOT_UUID, "identifier": "GEN-37",
                "team": {"id": "team", "organization": {"id": "workspace"}},
                "project": {"id": "project"},
                "comments": {"nodes": [dict(item) for item in self.comments],
                             "pageInfo": {"hasNextPage": False, "endCursor": None}},
            }}
        if "commentCreate" in query:
            comment_id = variables["input"].get("id", f"comment-{len(self.comments) + 1}")
            if any(item["id"] == comment_id for item in self.comments):
                from workstream_linear import LinearTransportError
                raise LinearTransportError("duplicate comment id")
            comment = {"id": comment_id,
                       "body": variables["input"]["body"],
                       "createdAt": "now", "updatedAt": "now"}
            self.comments.append(comment)
            return {"commentCreate": {"success": True, "comment": dict(comment)}}
        raise AssertionError("unexpected GraphQL operation")


class RacingProjectionClient(FakeProjectionClient):
    def __init__(self, winner):
        super().__init__()
        self.winner = winner
        self.injected = False

    def execute(self, query, variables):
        if "commentCreate" in query and not self.injected:
            self.injected = True
            self.comments.append({
                "id": variables["input"]["id"],
                "body": encode_projection_comment(self.winner),
                "createdAt": "now", "updatedAt": "now",
            })
            from workstream_linear import LinearTransportError
            raise LinearTransportError("duplicate comment id")
        return super().execute(query, variables)


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
    @staticmethod
    def relation_target_resolver(relations):
        root = {
            "workspace_id": "workspace", "issue_id": ROOT_UUID,
            "identifier": "GEN-37",
        }
        resolved = {}
        for relation in relations:
            target = relation["target"]
            inverse = (
                "blocked_by" if relation["type"] == "blocks"
                else "blocks" if relation["type"] == "blocked_by"
                else None
            )
            resolved[f"{target['workspace_id']}:{target['issue_id']}"] = {
                **target,
                "relations": ([{"type": inverse, "target": root}] if inverse else []),
            }
        return resolved

    @staticmethod
    def incomplete_relation_target_resolver(relations):
        target = relations[0]["target"]
        raise RelationReadbackError(
            f"relation_target_readback_incomplete:{target['identifier']}"
        )

    @staticmethod
    def graph_snapshot():
        return {
            "root": {
                "id": ROOT_UUID, "identifier": "GEN-37",
                "url": "https://linear.app/acme/issue/GEN-37/root",
                "plan_revision": PLAN, "revision": 0,
                "status": "In Progress", "next_action": "Reconcile.",
            },
            "children": [],
        }

    def legacy_relation_fixture(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, workspace_id="workspace", team_id="team",
            project_id="project", root_issue_id=ROOT_UUID,
        )
        relation = {"type": "blocks", "target": {
            "workspace_id": "workspace", "issue_id": TARGET_UUID,
            "identifier": "GEN-14",
        }}
        base = [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": {
                "sha256": PLAN, "identity": "https://example.test/plan",
            }},
            {"kind": "provenance", "key": "old", "value": {
                "agent": "codex", "machine": "M5", "session_id": "old",
                "worktree": {"state": "safe", "head": HEAD},
            }},
            {"kind": "relation", "key": "blocks:GEN-14", "value": relation},
        ]
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, base), remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
        )
        return client, adapter, base, source

    def terminal_repair_fixture(self, *, contract_mutator=None):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        contract = evidence_contract()
        contract["slice_id"] = "gen72-terminal"
        contract["owning_child"] = "GEN-72"
        if contract_mutator is not None:
            contract_mutator(contract)
        base = [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": source},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
                "worktree": {"state": "safe", "head": HEAD},
            }},
            {"kind": "evidence_contract", "key": contract["slice_id"],
             "value": contract},
        ]
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, base), remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
        )
        child = {
            "id": "77777777-7777-4777-8777-777777777777",
            "identifier": "GEN-72", "title": "terminal child",
            "parent": {"id": ROOT_UUID, "identifier": "GEN-37"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project"},
            "assignee": {"id": "88888888-8888-4888-8888-888888888888"},
            "state_id": "99999999-9999-4999-8999-999999999999",
            "status": "Done", "status_type": "completed",
        }
        graph = self.graph_snapshot()
        graph["children"] = [
            {"identifier": "GEN-38", "title": "existing",
             "status": "In Progress", "next_action": "continue"},
            child,
        ]
        evidence_event = next(
            event for event in adapter.state().events
            if event["kind"] == "evidence_contract"
        )
        repair = {
            "child_identifier": "GEN-72",
            "child_issue_id": child["id"],
            "expected_child_readback_sha256": canonical_digest(
                terminal_child_readback(child)
            ),
            "expected_assignee_id": child["assignee"]["id"],
            "approved_evidence_heads": [{
                "key": evidence_event["key"],
                "event_id": evidence_event["event_id"],
                "value_sha256": canonical_digest(evidence_event["value"]),
            }],
        }
        manifest = {
            **reviewed_manifest(adapter, deepcopy(base)),
            "terminal_child_repairs": [repair],
        }
        return client, adapter, source, graph, child, manifest

    def test_legacy_unresolved_relation_cannot_bypass_reviewed_migration(self):
        client, adapter, base, source = self.legacy_relation_fixture()
        manifest = reviewed_manifest(adapter, base)
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError, "legacy_unresolved_relation_migration_required",
        ):
            load_material_history_for_projection_reconcile(
                self.graph_snapshot(), client.comments, "GEN-37", manifest, adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                relation_target_resolver=self.incomplete_relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_legacy_unresolved_relation_migration_refuses_stale_review_zero_writes(self):
        client, adapter, base, source = self.legacy_relation_fixture()
        retirement = reviewed_retirement(adapter, "relation", "blocks:GEN-14")
        manifest = reviewed_manifest(adapter, base[:-1], [retirement])
        late = build_projection_event(
            workstream_id="GEN-37", kind="provenance", key="late",
            value={"agent": "codex", "machine": "M3", "session_id": "late"},
            plan_revision=PLAN, expected_revision=adapter.state().revision,
            created_at="2026-08-27T18:30:00Z", authority=AUTHORITY,
        )
        adapter.append(late)
        writes_before = len(client.comments)
        with self.assertRaisesRegex(LinearProjectionError, "stale_reload_required"):
            load_material_history_for_projection_reconcile(
                self.graph_snapshot(), client.comments, "GEN-37", manifest, adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                relation_target_resolver=self.incomplete_relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_invalid_legacy_relation_replacement_has_zero_partial_writes(self):
        client, adapter, base, source = self.legacy_relation_fixture()
        replacement = deepcopy(base[-1])
        replacement["value"]["target"] = {
            **replacement["value"]["target"], "issue_id": CHANGED_TARGET_UUID,
        }
        manifest = reviewed_manifest(adapter, [*base[:-1], replacement])
        snapshot, unresolved = load_material_history_for_projection_reconcile(
            self.graph_snapshot(), client.comments, "GEN-37", manifest, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            relation_target_resolver=self.incomplete_relation_target_resolver,
        )
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            RelationReadbackError, "relation_target_readback_incomplete",
        ):
            reconcile_required_projection(
                adapter, snapshot, manifest, remote_head=HEAD,
                created_at="2026-08-27T19:00:00Z", authenticated_source=source,
                relation_target_resolver=self.incomplete_relation_target_resolver,
                legacy_unresolved_relation_heads=unresolved,
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_reviewed_scope_and_source_repair_previews_strict_final_state(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        current_scope = scope()
        current = [
            {"kind": "scope", "key": "root", "value": current_scope},
            {"kind": "source", "key": "root", "value": {
                "identity": "https://github.com/acme/plans/blob/main/PLAN.md",
                "sha256": PLAN,
            }},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
                "worktree": {"state": "safe", "head": HEAD},
            }},
        ]
        current_source = current[1]["value"]
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, current), remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z",
            authenticated_source=current_source,
        )
        exact_source = {
            "identity": "https://github.com/acme/plans/blob/" + "a" * 40 + "/PLAN.md",
            "sha256": PLAN,
        }
        desired_scope = deepcopy(current_scope)
        desired_scope["child_ownership"]["GEN-72"] = (
            "github.com:id:R_agent_workstream"
        )
        desired = [
            {"kind": "scope", "key": "root", "value": desired_scope},
            {"kind": "source", "key": "root", "value": exact_source},
            current[2],
        ]
        graph = {
            "root": {
                "id": ROOT_UUID, "identifier": "GEN-37",
                "url": "https://linear.app/acme/issue/GEN-37/root",
                "plan_revision": PLAN, "revision": 0,
                "status": "In Progress", "next_action": "Reconcile.",
            },
            "children": [
                {"identifier": "GEN-38", "title": "existing",
                 "status": "In Progress", "next_action": "continue"},
                {"identifier": "GEN-72", "title": "new child",
                 "status": "In Progress", "next_action": "implement"},
            ],
        }
        manifest = reviewed_manifest(adapter, desired)
        writes_before = len(client.comments)

        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", manifest, adapter,
            authenticated_route=AUTHORITY, authenticated_source=exact_source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )

        self.assertEqual(unresolved, frozenset())
        self.assertEqual(len(client.comments), writes_before)
        self.assertEqual(preview["scope"], desired_scope)
        self.assertEqual(preview["source"], exact_source)
        result = reconcile_required_projection(
            adapter, preview, manifest, remote_head=HEAD,
            created_at="2026-08-27T19:00:00Z",
            authenticated_source=exact_source,
        )
        self.assertTrue(result["readback_verified"])
        strict = add_material_history(
            graph, client.comments, "GEN-37",
            authenticated_route=AUTHORITY, authenticated_source=exact_source,
        )
        self.assertEqual(strict["scope"], desired_scope)
        self.assertEqual(strict["source"], exact_source)

    def test_source_repair_that_leaves_scope_invalid_has_zero_writes(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        current = [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": {
                "identity": "https://example.test/plan", "sha256": PLAN,
            }},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
                "worktree": {"state": "safe", "head": HEAD},
            }},
        ]
        current[1]["value"] = {
            "identity": "https://github.com/acme/plans/blob/main/PLAN.md",
            "sha256": PLAN,
        }
        source = current[1]["value"]
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, current), remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
        )
        desired = deepcopy(current)
        desired[1]["value"] = {
            "identity": "https://github.com/acme/plans/blob/" + "a" * 40 + "/PLAN.md",
            "sha256": PLAN,
        }
        graph = {
            "root": {
                "id": ROOT_UUID, "identifier": "GEN-37",
                "url": "https://linear.app/acme/issue/GEN-37/root",
                "plan_revision": PLAN, "revision": 0,
                "status": "In Progress", "next_action": "Reconcile.",
            },
            "children": [
                {"identifier": "GEN-38", "title": "existing",
                 "status": "In Progress", "next_action": "continue"},
                {"identifier": "GEN-72", "title": "new child", "status": "Done"},
            ],
        }
        writes_before = len(client.comments)
        with self.assertRaisesRegex(ResumeError, "unowned_children:GEN-72"):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37",
                reviewed_manifest(adapter, desired), adapter,
                authenticated_route=AUTHORITY,
                authenticated_source=desired[1]["value"],
                remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_reviewed_scope_repair_over_resume_budget_has_zero_writes(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        current_scope = scope()
        source = {
            "identity": "https://example.test/plan", "sha256": PLAN,
        }
        current = [
            {"kind": "scope", "key": "root", "value": current_scope},
            {"kind": "source", "key": "root", "value": source},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
                "worktree": {"state": "safe", "head": HEAD},
            }},
        ]
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, current), remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
        )
        desired_scope = deepcopy(current_scope)
        desired_scope["child_ownership"]["GEN-72"] = (
            "github.com:id:R_agent_workstream"
        )
        desired = deepcopy(current)
        desired[0]["value"] = desired_scope
        graph = self.graph_snapshot()
        graph["children"] = [
            {"identifier": "GEN-38", "title": "existing",
             "status": "In Progress", "next_action": "continue"},
            {"identifier": "GEN-72", "title": "active", "status": "In Progress",
             "next_action": "continue"},
        ]
        writes_before = len(client.comments)

        with self.assertRaisesRegex(ResumeError, "resume_context_over_budget"):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37",
                reviewed_manifest(adapter, desired), adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                remote_head=HEAD, max_bytes=1,
                relation_target_resolver=self.relation_target_resolver,
            )

        self.assertEqual(len(client.comments), writes_before)
        with self.assertRaisesRegex(
            ResumeError, "resume_context_over_item_budget",
        ):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37",
                reviewed_manifest(adapter, desired), adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                remote_head=HEAD, max_bytes=1_000_000, max_items=0,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_scope_source_repair_migrates_unreadable_relation_first(self):
        client, adapter, base, _source = self.legacy_relation_fixture()
        retirement = reviewed_retirement(adapter, "relation", "blocks:GEN-14")
        exact_source = {
            "identity": "https://github.com/acme/plans/blob/" + "a" * 40 + "/PLAN.md",
            "sha256": PLAN,
        }
        desired = deepcopy(base[:-1])
        desired[1]["value"] = exact_source
        manifest = reviewed_manifest(adapter, desired, [retirement])
        graph = self.graph_snapshot()
        graph["children"] = [
            {"identifier": "GEN-38", "title": "existing",
             "status": "In Progress", "next_action": "continue"},
        ]
        snapshot, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", manifest, adapter,
            authenticated_route=AUTHORITY, authenticated_source=exact_source,
            remote_head=HEAD,
            relation_target_resolver=self.incomplete_relation_target_resolver,
        )
        revision_before = adapter.state().revision

        result = reconcile_required_projection(
            adapter, snapshot, manifest, remote_head=HEAD,
            created_at="2026-08-27T19:00:00Z",
            authenticated_source=exact_source,
            relation_target_resolver=self.relation_target_resolver,
            legacy_unresolved_relation_heads=unresolved,
        )

        appended = adapter.state().events[revision_before:]
        self.assertEqual(
            (appended[0]["kind"], appended[0]["key"], appended[0]["value"]),
            ("relation", "blocks:GEN-14", TOMBSTONE),
        )
        self.assertEqual(adapter.state().snapshot["source"], exact_source)
        self.assertTrue(result["readback_verified"])

    def test_terminal_child_repair_is_evidence_derived_full_and_idempotent(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        contract = evidence_contract()
        contract["slice_id"] = "gen72-terminal"
        contract["owning_child"] = "GEN-72"
        base = [
            {"kind": "scope", "key": "root", "value": scope()},
            {"kind": "source", "key": "root", "value": source},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
                "worktree": {"state": "safe", "head": HEAD},
            }},
            {"kind": "evidence_contract", "key": contract["slice_id"],
             "value": contract},
        ]
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, base), remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
        )
        child = {
            "id": "77777777-7777-4777-8777-777777777777",
            "identifier": "GEN-72", "title": "terminal child",
            "parent": {"id": ROOT_UUID, "identifier": "GEN-37"},
            "team": {"id": "team", "organization": {"id": "workspace"}},
            "project": {"id": "project"},
            "assignee": {"id": "88888888-8888-4888-8888-888888888888"},
            "state_id": "99999999-9999-4999-8999-999999999999",
            "status": "Done", "status_type": "completed",
        }
        graph = self.graph_snapshot()
        graph["children"] = [
            {"identifier": "GEN-38", "title": "existing",
             "status": "In Progress", "next_action": "continue"},
            child,
        ]
        evidence_event = next(
            event for event in adapter.state().events
            if event["kind"] == "evidence_contract"
        )
        repair = {
            "child_identifier": "GEN-72",
            "child_issue_id": child["id"],
            "expected_child_readback_sha256": canonical_digest(
                terminal_child_readback(child)
            ),
            "expected_assignee_id": child["assignee"]["id"],
            "approved_evidence_heads": [{
                "key": evidence_event["key"],
                "event_id": evidence_event["event_id"],
                "value_sha256": canonical_digest(evidence_event["value"]),
            }],
        }
        stale_manifest = {
            **reviewed_manifest(adapter, deepcopy(base)),
            "terminal_child_repairs": [repair],
        }
        prepared = prepare_terminal_child_repairs(
            stale_manifest, graph, adapter.state(),
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        before_revision = adapter.state().revision
        result = reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-27T19:00:00Z", authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda _identifier: repair[
                "expected_child_readback_sha256"
            ],
            legacy_unresolved_relation_heads=unresolved,
        )
        appended = adapter.state().events[before_revision:]
        self.assertEqual(
            [(event["kind"], event["key"]) for event in appended[:2]],
            [("child_closure", "GEN-72"), ("scope", "root")],
        )
        strict = add_material_history(
            graph, client.comments, "GEN-37",
            authenticated_route=AUTHORITY, authenticated_source=source,
        )
        context = compact_context(
            strict, "GEN-37", require_projection_authority=True,
        )
        self.assertEqual(context["resume_authority"], "full")
        self.assertEqual(context["scope"]["child_ownership"]["GEN-72"],
                         "github.com:id:R_agent_workstream")
        self.assertEqual(len(context["child_closures"]), 1)
        self.assertEqual(context["source"], source)
        self.assertEqual(context["disposition"]["remote_head"], HEAD)
        self.assertTrue(result["readback_verified"])

        replay = prepare_terminal_child_repairs(
            stale_manifest, graph, adapter.state(),
        )
        replay_preview, replay_unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", replay, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        comments_before_replay = len(client.comments)
        replay_result = reconcile_required_projection(
            adapter, replay_preview, replay, remote_head=HEAD,
            created_at="2026-08-27T20:00:00Z", authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda _identifier: repair[
                "expected_child_readback_sha256"
            ],
            legacy_unresolved_relation_heads=replay_unresolved,
        )
        self.assertEqual(replay_result["writes"], [])
        self.assertEqual(len(client.comments), comments_before_replay)
        self.assertFalse(any(
            "issueCreate" in query or "issueUpdate" in query
            for query, _variables in client.calls
        ))

    def test_generic_scope_repair_cannot_bypass_terminal_child_protocol(self):
        client, adapter, source, graph, _child, manifest = (
            self.terminal_repair_fixture()
        )
        manifest.pop("terminal_child_repairs")
        scope_item = next(
            item for item in manifest["projection"] if item["kind"] == "scope"
        )
        scope_item["value"]["child_ownership"]["GEN-72"] = (
            "github.com:id:R_agent_workstream"
        )
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_ownership_repair_required:GEN-72",
        ):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", manifest, adapter,
                authenticated_route=AUTHORITY, authenticated_source=source,
                remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_terminal_child_repair_contradictions_refuse_without_writes(self):
        cases = {
            "terminal_child_readback_missing:assignee_id": lambda graph, manifest: (
                graph["children"][1].__setitem__("assignee", None)
            ),
            "terminal_child_not_completed": lambda graph, manifest: (
                graph["children"][1].__setitem__("status_type", "canceled")
            ),
            "terminal_child_repair_route_mismatch:project_id": lambda graph, manifest: (
                graph["children"][1].__setitem__("project", {"id": "other-project"}),
                manifest["terminal_child_repairs"][0].__setitem__(
                    "expected_child_readback_sha256",
                    canonical_digest(terminal_child_readback(graph["children"][1])),
                ),
            ),
            "terminal_child_readback_changed_reload_required": lambda graph, manifest: (
                manifest["terminal_child_repairs"][0].__setitem__(
                    "expected_assignee_id", "other-assignee"
                )
            ),
            "terminal_child_repair_evidence_set_changed_reload_required": lambda graph, manifest: (
                manifest["terminal_child_repairs"][0]["approved_evidence_heads"][0].__setitem__(
                    "value_sha256", "0" * 64
                )
            ),
        }
        for expected, mutate in cases.items():
            with self.subTest(expected=expected):
                client, adapter, _source, graph, _child, manifest = (
                    self.terminal_repair_fixture()
                )
                writes_before = len(client.comments)
                mutate(graph, manifest)
                with self.assertRaisesRegex(
                    LinearProjectionError, re.escape(expected),
                ):
                    prepare_terminal_child_repairs(
                        manifest, graph, adapter.state(),
                    )
                self.assertEqual(len(client.comments), writes_before)
                self.assertFalse(any(
                    "issueCreate" in query or "issueUpdate" in query
                    for query, _variables in client.calls
                ))

        def remove_receipts(contract):
            contract["layers"]["logic"]["receipts"] = []

        client, adapter, _source, graph, _child, manifest = (
            self.terminal_repair_fixture(contract_mutator=remove_receipts)
        )
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError, "terminal_child_repair_evidence_invalid",
        ):
            prepare_terminal_child_repairs(manifest, graph, adapter.state())
        self.assertEqual(len(client.comments), writes_before)

        client, adapter, _source, graph, _child, manifest = (
            self.terminal_repair_fixture()
        )
        second_contract = deepcopy(evidence_contract())
        second_contract["slice_id"] = "gen72-other-owner"
        second_contract["owning_child"] = "GEN-72"
        second_contract["repository"] = "github.com/acme/other"
        second_contract["repository_key"] = "github.com:id:R_other"
        second_contract["exact_head"] = "b" * 40
        for layer in second_contract["layers"].values():
            for receipt in layer.get("receipts") or []:
                receipt["repository_key"] = second_contract["repository_key"]
                receipt["exact_head"] = second_contract["exact_head"]
        second_event = build_projection_event(
            workstream_id="GEN-37", kind="evidence_contract",
            key=second_contract["slice_id"], value=second_contract,
            plan_revision=PLAN, expected_revision=adapter.state().revision,
            created_at="2026-08-27T18:30:00Z", authority=AUTHORITY,
        )
        adapter.append(second_event)
        state = adapter.state()
        manifest.update(projection_review_contract(state))
        manifest["terminal_child_repairs"][0]["approved_evidence_heads"] = sorted([
            {
                "key": event["key"], "event_id": event["event_id"],
                "value_sha256": canonical_digest(event["value"]),
            }
            for event in state.events
            if event["kind"] == "evidence_contract"
        ], key=lambda item: (item["key"], item["event_id"]))
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError, "terminal_child_repair_owner_ambiguous",
        ):
            prepare_terminal_child_repairs(manifest, graph, state)
        self.assertEqual(len(client.comments), writes_before)

    def test_terminal_child_repair_resumes_after_closure_only_partial_write(self):
        client, adapter, source, graph, _child, stale_manifest = (
            self.terminal_repair_fixture()
        )
        prepared = prepare_terminal_child_repairs(
            stale_manifest, graph, adapter.state(),
        )
        closure_item = next(
            item for item in prepared["projection"]
            if item["kind"] == "child_closure"
        )
        closure_event = build_projection_event(
            workstream_id="GEN-37", kind="child_closure", key="GEN-72",
            value=closure_item["value"], plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-27T19:00:00Z", authority=AUTHORITY,
        )
        adapter.append(closure_event)
        partial_revision = adapter.state().revision

        resumed_manifest = prepare_terminal_child_repairs(
            stale_manifest, graph, adapter.state(),
        )
        self.assertEqual(
            resumed_manifest["expected_projection_revision"], partial_revision,
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", resumed_manifest, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        result = reconcile_required_projection(
            adapter, preview, resumed_manifest, remote_head=HEAD,
            created_at="2026-08-27T20:00:00Z", authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda _identifier: stale_manifest[
                "terminal_child_repairs"
            ][0]["expected_child_readback_sha256"],
            legacy_unresolved_relation_heads=unresolved,
        )
        appended = adapter.state().events[partial_revision:]
        self.assertEqual(
            [(event["kind"], event["key"]) for event in appended],
            [("scope", "root")],
        )
        strict = add_material_history(
            graph, client.comments, "GEN-37",
            authenticated_route=AUTHORITY, authenticated_source=source,
        )
        self.assertEqual(
            compact_context(strict, "GEN-37", require_projection_authority=True)[
                "resume_authority"
            ],
            "full",
        )
        self.assertTrue(result["readback_verified"])

    def test_terminal_child_repair_refuses_prewrite_issue_drift_with_zero_writes(self):
        client, adapter, source, graph, _child, stale_manifest = (
            self.terminal_repair_fixture()
        )
        prepared = prepare_terminal_child_repairs(
            stale_manifest, graph, adapter.state(),
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_readback_changed_reload_required",
        ):
            reconcile_required_projection(
                adapter, preview, prepared, remote_head=HEAD,
                created_at="2026-08-27T20:00:00Z",
                authenticated_source=source,
                relation_target_resolver=self.relation_target_resolver,
                terminal_child_fence=lambda _identifier: "0" * 64,
                legacy_unresolved_relation_heads=unresolved,
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_terminal_child_repair_refuses_midwrite_drift_before_ownership(self):
        client, adapter, source, graph, _child, stale_manifest = (
            self.terminal_repair_fixture()
        )
        prepared = prepare_terminal_child_repairs(
            stale_manifest, graph, adapter.state(),
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = stale_manifest["terminal_child_repairs"][0][
            "expected_child_readback_sha256"
        ]
        reads = 0

        def changing_fence(_identifier):
            nonlocal reads
            reads += 1
            return expected if reads <= 2 else "0" * 64

        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_readback_changed_reload_required",
        ):
            reconcile_required_projection(
                adapter, preview, prepared, remote_head=HEAD,
                created_at="2026-08-27T20:00:00Z",
                authenticated_source=source,
                relation_target_resolver=self.relation_target_resolver,
                terminal_child_fence=changing_fence,
                legacy_unresolved_relation_heads=unresolved,
            )
        appended = adapter.state().events[
            prepared["expected_projection_revision"]:
        ]
        self.assertEqual(
            [(event["kind"], event["key"]) for event in appended],
            [("child_closure", "GEN-72")],
        )
        self.assertNotIn(
            "GEN-72", adapter.state().snapshot["scope"]["child_ownership"],
        )

    def test_terminal_closure_refuses_evidence_add_retire_or_replace_drift(self):
        for mutation in ("add", "retire", "replace"):
            with self.subTest(mutation=mutation):
                client, adapter, source, graph, _child, stale_manifest = (
                    self.terminal_repair_fixture()
                )
                prepared = prepare_terminal_child_repairs(
                    stale_manifest, graph, adapter.state(),
                )
                preview, unresolved = load_material_history_for_projection_reconcile(
                    graph, client.comments, "GEN-37", prepared, adapter,
                    authenticated_route=AUTHORITY,
                    authenticated_source=source, remote_head=HEAD,
                    relation_target_resolver=self.relation_target_resolver,
                )
                expected_digest = stale_manifest["terminal_child_repairs"][0][
                    "expected_child_readback_sha256"
                ]
                reconcile_required_projection(
                    adapter, preview, prepared, remote_head=HEAD,
                    created_at="2026-08-27T20:00:00Z",
                    authenticated_source=source,
                    relation_target_resolver=self.relation_target_resolver,
                    terminal_child_fence=lambda _identifier: expected_digest,
                    legacy_unresolved_relation_heads=unresolved,
                )
                evidence_head = next(
                    event for event in reversed(adapter.state().events)
                    if event["kind"] == "evidence_contract"
                )
                key = evidence_head["key"]
                value = deepcopy(evidence_head["value"])
                supersedes = evidence_head["event_id"]
                if mutation == "add":
                    key = "gen72-late-evidence"
                    value["slice_id"] = key
                    supersedes = None
                elif mutation == "retire":
                    value = TOMBSTONE
                else:
                    value["layers"]["logic"]["receipts"][0]["id"] = (
                        "late-replacement"
                    )
                adapter.append(build_projection_event(
                    workstream_id="GEN-37", kind="evidence_contract", key=key,
                    value=value, plan_revision=PLAN,
                    expected_revision=adapter.state().revision,
                    created_at="2026-08-27T20:30:00Z",
                    supersedes_event_id=supersedes, authority=AUTHORITY,
                ))
                with self.assertRaisesRegex(
                    ResumeError, "child_closure_evidence_set_mismatch",
                ):
                    compact_context(
                        add_material_history(
                            graph, client.comments, "GEN-37",
                            authenticated_route=AUTHORITY,
                            authenticated_source=source,
                        ),
                        "GEN-37", require_projection_authority=True,
                    )

    def test_terminal_closure_cannot_be_retired_while_ownership_and_evidence_remain(self):
        client, adapter, source, graph, _child, stale_manifest = (
            self.terminal_repair_fixture()
        )
        prepared = prepare_terminal_child_repairs(
            stale_manifest, graph, adapter.state(),
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = stale_manifest["terminal_child_repairs"][0][
            "expected_child_readback_sha256"
        ]
        reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-27T20:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda _identifier: expected,
            legacy_unresolved_relation_heads=unresolved,
        )
        closure_head = next(
            event for event in reversed(adapter.state().events)
            if event["kind"] == "child_closure" and event["key"] == "GEN-72"
        )
        retirement = reviewed_retirement(
            adapter, "child_closure", "GEN-72",
        )
        desired = [
            item for item in prepared["projection"]
            if (item["kind"], item["key"]) != ("child_closure", "GEN-72")
        ]
        retirement_manifest = reviewed_manifest(
            adapter, desired, [retirement],
        )
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            ResumeError, "completed_owned_child_closure_missing:GEN-72",
        ):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", retirement_manifest, adapter,
                authenticated_route=AUTHORITY,
                authenticated_source=source, remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)

        evidence_head = next(
            event for event in reversed(adapter.state().events)
            if event["kind"] == "evidence_contract"
            and event["value"].get("owning_child") == "GEN-72"
        )
        evidence_retirement = reviewed_retirement(
            adapter, "evidence_contract", evidence_head["key"],
        )
        paired_desired = [
            item for item in desired
            if (item["kind"], item["key"])
            != ("evidence_contract", evidence_head["key"])
        ]
        paired_manifest = reviewed_manifest(
            adapter, paired_desired, [retirement, evidence_retirement],
        )
        with self.assertRaisesRegex(
            ResumeError, "completed_owned_child_closure_missing:GEN-72",
        ):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", paired_manifest, adapter,
                authenticated_route=AUTHORITY,
                authenticated_source=source, remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)

        reassigned = deepcopy(evidence_head["value"])
        reassigned["owning_child"] = "GEN-38"
        reassigned_desired = [
            *[
                item for item in desired
                if (item["kind"], item["key"])
                != ("evidence_contract", evidence_head["key"])
            ],
            {"kind": "evidence_contract", "key": evidence_head["key"],
             "value": reassigned},
        ]
        reassigned_manifest = reviewed_manifest(
            adapter, reassigned_desired, [retirement],
        )
        with self.assertRaisesRegex(
            ResumeError, "completed_owned_child_closure_missing:GEN-72",
        ):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37", reassigned_manifest, adapter,
                authenticated_route=AUTHORITY,
                authenticated_source=source, remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)

        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="child_closure", key="GEN-72",
            value=TOMBSTONE, plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-27T20:30:00Z",
            supersedes_event_id=closure_head["event_id"], authority=AUTHORITY,
        ))
        with self.assertRaisesRegex(
            ResumeError, "completed_owned_child_closure_missing:GEN-72",
        ):
            compact_context(
                add_material_history(
                    graph, client.comments, "GEN-37",
                    authenticated_route=AUTHORITY,
                    authenticated_source=source,
                ),
                "GEN-37", require_projection_authority=True,
            )

    def test_terminal_closure_add_or_replace_requires_matching_repair(self):
        client, adapter, source, graph, _child, stale_manifest = (
            self.terminal_repair_fixture()
        )
        prepared = prepare_terminal_child_repairs(
            stale_manifest, graph, adapter.state(),
        )
        preview, unresolved = load_material_history_for_projection_reconcile(
            graph, client.comments, "GEN-37", prepared, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            remote_head=HEAD,
            relation_target_resolver=self.relation_target_resolver,
        )
        expected = stale_manifest["terminal_child_repairs"][0][
            "expected_child_readback_sha256"
        ]
        reconcile_required_projection(
            adapter, preview, prepared, remote_head=HEAD,
            created_at="2026-08-27T20:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
            terminal_child_fence=lambda _identifier: expected,
            legacy_unresolved_relation_heads=unresolved,
        )
        closure_item = next(
            item for item in prepared["projection"]
            if (item["kind"], item["key"]) == ("child_closure", "GEN-72")
        )
        replacement = deepcopy(prepared["projection"])
        next(
            item for item in replacement
            if (item["kind"], item["key"]) == ("child_closure", "GEN-72")
        )["value"]["state_name"] = "Completed"
        replacement_manifest = reviewed_manifest(adapter, replacement)
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_closure_repair_required:GEN-72",
        ):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37",
                replacement_manifest, adapter,
                authenticated_route=AUTHORITY,
                authenticated_source=source, remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_closure_repair_required:GEN-72",
        ):
            reconcile_required_projection(
                adapter, {"root": {"identifier": "GEN-37"}},
                replacement_manifest, remote_head=HEAD,
                created_at="2026-08-27T20:15:00Z",
                authenticated_source=source,
            )
        self.assertEqual(len(client.comments), writes_before)

        closure_head = next(
            event for event in reversed(adapter.state().events)
            if event["kind"] == "child_closure" and event["key"] == "GEN-72"
        )
        adapter.append(build_projection_event(
            workstream_id="GEN-37", kind="child_closure", key="GEN-72",
            value=TOMBSTONE, plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-27T20:30:00Z",
            supersedes_event_id=closure_head["event_id"], authority=AUTHORITY,
        ))
        addition_manifest = reviewed_manifest(adapter, prepared["projection"])
        writes_before = len(client.comments)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_closure_repair_required:GEN-72",
        ):
            load_material_history_for_projection_reconcile(
                graph, client.comments, "GEN-37",
                addition_manifest, adapter,
                authenticated_route=AUTHORITY,
                authenticated_source=source, remote_head=HEAD,
                relation_target_resolver=self.relation_target_resolver,
            )
        self.assertEqual(len(client.comments), writes_before)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "terminal_child_closure_repair_required:GEN-72",
        ):
            reconcile_required_projection(
                adapter, {"root": {"identifier": "GEN-37"}},
                addition_manifest, remote_head=HEAD,
                created_at="2026-08-27T20:45:00Z",
                authenticated_source=source,
            )
        self.assertEqual(len(client.comments), writes_before)
        self.assertEqual(closure_item["value"]["child_identifier"], "GEN-72")

    def test_legacy_unresolved_relation_retirement_precedes_unrelated_writes(self):
        client, adapter, base, source = self.legacy_relation_fixture()
        retirement = reviewed_retirement(adapter, "relation", "blocks:GEN-14")
        desired = [*base[:-2], {
            "kind": "provenance", "key": "new", "value": {
                "agent": "claude", "machine": "M3", "session_id": "new",
                "worktree": {"state": "safe", "head": HEAD},
            },
        }]
        manifest = reviewed_manifest(adapter, desired, [retirement])
        snapshot, unresolved = load_material_history_for_projection_reconcile(
            self.graph_snapshot(), client.comments, "GEN-37", manifest, adapter,
            authenticated_route=AUTHORITY, authenticated_source=source,
            relation_target_resolver=self.incomplete_relation_target_resolver,
        )
        revision_before = adapter.state().revision
        result = reconcile_required_projection(
            adapter, snapshot, manifest, remote_head=HEAD,
            created_at="2026-08-27T19:00:00Z", authenticated_source=source,
            legacy_unresolved_relation_heads=unresolved,
        )
        appended = adapter.state().events[revision_before:]
        self.assertEqual(
            (appended[0]["kind"], appended[0]["key"], appended[0]["value"]),
            ("relation", "blocks:GEN-14", TOMBSTONE),
        )
        self.assertEqual(adapter.state().snapshot["relations"], [])
        self.assertTrue(result["readback_verified"])

    def test_concurrent_same_logical_relation_converges_without_duplicate_or_loss(self):
        client = FakeProjectionClient()
        first = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        second = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
        )
        relation = {"type": "related", "target": {
            "workspace_id": "workspace",
            "issue_id": "22222222-2222-4222-8222-222222222222",
            "identifier": "GEN-50",
        }}
        event_a = build_projection_event(
            workstream_id="GEN-37", kind="relation", key="related:GEN-50",
            value=relation, plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-28T12:00:00Z", authority=AUTHORITY,
        )
        event_b = build_projection_event(
            workstream_id="GEN-37", kind="relation", key="related:GEN-50",
            value=relation, plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-28T12:00:00Z", authority=AUTHORITY,
        )
        receipt_a = first.append(event_a)
        receipt_b = second.append(event_b)
        self.assertEqual(receipt_a["event_id"], receipt_b["event_id"])
        self.assertEqual(len(client.comments), 1)
        self.assertEqual(first.state().snapshot["relations"], [relation])

    def test_projection_refuses_unresolved_or_invalid_inverse_before_append(self):
        relation = {"type": "blocks", "target": {
            "workspace_id": "workspace", "issue_id": TARGET_UUID,
            "identifier": "GEN-50",
        }}
        root = {"workspace_id": "workspace", "issue_id": ROOT_UUID,
                "identifier": "GEN-37"}
        target_key = f"workspace:{TARGET_UUID}"
        cases = {
            "dangling_relation_target": {},
            "missing_relation_inverse": {
                target_key: {**relation["target"], "relations": []},
            },
            "contradictory_relation_inverse": {
                target_key: {**relation["target"], "relations": [
                    {"type": "blocks", "target": root},
                ]},
            },
        }
        for expected_error, targets in cases.items():
            with self.subTest(expected_error=expected_error):
                client = FakeProjectionClient()
                adapter = LinearProjectionAdapter(
                    client, issue_id="GEN-37", workstream_id="GEN-37",
                    plan_revision=PLAN, **AUTHORITY,
                )
                projected = [
                    {"kind": "scope", "key": "root", "value": scope()},
                    {"kind": "source", "key": "root", "value": {
                        "sha256": PLAN, "identity": "https://example.test/plan",
                    }},
                    {"kind": "provenance", "key": "session", "value": {
                        "agent": "codex", "machine": "M5", "session_id": "session",
                    }},
                    {"kind": "relation", "key": "blocks:GEN-50", "value": relation},
                ]
                with self.assertRaisesRegex(LinearProjectionError, expected_error):
                    reconcile_required_projection(
                        adapter, {"root": {"identifier": "GEN-37"}},
                        reviewed_manifest(adapter, projected), remote_head=HEAD,
                        created_at="2026-08-28T12:00:00Z",
                        authenticated_source={
                            "identity": "https://example.test/plan", "sha256": PLAN,
                        },
                        relation_target_resolver=lambda _relations, value=targets: value,
                    )
                self.assertEqual(client.comments, [])

    def _single_legacy_activation_comments(self, value):
        legacy = legacy_event(
            "scope", "root", scope(), 0, "2026-08-27T17:00:00Z",
        )
        activation = build_projection_event(
            workstream_id="GEN-37", kind="cas_activation", key="root",
            value=value, plan_revision=PLAN, expected_revision=1,
            created_at="2026-08-27T18:00:00Z", authority=AUTHORITY,
        )
        return legacy, [
            legacy_comment(legacy, "legacy-0"), projection_comment(activation),
        ]

    def test_literal_gen37_040_activation_and_predecessors_remain_readable(self):
        state = reduce_projection_comments(
            gen37_040_activation_fixture(), workstream_id="GEN-37",
            expected_plan_revision=PLAN,
            authenticated_route={
                "workspace_id": "d830b5ae-5616-492d-a71d-3313387e8b6f",
                "team_id": "d59c5509-8d96-4093-b7d5-0437ced5c679",
                "project_id": "eea2522b-187d-4f5b-af27-fc833d4fd1cb",
                "root_issue_id": "409c1423-f949-4655-9f5f-d3213d7b434f",
            },
        )
        self.assertEqual(state.revision, 16)
        activation = state.events[-1]
        self.assertEqual(activation["event_id"], "wsp_401f8448524cf890557c9a2a84bb674a")
        self.assertEqual(
            set(activation["value"]),
            {"legacy_event_ids", "legacy_events_sha256"},
        )
        self.assertEqual(state.snapshot["scope"]["namespace"], "agent-workstream-continuity")

    def test_unversioned_d457_full_event_digest_remains_readable(self):
        legacy = legacy_event(
            "scope", "root", scope(), 0, "2026-08-27T17:00:00Z",
        )
        value = {
            "legacy_event_ids": [legacy["event_id"]],
            "legacy_events_sha256": hashlib.sha256(
                projection_module._canonical([legacy])
            ).hexdigest(),
        }
        _legacy, comments = self._single_legacy_activation_comments(value)
        state = reduce_projection_comments(
            comments, workstream_id="GEN-37", expected_plan_revision=PLAN,
            authenticated_route=AUTHORITY,
        )
        self.assertEqual(state.revision, 2)

    def test_tagged_activation_rejects_ids_digest_without_fallback(self):
        legacy = legacy_event(
            "scope", "root", scope(), 0, "2026-08-27T17:00:00Z",
        )
        value = {
            "legacy_digest_kind": projection_module.LEGACY_DIGEST_KIND_FULL_EVENTS,
            "legacy_event_ids": [legacy["event_id"]],
            "legacy_events_sha256": hashlib.sha256(
                projection_module._canonical([legacy["event_id"]])
            ).hexdigest(),
        }
        _legacy, comments = self._single_legacy_activation_comments(value)
        with self.assertRaisesRegex(
            LinearProjectionError, "activation_legacy_digest_mismatch",
        ):
            reduce_projection_comments(
                comments, workstream_id="GEN-37", expected_plan_revision=PLAN,
                authenticated_route=AUTHORITY,
            )

    def test_tagged_activation_at_zero_rejects_unreviewed_digest(self):
        activation = build_projection_event(
            workstream_id="GEN-37", kind="cas_activation", key="root",
            value={
                "legacy_digest_kind": (
                    projection_module.LEGACY_DIGEST_KIND_FULL_EVENTS
                ),
                "legacy_event_ids": [],
                "legacy_events_sha256": "a" * 64,
            },
            plan_revision=PLAN, expected_revision=0,
            created_at="2026-08-27T18:00:00Z", authority=AUTHORITY,
        )
        with self.assertRaisesRegex(
            LinearProjectionError, "activation_without_legacy",
        ):
            reduce_projection_comments(
                [projection_comment(activation)], workstream_id="GEN-37",
                expected_plan_revision=PLAN, authenticated_route=AUTHORITY,
            )

    def test_tagged_activation_rejects_reversed_reviewed_id_order(self):
        first = legacy_event(
            "scope", "root", scope(), 0, "2026-08-27T17:00:00Z",
        )
        second = legacy_event(
            "scope", "ordered", scope(), 1,
            "2026-08-27T17:01:00Z",
        )
        activation = build_projection_event(
            workstream_id="GEN-37", kind="cas_activation", key="root",
            value={
                "legacy_digest_kind": (
                    projection_module.LEGACY_DIGEST_KIND_FULL_EVENTS
                ),
                "legacy_event_ids": [second["event_id"], first["event_id"]],
                "legacy_events_sha256": hashlib.sha256(
                    projection_module._canonical([first, second])
                ).hexdigest(),
            },
            plan_revision=PLAN, expected_revision=2,
            created_at="2026-08-27T18:00:00Z", authority=AUTHORITY,
        )
        with self.assertRaisesRegex(
            LinearProjectionError, "activation_legacy_order_mismatch",
        ):
            reduce_projection_comments(
                [
                    legacy_comment(first, "legacy-0"),
                    legacy_comment(second, "legacy-1"),
                    projection_comment(activation),
                ],
                workstream_id="GEN-37", expected_plan_revision=PLAN,
                authenticated_route=AUTHORITY,
            )

    def test_untagged_activation_rejects_neither_and_ambiguous_both(self):
        legacy = legacy_event(
            "scope", "root", scope(), 0, "2026-08-27T17:00:00Z",
        )
        neither = {
            "legacy_event_ids": [legacy["event_id"]],
            "legacy_events_sha256": "0" * 64,
        }
        _legacy, comments = self._single_legacy_activation_comments(neither)
        with self.assertRaisesRegex(
            LinearProjectionError, "activation_legacy_digest_mismatch",
        ):
            reduce_projection_comments(
                comments, workstream_id="GEN-37", expected_plan_revision=PLAN,
                authenticated_route=AUTHORITY,
            )
        ambiguous = {**neither, "legacy_events_sha256": "a" * 64}
        with mock.patch.object(
            projection_module, "_activation_digest_candidates",
            return_value=("a" * 64, "a" * 64),
        ):
            self.assertFalse(projection_module._activation_legacy_digest_is_valid(
                ambiguous, [legacy],
            ))

    def test_activation_rejects_unknown_tag_and_extra_fields(self):
        base = {
            "legacy_digest_kind": projection_module.LEGACY_DIGEST_KIND_FULL_EVENTS,
            "legacy_event_ids": ["wsp_legacy"],
            "legacy_events_sha256": "a" * 64,
        }
        for value in (
            {**base, "legacy_digest_kind": "unknown"},
            {**base, "extra": "mixed"},
            {key: item for key, item in base.items() if key != "legacy_event_ids"},
        ):
            with self.assertRaisesRegex(
                LinearProjectionError, "invalid_projection_cas_activation",
            ):
                build_projection_event(
                    workstream_id="GEN-37", kind="cas_activation", key="root",
                    value=value, plan_revision=PLAN, expected_revision=1,
                    created_at="2026-08-27T18:00:00Z", authority=AUTHORITY,
                )

    def event(self, kind, key, value, revision, supersedes=None):
        return build_projection_event(
            workstream_id="GEN-37", kind=kind, key=key, value=value,
            plan_revision=PLAN, expected_revision=revision,
            created_at=f"2026-08-27T12:{revision:02d}:00Z",
            supersedes_event_id=supersedes,
            authority=AUTHORITY,
        )

    def test_deterministic_remote_slot_collision_refuses_loser_without_poisoning(self):
        winner = self.event("provenance", "winner", {
            "agent": "claude", "machine": "M3", "session_id": "winner",
        }, 0)
        loser = self.event("provenance", "loser", {
            "agent": "codex", "machine": "M5", "session_id": "loser",
        }, 0)
        client = RacingProjectionClient(winner)
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN,
            workspace_id="workspace", team_id="team", project_id="project",
            root_issue_id=ROOT_UUID,
        )
        with self.assertRaisesRegex(LinearProjectionError, "projection_slot_lost_reload_required"):
            adapter.append(loser)
        state = adapter.state()
        self.assertEqual(state.revision, 1)
        self.assertEqual([item["event_id"] for item in state.events], [winner["event_id"]])
        self.assertEqual(
            client.comments[0]["id"], projection_slot_id("GEN-37", PLAN, 0, AUTHORITY),
        )

    def test_identical_remote_slot_collision_is_crash_safe_replay(self):
        event = self.event("provenance", "same", {
            "agent": "codex", "machine": "M5", "session_id": "same",
        }, 0)
        client = RacingProjectionClient(event)
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN,
            workspace_id="workspace", team_id="team", project_id="project",
            root_issue_id=ROOT_UUID,
        )
        receipt = adapter.append(event)
        self.assertEqual(
            receipt["remote_id"], projection_slot_id("GEN-37", PLAN, 0, AUTHORITY),
        )
        self.assertEqual(len(client.comments), 1)
        self.assertEqual(adapter.append(event)["remote_id"], receipt["remote_id"])
        self.assertEqual(len(client.comments), 1)

    def test_reducer_rejects_duplicate_revision_and_wrong_v2_remote_slot(self):
        first = self.event("provenance", "one", {
            "agent": "codex", "machine": "M5", "session_id": "one",
        }, 0)
        second = self.event("provenance", "two", {
            "agent": "claude", "machine": "M3", "session_id": "two",
        }, 0)
        with self.assertRaisesRegex(LinearProjectionError, "projection_revision_mismatch"):
            reduce_projection_comments(
                [projection_comment(first), projection_comment(second)],
                workstream_id="GEN-37", expected_plan_revision=PLAN,
            )
        with self.assertRaisesRegex(LinearProjectionError, "projection_slot_identity_mismatch"):
            reduce_projection_comments(
                [{"id": "arbitrary", "body": encode_projection_comment(first)}],
                workstream_id="GEN-37", expected_plan_revision=PLAN,
            )

    def test_v1_history_activation_quarantines_late_v1_writer(self):
        client = FakeProjectionClient()
        first = legacy_event(
            "provenance", "legacy-one",
            {"agent": "codex", "machine": "M5", "session_id": "legacy-one"},
            0, "2026-08-27T11:00:00Z",
        )
        second = legacy_event(
            "provenance", "legacy-two",
            {"agent": "claude", "machine": "M3", "session_id": "legacy-two"},
            0, "2026-08-27T11:01:00Z",
        )
        client.comments.extend([
            legacy_comment(first, "legacy-comment-1"),
            legacy_comment(second, "legacy-comment-2"),
        ])
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN,
            **AUTHORITY,
        )
        self.assertEqual(adapter.state().revision, 2)
        premature = self.event("provenance", "modern", {
            "agent": "codex", "machine": "M5", "session_id": "modern",
        }, 2)
        with self.assertRaisesRegex(LinearProjectionError, "v2_activation_required"):
            adapter.append(premature)
        activation = adapter.activate_v2(created_at="2026-08-27T11:02:00Z")
        self.assertEqual(activation["revision"], 3)

        late = legacy_event(
            "provenance", "late-v1",
            {"agent": "codex", "machine": "M1", "session_id": "late-v1"},
            2, "2026-08-27T11:03:00Z",
        )
        client.comments.append(legacy_comment(late, "late-arbitrary-comment"))
        modern = self.event("provenance", "modern", {
            "agent": "codex", "machine": "M5", "session_id": "modern",
        }, 3)
        adapter.append(modern)
        state = adapter.state()
        self.assertEqual(state.revision, 4)
        self.assertEqual(
            [item["session_id"] for item in state.snapshot["provenance"]],
            ["legacy-two", "legacy-one", "modern"],
        )
        self.assertEqual(
            [event["event_id"] for event in state.snapshot["projection_quarantined"]],
            [late["event_id"]],
        )
        disposition = self.event("quarantine_disposition", "root", {
            "event_ids": [late["event_id"]],
            "events_sha256": hashlib.sha256(json.dumps(
                [late], ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            ).encode()).hexdigest(),
            "review_artifact_identity": "https://example.test/reviews/late-v1.md",
            "review_artifact_sha256": "d" * 64,
            "reviewed_at": "2026-08-27T11:04:00Z",
        }, 4)
        adapter.append(disposition)
        self.assertEqual(adapter.state().snapshot["projection_unresolved_quarantine"], [])

    def test_slots_are_isolated_by_immutable_linear_route(self):
        other = {**AUTHORITY, "workspace_id": "other-workspace", "root_issue_id": "other-root"}
        self.assertNotEqual(
            projection_slot_id("GEN-37", PLAN, 0, AUTHORITY),
            projection_slot_id("GEN-37", PLAN, 0, other),
        )

    def test_append_refuses_when_linear_schema_lacks_client_comment_id(self):
        class NoIdClient(FakeProjectionClient):
            def execute(self, query, variables):
                if "WorkstreamProjectionCommentCreateCapability" in query:
                    return {"__type": {"inputFields": [{"name": "body"}]}}
                return super().execute(query, variables)

        client = NoIdClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN,
            **AUTHORITY,
        )
        with self.assertRaisesRegex(LinearProjectionError, "id_capability_unavailable"):
            adapter.append(self.event("provenance", "one", {
                "agent": "codex", "machine": "M5", "session_id": "one",
            }, 0))
        self.assertEqual(client.comments, [])

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
            {**projection_comment(event),
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
            authority=AUTHORITY,
        )
        enriched["projection_events"].append(stale)
        enriched["projection_revision"] += 1
        with self.assertRaisesRegex(ResumeError, "projection_plan_drift"):
            compact_context(enriched, "GEN-37")

    def test_replay_is_zero_write_and_unfenced_replacement_fails_closed(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN,
            workspace_id="workspace", team_id="team", project_id="project",
            root_issue_id=ROOT_UUID,
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
            client, issue_id="GEN-37", workstream_id="GEN-37", plan_revision=PLAN,
            workspace_id="workspace", team_id="team", project_id="project",
            root_issue_id=ROOT_UUID,
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
            [projection_comment(event)],
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
            authority=AUTHORITY,
        )
        comments = [projection_comment(current), projection_comment(stale)]
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
            authority=AUTHORITY,
        )
        current = self.event(
            "source", "root", {"sha256": PLAN, "identity": "https://example.test/current"}, 0,
        )
        comments = [projection_comment(old), projection_comment(current)]
        reduced = reduce_projection_comments(
            comments, workstream_id="GEN-37", expected_plan_revision=PLAN,
        )
        self.assertEqual(reduced.revision, 1)
        self.assertEqual(reduced.snapshot["source"]["identity"], "https://example.test/current")
        conflict = self.event(
            "source", "root", {"sha256": PLAN, "identity": "https://example.test/other"}, 0,
        )
        comments.append(projection_comment(conflict))
        with self.assertRaisesRegex(LinearProjectionError, "projection_revision_mismatch"):
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
            [projection_comment(event) for event in events],
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
            authority=AUTHORITY,
        )
        with self.assertRaisesRegex(LinearProjectionError, "source_plan_mismatch"):
            reduce_projection_comments(
                [projection_comment(wrong_source)],
                workstream_id="GEN-37", expected_plan_revision=PLAN,
            )
        scoped = self.event("scope", "root", scope(), 0)
        with self.assertRaisesRegex(LinearProjectionError, "route_mismatch:project_id"):
            reduce_projection_comments(
                [projection_comment(scoped)],
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
        comments = [projection_comment(event) for event in events]
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

    def test_product_reconcile_activates_exact_reviewed_v1_then_replays_noop(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
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
        disposition = {
            "disposition": "attach", "remote_head": HEAD,
            "recovered_from_checkpoint": "wsc-live",
        }
        legacy_values = [
            *[(item["kind"], item["key"], item["value"]) for item in projection],
            ("disposition", "root", disposition),
        ]
        for revision, (kind, key, value) in enumerate(legacy_values):
            event = legacy_event(
                kind, key, value, revision, f"2026-08-27T17:0{revision}:00Z",
            )
            client.comments.append(legacy_comment(event, f"legacy-{revision}"))
        snapshot = {
            "root": {"identifier": "GEN-37"},
            "latest_checkpoint": {
                "checkpoint_event_id": "wsc-live",
                "worktree": {"state": "safe", "head": HEAD},
            },
        }
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        manifest = reviewed_manifest(adapter, projection)
        legacy_events = adapter.state().events
        self.assertEqual(
            manifest["expected_legacy_v1_event_ids"],
            [event["event_id"] for event in legacy_events],
        )
        self.assertEqual(
            manifest["expected_legacy_v1_events_sha256"],
            hashlib.sha256(json.dumps(
                list(legacy_events), sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")).hexdigest(),
        )
        first = reconcile_required_projection(
            adapter, snapshot, manifest, remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
        )
        self.assertEqual(len(first["writes"]), 1)
        activation = adapter.state().events[-1]
        self.assertEqual(activation["kind"], "cas_activation")
        self.assertEqual(
            set(activation["value"]),
            {"legacy_digest_kind", "legacy_event_ids", "legacy_events_sha256"},
        )
        self.assertEqual(
            activation["value"]["legacy_digest_kind"],
            projection_module.LEGACY_DIGEST_KIND_FULL_EVENTS,
        )
        self.assertEqual(len(client.comments), 5)

        replay_manifest = reviewed_manifest(adapter, projection)
        self.assertEqual(replay_manifest["expected_legacy_v1_event_ids"], [])
        self.assertIsNone(replay_manifest["expected_legacy_v1_events_sha256"])
        replay = reconcile_required_projection(
            adapter, snapshot, replay_manifest, remote_head=HEAD,
            created_at="2026-08-27T18:01:00Z", authenticated_source=source,
        )
        self.assertEqual(replay["writes"], [])
        self.assertEqual(len(client.comments), 5)

    def test_product_reconcile_refuses_legacy_change_during_activation(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
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
        for revision, item in enumerate(projection):
            event = legacy_event(
                item["kind"], item["key"], item["value"], revision,
                f"2026-08-27T17:0{revision}:00Z",
            )
            client.comments.append(legacy_comment(event, f"legacy-{revision}"))
        manifest = reviewed_manifest(adapter, projection)
        late = legacy_event(
            "relation", "blocks:GEN-99", {"type": "blocks", "target": {
                "workspace_id": "workspace", "issue_id": "late-uuid",
                "identifier": "GEN-99",
            }}, 3, "2026-08-27T17:03:00Z",
        )
        activate = adapter.activate_v2

        def race(**kwargs):
            client.comments.append(legacy_comment(late, "legacy-late"))
            return activate(**kwargs)

        with mock.patch.object(adapter, "activate_v2", side_effect=race):
            with self.assertRaisesRegex(
                LinearProjectionError, "activation_stale_reload_required",
            ):
                reconcile_required_projection(
                    adapter, {"root": {"identifier": "GEN-37"}}, manifest,
                    remote_head=HEAD, created_at="2026-08-27T18:00:00Z",
                    authenticated_source={
                        "identity": "https://example.test/plan", "sha256": PLAN,
                    },
                )
        self.assertEqual(len(client.comments), 4)
        self.assertFalse(any(
            event["schema_version"] == 2 for event in adapter.state().events
        ))

    def _assert_product_reconcile_refuses_post_activation_legacy_race(
        self, *, activation_observation: int, expected_error: str,
    ):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN, **AUTHORITY,
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
        for revision, item in enumerate(projection):
            event = legacy_event(
                item["kind"], item["key"], item["value"], revision,
                f"2026-08-27T17:0{revision}:00Z",
            )
            client.comments.append(legacy_comment(event, f"legacy-{revision}"))
        manifest = reviewed_manifest(adapter, projection)
        late = legacy_event(
            "relation", "blocks:GEN-99", {"type": "blocks", "target": {
                "workspace_id": "workspace", "issue_id": "late-uuid",
                "identifier": "GEN-99",
            }}, 3, "2026-08-27T17:03:00Z",
        )
        original_state = adapter.state
        observations = 0
        injected = False

        def raced_state():
            nonlocal observations, injected
            state = original_state()
            if any(event["kind"] == "cas_activation" for event in state.events):
                observations += 1
                if observations == activation_observation:
                    client.comments.append(legacy_comment(late, "legacy-late"))
                    injected = True
            return state

        with mock.patch.object(adapter, "state", side_effect=raced_state):
            with self.assertRaisesRegex(LinearProjectionError, expected_error):
                reconcile_required_projection(
                    adapter, {"root": {"identifier": "GEN-37"}}, manifest,
                    remote_head=HEAD, created_at="2026-08-27T18:00:00Z",
                    authenticated_source={
                        "identity": "https://example.test/plan", "sha256": PLAN,
                    },
                )
        self.assertTrue(injected)
        state = original_state()
        self.assertEqual(state.snapshot["scope"], scope())
        self.assertFalse(any(event["value"] == TOMBSTONE for event in state.events))
        self.assertEqual(
            [event["event_id"] for event in state.snapshot["projection_quarantined"]],
            [late["event_id"]],
        )

    def test_product_reconcile_refuses_late_v1_after_activation_create(self):
        self._assert_product_reconcile_refuses_post_activation_legacy_race(
            activation_observation=1,
            expected_error="projection_v2_activation_readback_mismatch",
        )

    def test_product_reconcile_refuses_late_v1_before_first_data_append(self):
        self._assert_product_reconcile_refuses_post_activation_legacy_race(
            activation_observation=6,
            expected_error="projection_quarantine_changed_reload_required",
        )

    def test_product_reconcile_refuses_late_v1_before_final_readback(self):
        self._assert_product_reconcile_refuses_post_activation_legacy_race(
            activation_observation=9,
            expected_error="projection_final_contract_mismatch",
        )

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
            "workspace_id": "workspace", "issue_id": TARGET_UUID,
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
            relation_target_resolver=self.relation_target_resolver,
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
            authority=AUTHORITY,
        )
        client.comments.append({
            **projection_comment(late),
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
            "workspace_id": "workspace", "issue_id": TARGET_UUID,
            "identifier": "GEN-14",
        }}
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, [*base, {
                "kind": "relation", "key": "blocks:GEN-14", "value": relation,
            }]), remote_head=HEAD, created_at="2026-08-27T18:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
        )
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, base), remote_head=HEAD,
            created_at="2026-08-27T19:00:00Z", authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
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
                    "workspace_id": "workspace", "issue_id": TARGET_UUID,
                    "identifier": "GEN-14",
                },
            }},
        ]
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        reconcile_required_projection(
            adapter, {"root": {"identifier": "GEN-37"}},
            reviewed_manifest(adapter, base), remote_head=HEAD,
            created_at="2026-08-27T18:00:00Z", authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
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
                "workspace_id": "workspace", "issue_id": CHANGED_TARGET_UUID,
                "identifier": "GEN-14",
            }},
            plan_revision=PLAN, expected_revision=adapter.state().revision,
            created_at="2026-08-27T18:00:30Z",
            supersedes_event_id=current["event_id"],
            authority=AUTHORITY,
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
            "workspace_id": "workspace", "issue_id": TARGET_UUID,
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
            relation_target_resolver=self.relation_target_resolver,
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

    def test_retired_relation_and_evidence_can_reactivate_same_key(self):
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
            "workspace_id": "workspace", "issue_id": TARGET_UUID,
            "identifier": "GEN-14",
        }}
        projected = [
            *base,
            {"kind": "relation", "key": "blocks:GEN-14", "value": relation},
            {"kind": "evidence_contract", "key": "gen37-resume",
             "value": evidence_contract()},
        ]
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        snapshot = {"root": {"identifier": "GEN-37"}}
        reconcile_required_projection(
            adapter, snapshot, reviewed_manifest(adapter, projected),
            remote_head=HEAD, created_at="2026-08-27T18:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
        )
        retirements = [
            reviewed_retirement(adapter, "relation", "blocks:GEN-14"),
            reviewed_retirement(adapter, "evidence_contract", "gen37-resume"),
        ]
        reconcile_required_projection(
            adapter, snapshot, reviewed_manifest(adapter, base, retirements),
            remote_head=HEAD, created_at="2026-08-27T19:00:00Z",
            authenticated_source=source,
        )
        retired_state = adapter.state()
        tombstones = {
            (event["kind"], event["key"]): event["event_id"]
            for event in retired_state.events if event["value"] == TOMBSTONE
        }
        result = reconcile_required_projection(
            adapter, snapshot, reviewed_manifest(adapter, projected),
            remote_head=HEAD, created_at="2026-08-27T20:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
        )
        state = adapter.state()
        self.assertEqual(state.snapshot["relations"], [relation])
        self.assertEqual(state.snapshot["evidence_contracts"], [evidence_contract()])
        reactivated = {
            (event["kind"], event["key"]): event
            for event in state.events
            if (event["kind"], event["key"]) in tombstones
            and event["value"] != TOMBSTONE
        }
        self.assertEqual(set(reactivated), set(tombstones))
        for identity, event in reactivated.items():
            self.assertEqual(event["supersedes_event_id"], tombstones[identity])
        self.assertEqual(len(result["writes"]), 2)

    def test_reactivation_refuses_stale_tombstone_head_without_writing(self):
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
            "workspace_id": "workspace", "issue_id": TARGET_UUID,
            "identifier": "GEN-14",
        }}
        projected = [*base, {
            "kind": "relation", "key": "blocks:GEN-14", "value": relation,
        }]
        source = {"identity": "https://example.test/plan", "sha256": PLAN}
        snapshot = {"root": {"identifier": "GEN-37"}}
        reconcile_required_projection(
            adapter, snapshot, reviewed_manifest(adapter, projected),
            remote_head=HEAD, created_at="2026-08-27T18:00:00Z",
            authenticated_source=source,
            relation_target_resolver=self.relation_target_resolver,
        )
        reconcile_required_projection(
            adapter, snapshot, reviewed_manifest(adapter, base, [
                reviewed_retirement(adapter, "relation", "blocks:GEN-14"),
            ]), remote_head=HEAD, created_at="2026-08-27T19:00:00Z",
            authenticated_source=source,
        )
        stale_manifest = reviewed_manifest(adapter, projected)
        tombstone = next(
            event for event in reversed(adapter.state().events)
            if event["kind"] == "relation" and event["key"] == "blocks:GEN-14"
        )
        competitor = build_projection_event(
            workstream_id="GEN-37", kind="relation", key="blocks:GEN-14",
            value=relation, plan_revision=PLAN,
            expected_revision=adapter.state().revision,
            created_at="2026-08-27T19:30:00Z",
            supersedes_event_id=tombstone["event_id"],
            authority=AUTHORITY,
        )
        adapter.append(competitor)
        writes_before = len(client.comments)
        with self.assertRaisesRegex(LinearProjectionError, "stale_reload_required"):
            reconcile_required_projection(
                adapter, snapshot, stale_manifest, remote_head=HEAD,
                created_at="2026-08-27T20:00:00Z", authenticated_source=source,
            )
        self.assertEqual(len(client.comments), writes_before)

    def test_product_reconcile_refuses_unverified_source_bytes(self):
        client = FakeProjectionClient()
        adapter = LinearProjectionAdapter(
            client, issue_id="GEN-37", workstream_id="GEN-37",
            plan_revision=PLAN,
            workspace_id="workspace", team_id="team", project_id="project",
            root_issue_id=ROOT_UUID,
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

    def test_manifest_source_sync_refreshes_stale_same_document_source(self):
        exact = (
            "https://github.com/acme/plans/blob/" + "a" * 40 + "/PLAN.md"
        )
        manifest = {"projection": [{
            "kind": "source", "key": "root", "value": {
                "identity": "https://github.com/acme/plans/blob/main/PLAN.md",
                "sha256": "old",
            },
        }]}

        synced, source = workstream_projection.synchronize_manifest_source(
            manifest, f"Canonical plan: {exact}",
            {"identity": "https://github.com/acme/plans/blob/main/PLAN.md",
             "sha256": "new", "bytes": 10},
        )

        self.assertEqual(synced["projection"][0]["value"], {
            "identity": exact, "sha256": "new",
        })
        self.assertEqual(source, {"identity": exact, "sha256": "new", "bytes": 10})
        self.assertEqual(manifest["projection"][0]["value"]["sha256"], "old")

    def test_manifest_source_sync_refuses_ambiguous_issue_without_mutation(self):
        manifest = {"projection": []}
        original = deepcopy(manifest)
        with self.assertRaisesRegex(ValueError, "canonical_plan_source_ambiguous"):
            workstream_projection.synchronize_manifest_source(
                manifest,
                "Canonical plan: https://example.test/one\n"
                "Canonical plan: https://example.test/two",
                {"identity": "https://example.test/one", "sha256": "new"},
            )
        self.assertEqual(manifest, original)

    def test_manifest_source_sync_refuses_unreviewed_live_document_change(self):
        manifest = {"projection": []}
        original = deepcopy(manifest)
        with self.assertRaisesRegex(
            LinearProjectionError,
            "live_source_document_change_requires_explicit_review",
        ):
            workstream_projection.synchronize_manifest_source(
                manifest,
                "Canonical plan: https://github.com/acme/new/blob/main/PLAN.md",
                {"identity": "https://github.com/acme/new/blob/main/PLAN.md",
                 "sha256": "new"},
                {"identity": "https://github.com/acme/old/blob/main/PLAN.md",
                 "sha256": "old"},
            )
        self.assertEqual(manifest, original)

    def test_manifest_source_sync_allows_reviewed_live_document_change(self):
        canonical = "https://github.com/acme/new/blob/main/PLAN.md"
        manifest = {"projection": [{
            "kind": "source", "key": "root", "value": {
                "identity": canonical, "sha256": "new",
            },
        }]}

        synced, source = workstream_projection.synchronize_manifest_source(
            manifest, f"Canonical plan: {canonical}",
            {"identity": canonical, "sha256": "new"},
            {"identity": "https://github.com/acme/old/blob/main/PLAN.md",
             "sha256": "old"},
        )

        self.assertEqual(synced["projection"][0]["value"], source)
        self.assertEqual(source, {"identity": canonical, "sha256": "new"})

    def test_manifest_source_sync_refreshes_same_live_document_without_explicit_item(self):
        exact = "https://github.com/acme/plans/blob/" + "a" * 40 + "/PLAN.md"
        manifest = {"projection": []}

        synced, source = workstream_projection.synchronize_manifest_source(
            manifest, f"Canonical plan: {exact}",
            {"identity": exact, "sha256": "new"},
            {"identity": "https://github.com/acme/plans/blob/main/PLAN.md",
             "sha256": "old"},
        )

        self.assertEqual(synced["projection"], [
            {"kind": "source", "key": "root", "value": source},
        ])
        self.assertEqual(manifest, {"projection": []})

    def test_manifest_source_sync_checks_prior_generation_document(self):
        old_plan = "b" * 64
        old_identity = "https://github.com/acme/old/blob/main/PLAN.md"
        old = build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={"identity": old_identity, "sha256": old_plan},
            plan_revision=old_plan, expected_revision=0,
            created_at="2026-08-26T12:00:00Z", authority=AUTHORITY,
        )
        reduced = reduce_projection_comments(
            [projection_comment(old)], workstream_id="GEN-37",
            expected_plan_revision=PLAN,
        )
        self.assertIsNone(reduced.snapshot["source"])
        manifest = {"projection": []}
        original = deepcopy(manifest)

        with self.assertRaisesRegex(
            LinearProjectionError,
            "live_source_document_change_requires_explicit_review",
        ):
            workstream_projection.synchronize_manifest_source(
                manifest,
                "Canonical plan: https://github.com/acme/new/blob/main/PLAN.md",
                {"identity": "https://github.com/acme/new/blob/main/PLAN.md",
                 "sha256": PLAN},
                reduced.snapshot["source"],
                reduced.snapshot["projection_history"],
            )

        self.assertEqual(manifest, original)

    def test_manifest_source_sync_auto_refreshes_prior_generation_same_document(self):
        old_plan = "b" * 64
        old = build_projection_event(
            workstream_id="GEN-37", kind="source", key="root",
            value={
                "identity": "https://github.com/acme/plans/blob/main/PLAN.md",
                "sha256": old_plan,
            },
            plan_revision=old_plan, expected_revision=0,
            created_at="2026-08-26T12:00:00Z", authority=AUTHORITY,
        )
        reduced = reduce_projection_comments(
            [projection_comment(old)], workstream_id="GEN-37",
            expected_plan_revision=PLAN,
        )
        exact = "https://github.com/acme/plans/blob/" + "a" * 40 + "/PLAN.md"

        synced, source = workstream_projection.synchronize_manifest_source(
            {"projection": []}, f"Canonical plan: {exact}",
            {"identity": exact, "sha256": PLAN},
            reduced.snapshot["source"], reduced.snapshot["projection_history"],
        )

        self.assertEqual(synced["projection"][0]["value"], source)
        self.assertEqual(source, {"identity": exact, "sha256": PLAN})

    def test_canonical_source_final_readback_refuses_concurrent_change(self):
        with self.assertRaisesRegex(
            LinearProjectionError, "canonical_plan_changed_during_projection",
        ):
            workstream_projection.validate_canonical_source_readback(
                "Canonical plan: https://github.com/acme/new/blob/main/PLAN.md",
                {"identity": "https://github.com/acme/old/blob/main/PLAN.md",
                 "sha256": "a" * 64},
            )

    def test_projection_cli_end_to_end_is_idempotent_and_full_resume_verified(self):
        raw = b"# Exact plan\n\n## Deliver\n"
        digest = hashlib.sha256(raw).hexdigest()
        identity = "https://example.test/commit/plan.md"
        client = FakeProjectionClient()
        scoped = scope()
        manifest = {
            "expected_projection_revision": 0,
            "expected_active_heads": [],
            "expected_legacy_v1_event_ids": [],
            "expected_legacy_v1_events_sha256": None,
            "expected_projection_quarantine_count": 0,
            "expected_projection_quarantine_sha256": hashlib.sha256(b"[]").hexdigest(),
            "retirements": [],
            "projection": [
            {"kind": "scope", "key": "root", "value": scoped},
            {"kind": "provenance", "key": "session", "value": {
                "agent": "codex", "machine": "M5", "session_id": "session",
                "worktree": {"state": "safe", "head": HEAD},
            }},
        ]}
        graph = {
            "root": {"identifier": "GEN-37", "url": "https://linear/GEN-37",
                     "description": f"Canonical plan: {identity}",
                     "plan_revision": digest, "revision": 0,
                     "status": "In Progress", "next_action": "continue"},
            "children": [{"identifier": "GEN-38", "title": "Resume transport",
                          "status": "In Progress", "next_action": "continue"}],
            "decisions": [],
        }
        route = {"workspace_id": "workspace", "team_id": "team",
                 "project_id": "project", "root_issue_id": ROOT_UUID}
        comments = mock.Mock()
        historical_comment = {
            "id": "existing-history", "body": "preserve this prior comment",
            "createdAt": "before", "updatedAt": "before",
        }
        client.comments.append(dict(historical_comment))
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
                "--max-bytes", "65536", "--max-items", "500",
            ]
            compact = workstream_projection.compact_context
            for expected_writes in (5, 5):
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
                    with mock.patch.object(
                        workstream_projection, "compact_context", wraps=compact,
                    ) as compact_mock:
                        self.assertEqual(workstream_projection.main(), 0)
                        self.assertEqual(compact_mock.call_args.kwargs["max_bytes"], 65536)
                        self.assertEqual(compact_mock.call_args.kwargs["max_items"], 500)
                payload = json.loads(output.getvalue())
                self.assertTrue(payload["readback_verified"])
                self.assertEqual(payload["source_sync"], {
                    "identity": identity,
                    "sha256": digest,
                    "resume_authority": "full",
                })
                self.assertEqual(len(client.comments), expected_writes)
                manifest.update(payload["projection_contract"])
        self.assertEqual(client.comments[0], historical_comment)
        self.assertFalse(any(
            "issueCreate" in query or "issueUpdate" in query
            for query, _variables in client.calls
        ))
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
